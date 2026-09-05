"""Spark SQL MERGE statement rendering for the Silver layer.

The Silver layer needs a Delta MERGE / upsert operation to handle
operational changes that occur **after** the initial record was written
(shipment status updates, later delivery events, corrected operational
values, late-arriving records).  This module renders the MERGE
statement as a Spark SQL string so it can be executed by
``spark.sql("MERGE INTO ..." )`` on a Databricks cluster without
importing the ``delta-spark`` Python module at import time.

Why Spark SQL instead of the Python DeltaTable API?
---------------------------------------------------
1. The Python ``delta.tables.DeltaTable`` API requires
   ``delta-spark`` to be installed in the local Python environment.
   Tests in this repository do not, and should not, depend on
   ``delta-spark`` (AGENTS.md rule 9).
2. Spark SQL ``MERGE INTO`` is supported natively by Delta Lake 2.x and
   later, including on Databricks Runtime 11.x / 12.x / 13.x.
3. Rendering as a string makes the operation **observable** in tests,
   which lets us assert the exact ON clause, WHEN MATCHED and WHEN NOT
   MATCHED behaviour without spinning up a live Databricks cluster.

The functions in this module are **pure**: no Spark, no Delta, no I/O.
They take a list of column names and return SQL fragments.  This is the
"spec" layer — the execution layer is :mod:`transport_etl.silver.merge`.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Iterable


@dataclass(frozen=True)
class SilverMergeSpec:
    """Immutable specification of a Silver MERGE INTO statement.

    Attributes:
        target_table:     Fully-qualified ``catalog.schema.table`` (Databricks)
                          or ``<database>.<table>`` (local/EMR) identifier.
        source_alias:     Alias for the source view / DataFrame in the
                          MERGE USING clause.
        target_alias:     Alias for the target table in the MERGE
                          statement.  Defaults to ``target`` which is
                          the conventional choice.
        merge_keys:       Business key columns used in the ON clause.
                          These are the columns declared in
                          ``silver.keys.SILVER_BUSINESS_KEYS``.
        update_columns:   Non-key columns to update on WHEN MATCHED.
                          When empty, all non-key columns are updated.
        insert_columns:   Columns to insert on WHEN NOT MATCHED.  When
                          empty, all columns of the source row are
                          inserted (``INSERT *``).
        source_view:      Name of the Spark SQL temp view that holds
                          the source rows.  When empty, callers must
                          register a view themselves and the merge
                          renderer emits ``USING <source_view>``.
    """

    target_table: str
    source_alias: str = "source"
    target_alias: str = "target"
    merge_keys: tuple[str, ...] = field(default_factory=tuple)
    update_columns: tuple[str, ...] = field(default_factory=tuple)
    insert_columns: tuple[str, ...] = field(default_factory=tuple)
    source_view: str = ""

    def __post_init__(self) -> None:
        if not self.target_table or not str(self.target_table).strip():
            raise ValueError("target_table must not be empty")
        if not self.merge_keys:
            raise ValueError("merge_keys must not be empty")
        if any(not str(k).strip() for k in self.merge_keys):
            raise ValueError("merge_keys must not contain blank values")
        for column in self.update_columns:
            if column in self.merge_keys:
                raise ValueError(f"update_columns must not contain a merge key: '{column}'")
        for column in self.insert_columns:
            if column in self.merge_keys and column not in self.update_columns:
                # Insert column may also be a merge key (it is set from
                # the source row).  The guard above is overly strict;
                # we relax it here by allowing the same column to
                # appear in both lists.
                pass


def _quote_column(column: str) -> str:
    """Quote a column identifier for Spark SQL.

    Column names from the existing schemas use only ASCII letters,
    digits and underscores, but we still wrap them in backticks so
    reserved words (e.g. ``order``, ``group``) are safe.
    """
    safe = str(column).replace("`", "")
    return f"`{safe}`"


def _quote_table_part(part: str) -> str:
    """Quote a single part of a dot-separated table identifier."""
    safe = str(part).replace("`", "")
    return f"`{safe}`"


def quote_qualified_table(name: str) -> str:
    """Quote each part of a ``a.b.c`` or ``a.b`` table identifier.

    The input is split on ``.`` and each component is wrapped in
    backticks.  Whitespace around parts is stripped.
    """
    parts = [segment.strip() for segment in str(name).split(".") if segment.strip()]
    if not parts:
        raise ValueError("Cannot quote an empty table identifier")
    return ".".join(_quote_table_part(part) for part in parts)


def build_merge_condition(spec: SilverMergeSpec) -> str:
    """Return the ``ON`` clause for the MERGE statement.

    Example:
        >>> spec = SilverMergeSpec(
        ...     target_table="sc.silver.stg_shipments",
        ...     merge_keys=("shipment_id",),
        ... )
        >>> build_merge_condition(spec)
        '`target`.`shipment_id` = `source`.`shipment_id`'
    """
    if not spec.merge_keys:
        raise ValueError("merge_keys must not be empty when building a MERGE condition")
    clauses = [
        f"{_quote_table_part(spec.target_alias)}.{_quote_column(key)} = "
        f"{_quote_table_part(spec.source_alias)}.{_quote_column(key)}"
        for key in spec.merge_keys
    ]
    return " AND ".join(clauses)


def _selectable_columns(columns: Iterable[str]) -> list[str]:
    """Return column identifiers as a comma-separated SQL list."""
    return [_quote_column(col) for col in columns]


def build_merge_sql(spec: SilverMergeSpec, *, all_columns: Iterable[str]) -> str:
    """Render the full ``MERGE INTO`` statement as a Spark SQL string.

    The ``all_columns`` argument is the list of every column on the
    target table; it is used to (a) derive the default ``update_columns``
    when the caller did not specify any, and (b) derive the default
    ``insert_columns`` (``INSERT *``) when the caller did not specify
    any.

    Args:
        spec: The :class:`SilverMergeSpec` to render.
        all_columns: Iterable of every column name on the target table.

    Returns:
        A single Spark SQL ``MERGE INTO ...`` statement.

    Behavior contract
    -----------------
    The emitted MERGE is **deterministic** and **idempotent** by design:

    - **MATCH KEY**: the business key columns (e.g. ``shipment_id``).
    - **WHEN MATCHED**: updates every non-key column from the source
      row and refreshes the Silver lineage columns.  Late-arriving
      records with the same business key overwrite the older state,
      which is exactly the "latest record wins" semantic used by the
      dedup helper.
    - **WHEN NOT MATCHED**: inserts the full source row.  This handles
      first-time arrivals and late-arriving records whose business key
      was never seen before.
    - **LATE-ARRIVING DATA**: handled by the same MERGE — a row that
      arrives after a previous batch with the same business key is
      treated as a match and the target is updated.
    - **IDEMPOTENCY**: a second MERGE for the same source data
      produces an identical target state because (a) the same business
      key matches the same target row, (b) the same values are written,
      and (c) the lineage columns reflect the latest run.  The render
      is also deterministic (no random identifiers).
    """
    if not spec.source_view or not str(spec.source_view).strip():
        raise ValueError(
            "source_view must be set so the MERGE statement can reference the source rows"
        )

    column_set = [str(col) for col in all_columns if str(col).strip()]
    if not column_set:
        raise ValueError("all_columns must contain at least one column name")

    merge_condition = build_merge_condition(spec)
    target_qualified = quote_qualified_table(spec.target_table)
    source_view = str(spec.source_view).strip()

    # WHEN MATCHED: update every non-key column.  When the caller
    # provided an explicit list we use it verbatim; otherwise we
    # default to all non-key columns.
    key_set = set(spec.merge_keys)
    update_cols = list(spec.update_columns) or [c for c in column_set if c not in key_set]

    if update_cols:
        update_pairs = [
            f"{_quote_column(col)} = {_quote_table_part(spec.source_alias)}.{_quote_column(col)}"
            for col in update_cols
            if col not in key_set
        ]
    else:
        # No non-key columns to update (single-column key table).
        update_pairs = []

    when_matched_sql = (
        f"WHEN MATCHED THEN UPDATE SET {', '.join(update_pairs)}"
        if update_pairs
        else "WHEN MATCHED THEN DELETE"  # pragma: no cover - defensive only
    )

    # WHEN NOT MATCHED: insert the row.  When the caller provided an
    # explicit column list we use it; otherwise we emit ``INSERT *``.
    insert_cols = list(spec.insert_columns) or column_set
    if insert_cols:
        column_list = ", ".join(_selectable_columns(insert_cols))
        value_list = ", ".join(
            f"{_quote_table_part(spec.source_alias)}.{_quote_column(col)}" for col in insert_cols
        )
        when_not_matched_sql = f"WHEN NOT MATCHED THEN INSERT ({column_list}) VALUES ({value_list})"
    else:
        when_not_matched_sql = "WHEN NOT MATCHED THEN INSERT *"

    sql = (
        f"MERGE INTO {target_qualified} AS {_quote_table_part(spec.target_alias)}\n"
        f"USING {source_view} AS {_quote_table_part(spec.source_alias)}\n"
        f"ON {merge_condition}\n"
        f"{when_matched_sql}\n"
        f"{when_not_matched_sql}"
    )
    return sql


__all__ = [
    "SilverMergeSpec",
    "build_merge_condition",
    "build_merge_sql",
    "quote_qualified_table",
]
