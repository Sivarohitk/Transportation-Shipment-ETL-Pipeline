"""Delta Lake publishing backend for the Databricks execution target.

This module is the Databricks-specific counterpart to ``hive_writer.py``.
It provides two operations:

1. **Overwrite** — a full partition-replace write using Spark's native Delta
   writer.  This is the default mode for Bronze and Gold outputs.

2. **Merge stub** — ``build_merge_spec`` returns a ``DeltaMergeSpec`` value
   object that describes a future MERGE/upsert operation.  The spec is
   intentionally decoupled from write execution so that Silver-layer business
   logic (Phase 4+) can be added without touching this module.

Design constraints (see AGENTS.md):
- This module must import cleanly in environments without PySpark or
  Databricks.  All Spark-dependent code is inside functions or guarded by
  ``try/except ModuleNotFoundError``.
- Only the Databricks execution target uses Delta.  Parquet remains the
  default for local and EMR.
- No credentials, workspace URLs, or account IDs are referenced here.
- Table names must arrive as pre-resolved ``catalog.schema.table`` strings
  produced by ``common/catalog.py``.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

LOGGER = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# DeltaMergeSpec — value object describing a future MERGE operation
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class DeltaMergeSpec:
    """Immutable specification for a Delta MERGE / upsert operation.

    This value object captures *what* a MERGE should do.  It is produced by
    ``build_merge_spec`` and consumed by ``execute_merge`` (which requires a
    live Databricks session).  Separating specification from execution allows
    unit tests to verify the spec without a running cluster.

    Attributes:
        target_table:   Fully-qualified ``catalog.schema.table`` identifier.
        source_alias:   Alias for the incoming (source) DataFrame in the
                        MERGE statement.
        target_alias:   Alias for the target Delta table in the MERGE statement.
        merge_keys:     Column names that uniquely identify a row (match keys).
        update_columns: Column names to update when a match is found.
                        ``None`` means update all non-key columns.
        insert_all_on_not_matched: When ``True`` the MERGE inserts the full
                                   source row when no match is found.
    """

    target_table: str
    source_alias: str = "source"
    target_alias: str = "target"
    merge_keys: tuple[str, ...] = field(default_factory=tuple)
    update_columns: tuple[str, ...] | None = None
    insert_all_on_not_matched: bool = True

    def merge_condition(self) -> str:
        """Return the ON clause expression joining source and target."""
        if not self.merge_keys:
            raise ValueError("merge_keys must not be empty when building a MERGE condition")
        clauses = [
            f"{self.target_alias}.{key} = {self.source_alias}.{key}" for key in self.merge_keys
        ]
        return " AND ".join(clauses)


def build_merge_spec(
    target_table: str,
    merge_keys: list[str],
    update_columns: list[str] | None = None,
    source_alias: str = "source",
    target_alias: str = "target",
    insert_all_on_not_matched: bool = True,
) -> DeltaMergeSpec:
    """Build a ``DeltaMergeSpec`` from configuration arguments.

    This is the intended entry point for constructing merge specifications.
    Silver-layer transforms call this to declare *intent*; ``execute_merge``
    carries out the actual write.

    Args:
        target_table:   Fully-qualified ``catalog.schema.table`` identifier.
        merge_keys:     Business key columns used in the MERGE ON clause.
        update_columns: Columns to update on match.  ``None`` means all
                        non-key columns.
        source_alias:   Alias for the incoming DataFrame.
        target_alias:   Alias for the existing Delta table.
        insert_all_on_not_matched: Insert full source row when no match found.

    Returns:
        An immutable ``DeltaMergeSpec`` instance.
    """
    if not target_table or not target_table.strip():
        raise ValueError("target_table must not be empty")
    if not merge_keys:
        raise ValueError("merge_keys must not be empty")

    return DeltaMergeSpec(
        target_table=target_table.strip(),
        source_alias=source_alias,
        target_alias=target_alias,
        merge_keys=tuple(merge_keys),
        update_columns=tuple(update_columns) if update_columns is not None else None,
        insert_all_on_not_matched=insert_all_on_not_matched,
    )


# ---------------------------------------------------------------------------
# Write mode helpers — pure logic, no Spark dependency
# ---------------------------------------------------------------------------

# Supported Spark write modes for Delta.
# ``overwrite`` replaces the specified partitions (dynamic partition overwrite).
# ``append``    adds rows without touching existing data.
# Both are safe for incremental processing when used with the right
# ``spark.sql.sources.partitionOverwriteMode=dynamic`` setting.
_SUPPORTED_DELTA_WRITE_MODES = {"overwrite", "append"}


def resolve_delta_write_mode(mode: str) -> str:
    """Return a validated, normalised Delta write mode string.

    Args:
        mode: Caller-supplied write mode (case-insensitive).

    Returns:
        Normalised lowercase mode string.

    Raises:
        ValueError: If ``mode`` is not supported for Delta writes.
    """
    normalised = str(mode).strip().lower()
    if normalised not in _SUPPORTED_DELTA_WRITE_MODES:
        raise ValueError(
            f"Unsupported Delta write mode: '{mode}'. "
            f"Expected one of {sorted(_SUPPORTED_DELTA_WRITE_MODES)}."
        )
    return normalised


# ---------------------------------------------------------------------------
# Delta write — requires PySpark; guarded so the module imports without it
# ---------------------------------------------------------------------------


def write_delta_table(
    df: Any,
    table_name: str,
    partitions: list[str],
    mode: str = "overwrite",
    logger: Any | None = None,
) -> None:
    """Write a Spark DataFrame as a managed Delta table on Databricks.

    The table is written using ``saveAsTable`` so the Delta table is
    registered in Unity Catalog automatically.  The caller must pass a
    fully-qualified ``catalog.schema.table`` name produced by
    ``common/catalog.resolve_table_name``.

    Partition columns are applied explicitly.  Dynamic partition overwrite
    mode (``spark.sql.sources.partitionOverwriteMode=dynamic``) must be
    enabled at the session or cluster level — this is set in
    ``config/databricks.yaml`` and ``config/spark/databricks.conf``.

    This function requires an active PySpark session.  It must only be called
    when the Databricks Spark profile is active.  Importing this module in a
    non-Spark environment is safe; calling this function without PySpark will
    raise ``ImportError``.

    Args:
        df:         Spark DataFrame to persist.
        table_name: Fully-qualified ``catalog.schema.table`` identifier.
        partitions: Ordered list of partition column names.
        mode:       Write mode — ``"overwrite"`` or ``"append"``.
        logger:     Optional Python logger for progress messages.
    """
    try:
        from pyspark.sql import DataFrame  # noqa: F401
    except ModuleNotFoundError as exc:
        raise ImportError(
            "write_delta_table requires PySpark. " "Install pyspark before calling this function."
        ) from exc

    if df is None or not hasattr(df, "write"):
        raise TypeError("df must be a Spark DataFrame")

    validated_mode = resolve_delta_write_mode(mode)

    log = logger or LOGGER
    log.info(
        "Writing Delta table table=%s partitions=%s mode=%s",
        table_name,
        partitions,
        validated_mode,
    )

    writer = df.write.format("delta").mode(validated_mode)
    if partitions:
        writer = writer.partitionBy(*partitions)

    writer.saveAsTable(table_name)

    log.info("Delta write complete table=%s", table_name)


# ---------------------------------------------------------------------------
# Delta MERGE execution — requires PySpark + Delta; guarded import
# ---------------------------------------------------------------------------


def execute_merge(
    spark: Any,
    source_df: Any,
    spec: DeltaMergeSpec,
    logger: Any | None = None,
) -> None:
    """Execute a Delta MERGE using a pre-built ``DeltaMergeSpec``.

    This function relies on the ``delta-spark`` library (``DeltaTable`` API)
    which is present on the Databricks runtime.  It is not available in
    standard PySpark and must not be called outside Databricks.

    The function is marked ``# pragma: no cover`` because integration tests
    for real MERGE operations require a live Databricks workspace and are
    therefore skipped in CI (see AGENTS.md rule 9).

    Args:
        spark:      Active SparkSession (must be the Databricks cluster session).
        source_df:  Incoming DataFrame containing new/updated rows.
        spec:       ``DeltaMergeSpec`` describing the merge operation.
        logger:     Optional Python logger.
    """  # pragma: no cover
    try:
        from delta.tables import DeltaTable  # type: ignore[import]
    except ModuleNotFoundError as exc:
        raise ImportError(
            "execute_merge requires the delta-spark package which is available "
            "on the Databricks runtime.  Do not call this function outside Databricks."
        ) from exc

    log = logger or LOGGER
    log.info(
        "Executing Delta MERGE target=%s merge_keys=%s",
        spec.target_table,
        spec.merge_keys,
    )

    delta_table = DeltaTable.forName(spark, spec.target_table)
    merge_condition = spec.merge_condition()

    merge_builder = delta_table.alias(spec.target_alias).merge(
        source_df.alias(spec.source_alias),
        merge_condition,
    )

    if spec.update_columns is not None:
        update_set = {col: f"{spec.source_alias}.{col}" for col in spec.update_columns}
        merge_builder = merge_builder.whenMatchedUpdate(set=update_set)
    else:
        merge_builder = merge_builder.whenMatchedUpdateAll()

    if spec.insert_all_on_not_matched:
        merge_builder = merge_builder.whenNotMatchedInsertAll()

    merge_builder.execute()
    log.info("Delta MERGE complete target=%s", spec.target_table)
