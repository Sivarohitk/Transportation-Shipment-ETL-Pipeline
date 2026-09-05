"""Silver-layer MERGE orchestrator.

This module is the execution layer for the Silver MERGE contract
declared in :mod:`transport_etl.silver.merge_spec`.  It performs the
following steps:

1. Builds a :class:`SilverMergeSpec` for the requested table.
2. Registers the source DataFrame as a temporary view so the rendered
   MERGE statement can reference it.
3. Renders the MERGE statement as a Spark SQL string.
4. Executes it through ``spark.sql(...)``.

A live Delta table on Databricks is required for step 4.  When
``delta-spark`` is not present, the function raises a clear
``ImportError`` so the caller can decide whether to skip the MERGE
(e.g. on local / EMR where Parquet overwrite is used instead).

Local / EMR
-----------
The orchestrator is only invoked when the active execution target is
``databricks`` (see :mod:`transport_etl.common.catalog`).  Local and
EMR code paths must continue to use Parquet overwrite because
``MERGE INTO`` requires a Delta table.

Idempotency
-----------
The orchestrator never modifies the source DataFrame and only writes
to the target via a single ``MERGE INTO`` call.  Re-running the
orchestrator with the same source produces the same target state.

Quarantine
----------
Invalid records must be quarantined **before** the orchestrator is
called; the orchestrator only writes rows whose quality checks have
already passed.
"""

from __future__ import annotations

import logging
from typing import Any, Iterable

try:
    from pyspark.sql import DataFrame, SparkSession
except ModuleNotFoundError:  # pragma: no cover
    DataFrame = Any  # type: ignore[assignment]
    SparkSession = Any  # type: ignore[assignment]

from transport_etl.silver.keys import business_key_for
from transport_etl.silver.merge_spec import (
    SilverMergeSpec,
    build_merge_sql,
)

LOGGER = logging.getLogger("transport_etl.silver.merge")


def _require_spark() -> None:
    """Ensure pyspark is available before executing Spark operations.

    The function checks the ``pyspark.sql.functions`` import guard —
    the same pattern used by ``quality.rules`` and ``quality.duplicates``.
    """
    if DataFrame is Any:  # type: ignore[comparison-overlap]
        raise ImportError("pyspark is required for Silver MERGE operations")


def build_silver_merge_spec(
    *,
    target_table: str,
    table_name: str,
    all_columns: Iterable[str],
    source_view: str,
    update_columns: Iterable[str] | None = None,
    insert_columns: Iterable[str] | None = None,
    target_alias: str = "target",
    source_alias: str = "source",
) -> SilverMergeSpec:
    """Build a :class:`SilverMergeSpec` for a Silver table.

    This helper looks up the canonical business key from
    :mod:`transport_etl.silver.keys` so callers cannot accidentally
    pick the wrong key.  The same key is used by the dedup helper
    and the MERGE ON clause — by construction they cannot diverge.

    Args:
        target_table: Fully-qualified Delta table identifier
            (``catalog.schema.table``).
        table_name: Silver logical table name (e.g. ``stg_shipments``).
        all_columns: Iterable of every column on the target table.
        source_view: Temp view that holds the source DataFrame.
        update_columns: Explicit non-key columns to refresh on match.
            When ``None`` the rendered MERGE updates every non-key
            column.
        insert_columns: Explicit columns to insert when no match is
            found.  When ``None`` the rendered MERGE inserts the full
            source row (``INSERT *``).
        target_alias: Alias for the target table in the rendered
            statement.
        source_alias: Alias for the source view in the rendered
            statement.

    Returns:
        An immutable :class:`SilverMergeSpec`.
    """
    merge_keys = business_key_for(table_name)
    return SilverMergeSpec(
        target_table=target_table,
        source_alias=source_alias,
        target_alias=target_alias,
        merge_keys=merge_keys,
        update_columns=tuple(update_columns) if update_columns is not None else (),
        insert_columns=tuple(insert_columns) if insert_columns is not None else (),
        source_view=source_view,
    )


def register_source_view(
    spark: SparkSession,
    source_df: DataFrame,
    view_name: str,
) -> str:
    """Register a Spark temp view containing the MERGE source rows.

    The view name is returned unchanged so the caller can pass it to
    :func:`build_silver_merge_spec`.  Spark temp views are
    session-scoped and do not persist, which is exactly what we want
    for a single-batch MERGE.
    """
    _require_spark()
    if source_df is None:
        raise ValueError("source_df must not be None")
    if not view_name or not str(view_name).strip():
        raise ValueError("view_name must not be empty")
    source_df.createOrReplaceTempView(str(view_name).strip())
    return str(view_name).strip()


def render_silver_merge_sql(
    spec: SilverMergeSpec,
    all_columns: Iterable[str],
) -> str:
    """Render the MERGE SQL string for a :class:`SilverMergeSpec`.

    Exposed so tests can assert the exact SQL text.
    """
    return build_merge_sql(spec, all_columns=all_columns)


def execute_silver_merge(
    spark: SparkSession,
    source_df: DataFrame,
    *,
    target_table: str,
    table_name: str,
    all_columns: Iterable[str],
    view_name: str | None = None,
) -> str:
    """Execute a Spark SQL MERGE INTO statement against a Delta table.

    Steps:

    1. Register the source DataFrame as a temp view (or use the
       caller-supplied ``view_name``).
    2. Build the :class:`SilverMergeSpec`.
    3. Render the MERGE SQL.
    4. Execute via ``spark.sql(...)``.

    Args:
        spark: Active SparkSession (must be a Databricks cluster session
            with Delta Lake available).
        source_df: Source DataFrame containing the Silver records to
            merge.
        target_table: Fully-qualified Delta table name.
        table_name: Silver logical table name (e.g. ``stg_shipments``).
        all_columns: Iterable of every column on the target table.
        view_name: Optional temp view name.  When ``None`` a default
            ``silver_<table>`` view is created.

    Returns:
        The MERGE SQL string that was executed.
    """
    _require_spark()
    if spark is None or not hasattr(spark, "sql"):
        raise ValueError("execute_silver_merge requires a live SparkSession")

    view = view_name or f"silver_{table_name}"
    register_source_view(spark=spark, source_df=source_df, view_name=view)

    spec = build_silver_merge_spec(
        target_table=target_table,
        table_name=table_name,
        all_columns=all_columns,
        source_view=view,
    )
    sql = build_merge_sql(spec, all_columns=all_columns)

    LOGGER.info(
        "Executing Silver MERGE target=%s table=%s",
        target_table,
        table_name,
    )
    LOGGER.debug("Silver MERGE SQL: %s", sql)

    spark.sql(sql)
    LOGGER.info("Silver MERGE complete target=%s", target_table)
    return sql


__all__ = [
    "build_silver_merge_spec",
    "execute_silver_merge",
    "register_source_view",
    "render_silver_merge_sql",
]
