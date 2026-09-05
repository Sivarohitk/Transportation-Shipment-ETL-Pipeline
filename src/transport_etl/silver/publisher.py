"""Silver layer publisher.

Dispatches a Silver DataFrame to the correct backend based on the active
execution target:

- **Local / EMR** (Parquet + Hive) — delegates to
  ``publish.hive_writer.write_partitioned_table`` so the existing
  ``curated.<table>`` Parquet path remains unchanged.

- **Databricks** (Delta) — renders and executes a Spark SQL
  ``MERGE INTO`` statement via :mod:`transport_etl.silver.merge`.  The
  MERGE contract (MATCH KEY, WHEN MATCHED, WHEN NOT MATCHED, late-
  arriving data, idempotency) is documented in
  :mod:`transport_etl.silver.merge_spec`.

Rerun semantics
---------------

- **Parquet (local / EMR):** the existing ``mode=overwrite`` Parquet
  path is preserved, with dynamic partition overwrite.  A rerun of the
  same ``run_date`` produces an identical Parquet payload for the
  source columns; the Silver lineage columns (``_silver_batch_id``,
  ``_silver_run_date``) are stable for the same batch.
- **Delta (Databricks):** the ``MERGE INTO`` statement is **inherently
  idempotent** because it matches on the business key and refreshes
  every row with the source values.  Rerunning the same source yields
  the same target state.

Rerun semantics are tested in :mod:`tests.integration.test_silver_layer`.
"""

from __future__ import annotations

import logging
from typing import Any, Mapping

try:
    from pyspark.sql import DataFrame, SparkSession
except ModuleNotFoundError:  # pragma: no cover
    DataFrame = Any  # type: ignore[assignment]
    SparkSession = Any  # type: ignore[assignment]

from transport_etl.common.catalog import is_databricks, resolve_table_name
from transport_etl.common.constants import (
    SILVER_TABLE_NAMES,
    TABLE_SILVER_CARRIERS,
    TABLE_SILVER_DELIVERY_EVENTS,
    TABLE_SILVER_SHIPMENTS,
)
from transport_etl.publish.hive_writer import write_partitioned_table
from transport_etl.silver.keys import is_silver_table

LOGGER = logging.getLogger("transport_etl.silver.publisher")


def _resolve_silver_base_path(config: Mapping[str, Any]) -> str:
    """Resolve the base path under which Silver Parquet outputs are written."""
    paths = config.get("paths", {}) if isinstance(config.get("paths"), Mapping) else {}
    staging = paths.get("staging_base_path") or paths.get("curated_base_path")
    if staging:
        return str(staging)
    return "data/local/curated"


def _resolve_silver_database(config: Mapping[str, Any]) -> str:
    """Return the Hive database name to use for Silver on local/EMR."""
    hive_section = config.get("hive", {}) if isinstance(config.get("hive"), Mapping) else {}
    database = hive_section.get("database", "curated")
    return str(database).strip() or "curated"


def _ensure_supported_table(table_name: str) -> str:
    """Validate that ``table_name`` is a known Silver table."""
    if not is_silver_table(table_name):
        raise ValueError(
            f"Unknown Silver table name '{table_name}'. "
            f"Expected one of {list(SILVER_TABLE_NAMES)}."
        )
    return str(table_name)


def _publish_local(
    df: DataFrame,
    *,
    config: Mapping[str, Any],
    table_name: str,
    partitions: list[str],
    mode: str,
    write_config: Mapping[str, Any] | None,
    writer_options: Mapping[str, Any] | None,
    spark: SparkSession | None,
    logger: Any,
    register_hive_table: bool | None = None,
    repair_partitions: bool | None = None,
) -> str:
    """Publish a Silver table as Parquet (local/EMR path)."""
    database = _resolve_silver_database(config)
    resolved_table = resolve_table_name(config=config, layer="silver", table=table_name)
    base_path = _resolve_silver_base_path(config)
    output_path = f"{base_path.rstrip('/\\')}/{table_name}"

    return write_partitioned_table(
        df=df,
        table_name=resolved_table,
        output_path=output_path,
        partitions=partitions,
        mode=mode,
        spark=spark,
        database=database,
        register_hive_table=register_hive_table if register_hive_table is not None else True,
        repair_partitions=repair_partitions if repair_partitions is not None else True,
        writer_options=writer_options,
        write_config=write_config,
        logger=logger,
        output_format="parquet",
    )


def _publish_databricks(
    df: DataFrame,
    *,
    config: Mapping[str, Any],
    table_name: str,
    spark: SparkSession,
    logger: Any,
) -> str:
    """Publish a Silver table via Delta MERGE on Databricks."""
    from transport_etl.silver.merge import execute_silver_merge

    target_table = resolve_table_name(config=config, layer="silver", table=table_name)
    all_columns = list(df.columns)
    sql = execute_silver_merge(
        spark=spark,
        source_df=df,
        target_table=target_table,
        table_name=table_name,
        all_columns=all_columns,
    )
    logger.info("Silver publish (Delta MERGE) complete table=%s", target_table)
    return sql


def publish_silver_table(
    df: DataFrame,
    *,
    config: Mapping[str, Any],
    table_name: str,
    spark: SparkSession | None = None,
    partitions: list[str] | None = None,
    mode: str = "overwrite",
    writer_options: Mapping[str, Any] | None = None,
    write_config: Mapping[str, Any] | None = None,
    logger: Any | None = None,
    register_hive_table: bool | None = None,
    repair_partitions: bool | None = None,
) -> str:
    """Publish a Silver DataFrame to the active execution backend.

    Args:
        df: Silver DataFrame produced by :mod:`transport_etl.silver.builder`.
        config: Merged application config dictionary.
        table_name: Silver logical table name (one of
            :data:`transport_etl.common.constants.SILVER_TABLE_NAMES`).
        spark: Optional SparkSession.  Required when targeting Databricks.
        partitions: Partition columns.  Defaults to an empty list (the
            publish writer does not enforce partitioning; callers can
            opt in via configuration).
        mode: Spark write mode (Parquet path only).
        writer_options: Optional Spark writer options (Parquet path only).
        write_config: Optional write-fallback configuration (Parquet path only).
        logger: Optional logger instance.

    Returns:
        The destination identifier written to.  For Parquet targets this
        is a path; for Delta targets it is the rendered MERGE SQL
        string (returned for observability — execution is logged in the
        caller).
    """
    log = logger or LOGGER
    _ensure_supported_table(table_name)

    if df is None or not hasattr(df, "columns"):
        log.warning("Silver publish skipped: empty DataFrame for table=%s", table_name)
        return ""

    if is_databricks(config):
        if spark is None:
            raise ValueError(
                "publish_silver_table requires a SparkSession when targeting Databricks"
            )
        return _publish_databricks(
            df,
            config=config,
            table_name=table_name,
            spark=spark,
            logger=log,
        )

    silver_partitions = list(partitions or [])
    return _publish_local(
        df,
        config=config,
        table_name=table_name,
        partitions=silver_partitions,
        mode=mode,
        write_config=write_config,
        writer_options=writer_options,
        spark=spark,
        logger=log,
        register_hive_table=register_hive_table,
        repair_partitions=repair_partitions,
    )


__all__ = [
    "TABLE_SILVER_CARRIERS",
    "TABLE_SILVER_DELIVERY_EVENTS",
    "TABLE_SILVER_SHIPMENTS",
    "publish_silver_table",
]
