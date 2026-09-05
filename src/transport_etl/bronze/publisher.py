"""Bronze layer publisher.

Dispatches a Bronze DataFrame to the correct backend based on the active
execution target:

- **Local / EMR** (Parquet + Hive) — delegates to
  ``publish.hive_writer.write_partitioned_table`` so the existing Phase 1
  Parquet write path and Windows local fallback behaviour remain
  unchanged.

- **Databricks** (Delta) — delegates to
  ``publish.delta_writer.write_delta_table`` and targets the
  ``<catalog>.bronze.<table>`` Unity Catalog identifier resolved by
  ``common.catalog.resolve_table_name``.

Rerun semantics
---------------

- **Parquet (local / EMR):** ``mode=overwrite`` combined with the
  ``spark.sql.sources.partitionOverwriteMode=dynamic`` setting that is
  already part of every Spark profile config.  Only the partitions
  present in the new data are replaced, so reruns of the same ``run_date``
  produce an identical, deterministic Parquet payload for unchanged
  sources.

- **Delta (Databricks):** ``saveAsTable`` with ``mode=overwrite`` and the
  same dynamic partition overwrite setting.  Matching source files
  produce matching Delta payloads; the metadata ``_ingested_at`` and
  ``_batch_id`` columns will reflect the rerun, but row content from
  ``_source_file``, ``_run_date``, and source columns is stable.

All paths, catalog names, schema names, and table names are read from
the merged application config.  No string literals for storage
locations are introduced in this module.
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
    BRONZE_TABLE_NAMES,
    TABLE_BRONZE_CARRIERS,
    TABLE_BRONZE_DELIVERY_EVENTS,
    TABLE_BRONZE_SHIPMENTS,
)
from transport_etl.publish.hive_writer import write_partitioned_table

LOGGER = logging.getLogger(__name__)


def _require_spark() -> None:
    """Ensure pyspark is available before executing Spark operations."""
    if SparkSession is Any:  # type: ignore[comparison-overlap]
        return
    raise ImportError("pyspark is required for Bronze publishing")


def _resolve_bronze_output_format(config: Mapping[str, Any]) -> str:
    """Return ``"delta"`` on Databricks, otherwise ``"parquet"``.

    The decision is driven by the active Spark profile rather than by a
    free-form configuration flag so local and EMR code paths remain
    untouched (see AGENTS.md rules 1, 2, 3).
    """
    if is_databricks(config):
        return "delta"
    return "parquet"


def _resolve_bronze_base_path(config: Mapping[str, Any]) -> str:
    """Resolve the base path under which Bronze Parquet outputs are written.

    For Databricks targets the base path is unused — Unity Catalog
    manages storage locations — but it is still resolved so the caller
    can log a meaningful destination and remain symmetric with the
    Parquet path.
    """
    paths = config.get("paths", {}) if isinstance(config.get("paths"), Mapping) else {}
    staging = paths.get("staging_base_path") or paths.get("curated_base_path")
    if staging:
        return str(staging)
    return "data/local/curated"


def publish_bronze_table(
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
    """Publish a Bronze DataFrame to the active execution backend.

    Args:
        df: Bronze DataFrame produced by ``bronze.builder``.
        config: Merged application config dictionary.
        table_name: Bronze table base name (one of ``BRONZE_TABLE_NAMES``).
        spark: Optional SparkSession used for Hive registration.  When
            ``None`` the writer attempts to read it from the DataFrame.
        partitions: Partition columns.  Defaults to an empty list for
            Bronze — Phase 4 Bronze is stored unpartitioned so a full
            overwrite can deterministically replace the dataset for a
            given run date.
        mode: Spark write mode (``"overwrite"`` or ``"append"``).
        writer_options: Optional Spark writer options (Parquet only).
        write_config: Optional write fallback configuration (Parquet only).
        logger: Optional logger instance.

    Returns:
        The destination identifier written to: a path string for Parquet
        targets, or the resolved ``catalog.schema.table`` identifier for
        Delta targets.
    """
    log = logger or LOGGER
    if table_name not in BRONZE_TABLE_NAMES:
        raise ValueError(
            f"Unknown Bronze table name '{table_name}'. Expected one of {list(BRONZE_TABLE_NAMES)}."
        )

    output_format = _resolve_bronze_output_format(config)
    bronze_base_path = _resolve_bronze_base_path(config)
    resolved_table = resolve_table_name(config=config, layer="bronze", table=table_name)
    bronze_partitions = list(partitions) if partitions else []

    log.info(
        "Publishing Bronze table=%s format=%s partition_columns=%s mode=%s",
        resolved_table,
        output_format,
        bronze_partitions,
        mode,
    )

    # Default register/repair behaviour mirrors the curated writer:
    # Parquet targets register and repair (local/EMR); Delta targets
    # skip both because Unity Catalog manages metadata automatically.
    # Callers may override for local development scenarios.
    if register_hive_table is None:
        register_hive_table = output_format == "parquet"
    if repair_partitions is None:
        repair_partitions = output_format == "parquet"

    written = write_partitioned_table(
        df=df,
        table_name=resolved_table,
        output_path=f"{bronze_base_path.rstrip('/\\')}/{table_name}",
        partitions=bronze_partitions,
        mode=mode,
        spark=spark,
        database=str(
            (config.get("hive", {}) or {}).get("database", "curated")
            if isinstance(config.get("hive"), Mapping)
            else "curated"
        ),
        register_hive_table=register_hive_table,
        repair_partitions=repair_partitions,
        writer_options=writer_options,
        write_config=write_config,
        logger=log,
        output_format=output_format,
    )

    log.info("Bronze publish complete table=%s destination=%s", resolved_table, written)
    return written


__all__ = [
    "BRONZE_TABLE_NAMES",
    "TABLE_BRONZE_CARRIERS",
    "TABLE_BRONZE_DELIVERY_EVENTS",
    "TABLE_BRONZE_SHIPMENTS",
    "publish_bronze_table",
]
