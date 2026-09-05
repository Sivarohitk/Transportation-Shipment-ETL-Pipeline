"""Writers for curated table outputs.

Dispatches to the appropriate backend based on ``output_format``:

- ``"parquet"`` (default) — existing Parquet + Hive registration path.
  Used for local and EMR targets.  Behavior is identical to Phase 1.
- ``"delta"`` — Databricks Delta backend via ``delta_writer.write_delta_table``.
  Used when ``spark.profile = databricks``.  The table name must be a
  fully-qualified ``catalog.schema.table`` string from ``common/catalog.py``.

The ``output_format`` should be derived from the active config rather than
hardcoded at call sites:

    from transport_etl.common.catalog import is_databricks
    fmt = "delta" if is_databricks(config) else "parquet"
"""

from __future__ import annotations

import logging
from typing import Any, Mapping

from transport_etl.common.io import write_parquet
from transport_etl.publish.catalog import register_parquet_table
from transport_etl.publish.partitions import ensure_partition_columns, required_partition_columns

LOGGER = logging.getLogger(__name__)

# Accepted output format values for write_partitioned_table.
_FORMAT_PARQUET = "parquet"
_FORMAT_DELTA = "delta"
_SUPPORTED_OUTPUT_FORMATS = {_FORMAT_PARQUET, _FORMAT_DELTA}


def _resolve_output_format(output_format: str) -> str:
    """Return a validated, normalised output format string."""
    normalised = str(output_format).strip().lower()
    if normalised not in _SUPPORTED_OUTPUT_FORMATS:
        raise ValueError(
            f"Unsupported output_format: '{output_format}'. "
            f"Expected one of {sorted(_SUPPORTED_OUTPUT_FORMATS)}."
        )
    return normalised


def write_partitioned_table(
    df: Any,
    table_name: str,
    output_path: str,
    partitions: list[str],
    mode: str = "overwrite",
    spark: Any | None = None,
    database: str = "curated",
    register_hive_table: bool = True,
    repair_partitions: bool = True,
    writer_options: Mapping[str, Any] | None = None,
    write_config: Mapping[str, Any] | None = None,
    logger: Any | None = None,
    output_format: str = _FORMAT_PARQUET,
) -> str:
    """Write a partitioned curated table to the appropriate storage backend.

    Dispatches to Parquet (local/EMR) or Delta (Databricks) based on
    ``output_format``.

    Parquet path (``output_format="parquet"``, unchanged from Phase 1):
        Writes partitioned Parquet files to ``output_path`` and optionally
        registers an external Hive table.  Supports Windows local fallback.

    Delta path (``output_format="delta"``):
        Writes a managed Delta table via ``saveAsTable``.  The ``table_name``
        must be a fully-qualified ``catalog.schema.table`` string.
        ``output_path`` is accepted for API consistency but is not used for
        the Delta write (Unity Catalog manages storage locations).
        ``register_hive_table`` and ``repair_partitions`` are ignored for
        Delta because Unity Catalog handles metadata automatically.

    Args:
        df:                 Curated Spark DataFrame to persist.
        table_name:         Target table name.  For Delta this must be a
                            three-part ``catalog.schema.table`` identifier.
        output_path:        Destination Parquet path (Parquet path only).
        partitions:         Ordered list of partition column names.
        mode:               Spark write mode (``"overwrite"`` or ``"append"``).
        spark:              Optional SparkSession for Hive registration.
        database:           Target Hive database (Parquet path only).
        register_hive_table: Register external Hive table after write
                            (Parquet path only).
        repair_partitions:  Run MSCK REPAIR TABLE after write (Parquet only).
        writer_options:     Optional Spark writer options (Parquet path only).
        write_config:       Optional write fallback config (Parquet path only).
        logger:             Optional Python logger.
        output_format:      ``"parquet"`` (default) or ``"delta"``.

    Returns:
        For Parquet: the final output path written (may be a fallback path).
        For Delta:   the ``table_name`` string (path is Unity Catalog-managed).
    """
    log = logger or LOGGER

    if df is None or not hasattr(df, "columns"):
        return output_path

    validated_format = _resolve_output_format(output_format)
    partition_columns = partitions or required_partition_columns()
    prepared_df = ensure_partition_columns(df=df, partitions=partition_columns)

    # ------------------------------------------------------------------
    # Delta path — Databricks / Unity Catalog
    # ------------------------------------------------------------------
    if validated_format == _FORMAT_DELTA:
        from transport_etl.publish.delta_writer import write_delta_table

        write_delta_table(
            df=prepared_df,
            table_name=table_name,
            partitions=partition_columns,
            mode=mode,
            logger=log,
        )
        return table_name

    # ------------------------------------------------------------------
    # Parquet path — local / EMR (unchanged from Phase 1)
    # ------------------------------------------------------------------
    written_format, written_path = write_parquet(
        df=prepared_df,
        path=output_path,
        mode=mode,
        partition_by=partition_columns,
        options=writer_options,
        write_config=write_config,
        logger=log,
    )

    session = spark
    if session is None and hasattr(prepared_df, "sparkSession"):
        session = prepared_df.sparkSession

    if register_hive_table and session is not None and written_format == "parquet":
        register_parquet_table(
            spark=session,
            table_name=table_name,
            output_path=written_path,
            schema=prepared_df.schema,
            partitions=partition_columns,
            database=database,
            repair_partitions=repair_partitions,
        )
    elif register_hive_table and session is not None and written_format != "parquet":
        log.warning(
            "Skipping Hive registration for table=%s because output format=%s path=%s",
            table_name,
            written_format,
            written_path,
        )

    return written_path
