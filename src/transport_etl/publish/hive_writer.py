"""Writers for Hive-style curated table outputs."""

from __future__ import annotations

from typing import Any, Mapping

from transport_etl.common.io import write_parquet
from transport_etl.publish.catalog import register_parquet_table
from transport_etl.publish.partitions import ensure_partition_columns, required_partition_columns


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
) -> str:
    """Write partitioned Parquet and optionally register Hive metadata.

    Args:
        df: Curated Spark DataFrame to persist.
        table_name: Target Hive table name.
        output_path: Destination Parquet path.
        partitions: Partition column list.
        mode: Spark write mode.
        spark: Optional SparkSession. If omitted, inferred from `df` when possible.
        database: Target Hive database name.
        register_hive_table: Whether to register/refresh Hive metadata.
        repair_partitions: Whether to run MSCK REPAIR TABLE after write.
        writer_options: Optional Spark writer options.
        write_config: Optional write fallback config.
        logger: Optional logger for fallback and registration messages.

    Returns:
        Final output path written (fallback path when used).
    """
    if df is None or not hasattr(df, "columns"):
        return output_path

    partition_columns = partitions or required_partition_columns()
    prepared_df = ensure_partition_columns(df=df, partitions=partition_columns)

    written_format, written_path = write_parquet(
        df=prepared_df,
        path=output_path,
        mode=mode,
        partition_by=partition_columns,
        options=writer_options,
        write_config=write_config,
        logger=logger,
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
    elif register_hive_table and session is not None and logger is not None:
        logger.warning(
            "Skipping Hive registration for table=%s because output format=%s path=%s",
            table_name,
            written_format,
            written_path,
        )

    return written_path
