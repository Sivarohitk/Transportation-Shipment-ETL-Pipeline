"""Hive catalog registration helpers."""

from __future__ import annotations

from typing import Any


def _supports_spark(spark: Any) -> bool:
    """Return whether Spark SQL operations are available."""
    return spark is not None and hasattr(spark, "sql") and hasattr(spark, "catalog")


def _quote_ident(identifier: str) -> str:
    """Quote SQL identifier with backticks."""
    safe = identifier.replace("`", "")
    return f"`{safe}`"


def _spark_type_to_sql(data_type: Any) -> str:
    """Convert Spark DataType to SQL DDL type string."""
    if hasattr(data_type, "simpleString"):
        raw = str(data_type.simpleString()).lower()
    else:
        raw = str(data_type).lower()

    mapping = {
        "string": "STRING",
        "int": "INT",
        "integer": "INT",
        "bigint": "BIGINT",
        "long": "BIGINT",
        "double": "DOUBLE",
        "float": "FLOAT",
        "boolean": "BOOLEAN",
        "date": "DATE",
        "timestamp": "TIMESTAMP",
    }

    if raw in mapping:
        return mapping[raw]

    # Keep complex Spark types readable for SQL DDL (e.g., decimal(10,2), array<string>).
    return raw.upper()


def ensure_curated_database(spark: Any, database: str = "curated", location: str | None = None) -> None:
    """Ensure curated Hive database exists."""
    if not _supports_spark(spark):
        return

    if location:
        escaped_location = location.replace("'", "''")
        spark.sql(
            f"CREATE DATABASE IF NOT EXISTS {_quote_ident(database)} LOCATION '{escaped_location}'"
        )
    else:
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {_quote_ident(database)}")


def table_exists(spark: Any, database: str, table_name: str) -> bool:
    """Return True when table exists in the Spark catalog."""
    if not _supports_spark(spark):
        return False
    return bool(spark.catalog.tableExists(f"{database}.{table_name}"))


def register_parquet_table(
    spark: Any,
    table_name: str,
    output_path: str,
    schema: Any,
    partitions: list[str],
    database: str = "curated",
    repair_partitions: bool = True,
) -> None:
    """Create/ensure external Parquet table and optionally repair partitions."""
    if not _supports_spark(spark):
        return

    ensure_curated_database(spark=spark, database=database)

    fields = getattr(schema, "fields", []) if schema is not None else []
    non_partition_fields = [field for field in fields if getattr(field, "name", None) not in partitions]

    if not non_partition_fields:
        raise ValueError("Cannot register table without non-partition fields")

    columns_ddl = ",\n  ".join(
        f"{_quote_ident(field.name)} {_spark_type_to_sql(field.dataType)}"
        for field in non_partition_fields
    )
    partitions_ddl = ", ".join(_quote_ident(partition) for partition in partitions)
    escaped_path = output_path.replace("'", "''")

    create_sql = (
        f"CREATE TABLE IF NOT EXISTS {_quote_ident(database)}.{_quote_ident(table_name)} (\n"
        f"  {columns_ddl}\n"
        f")\n"
        f"USING PARQUET\n"
        f"PARTITIONED BY ({partitions_ddl})\n"
        f"LOCATION '{escaped_path}'"
    )

    spark.sql(create_sql)

    if repair_partitions:
        spark.sql(f"MSCK REPAIR TABLE {_quote_ident(database)}.{_quote_ident(table_name)}")

    spark.sql(f"REFRESH TABLE {_quote_ident(database)}.{_quote_ident(table_name)}")
