"""Null quality checks for critical columns."""

from __future__ import annotations

from typing import Any

try:
    from pyspark.sql import DataFrame
    from pyspark.sql import functions as F
except ModuleNotFoundError:  # pragma: no cover - allows non-Spark unit tests
    DataFrame = Any  # type: ignore[assignment]
    F = None  # type: ignore[assignment]


CRITICAL_COLUMNS_BY_ENTITY: dict[str, list[str]] = {
    "shipments": [
        "shipment_id",
        "carrier_id",
        "origin_state",
        "destination_state",
        "pickup_ts",
        "promised_delivery_ts",
        "updated_at",
    ],
    "carriers": [
        "carrier_id",
        "carrier_name",
        "service_mode",
        "updated_at",
    ],
    "delivery_events": [
        "event_id",
        "shipment_id",
        "event_type",
        "event_ts",
        "updated_at",
    ],
}


def _require_spark() -> None:
    """Ensure pyspark is available before executing Spark operations."""
    if F is None:
        raise ImportError("pyspark is required for null quality checks")


def critical_columns_for_entity(entity: str) -> list[str]:
    """Return critical columns for a supported entity."""
    normalized = entity.strip().lower()
    if normalized.endswith("s"):
        key = normalized
    else:
        key = f"{normalized}s"

    return list(CRITICAL_COLUMNS_BY_ENTITY.get(key, []))


def build_null_or_blank_condition(column_name: str) -> Any:
    """Build a null-or-blank condition for the provided column."""
    _require_spark()
    return F.col(column_name).isNull() | (F.trim(F.col(column_name).cast("string")) == F.lit(""))


def check_required_nulls(df: Any, required_columns: list[str]) -> dict[str, int]:
    """Count null/blank records for each required column.

    Returns zeros for all columns when `df` is None to keep function behavior
    safe for lightweight unit tests.
    """
    if not required_columns:
        return {}

    if df is None:
        return {col: 0 for col in required_columns}

    _require_spark()

    present_columns = [col for col in required_columns if col in df.columns]
    missing_columns = [col for col in required_columns if col not in df.columns]

    if not present_columns:
        total_count = int(df.count())
        return {col: total_count for col in required_columns}

    agg_expressions = [
        F.sum(F.when(build_null_or_blank_condition(col), F.lit(1)).otherwise(F.lit(0))).alias(col)
        for col in present_columns
    ]
    row = df.agg(*agg_expressions).collect()[0].asDict()

    counts = {col: int(row.get(col, 0) or 0) for col in present_columns}

    if missing_columns:
        total_count = int(df.count())
        for col in missing_columns:
            counts[col] = total_count

    return counts


def null_profile(df: DataFrame, required_columns: list[str]) -> list[dict[str, Any]]:
    """Return null count and null rate statistics for required columns."""
    counts = check_required_nulls(df=df, required_columns=required_columns)
    total_count = int(df.count())

    profile: list[dict[str, Any]] = []
    for column in required_columns:
        null_count = int(counts.get(column, 0))
        null_rate = float(null_count / total_count) if total_count > 0 else 0.0
        profile.append(
            {
                "column": column,
                "null_count": null_count,
                "total_count": total_count,
                "null_rate": null_rate,
            }
        )

    return profile


def filter_rows_with_required_nulls(df: DataFrame, required_columns: list[str]) -> DataFrame:
    """Return rows with null/blank values in one or more required columns."""
    _require_spark()
    if not required_columns:
        return df.limit(0)

    violation_exprs = [
        F.when(build_null_or_blank_condition(column), F.lit(column))
        for column in required_columns
        if column in df.columns
    ]

    if not violation_exprs:
        return df.withColumn("__failed_null_columns", F.array()).limit(0)

    failed_columns = F.expr("filter(__failed_null_columns_raw, x -> x is not null)")
    return (
        df.withColumn("__failed_null_columns_raw", F.array(*violation_exprs))
        .withColumn("__failed_null_columns", failed_columns)
        .drop("__failed_null_columns_raw")
        .filter(F.size(F.col("__failed_null_columns")) > 0)
    )


def split_by_required_nulls(
    df: DataFrame, required_columns: list[str]
) -> tuple[DataFrame, DataFrame]:
    """Split input into valid and invalid (null violating) records."""
    _require_spark()
    invalid_df = filter_rows_with_required_nulls(df=df, required_columns=required_columns)
    present_columns = [column for column in required_columns if column in df.columns]
    if not present_columns:
        return df, invalid_df

    invalid_condition = build_null_or_blank_condition(present_columns[0])
    for column in present_columns[1:]:
        invalid_condition = invalid_condition | build_null_or_blank_condition(column)

    valid_df = df.filter(~invalid_condition)
    return valid_df, invalid_df
