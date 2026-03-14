"""Partition utilities for curated outputs."""

from __future__ import annotations

from typing import Any, Mapping

try:
    from pyspark.sql import DataFrame
    from pyspark.sql import functions as F
except ModuleNotFoundError:  # pragma: no cover
    DataFrame = Any  # type: ignore[assignment]
    F = None  # type: ignore[assignment]

from transport_etl.common.constants import (
    PARTITION_COL_CARRIER,
    PARTITION_COL_DATE,
    PARTITION_COL_REGION,
)


PARTITION_COLUMNS = [PARTITION_COL_DATE, PARTITION_COL_REGION, PARTITION_COL_CARRIER]
DEFAULT_PARTITION_VALUES: dict[str, Any] = {
    PARTITION_COL_DATE: "1970-01-01",
    PARTITION_COL_REGION: "UNKNOWN",
    PARTITION_COL_CARRIER: "UNKNOWN",
}


def required_partition_columns() -> list[str]:
    """Return required partition columns for curated tables."""
    return list(PARTITION_COLUMNS)


def default_partition_values() -> dict[str, Any]:
    """Return default fallback values for required partition columns."""
    return dict(DEFAULT_PARTITION_VALUES)


def missing_partition_columns(df: DataFrame, partitions: list[str] | None = None) -> list[str]:
    """Return missing partition columns for a DataFrame."""
    target = partitions or required_partition_columns()
    return [column for column in target if column not in df.columns]


def _require_spark() -> None:
    """Ensure pyspark is available before executing Spark operations."""
    if F is None:
        raise ImportError("pyspark is required for partition utilities")


def _derive_partition_date(df: DataFrame) -> Any:
    """Derive `p_date` from common timestamp columns when absent/null."""
    candidate_columns = [
        "p_date",
        "event_ts",
        "pickup_ts",
        "actual_delivery_ts",
        "promised_delivery_ts",
        "updated_at",
    ]
    expressions = [F.to_date(F.col(column)) for column in candidate_columns if column in df.columns]

    if expressions:
        return F.coalesce(*expressions, F.to_date(F.lit(DEFAULT_PARTITION_VALUES[PARTITION_COL_DATE])))

    return F.to_date(F.lit(DEFAULT_PARTITION_VALUES[PARTITION_COL_DATE]))


def ensure_partition_columns(
    df: DataFrame,
    partitions: list[str] | None = None,
    defaults: Mapping[str, Any] | None = None,
) -> DataFrame:
    """Ensure required partition columns exist and are non-null.

    This helper is idempotent and safe to run repeatedly.
    """
    _require_spark()
    target = partitions or required_partition_columns()
    fill_values = {**default_partition_values(), **(defaults or {})}

    result = df

    if PARTITION_COL_DATE in target:
        result = result.withColumn(PARTITION_COL_DATE, _derive_partition_date(result))

    if PARTITION_COL_REGION in target:
        if PARTITION_COL_REGION not in result.columns:
            result = result.withColumn(PARTITION_COL_REGION, F.lit(fill_values[PARTITION_COL_REGION]))
        result = result.withColumn(
            PARTITION_COL_REGION,
            F.coalesce(F.col(PARTITION_COL_REGION), F.lit(fill_values[PARTITION_COL_REGION])),
        )

    if PARTITION_COL_CARRIER in target:
        if PARTITION_COL_CARRIER not in result.columns:
            result = result.withColumn(PARTITION_COL_CARRIER, F.lit(fill_values[PARTITION_COL_CARRIER]))
        result = result.withColumn(
            PARTITION_COL_CARRIER,
            F.coalesce(F.col(PARTITION_COL_CARRIER), F.lit(fill_values[PARTITION_COL_CARRIER])),
        )

    # Add any other requested partition columns using provided default values.
    for column in target:
        if column in {PARTITION_COL_DATE, PARTITION_COL_REGION, PARTITION_COL_CARRIER}:
            continue
        default_value = fill_values.get(column, "UNKNOWN")
        if column not in result.columns:
            result = result.withColumn(column, F.lit(default_value))
        result = result.withColumn(column, F.coalesce(F.col(column), F.lit(default_value)))

    return result
