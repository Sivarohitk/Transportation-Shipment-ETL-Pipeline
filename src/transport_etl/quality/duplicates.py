"""Duplicate detection and optional de-duplication helpers."""

from __future__ import annotations

from typing import Any

try:
    from pyspark.sql import DataFrame
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window
except ModuleNotFoundError:  # pragma: no cover - allows non-Spark unit tests
    DataFrame = Any  # type: ignore[assignment]
    F = None  # type: ignore[assignment]
    Window = None  # type: ignore[assignment]


ENTITY_KEY_COLUMNS: dict[str, list[str]] = {
    "shipments": ["shipment_id"],
    "carriers": ["carrier_id"],
    "delivery_events": ["event_id"],
}


def _require_spark() -> None:
    """Ensure pyspark is available before executing Spark operations."""
    if F is None or Window is None:
        raise ImportError("pyspark is required for duplicate checks")


def key_columns_for_entity(entity: str) -> list[str]:
    """Return key columns used for duplicate detection by entity."""
    normalized = entity.strip().lower()
    if normalized.endswith("s"):
        key = normalized
    else:
        key = f"{normalized}s"
    return list(ENTITY_KEY_COLUMNS.get(key, []))


def _validate_key_columns(df: DataFrame, key_columns: list[str]) -> None:
    """Validate duplicate key columns for a DataFrame."""
    if not key_columns:
        raise ValueError("At least one key column is required for duplicate detection")

    missing = [column for column in key_columns if column not in df.columns]
    if missing:
        raise ValueError(f"Duplicate key columns missing from DataFrame: {missing}")


def find_duplicate_keys(df: Any, key_columns: list[str]) -> Any:
    """Return grouped duplicate keys with their duplicate counts.

    Returns `None` when `df` is None to preserve compatibility with lightweight
    unit tests that do not spin up Spark.
    """
    if df is None:
        return None

    _require_spark()
    _validate_key_columns(df=df, key_columns=key_columns)

    return (
        df.groupBy(*key_columns)
        .agg(F.count(F.lit(1)).alias("duplicate_count"))
        .filter(F.col("duplicate_count") > F.lit(1))
    )


def find_entity_duplicates(df: DataFrame, entity: str) -> DataFrame:
    """Find duplicate keys for a known entity."""
    keys = key_columns_for_entity(entity)
    if not keys:
        raise ValueError(f"No duplicate-key definition configured for entity: {entity}")

    return find_duplicate_keys(df=df, key_columns=keys)


def duplicate_records(df: DataFrame, key_columns: list[str]) -> DataFrame:
    """Return full duplicate records joined from duplicate keys."""
    _require_spark()
    _validate_key_columns(df=df, key_columns=key_columns)

    dup_keys = find_duplicate_keys(df=df, key_columns=key_columns)
    return df.join(dup_keys.select(*key_columns), on=key_columns, how="inner")


def split_by_duplicates(
    df: DataFrame,
    key_columns: list[str],
    order_by_columns: list[str] | None = None,
) -> tuple[DataFrame, DataFrame]:
    """Split DataFrame into deduplicated and duplicate record subsets.

    The first row per key partition is retained, preferring higher values for
    provided `order_by_columns` (e.g., `updated_at`).
    """
    _require_spark()
    _validate_key_columns(df=df, key_columns=key_columns)

    order_columns = [col for col in (order_by_columns or []) if col in df.columns]
    if not order_columns:
        sort_exprs = [F.lit(1)]
    else:
        sort_exprs = [F.col(col).desc_nulls_last() for col in order_columns]

    window_spec = Window.partitionBy(*key_columns).orderBy(*sort_exprs)
    ranked = df.withColumn("__dup_rank", F.row_number().over(window_spec))

    deduped_df = ranked.filter(F.col("__dup_rank") == 1).drop("__dup_rank")
    duplicate_df = ranked.filter(F.col("__dup_rank") > 1).drop("__dup_rank")

    return deduped_df, duplicate_df
