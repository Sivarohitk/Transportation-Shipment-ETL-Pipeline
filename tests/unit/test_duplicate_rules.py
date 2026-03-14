"""Unit tests for duplicate rule helpers."""

from __future__ import annotations

from transport_etl.quality.duplicates import (
    find_duplicate_keys,
    key_columns_for_entity,
    split_by_duplicates,
)


def test_key_columns_for_entity_returns_primary_key_mapping() -> None:
    """Duplicate key map should expose configured entity key columns."""
    assert key_columns_for_entity("shipment") == ["shipment_id"]
    assert key_columns_for_entity("carrier") == ["carrier_id"]
    assert key_columns_for_entity("delivery_event") == ["event_id"]


def test_find_duplicate_keys_handles_none_df() -> None:
    """Duplicate finder should safely return None for placeholder mode."""
    assert find_duplicate_keys(df=None, key_columns=["shipment_id"]) is None


def test_find_duplicate_keys_detects_duplicate_groups(spark) -> None:
    """Duplicate finder should identify duplicated keys and counts."""
    df = spark.createDataFrame(
        [("SHP1",), ("SHP1",), ("SHP2",)],
        schema="shipment_id string",
    )

    duplicate_rows = find_duplicate_keys(df=df, key_columns=["shipment_id"]).collect()

    assert len(duplicate_rows) == 1
    assert duplicate_rows[0]["shipment_id"] == "SHP1"
    assert duplicate_rows[0]["duplicate_count"] == 2


def test_split_by_duplicates_keeps_latest_record_per_key(spark) -> None:
    """Dedup splitter should keep the latest record when order_by is provided."""
    from pyspark.sql import functions as F

    df = spark.createDataFrame(
        [
            ("SHP1", "old", "2026-01-01T10:00:00Z"),
            ("SHP1", "new", "2026-01-01T11:00:00Z"),
            ("SHP2", "only", "2026-01-01T09:00:00Z"),
        ],
        schema="shipment_id string, payload string, updated_at string",
    ).withColumn("updated_at", F.to_timestamp("updated_at", "yyyy-MM-dd'T'HH:mm:ssX"))

    deduped_df, duplicate_df = split_by_duplicates(
        df=df,
        key_columns=["shipment_id"],
        order_by_columns=["updated_at"],
    )

    kept_payload = (
        deduped_df.filter(F.col("shipment_id") == "SHP1")
        .select("payload")
        .collect()[0]["payload"]
    )

    assert deduped_df.count() == 2
    assert duplicate_df.count() == 1
    assert kept_payload == "new"
