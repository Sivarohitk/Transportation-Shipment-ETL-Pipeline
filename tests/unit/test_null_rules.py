"""Unit tests for null rules."""

from __future__ import annotations

from transport_etl.quality.nulls import (
    check_required_nulls,
    critical_columns_for_entity,
    filter_rows_with_required_nulls,
    split_by_required_nulls,
)


def test_critical_columns_for_entity_returns_defaults() -> None:
    """Null-rule config should provide critical columns for known entities."""
    columns = critical_columns_for_entity("shipment")

    assert "shipment_id" in columns
    assert "carrier_id" in columns


def test_check_required_nulls_handles_none_df() -> None:
    """Null checker should return zero counts for placeholder/non-Spark test mode."""
    result = check_required_nulls(df=None, required_columns=["shipment_id", "carrier_id"])

    assert result == {"shipment_id": 0, "carrier_id": 0}


def test_null_rule_filters_and_splits_invalid_rows(spark) -> None:
    """Null rule utilities should isolate rows with missing critical values."""
    df = spark.createDataFrame(
        [
            ("SHP1", "CAR1"),
            (None, "CAR2"),
            (" ", None),
        ],
        schema="shipment_id string, carrier_id string",
    )

    counts = check_required_nulls(df=df, required_columns=["shipment_id", "carrier_id"])
    assert counts["shipment_id"] == 2
    assert counts["carrier_id"] == 1

    invalid_df = filter_rows_with_required_nulls(
        df=df,
        required_columns=["shipment_id", "carrier_id"],
    )
    valid_df, split_invalid_df = split_by_required_nulls(
        df=df,
        required_columns=["shipment_id", "carrier_id"],
    )

    assert invalid_df.count() == 2
    assert split_invalid_df.count() == 2
    assert valid_df.count() == 1
