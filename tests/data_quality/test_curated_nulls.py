"""Data quality tests for curated null policies."""

from __future__ import annotations


def _assert_no_nulls(df, columns: list[str]) -> None:
    """Assert that a DataFrame has no null values in selected columns."""
    from pyspark.sql import functions as F

    for column in columns:
        assert df.filter(F.col(column).isNull()).count() == 0


def test_curated_null_policy(curated_frames: dict[str, object]) -> None:
    """Curated key columns should be non-null for analytics reliability."""
    dim_carrier_df = curated_frames["dim_carrier"]
    fct_shipment_df = curated_frames["fct_shipment"]
    fct_delivery_event_df = curated_frames["fct_delivery_event"]

    _assert_no_nulls(dim_carrier_df, ["carrier_id", "region_code", "p_date"])
    _assert_no_nulls(fct_shipment_df, ["shipment_id", "carrier_id", "region_code", "p_date"])
    _assert_no_nulls(
        fct_delivery_event_df,
        ["event_id", "shipment_id", "carrier_id", "region_code", "p_date"],
    )
