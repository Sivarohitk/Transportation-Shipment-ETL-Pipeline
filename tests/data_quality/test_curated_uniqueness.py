"""Data quality tests for curated uniqueness policies."""

from __future__ import annotations


def _assert_unique(df, keys: list[str]) -> None:
    """Assert uniqueness for a key or composite key."""
    from pyspark.sql import functions as F

    assert df.groupBy(*keys).count().filter(F.col("count") > 1).count() == 0


def test_curated_uniqueness(curated_frames: dict[str, object]) -> None:
    """Curated tables should respect their declared grains."""
    _assert_unique(curated_frames["dim_carrier"], ["carrier_id", "p_date"])
    _assert_unique(curated_frames["fct_shipment"], ["shipment_id"])
    _assert_unique(curated_frames["fct_delivery_event"], ["event_id"])
    _assert_unique(curated_frames["agg_shipment_daily"], ["p_date", "region_code", "carrier_id"])
    _assert_unique(curated_frames["kpi_delivery_daily"], ["p_date", "region_code", "carrier_id"])
