"""Unit tests for shipment fact transformation."""

from __future__ import annotations

import pytest

from transport_etl.transform.build_fct_shipment import build_fct_shipment


def test_build_fct_shipment_returns_identity_without_spark() -> None:
    """Builder should keep non-Spark unit tests stable when Spark is unavailable."""
    payload = object()
    assert build_fct_shipment(payload, region_lookup_df=None) is payload


def test_ingest_shipments_enforces_schema_types(ingested_frames: dict[str, object]) -> None:
    """Ingestion should enforce typed schema from JSON definitions."""
    shipments_df = ingested_frames["shipments"]
    type_map = dict(shipments_df.dtypes)

    assert type_map["pickup_ts"] == "timestamp"
    assert type_map["promised_delivery_ts"] == "timestamp"
    assert type_map["shipping_cost_usd"] == "double"
    assert type_map["distance_miles"] == "double"


def test_build_fct_shipment_outputs_kpi_ready_columns(
    ingested_frames: dict[str, object],
    region_lookup_df,
) -> None:
    """Shipment fact builder should output deduped, KPI-ready records."""
    from pyspark.sql import functions as F

    fct_df = build_fct_shipment(
        stg_shipments_df=ingested_frames["shipments"],
        region_lookup_df=region_lookup_df,
    )

    required_columns = {
        "shipment_id",
        "carrier_id",
        "region_code",
        "p_date",
        "on_time_delivery_flag",
        "delay_minutes",
        "exception_flag",
        "transit_time_hours",
    }
    assert required_columns.issubset(set(fct_df.columns))

    duplicate_count = fct_df.groupBy("shipment_id").count().filter(F.col("count") > 1).count()
    assert duplicate_count == 0

    shp1002 = (
        fct_df.filter(F.col("shipment_id") == "SHP1002")
        .select("on_time_delivery_flag", "delay_minutes", "region_code")
        .collect()[0]
    )
    assert shp1002["on_time_delivery_flag"] == 0
    assert shp1002["delay_minutes"] > 0
    assert shp1002["region_code"] == "WEST"

    shp1006 = (
        fct_df.filter(F.col("shipment_id") == "SHP1006").select("shipping_cost_usd").collect()[0]
    )
    assert shp1006["shipping_cost_usd"] == pytest.approx(415.0, rel=1e-9)
