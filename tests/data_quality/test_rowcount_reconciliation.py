"""Data quality tests for row-count reconciliation."""

from __future__ import annotations


def test_rowcount_reconciliation(
    ingested_frames: dict[str, object],
    staging_frames: dict[str, object],
    curated_frames: dict[str, object],
) -> None:
    """Curated row counts should reconcile with upstream staging counts."""
    from pyspark.sql import functions as F

    raw_shipment_count = ingested_frames["shipments"].count()
    raw_event_count = ingested_frames["delivery_events"].count()

    stg_shipment_count = staging_frames["shipments"].count()
    stg_event_count = staging_frames["delivery_events"].count()

    fct_shipment_count = curated_frames["fct_shipment"].count()
    fct_event_count = curated_frames["fct_delivery_event"].count()
    agg_count = curated_frames["agg_shipment_daily"].count()
    kpi_count = curated_frames["kpi_delivery_daily"].count()

    assert stg_shipment_count <= raw_shipment_count
    assert stg_event_count <= raw_event_count
    assert fct_shipment_count <= stg_shipment_count
    assert fct_event_count <= stg_event_count
    assert agg_count > 0
    assert kpi_count == agg_count

    agg_total_shipments = (
        curated_frames["agg_shipment_daily"]
        .agg(F.sum(F.col("total_shipments")).alias("total_shipments"))
        .collect()[0]["total_shipments"]
    )
    kpi_total_volume = (
        curated_frames["kpi_delivery_daily"]
        .agg(F.sum(F.col("volume_by_carrier")).alias("total_volume"))
        .collect()[0]["total_volume"]
    )

    assert agg_total_shipments == fct_shipment_count
    assert kpi_total_volume == agg_total_shipments
