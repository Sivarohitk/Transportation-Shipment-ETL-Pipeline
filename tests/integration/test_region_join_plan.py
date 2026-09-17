"""The bounded state lookup should broadcast rather than shuffle large facts."""

from __future__ import annotations

import pytest

from transport_etl.transform.enrich_region import enrich_with_region


def test_region_join_broadcasts_lookup_and_preserves_deterministic_mapping(spark):
    if not hasattr(spark, "_jvm"):
        pytest.skip("Physical-plan assertion requires a classic SparkSession")
    previous = spark.conf.get("spark.sql.autoBroadcastJoinThreshold")
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    try:
        shipments = spark.createDataFrame(
            [("S1", "CA"), ("S2", "TX")], ["shipment_id", "destination_state"]
        )
        lookup = spark.createDataFrame(
            [("CA", "WEST"), ("CA", "PACIFIC"), ("TX", "SOUTH")],
            ["state_code", "region_code"],
        )
        enriched = enrich_with_region(shipments, lookup)
        plan = enriched._jdf.queryExecution().executedPlan().toString()
        assert "BroadcastHashJoin" in plan
        assert {row["shipment_id"]: row["region_code"] for row in enriched.collect()} == {
            "S1": "PACIFIC",
            "S2": "SOUTH",
        }
    finally:
        spark.conf.set("spark.sql.autoBroadcastJoinThreshold", previous)
