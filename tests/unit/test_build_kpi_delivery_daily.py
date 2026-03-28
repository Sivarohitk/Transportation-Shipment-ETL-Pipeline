"""Unit tests for KPI transformation logic."""

from __future__ import annotations

from transport_etl.transform.build_kpi_delivery_daily import build_kpi_delivery_daily


def test_build_kpi_delivery_daily_returns_identity_without_spark() -> None:
    """Builder should keep non-Spark unit tests stable in placeholder mode."""
    payload = object()
    assert build_kpi_delivery_daily(payload) is payload


def test_build_kpi_delivery_daily_grain_and_columns(curated_frames: dict[str, object]) -> None:
    """KPI output should be unique at date/region/carrier grain."""
    from pyspark.sql import functions as F

    kpi_df = curated_frames["kpi_delivery_daily"]
    expected_columns = {
        "p_date",
        "region_code",
        "carrier_id",
        "on_time_delivery_rate",
        "avg_transit_hours",
        "late_delivery_rate",
        "first_attempt_success_rate",
        "exception_rate",
        "avg_cost_per_mile",
        "volume_by_carrier",
        "delivery_event_density",
    }
    assert expected_columns.issubset(set(kpi_df.columns))

    row_count = kpi_df.count()
    grain_count = kpi_df.select("p_date", "region_code", "carrier_id").distinct().count()
    assert row_count > 0
    assert row_count == grain_count

    out_of_range_rates = kpi_df.filter(
        (F.col("on_time_delivery_rate") < 0)
        | (F.col("on_time_delivery_rate") > 1)
        | (F.col("late_delivery_rate") < 0)
        | (F.col("late_delivery_rate") > 1)
        | (F.col("first_attempt_success_rate") < 0)
        | (F.col("first_attempt_success_rate") > 1)
        | (F.col("exception_rate") < 0)
        | (F.col("exception_rate") > 1)
    ).count()
    assert out_of_range_rates == 0


def test_build_kpi_delivery_daily_matches_formulae(curated_frames: dict[str, object]) -> None:
    """KPI metrics should align with formulae from aggregate and event facts."""
    from pyspark.sql import functions as F

    agg_df = curated_frames["agg_shipment_daily"]
    event_df = curated_frames["fct_delivery_event"]
    kpi_df = curated_frames["kpi_delivery_daily"]

    event_rollup = event_df.groupBy("p_date", "region_code", "carrier_id").agg(
        F.countDistinct(F.when(F.col("event_type") == "DELIVERED", F.col("shipment_id"))).alias(
            "delivered_event_shipments"
        ),
        F.countDistinct(
            F.when(
                (F.col("event_type") == "DELIVERED") & (F.col("attempt_number") == 1),
                F.col("shipment_id"),
            )
        ).alias("first_attempt_success_shipments"),
        F.count(F.lit(1)).alias("total_delivery_events"),
    )

    expected = (
        agg_df.join(event_rollup, on=["p_date", "region_code", "carrier_id"], how="left")
        .withColumn(
            "delivered_event_shipments", F.coalesce(F.col("delivered_event_shipments"), F.lit(0))
        )
        .withColumn(
            "first_attempt_success_shipments",
            F.coalesce(F.col("first_attempt_success_shipments"), F.lit(0)),
        )
        .withColumn("total_delivery_events", F.coalesce(F.col("total_delivery_events"), F.lit(0)))
        .withColumn(
            "delivered_denominator",
            F.greatest(F.col("delivered_shipments"), F.col("delivered_event_shipments")),
        )
        .withColumn(
            "exp_on_time_delivery_rate",
            F.when(
                F.col("delivered_denominator") > 0,
                F.col("on_time_shipments") / F.col("delivered_denominator"),
            ).otherwise(F.lit(0.0)),
        )
        .withColumn(
            "exp_late_delivery_rate",
            F.when(
                F.col("delivered_denominator") > 0,
                F.col("delayed_shipments") / F.col("delivered_denominator"),
            ).otherwise(F.lit(0.0)),
        )
        .withColumn(
            "exp_first_attempt_success_rate",
            F.when(
                F.col("delivered_denominator") > 0,
                F.col("first_attempt_success_shipments") / F.col("delivered_denominator"),
            ).otherwise(F.lit(0.0)),
        )
        .withColumn(
            "exp_exception_rate",
            F.when(
                F.col("total_shipments") > 0,
                F.col("exception_shipments") / F.col("total_shipments"),
            ).otherwise(F.lit(0.0)),
        )
        .withColumn(
            "exp_avg_cost_per_mile",
            F.when(
                F.col("total_distance_miles") > 0,
                F.col("total_shipping_cost_usd") / F.col("total_distance_miles"),
            ).otherwise(F.lit(0.0)),
        )
        .withColumn("exp_volume_by_carrier", F.col("total_shipments"))
        .withColumn(
            "exp_delivery_event_density",
            F.when(
                F.col("total_shipments") > 0,
                F.col("total_delivery_events") / F.col("total_shipments"),
            ).otherwise(F.lit(0.0)),
        )
    )

    joined = kpi_df.alias("k").join(
        expected.alias("e"),
        on=["p_date", "region_code", "carrier_id"],
        how="inner",
    )
    assert joined.count() == kpi_df.count()

    metrics = [
        ("on_time_delivery_rate", "exp_on_time_delivery_rate"),
        ("late_delivery_rate", "exp_late_delivery_rate"),
        ("first_attempt_success_rate", "exp_first_attempt_success_rate"),
        ("exception_rate", "exp_exception_rate"),
        ("avg_cost_per_mile", "exp_avg_cost_per_mile"),
        ("volume_by_carrier", "exp_volume_by_carrier"),
        ("delivery_event_density", "exp_delivery_event_density"),
    ]

    for actual_col, expected_col in metrics:
        max_abs_diff = joined.select(
            F.max(F.abs(F.col(f"k.{actual_col}") - F.col(f"e.{expected_col}"))).alias("d")
        ).collect()[0]["d"]
        assert (max_abs_diff or 0.0) < 1e-9
