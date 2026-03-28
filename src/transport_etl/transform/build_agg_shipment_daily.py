"""Build curated agg_shipment_daily table."""

from __future__ import annotations

from typing import Any

try:
    from pyspark.sql import DataFrame
    from pyspark.sql import functions as F
except ModuleNotFoundError:  # pragma: no cover
    DataFrame = Any  # type: ignore[assignment]
    F = None  # type: ignore[assignment]


def _supports_spark(df: Any) -> bool:
    """Return whether Spark DataFrame operations are available."""
    return F is not None and hasattr(df, "columns")


def build_agg_shipment_daily(fct_shipment_df: Any) -> Any:
    """Aggregate shipment facts into daily carrier-region summaries.

    Grain:
    - One row per (`p_date`, `region_code`, `carrier_id`).

    The aggregate retains KPI-driving metrics so downstream KPI models can
    compute rates without re-reading shipment grain data.
    """
    if not _supports_spark(fct_shipment_df):
        # Keeps non-Spark unit tests stable in lightweight environments.
        return fct_shipment_df

    df = fct_shipment_df

    # Partition contract must always be present and non-null.
    df = (
        df.withColumn("p_date", F.coalesce(F.col("p_date"), F.current_date()))
        .withColumn("region_code", F.coalesce(F.col("region_code"), F.lit("UNKNOWN")))
        .withColumn("carrier_id", F.coalesce(F.col("carrier_id"), F.lit("UNKNOWN")))
    )

    # Ensure numeric KPI columns exist for robust aggregation.
    safe_on_time = F.coalesce(F.col("on_time_delivery_flag"), F.lit(0))
    safe_delay = F.coalesce(F.col("delay_minutes"), F.lit(0.0))
    safe_exception = F.coalesce(F.col("exception_flag"), F.lit(0))
    safe_transit = F.col("transit_time_hours")
    safe_cost = F.coalesce(F.col("shipping_cost_usd"), F.lit(0.0))
    safe_distance = F.coalesce(F.col("distance_miles"), F.lit(0.0))
    delivered_flag = F.coalesce(F.col("delivered_flag"), F.lit(0))

    agg = (
        df.groupBy("p_date", "region_code", "carrier_id")
        .agg(
            F.countDistinct(F.col("shipment_id")).alias("total_shipments"),
            F.sum(delivered_flag).cast("long").alias("delivered_shipments"),
            F.sum(safe_on_time).cast("long").alias("on_time_shipments"),
            F.sum(F.when(safe_delay > F.lit(0.0), F.lit(1)).otherwise(F.lit(0)))
            .cast("long")
            .alias("delayed_shipments"),
            F.sum(safe_exception).cast("long").alias("exception_shipments"),
            F.sum(safe_delay).cast("double").alias("total_delay_minutes"),
            F.avg(F.when(safe_delay > F.lit(0.0), safe_delay))
            .cast("double")
            .alias("avg_delay_minutes"),
            F.avg(safe_transit).cast("double").alias("avg_transit_time_hours"),
            F.sum(safe_cost).cast("double").alias("total_shipping_cost_usd"),
            F.sum(safe_distance).cast("double").alias("total_distance_miles"),
        )
        .withColumn(
            "on_time_delivery_rate",
            F.when(
                F.col("delivered_shipments") > 0,
                F.col("on_time_shipments") / F.col("delivered_shipments"),
            ).otherwise(F.lit(0.0)),
        )
        .withColumn(
            "exception_rate",
            F.when(
                F.col("total_shipments") > 0,
                F.col("exception_shipments") / F.col("total_shipments"),
            ).otherwise(F.lit(0.0)),
        )
        .withColumn(
            "avg_cost_per_mile",
            F.when(
                F.col("total_distance_miles") > 0,
                F.col("total_shipping_cost_usd") / F.col("total_distance_miles"),
            ).otherwise(F.lit(0.0)),
        )
    )

    return agg
