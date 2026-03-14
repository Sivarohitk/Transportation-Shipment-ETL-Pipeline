"""Build curated kpi_delivery_daily table."""

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


def _safe_col(df: DataFrame, column: str, fallback: Any) -> Any:
    """Return existing column expression or fallback literal."""
    if column in df.columns:
        return F.col(column)
    if hasattr(fallback, "_jc"):
        return fallback
    return F.lit(fallback)


def _delivery_event_metrics(fct_delivery_event_df: Any) -> Any:
    """Aggregate event-level metrics required for KPI rate calculations."""
    if not _supports_spark(fct_delivery_event_df):
        return None

    return (
        fct_delivery_event_df.groupBy("p_date", "region_code", "carrier_id")
        .agg(
            F.count(F.lit(1)).cast("long").alias("total_delivery_events"),
            F.countDistinct(
                F.when(F.col("event_type") == F.lit("DELIVERED"), F.col("shipment_id"))
            ).cast("long").alias("delivered_event_shipments"),
            F.countDistinct(
                F.when(
                    (F.col("event_type") == F.lit("DELIVERED"))
                    & (F.col("attempt_number") == F.lit(1)),
                    F.col("shipment_id"),
                )
            ).cast("long").alias("first_attempt_success_shipments"),
        )
    )


def build_kpi_delivery_daily(
    agg_shipment_daily_df: Any,
    fct_delivery_event_df: Any | None = None,
) -> Any:
    """Compute daily KPI outputs at (`p_date`, `region_code`, `carrier_id`) grain.

    Required KPI outputs:
    - on_time_delivery_rate
    - avg_transit_hours
    - late_delivery_rate
    - first_attempt_success_rate
    - exception_rate
    - avg_cost_per_mile
    - volume_by_carrier
    - delivery_event_density
    """
    if not _supports_spark(agg_shipment_daily_df):
        # Keeps lightweight unit tests stable in non-Spark environments.
        return agg_shipment_daily_df

    base = (
        agg_shipment_daily_df
        .withColumn("p_date", _safe_col(agg_shipment_daily_df, "p_date", F.current_date()))
        .withColumn("region_code", F.coalesce(_safe_col(agg_shipment_daily_df, "region_code", None), F.lit("UNKNOWN")))
        .withColumn("carrier_id", F.coalesce(_safe_col(agg_shipment_daily_df, "carrier_id", None), F.lit("UNKNOWN")))
        .withColumn("total_shipments", _safe_col(agg_shipment_daily_df, "total_shipments", 0).cast("long"))
        .withColumn("delivered_shipments", _safe_col(agg_shipment_daily_df, "delivered_shipments", 0).cast("long"))
        .withColumn("on_time_shipments", _safe_col(agg_shipment_daily_df, "on_time_shipments", 0).cast("long"))
        .withColumn("delayed_shipments", _safe_col(agg_shipment_daily_df, "delayed_shipments", 0).cast("long"))
        .withColumn("exception_shipments", _safe_col(agg_shipment_daily_df, "exception_shipments", 0).cast("long"))
        .withColumn("avg_transit_time_hours", _safe_col(agg_shipment_daily_df, "avg_transit_time_hours", None).cast("double"))
        .withColumn("total_shipping_cost_usd", _safe_col(agg_shipment_daily_df, "total_shipping_cost_usd", 0.0).cast("double"))
        .withColumn("total_distance_miles", _safe_col(agg_shipment_daily_df, "total_distance_miles", 0.0).cast("double"))
    )

    event_metrics_df = _delivery_event_metrics(fct_delivery_event_df)
    if event_metrics_df is not None:
        base = base.join(event_metrics_df, on=["p_date", "region_code", "carrier_id"], how="left")
    else:
        base = (
            base.withColumn("total_delivery_events", F.lit(0).cast("long"))
            .withColumn("delivered_event_shipments", F.lit(0).cast("long"))
            .withColumn("first_attempt_success_shipments", F.lit(0).cast("long"))
        )

    base = (
        base.withColumn("total_delivery_events", F.coalesce(F.col("total_delivery_events"), F.lit(0)))
        .withColumn("delivered_event_shipments", F.coalesce(F.col("delivered_event_shipments"), F.lit(0)))
        .withColumn("first_attempt_success_shipments", F.coalesce(F.col("first_attempt_success_shipments"), F.lit(0)))
    )

    delivered_denominator = F.greatest(F.col("delivered_event_shipments"), F.col("delivered_shipments"))

    result = (
        base
        .withColumn(
            "on_time_delivery_rate",
            F.when(delivered_denominator > F.lit(0), F.col("on_time_shipments") / delivered_denominator).otherwise(F.lit(0.0)),
        )
        .withColumn(
            "avg_transit_hours",
            F.coalesce(F.col("avg_transit_time_hours"), F.lit(0.0)),
        )
        .withColumn(
            "late_delivery_rate",
            F.when(delivered_denominator > F.lit(0), F.col("delayed_shipments") / delivered_denominator).otherwise(F.lit(0.0)),
        )
        .withColumn(
            "first_attempt_success_rate",
            F.when(delivered_denominator > F.lit(0), F.col("first_attempt_success_shipments") / delivered_denominator).otherwise(F.lit(0.0)),
        )
        .withColumn(
            "exception_rate",
            F.when(F.col("total_shipments") > F.lit(0), F.col("exception_shipments") / F.col("total_shipments")).otherwise(F.lit(0.0)),
        )
        .withColumn(
            "avg_cost_per_mile",
            F.when(F.col("total_distance_miles") > F.lit(0), F.col("total_shipping_cost_usd") / F.col("total_distance_miles")).otherwise(F.lit(0.0)),
        )
        .withColumn("volume_by_carrier", F.col("total_shipments"))
        .withColumn(
            "delivery_event_density",
            F.when(F.col("total_shipments") > F.lit(0), F.col("total_delivery_events") / F.col("total_shipments")).otherwise(F.lit(0.0)),
        )
    )

    output_columns = [
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
        "total_shipments",
        "delivered_shipments",
        "on_time_shipments",
        "delayed_shipments",
        "exception_shipments",
        "total_delivery_events",
    ]

    return result.select(*[column for column in output_columns if column in result.columns])
