"""Build curated ``route_performance`` Gold analytics table.

Grain:
    - One row per (``p_date``, ``origin_region_code``,
      ``destination_region_code``, ``carrier_id``).

Source data:
    - ``fct_shipment`` (per-shipment KPIs and the
      ``origin_region_code`` / ``destination_region_code`` /
      ``region_code`` fields already produced by the Silver/Gold
      enrichment pipeline).

Metrics (all supported by the existing source columns):
    - ``shipment_count``
    - ``avg_transit_hours``
    - ``on_time_rate``
    - ``late_rate``
    - ``exception_rate``
    - ``avg_cost_per_mile``

Metrics that are NOT produced here (and why):
    - Lane volume comparison vs prior period: not derivable from a
      single batch — would require a wider time window.
    - Carrier exclusivity (e.g. percent of lane owned by one carrier):
      requires a second pass over the same grain; not produced in
      Phase 6 to keep the model bounded by the documented metrics.
    - SLA / contract attainment: the source data lacks SLA targets.

``origin_region_code`` is produced by the Silver / Gold region
enrichment.  When a shipment is missing the field we default to
``"UNKNOWN"`` so the grain remains consistent and the partition
contract holds.
"""

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


def _safe_col(df: DataFrame, column: str, default: Any) -> Any:
    """Return the column expression if present, else the default literal."""
    if column in df.columns:
        return F.col(column)
    if hasattr(default, "_jc"):
        return default
    return F.lit(default)


def build_route_performance(fct_shipment_df: Any) -> Any:
    """Compute daily route performance metrics.

    Args:
        fct_shipment_df: ``fct_shipment`` Gold fact frame produced by
            :func:`transport_etl.transform.build_fct_shipment`.

    Returns:
        Spark DataFrame at the ``(p_date, origin_region_code,
        destination_region_code, carrier_id)`` grain.
    """
    if not _supports_spark(fct_shipment_df):
        return fct_shipment_df

    df = fct_shipment_df
    df = (
        df.withColumn(
            "p_date", F.coalesce(_safe_col(df, "p_date", F.current_date()), F.current_date())
        )
        .withColumn("carrier_id", F.coalesce(_safe_col(df, "carrier_id", None), F.lit("UNKNOWN")))
        .withColumn(
            "origin_region_code",
            F.coalesce(_safe_col(df, "origin_region_code", None), F.lit("UNKNOWN")),
        )
        .withColumn(
            "destination_region_code",
            F.coalesce(_safe_col(df, "region_code", None), F.lit("UNKNOWN")),
        )
    )

    safe_on_time = F.coalesce(_safe_col(df, "on_time_delivery_flag", 0), F.lit(0))
    safe_exception = F.coalesce(_safe_col(df, "exception_flag", 0), F.lit(0))
    safe_transit = _safe_col(df, "transit_time_hours", None)
    safe_cost = F.coalesce(_safe_col(df, "shipping_cost_usd", 0.0), F.lit(0.0))
    safe_distance = F.coalesce(_safe_col(df, "distance_miles", 0.0), F.lit(0.0))
    safe_delivered = F.coalesce(_safe_col(df, "delivered_flag", 0), F.lit(0))
    safe_delay = F.coalesce(_safe_col(df, "delay_minutes", 0.0), F.lit(0.0))

    aggregate = df.groupBy(
        "p_date",
        "origin_region_code",
        "destination_region_code",
        "carrier_id",
    ).agg(
        F.countDistinct(F.col("shipment_id")).cast("long").alias("shipment_count"),
        F.sum(safe_delivered).cast("long").alias("delivered_shipments"),
        F.sum(safe_on_time).cast("long").alias("on_time_shipments"),
        F.sum(F.when(safe_delay > F.lit(0.0), F.lit(1)).otherwise(F.lit(0)))
        .cast("long")
        .alias("late_shipments"),
        F.sum(safe_exception).cast("long").alias("exception_shipments"),
        F.avg(safe_transit).cast("double").alias("avg_transit_hours"),
        F.sum(safe_cost).cast("double").alias("total_shipping_cost_usd"),
        F.sum(safe_distance).cast("double").alias("total_distance_miles"),
    )

    result = (
        aggregate.withColumn(
            "on_time_rate",
            F.when(
                F.col("delivered_shipments") > F.lit(0),
                F.col("on_time_shipments") / F.col("delivered_shipments"),
            ).otherwise(F.lit(0.0)),
        )
        .withColumn(
            "late_rate",
            F.when(
                F.col("delivered_shipments") > F.lit(0),
                F.col("late_shipments") / F.col("delivered_shipments"),
            ).otherwise(F.lit(0.0)),
        )
        .withColumn(
            "exception_rate",
            F.when(
                F.col("shipment_count") > F.lit(0),
                F.col("exception_shipments") / F.col("shipment_count"),
            ).otherwise(F.lit(0.0)),
        )
        .withColumn(
            "avg_cost_per_mile",
            F.when(
                F.col("total_distance_miles") > F.lit(0),
                F.col("total_shipping_cost_usd") / F.col("total_distance_miles"),
            ).otherwise(F.lit(0.0)),
        )
        .withColumn(
            "avg_transit_hours",
            F.coalesce(F.col("avg_transit_hours"), F.lit(0.0)),
        )
    )

    output_columns = [
        "p_date",
        "origin_region_code",
        "destination_region_code",
        "carrier_id",
        "shipment_count",
        "delivered_shipments",
        "on_time_shipments",
        "late_shipments",
        "exception_shipments",
        "on_time_rate",
        "late_rate",
        "exception_rate",
        "avg_transit_hours",
        "avg_cost_per_mile",
        "total_shipping_cost_usd",
        "total_distance_miles",
    ]
    return result.select(*[col for col in output_columns if col in result.columns])
