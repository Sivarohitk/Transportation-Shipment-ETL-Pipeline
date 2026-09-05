"""Build curated ``carrier_performance`` Gold analytics table.

Grain:
    - One row per (``p_date``, ``carrier_id``, ``service_mode``).

Source data:
    - ``fct_shipment`` (per-shipment KPIs: on-time flag, transit hours,
      exception flag, cost, distance, delivery flag)
    - ``fct_delivery_event`` (per-event first-attempt success count)
    - ``dim_carrier`` (service_mode is only available here)

Metrics (all supported by the existing source columns):
    - ``shipment_volume``
    - ``delivered_shipments``
    - ``on_time_delivery_rate``
    - ``late_delivery_rate``
    - ``avg_transit_hours``
    - ``exception_rate``
    - ``first_attempt_success_rate``
    - ``avg_cost_per_mile``
    - ``total_shipping_cost_usd``
    - ``total_distance_miles``

Metrics that are NOT produced here (and why):
    - Revenue / margin: not derivable from the source data.
    - Customer satisfaction score: not present in the source data.
    - Carbon / fuel metrics: not present in the source data.

The carrier service_mode comes from ``dim_carrier``.  When the
dimension is unavailable the builder falls back to a literal
``"UNKNOWN"`` so the resulting table is still grain-consistent.
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


def _first_attempt_metrics(fct_delivery_event_df: DataFrame) -> Any:
    """Aggregate event-level first-attempt success counts by grain.

    Returns ``None`` when the input frame is missing the columns we
    need to compute the metric; in that case the caller should
    substitute zero values.
    """
    if fct_delivery_event_df is None or not _supports_spark(fct_delivery_event_df):
        return None

    required = {"p_date", "carrier_id", "event_type", "shipment_id"}
    if not required.issubset(set(fct_delivery_event_df.columns)):
        return None

    aggregate = fct_delivery_event_df.groupBy("p_date", "carrier_id").agg(
        F.countDistinct(
            F.when(
                (F.col("event_type") == F.lit("DELIVERED")) & (F.col("attempt_number") == F.lit(1)),
                F.col("shipment_id"),
            )
        )
        .cast("long")
        .alias("first_attempt_success_shipments"),
    )
    return aggregate


def build_carrier_performance(
    fct_shipment_df: Any,
    fct_delivery_event_df: Any | None = None,
    dim_carrier_df: Any | None = None,
) -> Any:
    """Compute daily carrier-level performance metrics.

    Args:
        fct_shipment_df: ``fct_shipment`` Gold fact frame produced by
            :func:`transport_etl.transform.build_fct_shipment`.
        fct_delivery_event_df: ``fct_delivery_event`` Gold fact frame
            produced by
            :func:`transport_etl.transform.build_fct_delivery_event`.
            Optional; when omitted first-attempt metrics default to zero.
        dim_carrier_df: ``dim_carrier`` Gold dimension frame produced by
            :func:`transport_etl.transform.build_dim_carrier`.
            Optional; when omitted the ``service_mode`` column defaults
            to ``"UNKNOWN"`` so the grain remains ``(p_date, carrier_id,
            service_mode)``.

    Returns:
        Spark DataFrame at ``(p_date, carrier_id, service_mode)`` grain.
    """
    if not _supports_spark(fct_shipment_df):
        return fct_shipment_df

    # Attach ``service_mode`` (from ``dim_carrier``) before aggregation
    # so the grain is meaningful.  The left-join keeps every shipment
    # even if the dimension is missing for a particular carrier_id on
    # a particular p_date.
    df = fct_shipment_df
    if dim_carrier_df is not None and "carrier_id" in dim_carrier_df.columns:
        carrier_cols = [
            col for col in ("carrier_id", "service_mode", "p_date") if col in dim_carrier_df.columns
        ]
        if "service_mode" in carrier_cols and "carrier_id" in carrier_cols:
            deduped_dim = dim_carrier_df.select(*carrier_cols).dropDuplicates(
                [c for c in ("carrier_id", "p_date") if c in carrier_cols]
            )
            df = df.join(deduped_dim, on=["carrier_id", "p_date"], how="left")

    if "service_mode" not in df.columns:
        df = df.withColumn("service_mode", F.lit("UNKNOWN"))
    else:
        df = df.withColumn("service_mode", F.coalesce(F.col("service_mode"), F.lit("UNKNOWN")))

    # Stable partition contract: p_date / carrier_id / service_mode.
    df = df.withColumn(
        "p_date", F.coalesce(_safe_col(df, "p_date", F.current_date()), F.current_date())
    ).withColumn("carrier_id", F.coalesce(_safe_col(df, "carrier_id", None), F.lit("UNKNOWN")))

    safe_on_time = F.coalesce(_safe_col(df, "on_time_delivery_flag", 0), F.lit(0))
    safe_exception = F.coalesce(_safe_col(df, "exception_flag", 0), F.lit(0))
    safe_transit = _safe_col(df, "transit_time_hours", None)
    safe_cost = F.coalesce(_safe_col(df, "shipping_cost_usd", 0.0), F.lit(0.0))
    safe_distance = F.coalesce(_safe_col(df, "distance_miles", 0.0), F.lit(0.0))
    safe_delivered = F.coalesce(_safe_col(df, "delivered_flag", 0), F.lit(0))
    safe_delay = F.coalesce(_safe_col(df, "delay_minutes", 0.0), F.lit(0.0))

    aggregate = df.groupBy("p_date", "carrier_id", "service_mode").agg(
        F.countDistinct(F.col("shipment_id")).cast("long").alias("shipment_volume"),
        F.sum(safe_delivered).cast("long").alias("delivered_shipments"),
        F.sum(safe_on_time).cast("long").alias("on_time_shipments"),
        F.sum(F.when(safe_delay > F.lit(0.0), F.lit(1)).otherwise(F.lit(0)))
        .cast("long")
        .alias("late_shipments"),
        F.sum(safe_exception).cast("long").alias("exception_shipments"),
        F.sum(safe_delay).cast("double").alias("total_delay_minutes"),
        F.avg(safe_transit).cast("double").alias("avg_transit_hours"),
        F.sum(safe_cost).cast("double").alias("total_shipping_cost_usd"),
        F.sum(safe_distance).cast("double").alias("total_distance_miles"),
    )

    # First-attempt success comes from the event fact; left-join on
    # the smaller grain.
    event_metrics = (
        _first_attempt_metrics(fct_delivery_event_df) if fct_delivery_event_df is not None else None
    )
    if event_metrics is not None:
        aggregate = aggregate.join(event_metrics, on=["p_date", "carrier_id"], how="left")
    else:
        aggregate = aggregate.withColumn("first_attempt_success_shipments", F.lit(0).cast("long"))

    aggregate = aggregate.withColumn(
        "first_attempt_success_shipments",
        F.coalesce(F.col("first_attempt_success_shipments"), F.lit(0)),
    )

    # Denominator convention matches the existing kpi_delivery_daily
    # build: use delivered_shipments so the rates are interpretable
    # in the same way as the daily KPI model.
    delivered_denominator = F.col("delivered_shipments")

    result = (
        aggregate.withColumn(
            "on_time_delivery_rate",
            F.when(
                delivered_denominator > F.lit(0),
                F.col("on_time_shipments") / delivered_denominator,
            ).otherwise(F.lit(0.0)),
        )
        .withColumn(
            "late_delivery_rate",
            F.when(
                delivered_denominator > F.lit(0),
                F.col("late_shipments") / delivered_denominator,
            ).otherwise(F.lit(0.0)),
        )
        .withColumn(
            "exception_rate",
            F.when(
                F.col("shipment_volume") > F.lit(0),
                F.col("exception_shipments") / F.col("shipment_volume"),
            ).otherwise(F.lit(0.0)),
        )
        .withColumn(
            "first_attempt_success_rate",
            F.when(
                delivered_denominator > F.lit(0),
                F.col("first_attempt_success_shipments") / delivered_denominator,
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
        .withColumn(
            "total_delay_minutes",
            F.coalesce(F.col("total_delay_minutes"), F.lit(0.0)),
        )
    )

    output_columns = [
        "p_date",
        "carrier_id",
        "service_mode",
        "shipment_volume",
        "delivered_shipments",
        "on_time_shipments",
        "late_shipments",
        "exception_shipments",
        "first_attempt_success_shipments",
        "on_time_delivery_rate",
        "late_delivery_rate",
        "exception_rate",
        "first_attempt_success_rate",
        "avg_transit_hours",
        "total_delay_minutes",
        "avg_cost_per_mile",
        "total_shipping_cost_usd",
        "total_distance_miles",
    ]
    return result.select(*[col for col in output_columns if col in result.columns])
