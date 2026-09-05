"""Build curated ``delivery_exception_summary`` Gold analytics table.

Grain:
    - One row per (``p_date``, ``event_type``, ``carrier_id``,
      ``region_code``).

Source data:
    - ``fct_delivery_event`` (per-event KPIs and the
      ``event_type`` / ``region_code`` / ``carrier_id`` /
      ``delay_reason`` fields already produced by the Gold event
      builder).
    - ``fct_shipment`` (used to compute the shipment-level
      denominator — total shipments per ``(p_date, carrier_id,
      region_code)`` — so the rate metric is interpretable).

Metrics (all supported by the existing source columns):
    - ``event_count`` — number of delivery events of this type
    - ``shipment_count`` — distinct shipments touched by events of
      this type
    - ``rate`` — events per shipment (a measure of how often this
      event type fires per affected shipment)
    - ``avg_delay_minutes`` — average event-level delay vs the
      shipment's promised delivery timestamp.  Zero for events that
      have no associated delay.
    - ``exception_shipment_count`` — distinct shipments that have an
      ``exception_flag = 1`` within the grain (delay / exception /
      hold events as classified in ``fct_delivery_event``)

Metrics that are NOT produced here (and why):
    - Cost of exception (financial impact): requires
      shipment-level cost data joined on the event, which is
      derivable but not part of the Phase-6 contract to keep the
      table focused on operational event diagnostics.
    - Time-to-recovery (mean minutes between the exception event
      and the next DELIVERED event for the same shipment): requires
      a window join that is documented as a Phase-7 / future
      enhancement to avoid hiding that complexity in Phase 6.
"""

from __future__ import annotations

from typing import Any

try:
    from pyspark.sql import DataFrame
    from pyspark.sql import functions as F
except ModuleNotFoundError:  # pragma: no cover
    DataFrame = Any  # type: ignore[assignment]
    F = None  # type: ignore[assignment]

from transport_etl.common.constants import (
    DELIVERY_EXCEPTION_EVENT_TYPES,
)


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


def _shipment_denominator(fct_shipment_df: DataFrame) -> Any:
    """Total shipments per (p_date, carrier_id, region_code).

    Used as the denominator for the exception rate so the metric is
    comparable across grains.  When the shipment fact is missing or
    lacks the required columns the function returns ``None`` and the
    caller substitutes zero.
    """
    if fct_shipment_df is None or not _supports_spark(fct_shipment_df):
        return None

    required = {"p_date", "carrier_id", "shipment_id", "region_code"}
    if not required.issubset(set(fct_shipment_df.columns)):
        return None

    return fct_shipment_df.groupBy("p_date", "carrier_id", "region_code").agg(
        F.countDistinct(F.col("shipment_id")).cast("long").alias("total_shipments")
    )


def build_delivery_exception_summary(
    fct_delivery_event_df: Any,
    fct_shipment_df: Any | None = None,
) -> Any:
    """Compute daily delivery exception / event-type summary metrics.

    Args:
        fct_delivery_event_df: ``fct_delivery_event`` Gold fact frame.
        fct_shipment_df: ``fct_shipment`` Gold fact frame (optional,
            used to compute the per-grain shipment denominator for
            the rate metric).

    Returns:
        Spark DataFrame at the ``(p_date, event_type, carrier_id,
        region_code)`` grain.
    """
    if not _supports_spark(fct_delivery_event_df):
        return fct_delivery_event_df

    df = fct_delivery_event_df
    df = (
        df.withColumn(
            "p_date", F.coalesce(_safe_col(df, "p_date", F.current_date()), F.current_date())
        )
        .withColumn("carrier_id", F.coalesce(_safe_col(df, "carrier_id", None), F.lit("UNKNOWN")))
        .withColumn("region_code", F.coalesce(_safe_col(df, "region_code", None), F.lit("UNKNOWN")))
        .withColumn("event_type", F.coalesce(_safe_col(df, "event_type", None), F.lit("UNKNOWN")))
    )

    safe_exception_flag = F.coalesce(_safe_col(df, "exception_flag", 0), F.lit(0))
    safe_delay_minutes = F.coalesce(_safe_col(df, "delay_minutes", 0.0), F.lit(0.0))

    aggregate = df.groupBy("p_date", "event_type", "carrier_id", "region_code").agg(
        F.count(F.lit(1)).cast("long").alias("event_count"),
        F.countDistinct(F.col("shipment_id")).cast("long").alias("shipment_count"),
        F.sum(safe_exception_flag).cast("long").alias("exception_event_count"),
        F.countDistinct(F.when(safe_exception_flag == F.lit(1), F.col("shipment_id")))
        .cast("long")
        .alias("exception_shipment_count"),
        F.avg(safe_delay_minutes).cast("double").alias("avg_delay_minutes"),
    )

    # Compute total_shipments denominator per (p_date, carrier_id, region_code)
    # so the rate metric is the per-grain ratio of events to shipments.
    shipment_denominator = (
        _shipment_denominator(fct_shipment_df) if fct_shipment_df is not None else None
    )
    if shipment_denominator is not None:
        aggregate = aggregate.join(
            shipment_denominator,
            on=["p_date", "carrier_id", "region_code"],
            how="left",
        )
        aggregate = aggregate.withColumn(
            "total_shipments",
            F.coalesce(F.col("total_shipments"), F.lit(0)),
        )
        aggregate = aggregate.withColumn(
            "rate",
            F.when(
                F.col("total_shipments") > F.lit(0),
                F.col("event_count") / F.col("total_shipments"),
            ).otherwise(F.lit(0.0)),
        )
    else:
        # Without a shipment denominator we can still report
        # ``event_count`` / ``shipment_count`` and fall back to the
        # ratio of events to distinct shipments as a derived rate.
        aggregate = aggregate.withColumn("total_shipments", F.lit(0).cast("long"))
        aggregate = aggregate.withColumn(
            "rate",
            F.when(
                F.col("shipment_count") > F.lit(0),
                F.col("event_count") / F.col("shipment_count"),
            ).otherwise(F.lit(0.0)),
        )

    aggregate = aggregate.withColumn(
        "avg_delay_minutes",
        F.coalesce(F.col("avg_delay_minutes"), F.lit(0.0)),
    )
    aggregate = aggregate.withColumn(
        "is_exception_event_type",
        F.col("event_type").isin(list(DELIVERY_EXCEPTION_EVENT_TYPES)),
    )

    output_columns = [
        "p_date",
        "event_type",
        "carrier_id",
        "region_code",
        "event_count",
        "shipment_count",
        "exception_event_count",
        "exception_shipment_count",
        "rate",
        "avg_delay_minutes",
        "is_exception_event_type",
        "total_shipments",
    ]
    return aggregate.select(*[col for col in output_columns if col in aggregate.columns])
