"""Build curated fct_delivery_event table."""

from __future__ import annotations

from typing import Any

try:
    from pyspark.sql import DataFrame
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window
except ModuleNotFoundError:  # pragma: no cover
    DataFrame = Any  # type: ignore[assignment]
    F = None  # type: ignore[assignment]
    Window = None  # type: ignore[assignment]

from transport_etl.transform.enrich_region import enrich_delivery_events_with_region, enrich_shipments_with_region
from transport_etl.transform.standardize import standardize_columns


def _supports_spark(df: Any) -> bool:
    """Return whether Spark DataFrame operations are available."""
    return F is not None and Window is not None and hasattr(df, "columns")


def _deduplicate_by_key(df: DataFrame, key_column: str) -> DataFrame:
    """Keep latest row per key using `updated_at` ordering."""
    if key_column not in df.columns:
        return df

    order_exprs = [F.col("updated_at").desc_nulls_last()]
    if "event_ts" in df.columns:
        order_exprs.append(F.col("event_ts").desc_nulls_last())

    window = Window.partitionBy(key_column).orderBy(*order_exprs)
    return (
        df.withColumn("__rn", F.row_number().over(window))
        .filter(F.col("__rn") == 1)
        .drop("__rn")
    )


def _delay_minutes_expr(event_ts_col: str, promised_col: str) -> Any:
    """Compute positive delay minutes for event timestamp vs promised timestamp."""
    diff_minutes = (F.unix_timestamp(F.col(event_ts_col)) - F.unix_timestamp(F.col(promised_col))) / F.lit(60.0)
    return F.when(
        F.col(event_ts_col).isNotNull() & F.col(promised_col).isNotNull(),
        F.greatest(diff_minutes, F.lit(0.0)),
    ).otherwise(F.lit(None).cast("double"))


def _transit_time_hours_expr(event_ts_col: str, pickup_col: str) -> Any:
    """Compute transit duration hours from pickup to event timestamp."""
    diff_hours = (F.unix_timestamp(F.col(event_ts_col)) - F.unix_timestamp(F.col(pickup_col))) / F.lit(3600.0)
    return F.when(
        F.col(event_ts_col).isNotNull() & F.col(pickup_col).isNotNull(),
        diff_hours,
    ).otherwise(F.lit(None).cast("double"))


def build_fct_delivery_event(
    stg_events_df: Any,
    stg_shipments_df: Any,
    region_lookup_df: Any | None = None,
) -> Any:
    """Create curated `fct_delivery_event` fact table.

    Grain:
    - One row per `event_id` representing latest event update.

    Business columns for KPI:
    - `on_time_delivery_flag` (for delivered events)
    - `delay_minutes`
    - `exception_flag`
    - `transit_time_hours`
    """
    if not _supports_spark(stg_events_df):
        # Keeps non-Spark unit tests stable in lightweight environments.
        return stg_events_df

    events = standardize_columns(stg_events_df)
    if "event_id" in events.columns:
        events = events.withColumn("event_id", F.upper(F.trim(F.col("event_id"))))
    if "shipment_id" in events.columns:
        events = events.withColumn("shipment_id", F.upper(F.trim(F.col("shipment_id"))))

    events = _deduplicate_by_key(events, "event_id")

    if _supports_spark(region_lookup_df):
        events = enrich_delivery_events_with_region(
            events_df=events,
            region_lookup_df=region_lookup_df,
            event_state_column="event_state",
            output_region_column="event_region_code",
        )

    shipment_projection = None
    if _supports_spark(stg_shipments_df):
        shipments = standardize_columns(stg_shipments_df)
        if "shipment_id" in shipments.columns:
            shipments = shipments.withColumn("shipment_id", F.upper(F.trim(F.col("shipment_id"))))
        if "carrier_id" in shipments.columns:
            shipments = shipments.withColumn("carrier_id", F.upper(F.trim(F.col("carrier_id"))))

        shipments = _deduplicate_by_key(shipments, "shipment_id")

        if _supports_spark(region_lookup_df):
            shipments = enrich_shipments_with_region(
                shipments_df=shipments,
                region_lookup_df=region_lookup_df,
                destination_state_column="destination_state",
                output_region_column="shipment_region_code",
                include_origin_region=False,
            )

        projection_exprs = [
            F.col(column)
            for column in [
                "shipment_id",
                "carrier_id",
                "pickup_ts",
                "promised_delivery_ts",
                "actual_delivery_ts",
                "destination_state",
                "shipment_region_code",
            ]
            if column in shipments.columns
        ]
        if "region_code" in shipments.columns:
            # Avoid duplicate `region_code` after join with events.
            projection_exprs.append(F.col("region_code").alias("shipment_base_region_code"))

        shipment_projection = shipments.select(*projection_exprs)

    joined = events
    if shipment_projection is not None and "shipment_id" in joined.columns:
        joined = joined.join(shipment_projection, on="shipment_id", how="left")

    region_candidates = [
        col
        for col in ["event_region_code", "shipment_region_code", "shipment_base_region_code", "region_code"]
        if col in joined.columns
    ]
    region_expr = F.coalesce(*[F.col(col) for col in region_candidates], F.lit("UNKNOWN"))

    result = (
        joined.withColumn("carrier_id", F.coalesce(F.col("carrier_id"), F.lit("UNKNOWN")))
        .withColumn("region_code", region_expr)
        .withColumn(
            "p_date",
            F.coalesce(F.to_date(F.col("event_ts")), F.to_date(F.col("updated_at")), F.current_date()),
        )
        .withColumn(
            "on_time_delivery_flag",
            F.when(
                (F.col("event_type") == F.lit("DELIVERED"))
                & F.col("event_ts").isNotNull()
                & F.col("promised_delivery_ts").isNotNull()
                & (F.col("event_ts") <= F.col("promised_delivery_ts")),
                F.lit(1),
            ).otherwise(F.lit(0)),
        )
        .withColumn("delay_minutes", _delay_minutes_expr("event_ts", "promised_delivery_ts"))
        .withColumn(
            "exception_flag",
            F.when(F.col("event_type").isin("EXCEPTION", "HOLD"), F.lit(1)).otherwise(F.lit(0)),
        )
        .withColumn(
            "transit_time_hours",
            F.when(
                F.col("event_type") == F.lit("DELIVERED"),
                _transit_time_hours_expr("event_ts", "pickup_ts"),
            ).otherwise(F.lit(None).cast("double")),
        )
    )

    drop_candidates = ["event_region_code", "shipment_region_code", "shipment_base_region_code"]
    result = result.drop(*[col for col in drop_candidates if col in result.columns])

    select_columns = [
        "event_id",
        "shipment_id",
        "carrier_id",
        "event_type",
        "event_ts",
        "event_city",
        "event_state",
        "delay_reason",
        "attempt_number",
        "updated_at",
        "region_code",
        "p_date",
        "on_time_delivery_flag",
        "delay_minutes",
        "exception_flag",
        "transit_time_hours",
    ]
    available_columns = [col for col in select_columns if col in result.columns]

    return result.select(*available_columns)
