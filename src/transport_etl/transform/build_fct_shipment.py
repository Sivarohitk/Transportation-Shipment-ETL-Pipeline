"""Build curated fct_shipment table."""

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

from transport_etl.transform.enrich_region import enrich_shipments_with_region
from transport_etl.transform.standardize import standardize_columns


def _supports_spark(df: Any) -> bool:
    """Return whether Spark DataFrame operations are available."""
    return F is not None and Window is not None and hasattr(df, "columns")


def _deduplicate_shipments(df: DataFrame) -> DataFrame:
    """Keep latest shipment row per `shipment_id` based on `updated_at`."""
    window = Window.partitionBy("shipment_id").orderBy(
        F.col("updated_at").desc_nulls_last(),
        F.col("pickup_ts").desc_nulls_last(),
    )
    return (
        df.withColumn("__rn", F.row_number().over(window))
        .filter(F.col("__rn") == 1)
        .drop("__rn")
    )


def _delay_minutes_expr(actual_col: str, promised_col: str) -> Any:
    """Compute positive delay minutes from actual vs promised timestamps."""
    diff_minutes = (F.unix_timestamp(F.col(actual_col)) - F.unix_timestamp(F.col(promised_col))) / F.lit(60.0)
    return F.when(
        F.col(actual_col).isNotNull() & F.col(promised_col).isNotNull(),
        F.greatest(diff_minutes, F.lit(0.0)),
    ).otherwise(F.lit(None).cast("double"))


def _transit_time_hours_expr(actual_col: str, pickup_col: str) -> Any:
    """Compute transit duration hours from pickup to delivery."""
    diff_hours = (F.unix_timestamp(F.col(actual_col)) - F.unix_timestamp(F.col(pickup_col))) / F.lit(3600.0)
    return F.when(
        F.col(actual_col).isNotNull() & F.col(pickup_col).isNotNull(),
        diff_hours,
    ).otherwise(F.lit(None).cast("double"))


def build_fct_shipment(stg_shipments_df: Any, region_lookup_df: Any) -> Any:
    """Create curated `fct_shipment` fact table.

    Grain:
    - One row per `shipment_id` representing latest shipment state.

    Business columns for KPI:
    - `on_time_delivery_flag`
    - `delay_minutes`
    - `exception_flag`
    - `transit_time_hours`
    """
    if not _supports_spark(stg_shipments_df):
        # Keeps non-Spark unit tests stable in lightweight environments.
        return stg_shipments_df

    standardized = standardize_columns(stg_shipments_df)

    if "shipment_id" in standardized.columns:
        standardized = standardized.withColumn("shipment_id", F.upper(F.trim(F.col("shipment_id"))))
    if "carrier_id" in standardized.columns:
        standardized = standardized.withColumn("carrier_id", F.upper(F.trim(F.col("carrier_id"))))

    deduped = _deduplicate_shipments(standardized)

    enriched = deduped
    if _supports_spark(region_lookup_df):
        enriched = enrich_shipments_with_region(
            shipments_df=deduped,
            region_lookup_df=region_lookup_df,
            destination_state_column="destination_state",
            output_region_column="region_code",
            include_origin_region=True,
            origin_state_column="origin_state",
            origin_region_column="origin_region_code",
        )

    result = (
        enriched.withColumn("delivered_flag", F.when(F.col("actual_delivery_ts").isNotNull(), F.lit(1)).otherwise(F.lit(0)))
        .withColumn(
            "on_time_delivery_flag",
            F.when(
                F.col("actual_delivery_ts").isNotNull()
                & F.col("promised_delivery_ts").isNotNull()
                & (F.col("actual_delivery_ts") <= F.col("promised_delivery_ts")),
                F.lit(1),
            ).otherwise(F.lit(0)),
        )
        .withColumn("delay_minutes", _delay_minutes_expr("actual_delivery_ts", "promised_delivery_ts"))
        .withColumn(
            "exception_flag",
            F.when(F.col("destination_state").isNull(), F.lit(1))
            .when(
                F.col("actual_delivery_ts").isNull()
                & F.col("promised_delivery_ts").isNotNull()
                & F.col("updated_at").isNotNull()
                & (F.col("updated_at") > F.col("promised_delivery_ts")),
                F.lit(1),
            )
            .otherwise(F.lit(0)),
        )
        .withColumn("transit_time_hours", _transit_time_hours_expr("actual_delivery_ts", "pickup_ts"))
        .withColumn(
            "p_date",
            F.coalesce(
                F.to_date(F.col("pickup_ts")),
                F.to_date(F.col("promised_delivery_ts")),
                F.to_date(F.col("updated_at")),
                F.current_date(),
            ),
        )
        .withColumn("region_code", F.coalesce(F.col("region_code"), F.lit("UNKNOWN")))
        .withColumn("carrier_id", F.coalesce(F.col("carrier_id"), F.lit("UNKNOWN")))
    )

    select_columns = [
        "shipment_id",
        "carrier_id",
        "origin_state",
        "destination_state",
        "origin_region_code",
        "region_code",
        "pickup_ts",
        "promised_delivery_ts",
        "actual_delivery_ts",
        "updated_at",
        "shipping_cost_usd",
        "distance_miles",
        "delivered_flag",
        "on_time_delivery_flag",
        "delay_minutes",
        "exception_flag",
        "transit_time_hours",
        "p_date",
    ]
    available_columns = [col for col in select_columns if col in result.columns]

    return result.select(*available_columns)
