"""Region enrichment helpers for shipment and event datasets."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Mapping

try:
    from pyspark.sql import DataFrame, SparkSession
    from pyspark.sql import functions as F
    from pyspark.sql import types as T
    from pyspark.sql.window import Window
except ModuleNotFoundError:  # pragma: no cover - allows non-Spark unit tests
    DataFrame = Any  # type: ignore[assignment]
    SparkSession = Any  # type: ignore[assignment]
    F = None  # type: ignore[assignment]
    T = None  # type: ignore[assignment]
    Window = None  # type: ignore[assignment]

from transport_etl.common.io import read_csv
from transport_etl.transform.standardize import normalize_region_code_values, normalize_state_code_values


LOGGER = logging.getLogger(__name__)
DEFAULT_REGION_LOOKUP_PATH = str(
    Path(__file__).resolve().parents[3] / "data" / "sample" / "reference" / "region_lookup.csv"
)


def _require_spark() -> None:
    """Ensure pyspark is available before executing Spark transformations."""
    if F is None or T is None or Window is None:
        raise ImportError("pyspark is required for region enrichment transforms")


def load_region_lookup(
    spark: SparkSession,
    lookup_path: str = DEFAULT_REGION_LOOKUP_PATH,
    read_options: Mapping[str, Any] | None = None,
) -> DataFrame:
    """Load and normalize region lookup from CSV."""
    _require_spark()
    schema = T.StructType(
        [
            T.StructField("state_code", T.StringType(), False),
            T.StructField("region_code", T.StringType(), False),
        ]
    )

    options = {"header": "true", "mode": "PERMISSIVE"}
    if read_options:
        options.update({str(k): str(v) for k, v in read_options.items()})

    raw_lookup = read_csv(spark=spark, path=lookup_path, schema=schema, options=options)
    return prepare_region_lookup(raw_lookup)


def prepare_region_lookup(region_lookup_df: DataFrame) -> DataFrame:
    """Normalize and deduplicate state-to-region mapping records."""
    _require_spark()

    normalized = normalize_state_code_values(region_lookup_df, columns=["state_code"])
    normalized = normalize_region_code_values(normalized, columns=["region_code"])

    # Business decision: if duplicate state mappings exist, keep one stable mapping
    # by selecting alphabetically smallest canonical region.
    window = Window.partitionBy("state_code").orderBy(F.col("region_code").asc_nulls_last())
    deduped = (
        normalized.filter(F.col("state_code").isNotNull() & F.col("region_code").isNotNull())
        .withColumn("__rn", F.row_number().over(window))
        .filter(F.col("__rn") == 1)
        .drop("__rn")
    )

    return deduped


def enrich_with_region(
    df: DataFrame,
    region_lookup_df: DataFrame,
    state_column: str = "destination_state",
    output_column: str = "region_code",
) -> DataFrame:
    """Attach region code to records based on a state column.

    This transformation is idempotent: re-running it yields identical output.
    """
    _require_spark()

    if state_column not in df.columns:
        LOGGER.warning("State column '%s' not found. Region enrichment skipped.", state_column)
        return df

    lookup = prepare_region_lookup(region_lookup_df).select(
        F.col("state_code").alias("__lookup_state"),
        F.col("region_code").alias("__lookup_region"),
    )

    working = normalize_state_code_values(df, columns=[state_column])
    working = working.withColumn("__join_state", F.col(state_column))

    joined = working.join(
        lookup,
        on=working["__join_state"] == lookup["__lookup_state"],
        how="left",
    )

    if output_column in df.columns:
        region_expr = F.coalesce(F.col("__lookup_region"), F.col(output_column))
    else:
        region_expr = F.col("__lookup_region")

    enriched = joined.withColumn(output_column, region_expr)
    enriched = normalize_region_code_values(enriched, columns=[output_column])

    return enriched.drop("__join_state", "__lookup_state", "__lookup_region")


def enrich_shipments_with_region(
    shipments_df: DataFrame,
    region_lookup_df: DataFrame,
    destination_state_column: str = "destination_state",
    output_region_column: str = "region_code",
    include_origin_region: bool = True,
    origin_state_column: str = "origin_state",
    origin_region_column: str = "origin_region_code",
) -> DataFrame:
    """Enrich shipments with region information from state codes.

    Business decision: destination region is the default primary shipment region
    because downstream KPIs and partitions are destination-centric.
    """
    enriched = enrich_with_region(
        df=shipments_df,
        region_lookup_df=region_lookup_df,
        state_column=destination_state_column,
        output_column=output_region_column,
    )

    if include_origin_region and origin_state_column in enriched.columns:
        enriched = enrich_with_region(
            df=enriched,
            region_lookup_df=region_lookup_df,
            state_column=origin_state_column,
            output_column=origin_region_column,
        )

    return enriched


def enrich_delivery_events_with_region(
    events_df: DataFrame,
    region_lookup_df: DataFrame,
    event_state_column: str = "event_state",
    output_region_column: str = "region_code",
) -> DataFrame:
    """Enrich delivery events with region information."""
    return enrich_with_region(
        df=events_df,
        region_lookup_df=region_lookup_df,
        state_column=event_state_column,
        output_column=output_region_column,
    )
