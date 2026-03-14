"""Build curated dim_carrier table."""

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

from transport_etl.transform.standardize import normalize_region_code_values, standardize_carrier_names


def _supports_spark(df: Any) -> bool:
    """Return whether Spark DataFrame operations are available."""
    return F is not None and Window is not None and hasattr(df, "columns")


def _deduplicate_carriers(df: DataFrame) -> DataFrame:
    """Keep the latest carrier row per `carrier_id` using `updated_at` ordering."""
    window = Window.partitionBy("carrier_id").orderBy(
        F.col("updated_at").desc_nulls_last(),
        F.col("carrier_name").asc_nulls_last(),
    )
    return (
        df.withColumn("__rn", F.row_number().over(window))
        .filter(F.col("__rn") == 1)
        .drop("__rn")
    )


def _resolve_snapshot_date(run_date: str | None) -> Any:
    """Resolve snapshot date expression used for `p_date` partition."""
    if run_date:
        return F.to_date(F.lit(run_date))
    return F.current_date()


def build_dim_carrier(stg_carriers_df: Any, run_date: str | None) -> Any:
    """Create curated `dim_carrier` snapshot.

    Grain:
    - One row per `carrier_id` per `p_date` snapshot.

    Business decisions:
    - Keep latest source update per carrier for the snapshot.
    - Default unknown partition values for region/carrier when null.
    """
    if not _supports_spark(stg_carriers_df):
        # Keeps non-Spark unit tests stable in lightweight environments.
        return stg_carriers_df

    base = standardize_carrier_names(stg_carriers_df, carrier_name_column="carrier_name")
    region_columns = [column for column in ["home_region_code", "region_code"] if column in base.columns]
    if region_columns:
        base = normalize_region_code_values(base, columns=region_columns)

    if "carrier_id" in base.columns:
        base = base.withColumn("carrier_id", F.upper(F.trim(F.col("carrier_id"))))

    deduped = _deduplicate_carriers(base)

    snapshot = deduped.withColumn("p_date", _resolve_snapshot_date(run_date))
    region_sources = []
    if "home_region_code" in snapshot.columns:
        region_sources.append(F.col("home_region_code"))
    if "region_code" in snapshot.columns:
        region_sources.append(F.col("region_code"))

    # Carriers staging uses `home_region_code`; publish a curated `region_code`
    # and default to UNKNOWN when no region value is available.
    region_sources.append(F.lit("UNKNOWN"))
    snapshot = snapshot.withColumn(
        "region_code",
        F.coalesce(*region_sources),
    )
    snapshot = snapshot.withColumn("carrier_id", F.coalesce(F.col("carrier_id"), F.lit("UNKNOWN")))

    select_columns = [
        "carrier_id",
        "carrier_name",
        "scac",
        "service_mode",
        "region_code",
        "is_active",
        "updated_at",
        "p_date",
    ]

    available_columns = [col for col in select_columns if col in snapshot.columns]
    return snapshot.select(*available_columns)
