"""Reusable standardization transforms for raw/staging datasets."""

from __future__ import annotations

from typing import Any, Mapping

try:
    from pyspark.sql import DataFrame
    from pyspark.sql import functions as F
    from pyspark.sql import types as T
except ModuleNotFoundError:  # pragma: no cover - allows non-Spark unit tests
    DataFrame = Any  # type: ignore[assignment]
    F = None  # type: ignore[assignment]
    T = None  # type: ignore[assignment]


# Canonical operational status/event tokens.
_STATUS_MAP: dict[str, str] = {
    "PICKUP": "PICKED_UP",
    "PICKEDUP": "PICKED_UP",
    "PICKED_UP": "PICKED_UP",
    "INTRANSIT": "IN_TRANSIT",
    "IN_TRANSIT": "IN_TRANSIT",
    "TRANSIT": "IN_TRANSIT",
    "OUTFORDELIVERY": "OUT_FOR_DELIVERY",
    "OUT_FOR_DELIVERY": "OUT_FOR_DELIVERY",
    "DELIVERYATTEMPT": "DELIVERY_ATTEMPT",
    "DELIVERY_ATTEMPT": "DELIVERY_ATTEMPT",
    "ATTEMPTED": "DELIVERY_ATTEMPT",
    "DELIVERED": "DELIVERED",
    "COMPLETE": "DELIVERED",
    "COMPLETED": "DELIVERED",
    "DELAY": "DELAYED",
    "DELAYED": "DELAYED",
    "LATE": "DELAYED",
    "EXCEPTION": "EXCEPTION",
    "FAILED": "EXCEPTION",
    "ERROR": "EXCEPTION",
    "HOLD": "HOLD",
    "ONHOLD": "HOLD",
    "ON_HOLD": "HOLD",
    "CANCELLED": "CANCELLED",
    "CANCELED": "CANCELLED",
}

# Canonical region labels used for partitioning and KPI rollups.
_REGION_MAP: dict[str, str] = {
    "WEST": "WEST",
    "WESTERN": "WEST",
    "SOUTH": "SOUTH",
    "SOUTHERN": "SOUTH",
    "MIDWEST": "MIDWEST",
    "MID_WEST": "MIDWEST",
    "CENTRAL": "MIDWEST",
    "NORTHEAST": "NORTHEAST",
    "NORTH_EAST": "NORTHEAST",
    "NE": "NORTHEAST",
}

_STATE_COLUMNS_DEFAULT = ["state_code", "origin_state", "destination_state", "event_state"]
_REGION_COLUMNS_DEFAULT = [
    "region_code",
    "home_region_code",
    "origin_region_code",
    "destination_region_code",
]


def _require_spark() -> None:
    """Ensure pyspark is available before executing Spark transformations."""
    if F is None or T is None:
        raise ImportError("pyspark is required for standardization transforms")


def _mapping_expr(mapping: Mapping[str, str]) -> Any:
    """Build Spark map expression from a Python dictionary."""
    _require_spark()
    items: list[Any] = []
    for key, value in mapping.items():
        items.extend([F.lit(key), F.lit(value)])
    return F.create_map(*items)


def _normalized_token_expr(column_expr: Any) -> Any:
    """Normalize free-form text into a stable uppercase token."""
    _require_spark()
    return F.regexp_replace(F.upper(F.trim(column_expr.cast("string"))), r"[^A-Z0-9]+", "_")


def clean_text_columns(df: DataFrame, columns: list[str] | None = None) -> DataFrame:
    """Trim, de-noise, and nullify blank values for string columns.

    This is idempotent: running it repeatedly produces the same cleaned output.
    """
    _require_spark()
    target_columns = columns or [
        field.name for field in df.schema.fields if isinstance(field.dataType, T.StringType)
    ]

    result = df
    for column in target_columns:
        if column not in result.columns:
            continue

        # Remove non-printable control chars and collapse internal whitespace.
        cleaned = F.regexp_replace(F.col(column), r"[\x00-\x1F\x7F]", " ")
        cleaned = F.regexp_replace(cleaned, r"\s+", " ")
        cleaned = F.trim(cleaned)
        result = result.withColumn(column, F.when(F.length(cleaned) == 0, F.lit(None)).otherwise(cleaned))

    return result


def normalize_state_code_values(df: DataFrame, columns: list[str] | None = None) -> DataFrame:
    """Normalize state-code columns to USPS-style uppercase 2-letter values."""
    _require_spark()
    state_columns = columns or _STATE_COLUMNS_DEFAULT

    result = df
    for column in state_columns:
        if column not in result.columns:
            continue

        # Business decision: invalid state tokens are set to null rather than forced,
        # to avoid incorrect region assignments during lookup joins.
        normalized = F.regexp_replace(F.upper(F.trim(F.col(column))), r"[^A-Z]", "")
        result = result.withColumn(
            column,
            F.when(F.length(normalized) == 2, normalized).otherwise(F.lit(None)),
        )

    return result


def normalize_region_code_values(df: DataFrame, columns: list[str] | None = None) -> DataFrame:
    """Normalize region columns to canonical region codes."""
    _require_spark()
    region_columns = columns or _REGION_COLUMNS_DEFAULT
    region_map = _mapping_expr(_REGION_MAP)

    result = df
    for column in region_columns:
        if column not in result.columns:
            continue

        token = _normalized_token_expr(F.col(column))
        canonical = F.coalesce(region_map[token], token)
        result = result.withColumn(
            column,
            F.when(F.col(column).isNull(), F.lit(None)).otherwise(canonical),
        )

    return result


def standardize_status_values(df: DataFrame, status_column: str = "status") -> DataFrame:
    """Standardize shipment status-like values to canonical labels."""
    _require_spark()
    if status_column not in df.columns:
        return df

    status_map = _mapping_expr(_STATUS_MAP)
    token = _normalized_token_expr(F.col(status_column))
    canonical = F.coalesce(status_map[token], token)

    return df.withColumn(
        status_column,
        F.when(F.col(status_column).isNull(), F.lit(None)).otherwise(canonical),
    )


def standardize_event_type_values(df: DataFrame, event_type_column: str = "event_type") -> DataFrame:
    """Standardize event-type values using the status canonicalization map."""
    return standardize_status_values(df=df, status_column=event_type_column)


def standardize_carrier_names(df: DataFrame, carrier_name_column: str = "carrier_name") -> DataFrame:
    """Normalize carrier-name text formatting.

    Carrier names keep original business casing by default; this only enforces
    whitespace cleanliness to avoid accidental value splitting.
    """
    _require_spark()
    if carrier_name_column not in df.columns:
        return df

    cleaned = F.regexp_replace(F.trim(F.col(carrier_name_column)), r"\s+", " ")
    return df.withColumn(
        carrier_name_column,
        F.when(F.length(cleaned) == 0, F.lit(None)).otherwise(cleaned),
    )


def standardize_columns(df: Any) -> Any:
    """Apply idempotent baseline standardization for transport datasets.

    Separation of concerns:
    - This function only standardizes values and text quality.
    - Region lookup joins are handled in `transform/enrich_region.py`.
    """
    _require_spark()
    result = clean_text_columns(df)
    result = normalize_state_code_values(result)
    result = normalize_region_code_values(result)
    result = standardize_status_values(result, status_column="status")
    result = standardize_event_type_values(result, event_type_column="event_type")
    result = standardize_carrier_names(result)
    return result
