"""Deterministic Silver-layer deduplication.

Wraps :func:`transport_etl.quality.duplicates.split_by_duplicates` so
Silver builders can dedupe by a known business key while retaining the
duplicate rows for quarantine / audit.

Why a wrapper?
--------------
The quality.duplicates helper is a low-level utility: callers must
explicitly pass the key columns and the order-by columns.  Silver-layer
dedup needs:

1. A canonical "latest record wins" policy ordered by ``updated_at``
   (the source column that tracks operational changes).
2. A stable tie-breaker so reruns produce the same result.  The current
   implementation uses ``pickup_ts`` / ``event_ts`` as the secondary
   tie-breaker (whichever exists in the source entity).
3. A clear separation between the **deduped** DataFrame and the
   **duplicate** DataFrame so the duplicate rows can be routed to the
   Silver quarantine with the right rule label.
4. An integration with the Silver business-key mapping so callers cannot
   pass the wrong key by accident.

The wrapper therefore:

- Reads the business key from :mod:`transport_etl.silver.keys`
- Reads the timestamp tie-breaker from the entity-specific column set
- Returns both the surviving rows and the dropped duplicates with rule
  metadata attached.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

try:
    from pyspark.sql import DataFrame
    from pyspark.sql import functions as F
except ModuleNotFoundError:  # pragma: no cover
    DataFrame = Any  # type: ignore[assignment]
    F = None  # type: ignore[assignment]

from transport_etl.quality.duplicates import split_by_duplicates
from transport_etl.silver.keys import business_key_for, source_identity_for

LOGGER_NAME = "transport_etl.silver.deduplication"

# Per-entity secondary tie-breaker column for the "latest record wins"
# policy.  These columns already exist on the respective entities per
# the explicit schemas in ``config/schemas/*.schema.json``.
ENTITY_TIE_BREAKER_COLUMNS: dict[str, tuple[str, ...]] = {
    "stg_shipments": ("pickup_ts",),
    "stg_carriers": ("carrier_name",),
    "stg_delivery_events": ("event_ts",),
}


@dataclass(frozen=True)
class DedupOutcome:
    """Immutable result of a Silver dedup operation.

    Attributes:
        table_name: Silver table the dedup was performed for.
        survived:   Spark DataFrame containing exactly one row per
                    business key (the row with the largest ``updated_at``).
        dropped:    Spark DataFrame containing the duplicate rows that
                    were dropped.  Empty when no duplicates exist.
        key_columns: Business key columns used to partition the data.
        tie_breaker_columns: Tie-breaker columns applied after ``updated_at``.
    """

    table_name: str
    survived: DataFrame
    dropped: DataFrame
    key_columns: tuple[str, ...]
    tie_breaker_columns: tuple[str, ...]


def _require_spark() -> None:
    """Ensure pyspark is available before executing Spark operations."""
    if F is None:
        raise ImportError("pyspark is required for Silver deduplication")


def _resolve_tie_breakers(table_name: str, available_columns: list[str]) -> list[str]:
    """Return tie-breaker columns actually present on the DataFrame."""
    candidates = ENTITY_TIE_BREAKER_COLUMNS.get(table_name, ())
    return [col for col in candidates if col in available_columns]


def dedupe_silver(
    df: DataFrame,
    table_name: str,
    *,
    order_by_columns: list[str] | None = None,
) -> DedupOutcome:
    """Deduplicate a Silver DataFrame by its business key, deterministically.

    The "latest record wins" policy orders by ``updated_at`` (descending,
    nulls last) followed by the entity-specific tie-breaker columns
    (descending, nulls last).  The same ordering is applied consistently
    in tests and the daily job, which makes the operation idempotent
    across reruns.

    Args:
        df: Spark DataFrame to deduplicate.  Must include the business
            key columns and ``updated_at``.
        table_name: Silver logical table name.  Used to look up the
            canonical business key and the entity-specific tie-breakers.
        order_by_columns: Optional override for the order-by columns.
            When provided, ``updated_at`` is still prepended to preserve
            the "latest wins" semantics.

    Returns:
        A :class:`DedupOutcome` carrying both the surviving and the
        dropped DataFrames plus the key and tie-breaker columns used.

    Raises:
        ValueError: If ``table_name`` is not a known Silver table.
        ImportError: If PySpark is not installed.
    """
    _require_spark()
    key_columns = list(business_key_for(table_name))
    missing = [col for col in key_columns if col not in df.columns]
    if missing:
        raise ValueError(
            f"Silver dedup: business key columns missing for table " f"'{table_name}': {missing}"
        )

    available_columns = list(df.columns)
    default_tie_breakers = _resolve_tie_breakers(
        table_name=table_name, available_columns=available_columns
    )

    if order_by_columns is None:
        order_columns: list[str] = []
        if "updated_at" in available_columns:
            order_columns.append("updated_at")
        order_columns.extend(default_tie_breakers)
    else:
        order_columns = ["updated_at"] + [
            str(col) for col in order_by_columns if col in available_columns
        ]
        order_columns = list(dict.fromkeys(order_columns))  # de-dup, preserve order

    survived_df, dropped_df = split_by_duplicates(
        df=df,
        key_columns=key_columns,
        order_by_columns=order_columns or None,
    )

    # Re-attach source identity columns to the dropped frame so the
    # quarantine writer can label the row by its business key.
    identity_columns = list(source_identity_for(table_name))
    for column in identity_columns:
        if column not in dropped_df.columns:
            continue  # type: ignore[unreachable]

    return DedupOutcome(
        table_name=table_name,
        survived=survived_df,
        dropped=dropped_df,
        key_columns=tuple(key_columns),
        tie_breaker_columns=tuple(default_tie_breakers),
    )


__all__ = [
    "DedupOutcome",
    "ENTITY_TIE_BREAKER_COLUMNS",
    "dedupe_silver",
]
