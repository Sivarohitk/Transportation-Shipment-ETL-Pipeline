"""Silver layer business keys.

The Silver layer uses one business key per entity.  These keys are the
authoritative column(s) used by:

- the deterministic dedup helper (``silver.deduplication``)
- the Delta MERGE ``ON`` clause (``silver.merge_spec``)
- the quarantine writer when attributing a row to a source identity

Keys are derived from the explicit primary keys declared in
``config/schemas/*.schema.json`` (see ``ENTITY_KEY_COLUMNS`` in
``quality.duplicates``).  This module is the single source of truth so
that test code and Silver builders cannot drift from the schemas.

Why a separate module?
----------------------
The business key is referenced by:

1. Quality rule configurations (e.g. ``primary_key`` arg of ``run_quality_rules``)
2. Dedup helpers (window partition keys)
3. Delta MERGE specifications (ON clause)
4. Quarantine error labels (which source column identifies the bad row)

A single, well-documented mapping avoids string-typo bugs that have
caused production data lakes to silently duplicate rows on MERGE.
"""

from __future__ import annotations

from typing import Any, Mapping

from transport_etl.common.constants import (
    SILVER_BUSINESS_KEYS,
    SILVER_TABLE_NAMES,
    TABLE_SILVER_CARRIERS,
    TABLE_SILVER_DELIVERY_EVENTS,
    TABLE_SILVER_SHIPMENTS,
)

# Per-entity metadata used by the Silver quarantine writer to identify the
# source record.  ``source_identity_columns`` lists the business key columns
# (same as the MERGE key) — they are the most reliable source identifier
# because the ingest module already enforces uniqueness on them.
ENTITY_SOURCE_IDENTITY: dict[str, tuple[str, ...]] = {
    TABLE_SILVER_SHIPMENTS: ("shipment_id",),
    TABLE_SILVER_CARRIERS: ("carrier_id",),
    TABLE_SILVER_DELIVERY_EVENTS: ("event_id",),
}


def _validate_silver_table_name(table_name: str) -> str:
    """Return the canonical Silver table name or raise ``ValueError``."""
    if not table_name or not str(table_name).strip():
        raise ValueError("Silver table name must not be empty")
    if table_name not in SILVER_TABLE_NAMES:
        raise ValueError(
            f"Unknown Silver table name '{table_name}'. "
            f"Expected one of {list(SILVER_TABLE_NAMES)}."
        )
    return table_name


def business_key_for(table_name: str) -> tuple[str, ...]:
    """Return the business key column tuple for a Silver entity.

    Args:
        table_name: One of the values in ``SILVER_TABLE_NAMES``.

    Returns:
        Ordered tuple of business key column names.  At least one element.

    Raises:
        ValueError: If ``table_name`` is not a recognised Silver table.
    """
    table_name = _validate_silver_table_name(table_name)
    return SILVER_BUSINESS_KEYS[table_name]


def business_key_str_for(table_name: str) -> str:
    """Return the canonical, comma-separated MERGE ON key string.

    Example:
        >>> business_key_str_for("stg_shipments")
        'shipment_id'
    """
    return ", ".join(business_key_for(table_name))


def source_identity_for(table_name: str) -> tuple[str, ...]:
    """Return the source identity columns used to attribute a quarantined row.

    For the three Silver entities the source identity is identical to the
    business key because the underlying primary key is unique.
    """
    table_name = _validate_silver_table_name(table_name)
    return ENTITY_SOURCE_IDENTITY[table_name]


def is_silver_table(table_name: str | None) -> bool:
    """Return True when ``table_name`` is a known Silver logical table."""
    return isinstance(table_name, str) and table_name in SILVER_TABLE_NAMES


def known_silver_tables() -> tuple[str, ...]:
    """Return the tuple of supported Silver logical table names."""
    return tuple(SILVER_TABLE_NAMES)


__all__ = [
    "business_key_for",
    "business_key_str_for",
    "is_silver_table",
    "known_silver_tables",
    "source_identity_for",
]


def _build_silver_keys_view() -> Mapping[str, Any]:
    """Return a stable read-only view used by documentation helpers and tests."""
    return {
        table: {
            "business_key": SILVER_BUSINESS_KEYS[table],
            "source_identity": ENTITY_SOURCE_IDENTITY[table],
        }
        for table in SILVER_TABLE_NAMES
    }
