"""Silver layer for the supply chain transportation lakehouse.

The Silver layer takes the operational records written by the Bronze
layer and produces validated, standardized, deduplicated, region-
enriched datasets.  Silver is the system of record for downstream
Gold models and the eventual reporting layer.

Design goals
-----------

- **Reuse the existing helpers** in ``transport_etl.quality``,
  ``transport_etl.transform``, and ``transport_etl.ingest`` rather than
  introducing parallel implementations.
- **Quarantine, never silently drop** invalid records.
- **Deterministic dedup** by the per-entity business key, ordered by
  ``updated_at`` (the source column that tracks operational changes).
- **Idempotent MERGE** for the Databricks target so reruns converge to
  the same target state.

Public API
----------

- Business-key helpers: :mod:`transport_etl.silver.keys`
- Deterministic dedup: :func:`dedupe_silver`
- Per-entity builders:
  :func:`build_silver_shipments`, :func:`build_silver_carriers`,
  :func:`build_silver_delivery_events`
- Quarantine writer: :func:`quarantine_silver_records`
- MERGE renderer / orchestrator:
  :mod:`transport_etl.silver.merge_spec`, :mod:`transport_etl.silver.merge`
- Target-aware publisher: :func:`publish_silver_table`

All catalog, schema, table, and storage path identifiers are read from
configuration — nothing is hardcoded in this package.
"""

from __future__ import annotations

from transport_etl.silver.builder import (
    RULE_DROPPED_BY_DEDUP,
    RULE_DUPLICATE_KEYS,
    RULE_INVALID_ALLOWED_VALUE,
    RULE_NON_NEGATIVE,
    RULE_REQUIRED_NULLS,
    RULE_SCHEMA_DRIFT,
    RULE_TIMESTAMP_ORDER,
    SILVER_METADATA_COLUMNS,
    SilverBuildResult,
    build_silver_carriers,
    build_silver_delivery_events,
    build_silver_shipments,
)
from transport_etl.silver.deduplication import DedupOutcome, dedupe_silver
from transport_etl.silver.keys import (
    business_key_for,
    business_key_str_for,
    is_silver_table,
    known_silver_tables,
    source_identity_for,
)
from transport_etl.silver.merge import (
    build_silver_merge_spec,
    execute_silver_merge,
    register_source_view,
    render_silver_merge_sql,
)
from transport_etl.silver.merge_spec import (
    SilverMergeSpec,
    build_merge_condition,
    build_merge_sql,
    quote_qualified_table,
)
from transport_etl.silver.publisher import (
    TABLE_SILVER_CARRIERS,
    TABLE_SILVER_DELIVERY_EVENTS,
    TABLE_SILVER_SHIPMENTS,
    publish_silver_table,
)
from transport_etl.silver.quarantine import (
    ALL_SILVER_RULE_NAMES,
    quarantine_silver_records,
)

__all__ = [
    "ALL_SILVER_RULE_NAMES",
    "DedupOutcome",
    "RULE_DROPPED_BY_DEDUP",
    "RULE_DUPLICATE_KEYS",
    "RULE_INVALID_ALLOWED_VALUE",
    "RULE_NON_NEGATIVE",
    "RULE_REQUIRED_NULLS",
    "RULE_SCHEMA_DRIFT",
    "RULE_TIMESTAMP_ORDER",
    "SILVER_METADATA_COLUMNS",
    "SilverBuildResult",
    "SilverMergeSpec",
    "TABLE_SILVER_CARRIERS",
    "TABLE_SILVER_DELIVERY_EVENTS",
    "TABLE_SILVER_SHIPMENTS",
    "build_merge_condition",
    "build_merge_sql",
    "build_silver_carriers",
    "build_silver_delivery_events",
    "build_silver_merge_spec",
    "build_silver_shipments",
    "business_key_for",
    "business_key_str_for",
    "dedupe_silver",
    "execute_silver_merge",
    "is_silver_table",
    "known_silver_tables",
    "publish_silver_table",
    "quarantine_silver_records",
    "quote_qualified_table",
    "register_source_view",
    "render_silver_merge_sql",
    "source_identity_for",
]
