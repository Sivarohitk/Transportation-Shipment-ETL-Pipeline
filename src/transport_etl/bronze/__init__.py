"""Bronze layer for the supply chain transportation lakehouse.

The Bronze layer preserves raw source records with the smallest possible
transformation footprint.  It reuses the explicit schema parsing from
``config/schemas/*.schema.json`` via ``transport_etl.ingest`` and adds
operational ingestion metadata columns (``_ingested_at``, ``_source_file``,
``_batch_id``, ``_run_date``).  Invalid records are quarantined through
the existing quality strategy — never silently dropped.

Public API
----------

- ``build_bronze_shipments``, ``build_bronze_carriers``,
  ``build_bronze_delivery_events`` — produce Bronze DataFrames.
- ``publish_bronze_table`` — writes a Bronze DataFrame to the active
  backend (Parquet on local/EMR, Delta on Databricks).
- ``BRONZE_METADATA_COLUMNS`` — the canonical metadata column contract.

All catalog, schema, table, and storage path identifiers are read from
configuration — nothing is hardcoded in this package.
"""

from __future__ import annotations

from transport_etl.bronze.builder import (
    build_bronze_carriers,
    build_bronze_delivery_events,
    build_bronze_shipments,
)
from transport_etl.bronze.metadata import (
    BRONZE_METADATA_COLUMNS,
    META_COL_BATCH_ID,
    META_COL_INGESTED_AT,
    META_COL_RUN_DATE,
    META_COL_SOURCE_FILE,
    build_batch_id,
)
from transport_etl.bronze.publisher import (
    BRONZE_TABLE_NAMES,
    TABLE_BRONZE_CARRIERS,
    TABLE_BRONZE_DELIVERY_EVENTS,
    TABLE_BRONZE_SHIPMENTS,
    publish_bronze_table,
)

__all__ = [
    "BRONZE_METADATA_COLUMNS",
    "BRONZE_TABLE_NAMES",
    "META_COL_BATCH_ID",
    "META_COL_INGESTED_AT",
    "META_COL_RUN_DATE",
    "META_COL_SOURCE_FILE",
    "TABLE_BRONZE_CARRIERS",
    "TABLE_BRONZE_DELIVERY_EVENTS",
    "TABLE_BRONZE_SHIPMENTS",
    "build_batch_id",
    "build_bronze_carriers",
    "build_bronze_delivery_events",
    "build_bronze_shipments",
    "publish_bronze_table",
]
