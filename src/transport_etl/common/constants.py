"""Project-level constants and configuration defaults."""

from __future__ import annotations

import os
from pathlib import Path

# Base paths
PROJECT_ROOT = Path(__file__).resolve().parents[3]
RESOURCE_BASE_PATH_ENV = "TRANSPORT_ETL_RESOURCE_BASE_PATH"
TEST_QUARANTINE_BASE_PATH_ENV = "TRANSPORT_ETL_TEST_QUARANTINE_BASE_PATH"
CONFIG_DIR = PROJECT_ROOT / "config"
SPARK_PROFILE_DIR = CONFIG_DIR / "spark"

# Config file names
BASE_CONFIG_FILE = "base.yaml"
DEV_CONFIG_FILE = "dev.yaml"
PROD_CONFIG_FILE = "prod.yaml"
GLUE_CONFIG_FILE = "glue.yaml"
DATABRICKS_CONFIG_FILE = "databricks.yaml"
SUPPORTED_ENVS = {"base", "dev", "prod", "glue", "databricks"}

# Spark profiles
SPARK_PROFILE_LOCAL = "local"
SPARK_PROFILE_EMR = "emr"
SPARK_PROFILE_GLUE = "glue"
SPARK_PROFILE_DATABRICKS = "databricks"
SUPPORTED_SPARK_PROFILES = {
    SPARK_PROFILE_LOCAL,
    SPARK_PROFILE_EMR,
    SPARK_PROFILE_GLUE,
    SPARK_PROFILE_DATABRICKS,
}
SPARK_LOCAL_CONF_FILE = "local.conf"
SPARK_EMR_CONF_FILE = "emr.conf"
SPARK_GLUE_CONF_FILE = "glue.conf"
SPARK_DATABRICKS_CONF_FILE = "databricks.conf"

# Table names
CURATED_DB = "curated"
TABLE_DIM_CARRIER = "dim_carrier"
TABLE_FCT_SHIPMENT = "fct_shipment"
TABLE_FCT_DELIVERY_EVENT = "fct_delivery_event"
TABLE_AGG_SHIPMENT_DAILY = "agg_shipment_daily"
TABLE_KPI_DELIVERY_DAILY = "kpi_delivery_daily"

# Additional Phase-6 Gold analytics tables.  These build on the existing
# ``fct_shipment`` / ``fct_delivery_event`` / ``dim_carrier`` Gold facts
# and surface supply-chain KPIs not present in the daily roll-up.
TABLE_GOLD_CARRIER_PERFORMANCE = "carrier_performance"
TABLE_GOLD_ROUTE_PERFORMANCE = "route_performance"
TABLE_GOLD_DELIVERY_EXCEPTION_SUMMARY = "delivery_exception_summary"

# Compatibility fallback used only when a caller supplies a partial local
# configuration instead of loading the environment YAML files.
DEFAULT_LOCAL_CURATED_BASE_PATH = "data/local/curated"
GOLD_ANALYTICS_TABLE_NAMES: tuple[str, ...] = (
    TABLE_GOLD_CARRIER_PERFORMANCE,
    TABLE_GOLD_ROUTE_PERFORMANCE,
    TABLE_GOLD_DELIVERY_EXCEPTION_SUMMARY,
)

# Event types that count as delivery exceptions (mirrors the
# ``fct_delivery_event`` builder's exception_flag).
DELIVERY_EXCEPTION_EVENT_TYPES: frozenset[str] = frozenset({"DELAYED", "EXCEPTION", "HOLD"})

# Bronze logical table names (medallion layer).
# On Databricks these resolve to ``<catalog>.bronze.<table>``; on local/EMR
# the existing ``curated`` hive database convention is preserved.
TABLE_BRONZE_SHIPMENTS = "raw_shipments"
TABLE_BRONZE_CARRIERS = "raw_carriers"
TABLE_BRONZE_DELIVERY_EVENTS = "raw_delivery_events"
BRONZE_TABLE_NAMES: tuple[str, ...] = (
    TABLE_BRONZE_SHIPMENTS,
    TABLE_BRONZE_CARRIERS,
    TABLE_BRONZE_DELIVERY_EVENTS,
)

# Silver logical table names (medallion layer).
# On Databricks these resolve to ``<catalog>.silver.<table>``; on local/EMR
# the existing ``curated`` hive database convention is preserved.
TABLE_SILVER_SHIPMENTS = "stg_shipments"
TABLE_SILVER_CARRIERS = "stg_carriers"
TABLE_SILVER_DELIVERY_EVENTS = "stg_delivery_events"
SILVER_TABLE_NAMES: tuple[str, ...] = (
    TABLE_SILVER_SHIPMENTS,
    TABLE_SILVER_CARRIERS,
    TABLE_SILVER_DELIVERY_EVENTS,
)

# Silver business keys (one per Silver entity).  These are the columns used
# in the Delta MERGE ON clause and the deterministic dedup partition key.
# They are derived from the explicit primary keys declared in
# ``config/schemas/*.schema.json`` and never invented here.
SILVER_BUSINESS_KEYS: dict[str, tuple[str, ...]] = {
    TABLE_SILVER_SHIPMENTS: ("shipment_id",),
    TABLE_SILVER_CARRIERS: ("carrier_id",),
    TABLE_SILVER_DELIVERY_EVENTS: ("event_id",),
}

# Silver operational lineage columns appended to every Silver record.  They
# support the documented MERGE idempotency story (the MERGE only refreshes
# rows when source ``_updated_at`` is strictly greater than the target).
META_COL_SILVER_VALID_FROM = "_silver_valid_from"
META_COL_SILVER_UPDATED_AT = "_silver_updated_at"
META_COL_SILVER_BATCH_ID = "_silver_batch_id"
META_COL_SILVER_RUN_DATE = "_silver_run_date"
SILVER_METADATA_COLUMNS: tuple[str, ...] = (
    META_COL_SILVER_VALID_FROM,
    META_COL_SILVER_UPDATED_AT,
    META_COL_SILVER_BATCH_ID,
    META_COL_SILVER_RUN_DATE,
)

# Bronze operational ingestion metadata columns.
# These are appended to every Bronze record produced by the builder.
META_COL_INGESTED_AT = "_ingested_at"
META_COL_SOURCE_FILE = "_source_file"
META_COL_BATCH_ID = "_batch_id"
META_COL_RUN_DATE = "_run_date"
BRONZE_METADATA_COLUMNS: tuple[str, ...] = (
    META_COL_INGESTED_AT,
    META_COL_SOURCE_FILE,
    META_COL_BATCH_ID,
    META_COL_RUN_DATE,
)

# Medallion layer schema defaults (Databricks / Unity Catalog only).
# Actual names are always read from config; these are fallback defaults only.
DEFAULT_UNITY_CATALOG = "supply_chain"
DEFAULT_BRONZE_SCHEMA = "bronze"
DEFAULT_SILVER_SCHEMA = "silver"
DEFAULT_GOLD_SCHEMA = "gold"

# Common partition contract
PARTITION_COL_DATE = "p_date"
PARTITION_COL_REGION = "region_code"
PARTITION_COL_CARRIER = "carrier_id"

# I/O defaults
DEFAULT_WRITE_MODE = "overwrite"
DEFAULT_PARQUET_COMPRESSION = "snappy"
DEFAULT_CSV_OPTIONS: dict[str, str] = {
    "header": "true",
    "inferSchema": "false",
    "mode": "PERMISSIVE",
    "timestampFormat": "yyyy-MM-dd'T'HH:mm:ssX",
}


def resolve_resource_path(*parts: str) -> Path:
    """Resolve a repository resource using an optional runtime root override."""
    root = Path(os.environ.get(RESOURCE_BASE_PATH_ENV, str(PROJECT_ROOT)))
    return root.joinpath(*parts)


# Logging defaults
DEFAULT_LOG_LEVEL = "INFO"
DEFAULT_LOGGER_NAME = "transport_etl"
