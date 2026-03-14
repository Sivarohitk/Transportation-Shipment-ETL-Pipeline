"""Project-level constants and configuration defaults."""

from __future__ import annotations

from pathlib import Path

# Base paths
PROJECT_ROOT = Path(__file__).resolve().parents[3]
CONFIG_DIR = PROJECT_ROOT / "config"
SPARK_PROFILE_DIR = CONFIG_DIR / "spark"

# Config file names
BASE_CONFIG_FILE = "base.yaml"
DEV_CONFIG_FILE = "dev.yaml"
PROD_CONFIG_FILE = "prod.yaml"
SUPPORTED_ENVS = {"base", "dev", "prod"}

# Spark profiles
SPARK_PROFILE_LOCAL = "local"
SPARK_PROFILE_EMR = "emr"
SUPPORTED_SPARK_PROFILES = {SPARK_PROFILE_LOCAL, SPARK_PROFILE_EMR}
SPARK_LOCAL_CONF_FILE = "local.conf"
SPARK_EMR_CONF_FILE = "emr.conf"

# Table names
CURATED_DB = "curated"
TABLE_DIM_CARRIER = "dim_carrier"
TABLE_FCT_SHIPMENT = "fct_shipment"
TABLE_FCT_DELIVERY_EVENT = "fct_delivery_event"
TABLE_AGG_SHIPMENT_DAILY = "agg_shipment_daily"
TABLE_KPI_DELIVERY_DAILY = "kpi_delivery_daily"

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

# Logging defaults
DEFAULT_LOG_LEVEL = "INFO"
DEFAULT_LOGGER_NAME = "transport_etl"
