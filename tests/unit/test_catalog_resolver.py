"""Unit tests for the configuration-driven catalog/table name resolver.

These tests exercise ``common/catalog.py`` and require no PySpark session.
"""

from __future__ import annotations

import pytest  # noqa: F401

from transport_etl.common.catalog import (
    get_catalog,
    get_schema,
    is_databricks,
    resolve_table_name,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _databricks_config(
    catalog: str = "supply_chain",
    bronze: str = "bronze",
    silver: str = "silver",
    gold: str = "gold",
) -> dict:
    """Return a minimal Databricks-targeted config dict."""
    return {
        "spark": {"profile": "databricks"},
        "unity_catalog": {
            "catalog": catalog,
            "bronze_schema": bronze,
            "silver_schema": silver,
            "gold_schema": gold,
        },
        "hive": {"database": "curated"},
    }


def _local_config(database: str = "curated") -> dict:
    """Return a minimal local-targeted config dict."""
    return {
        "spark": {"profile": "local"},
        "hive": {"database": database},
    }


def _emr_config(database: str = "curated") -> dict:
    """Return a minimal EMR-targeted config dict."""
    return {
        "spark": {"profile": "emr"},
        "hive": {"database": database},
    }


# ---------------------------------------------------------------------------
# is_databricks
# ---------------------------------------------------------------------------


def test_is_databricks_returns_true_for_databricks_profile() -> None:
    assert is_databricks(_databricks_config()) is True


def test_is_databricks_returns_false_for_local_profile() -> None:
    assert is_databricks(_local_config()) is False


def test_is_databricks_returns_false_for_emr_profile() -> None:
    assert is_databricks(_emr_config()) is False


def test_is_databricks_returns_false_when_spark_section_missing() -> None:
    assert is_databricks({}) is False


# ---------------------------------------------------------------------------
# get_catalog
# ---------------------------------------------------------------------------


def test_get_catalog_returns_configured_value() -> None:
    cfg = _databricks_config(catalog="my_org_catalog")
    assert get_catalog(cfg) == "my_org_catalog"


def test_get_catalog_returns_default_when_not_set() -> None:
    cfg = {"spark": {"profile": "databricks"}}
    assert get_catalog(cfg) == "supply_chain"


def test_get_catalog_strips_whitespace() -> None:
    cfg = {"spark": {"profile": "databricks"}, "unity_catalog": {"catalog": "  corp  "}}
    assert get_catalog(cfg) == "corp"


# ---------------------------------------------------------------------------
# get_schema
# ---------------------------------------------------------------------------


def test_get_schema_returns_bronze_schema() -> None:
    cfg = _databricks_config(bronze="ingest")
    assert get_schema(cfg, "bronze") == "ingest"


def test_get_schema_returns_silver_schema() -> None:
    cfg = _databricks_config(silver="clean")
    assert get_schema(cfg, "silver") == "clean"


def test_get_schema_returns_gold_schema() -> None:
    cfg = _databricks_config(gold="analytics")
    assert get_schema(cfg, "gold") == "analytics"


def test_get_schema_uses_defaults_when_unity_catalog_section_missing() -> None:
    cfg = {"spark": {"profile": "databricks"}}
    assert get_schema(cfg, "bronze") == "bronze"
    assert get_schema(cfg, "silver") == "silver"
    assert get_schema(cfg, "gold") == "gold"


def test_get_schema_raises_for_unknown_layer() -> None:
    with pytest.raises(ValueError, match="Unknown medallion layer"):
        get_schema(_databricks_config(), "platinum")


# ---------------------------------------------------------------------------
# resolve_table_name — Databricks target
# ---------------------------------------------------------------------------


def test_resolve_table_name_databricks_gold() -> None:
    cfg = _databricks_config()
    result = resolve_table_name(cfg, "gold", "fct_shipment")
    assert result == "supply_chain.gold.fct_shipment"


def test_resolve_table_name_databricks_bronze() -> None:
    cfg = _databricks_config()
    result = resolve_table_name(cfg, "bronze", "raw_shipments")
    assert result == "supply_chain.bronze.raw_shipments"


def test_resolve_table_name_databricks_silver() -> None:
    cfg = _databricks_config()
    result = resolve_table_name(cfg, "silver", "stg_shipments")
    assert result == "supply_chain.silver.stg_shipments"


def test_resolve_table_name_databricks_kpi() -> None:
    cfg = _databricks_config()
    result = resolve_table_name(cfg, "gold", "kpi_delivery_daily")
    assert result == "supply_chain.gold.kpi_delivery_daily"


def test_resolve_table_name_databricks_custom_catalog() -> None:
    cfg = _databricks_config(catalog="acme", gold="reporting")
    result = resolve_table_name(cfg, "gold", "dim_carrier")
    assert result == "acme.reporting.dim_carrier"


# ---------------------------------------------------------------------------
# resolve_table_name — Local / EMR targets
# ---------------------------------------------------------------------------


def test_resolve_table_name_local_uses_hive_database() -> None:
    cfg = _local_config(database="curated")
    result = resolve_table_name(cfg, "gold", "fct_shipment")
    assert result == "curated.fct_shipment"


def test_resolve_table_name_emr_uses_hive_database() -> None:
    cfg = _emr_config(database="prod_curated")
    result = resolve_table_name(cfg, "gold", "kpi_delivery_daily")
    assert result == "prod_curated.kpi_delivery_daily"


def test_resolve_table_name_local_layer_ignored() -> None:
    """Layer argument is not used for local/EMR; only hive.database matters."""
    cfg = _local_config()
    bronze_result = resolve_table_name(cfg, "bronze", "raw_shipments")
    gold_result = resolve_table_name(cfg, "gold", "raw_shipments")
    assert bronze_result == gold_result == "curated.raw_shipments"


def test_resolve_table_name_local_fallback_database_default() -> None:
    """When hive section is absent the default database 'curated' is used."""
    cfg = {"spark": {"profile": "local"}}
    result = resolve_table_name(cfg, "gold", "dim_carrier")
    assert result == "curated.dim_carrier"


# ---------------------------------------------------------------------------
# resolve_table_name — edge cases
# ---------------------------------------------------------------------------


def test_resolve_table_name_raises_on_empty_table() -> None:
    with pytest.raises(ValueError, match="table name must not be empty"):
        resolve_table_name(_local_config(), "gold", "")


def test_resolve_table_name_strips_table_whitespace() -> None:
    cfg = _local_config()
    result = resolve_table_name(cfg, "gold", "  fct_shipment  ")
    assert result == "curated.fct_shipment"
