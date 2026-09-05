"""Configuration-driven catalog and table name resolution.

Produces fully-qualified table identifiers for each execution target:

- Local / EMR:   ``<database>.<table>``   (two-part Hive convention)
- Databricks:    ``<catalog>.<schema>.<table>``  (Unity Catalog convention)

All names are read from the merged config dictionary.  No catalog, schema,
or table name is hardcoded here; defaults are pulled from ``constants.py``
and can always be overridden via ``config/databricks.yaml`` or runtime
``--overrides``.

This module has no PySpark dependency and can be imported and tested without
a running Spark session.
"""

from __future__ import annotations

from typing import Any, Mapping

from transport_etl.common.constants import (
    CURATED_DB,
    DEFAULT_BRONZE_SCHEMA,
    DEFAULT_GOLD_SCHEMA,
    DEFAULT_SILVER_SCHEMA,
    DEFAULT_UNITY_CATALOG,
    SPARK_PROFILE_DATABRICKS,
)


def _get_spark_profile(config: Mapping[str, Any]) -> str:
    """Extract the Spark profile string from a merged config dictionary."""
    spark_section = config.get("spark", {})
    if not isinstance(spark_section, Mapping):
        return ""
    return str(spark_section.get("profile", "")).strip().lower()


def is_databricks(config: Mapping[str, Any]) -> bool:
    """Return True when the config targets the Databricks execution environment."""
    return _get_spark_profile(config) == SPARK_PROFILE_DATABRICKS


def _unity_catalog_section(config: Mapping[str, Any]) -> Mapping[str, Any]:
    """Return the ``unity_catalog`` config section, or an empty mapping."""
    section = config.get("unity_catalog", {})
    return section if isinstance(section, Mapping) else {}


def get_catalog(config: Mapping[str, Any]) -> str:
    """Return the Unity Catalog name from config.

    Falls back to ``DEFAULT_UNITY_CATALOG`` when not set.  Only meaningful
    for Databricks targets; callers should check ``is_databricks`` first.
    """
    return str(_unity_catalog_section(config).get("catalog", DEFAULT_UNITY_CATALOG)).strip()


def get_schema(config: Mapping[str, Any], layer: str) -> str:
    """Return the schema name for a given medallion layer.

    Args:
        config: Merged application config dictionary.
        layer:  One of ``"bronze"``, ``"silver"``, or ``"gold"``.

    Returns:
        Schema name string from config, or the matching default constant.
    """
    layer = layer.strip().lower()
    section = _unity_catalog_section(config)

    if layer == "bronze":
        return str(section.get("bronze_schema", DEFAULT_BRONZE_SCHEMA)).strip()
    if layer == "silver":
        return str(section.get("silver_schema", DEFAULT_SILVER_SCHEMA)).strip()
    if layer == "gold":
        return str(section.get("gold_schema", DEFAULT_GOLD_SCHEMA)).strip()

    raise ValueError(f"Unknown medallion layer: '{layer}'. Expected bronze, silver, or gold.")


def resolve_table_name(config: Mapping[str, Any], layer: str, table: str) -> str:
    """Return a fully-qualified table identifier for the active execution target.

    - Databricks: ``<catalog>.<schema>.<table>``
    - Local / EMR: ``<hive_database>.<table>``

    Args:
        config: Merged application config dictionary.
        layer:  Medallion layer (``"bronze"``, ``"silver"``, or ``"gold"``).
                Used only when targeting Databricks.
        table:  Base table name (e.g. ``"fct_shipment"``).

    Returns:
        Fully-qualified table identifier string.

    Examples:
        >>> # Databricks target
        >>> resolve_table_name(databricks_cfg, "gold", "fct_shipment")
        'supply_chain.gold.fct_shipment'

        >>> # Local / EMR target
        >>> resolve_table_name(dev_cfg, "gold", "fct_shipment")
        'curated.fct_shipment'
    """
    table = table.strip()
    if not table:
        raise ValueError("table name must not be empty")

    if is_databricks(config):
        catalog = get_catalog(config)
        schema = get_schema(config, layer)
        return f"{catalog}.{schema}.{table}"

    # Local and EMR: use the existing hive.database convention.
    hive_section = config.get("hive", {})
    if isinstance(hive_section, Mapping):
        raw_db = hive_section.get("database", CURATED_DB)
    else:
        raw_db = CURATED_DB
    database = str(raw_db).strip()
    return f"{database}.{table}"
