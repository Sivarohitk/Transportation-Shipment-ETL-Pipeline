"""Unit tests for Databricks configuration loading and Spark profile support.

These tests verify that:
- the 'databricks' shorthand resolves to config/databricks.yaml
- databricks.yaml loads correctly and merges with base.yaml
- the databricks Spark profile is accepted by build_spark_conf
- local and EMR configs are unaffected by the changes

All tests are pure Python and require no PySpark session.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from transport_etl.common.config import load_config, resolve_config_path
from transport_etl.common.constants import (
    DEFAULT_BRONZE_SCHEMA,
    DEFAULT_GOLD_SCHEMA,
    DEFAULT_SILVER_SCHEMA,
    DEFAULT_UNITY_CATALOG,
    SPARK_PROFILE_DATABRICKS,
    SPARK_PROFILE_EMR,
    SPARK_PROFILE_LOCAL,
    SUPPORTED_SPARK_PROFILES,
)
from transport_etl.common.spark import _parse_spark_conf_file, build_spark_conf

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------


def test_databricks_profile_constant_value() -> None:
    assert SPARK_PROFILE_DATABRICKS == "databricks"


def test_databricks_profile_in_supported_profiles() -> None:
    assert SPARK_PROFILE_DATABRICKS in SUPPORTED_SPARK_PROFILES


def test_local_and_emr_profiles_still_in_supported_profiles() -> None:
    assert SPARK_PROFILE_LOCAL in SUPPORTED_SPARK_PROFILES
    assert SPARK_PROFILE_EMR in SUPPORTED_SPARK_PROFILES


def test_default_unity_catalog_constant() -> None:
    assert DEFAULT_UNITY_CATALOG == "supply_chain"


def test_default_layer_schema_constants() -> None:
    assert DEFAULT_BRONZE_SCHEMA == "bronze"
    assert DEFAULT_SILVER_SCHEMA == "silver"
    assert DEFAULT_GOLD_SCHEMA == "gold"


# ---------------------------------------------------------------------------
# config.py: resolve_config_path
# ---------------------------------------------------------------------------


def test_resolve_config_path_databricks_shorthand() -> None:
    resolved = resolve_config_path("databricks")
    assert resolved.is_absolute()
    assert resolved.name == "databricks.yaml"


def test_resolve_config_path_databricks_yaml_shorthand() -> None:
    resolved = resolve_config_path("databricks.yaml")
    assert resolved.name == "databricks.yaml"


def test_resolve_config_path_local_unaffected() -> None:
    resolved = resolve_config_path("dev")
    assert resolved.name == "dev.yaml"


def test_resolve_config_path_emr_unaffected() -> None:
    resolved = resolve_config_path("prod")
    assert resolved.name == "prod.yaml"


# ---------------------------------------------------------------------------
# config.py: load_config with databricks
# ---------------------------------------------------------------------------


def test_load_databricks_config_spark_profile() -> None:
    """Databricks config must set spark.profile = databricks."""
    cfg = load_config("databricks")
    assert cfg["spark"]["profile"] == "databricks"


def test_load_databricks_config_env() -> None:
    cfg = load_config("databricks")
    assert cfg["app"]["env"] == "databricks"


def test_load_databricks_config_unity_catalog_section_present() -> None:
    cfg = load_config("databricks")
    assert "unity_catalog" in cfg
    uc = cfg["unity_catalog"]
    assert "catalog" in uc
    assert "bronze_schema" in uc
    assert "silver_schema" in uc
    assert "gold_schema" in uc


def test_load_databricks_config_hive_registration_disabled() -> None:
    cfg = load_config("databricks")
    assert cfg["hive"]["register_tables"] is False
    assert cfg["hive"]["repair_partitions"] is False


def test_load_databricks_config_inherits_base_partitioning() -> None:
    """Databricks config should inherit partition keys from base.yaml."""
    cfg = load_config("databricks")
    keys = cfg.get("partitioning", {}).get("keys", [])
    assert "p_date" in keys
    assert "region_code" in keys
    assert "carrier_id" in keys


def test_load_databricks_config_no_hardcoded_credentials() -> None:
    """Databricks config must not contain any credential-like strings."""
    cfg = load_config("databricks")
    forbidden = ("token", "secret", "password", "account_id", "workspace_url", "api_key")
    # The optional Redshift section is inherited from base.yaml and has an
    # intentionally empty secret_arn setting. Preserve the Databricks scan,
    # while asserting that the inherited credential value is also empty.
    redshift = cfg.pop("redshift")
    assert redshift["enabled"] is False
    assert redshift["secret_arn"] == ""
    cfg_str = str(cfg).lower()
    for term in forbidden:
        assert term not in cfg_str, f"Credential-like term '{term}' found in databricks config"


def test_load_databricks_config_paths_use_dbfs_or_env_vars() -> None:
    """Databricks paths must reference dbfs:// or expandable env vars, not s3:// or local."""
    cfg = load_config("databricks")
    paths = cfg.get("paths", {})
    for key, value in paths.items():
        assert not str(value).startswith(
            "data/"
        ), f"paths.{key} should not use a local relative path in databricks config"
        assert not str(value).startswith(
            "s3://"
        ), f"paths.{key} should not hardcode an S3 bucket in databricks config"


# ---------------------------------------------------------------------------
# config.py: existing local and EMR configs unaffected
# ---------------------------------------------------------------------------


def test_load_dev_config_unaffected() -> None:
    cfg = load_config("dev")
    assert cfg["spark"]["profile"] == "local"
    assert cfg["app"]["env"] == "dev"
    # unity_catalog section must not exist on local config
    assert "unity_catalog" not in cfg


def test_load_prod_config_unaffected() -> None:
    cfg = load_config("prod")
    assert cfg["spark"]["profile"] == "emr"
    assert cfg["app"]["env"] == "prod"
    assert "unity_catalog" not in cfg


# ---------------------------------------------------------------------------
# spark.py: build_spark_conf with databricks profile
# ---------------------------------------------------------------------------


def test_build_spark_conf_accepts_databricks_profile(tmp_path: Path) -> None:
    """build_spark_conf must not raise for the databricks profile."""
    # Write a minimal databricks.conf to a temp directory.
    spark_dir = tmp_path / "spark"
    spark_dir.mkdir()
    (spark_dir / "databricks.conf").write_text(
        "spark.sql.session.timeZone=UTC\n",
        encoding="utf-8",
    )

    conf = build_spark_conf(profile="databricks", spark_profile_dir=spark_dir)
    assert isinstance(conf, dict)
    assert conf.get("spark.sql.session.timeZone") == "UTC"


def test_build_spark_conf_databricks_no_master_key(tmp_path: Path) -> None:
    """Databricks profile must not inject spark.master (cluster manages this)."""
    spark_dir = tmp_path / "spark"
    spark_dir.mkdir()
    (spark_dir / "databricks.conf").write_text("", encoding="utf-8")

    conf = build_spark_conf(profile="databricks", spark_profile_dir=spark_dir)
    assert "spark.master" not in conf


def test_build_spark_conf_local_still_injects_master(tmp_path: Path) -> None:
    """Local profile must still inject spark.master = local[*] as before."""
    spark_dir = tmp_path / "spark"
    spark_dir.mkdir()
    (spark_dir / "local.conf").write_text("", encoding="utf-8")

    conf = build_spark_conf(profile="local", spark_profile_dir=spark_dir)
    assert conf.get("spark.master") == "local[*]"


def test_build_spark_conf_rejects_unknown_profile() -> None:
    with pytest.raises(ValueError, match="Unsupported Spark profile"):
        build_spark_conf(profile="unknown_profile")


# ---------------------------------------------------------------------------
# spark.py: databricks.conf file is parseable
# ---------------------------------------------------------------------------


def test_databricks_conf_file_is_parseable() -> None:
    """The actual config/spark/databricks.conf must parse without error."""
    from transport_etl.common.constants import SPARK_PROFILE_DIR

    conf_path = SPARK_PROFILE_DIR / "databricks.conf"
    assert conf_path.exists(), f"Expected {conf_path} to exist"

    parsed = _parse_spark_conf_file(conf_path)
    assert isinstance(parsed, dict)
    # File must not contain credential-like keys.
    for key in parsed:
        assert "token" not in key.lower()
        assert "secret" not in key.lower()
        assert "password" not in key.lower()
