"""Unit tests for config helpers."""

from __future__ import annotations

from pathlib import Path

from transport_etl.common.config import load_config, resolve_config_path


def test_load_config_merges_base_and_dev_values() -> None:
    """Config loader should merge base + dev settings."""
    cfg = load_config("dev")

    assert cfg["app"]["env"] == "dev"
    assert cfg["runtime"]["default_write_mode"] == "overwrite"
    assert cfg["paths"]["raw_base_path"] == "data/sample/raw"
    assert cfg["spark"]["profile"] == "local"
    assert cfg["redshift"]["enabled"] is False
    assert cfg["glue"]["enabled"] is False
    assert cfg["glue"]["failure_policy"] == "fail"
    assert cfg["pipeline_state"] == {
        "enabled": True,
        "backend": "auto",
        "root_path": "",
    }
    assert cfg["audit"] == {"enabled": True, "backend": "auto", "path": ""}
    assert cfg["cloudwatch"]["enabled"] is False


def test_load_prod_config_expands_redshift_placeholders(monkeypatch) -> None:
    """Production Redshift values should come from environment variables."""
    values = {
        "REGION": "us-east-1",
        "DATABASE": "analytics",
        "WORKGROUP_NAME": "transport-workgroup",
        "CLUSTER_IDENTIFIER": "",
        "SECRET_ARN": "",
        "DATABASE_USER": "",
        "IAM_ROLE_ARN": "arn:aws:iam::123456789012:role/redshift-copy",
        "SOURCE_S3_PATH": "s3://transport-bucket/redshift-ready",
        "STAGING_SCHEMA": "transport_staging",
        "TARGET_SCHEMA": "transport_analytics",
        "AUDIT_SCHEMA": "transport_audit",
    }
    for suffix, value in values.items():
        monkeypatch.setenv(f"TRANSPORT_ETL_REDSHIFT_{suffix}", value)

    cfg = load_config("prod")

    assert cfg["redshift"]["enabled"] is False
    assert cfg["redshift"]["region"] == "us-east-1"
    assert cfg["redshift"]["source_s3_path"] == "s3://transport-bucket/redshift-ready"


def test_load_prod_config_expands_glue_placeholders(monkeypatch) -> None:
    """The opt-in Glue catalog target should read deployment values from env."""
    monkeypatch.setenv("TRANSPORT_ETL_GLUE_REGION", "us-east-1")
    monkeypatch.setenv("TRANSPORT_ETL_GLUE_DATABASE", "transport_curated")
    monkeypatch.setenv("TRANSPORT_ETL_GLUE_CATALOG_ID", "")

    cfg = load_config("prod")

    assert cfg["glue"]["enabled"] is False
    assert cfg["glue"]["region"] == "us-east-1"
    assert cfg["glue"]["database"] == "transport_curated"
    assert cfg["glue"]["catalog_id"] == ""
    assert cfg["pipeline_state"]["enabled"] is True
    assert cfg["audit"]["enabled"] is True
    assert cfg["cloudwatch"]["enabled"] is False


def test_resolve_config_path_handles_short_name() -> None:
    """Config path resolver should convert short env names to absolute paths."""
    resolved = resolve_config_path("prod")

    assert resolved.is_absolute()
    assert resolved.name == "prod.yaml"


def test_load_config_expands_environment_variables(
    tmp_path: Path,
    monkeypatch,
) -> None:
    """Config loader should expand environment variables in YAML string values."""
    config_dir = tmp_path / "config"
    config_dir.mkdir(parents=True, exist_ok=True)

    (config_dir / "base.yaml").write_text(
        (
            "app:\n"
            "  env: base\n"
            "paths:\n"
            "  raw_base_path: ${RAW_ROOT}/raw\n"
            "runtime:\n"
            "  fail_fast: true\n"
        ),
        encoding="utf-8",
    )
    (config_dir / "dev.yaml").write_text(
        ("app:\n" "  env: dev\n" "runtime:\n" "  fail_fast: false\n"),
        encoding="utf-8",
    )

    monkeypatch.setenv("RAW_ROOT", "/tmp/transport")
    cfg = load_config(config_path="dev", config_dir=config_dir)

    assert cfg["paths"]["raw_base_path"] == "/tmp/transport/raw"
    assert cfg["runtime"]["fail_fast"] is False
    assert str(cfg["app"]["config_path"]).endswith("dev.yaml")
