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
        (
            "app:\n"
            "  env: dev\n"
            "runtime:\n"
            "  fail_fast: false\n"
        ),
        encoding="utf-8",
    )

    monkeypatch.setenv("RAW_ROOT", "/tmp/transport")
    cfg = load_config(config_path="dev", config_dir=config_dir)

    assert cfg["paths"]["raw_base_path"] == "/tmp/transport/raw"
    assert cfg["runtime"]["fail_fast"] is False
    assert str(cfg["app"]["config_path"]).endswith("dev.yaml")
