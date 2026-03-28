"""Configuration loading and merge helpers for YAML-based ETL settings."""

from __future__ import annotations

import copy
import os
from pathlib import Path
from typing import Any, Mapping

import yaml

from transport_etl.common.constants import (
    BASE_CONFIG_FILE,
    CONFIG_DIR,
    DEV_CONFIG_FILE,
    PROD_CONFIG_FILE,
)


def _read_yaml_file(path: Path) -> dict[str, Any]:
    """Read a YAML file and return an object dictionary."""
    if not path.exists():
        raise FileNotFoundError(f"Config path not found: {path}")

    with path.open("r", encoding="utf-8") as handle:
        data = yaml.safe_load(handle) or {}

    if not isinstance(data, dict):
        raise ValueError(f"Config root must be a mapping: {path}")

    return data


def _deep_merge(base: dict[str, Any], override: Mapping[str, Any]) -> dict[str, Any]:
    """Recursively merge two dictionaries without mutating inputs."""
    merged = copy.deepcopy(base)
    for key, value in override.items():
        if isinstance(value, Mapping) and isinstance(merged.get(key), dict):
            merged[key] = _deep_merge(merged[key], value)  # type: ignore[arg-type]
        else:
            merged[key] = copy.deepcopy(value)
    return merged


def _expand_environment_values(payload: Any) -> Any:
    """Expand environment variables in all string values recursively."""
    if isinstance(payload, str):
        return os.path.expandvars(payload)
    if isinstance(payload, list):
        return [_expand_environment_values(item) for item in payload]
    if isinstance(payload, dict):
        return {k: _expand_environment_values(v) for k, v in payload.items()}
    return payload


def _resolve_named_config(name: str, config_dir: Path) -> Path:
    """Resolve config by shorthand name to absolute path."""
    normalized = name.strip().lower()
    if normalized in {"base", "base.yaml"}:
        return config_dir / BASE_CONFIG_FILE
    if normalized in {"dev", "dev.yaml"}:
        return config_dir / DEV_CONFIG_FILE
    if normalized in {"prod", "prod.yaml"}:
        return config_dir / PROD_CONFIG_FILE
    return Path(name)


def resolve_config_path(
    config_path: str | Path | None = None, config_dir: str | Path = CONFIG_DIR
) -> Path:
    """Resolve user-provided config path to an absolute path.

    If no path is provided, this defaults to `config/dev.yaml`.
    """
    config_root = Path(config_dir)
    if not config_root.is_absolute():
        config_root = (Path.cwd() / config_root).resolve()

    if config_path is None:
        return (config_root / DEV_CONFIG_FILE).resolve()

    if isinstance(config_path, Path):
        path = config_path
    else:
        path = _resolve_named_config(config_path, config_root)

    if not path.is_absolute():
        path = (Path.cwd() / path).resolve()

    return path


def load_config(
    config_path: str | Path | None = None, config_dir: str | Path = CONFIG_DIR
) -> dict[str, Any]:
    """Load and merge base config with env-specific overrides.

    Behavior:
    - Reads `config/base.yaml` as the base layer.
    - Merges with the selected config file (dev/prod/custom) when different from base.
    - Expands environment variables in string values.

    Args:
        config_path: Config file path or shorthand (`dev`, `prod`, `base`).
        config_dir: Directory containing environment YAML files.

    Returns:
        A merged configuration dictionary.
    """
    selected_path = resolve_config_path(config_path=config_path, config_dir=config_dir)
    base_path = resolve_config_path(config_path=BASE_CONFIG_FILE, config_dir=config_dir)

    base_config = _read_yaml_file(base_path)
    if selected_path.resolve() == base_path.resolve():
        merged = base_config
    else:
        override = _read_yaml_file(selected_path)
        merged = _deep_merge(base_config, override)

    merged = _expand_environment_values(merged)

    merged.setdefault("app", {})
    merged.setdefault("spark", {})
    merged.setdefault("paths", {})
    merged.setdefault("logging", {})

    merged["app"].setdefault("config_path", str(selected_path))
    return merged
