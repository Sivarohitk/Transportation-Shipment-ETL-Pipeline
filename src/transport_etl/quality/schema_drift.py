"""Schema drift detection utilities for raw and staging datasets."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping, Sequence

_TYPE_ALIASES = {
    "str": "string",
    "varchar": "string",
    "char": "string",
    "int": "integer",
    "bigint": "long",
    "float": "double",
    "bool": "boolean",
    "timestamp_ntz": "timestamp",
}


def _normalize_type_name(type_name: str) -> str:
    """Normalize type names across schema representations."""
    raw = type_name.strip().lower()
    return _TYPE_ALIASES.get(raw, raw)


def _normalize_column_spec(raw: Mapping[str, Any]) -> dict[str, Any]:
    """Normalize a single column schema specification."""
    name = str(raw["name"]).strip()
    type_name = _normalize_type_name(str(raw.get("type", "string")))
    nullable = bool(raw.get("nullable", True))
    return {"name": name, "type": type_name, "nullable": nullable}


def _columns_from_schema_definition(schema_def: Mapping[str, Any]) -> list[dict[str, Any]]:
    """Extract normalized columns from a schema JSON payload."""
    columns = schema_def.get("columns")
    if not isinstance(columns, list):
        raise ValueError("Expected schema definition to contain a 'columns' list")
    return [_normalize_column_spec(col) for col in columns if isinstance(col, Mapping)]


def _columns_from_spark_schema(schema: Any) -> list[dict[str, Any]]:
    """Extract normalized columns from a Spark StructType-like object."""
    fields = getattr(schema, "fields", None)
    if not isinstance(fields, list):
        raise ValueError("Spark schema object does not expose a 'fields' list")

    normalized: list[dict[str, Any]] = []
    for field in fields:
        dtype = getattr(field, "dataType", None)
        if dtype is None:
            type_name = "string"
        elif hasattr(dtype, "simpleString"):
            type_name = str(dtype.simpleString())
        else:
            type_name = str(dtype)

        normalized.append(
            {
                "name": str(getattr(field, "name", "")).strip(),
                "type": _normalize_type_name(type_name),
                "nullable": bool(getattr(field, "nullable", True)),
            }
        )

    return normalized


def _columns_from_mapping(schema_map: Mapping[str, Any]) -> list[dict[str, Any]]:
    """Extract normalized columns from simple mapping forms.

    Supported mapping forms:
    - {"column": "type"}
    - {"column": {"type": "...", "nullable": true}}
    """
    normalized: list[dict[str, Any]] = []
    for name, value in schema_map.items():
        if isinstance(value, Mapping):
            raw = {
                "name": name,
                "type": value.get("type", "string"),
                "nullable": value.get("nullable", True),
            }
        else:
            raw = {"name": name, "type": value, "nullable": True}

        normalized.append(_normalize_column_spec(raw))

    return normalized


def _load_json_if_path(schema: Any) -> Any:
    """Load JSON content when given a schema path."""
    if isinstance(schema, (str, Path)):
        candidate = Path(schema)
        if candidate.exists() and candidate.suffix.lower() == ".json":
            with candidate.open("r", encoding="utf-8") as handle:
                return json.load(handle)
    return schema


def normalize_schema(schema: Any) -> list[dict[str, Any]]:
    """Normalize various schema representations into a common list format.

    Supported inputs:
    - schema JSON definition dictionaries (with `columns`)
    - Spark StructType objects
    - mapping forms like {column_name: type}
    - list of {name, type, nullable} dictionaries
    - path to a `.json` schema definition file
    """
    payload = _load_json_if_path(schema)

    if payload is None:
        return []

    if isinstance(payload, Mapping):
        if "columns" in payload:
            return _columns_from_schema_definition(payload)
        return _columns_from_mapping(payload)

    if hasattr(payload, "fields"):
        return _columns_from_spark_schema(payload)

    if isinstance(payload, Sequence) and not isinstance(payload, (str, bytes, bytearray)):
        normalized: list[dict[str, Any]] = []
        for item in payload:
            if isinstance(item, Mapping):
                normalized.append(_normalize_column_spec(item))
        return normalized

    raise TypeError(f"Unsupported schema representation: {type(payload)}")


def detect_schema_drift(
    actual_schema: Any,
    expected_schema: Any,
    enforce_column_order: bool = False,
) -> list[str]:
    """Compare actual schema against expected schema and return drift findings."""
    actual_columns = normalize_schema(actual_schema)
    expected_columns = normalize_schema(expected_schema)

    actual_names = [col["name"] for col in actual_columns]
    expected_names = [col["name"] for col in expected_columns]

    actual_map = {col["name"]: col for col in actual_columns}
    expected_map = {col["name"]: col for col in expected_columns}

    findings: list[str] = []

    for name in expected_names:
        if name not in actual_map:
            findings.append(f"missing_column:{name}")

    for name in actual_names:
        if name not in expected_map:
            findings.append(f"unexpected_column:{name}")

    for name in expected_names:
        if name not in actual_map:
            continue

        actual = actual_map[name]
        expected = expected_map[name]

        if actual["type"] != expected["type"]:
            findings.append(
                f"type_mismatch:{name}:expected={expected['type']}:actual={actual['type']}"
            )

        if bool(actual["nullable"]) != bool(expected["nullable"]):
            findings.append(
                f"nullability_mismatch:{name}:expected={expected['nullable']}:actual={actual['nullable']}"
            )

    if enforce_column_order:
        shared_actual = [name for name in actual_names if name in expected_map]
        shared_expected = [name for name in expected_names if name in actual_map]
        if shared_actual != shared_expected:
            findings.append("column_order_mismatch")

    return findings


def has_schema_drift(
    actual_schema: Any, expected_schema: Any, enforce_column_order: bool = False
) -> bool:
    """Return True when any schema drift finding is present."""
    return len(detect_schema_drift(actual_schema, expected_schema, enforce_column_order)) > 0


def detect_df_schema_drift(
    df: Any, expected_schema: Any, enforce_column_order: bool = False
) -> list[str]:
    """Detect schema drift for a DataFrame-like object with a `schema` attribute."""
    if df is None or not hasattr(df, "schema"):
        raise ValueError("A DataFrame-like object with a 'schema' attribute is required")

    return detect_schema_drift(df.schema, expected_schema, enforce_column_order)
