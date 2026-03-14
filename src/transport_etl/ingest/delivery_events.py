"""Delivery events ingestion with schema enforcement and row-level validation."""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any, Mapping

try:
    from pyspark.sql import DataFrame, SparkSession
    from pyspark.sql import functions as F
    from pyspark.sql import types as T
except ModuleNotFoundError:  # pragma: no cover - allows import in non-Spark environments
    DataFrame = Any  # type: ignore[assignment]
    SparkSession = Any  # type: ignore[assignment]
    F = None  # type: ignore[assignment]
    T = None  # type: ignore[assignment]

from transport_etl.common.io import (
    ensure_local_dir,
    is_cloud_path,
    read_csv,
    write_invalid_records_with_fallback,
)


LOGGER = logging.getLogger(__name__)
_SCHEMA_PATH = Path(__file__).resolve().parents[3] / "config" / "schemas" / "delivery_events.schema.json"
_CORRUPT_COL = "_corrupt_record"


def _require_spark() -> None:
    """Ensure pyspark is available before executing Spark operations."""
    if F is None or T is None:
        raise ImportError("pyspark is required for delivery-event ingestion")


def load_delivery_events_schema_definition(schema_path: str | Path = _SCHEMA_PATH) -> dict[str, Any]:
    """Load the delivery events schema definition JSON."""
    path = Path(schema_path)
    if not path.exists():
        raise FileNotFoundError(f"Delivery event schema file not found: {path}")

    with path.open("r", encoding="utf-8-sig") as handle:
        payload = json.load(handle)

    if not isinstance(payload, dict) or "columns" not in payload:
        raise ValueError("Delivery events schema definition is invalid: missing 'columns'")

    return payload


def _spark_type(type_name: str) -> T.DataType:
    """Map schema type name to Spark data type."""
    _require_spark()
    normalized = type_name.strip().lower()
    mapping: dict[str, T.DataType] = {
        "string": T.StringType(),
        "double": T.DoubleType(),
        "float": T.DoubleType(),
        "int": T.IntegerType(),
        "integer": T.IntegerType(),
        "long": T.LongType(),
        "bigint": T.LongType(),
        "boolean": T.BooleanType(),
        "bool": T.BooleanType(),
        "timestamp": T.TimestampType(),
    }
    if normalized not in mapping:
        raise ValueError(f"Unsupported schema type: {type_name}")
    return mapping[normalized]


def build_delivery_events_schema(schema_def: Mapping[str, Any] | None = None) -> T.StructType:
    """Build typed Spark schema for cleaned delivery event records."""
    _require_spark()
    definition = dict(schema_def or load_delivery_events_schema_definition())
    fields = [
        T.StructField(col["name"], _spark_type(str(col["type"])), bool(col.get("nullable", True)))
        for col in definition["columns"]
    ]
    return T.StructType(fields)


def _build_raw_read_schema(schema_def: Mapping[str, Any]) -> T.StructType:
    """Build raw read schema where all source columns are loaded as strings."""
    _require_spark()
    fields = [T.StructField(col["name"], T.StringType(), True) for col in schema_def["columns"]]
    fields.append(T.StructField(_CORRUPT_COL, T.StringType(), True))
    return T.StructType(fields)


def _timestamp_formats(schema_def: Mapping[str, Any]) -> list[str]:
    """Return allowed timestamp formats from schema options."""
    options = schema_def.get("options", {})
    if not isinstance(options, Mapping):
        return ["yyyy-MM-dd'T'HH:mm:ssX"]

    formats = options.get("timestamp_formats")
    if isinstance(formats, list) and formats:
        return [str(item) for item in formats]

    single = options.get("timestamp_format")
    if single:
        return [str(single)]

    return ["yyyy-MM-dd'T'HH:mm:ssX"]


def _normalize_text(df: DataFrame, schema_def: Mapping[str, Any]) -> DataFrame:
    """Trim strings and apply case normalization from schema metadata."""
    for col_def in schema_def["columns"]:
        name = str(col_def["name"])
        expr = F.trim(F.col(name))

        if col_def.get("uppercase"):
            expr = F.upper(expr)
        elif col_def.get("lowercase"):
            expr = F.lower(expr)

        df = df.withColumn(name, F.when(F.length(expr) == 0, F.lit(None)).otherwise(expr))

    return df


def _parse_boolean(raw_col: str) -> Any:
    """Parse truthy/falsey strings into a boolean Spark column expression."""
    lowered = F.lower(F.col(raw_col))
    return (
        F.when(lowered.isin("true", "t", "1", "yes", "y"), F.lit(True))
        .when(lowered.isin("false", "f", "0", "no", "n"), F.lit(False))
        .otherwise(F.lit(None).cast("boolean"))
    )


def _parse_timestamp(raw_col: str, formats: list[str]) -> Any:
    """Parse timestamps from one or more allowed timestamp formats."""
    parsed = None
    for fmt in formats:
        candidate = F.to_timestamp(F.col(raw_col), fmt)
        parsed = candidate if parsed is None else F.coalesce(parsed, candidate)

    if parsed is None:
        parsed = F.to_timestamp(F.col(raw_col))
    else:
        parsed = F.coalesce(parsed, F.to_timestamp(F.col(raw_col)))

    return parsed


def _cast_columns(df: DataFrame, schema_def: Mapping[str, Any]) -> DataFrame:
    """Cast normalized raw columns into their target schema types."""
    ts_formats = _timestamp_formats(schema_def)

    for col_def in schema_def["columns"]:
        name = str(col_def["name"])
        type_name = str(col_def["type"]).lower()

        if type_name == "string":
            continue

        raw_col = f"__raw_{name}"
        df = df.withColumn(raw_col, F.col(name))

        if type_name in {"double", "float"}:
            df = df.withColumn(name, F.col(raw_col).cast("double"))
        elif type_name in {"int", "integer"}:
            df = df.withColumn(name, F.col(raw_col).cast("int"))
        elif type_name in {"long", "bigint"}:
            df = df.withColumn(name, F.col(raw_col).cast("bigint"))
        elif type_name in {"boolean", "bool"}:
            df = df.withColumn(name, _parse_boolean(raw_col))
        elif type_name == "timestamp":
            df = df.withColumn(name, _parse_timestamp(raw_col, ts_formats))
        else:
            raise ValueError(f"Unsupported delivery event type conversion: {type_name}")

    return df


def _validation_errors(df: DataFrame, schema_def: Mapping[str, Any]) -> list[Any]:
    """Build row-level validation expressions for delivery event ingestion."""
    errors: list[Any] = [
        F.when(F.col(_CORRUPT_COL).isNotNull(), F.lit("corrupt_record")),
    ]

    required = schema_def.get("required_columns")
    if not isinstance(required, list):
        required = [c["name"] for c in schema_def["columns"] if not bool(c.get("nullable", True))]

    for column in required:
        errors.append(F.when(F.col(str(column)).isNull(), F.lit(f"missing_required:{column}")))

    for col_def in schema_def["columns"]:
        name = str(col_def["name"])
        type_name = str(col_def["type"]).lower()

        if type_name != "string":
            raw_col = f"__raw_{name}"
            errors.append(
                F.when(
                    F.col(raw_col).isNotNull() & F.col(name).isNull(),
                    F.lit(f"invalid_{type_name}:{name}"),
                )
            )

        if "length" in col_def:
            expected_len = int(col_def["length"])
            errors.append(
                F.when(
                    F.col(name).isNotNull() & (F.length(F.col(name)) != F.lit(expected_len)),
                    F.lit(f"invalid_length:{name}"),
                )
            )

        if "min" in col_def:
            errors.append(
                F.when(
                    F.col(name).isNotNull() & (F.col(name) < F.lit(col_def["min"])),
                    F.lit(f"below_min:{name}"),
                )
            )

        if "max" in col_def:
            errors.append(
                F.when(
                    F.col(name).isNotNull() & (F.col(name) > F.lit(col_def["max"])),
                    F.lit(f"above_max:{name}"),
                )
            )

        if "allowed_values" in col_def and isinstance(col_def["allowed_values"], list):
            allowed = [str(item) for item in col_def["allowed_values"]]
            errors.append(
                F.when(
                    F.col(name).isNotNull() & (~F.col(name).isin(*allowed)),
                    F.lit(f"invalid_value:{name}"),
                )
            )

    errors.append(
        F.when(
            F.col("event_type").isin("DELIVERY_ATTEMPT", "DELIVERED") & F.col("attempt_number").isNull(),
            F.lit("missing_required:attempt_number_for_delivery_event"),
        )
    )
    errors.append(
        F.when(
            F.col("event_type").isin("DELAYED", "EXCEPTION", "HOLD") & F.col("delay_reason").isNull(),
            F.lit("missing_required:delay_reason_for_delay_or_exception"),
        )
    )
    errors.append(
        F.when(
            F.col("event_ts").isNotNull()
            & F.col("updated_at").isNotNull()
            & (F.col("updated_at") < F.col("event_ts")),
            F.lit("invalid_time_order:updated_at_before_event_ts"),
        )
    )

    return errors


def _attach_validation(df: DataFrame, schema_def: Mapping[str, Any]) -> DataFrame:
    """Attach validation result columns for valid/invalid split."""
    error_exprs = _validation_errors(df, schema_def)
    raw_array_expr = F.array(*error_exprs) if error_exprs else F.array(F.lit(None))

    # Use SQL `filter` to remove nulls from the collected error array safely.
    # This avoids passing a Column object where Spark expects a Python literal.
    return (
        df.withColumn("__ingest_errors_raw", raw_array_expr)
        .withColumn(
            "__ingest_errors",
            F.expr("filter(__ingest_errors_raw, x -> x is not null)"),
        )
        .drop("__ingest_errors_raw")
        .withColumn("__is_valid", F.size(F.col("__ingest_errors")) == 0)
    )


def _write_invalid_records(
    invalid_df: DataFrame,
    bad_records_path: str | None,
    source_path: str,
    entity: str,
    bad_record_write_config: Mapping[str, Any] | None = None,
) -> None:
    """Persist invalid rows for operational triage."""
    if not bad_records_path:
        return

    destination = bad_records_path.rstrip("/\\") + f"/{entity}"
    output_df = (
        invalid_df.withColumn("entity", F.lit(entity))
        .withColumn("source_path", F.lit(source_path))
        .withColumn("ingested_at", F.current_timestamp())
    )
    write_invalid_records_with_fallback(
        df=output_df,
        destination=destination,
        mode="append",
        write_config=bad_record_write_config,
        logger=LOGGER,
    )


def read_delivery_events_raw(
    spark: SparkSession,
    source_path: str,
    bad_records_path: str | None = None,
    read_options: Mapping[str, Any] | None = None,
    bad_record_write_config: Mapping[str, Any] | None = None,
) -> DataFrame:
    """Read, clean, and validate delivery event raw data for staging.

    Returns only valid rows, while invalid rows are optionally written to a
    quarantine path via `bad_records_path`.
    """
    _require_spark()
    schema_def = load_delivery_events_schema_definition()

    options = {
        **dict(schema_def.get("options", {})),
        _CORRUPT_COL: _CORRUPT_COL,
        "columnNameOfCorruptRecord": _CORRUPT_COL,
    }
    options.pop(_CORRUPT_COL, None)

    if bad_records_path:
        parser_path = bad_records_path.rstrip("/\\") + "/parser/delivery_events"
        if not is_cloud_path(parser_path):
            ensure_local_dir(parser_path)
        options["badRecordsPath"] = parser_path

    if read_options:
        options.update({str(k): str(v) for k, v in read_options.items()})

    raw_df = read_csv(
        spark=spark,
        path=source_path,
        schema=_build_raw_read_schema(schema_def),
        options=options,
    )

    normalized = _normalize_text(raw_df, schema_def)
    casted = _cast_columns(normalized, schema_def)
    validated = _attach_validation(casted, schema_def)

    invalid_df = validated.filter(~F.col("__is_valid"))
    _write_invalid_records(
        invalid_df=invalid_df,
        bad_records_path=bad_records_path,
        source_path=source_path,
        entity=str(schema_def.get("entity", "delivery_events")),
        bad_record_write_config=bad_record_write_config,
    )

    helper_columns = [
        _CORRUPT_COL,
        "__ingest_errors",
        "__is_valid",
    ] + [
        f"__raw_{col['name']}"
        for col in schema_def["columns"]
        if str(col["type"]).lower() != "string"
    ]

    ordered_columns = [str(col["name"]) for col in schema_def["columns"]]
    valid_df = validated.filter(F.col("__is_valid")).drop(*helper_columns).select(*ordered_columns)

    LOGGER.info("Delivery event ingestion completed for source_path=%s", source_path)
    return valid_df

