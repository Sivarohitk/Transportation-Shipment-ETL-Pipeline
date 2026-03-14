"""Quality rule orchestration and reusable business-rule validators."""

from __future__ import annotations

import logging
from dataclasses import asdict, dataclass
from typing import Any, Mapping

from transport_etl.common.io import write_invalid_records_with_fallback
from transport_etl.quality.duplicates import duplicate_records, key_columns_for_entity
from transport_etl.quality.nulls import critical_columns_for_entity, filter_rows_with_required_nulls
from transport_etl.quality.schema_drift import detect_schema_drift

try:
    from pyspark.sql import DataFrame
    from pyspark.sql import functions as F
except ModuleNotFoundError:  # pragma: no cover - allows non-Spark unit tests
    DataFrame = Any  # type: ignore[assignment]
    F = None  # type: ignore[assignment]


LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class RuleResult:
    """Summary result for a single quality rule."""

    rule_name: str
    failed_count: int
    status: str


def _require_spark() -> None:
    """Ensure pyspark is available before executing Spark operations."""
    if F is None:
        raise ImportError("pyspark is required for quality rule execution")


def _empty_like(df: DataFrame, extra_columns: list[str] | None = None) -> DataFrame:
    """Return an empty DataFrame with optional additional string columns."""
    _require_spark()
    extra_columns = extra_columns or []
    empty_df = df.limit(0)
    for col in extra_columns:
        empty_df = empty_df.withColumn(col, F.lit(None).cast("string"))
    return empty_df


def _label_invalid_rows(df: DataFrame, rule_name: str, reason_expr: Any) -> DataFrame:
    """Attach standardized rule metadata columns to invalid rows."""
    _require_spark()
    return (
        df.withColumn("__rule_name", F.lit(rule_name))
        .withColumn("__rule_reason", reason_expr)
    )


def validate_allowed_values(
    df: DataFrame,
    column: str,
    allowed_values: list[Any],
    allow_null: bool = True,
) -> DataFrame:
    """Return invalid rows where values are outside configured allowed set."""
    _require_spark()
    if column not in df.columns:
        return _label_invalid_rows(
            _empty_like(df),
            f"allowed_values:{column}",
            F.lit(f"missing_column:{column}"),
        )

    allowed = [str(value) for value in allowed_values]
    condition = ~F.col(column).isin(*allowed)
    if allow_null:
        condition = condition & F.col(column).isNotNull()

    invalid = df.filter(condition)
    reason = F.concat(F.lit(f"invalid_allowed_value:{column}:"), F.col(column).cast("string"))
    return _label_invalid_rows(invalid, f"allowed_values:{column}", reason)


def validate_non_negative(df: DataFrame, column: str) -> DataFrame:
    """Return invalid rows where numeric column values are negative."""
    _require_spark()
    if column not in df.columns:
        return _label_invalid_rows(
            _empty_like(df),
            f"non_negative:{column}",
            F.lit(f"missing_column:{column}"),
        )

    invalid = df.filter(F.col(column).isNotNull() & (F.col(column) < F.lit(0)))
    reason = F.concat(F.lit(f"negative_value:{column}:"), F.col(column).cast("string"))
    return _label_invalid_rows(invalid, f"non_negative:{column}", reason)


def validate_timestamp_order(
    df: DataFrame,
    start_column: str,
    end_column: str,
    allow_equal: bool = True,
    allow_null_end: bool = True,
) -> DataFrame:
    """Return invalid rows where end timestamp violates ordering relative to start."""
    _require_spark()
    missing = [col for col in [start_column, end_column] if col not in df.columns]
    if missing:
        return _label_invalid_rows(
            _empty_like(df),
            f"timestamp_order:{start_column}->{end_column}",
            F.lit(f"missing_column:{','.join(missing)}"),
        )

    if allow_equal:
        bad_order = F.col(end_column) < F.col(start_column)
    else:
        bad_order = F.col(end_column) <= F.col(start_column)

    condition = F.col(start_column).isNotNull() & bad_order
    if allow_null_end:
        condition = condition & F.col(end_column).isNotNull()
    else:
        condition = condition | (F.col(start_column).isNotNull() & F.col(end_column).isNull())

    invalid = df.filter(condition)
    reason = F.lit(f"invalid_timestamp_order:{start_column}->{end_column}")
    return _label_invalid_rows(invalid, f"timestamp_order:{start_column}->{end_column}", reason)


def quarantine_bad_records(
    invalid_df: DataFrame,
    quarantine_base_path: str,
    entity: str,
    rule_name: str,
    mode: str = "append",
    write_config: Mapping[str, Any] | None = None,
) -> str:
    """Write invalid records to a quarantine path and return destination path."""
    _require_spark()
    destination = quarantine_base_path.rstrip("/\\") + f"/{entity}/{rule_name}"
    output_df = invalid_df.withColumn("quarantined_at", F.current_timestamp())
    write_invalid_records_with_fallback(
        df=output_df,
        destination=destination,
        mode=mode,
        write_config=write_config,
        logger=LOGGER,
    )

    return destination


def _merge_invalid_frames(frames: list[DataFrame], df: DataFrame) -> DataFrame:
    """Union invalid DataFrames with consistent columns."""
    _require_spark()
    if not frames:
        return _empty_like(df, extra_columns=["__rule_name", "__rule_reason"])

    merged = frames[0]
    for frame in frames[1:]:
        merged = merged.unionByName(frame, allowMissingColumns=True)

    return merged


def _resolve_required_columns(context: Mapping[str, Any], entity: str) -> list[str]:
    """Resolve required columns from context or default entity critical columns."""
    required = context.get("required_columns")
    if isinstance(required, list):
        return [str(col) for col in required]
    return critical_columns_for_entity(entity)


def _resolve_duplicate_keys(context: Mapping[str, Any], entity: str) -> list[str]:
    """Resolve duplicate key columns from context or default entity keys."""
    keys = context.get("duplicate_keys")
    if isinstance(keys, list):
        return [str(col) for col in keys]
    return key_columns_for_entity(entity)


def run_quality_rules(context: dict[str, Any]) -> dict[str, Any]:
    """Execute practical quality checks and return summarized results.

    Expected context keys:
    - `df` (required): Spark DataFrame
    - `entity` (optional): shipments/carriers/delivery_events
    - `required_columns` (optional)
    - `duplicate_keys` (optional)
    - `schema_expected` (optional): expected schema definition for drift checks
    - `allowed_values` (optional): dict[column] -> list
    - `non_negative_columns` (optional): list[column]
    - `timestamp_order_rules` (optional): list[{start_column,end_column,...}]
    - `quarantine_path` (optional): base output path for invalid records
    - `fail_fast` (optional): bool
    - `primary_key` (optional): used to produce `clean_df`
    """
    df = context.get("df")
    if df is None:
        return {
            "status": "NOOP",
            "failed_rules": [],
            "rule_results": [],
            "invalid_count": 0,
            "clean_df": None,
            "invalid_df": None,
        }

    _require_spark()

    entity = str(context.get("entity", "dataset")).strip().lower()
    quarantine_path = context.get("quarantine_path")
    quarantine_write_config = (
        context.get("quarantine_write_config")
        if isinstance(context.get("quarantine_write_config"), Mapping)
        else None
    )
    fail_fast = bool(context.get("fail_fast", False))
    logger = context.get("logger") or LOGGER

    failed_rules: list[str] = []
    rule_summaries: list[RuleResult] = []
    invalid_frames: list[DataFrame] = []

    def _register(rule_name: str, invalid_df: DataFrame) -> None:
        nonlocal failed_rules, rule_summaries, invalid_frames
        count = int(invalid_df.count())
        status = "PASS" if count == 0 else "FAIL"
        rule_summaries.append(RuleResult(rule_name=rule_name, failed_count=count, status=status))

        if count > 0:
            failed_rules.append(rule_name)
            invalid_frames.append(invalid_df)
            logger.warning("Quality rule failed: %s (rows=%s)", rule_name, count)

            if quarantine_path:
                quarantine_bad_records(
                    invalid_df=invalid_df,
                    quarantine_base_path=str(quarantine_path),
                    entity=entity,
                    rule_name=rule_name,
                    write_config=quarantine_write_config,
                )

            if fail_fast:
                raise ValueError(f"Quality rule failed: {rule_name} (rows={count})")

    expected_schema = context.get("schema_expected")
    if expected_schema is not None:
        drift_findings = detect_schema_drift(df.schema, expected_schema)
        drift_count = len(drift_findings)
        drift_status = "PASS" if drift_count == 0 else "FAIL"
        rule_summaries.append(
            RuleResult(rule_name="schema_drift", failed_count=drift_count, status=drift_status)
        )
        if drift_count > 0:
            failed_rules.append("schema_drift")
            logger.warning("Schema drift detected: %s", drift_findings)
            if fail_fast:
                raise ValueError(f"Schema drift detected: {drift_findings}")

    required_columns = _resolve_required_columns(context, entity)
    if required_columns:
        null_invalid = filter_rows_with_required_nulls(df=df, required_columns=required_columns)
        null_invalid = _label_invalid_rows(
            null_invalid,
            "required_nulls",
            F.concat_ws(",", F.col("__failed_null_columns")),
        )
        _register("required_nulls", null_invalid)

    duplicate_keys = _resolve_duplicate_keys(context, entity)
    if duplicate_keys:
        dup_invalid = duplicate_records(df=df, key_columns=duplicate_keys)
        dup_invalid = _label_invalid_rows(
            dup_invalid,
            "duplicate_keys",
            F.lit(f"duplicate_key:{','.join(duplicate_keys)}"),
        )
        _register("duplicate_keys", dup_invalid)

    allowed_values = context.get("allowed_values", {})
    if isinstance(allowed_values, Mapping):
        for column, values in allowed_values.items():
            if not isinstance(values, list):
                continue
            invalid = validate_allowed_values(df=df, column=str(column), allowed_values=values)
            _register(f"allowed_values:{column}", invalid)

    non_negative_columns = context.get("non_negative_columns", [])
    if isinstance(non_negative_columns, list):
        for column in non_negative_columns:
            invalid = validate_non_negative(df=df, column=str(column))
            _register(f"non_negative:{column}", invalid)

    timestamp_order_rules = context.get("timestamp_order_rules", [])
    if isinstance(timestamp_order_rules, list):
        for rule in timestamp_order_rules:
            if not isinstance(rule, Mapping):
                continue

            start_col = rule.get("start_column")
            end_col = rule.get("end_column")
            if not start_col or not end_col:
                continue

            invalid = validate_timestamp_order(
                df=df,
                start_column=str(start_col),
                end_column=str(end_col),
                allow_equal=bool(rule.get("allow_equal", True)),
                allow_null_end=bool(rule.get("allow_null_end", True)),
            )
            _register(f"timestamp_order:{start_col}->{end_col}", invalid)

    invalid_df = _merge_invalid_frames(frames=invalid_frames, df=df)
    invalid_count = int(invalid_df.count()) if invalid_frames else 0

    clean_df = df
    primary_key = context.get("primary_key")
    if invalid_frames and isinstance(primary_key, list) and all(col in df.columns for col in primary_key):
        invalid_keys = invalid_df.select(*primary_key).dropna().dropDuplicates()
        clean_df = df.join(invalid_keys, on=primary_key, how="left_anti")

    return {
        "status": "PASS" if not failed_rules else "FAIL",
        "failed_rules": failed_rules,
        "rule_results": [asdict(item) for item in rule_summaries],
        "invalid_count": invalid_count,
        "clean_df": clean_df,
        "invalid_df": invalid_df,
    }
