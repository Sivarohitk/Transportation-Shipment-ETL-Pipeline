"""Silver layer builder per entity.

The Silver builder is the **transformation entry point** for one entity.
It receives a Bronze DataFrame (or a DataFrame produced by the existing
ingest modules) and produces a deduplicated, standardized, region-
enriched, quality-validated Silver DataFrame.  Invalid records are
returned alongside so the caller can quarantine them.

The builder reuses the existing helpers:

- :func:`transport_etl.transform.standardize.standardize_columns`
- :func:`transport_etl.transform.enrich_region.enrich_shipments_with_region`
  / :func:`enrich_delivery_events_with_region`
- :func:`transport_etl.quality.rules.run_quality_rules`
- :func:`transport_etl.quality.schema_drift.detect_schema_drift`
- :func:`transport_etl.silver.deduplication.dedupe_silver`
- :func:`transport_etl.silver.quarantine.quarantine_silver_records`

Silver-lineage columns are appended to every surviving row so the
MERGE / overwrite path can:

- Determine which rows are newer than the target (``_silver_updated_at``).
- Attribute the row to a specific batch / run for audit.
- Ensure the rendered MERGE only refreshes rows whose lineage has
  advanced.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Mapping

try:
    from pyspark.sql import DataFrame, SparkSession
    from pyspark.sql import functions as F
    from pyspark.sql import types as T
except ModuleNotFoundError:  # pragma: no cover
    DataFrame = Any  # type: ignore[assignment]
    SparkSession = Any  # type: ignore[assignment]
    F = None  # type: ignore[assignment]
    T = None  # type: ignore[assignment]

from transport_etl.common.constants import (
    META_COL_SILVER_BATCH_ID,
    META_COL_SILVER_RUN_DATE,
    META_COL_SILVER_UPDATED_AT,
    META_COL_SILVER_VALID_FROM,
    SILVER_METADATA_COLUMNS,
    SILVER_TABLE_NAMES,
    TABLE_SILVER_CARRIERS,
    TABLE_SILVER_DELIVERY_EVENTS,
    TABLE_SILVER_SHIPMENTS,
)
from transport_etl.quality.rules import run_quality_rules
from transport_etl.quality.schema_drift import detect_schema_drift
from transport_etl.silver.deduplication import dedupe_silver
from transport_etl.silver.keys import is_silver_table
from transport_etl.silver.quarantine import (
    RULE_DROPPED_BY_DEDUP,
    RULE_DUPLICATE_KEYS,
    RULE_INVALID_ALLOWED_VALUE,
    RULE_NON_NEGATIVE,
    RULE_REQUIRED_NULLS,
    RULE_SCHEMA_DRIFT,
    RULE_TIMESTAMP_ORDER,
    quarantine_silver_records,
)
from transport_etl.transform.enrich_region import (
    enrich_delivery_events_with_region,
    enrich_shipments_with_region,
)
from transport_etl.transform.standardize import standardize_columns

LOGGER = logging.getLogger("transport_etl.silver.builder")


@dataclass
class SilverBuildResult:
    """Result of a Silver build for one entity.

    Attributes:
        table_name: Silver logical table name.
        silver_df: The deduplicated, standardized, region-enriched
            DataFrame.  Empty when all rows are quarantined.
        invalid_dfs: Mapping of rule name to invalid DataFrame.
        dropped_dups: DataFrame containing the rows dropped during
            dedup (empty when no duplicates existed).
        invalid_count: Total number of invalid records across rules.
        survived_count: Row count of ``silver_df``.
    """

    table_name: str
    silver_df: DataFrame
    invalid_dfs: dict[str, DataFrame] = field(default_factory=dict)
    dropped_dups: DataFrame | None = None
    invalid_count: int = 0
    survived_count: int = 0


def _require_spark() -> None:
    """Ensure pyspark is available before executing Spark operations."""
    if F is None or T is None:
        raise ImportError("pyspark is required for Silver builders")


def _validate_table_name(table_name: str) -> str:
    """Return the canonical Silver table name or raise ``ValueError``."""
    if not is_silver_table(table_name):
        raise ValueError(
            f"Unknown Silver table name '{table_name}'. "
            f"Expected one of {list(SILVER_TABLE_NAMES)}."
        )
    return str(table_name)


def _attach_silver_metadata(
    df: DataFrame,
    *,
    batch_id: str,
    run_date: str,
) -> DataFrame:
    """Append Silver operational lineage columns to a Silver DataFrame.

    Columns:
        - ``_silver_valid_from``: ``current_timestamp()`` at build time.
        - ``_silver_updated_at``: ``updated_at`` from the source record
          (when present) so the MERGE can compare lineage to the
          target.  Falls back to ``current_timestamp()`` when missing.
        - ``_silver_batch_id``: batch identifier from the runner.
        - ``_silver_run_date``: run date from the runner.
    """
    _require_spark()
    enriched = df.withColumn(META_COL_SILVER_VALID_FROM, F.current_timestamp())
    if "updated_at" in df.columns:
        enriched = enriched.withColumn(META_COL_SILVER_UPDATED_AT, F.col("updated_at"))
    else:
        enriched = enriched.withColumn(META_COL_SILVER_UPDATED_AT, F.current_timestamp())
    enriched = enriched.withColumn(META_COL_SILVER_BATCH_ID, F.lit(str(batch_id)))
    enriched = enriched.withColumn(META_COL_SILVER_RUN_DATE, F.lit(str(run_date)))
    return enriched


def _build_shipments_context(schema_def: Mapping[str, Any]) -> dict[str, Any]:
    """Build the quality-rule context for the shipments entity."""
    return {
        "entity": "shipments",
        "required_columns": schema_def.get("required_columns"),
        "duplicate_keys": schema_def.get("primary_key"),
        "allowed_values": {
            name: values for name, values in _allowed_values_by_column(schema_def).items()
        },
        "non_negative_columns": _non_negative_columns(schema_def),
        "timestamp_order_rules": [
            {
                "start_column": "pickup_ts",
                "end_column": "promised_delivery_ts",
                "allow_equal": True,
                "allow_null_end": False,
            },
            {
                "start_column": "pickup_ts",
                "end_column": "actual_delivery_ts",
                "allow_equal": True,
                "allow_null_end": True,
            },
        ],
        "primary_key": schema_def.get("primary_key"),
    }


def _build_carriers_context(schema_def: Mapping[str, Any]) -> dict[str, Any]:
    """Build the quality-rule context for the carriers entity."""
    return {
        "entity": "carriers",
        "required_columns": schema_def.get("required_columns"),
        "duplicate_keys": schema_def.get("primary_key"),
        "allowed_values": _allowed_values_by_column(schema_def),
        "primary_key": schema_def.get("primary_key"),
    }


def _build_delivery_events_context(schema_def: Mapping[str, Any]) -> dict[str, Any]:
    """Build the quality-rule context for the delivery_events entity."""
    return {
        "entity": "delivery_events",
        "required_columns": schema_def.get("required_columns"),
        "duplicate_keys": schema_def.get("primary_key"),
        "allowed_values": _allowed_values_by_column(schema_def),
        "non_negative_columns": _non_negative_columns(schema_def),
        "timestamp_order_rules": [
            {
                "start_column": "event_ts",
                "end_column": "updated_at",
                "allow_equal": True,
                "allow_null_end": False,
            }
        ],
        "primary_key": schema_def.get("primary_key"),
    }


def _allowed_values_by_column(schema_def: Mapping[str, Any]) -> dict[str, list[Any]]:
    """Extract ``allowed_values`` constraints from a schema definition."""
    allowed: dict[str, list[Any]] = {}
    for column in schema_def.get("columns", []) or []:
        if not isinstance(column, Mapping):
            continue
        values = column.get("allowed_values")
        name = column.get("name")
        if isinstance(values, list) and name:
            allowed[str(name)] = [str(v) for v in values]
    return allowed


def _non_negative_columns(schema_def: Mapping[str, Any]) -> list[str]:
    """Extract numeric columns whose ``min`` is non-negative."""
    numeric_types = {"double", "float", "int", "integer", "long", "bigint"}
    columns: list[str] = []
    for column in schema_def.get("columns", []) or []:
        if not isinstance(column, Mapping):
            continue
        if str(column.get("type", "")).strip().lower() not in numeric_types:
            continue
        try:
            minimum = float(column["min"])
        except (KeyError, TypeError, ValueError):
            continue
        if minimum >= 0 and column.get("name"):
            columns.append(str(column["name"]))
    return columns


def _process_quality_results(
    quality_result: Mapping[str, Any],
    table_name: str,
    *,
    quarantine_path: str | None,
    batch_id: str,
    run_date: str,
    write_config: Mapping[str, Any] | None,
    logger: Any,
) -> tuple[DataFrame, dict[str, DataFrame], int]:
    """Translate ``run_quality_rules`` output into Silver artefacts.

    Returns a tuple of (clean_df, invalid_dfs_by_rule, invalid_count).
    Invalid frames are routed to the configured Silver quarantine path
    using the :func:`quarantine_silver_records` helper.

    The function surfaces one invalid frame per Silver rule so the
    quarantine layout (`<table>/<rule>/...`) mirrors the rule names
    declared in :data:`transport_etl.silver.quarantine.ALL_SILVER_RULE_NAMES`.
    """
    _require_spark()
    invalid_dfs: dict[str, DataFrame] = {}

    invalid_df = quality_result.get("invalid_df")
    fallback = invalid_df if invalid_df is not None else quality_result.get("clean_df")

    # The ``run_quality_rules`` helper already labels every invalid
    # row with ``__rule_name`` / ``__rule_reason``.  We split the
    # merged invalid frame per rule by inspecting ``__rule_name`` so
    # the quarantine layout matches the documented rule names.
    if invalid_df is not None and "__rule_name" in invalid_df.columns:
        failed_rules = list(quality_result.get("failed_rules") or [])
        rule_label_map = _build_rule_label_map()
        for failed_rule in failed_rules:
            silver_label = _map_failed_rule_to_silver_label(str(failed_rule), rule_label_map)
            if silver_label is None:
                continue
            subset = invalid_df.filter(F.col("__rule_name") == F.lit(silver_label))
            if subset is not None:
                invalid_dfs[silver_label] = subset
            else:
                invalid_dfs[silver_label] = _empty_with_columns(
                    invalid_df, [RULE_REQUIRED_NULLS, RULE_DUPLICATE_KEYS]
                )

    # When the rule pipeline ran in NOOP mode (no DataFrame supplied)
    # the invalid_df is an empty frame.  We still emit a key per
    # documented Silver rule so the returned map is predictable.
    if not invalid_dfs:
        for silver_rule in (
            RULE_REQUIRED_NULLS,
            RULE_DUPLICATE_KEYS,
            RULE_INVALID_ALLOWED_VALUE,
            RULE_NON_NEGATIVE,
            RULE_TIMESTAMP_ORDER,
            RULE_SCHEMA_DRIFT,
        ):
            invalid_dfs[silver_rule] = _empty_with_columns(
                fallback,
                [RULE_REQUIRED_NULLS, RULE_DUPLICATE_KEYS],
            )

    # Quarantine writes — only persist frames that contain rows.
    if quarantine_path:
        for rule_name, frame in invalid_dfs.items():
            try:
                if frame is None:
                    continue
                if int(frame.count() or 0) == 0:
                    continue
                quarantine_silver_records(
                    invalid_df=frame,
                    quarantine_path=quarantine_path,
                    table_name=table_name,
                    rule_name=rule_name,
                    batch_id=batch_id,
                    run_date=run_date,
                    write_config=write_config,
                )
            except Exception as exc:  # pragma: no cover - defensive
                logger.warning(
                    "Silver quarantine write failed for table=%s rule=%s error=%s",
                    table_name,
                    rule_name,
                    exc,
                )

    clean_df = quality_result.get("clean_df") or quality_result.get("df")
    if clean_df is None and invalid_df is not None:
        clean_df = invalid_df.limit(0)
    if clean_df is None:
        clean_df = fallback

    invalid_count = int(quality_result.get("invalid_count") or 0)
    return clean_df, invalid_dfs, invalid_count


def _build_rule_label_map() -> dict[str, str]:
    """Return the mapping from ``run_quality_rules`` rule names to
    Silver rule labels.

    The quality module emits rule names with column-suffixed strings
    such as ``allowed_values:event_type`` and ``non_negative:cost``.
    We map the prefix to a stable Silver rule label.
    """
    return {
        "required_nulls": RULE_REQUIRED_NULLS,
        "duplicate_keys": RULE_DUPLICATE_KEYS,
        "schema_drift": RULE_SCHEMA_DRIFT,
        "allowed_values": RULE_INVALID_ALLOWED_VALUE,
        "non_negative": RULE_NON_NEGATIVE,
        "timestamp_order": RULE_TIMESTAMP_ORDER,
    }


def _map_failed_rule_to_silver_label(failed_rule: str, mapping: Mapping[str, str]) -> str | None:
    """Map a single failed rule name to its Silver label."""
    if failed_rule in mapping:
        return mapping[failed_rule]
    for prefix, silver_label in mapping.items():
        if failed_rule.startswith(f"{prefix}:"):
            return silver_label
    return None


def _empty_with_columns(df: DataFrame | None, column_names: list[str]) -> DataFrame | None:
    """Return an empty DataFrame with the same schema as ``df`` plus
    the supplied Silver lineage columns.  Returns ``None`` when ``df``
    is ``None`` so callers can decide what to do.
    """
    _require_spark()
    if df is None:
        return None
    empty = df.limit(0)
    for column in column_names:
        if column not in empty.columns:
            empty = empty.withColumn(column, F.lit(None).cast("string"))
    return empty


def _strip_bronze_metadata(df: DataFrame) -> DataFrame:
    """Remove Bronze operational metadata columns from a DataFrame.

    The Silver layer accepts Bronze records (which carry ``_ingested_at``,
    ``_source_file``, ``_batch_id``, ``_run_date``) but those columns
    are not part of the source schema.  Strip them so downstream drift
    detection and quality checks do not produce false positives.
    """
    _require_spark()
    from transport_etl.common.constants import BRONZE_METADATA_COLUMNS

    drop_columns = [c for c in BRONZE_METADATA_COLUMNS if c in df.columns]
    if drop_columns:
        return df.drop(*drop_columns)
    return df


def _schema_drift_findings(
    df: DataFrame,
    schema_def: Mapping[str, Any],
) -> list[str]:
    """Run schema drift detection using the expected schema columns.

    The Bronze layer appends operational metadata columns to every
    record (``_ingested_at``, ``_source_file``, ``_batch_id``,
    ``_run_date``); we strip them before drift detection so they do
    not produce false-positive ``unexpected_column`` findings.
    """
    expected_columns: list[dict[str, Any]] = []
    for column in schema_def.get("columns", []) or []:
        if not isinstance(column, Mapping):
            continue
        if not column.get("name"):
            continue
        expected_columns.append(
            {
                "name": str(column["name"]),
                "type": str(column.get("type", "string")),
                "nullable": True,  # ingestion nullability is loosened by transforms
            }
        )
    if not expected_columns:
        return []
    cleaned_df = _strip_bronze_metadata(df)
    return detect_schema_drift(cleaned_df.schema, {"columns": expected_columns})


def _build_silver(
    spark: SparkSession,
    bronze_df: DataFrame,
    *,
    table_name: str,
    schema_def: Mapping[str, Any],
    region_lookup_df: DataFrame | None,
    batch_id: str,
    run_date: str,
    quarantine_path: str | None = None,
    write_config: Mapping[str, Any] | None = None,
    run_quality: bool = True,
    logger: Any | None = None,
) -> SilverBuildResult:
    """Generic Silver build flow shared by all entities."""
    _require_spark()
    log = logger or LOGGER
    table_name = _validate_table_name(table_name)

    # Strip Bronze operational metadata so the Silver quality and
    # schema-drift checks see only the source columns.  The Silver
    # builder appends its own lineage columns at the very end.
    source_df = _strip_bronze_metadata(bronze_df)

    # 1) Schema drift check (always; never raises).  The output of
    #    detect_schema_drift is also re-used inside run_quality_rules.
    drift_findings = _schema_drift_findings(source_df, schema_def)
    if drift_findings:
        log.warning("Silver schema drift table=%s findings=%s", table_name, drift_findings)

    # 2) Quality rules (uses existing run_quality_rules; never raises)
    quality_result: dict[str, Any] = {
        "status": "NOOP",
        "failed_rules": [],
        "rule_results": [],
        "invalid_count": 0,
        "clean_df": source_df,
        "invalid_df": source_df.limit(0),
    }
    if run_quality:
        context = _quality_context_for(table_name, schema_def)
        context.update(
            {
                "df": source_df,
                "schema_expected": {"columns": _expected_columns(schema_def)},
                "quarantine_path": None,  # we quarantine manually below
                "quarantine_write_config": write_config,
                "fail_fast": False,
                "logger": log,
            }
        )
        quality_result = run_quality_rules(context)

    # 3) Manual quarantine wiring (so we control the destination and
    #    the rule labels)
    clean_df, invalid_dfs, invalid_count = _process_quality_results(
        quality_result,
        table_name=table_name,
        quarantine_path=quarantine_path,
        batch_id=batch_id,
        run_date=run_date,
        write_config=write_config,
        logger=log,
    )

    # 4) Region enrichment + standardization (entity-specific)
    silver_df = _enrich_and_standardize(
        clean_df,
        table_name=table_name,
        region_lookup_df=region_lookup_df,
    )

    # 5) Deterministic dedup by business key
    dedup = dedupe_silver(silver_df, table_name=table_name)
    silver_df = dedup.survived
    dropped_dups = dedup.dropped

    if dropped_dups is not None and quarantine_path and int(dropped_dups.count() or 0) > 0:
        quarantine_silver_records(
            invalid_df=dropped_dups,
            quarantine_path=quarantine_path,
            table_name=table_name,
            rule_name=RULE_DROPPED_BY_DEDUP,
            batch_id=batch_id,
            run_date=run_date,
            write_config=write_config,
        )
        invalid_dfs[RULE_DROPPED_BY_DEDUP] = dropped_dups

    # 6) Silver-lineage columns
    silver_df = _attach_silver_metadata(silver_df, batch_id=batch_id, run_date=run_date)

    survived_count = int(silver_df.count() or 0)
    log.info(
        "Silver build complete table=%s survived=%s invalid=%s drift_findings=%s",
        table_name,
        survived_count,
        invalid_count,
        len(drift_findings),
    )

    return SilverBuildResult(
        table_name=table_name,
        silver_df=silver_df,
        invalid_dfs=invalid_dfs,
        dropped_dups=dropped_dups,
        invalid_count=invalid_count,
        survived_count=survived_count,
    )


def _quality_context_for(table_name: str, schema_def: Mapping[str, Any]) -> dict[str, Any]:
    """Return the per-entity quality rule context."""
    if table_name == TABLE_SILVER_SHIPMENTS:
        return _build_shipments_context(schema_def)
    if table_name == TABLE_SILVER_CARRIERS:
        return _build_carriers_context(schema_def)
    if table_name == TABLE_SILVER_DELIVERY_EVENTS:
        return _build_delivery_events_context(schema_def)
    raise ValueError(f"Unsupported Silver table: {table_name}")


def _expected_columns(schema_def: Mapping[str, Any]) -> list[dict[str, Any]]:
    """Convert a schema definition to a normalized column list."""
    expected: list[dict[str, Any]] = []
    for column in schema_def.get("columns", []) or []:
        if not isinstance(column, Mapping) or not column.get("name"):
            continue
        expected.append(
            {
                "name": str(column["name"]),
                "type": str(column.get("type", "string")),
                "nullable": True,
            }
        )
    return expected


def _enrich_and_standardize(
    df: DataFrame,
    *,
    table_name: str,
    region_lookup_df: DataFrame | None,
) -> DataFrame:
    """Apply standardization + region enrichment for a Silver entity."""
    _require_spark()
    result = standardize_columns(df)
    if table_name == TABLE_SILVER_SHIPMENTS and region_lookup_df is not None:
        result = enrich_shipments_with_region(
            shipments_df=result,
            region_lookup_df=region_lookup_df,
        )
    elif table_name == TABLE_SILVER_DELIVERY_EVENTS and region_lookup_df is not None:
        result = enrich_delivery_events_with_region(
            events_df=result,
            region_lookup_df=region_lookup_df,
        )
    return result


# ---------------------------------------------------------------------------
# Per-entity convenience builders
# ---------------------------------------------------------------------------


def build_silver_shipments(
    spark: SparkSession,
    bronze_df: DataFrame,
    *,
    schema_def: Mapping[str, Any],
    batch_id: str,
    run_date: str,
    region_lookup_df: DataFrame | None = None,
    quarantine_path: str | None = None,
    write_config: Mapping[str, Any] | None = None,
    run_quality: bool = True,
    logger: Any | None = None,
) -> SilverBuildResult:
    """Build Silver ``stg_shipments`` from a Bronze shipments DataFrame."""
    return _build_silver(
        spark,
        bronze_df,
        table_name=TABLE_SILVER_SHIPMENTS,
        schema_def=schema_def,
        region_lookup_df=region_lookup_df,
        batch_id=batch_id,
        run_date=run_date,
        quarantine_path=quarantine_path,
        write_config=write_config,
        run_quality=run_quality,
        logger=logger,
    )


def build_silver_carriers(
    spark: SparkSession,
    bronze_df: DataFrame,
    *,
    schema_def: Mapping[str, Any],
    batch_id: str,
    run_date: str,
    region_lookup_df: DataFrame | None = None,
    quarantine_path: str | None = None,
    write_config: Mapping[str, Any] | None = None,
    run_quality: bool = True,
    logger: Any | None = None,
) -> SilverBuildResult:
    """Build Silver ``stg_carriers`` from a Bronze carriers DataFrame."""
    return _build_silver(
        spark,
        bronze_df,
        table_name=TABLE_SILVER_CARRIERS,
        schema_def=schema_def,
        region_lookup_df=region_lookup_df,
        batch_id=batch_id,
        run_date=run_date,
        quarantine_path=quarantine_path,
        write_config=write_config,
        run_quality=run_quality,
        logger=logger,
    )


def build_silver_delivery_events(
    spark: SparkSession,
    bronze_df: DataFrame,
    *,
    schema_def: Mapping[str, Any],
    batch_id: str,
    run_date: str,
    region_lookup_df: DataFrame | None = None,
    quarantine_path: str | None = None,
    write_config: Mapping[str, Any] | None = None,
    run_quality: bool = True,
    logger: Any | None = None,
) -> SilverBuildResult:
    """Build Silver ``stg_delivery_events`` from a Bronze delivery events frame."""
    return _build_silver(
        spark,
        bronze_df,
        table_name=TABLE_SILVER_DELIVERY_EVENTS,
        schema_def=schema_def,
        region_lookup_df=region_lookup_df,
        batch_id=batch_id,
        run_date=run_date,
        quarantine_path=quarantine_path,
        write_config=write_config,
        run_quality=run_quality,
        logger=logger,
    )


__all__ = [
    "RULE_DROPPED_BY_DEDUP",
    "RULE_DUPLICATE_KEYS",
    "RULE_INVALID_ALLOWED_VALUE",
    "RULE_NON_NEGATIVE",
    "RULE_REQUIRED_NULLS",
    "RULE_SCHEMA_DRIFT",
    "RULE_TIMESTAMP_ORDER",
    "SILVER_METADATA_COLUMNS",
    "SilverBuildResult",
    "build_silver_carriers",
    "build_silver_delivery_events",
    "build_silver_shipments",
]
