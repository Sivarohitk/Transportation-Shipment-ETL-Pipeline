"""Daily batch job orchestration."""

from __future__ import annotations

import copy
import re
import uuid
from pathlib import Path
from typing import Any, Mapping

from transport_etl.common.catalog import is_databricks, resolve_table_name
from transport_etl.common.config import load_config
from transport_etl.common.constants import (
    DEFAULT_LOCAL_CURATED_BASE_PATH,
    TABLE_AGG_SHIPMENT_DAILY,
    TABLE_DIM_CARRIER,
    TABLE_FCT_DELIVERY_EVENT,
    TABLE_FCT_SHIPMENT,
    TABLE_GOLD_CARRIER_PERFORMANCE,
    TABLE_GOLD_DELIVERY_EXCEPTION_SUMMARY,
    TABLE_GOLD_ROUTE_PERFORMANCE,
    TABLE_KPI_DELIVERY_DAILY,
)
from transport_etl.common.dates import DATE_FMT, parse_date, resolve_run_date
from transport_etl.common.io import is_cloud_path, path_exists
from transport_etl.common.logging import configure_logging, get_logger
from transport_etl.common.spark import create_spark_session_from_config, stop_spark_session
from transport_etl.publish.hive_writer import write_partitioned_table
from transport_etl.publish.partitions import required_partition_columns

PROJECT_ROOT = Path(__file__).resolve().parents[3]
_DATED_CSV_PATTERN = re.compile(r"_(\d{4}-\d{2}-\d{2})\.csv$")
_RAW_ENTITIES = ("shipments", "carriers", "delivery_events")


def _resolve_gold_write_target(config: Mapping[str, Any], table_name: str) -> tuple[str, str]:
    """Return the target identifier and format for a Gold publication."""
    if is_databricks(config):
        return resolve_table_name(config, "gold", table_name), "delta"
    return table_name, "parquet"


def _resolve_runtime_resource_paths(config: Mapping[str, Any]) -> tuple[Path, Path]:
    """Resolve schema and staging-SQL directories without changing local defaults."""
    runtime = config.get("runtime", {}) if isinstance(config.get("runtime"), Mapping) else {}
    resource_root = Path(str(runtime.get("resource_base_path", PROJECT_ROOT)))
    return resource_root / "config" / "schemas", resource_root / "sql" / "staging"


def _join_storage_path(base_path: str, *parts: str) -> str:
    """Join paths for both local filesystems and cloud object storage URIs."""
    cleaned_parts = [part.strip("/\\") for part in parts if part and part.strip("/\\")]
    if is_cloud_path(base_path):
        base = base_path.rstrip("/\\")
        return "/".join([base] + cleaned_parts)
    return str(Path(base_path, *cleaned_parts))


def _safe_count(df: Any) -> int:
    """Return the row count of a DataFrame, or ``-1`` when unsupported."""
    if df is None:
        return -1
    if hasattr(df, "count"):
        try:
            return int(df.count())
        except Exception:  # pragma: no cover - defensive
            return -1
    return -1


def _set_nested_value(payload: dict[str, Any], dotted_key: str, value: Any) -> None:
    """Set a nested dictionary value using a dotted-path key."""
    keys = [segment for segment in dotted_key.split(".") if segment]
    if not keys:
        return

    current = payload
    for key in keys[:-1]:
        if key not in current or not isinstance(current[key], dict):
            current[key] = {}
        current = current[key]

    current[keys[-1]] = value


def _apply_overrides(config: dict[str, Any], overrides: Mapping[str, Any] | None) -> dict[str, Any]:
    """Apply runtime override values onto a loaded config dictionary."""
    merged = copy.deepcopy(config)
    if not overrides:
        return merged

    for key, value in overrides.items():
        if value is None:
            continue
        _set_nested_value(merged, str(key), value)

    return merged


def _extract_dated_suffix(csv_name: str) -> str | None:
    """Extract `YYYY-MM-DD` suffix from file names like `shipments_2026-01-01.csv`."""
    match = _DATED_CSV_PATTERN.search(csv_name)
    if not match:
        return None
    return match.group(1)


def _discover_latest_local_raw_date(raw_base_path: str) -> str | None:
    """Discover latest available date suffix across expected local raw CSV files."""
    if is_cloud_path(raw_base_path):
        return None

    base_path = Path(raw_base_path)
    if not base_path.exists():
        return None

    discovered_dates: set[str] = set()
    for entity in _RAW_ENTITIES:
        for csv_file in base_path.glob(f"{entity}_*.csv"):
            date_str = _extract_dated_suffix(csv_file.name)
            if date_str:
                discovered_dates.add(date_str)

    if not discovered_dates:
        return None

    return sorted(discovered_dates)[-1]


def _resolve_batch_date(run_date: str | None, raw_base_path: str) -> str:
    """Resolve batch date with validation and local sample-data fallback."""
    if run_date:
        return parse_date(run_date, field_name="run_date").strftime(DATE_FMT)

    discovered = _discover_latest_local_raw_date(raw_base_path)
    if discovered:
        return discovered

    return resolve_run_date(None)


def _resolve_entity_source_path(raw_base_path: str, entity: str, batch_date: str) -> str:
    """Resolve best available raw source path for a specific entity/date."""
    dated_file = _join_storage_path(raw_base_path, f"{entity}_{batch_date}.csv")
    if path_exists(dated_file):
        return dated_file

    undated_file = _join_storage_path(raw_base_path, f"{entity}.csv")
    if path_exists(undated_file):
        return undated_file

    raise FileNotFoundError(
        f"Raw source not found for entity='{entity}', date='{batch_date}', base='{raw_base_path}'"
    )


def _resolve_region_lookup_path(reference_base_path: str) -> str:
    """Resolve region lookup CSV path used for enrichment."""
    candidate = _join_storage_path(reference_base_path, "region_lookup.csv")
    if not path_exists(candidate):
        raise FileNotFoundError(f"Region lookup file not found: {candidate}")
    return candidate


def _load_sql_file(path: Path) -> str:
    """Read a SQL file from disk."""
    if not path.exists():
        raise FileNotFoundError(f"SQL file not found: {path}")
    return path.read_text(encoding="utf-8")


def _build_allowed_values(schema_def: Mapping[str, Any]) -> dict[str, list[Any]]:
    """Extract allowed-value constraints from schema definitions."""
    allowed: dict[str, list[Any]] = {}
    for column in schema_def.get("columns", []):
        if not isinstance(column, Mapping):
            continue

        values = column.get("allowed_values")
        name = column.get("name")
        if isinstance(values, list) and name:
            allowed[str(name)] = values

    return allowed


def _build_non_negative_columns(schema_def: Mapping[str, Any]) -> list[str]:
    """Extract columns configured with minimum 0 constraints."""
    numeric_types = {"double", "float", "int", "integer", "long", "bigint"}
    columns: list[str] = []

    for column in schema_def.get("columns", []):
        if not isinstance(column, Mapping):
            continue
        if str(column.get("type", "")).strip().lower() not in numeric_types:
            continue
        if "min" not in column:
            continue

        try:
            minimum = float(column["min"])
        except (TypeError, ValueError):
            continue

        if minimum >= 0 and column.get("name"):
            columns.append(str(column["name"]))

    return columns


def _schema_for_drift(schema_def: Mapping[str, Any]) -> dict[str, Any]:
    """Build drift-check schema focused on stable column names/types."""
    normalized_columns: list[dict[str, Any]] = []
    for column in schema_def.get("columns", []):
        if not isinstance(column, Mapping):
            continue
        if not column.get("name"):
            continue
        normalized_columns.append(
            {
                "name": str(column["name"]),
                "type": str(column.get("type", "string")),
                # Ingestion transformations can loosen Spark nullability metadata.
                "nullable": True,
            }
        )

    return {"columns": normalized_columns}


def _blocking_quality_failures(
    failed_rules: list[str],
    quality_config: Mapping[str, Any],
) -> list[str]:
    """Return configured blocking quality failures from a quality result."""
    blocked: list[str] = []
    if bool(quality_config.get("fail_on_schema_drift", True)) and "schema_drift" in failed_rules:
        blocked.append("schema_drift")
    if (
        bool(quality_config.get("fail_on_required_nulls", True))
        and "required_nulls" in failed_rules
    ):
        blocked.append("required_nulls")
    if (
        bool(quality_config.get("fail_on_duplicate_primary_keys", True))
        and "duplicate_keys" in failed_rules
    ):
        blocked.append("duplicate_keys")
    return blocked


def _run_staging_sql(
    spark: Any,
    sql_dir: Path,
    sql_file_name: str,
    raw_view_name: str,
    staging_view_name: str,
    input_df: Any,
) -> Any:
    """Run a staging SQL file against a supplied raw temp view."""
    input_df.createOrReplaceTempView(raw_view_name)
    query = _load_sql_file(sql_dir / sql_file_name)
    staged_df = spark.sql(query)
    staged_df.createOrReplaceTempView(staging_view_name)
    return staged_df


def _execute_daily_flow(
    spark: Any,
    config: Mapping[str, Any],
    batch_date: str,
    logger: Any,
) -> dict[str, Any]:
    """Execute end-to-end daily ETL flow."""
    from transport_etl.ingest.carriers import (
        load_carriers_schema_definition,
        read_carriers_raw,
    )
    from transport_etl.ingest.delivery_events import (
        load_delivery_events_schema_definition,
        read_delivery_events_raw,
    )
    from transport_etl.ingest.shipments import (
        load_shipments_schema_definition,
        read_shipments_raw,
    )
    from transport_etl.quality.rules import run_quality_rules
    from transport_etl.transform.build_agg_shipment_daily import build_agg_shipment_daily
    from transport_etl.transform.build_dim_carrier import build_dim_carrier
    from transport_etl.transform.build_fct_delivery_event import build_fct_delivery_event
    from transport_etl.transform.build_fct_shipment import build_fct_shipment
    from transport_etl.transform.build_kpi_delivery_daily import build_kpi_delivery_daily
    from transport_etl.transform.enrich_region import (
        enrich_delivery_events_with_region,
        enrich_shipments_with_region,
        load_region_lookup,
    )
    from transport_etl.transform.standardize import standardize_columns

    paths = config.get("paths", {}) if isinstance(config.get("paths"), Mapping) else {}
    raw_base_path = str(paths.get("raw_base_path", "data/sample/raw"))
    reference_base_path = str(paths.get("reference_base_path", "data/sample/reference"))
    staging_base_path = str(paths.get("staging_base_path", "data/local/staging"))
    curated_base_path = str(paths.get("curated_base_path", DEFAULT_LOCAL_CURATED_BASE_PATH))

    runtime_config = config.get("runtime", {}) if isinstance(config.get("runtime"), Mapping) else {}
    schema_dir, staging_sql_dir = _resolve_runtime_resource_paths(config)
    quality_config = config.get("quality", {}) if isinstance(config.get("quality"), Mapping) else {}
    io_config = config.get("io", {}) if isinstance(config.get("io"), Mapping) else {}
    hive_config = config.get("hive", {}) if isinstance(config.get("hive"), Mapping) else {}
    spark_config = config.get("spark", {}) if isinstance(config.get("spark"), Mapping) else {}
    invalid_record_write_config = (
        io_config.get("invalid_records", {})
        if isinstance(io_config.get("invalid_records"), Mapping)
        else {}
    )
    curated_write_config = (
        io_config.get("curated_outputs", {})
        if isinstance(io_config.get("curated_outputs"), Mapping)
        else {}
    )
    invalid_record_write_config = {
        **dict(invalid_record_write_config),
        "execution_mode": str(spark_config.get("profile", "local")),
    }
    curated_write_config = {
        **dict(curated_write_config),
        "execution_mode": str(spark_config.get("profile", "local")),
    }

    shipments_source = _resolve_entity_source_path(raw_base_path, "shipments", batch_date)
    carriers_source = _resolve_entity_source_path(raw_base_path, "carriers", batch_date)
    events_source = _resolve_entity_source_path(raw_base_path, "delivery_events", batch_date)
    region_lookup_source = _resolve_region_lookup_path(reference_base_path)

    ingest_bad_path = _join_storage_path(
        staging_base_path, "quarantine", "ingest", f"p_date={batch_date}"
    )
    quality_bad_path = _join_storage_path(
        staging_base_path, "quarantine", "quality", f"p_date={batch_date}"
    )

    logger.info(
        "Resolved inputs shipments=%s carriers=%s delivery_events=%s region_lookup=%s",
        shipments_source,
        carriers_source,
        events_source,
        region_lookup_source,
    )

    shipments_schema = load_shipments_schema_definition(schema_dir / "shipments.schema.json")
    carriers_schema = load_carriers_schema_definition(schema_dir / "carriers.schema.json")
    delivery_events_schema = load_delivery_events_schema_definition(
        schema_dir / "delivery_events.schema.json"
    )

    shipments_raw_df = read_shipments_raw(
        spark=spark,
        source_path=shipments_source,
        bad_records_path=ingest_bad_path,
        schema_def=shipments_schema,
        bad_record_write_config=invalid_record_write_config,
    )
    carriers_raw_df = read_carriers_raw(
        spark=spark,
        source_path=carriers_source,
        bad_records_path=ingest_bad_path,
        schema_def=carriers_schema,
        bad_record_write_config=invalid_record_write_config,
    )
    events_raw_df = read_delivery_events_raw(
        spark=spark,
        source_path=events_source,
        bad_records_path=ingest_bad_path,
        schema_def=delivery_events_schema,
        bad_record_write_config=invalid_record_write_config,
    )

    logger.info("Ingest complete; running quality checks")

    shipments_quality = run_quality_rules(
        {
            "df": shipments_raw_df,
            "entity": "shipments",
            "schema_expected": _schema_for_drift(shipments_schema),
            "required_columns": shipments_schema.get("required_columns"),
            "duplicate_keys": shipments_schema.get("primary_key"),
            "allowed_values": _build_allowed_values(shipments_schema),
            "non_negative_columns": _build_non_negative_columns(shipments_schema),
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
            "primary_key": shipments_schema.get("primary_key"),
            "quarantine_path": quality_bad_path,
            "quarantine_write_config": invalid_record_write_config,
            "fail_fast": False,
            "logger": logger,
        }
    )

    carriers_quality = run_quality_rules(
        {
            "df": carriers_raw_df,
            "entity": "carriers",
            "schema_expected": _schema_for_drift(carriers_schema),
            "required_columns": carriers_schema.get("required_columns"),
            "duplicate_keys": carriers_schema.get("primary_key"),
            "allowed_values": _build_allowed_values(carriers_schema),
            "primary_key": carriers_schema.get("primary_key"),
            "quarantine_path": quality_bad_path,
            "quarantine_write_config": invalid_record_write_config,
            "fail_fast": False,
            "logger": logger,
        }
    )

    events_quality = run_quality_rules(
        {
            "df": events_raw_df,
            "entity": "delivery_events",
            "schema_expected": _schema_for_drift(delivery_events_schema),
            "required_columns": delivery_events_schema.get("required_columns"),
            "duplicate_keys": delivery_events_schema.get("primary_key"),
            "allowed_values": _build_allowed_values(delivery_events_schema),
            "non_negative_columns": _build_non_negative_columns(delivery_events_schema),
            "timestamp_order_rules": [
                {
                    "start_column": "event_ts",
                    "end_column": "updated_at",
                    "allow_equal": True,
                    "allow_null_end": False,
                }
            ],
            "primary_key": delivery_events_schema.get("primary_key"),
            "quarantine_path": quality_bad_path,
            "quarantine_write_config": invalid_record_write_config,
            "fail_fast": False,
            "logger": logger,
        }
    )

    quality_summary = {
        "shipments": shipments_quality,
        "carriers": carriers_quality,
        "delivery_events": events_quality,
    }

    for entity, result in quality_summary.items():
        failed_rules = [str(rule) for rule in result.get("failed_rules", [])]
        invalid_count = int(result.get("invalid_count", 0) or 0)
        blocking = _blocking_quality_failures(
            failed_rules=failed_rules, quality_config=quality_config
        )

        if failed_rules:
            logger.warning(
                "Quality result entity=%s failed_rules=%s invalid_count=%s",
                entity,
                failed_rules,
                invalid_count,
            )
        else:
            logger.info("Quality result entity=%s status=PASS", entity)

        if blocking:
            raise ValueError(f"Blocking quality failures for entity '{entity}': {blocking}")

    region_lookup_df = load_region_lookup(spark=spark, lookup_path=region_lookup_source)

    partitioning = (
        config.get("partitioning", {}) if isinstance(config.get("partitioning"), Mapping) else {}
    )
    partition_keys = partitioning.get("keys")
    if not isinstance(partition_keys, list) or not partition_keys:
        partition_keys = required_partition_columns()
    partition_keys = [str(col) for col in partition_keys]

    write_mode = str(hive_config.get("mode", runtime_config.get("default_write_mode", "overwrite")))
    database = str(hive_config.get("database", "curated"))
    register_hive_tables = bool(
        hive_config.get("register_tables", spark_config.get("enable_hive_support", True))
    )
    repair_partitions = bool(hive_config.get("repair_partitions", True))
    parquet_options = (
        io_config.get("parquet", {}) if isinstance(io_config.get("parquet"), Mapping) else None
    )

    logger.info("Building Bronze layer outputs")

    from transport_etl.bronze.builder import (
        build_bronze_carriers,
        build_bronze_delivery_events,
        build_bronze_shipments,
    )
    from transport_etl.bronze.publisher import (
        TABLE_BRONZE_CARRIERS,
        TABLE_BRONZE_DELIVERY_EVENTS,
        TABLE_BRONZE_SHIPMENTS,
        publish_bronze_table,
    )

    bronze_batch_id = f"daily_{batch_date}"
    bronze_writes: dict[str, str] = {}
    bronze_dfs: dict[str, Any] = {}
    bronze_writer_config = {
        **dict(curated_write_config),
        "execution_mode": str(spark_config.get("profile", "local")),
    }

    for entity, source_path, builder in (
        (TABLE_BRONZE_SHIPMENTS, shipments_source, build_bronze_shipments),
        (TABLE_BRONZE_CARRIERS, carriers_source, build_bronze_carriers),
        (TABLE_BRONZE_DELIVERY_EVENTS, events_source, build_bronze_delivery_events),
    ):
        bronze_df = builder(
            spark=spark,
            source_path=source_path,
            run_date=batch_date,
            batch_id=bronze_batch_id,
            job_name="daily",
            quarantine_path=_join_storage_path(staging_base_path, "quarantine", "ingest"),
            bad_record_write_config=invalid_record_write_config,
            schema_def={
                TABLE_BRONZE_SHIPMENTS: shipments_schema,
                TABLE_BRONZE_CARRIERS: carriers_schema,
                TABLE_BRONZE_DELIVERY_EVENTS: delivery_events_schema,
            }[entity],
        )
        bronze_writes[entity] = publish_bronze_table(
            df=bronze_df,
            config=config,
            table_name=entity,
            spark=spark,
            mode=write_mode,
            writer_options=parquet_options,
            write_config=bronze_writer_config,
            logger=logger,
        )
        bronze_dfs[entity] = bronze_df

    bronze_writes_shipments_df = bronze_dfs[TABLE_BRONZE_SHIPMENTS]
    bronze_writes_carriers_df = bronze_dfs[TABLE_BRONZE_CARRIERS]
    bronze_writes_events_df = bronze_dfs[TABLE_BRONZE_DELIVERY_EVENTS]

    logger.info("Bronze layer published destinations=%s", bronze_writes)

    logger.info("Building Silver layer outputs")

    from transport_etl.ingest.carriers import load_carriers_schema_definition
    from transport_etl.ingest.delivery_events import load_delivery_events_schema_definition
    from transport_etl.ingest.shipments import load_shipments_schema_definition
    from transport_etl.silver.builder import (
        build_silver_carriers,
        build_silver_delivery_events,
        build_silver_shipments,
    )
    from transport_etl.silver.publisher import (
        TABLE_SILVER_CARRIERS,
        TABLE_SILVER_DELIVERY_EVENTS,
        TABLE_SILVER_SHIPMENTS,
        publish_silver_table,
    )

    silver_batch_id = f"daily_{batch_date}"
    silver_writes: dict[str, str] = {}
    silver_quarantine_path = _join_storage_path(
        staging_base_path, "quarantine", "silver", f"p_date={batch_date}"
    )
    silver_writer_config = {
        **dict(curated_write_config),
        "execution_mode": str(spark_config.get("profile", "local")),
    }

    silver_shipments_schema = shipments_schema
    silver_carriers_schema = carriers_schema
    silver_delivery_events_schema = delivery_events_schema

    silver_shipments_result = build_silver_shipments(
        spark=spark,
        bronze_df=bronze_writes_shipments_df,
        schema_def=silver_shipments_schema,
        batch_id=silver_batch_id,
        run_date=batch_date,
        region_lookup_df=region_lookup_df,
        quarantine_path=silver_quarantine_path,
        write_config=silver_writer_config,
        logger=logger,
    )
    silver_writes[TABLE_SILVER_SHIPMENTS] = publish_silver_table(
        df=silver_shipments_result.silver_df,
        config=config,
        table_name=TABLE_SILVER_SHIPMENTS,
        spark=spark,
        mode=write_mode,
        writer_options=parquet_options,
        write_config=silver_writer_config,
        logger=logger,
    )

    silver_carriers_result = build_silver_carriers(
        spark=spark,
        bronze_df=bronze_writes_carriers_df,
        schema_def=silver_carriers_schema,
        batch_id=silver_batch_id,
        run_date=batch_date,
        region_lookup_df=region_lookup_df,
        quarantine_path=silver_quarantine_path,
        write_config=silver_writer_config,
        logger=logger,
    )
    silver_writes[TABLE_SILVER_CARRIERS] = publish_silver_table(
        df=silver_carriers_result.silver_df,
        config=config,
        table_name=TABLE_SILVER_CARRIERS,
        spark=spark,
        mode=write_mode,
        writer_options=parquet_options,
        write_config=silver_writer_config,
        logger=logger,
    )

    silver_delivery_events_result = build_silver_delivery_events(
        spark=spark,
        bronze_df=bronze_writes_events_df,
        schema_def=silver_delivery_events_schema,
        batch_id=silver_batch_id,
        run_date=batch_date,
        region_lookup_df=region_lookup_df,
        quarantine_path=silver_quarantine_path,
        write_config=silver_writer_config,
        logger=logger,
    )
    silver_writes[TABLE_SILVER_DELIVERY_EVENTS] = publish_silver_table(
        df=silver_delivery_events_result.silver_df,
        config=config,
        table_name=TABLE_SILVER_DELIVERY_EVENTS,
        spark=spark,
        mode=write_mode,
        writer_options=parquet_options,
        write_config=silver_writer_config,
        logger=logger,
    )

    silver_summary = {
        TABLE_SILVER_SHIPMENTS: {
            "survived": silver_shipments_result.survived_count,
            "invalid": silver_shipments_result.invalid_count,
        },
        TABLE_SILVER_CARRIERS: {
            "survived": silver_carriers_result.survived_count,
            "invalid": silver_carriers_result.invalid_count,
        },
        TABLE_SILVER_DELIVERY_EVENTS: {
            "survived": silver_delivery_events_result.survived_count,
            "invalid": silver_delivery_events_result.invalid_count,
        },
    }
    logger.info(
        "Silver layer published destinations=%s summary=%s",
        silver_writes,
        silver_summary,
    )

    # Build Gold from canonical Silver outputs so Silver deduplication
    # and quarantine decisions govern downstream analytics.
    silver_shipments_df = _run_staging_sql(
        spark=spark,
        sql_dir=staging_sql_dir,
        sql_file_name="stg_shipments.sql",
        raw_view_name="raw_shipments",
        staging_view_name="stg_shipments",
        input_df=silver_shipments_result.silver_df,
    )
    silver_carriers_df = _run_staging_sql(
        spark=spark,
        sql_dir=staging_sql_dir,
        sql_file_name="stg_carriers.sql",
        raw_view_name="raw_carriers",
        staging_view_name="stg_carriers",
        input_df=silver_carriers_result.silver_df,
    )
    silver_events_df = _run_staging_sql(
        spark=spark,
        sql_dir=staging_sql_dir,
        sql_file_name="stg_delivery_events.sql",
        raw_view_name="raw_delivery_events",
        staging_view_name="stg_delivery_events",
        input_df=silver_delivery_events_result.silver_df,
    )

    silver_shipments_df = enrich_shipments_with_region(
        shipments_df=standardize_columns(silver_shipments_df),
        region_lookup_df=region_lookup_df,
    )
    silver_carriers_df = standardize_columns(silver_carriers_df)
    silver_events_df = enrich_delivery_events_with_region(
        events_df=standardize_columns(silver_events_df),
        region_lookup_df=region_lookup_df,
    )

    silver_shipments_df.createOrReplaceTempView("stg_shipments")
    silver_carriers_df.createOrReplaceTempView("stg_carriers")
    silver_events_df.createOrReplaceTempView("stg_delivery_events")

    dim_carrier_df = build_dim_carrier(
        stg_carriers_df=silver_carriers_df,
        run_date=batch_date,
    )
    fct_shipment_df = build_fct_shipment(
        stg_shipments_df=silver_shipments_df,
        region_lookup_df=region_lookup_df,
    )
    fct_delivery_event_df = build_fct_delivery_event(
        stg_events_df=silver_events_df,
        stg_shipments_df=silver_shipments_df,
        region_lookup_df=region_lookup_df,
    )
    agg_shipment_daily_df = build_agg_shipment_daily(fct_shipment_df=fct_shipment_df)
    kpi_delivery_daily_df = build_kpi_delivery_daily(
        agg_shipment_daily_df=agg_shipment_daily_df,
        fct_delivery_event_df=fct_delivery_event_df,
    )

    from transport_etl.transform.build_carrier_performance import (
        build_carrier_performance,
    )
    from transport_etl.transform.build_delivery_exception_summary import (
        build_delivery_exception_summary,
    )
    from transport_etl.transform.build_route_performance import (
        build_route_performance,
    )

    carrier_performance_df = build_carrier_performance(
        fct_shipment_df=fct_shipment_df,
        fct_delivery_event_df=fct_delivery_event_df,
        dim_carrier_df=dim_carrier_df,
    )
    route_performance_df = build_route_performance(fct_shipment_df=fct_shipment_df)
    delivery_exception_summary_df = build_delivery_exception_summary(
        fct_delivery_event_df=fct_delivery_event_df,
        fct_shipment_df=fct_shipment_df,
    )

    logger.info(
        "Gold analytics built from Silver: carrier_performance=%s, "
        "route_performance=%s, delivery_exception_summary=%s",
        _safe_count(carrier_performance_df),
        _safe_count(route_performance_df),
        _safe_count(delivery_exception_summary_df),
    )

    outputs: dict[str, str] = {}
    table_writes = [
        (TABLE_DIM_CARRIER, dim_carrier_df),
        (TABLE_FCT_SHIPMENT, fct_shipment_df),
        (TABLE_FCT_DELIVERY_EVENT, fct_delivery_event_df),
        (TABLE_AGG_SHIPMENT_DAILY, agg_shipment_daily_df),
        (TABLE_KPI_DELIVERY_DAILY, kpi_delivery_daily_df),
        (
            TABLE_GOLD_CARRIER_PERFORMANCE,
            carrier_performance_df,
        ),
        (
            TABLE_GOLD_ROUTE_PERFORMANCE,
            route_performance_df,
        ),
        (
            TABLE_GOLD_DELIVERY_EXCEPTION_SUMMARY,
            delivery_exception_summary_df,
        ),
    ]

    for table_name, dataframe in table_writes:
        output_path = _join_storage_path(curated_base_path, table_name)
        resolved_table, output_format = _resolve_gold_write_target(config, table_name)
        written_path = write_partitioned_table(
            df=dataframe,
            table_name=resolved_table,
            output_path=output_path,
            partitions=partition_keys,
            mode=write_mode,
            spark=spark,
            database=database,
            register_hive_table=register_hive_tables,
            repair_partitions=repair_partitions,
            writer_options=parquet_options,
            write_config=curated_write_config,
            logger=logger,
            output_format=output_format,
        )
        outputs[table_name] = written_path

    return {
        "batch_date": batch_date,
        "outputs": outputs,
        "bronze": bronze_writes,
        "silver": silver_writes,
        "silver_summary": silver_summary,
        "quality": {
            entity: {
                "status": result.get("status"),
                "failed_rules": result.get("failed_rules", []),
                "invalid_count": int(result.get("invalid_count", 0) or 0),
            }
            for entity, result in quality_summary.items()
        },
    }


def run_daily_batch(
    config_path: str,
    run_date: str | None = None,
    overrides: Mapping[str, Any] | None = None,
    config_dir: str | Path | None = None,
) -> int:
    """Run the end-to-end daily ETL workflow."""
    loaded_config = (
        load_config(config_path, config_dir=config_dir)
        if config_dir is not None
        else load_config(config_path)
    )
    config = _apply_overrides(loaded_config, overrides)

    logging_config = config.get("logging", {}) if isinstance(config.get("logging"), Mapping) else {}
    configure_logging(
        level=str(logging_config.get("level", "INFO")),
        json_logs=bool(logging_config.get("json", False)),
    )

    paths = config.get("paths", {}) if isinstance(config.get("paths"), Mapping) else {}
    raw_base_path = str(paths.get("raw_base_path", "data/sample/raw"))
    batch_date = _resolve_batch_date(run_date=run_date, raw_base_path=raw_base_path)
    run_id = f"daily_{batch_date}_{uuid.uuid4().hex[:8]}"

    logger = get_logger(
        "transport_etl.jobs.daily",
        run_id=run_id,
        job="daily",
        env=str(config.get("app", {}).get("env", "unknown")),
        batch_date=batch_date,
    )

    if run_date is None and batch_date != resolve_run_date(None):
        logger.info("No run_date supplied; discovered local batch_date=%s", batch_date)

    logger.info("Daily batch started")

    spark = None
    try:
        spark = create_spark_session_from_config(config=config)
        result = _execute_daily_flow(
            spark=spark, config=config, batch_date=batch_date, logger=logger
        )
        logger.info("Daily batch completed successfully; outputs=%s", result.get("outputs"))
        return 0
    except ModuleNotFoundError as exc:
        logger.error("Missing runtime dependency: %s", exc)
        return 2
    except Exception:
        logger.exception("Daily batch failed")
        return 1
    finally:
        stop_spark_session(spark)
