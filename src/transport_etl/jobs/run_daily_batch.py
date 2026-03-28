"""Daily batch job orchestration."""

from __future__ import annotations

import copy
import re
import uuid
from pathlib import Path
from typing import Any, Mapping

from transport_etl.common.config import load_config
from transport_etl.common.constants import (
    TABLE_AGG_SHIPMENT_DAILY,
    TABLE_DIM_CARRIER,
    TABLE_FCT_DELIVERY_EVENT,
    TABLE_FCT_SHIPMENT,
    TABLE_KPI_DELIVERY_DAILY,
)
from transport_etl.common.dates import DATE_FMT, parse_date, resolve_run_date
from transport_etl.common.io import is_cloud_path, path_exists
from transport_etl.common.logging import configure_logging, get_logger
from transport_etl.common.spark import create_spark_session_from_config, stop_spark_session
from transport_etl.publish.hive_writer import write_partitioned_table
from transport_etl.publish.partitions import required_partition_columns

PROJECT_ROOT = Path(__file__).resolve().parents[3]
SQL_STAGING_DIR = PROJECT_ROOT / "sql" / "staging"
_DATED_CSV_PATTERN = re.compile(r"_(\d{4}-\d{2}-\d{2})\.csv$")
_RAW_ENTITIES = ("shipments", "carriers", "delivery_events")


def _join_storage_path(base_path: str, *parts: str) -> str:
    """Join paths for both local filesystems and cloud object storage URIs."""
    cleaned_parts = [part.strip("/\\") for part in parts if part and part.strip("/\\")]
    if is_cloud_path(base_path):
        base = base_path.rstrip("/\\")
        return "/".join([base] + cleaned_parts)
    return str(Path(base_path, *cleaned_parts))


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

    if not is_cloud_path(raw_base_path):
        candidates = sorted(Path(raw_base_path).glob(f"{entity}_*.csv"), reverse=True)
        if candidates:
            return str(candidates[0])

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


def _resolve_clean_df(quality_result: Mapping[str, Any], fallback_df: Any) -> Any:
    """Return quality-cleaned DataFrame when available, else the fallback DataFrame."""
    failed_rules = {str(rule) for rule in quality_result.get("failed_rules", [])}
    if failed_rules and failed_rules.issubset({"duplicate_keys"}):
        # Keep duplicate rows for downstream deterministic dedupe logic.
        return fallback_df

    cleaned = quality_result.get("clean_df")
    return cleaned if cleaned is not None else fallback_df


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
    sql_file_name: str,
    raw_view_name: str,
    staging_view_name: str,
    input_df: Any,
) -> Any:
    """Run a staging SQL file against a supplied raw temp view."""
    input_df.createOrReplaceTempView(raw_view_name)
    query = _load_sql_file(SQL_STAGING_DIR / sql_file_name)
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
    curated_base_path = str(paths.get("curated_base_path", "data/local/curated"))

    runtime_config = config.get("runtime", {}) if isinstance(config.get("runtime"), Mapping) else {}
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

    runtime_fail_fast = bool(runtime_config.get("fail_fast", True))

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

    shipments_schema = load_shipments_schema_definition()
    carriers_schema = load_carriers_schema_definition()
    delivery_events_schema = load_delivery_events_schema_definition()

    shipments_raw_df = read_shipments_raw(
        spark=spark,
        source_path=shipments_source,
        bad_records_path=ingest_bad_path,
        bad_record_write_config=invalid_record_write_config,
    )
    carriers_raw_df = read_carriers_raw(
        spark=spark,
        source_path=carriers_source,
        bad_records_path=ingest_bad_path,
        bad_record_write_config=invalid_record_write_config,
    )
    events_raw_df = read_delivery_events_raw(
        spark=spark,
        source_path=events_source,
        bad_records_path=ingest_bad_path,
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
            "fail_fast": runtime_fail_fast,
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
            "fail_fast": runtime_fail_fast,
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
            "fail_fast": runtime_fail_fast,
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

        if blocking and runtime_fail_fast:
            raise ValueError(f"Blocking quality failures for entity '{entity}': {blocking}")

    shipments_clean_df = _resolve_clean_df(shipments_quality, shipments_raw_df)
    carriers_clean_df = _resolve_clean_df(carriers_quality, carriers_raw_df)
    events_clean_df = _resolve_clean_df(events_quality, events_raw_df)

    stg_shipments_df = _run_staging_sql(
        spark=spark,
        sql_file_name="stg_shipments.sql",
        raw_view_name="raw_shipments",
        staging_view_name="stg_shipments",
        input_df=shipments_clean_df,
    )
    stg_carriers_df = _run_staging_sql(
        spark=spark,
        sql_file_name="stg_carriers.sql",
        raw_view_name="raw_carriers",
        staging_view_name="stg_carriers",
        input_df=carriers_clean_df,
    )
    stg_events_df = _run_staging_sql(
        spark=spark,
        sql_file_name="stg_delivery_events.sql",
        raw_view_name="raw_delivery_events",
        staging_view_name="stg_delivery_events",
        input_df=events_clean_df,
    )

    logger.info("Staging SQL complete; running standardization and region enrichment")

    stg_shipments_std_df = standardize_columns(stg_shipments_df)
    stg_carriers_std_df = standardize_columns(stg_carriers_df)
    stg_events_std_df = standardize_columns(stg_events_df)

    region_lookup_df = load_region_lookup(spark=spark, lookup_path=region_lookup_source)

    stg_shipments_enriched_df = enrich_shipments_with_region(
        shipments_df=stg_shipments_std_df,
        region_lookup_df=region_lookup_df,
    )
    stg_events_enriched_df = enrich_delivery_events_with_region(
        events_df=stg_events_std_df,
        region_lookup_df=region_lookup_df,
    )

    stg_shipments_enriched_df.createOrReplaceTempView("stg_shipments")
    stg_carriers_std_df.createOrReplaceTempView("stg_carriers")
    stg_events_enriched_df.createOrReplaceTempView("stg_delivery_events")

    dim_carrier_df = build_dim_carrier(stg_carriers_df=stg_carriers_std_df, run_date=batch_date)
    fct_shipment_df = build_fct_shipment(
        stg_shipments_df=stg_shipments_enriched_df,
        region_lookup_df=region_lookup_df,
    )
    fct_delivery_event_df = build_fct_delivery_event(
        stg_events_df=stg_events_enriched_df,
        stg_shipments_df=stg_shipments_enriched_df,
        region_lookup_df=region_lookup_df,
    )
    agg_shipment_daily_df = build_agg_shipment_daily(fct_shipment_df=fct_shipment_df)
    kpi_delivery_daily_df = build_kpi_delivery_daily(
        agg_shipment_daily_df=agg_shipment_daily_df,
        fct_delivery_event_df=fct_delivery_event_df,
    )

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

    outputs: dict[str, str] = {}
    table_writes = [
        (TABLE_DIM_CARRIER, dim_carrier_df),
        (TABLE_FCT_SHIPMENT, fct_shipment_df),
        (TABLE_FCT_DELIVERY_EVENT, fct_delivery_event_df),
        (TABLE_AGG_SHIPMENT_DAILY, agg_shipment_daily_df),
        (TABLE_KPI_DELIVERY_DAILY, kpi_delivery_daily_df),
    ]

    for table_name, dataframe in table_writes:
        output_path = _join_storage_path(curated_base_path, table_name)
        written_path = write_partitioned_table(
            df=dataframe,
            table_name=table_name,
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
        )
        outputs[table_name] = written_path

    return {
        "batch_date": batch_date,
        "outputs": outputs,
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
) -> int:
    """Run the end-to-end daily ETL workflow."""
    loaded_config = load_config(config_path)
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
