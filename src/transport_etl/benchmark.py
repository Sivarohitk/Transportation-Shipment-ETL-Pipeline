"""Measure one real daily ETL run and persist its observed audit counts."""

from __future__ import annotations

import argparse
import json
import os
import platform
import tempfile
import time
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Mapping

from transport_etl.common.config import load_config
from transport_etl.common.spark import create_spark_session_from_config, stop_spark_session
from transport_etl.jobs.run_daily_batch import _apply_overrides, run_daily_batch
from transport_etl.monitor.metrics import NoOpMetricsSink

SCHEMA_VERSION = 1
SPARK_CONFIGURATION_KEYS = (
    "spark.sql.shuffle.partitions",
    "spark.sql.adaptive.enabled",
    "spark.sql.adaptive.coalescePartitions.enabled",
    "spark.sql.adaptive.skewJoin.enabled",
    "spark.sql.autoBroadcastJoinThreshold",
    "spark.sql.parquet.compression.codec",
    "spark.sql.sources.partitionOverwriteMode",
)


class CapturingAuditStore:
    """Keep the job's real final audit record in process for one benchmark run."""

    def __init__(self) -> None:
        self.record: dict[str, Any] | None = None

    def write(self, record: Mapping[str, Any]) -> str:
        self.record = dict(record)
        return "benchmark-memory"


def build_benchmark_result(
    audit: Mapping[str, Any],
    *,
    elapsed_seconds: float,
    runtime_profile: str,
    spark_version: str,
    spark_configuration: Mapping[str, str],
    output_format: str,
) -> dict[str, Any]:
    """Build a stable JSON result from observed audit counts and wall time."""
    if elapsed_seconds <= 0:
        raise ValueError("elapsed_seconds must be positive")
    source = {str(key): int(value) for key, value in audit.get("source_rows", {}).items()}
    curated = {str(key): int(value) for key, value in audit.get("curated_rows", {}).items()}
    rejected = {str(key): int(value) for key, value in audit.get("rejected_rows", {}).items()}
    if any(value < 0 for value in (*source.values(), *curated.values(), *rejected.values())):
        raise ValueError("Benchmark row counts must be nonnegative")
    input_rows = sum(source.values())
    return {
        "schema_version": SCHEMA_VERSION,
        "synthetic_input": True,
        "measured_at_utc": datetime.now(timezone.utc).isoformat(),
        "run_id": str(audit.get("run_id", "")),
        "batch_date": audit.get("batch_date"),
        "environment": str(audit.get("environment", "unknown")),
        "runtime_profile": runtime_profile,
        "status": str(audit.get("status", "unknown")),
        "scope": "daily Spark ETL; pipeline state, cloud publishers, and Hive disabled",
        "source_rows": source,
        "curated_rows": curated,
        "rejected_rows_by_entity": rejected,
        "input_rows": input_rows,
        "output_rows": sum(curated.values()),
        "output_row_scope": "five core Gold audit counts",
        "rejected_rows": sum(rejected.values()),
        "elapsed_seconds": round(elapsed_seconds, 3),
        "input_rows_per_second": round(input_rows / elapsed_seconds, 3),
        "audit_duration_seconds": audit.get("duration_seconds"),
        "spark_version": spark_version,
        "python_version": platform.python_version(),
        "host_platform": platform.platform(),
        "spark_configuration": dict(spark_configuration),
        "output_format": output_format,
        "error_type": audit.get("error_type"),
    }


def write_benchmark_result(path: str | Path, result: Mapping[str, Any]) -> None:
    """Write the machine-readable result atomically to a local JSON file."""
    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary: str | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w", encoding="utf-8", dir=destination.parent, suffix=".tmp", delete=False
        ) as handle:
            temporary = handle.name
            json.dump(dict(result), handle, sort_keys=True, indent=2)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, destination)
    finally:
        if temporary and os.path.exists(temporary):
            os.unlink(temporary)


def _spark_configuration(spark: Any) -> dict[str, str]:
    values: dict[str, str] = {}
    for key in SPARK_CONFIGURATION_KEYS:
        try:
            values[key] = str(spark.conf.get(key))
        except Exception:
            values[key] = "unavailable"
    return values


def _output_format(audit: Mapping[str, Any]) -> str:
    locations = [str(value) for value in audit.get("outputs", {}).values()]
    if not locations:
        return "not_written"
    if any("_fallback_json" in location for location in locations):
        return "json_fallback"
    if any("_fallback_csv" in location for location in locations):
        return "csv_fallback"
    return "parquet"


def main(argv: list[str] | None = None) -> int:
    """Run the shared daily job once with state and remote publishers disabled."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", default="dev")
    parser.add_argument("--run-date", required=True, type=date.fromisoformat)
    parser.add_argument("--raw-base-path", default="data/generated/scale/raw")
    parser.add_argument("--reference-base-path", default="data/generated/scale/reference")
    parser.add_argument("--staging-base-path", default="data/generated/benchmark/staging")
    parser.add_argument("--curated-base-path", default="data/generated/benchmark/curated")
    parser.add_argument("--audit-base-path", default="data/generated/benchmark/audit")
    parser.add_argument(
        "--result-json", type=Path, default=Path("data/generated/benchmark/result.json")
    )
    args = parser.parse_args(argv)
    overrides = {
        "paths.raw_base_path": args.raw_base_path,
        "paths.reference_base_path": args.reference_base_path,
        "paths.staging_base_path": args.staging_base_path,
        "paths.curated_base_path": args.curated_base_path,
        "paths.audit_base_path": args.audit_base_path,
        "pipeline_state.enabled": False,
        "audit.enabled": False,
        "cloudwatch.enabled": False,
        "glue.enabled": False,
        "redshift.enabled": False,
        "hive.register_tables": False,
    }
    config = _apply_overrides(load_config(args.config), overrides)
    profile = str(config.get("spark", {}).get("profile", "unknown"))
    if profile not in {"local", "emr"}:
        parser.error("This CLI measures local/EMR Spark; use the managed job entrypoint for Glue")
    audit_store = CapturingAuditStore()
    start = time.perf_counter()
    spark = create_spark_session_from_config(config=config)
    try:
        spark_conf = _spark_configuration(spark)
        spark_version = str(spark.version)
        exit_code = run_daily_batch(
            config_path=args.config,
            run_date=args.run_date.isoformat(),
            overrides=overrides,
            spark_session=spark,
            audit_store=audit_store,
            metrics_sink=NoOpMetricsSink(),
        )
    finally:
        stop_spark_session(spark)
    elapsed = time.perf_counter() - start
    if audit_store.record is None:
        raise RuntimeError("Daily run did not produce an audit record")
    result = build_benchmark_result(
        audit_store.record,
        elapsed_seconds=elapsed,
        runtime_profile=profile,
        spark_version=spark_version,
        spark_configuration=spark_conf,
        output_format=_output_format(audit_store.record),
    )
    write_benchmark_result(args.result_json, result)
    print(
        f"status={result['status']} profile={profile} input={result['input_rows']} "
        f"output={result['output_rows']} rejected={result['rejected_rows']} "
        f"elapsed={result['elapsed_seconds']:.3f}s "
        f"throughput={result['input_rows_per_second']:.3f} input rows/s "
        f"format={result['output_format']} result={args.result_json}"
    )
    return exit_code


if __name__ == "__main__":  # pragma: no cover - exercised through main()
    raise SystemExit(main())
