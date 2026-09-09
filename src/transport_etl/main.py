"""CLI entry point for Transportation Shipment ETL."""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any, Sequence

from transport_etl.common.constants import (
    SPARK_PROFILE_DATABRICKS,
    SPARK_PROFILE_EMR,
    SPARK_PROFILE_LOCAL,
)
from transport_etl.jobs.run_backfill_batch import run_backfill_batch
from transport_etl.jobs.run_daily_batch import run_daily_batch


def build_parser() -> argparse.ArgumentParser:
    """Build command-line parser for ETL job dispatch."""
    parser = argparse.ArgumentParser(description="Transportation Shipment ETL")
    parser.add_argument("--job", choices=["daily", "backfill"], default="daily")
    parser.add_argument(
        "--config",
        default="dev",
        help="Config path or shorthand name (base, dev, prod)",
    )
    parser.add_argument(
        "--config-dir",
        type=Path,
        default=None,
        help="Directory containing base.yaml and environment config files",
    )
    parser.add_argument("--run-date", default=None, help="Daily run date in YYYY-MM-DD")
    parser.add_argument("--start-date", default=None, help="Backfill start date in YYYY-MM-DD")
    parser.add_argument("--end-date", default=None, help="Backfill end date in YYYY-MM-DD")

    parser.add_argument("--raw-base-path", default=None, help="Override paths.raw_base_path")
    parser.add_argument(
        "--reference-base-path", default=None, help="Override paths.reference_base_path"
    )
    parser.add_argument(
        "--staging-base-path", default=None, help="Override paths.staging_base_path"
    )
    parser.add_argument(
        "--curated-base-path", default=None, help="Override paths.curated_base_path"
    )
    parser.add_argument(
        "--resource-base-path",
        default=None,
        help="Override runtime.resource_base_path for schema and SQL assets",
    )
    parser.add_argument(
        "--spark-profile",
        choices=(SPARK_PROFILE_LOCAL, SPARK_PROFILE_EMR, SPARK_PROFILE_DATABRICKS),
        default=None,
        help="Override spark.profile",
    )
    parser.add_argument("--hive-database", default=None, help="Override hive.database")
    parser.add_argument("--catalog", default=None, help="Override unity_catalog.catalog")
    parser.add_argument(
        "--bronze-schema", default=None, help="Override unity_catalog.bronze_schema"
    )
    parser.add_argument(
        "--silver-schema", default=None, help="Override unity_catalog.silver_schema"
    )
    parser.add_argument("--gold-schema", default=None, help="Override unity_catalog.gold_schema")
    parser.add_argument(
        "--fail-fast",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Override runtime.fail_fast",
    )
    parser.add_argument(
        "--register-hive",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Override hive.register_tables",
    )
    parser.add_argument(
        "--raise-on-error",
        action="store_true",
        help="Raise when a job returns non-zero so managed runtimes record failure",
    )
    return parser


def _build_overrides(args: argparse.Namespace) -> dict[str, Any]:
    """Build config override payload from parsed CLI arguments."""
    overrides: dict[str, Any] = {}

    if args.raw_base_path:
        overrides["paths.raw_base_path"] = args.raw_base_path
    if args.reference_base_path:
        overrides["paths.reference_base_path"] = args.reference_base_path
    if args.staging_base_path:
        overrides["paths.staging_base_path"] = args.staging_base_path
    if args.curated_base_path:
        overrides["paths.curated_base_path"] = args.curated_base_path
    if args.resource_base_path:
        overrides["runtime.resource_base_path"] = args.resource_base_path
    if args.spark_profile:
        overrides["spark.profile"] = args.spark_profile
    if args.hive_database:
        overrides["hive.database"] = args.hive_database
    if args.catalog:
        overrides["unity_catalog.catalog"] = args.catalog
    if args.bronze_schema:
        overrides["unity_catalog.bronze_schema"] = args.bronze_schema
    if args.silver_schema:
        overrides["unity_catalog.silver_schema"] = args.silver_schema
    if args.gold_schema:
        overrides["unity_catalog.gold_schema"] = args.gold_schema
    if args.fail_fast is not None:
        overrides["runtime.fail_fast"] = bool(args.fail_fast)
    if args.register_hive is not None:
        overrides["hive.register_tables"] = bool(args.register_hive)

    return overrides


def main(argv: Sequence[str] | None = None) -> int:
    """Execute selected ETL job."""
    parser = build_parser()
    args = parser.parse_args(argv)
    overrides = _build_overrides(args)

    if args.job == "daily":
        status = run_daily_batch(
            config_path=args.config,
            config_dir=args.config_dir,
            run_date=args.run_date,
            overrides=overrides or None,
        )
    else:
        status = run_backfill_batch(
            config_path=args.config,
            config_dir=args.config_dir,
            start_date=args.start_date,
            end_date=args.end_date,
            overrides=overrides or None,
        )

    if args.raise_on_error and status != 0:
        raise RuntimeError(f"ETL job returned non-zero status {status}")
    return status


if __name__ == "__main__":
    raise SystemExit(main())
