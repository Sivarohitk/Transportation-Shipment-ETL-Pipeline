"""CLI entry point for Transportation Shipment ETL."""

from __future__ import annotations

import argparse
from typing import Any, Sequence

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
        "--spark-profile", choices=["local", "emr"], default=None, help="Override spark.profile"
    )
    parser.add_argument("--hive-database", default=None, help="Override hive.database")
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
    if args.spark_profile:
        overrides["spark.profile"] = args.spark_profile
    if args.hive_database:
        overrides["hive.database"] = args.hive_database
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
        return run_daily_batch(
            config_path=args.config,
            run_date=args.run_date,
            overrides=overrides or None,
        )

    return run_backfill_batch(
        config_path=args.config,
        start_date=args.start_date,
        end_date=args.end_date,
        overrides=overrides or None,
    )


if __name__ == "__main__":
    raise SystemExit(main())
