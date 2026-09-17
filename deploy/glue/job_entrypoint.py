"""AWS Glue Spark entrypoint for the shared transport_etl daily batch."""

from __future__ import annotations

import argparse
import sys
import tempfile
import zipfile
from contextlib import ExitStack
from datetime import date
from pathlib import Path, PurePosixPath
from typing import Any, Callable, Sequence
from urllib.parse import urlsplit

from transport_etl.common.config import load_config
from transport_etl.jobs.run_daily_batch import _apply_overrides, run_daily_batch
from transport_etl.publish.glue_catalog import GlueCatalogConfig
from transport_etl.publish.redshift import RedshiftConfig


def _boolean(value: str) -> bool:
    normalized = value.strip().lower()
    if normalized in {"true", "1", "yes"}:
        return True
    if normalized in {"false", "0", "no"}:
        return False
    raise argparse.ArgumentTypeError("Expected true or false")


def parse_job_arguments(argv: Sequence[str]) -> argparse.Namespace:
    """Parse owned parameters while tolerating Glue's injected system arguments."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--JOB_NAME", required=True)
    parser.add_argument("--run_date", required=True, help="Batch date in YYYY-MM-DD")
    parser.add_argument("--config", default="glue", help="Config name inside the resources archive")
    parser.add_argument("--resources_archive", default="transport_etl_resources.zip")
    for name in (
        "raw_base_path",
        "reference_base_path",
        "staging_base_path",
        "curated_base_path",
        "audit_base_path",
        "glue_region",
        "glue_database",
        "glue_catalog_id",
        "redshift_region",
        "redshift_database",
        "redshift_workgroup_name",
        "redshift_cluster_identifier",
        "redshift_secret_arn",
        "redshift_database_user",
        "redshift_iam_role_arn",
        "redshift_source_s3_path",
        "redshift_staging_schema",
        "redshift_target_schema",
        "redshift_audit_schema",
    ):
        parser.add_argument(f"--{name}")
    parser.add_argument("--glue_enabled", type=_boolean)
    parser.add_argument("--glue_failure_policy", choices=("fail", "warn"))
    parser.add_argument("--redshift_enabled", type=_boolean)
    parser.add_argument("--force", type=_boolean)
    args, _glue_system_args = parser.parse_known_args(list(argv))
    try:
        if date.fromisoformat(args.run_date).isoformat() != args.run_date:
            raise ValueError
    except ValueError:
        parser.error("--run_date must be a real date in YYYY-MM-DD format")
    return args


def extract_resources(archive_path: str | Path, destination: str | Path) -> Path:
    """Unpack the driver resource archive, rejecting traversal and invalid layout."""
    destination = Path(destination)
    with zipfile.ZipFile(archive_path) as archive:
        for item in archive.infolist():
            relative = PurePosixPath(item.filename)
            if (
                relative.is_absolute()
                or ".." in relative.parts
                or "\\" in item.filename
                or ":" in item.filename
            ):
                raise ValueError("Unsafe Glue resource archive member")
        archive.extractall(destination)
    if (
        not (destination / "config" / "base.yaml").is_file()
        or not (destination / "sql" / "staging" / "stg_shipments.sql").is_file()
    ):
        raise ValueError("Glue resource archive lacks required config or SQL files")
    return destination


def _overrides(args: argparse.Namespace, resource_root: Path) -> dict[str, Any]:
    overrides: dict[str, Any] = {
        "runtime.resource_base_path": str(resource_root),
        "spark.profile": "glue",
    }
    for name in (
        "raw_base_path",
        "reference_base_path",
        "staging_base_path",
        "curated_base_path",
        "audit_base_path",
    ):
        value = getattr(args, name)
        if value is not None:
            if not value.startswith("s3://"):
                raise ValueError(f"--{name} must be an s3:// path")
            overrides[f"paths.{name}"] = value
    for name in ("glue_region", "glue_database", "glue_catalog_id", "glue_failure_policy"):
        value = getattr(args, name)
        if value is not None:
            overrides[name.replace("glue_", "glue.", 1)] = value
    for name in (
        "redshift_region",
        "redshift_database",
        "redshift_workgroup_name",
        "redshift_cluster_identifier",
        "redshift_secret_arn",
        "redshift_database_user",
        "redshift_iam_role_arn",
        "redshift_source_s3_path",
        "redshift_staging_schema",
        "redshift_target_schema",
        "redshift_audit_schema",
    ):
        value = getattr(args, name)
        if value is not None:
            overrides[name.replace("redshift_", "redshift.", 1)] = value
    if args.glue_enabled is not None:
        overrides["glue.enabled"] = args.glue_enabled
    if args.redshift_enabled is not None:
        overrides["redshift.enabled"] = args.redshift_enabled
    if args.force is True:
        overrides["pipeline_state.force"] = True
    return overrides


def _validate_deployment_config(config: dict[str, Any]) -> None:
    """Reject example/unresolved storage and optional publisher settings pre-Spark."""
    paths = config.get("paths", {})
    if not isinstance(paths, dict):
        raise ValueError("Glue paths configuration must be a mapping")
    for key in (
        "raw_base_path",
        "reference_base_path",
        "staging_base_path",
        "curated_base_path",
        "audit_base_path",
    ):
        value = str(paths.get(key, ""))
        parsed = urlsplit(value)
        if (
            parsed.scheme != "s3"
            or not parsed.netloc
            or not parsed.path.strip("/")
            or parsed.netloc == "your-bucket"
            or "${" in value
            or "<" in value
            or ">" in value
            or parsed.query
            or parsed.fragment
        ):
            raise ValueError(f"Glue deployment requires a real S3 paths.{key}")
    for section in ("glue", "redshift"):
        settings = config.get(section, {})
        if isinstance(settings, dict) and settings.get("enabled"):
            for key, value in settings.items():
                if isinstance(value, str) and ("<" in value or ">" in value):
                    raise ValueError(f"Glue deployment requires a real {section}.{key}")
    GlueCatalogConfig.from_mapping(config)
    RedshiftConfig.from_mapping(config)


def _start_glue_runtime(job_name: str, args: argparse.Namespace) -> tuple[Any, Any]:
    """Import awsglue only inside the managed Glue runtime."""
    from awsglue.context import GlueContext
    from awsglue.job import Job
    from pyspark.context import SparkContext

    glue_context = GlueContext(SparkContext.getOrCreate())
    job = Job(glue_context)
    job.init(job_name, {"JOB_NAME": job_name, "run_date": args.run_date})
    return glue_context.spark_session, job


def run_glue_job(
    argv: Sequence[str],
    *,
    resource_root: Path | None = None,
    runtime_factory: Callable[[str, argparse.Namespace], tuple[Any, Any]] = _start_glue_runtime,
    daily_runner: Callable[..., int] = run_daily_batch,
) -> int:
    """Run the normal daily orchestration with a Glue-owned Spark session."""
    args = parse_job_arguments(argv)
    with ExitStack() as stack:
        if resource_root is None:
            temporary = Path(
                stack.enter_context(tempfile.TemporaryDirectory(prefix="transport-glue-"))
            )
            resource_root = extract_resources(args.resources_archive, temporary)
        resource_root = Path(resource_root).resolve()
        config_dir = resource_root / "config"
        loaded = load_config(args.config, config_dir=config_dir)
        if not isinstance(loaded.get("spark"), dict):
            raise ValueError("Spark configuration must be a mapping")
        overrides = _overrides(args, resource_root)
        _validate_deployment_config(_apply_overrides(loaded, overrides))
        spark, job = runtime_factory(args.JOB_NAME, args)
        status = daily_runner(
            config_path=args.config,
            config_dir=config_dir,
            run_date=args.run_date,
            overrides=overrides,
            spark_session=spark,
        )
        if status != 0:
            raise RuntimeError(f"Shared ETL daily batch returned status {status}")
        job.commit()
        return 0


if __name__ == "__main__":
    raise SystemExit(run_glue_job(sys.argv[1:]))
