"""I/O helpers for local and cloud storage paths used by Spark jobs."""

from __future__ import annotations

import csv
import json
import platform
import re
import shutil
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

from transport_etl.common.constants import (
    DEFAULT_CSV_OPTIONS,
    DEFAULT_PARQUET_COMPRESSION,
    DEFAULT_WRITE_MODE,
)

_WINUTILS_ERROR_MARKERS = (
    "did not find winutils.exe",
    "winutils.exe",
    "hadoop_home",
    "hadoop.home.dir",
    "could not locate executable null\\bin\\winutils.exe",
)


def is_cloud_path(path: str | Path) -> bool:
    """Return `True` for object-store URIs (e.g. s3://)."""
    value = str(path).lower()
    return value.startswith("s3://") or value.startswith("s3a://") or value.startswith("s3n://")


def ensure_local_dir(path: str | Path) -> Path:
    """Ensure a local directory exists and return its path.

    Cloud URIs are returned as-is without local directory operations.
    """
    path_obj = Path(path)
    if is_cloud_path(path_obj):
        return path_obj
    path_obj.mkdir(parents=True, exist_ok=True)
    return path_obj


def path_exists(path: str | Path) -> bool:
    """Check local path existence.

    Cloud path existence checks require Spark/Hadoop APIs and are intentionally skipped.
    """
    if is_cloud_path(path):
        return True
    return Path(path).exists()


def _normalize_options(options: Mapping[str, Any] | None) -> dict[str, str]:
    """Convert option values to strings expected by Spark readers/writers."""
    if not options:
        return {}
    return {str(key): str(value) for key, value in options.items()}


def _normalize_format(value: Any, default: str, allowed: set[str]) -> str:
    """Normalize and validate output format values."""
    normalized = str(value).strip().lower()
    if normalized in allowed:
        return normalized
    return default


def _is_windows() -> bool:
    """Return True when running on a Windows host."""
    return platform.system().lower().startswith("win")


def _is_winutils_error(exc: Exception) -> bool:
    """Detect Spark/Hadoop local-write errors caused by missing winutils setup."""
    text = str(exc).lower()
    if any(marker in text for marker in _WINUTILS_ERROR_MARKERS):
        return True

    java_exc = getattr(exc, "java_exception", None)
    if java_exc is None:
        return False
    return any(marker in str(java_exc).lower() for marker in _WINUTILS_ERROR_MARKERS)


def _write_in_format(
    df: Any,
    path: str,
    mode: str,
    fmt: str,
    partition_by: list[str] | None = None,
    options: Mapping[str, Any] | None = None,
) -> None:
    """Write a DataFrame using a Spark writer format."""
    writer = df.write.mode(mode).options(**_normalize_options(options))
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    if fmt == "parquet":
        writer.parquet(path)
        return
    if fmt == "json":
        writer.json(path)
        return
    if fmt == "csv":
        writer.option("header", "true").csv(path)
        return
    raise ValueError(f"Unsupported write format: {fmt}")


def _normalized_mode(mode: str) -> str:
    """Return normalized Spark write mode."""
    return str(mode).strip().lower()


def _supports_windows_local_fallback(
    destination: str,
    exc: Exception,
    write_config: Mapping[str, Any],
) -> bool:
    """Return whether fallback should be used for a failed Spark write."""
    fallback_toggle = write_config.get("windows_local_fallback_enabled")
    windows_fallback_enabled = bool(fallback_toggle) if fallback_toggle is not None else True
    execution_mode = str(write_config.get("execution_mode", "local")).strip().lower()
    local_only = execution_mode == "local"
    cloud_target = is_cloud_path(destination)

    return bool(
        windows_fallback_enabled
        and _is_windows()
        and local_only
        and not cloud_target
        and _is_winutils_error(exc)
    )


def _partition_path_value(value: Any) -> str:
    """Normalize partition value into a safe folder token."""
    if value is None:
        return "UNKNOWN"

    if hasattr(value, "isoformat"):
        raw = str(value.isoformat())
    else:
        raw = str(value)

    cleaned = raw.strip()
    if not cleaned:
        return "UNKNOWN"

    # Keep Hive-style partition folders readable and Windows-path safe.
    cleaned = cleaned.replace("=", "-")
    cleaned = re.sub(r'[<>:"/\\|?*\x00-\x1F]', "_", cleaned)
    return cleaned or "UNKNOWN"


def _partitioned_destination_dir(
    destination: str,
    partition_by: list[str],
    partition_values: tuple[str, ...],
) -> Path:
    """Build local partition folder path for fallback writes."""
    target = Path(destination)
    for column, value in zip(partition_by, partition_values):
        target = target / f"{column}={value}"
    return ensure_local_dir(target)


def _json_safe_row_value(value: Any) -> Any:
    """Convert complex row values into JSON-serializable payloads."""
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)


def _write_python_json_fallback(
    df: Any,
    destination: str,
    mode: str,
    partition_by: list[str] | None = None,
) -> None:
    """Write JSONL fallback files using Python I/O and optional partition folders."""
    normalized_mode = _normalized_mode(mode)
    destination_path = Path(destination)
    if normalized_mode == "overwrite" and destination_path.exists():
        shutil.rmtree(destination_path, ignore_errors=True)

    ensure_local_dir(destination_path)

    columns = [str(col) for col in df.columns]
    rows = [row.asDict(recursive=True) for row in df.collect()]
    partitions = [column for column in (partition_by or []) if column in columns]

    grouped_rows: dict[tuple[str, ...], list[dict[str, Any]]] = {}
    if partitions:
        for row in rows:
            key = tuple(_partition_path_value(row.get(column)) for column in partitions)
            grouped_rows.setdefault(key, []).append(row)
    else:
        grouped_rows[tuple()] = rows

    for key, partition_rows in grouped_rows.items():
        target_dir = (
            _partitioned_destination_dir(destination, partitions, key) if partitions else ensure_local_dir(destination)
        )
        target_path = Path(target_dir) / (
            f"part-{datetime.now(tz=timezone.utc).strftime('%Y%m%d%H%M%S')}-{uuid.uuid4().hex}.jsonl"
        )
        with target_path.open("w", encoding="utf-8") as handle:
            for row in partition_rows:
                normalized = {col: _json_safe_row_value(row.get(col)) for col in columns}
                handle.write(json.dumps(normalized, default=str))
                handle.write("\n")


def _write_python_csv_fallback(
    df: Any,
    destination: str,
    mode: str,
    partition_by: list[str] | None = None,
) -> None:
    """Write CSV fallback files using Python I/O and optional partition folders."""
    normalized_mode = _normalized_mode(mode)
    destination_path = Path(destination)
    if normalized_mode == "overwrite" and destination_path.exists():
        shutil.rmtree(destination_path, ignore_errors=True)

    ensure_local_dir(destination_path)

    columns = [str(col) for col in df.columns]
    rows = [row.asDict(recursive=True) for row in df.collect()]
    partitions = [column for column in (partition_by or []) if column in columns]

    grouped_rows: dict[tuple[str, ...], list[dict[str, Any]]] = {}
    if partitions:
        for row in rows:
            key = tuple(_partition_path_value(row.get(column)) for column in partitions)
            grouped_rows.setdefault(key, []).append(row)
    else:
        grouped_rows[tuple()] = rows

    for key, partition_rows in grouped_rows.items():
        target_dir = (
            _partitioned_destination_dir(destination, partitions, key) if partitions else ensure_local_dir(destination)
        )
        target_path = Path(target_dir) / (
            f"part-{datetime.now(tz=timezone.utc).strftime('%Y%m%d%H%M%S')}-{uuid.uuid4().hex}.csv"
        )
        with target_path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=columns)
            writer.writeheader()
            for row in partition_rows:
                normalized = {col: _json_safe_row_value(row.get(col)) for col in columns}
                writer.writerow(normalized)


def write_invalid_records_with_fallback(
    df: Any,
    destination: str,
    mode: str = "append",
    write_config: Mapping[str, Any] | None = None,
    logger: Any | None = None,
) -> str:
    """Write invalid/quarantine records with optional Windows local fallback.

    Default behavior writes Parquet. If enabled via config, Windows local runs can
    fall back to CSV or JSON when Spark Parquet writes fail due to missing
    HADOOP_HOME/winutils configuration.

    Returns:
        The format that was used to successfully write records.
    """
    config = dict(write_config or {})
    primary_format = _normalize_format(
        config.get("format", "parquet"),
        default="parquet",
        allowed={"parquet"},
    )
    primary_options = config.get("parquet_options")
    if not isinstance(primary_options, Mapping):
        primary_options = None

    if not is_cloud_path(destination):
        ensure_local_dir(destination)

    try:
        _write_in_format(
            df=df,
            path=destination,
            mode=mode,
            fmt=primary_format,
            partition_by=None,
            options=primary_options,
        )
        return primary_format
    except Exception as exc:
        if not _supports_windows_local_fallback(destination=destination, exc=exc, write_config=config):
            raise

        fallback_format = _normalize_format(
            config.get("windows_local_fallback_format", "json"),
            default="json",
            allowed={"csv", "json"},
        )
        fallback_destination = destination.rstrip("/\\") + f"/_fallback_{fallback_format}"
        ensure_local_dir(fallback_destination)

        if logger is not None:
            logger.warning(
                "Parquet invalid-record write failed with winutils/HADOOP_HOME error; "
                "falling back to %s at %s",
                fallback_format,
                fallback_destination,
            )

        try:
            if fallback_format == "json":
                _write_python_json_fallback(df=df, destination=fallback_destination, mode=mode, partition_by=None)
            else:
                _write_python_csv_fallback(df=df, destination=fallback_destination, mode=mode, partition_by=None)
            return fallback_format
        except Exception as fallback_exc:
            if logger is not None:
                logger.exception("Fallback invalid-record write failed")
            raise fallback_exc from exc


def read_csv(
    spark: Any,
    path: str,
    schema: Any | None = None,
    options: Mapping[str, Any] | None = None,
) -> Any:
    """Read CSV data with configurable options and optional schema."""
    if not path_exists(path):
        raise FileNotFoundError(f"Input CSV path not found: {path}")

    csv_options = {**DEFAULT_CSV_OPTIONS, **_normalize_options(options)}
    reader = spark.read.options(**csv_options)
    if schema is not None:
        reader = reader.schema(schema)
    return reader.csv(path)


def read_parquet(spark: Any, path: str, options: Mapping[str, Any] | None = None) -> Any:
    """Read Parquet dataset from local or cloud path."""
    if not path_exists(path):
        raise FileNotFoundError(f"Input Parquet path not found: {path}")

    reader = spark.read.options(**_normalize_options(options))
    return reader.parquet(path)


def write_parquet(
    df: Any,
    path: str,
    mode: str = DEFAULT_WRITE_MODE,
    partition_by: list[str] | None = None,
    options: Mapping[str, Any] | None = None,
    write_config: Mapping[str, Any] | None = None,
    logger: Any | None = None,
) -> tuple[str, str]:
    """Write a DataFrame to Parquet with optional Windows local fallback.

    Returns:
        Tuple of (`written_format`, `written_path`).
    """
    if not is_cloud_path(path):
        ensure_local_dir(Path(path).parent)

    writer_options = {"compression": DEFAULT_PARQUET_COMPRESSION}
    writer_options.update(_normalize_options(options))
    config = dict(write_config or {})
    primary_format = _normalize_format(
        config.get("format", "parquet"),
        default="parquet",
        allowed={"parquet"},
    )

    try:
        _write_in_format(
            df=df,
            path=path,
            mode=mode,
            fmt=primary_format,
            partition_by=partition_by,
            options=writer_options,
        )
        return primary_format, path
    except Exception as exc:
        if not _supports_windows_local_fallback(destination=path, exc=exc, write_config=config):
            raise

        fallback_format = _normalize_format(
            config.get("windows_local_fallback_format", "json"),
            default="json",
            allowed={"csv", "json"},
        )
        fallback_destination = path.rstrip("/\\") + f"/_fallback_{fallback_format}"
        ensure_local_dir(fallback_destination)

        if logger is not None:
            logger.warning(
                "Parquet curated write failed with winutils/HADOOP_HOME error; "
                "falling back to %s at %s",
                fallback_format,
                fallback_destination,
            )

        if fallback_format == "json":
            _write_python_json_fallback(
                df=df,
                destination=fallback_destination,
                mode=mode,
                partition_by=partition_by,
            )
        else:
            _write_python_csv_fallback(
                df=df,
                destination=fallback_destination,
                mode=mode,
                partition_by=partition_by,
            )

        return fallback_format, fallback_destination
