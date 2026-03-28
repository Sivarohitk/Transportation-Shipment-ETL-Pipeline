"""Spark session factory and profile loading helpers."""

from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Any, Mapping

from transport_etl.common.constants import (
    SPARK_EMR_CONF_FILE,
    SPARK_LOCAL_CONF_FILE,
    SPARK_PROFILE_DIR,
    SPARK_PROFILE_EMR,
    SPARK_PROFILE_LOCAL,
    SUPPORTED_SPARK_PROFILES,
)


def _profile_conf_path(profile: str, spark_profile_dir: str | Path = SPARK_PROFILE_DIR) -> Path:
    """Resolve profile config path for local/emr Spark settings."""
    if profile == SPARK_PROFILE_LOCAL:
        filename = SPARK_LOCAL_CONF_FILE
    elif profile == SPARK_PROFILE_EMR:
        filename = SPARK_EMR_CONF_FILE
    else:
        raise ValueError(f"Unsupported Spark profile: {profile}")

    directory = Path(spark_profile_dir)
    if not directory.is_absolute():
        directory = (Path.cwd() / directory).resolve()
    return directory / filename


def _parse_spark_conf_file(path: Path) -> dict[str, str]:
    """Parse key=value Spark config file format."""
    conf: dict[str, str] = {}
    if not path.exists():
        return conf

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            continue

        key, value = line.split("=", 1)
        conf[key.strip()] = value.strip()

    return conf


def _normalize_conf(conf: Mapping[str, Any] | None) -> dict[str, str]:
    """Normalize Spark conf map to string key/value pairs."""
    if not conf:
        return {}
    return {str(key): str(value) for key, value in conf.items()}


def build_spark_conf(
    profile: str,
    app_conf: Mapping[str, Any] | None = None,
    extra_conf: Mapping[str, Any] | None = None,
    spark_profile_dir: str | Path = SPARK_PROFILE_DIR,
) -> dict[str, str]:
    """Build merged Spark configuration map from profile + app + runtime overrides."""
    if profile not in SUPPORTED_SPARK_PROFILES:
        raise ValueError(f"Unsupported Spark profile: {profile}")

    profile_conf = _parse_spark_conf_file(_profile_conf_path(profile, spark_profile_dir))

    app_level_conf = {}
    if app_conf and isinstance(app_conf.get("conf"), Mapping):
        app_level_conf = _normalize_conf(app_conf["conf"])

    merged = {**profile_conf, **app_level_conf, **_normalize_conf(extra_conf)}

    if profile == SPARK_PROFILE_LOCAL and "spark.master" not in merged:
        merged["spark.master"] = "local[*]"

    return merged


def create_spark_session(
    app_name: str,
    profile: str = SPARK_PROFILE_LOCAL,
    enable_hive_support: bool = True,
    conf: Mapping[str, Any] | None = None,
    spark_profile_dir: str | Path = SPARK_PROFILE_DIR,
):
    """Create and return a configured SparkSession.

    Args:
        app_name: Spark application name.
        profile: Runtime profile (`local` or `emr`).
        enable_hive_support: Enable Hive catalog support if true.
        conf: Additional Spark conf values.
        spark_profile_dir: Directory containing profile `.conf` files.
    """
    from pyspark.sql import SparkSession

    def _session_is_live(session: Any) -> bool:
        """Return true when a SparkSession still has a live JVM context."""
        if session is None:
            return False
        try:
            spark_context = session.sparkContext
        except Exception:
            return False

        if spark_context is None:
            return False

        try:
            return spark_context._jsc is not None
        except Exception:
            return False

    def _clear_registered_sessions() -> None:
        """Clear global active/default Spark session references."""
        try:
            SparkSession.clearActiveSession()
            SparkSession.clearDefaultSession()
        except Exception:
            # Keep stop/create logic resilient across Spark/PySpark versions.
            return

    existing_session = SparkSession.getActiveSession()
    if existing_session is None and hasattr(SparkSession, "getDefaultSession"):
        existing_session = SparkSession.getDefaultSession()

    if existing_session is not None and not _session_is_live(existing_session):
        _clear_registered_sessions()
        existing_session = None

    spark_conf = build_spark_conf(
        profile=profile,
        app_conf=None,
        extra_conf=conf,
        spark_profile_dir=spark_profile_dir,
    )

    if profile == SPARK_PROFILE_LOCAL:
        os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
        os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
        spark_conf.setdefault("spark.pyspark.python", sys.executable)
        spark_conf.setdefault("spark.executorEnv.PYSPARK_PYTHON", sys.executable)

    builder = SparkSession.builder.appName(app_name)
    for key, value in spark_conf.items():
        builder = builder.config(key, value)

    if enable_hive_support:
        builder = builder.enableHiveSupport()

    spark_session = builder.getOrCreate()
    # Track ownership so callers do not stop externally managed sessions.
    setattr(spark_session, "_transport_etl_owned_session", existing_session is None)
    return spark_session


def create_spark_session_from_config(
    config: Mapping[str, Any],
    profile: str | None = None,
    app_name: str | None = None,
    extra_conf: Mapping[str, Any] | None = None,
):
    """Create SparkSession from merged app config dictionary."""
    spark_section = config.get("spark", {}) if isinstance(config.get("spark"), Mapping) else {}
    selected_profile = profile or str(spark_section.get("profile", SPARK_PROFILE_LOCAL))
    selected_app_name = app_name or str(spark_section.get("app_name", "transport-shipment-etl"))
    hive_enabled = bool(spark_section.get("enable_hive_support", True))

    combined_extra = _normalize_conf(extra_conf)
    return create_spark_session(
        app_name=selected_app_name,
        profile=selected_profile,
        enable_hive_support=hive_enabled,
        conf={
            **_normalize_conf(
                spark_section.get("conf") if isinstance(spark_section, Mapping) else {}
            ),
            **combined_extra,
        },
    )


def stop_spark_session(spark: Any, *, only_if_owned: bool = True) -> None:
    """Stop SparkSession if provided.

    Args:
        spark: SparkSession to stop.
        only_if_owned: When true, skip stopping sessions not created by
            `create_spark_session`.
    """
    if spark is None:
        return

    if only_if_owned and not bool(getattr(spark, "_transport_etl_owned_session", True)):
        return

    from pyspark.sql import SparkSession

    try:
        spark_context = spark.sparkContext
    except Exception:
        spark_context = None

    is_live = False
    if spark_context is not None:
        try:
            is_live = spark_context._jsc is not None
        except Exception:
            is_live = False

    try:
        if is_live:
            spark.stop()
    finally:
        try:
            SparkSession.clearActiveSession()
            SparkSession.clearDefaultSession()
        except Exception:
            pass
