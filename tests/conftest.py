"""Shared pytest fixtures for ETL tests."""

from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Any, Iterator

import pytest


def _is_spark_session_live(session: Any) -> bool:
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


def _cleanup_registered_spark_sessions() -> None:
    """Stop and clear any previously registered global Spark sessions."""
    try:
        from pyspark.sql import SparkSession
    except Exception:
        return

    sessions: list[Any] = []
    active = SparkSession.getActiveSession()
    if active is not None:
        sessions.append(active)

    if hasattr(SparkSession, "getDefaultSession"):
        default = SparkSession.getDefaultSession()
        if default is not None and default not in sessions:
            sessions.append(default)

    for session in sessions:
        if _is_spark_session_live(session):
            try:
                session.stop()
            except Exception:
                pass

    try:
        SparkSession.clearActiveSession()
        SparkSession.clearDefaultSession()
    except Exception:
        pass


@pytest.fixture(scope="session")
def project_root() -> Path:
    """Return repository root path."""
    return Path(__file__).resolve().parents[1]


@pytest.fixture(scope="session")
def sample_run_date() -> str:
    """Provide a stable run date for deterministic assertions."""
    return "2026-01-01"


@pytest.fixture(scope="session")
def sample_paths(project_root: Path) -> dict[str, Path]:
    """Return canonical sample input paths."""
    return {
        "raw_shipments": project_root / "data" / "sample" / "raw" / "shipments_2026-01-01.csv",
        "raw_carriers": project_root / "data" / "sample" / "raw" / "carriers_2026-01-01.csv",
        "raw_delivery_events": project_root / "data" / "sample" / "raw" / "delivery_events_2026-01-01.csv",
        "region_lookup": project_root / "data" / "sample" / "reference" / "region_lookup.csv",
    }


@pytest.fixture(scope="function")
def spark(tmp_path_factory: pytest.TempPathFactory) -> Iterator[Any]:
    """Create a local SparkSession for tests."""
    pytest.importorskip("pyspark")
    from pyspark.sql import SparkSession

    _cleanup_registered_spark_sessions()

    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    warehouse_dir = tmp_path_factory.mktemp("spark_warehouse")
    ivy_dir = tmp_path_factory.mktemp("spark_ivy")

    spark_session = (
        SparkSession.builder.appName("transport-etl-tests")
        .master("local[2]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.warehouse.dir", str(warehouse_dir))
        .config("spark.ui.enabled", "false")
        .config("spark.jars.ivy", str(ivy_dir))
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.pyspark.python", sys.executable)
        .config("spark.executorEnv.PYSPARK_PYTHON", sys.executable)
        .getOrCreate()
    )

    spark_session.sparkContext.setLogLevel("ERROR")
    yield spark_session
    try:
        if _is_spark_session_live(spark_session):
            spark_session.stop()
    finally:
        _cleanup_registered_spark_sessions()


@pytest.fixture(scope="session")
def staging_sql(project_root: Path) -> dict[str, str]:
    """Load staging SQL query text for temp-view execution."""
    sql_dir = project_root / "sql" / "staging"
    return {
        "shipments": (sql_dir / "stg_shipments.sql").read_text(encoding="utf-8"),
        "carriers": (sql_dir / "stg_carriers.sql").read_text(encoding="utf-8"),
        "delivery_events": (sql_dir / "stg_delivery_events.sql").read_text(encoding="utf-8"),
    }


@pytest.fixture()
def ingested_frames(spark: Any, sample_paths: dict[str, Path], tmp_path: Path) -> dict[str, Any]:
    """Read sample inputs through ingestion modules."""
    from transport_etl.ingest.carriers import read_carriers_raw
    from transport_etl.ingest.delivery_events import read_delivery_events_raw
    from transport_etl.ingest.shipments import read_shipments_raw

    bad_records_base = tmp_path / "quarantine"

    shipments_df = read_shipments_raw(
        spark=spark,
        source_path=str(sample_paths["raw_shipments"]),
        bad_records_path=str(bad_records_base),
    )
    carriers_df = read_carriers_raw(
        spark=spark,
        source_path=str(sample_paths["raw_carriers"]),
        bad_records_path=str(bad_records_base),
    )
    delivery_events_df = read_delivery_events_raw(
        spark=spark,
        source_path=str(sample_paths["raw_delivery_events"]),
        bad_records_path=str(bad_records_base),
    )

    return {
        "shipments": shipments_df,
        "carriers": carriers_df,
        "delivery_events": delivery_events_df,
    }


@pytest.fixture()
def region_lookup_df(spark: Any, sample_paths: dict[str, Path]) -> Any:
    """Load normalized region lookup DataFrame."""
    from transport_etl.transform.enrich_region import load_region_lookup

    return load_region_lookup(spark=spark, lookup_path=str(sample_paths["region_lookup"]))


@pytest.fixture()
def staging_frames(
    spark: Any,
    ingested_frames: dict[str, Any],
    staging_sql: dict[str, str],
) -> dict[str, Any]:
    """Apply staging SQL to ingested sample data."""
    ingested_frames["shipments"].createOrReplaceTempView("raw_shipments")
    ingested_frames["carriers"].createOrReplaceTempView("raw_carriers")
    ingested_frames["delivery_events"].createOrReplaceTempView("raw_delivery_events")

    stg_shipments_df = spark.sql(staging_sql["shipments"])
    stg_carriers_df = spark.sql(staging_sql["carriers"])
    stg_delivery_events_df = spark.sql(staging_sql["delivery_events"])

    return {
        "shipments": stg_shipments_df,
        "carriers": stg_carriers_df,
        "delivery_events": stg_delivery_events_df,
    }


@pytest.fixture()
def curated_frames(
    sample_run_date: str,
    staging_frames: dict[str, Any],
    region_lookup_df: Any,
) -> dict[str, Any]:
    """Build curated and KPI DataFrames from staged sample data."""
    from transport_etl.transform.build_agg_shipment_daily import build_agg_shipment_daily
    from transport_etl.transform.build_dim_carrier import build_dim_carrier
    from transport_etl.transform.build_fct_delivery_event import build_fct_delivery_event
    from transport_etl.transform.build_fct_shipment import build_fct_shipment
    from transport_etl.transform.build_kpi_delivery_daily import build_kpi_delivery_daily
    from transport_etl.transform.enrich_region import (
        enrich_delivery_events_with_region,
        enrich_shipments_with_region,
    )
    from transport_etl.transform.standardize import standardize_columns

    stg_shipments_std_df = standardize_columns(staging_frames["shipments"])
    stg_carriers_std_df = standardize_columns(staging_frames["carriers"])
    stg_delivery_events_std_df = standardize_columns(staging_frames["delivery_events"])

    stg_shipments_enriched_df = enrich_shipments_with_region(
        shipments_df=stg_shipments_std_df,
        region_lookup_df=region_lookup_df,
    )
    stg_delivery_events_enriched_df = enrich_delivery_events_with_region(
        events_df=stg_delivery_events_std_df,
        region_lookup_df=region_lookup_df,
    )

    dim_carrier_df = build_dim_carrier(
        stg_carriers_df=stg_carriers_std_df,
        run_date=sample_run_date,
    )
    fct_shipment_df = build_fct_shipment(
        stg_shipments_df=stg_shipments_enriched_df,
        region_lookup_df=region_lookup_df,
    )
    fct_delivery_event_df = build_fct_delivery_event(
        stg_events_df=stg_delivery_events_enriched_df,
        stg_shipments_df=stg_shipments_enriched_df,
        region_lookup_df=region_lookup_df,
    )
    agg_shipment_daily_df = build_agg_shipment_daily(fct_shipment_df=fct_shipment_df)
    kpi_delivery_daily_df = build_kpi_delivery_daily(
        agg_shipment_daily_df=agg_shipment_daily_df,
        fct_delivery_event_df=fct_delivery_event_df,
    )

    return {
        "dim_carrier": dim_carrier_df,
        "fct_shipment": fct_shipment_df,
        "fct_delivery_event": fct_delivery_event_df,
        "agg_shipment_daily": agg_shipment_daily_df,
        "kpi_delivery_daily": kpi_delivery_daily_df,
    }
