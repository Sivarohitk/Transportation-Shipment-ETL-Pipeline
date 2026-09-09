"""Bronze layer builder.

Wraps the existing ``transport_etl.ingest`` modules to produce Bronze
DataFrames that:

1. Preserve source records as closely as practical — no Silver business
   cleaning happens here.
2. Reuse the existing explicit schema parsing from
   ``config/schemas/*.schema.json`` via the existing ingestion modules.
3. Carry operational ingestion metadata columns (``_ingested_at``,
   ``_source_file``, ``_batch_id``, ``_run_date``).

Invalid records are quarantined by the underlying ingest modules using
the existing quality strategy — they are never silently dropped.

Public API:
    - ``build_bronze_shipments``
    - ``build_bronze_carriers``
    - ``build_bronze_delivery_events``

Each builder returns a Spark DataFrame ready for publishing.  Callers
are responsible for writing the DataFrame through the target-aware
publisher (``transport_etl.bronze.publisher``).
"""

from __future__ import annotations

import logging
from typing import Any, Mapping

try:
    from pyspark.sql import DataFrame, SparkSession
    from pyspark.sql import functions as F
except ModuleNotFoundError:  # pragma: no cover - allows non-Spark unit tests
    DataFrame = Any  # type: ignore[assignment]
    SparkSession = Any  # type: ignore[assignment]
    F = None  # type: ignore[assignment]

from transport_etl.bronze.metadata import (
    BRONZE_METADATA_COLUMNS,
    META_COL_BATCH_ID,
    META_COL_INGESTED_AT,
    META_COL_RUN_DATE,
    META_COL_SOURCE_FILE,
    build_batch_id,
)
from transport_etl.ingest.carriers import read_carriers_raw
from transport_etl.ingest.delivery_events import read_delivery_events_raw
from transport_etl.ingest.shipments import read_shipments_raw

LOGGER = logging.getLogger(__name__)


def _require_spark() -> None:
    """Ensure pyspark is available before executing Spark operations."""
    if F is None:
        raise ImportError("pyspark is required for Bronze layer builders")


def _resolve_batch_id(batch_id: str | None, run_date: str | None, job_name: str) -> str:
    """Return a deterministic batch identifier for the Bronze records."""
    if batch_id:
        return str(batch_id)
    if run_date:
        return build_batch_id(run_date=run_date, job_name=job_name)
    return build_batch_id(run_date="undated", job_name=job_name)


def _attach_metadata(
    df: DataFrame,
    source_file: str,
    batch_id: str,
    run_date: str | None,
) -> DataFrame:
    """Attach operational ingestion metadata columns to a Bronze DataFrame.

    The metadata columns are always written in the canonical order
    defined by ``BRONZE_METADATA_COLUMNS`` so downstream tools can rely
    on a stable column contract.
    """
    _require_spark()
    enriched = df.withColumn(META_COL_INGESTED_AT, F.current_timestamp()).withColumn(
        META_COL_SOURCE_FILE, F.lit(str(source_file))
    )
    enriched = enriched.withColumn(META_COL_BATCH_ID, F.lit(str(batch_id)))
    if run_date:
        enriched = enriched.withColumn(META_COL_RUN_DATE, F.lit(str(run_date)))
    else:
        enriched = enriched.withColumn(META_COL_RUN_DATE, F.lit(None).cast("string"))

    return enriched


def _resolve_bad_records_path(
    base_path: str | None,
    run_date: str | None,
    entity: str,
) -> str | None:
    """Resolve the quarantine path used by the underlying ingest modules.

    Returns ``None`` when no base path is supplied so callers can disable
    quarantine writes entirely (matching Phase 1 behaviour).
    """
    if not base_path:
        return None
    normalized_base = str(base_path).rstrip("/\\")
    if run_date:
        return f"{normalized_base}/p_date={run_date}"
    return f"{normalized_base}/{entity}"


def _log_summary(df: DataFrame, entity: str, source_file: str) -> int:
    """Log Bronze record count for an entity and return the count."""
    _require_spark()
    count = int(df.count())
    LOGGER.info(
        "Bronze build entity=%s source=%s rows=%s metadata=%s",
        entity,
        source_file,
        count,
        list(BRONZE_METADATA_COLUMNS),
    )
    return count


def build_bronze_shipments(
    spark: SparkSession,
    source_path: str,
    *,
    run_date: str | None = None,
    batch_id: str | None = None,
    job_name: str = "daily",
    quarantine_path: str | None = None,
    read_options: Mapping[str, Any] | None = None,
    bad_record_write_config: Mapping[str, Any] | None = None,
    schema_def: Mapping[str, Any] | None = None,
) -> DataFrame:
    """Build a Bronze DataFrame for the shipments entity.

    Args:
        spark: Active SparkSession.
        source_path: Absolute or cloud path to the raw shipments CSV.
        run_date: Batch run date (``YYYY-MM-DD``).  Stored in metadata.
        batch_id: Optional explicit batch identifier; derived from
            ``run_date`` and ``job_name`` when omitted.
        job_name: Logical job name used in the derived ``batch_id``.
        quarantine_path: Optional base path for invalid-record quarantine.
        read_options: Optional Spark CSV reader options forwarded to the
            underlying ingestion module.
        bad_record_write_config: Optional write configuration forwarded to
            the underlying quarantine writer.

    Returns:
        Spark DataFrame with source columns plus Bronze metadata columns.
    """
    _require_spark()
    resolved_batch = _resolve_batch_id(batch_id, run_date, job_name)
    bad_path = _resolve_bad_records_path(quarantine_path, run_date, "shipments")

    raw_df = read_shipments_raw(
        spark=spark,
        source_path=source_path,
        bad_records_path=bad_path,
        read_options=read_options,
        bad_record_write_config=bad_record_write_config,
        schema_def=schema_def,
    )
    bronze_df = _attach_metadata(
        df=raw_df,
        source_file=source_path,
        batch_id=resolved_batch,
        run_date=run_date,
    )
    _log_summary(bronze_df, entity="shipments", source_file=source_path)
    return bronze_df


def build_bronze_carriers(
    spark: SparkSession,
    source_path: str,
    *,
    run_date: str | None = None,
    batch_id: str | None = None,
    job_name: str = "daily",
    quarantine_path: str | None = None,
    read_options: Mapping[str, Any] | None = None,
    bad_record_write_config: Mapping[str, Any] | None = None,
    schema_def: Mapping[str, Any] | None = None,
) -> DataFrame:
    """Build a Bronze DataFrame for the carriers entity."""
    _require_spark()
    resolved_batch = _resolve_batch_id(batch_id, run_date, job_name)
    bad_path = _resolve_bad_records_path(quarantine_path, run_date, "carriers")

    raw_df = read_carriers_raw(
        spark=spark,
        source_path=source_path,
        bad_records_path=bad_path,
        read_options=read_options,
        bad_record_write_config=bad_record_write_config,
        schema_def=schema_def,
    )
    bronze_df = _attach_metadata(
        df=raw_df,
        source_file=source_path,
        batch_id=resolved_batch,
        run_date=run_date,
    )
    _log_summary(bronze_df, entity="carriers", source_file=source_path)
    return bronze_df


def build_bronze_delivery_events(
    spark: SparkSession,
    source_path: str,
    *,
    run_date: str | None = None,
    batch_id: str | None = None,
    job_name: str = "daily",
    quarantine_path: str | None = None,
    read_options: Mapping[str, Any] | None = None,
    bad_record_write_config: Mapping[str, Any] | None = None,
    schema_def: Mapping[str, Any] | None = None,
) -> DataFrame:
    """Build a Bronze DataFrame for the delivery_events entity."""
    _require_spark()
    resolved_batch = _resolve_batch_id(batch_id, run_date, job_name)
    bad_path = _resolve_bad_records_path(quarantine_path, run_date, "delivery_events")

    raw_df = read_delivery_events_raw(
        spark=spark,
        source_path=source_path,
        bad_records_path=bad_path,
        read_options=read_options,
        bad_record_write_config=bad_record_write_config,
        schema_def=schema_def,
    )
    bronze_df = _attach_metadata(
        df=raw_df,
        source_file=source_path,
        batch_id=resolved_batch,
        run_date=run_date,
    )
    _log_summary(bronze_df, entity="delivery_events", source_file=source_path)
    return bronze_df


__all__ = [
    "BRONZE_METADATA_COLUMNS",
    "build_bronze_carriers",
    "build_bronze_delivery_events",
    "build_bronze_shipments",
]
