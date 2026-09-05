"""Unit tests for the Bronze metadata definitions.

These tests cover the pure-Python helpers in ``transport_etl.bronze.metadata``
and the public Bronze metadata constants.  No PySpark session is required.
"""

from __future__ import annotations

from transport_etl.bronze.metadata import (
    BRONZE_METADATA_COLUMNS,
    META_COL_BATCH_ID,
    META_COL_INGESTED_AT,
    META_COL_RUN_DATE,
    META_COL_SOURCE_FILE,
    build_batch_id,
)
from transport_etl.bronze.publisher import (
    BRONZE_TABLE_NAMES,
    TABLE_BRONZE_CARRIERS,
    TABLE_BRONZE_DELIVERY_EVENTS,
    TABLE_BRONZE_SHIPMENTS,
)
from transport_etl.common.constants import (
    BRONZE_METADATA_COLUMNS as CONST_BRONZE_METADATA_COLUMNS,
)
from transport_etl.common.constants import (
    META_COL_BATCH_ID as CONST_META_COL_BATCH_ID,
)
from transport_etl.common.constants import (
    META_COL_INGESTED_AT as CONST_META_COL_INGESTED_AT,
)
from transport_etl.common.constants import (
    META_COL_RUN_DATE as CONST_META_COL_RUN_DATE,
)
from transport_etl.common.constants import (
    META_COL_SOURCE_FILE as CONST_META_COL_SOURCE_FILE,
)
from transport_etl.common.constants import (
    TABLE_BRONZE_CARRIERS as CONST_TABLE_BRONZE_CARRIERS,
)
from transport_etl.common.constants import (
    TABLE_BRONZE_DELIVERY_EVENTS as CONST_TABLE_BRONZE_DELIVERY_EVENTS,
)
from transport_etl.common.constants import (
    TABLE_BRONZE_SHIPMENTS as CONST_TABLE_BRONZE_SHIPMENTS,
)


class TestBronzeMetadataContract:
    """Verify the canonical metadata column contract."""

    def test_metadata_columns_order(self) -> None:
        assert BRONZE_METADATA_COLUMNS == (
            "_ingested_at",
            "_source_file",
            "_batch_id",
            "_run_date",
        )

    def test_metadata_constants_aliases_match(self) -> None:
        assert META_COL_INGESTED_AT == "_ingested_at"
        assert META_COL_SOURCE_FILE == "_source_file"
        assert META_COL_BATCH_ID == "_batch_id"
        assert META_COL_RUN_DATE == "_run_date"

    def test_metadata_columns_match_constants_module(self) -> None:
        """The bronze.metadata re-exports must match common.constants."""
        assert BRONZE_METADATA_COLUMNS == CONST_BRONZE_METADATA_COLUMNS
        assert META_COL_INGESTED_AT == CONST_META_COL_INGESTED_AT
        assert META_COL_SOURCE_FILE == CONST_META_COL_SOURCE_FILE
        assert META_COL_BATCH_ID == CONST_META_COL_BATCH_ID
        assert META_COL_RUN_DATE == CONST_META_COL_RUN_DATE

    def test_bronze_table_names_complete(self) -> None:
        assert BRONZE_TABLE_NAMES == ("raw_shipments", "raw_carriers", "raw_delivery_events")

    def test_bronze_table_name_constants_match(self) -> None:
        assert TABLE_BRONZE_SHIPMENTS == "raw_shipments"
        assert TABLE_BRONZE_CARRIERS == "raw_carriers"
        assert TABLE_BRONZE_DELIVERY_EVENTS == "raw_delivery_events"
        assert TABLE_BRONZE_SHIPMENTS == CONST_TABLE_BRONZE_SHIPMENTS
        assert TABLE_BRONZE_CARRIERS == CONST_TABLE_BRONZE_CARRIERS
        assert TABLE_BRONZE_DELIVERY_EVENTS == CONST_TABLE_BRONZE_DELIVERY_EVENTS


class TestBuildBatchId:
    """Deterministic batch id construction for Bronze metadata."""

    def test_returns_daily_prefix_for_default_job(self) -> None:
        assert build_batch_id(run_date="2026-01-01") == "daily_20260101"

    def test_custom_job_name(self) -> None:
        assert build_batch_id(run_date="2026-01-02", job_name="backfill") == "backfill_20260102"

    def test_normalises_whitespace_in_job_name(self) -> None:
        assert build_batch_id(run_date="2026-01-03", job_name=" Daily Run ") == "daily_run_20260103"

    def test_lowercases_job_name(self) -> None:
        assert build_batch_id(run_date="2026-01-04", job_name="DAILY") == "daily_20260104"

    def test_strips_dashes_from_run_date(self) -> None:
        assert build_batch_id(run_date="2026-12-31") == "daily_20261231"

    def test_handles_missing_run_date(self) -> None:
        assert build_batch_id(run_date="") == "daily_undated"

    def test_handles_whitespace_run_date(self) -> None:
        assert build_batch_id(run_date="   ") == "daily_undated"

    def test_truncates_long_job_name(self) -> None:
        long_job = "a" * 100
        batch_id = build_batch_id(run_date="2026-01-01", job_name=long_job)
        # Job portion should be 32 chars; full id has the date tail appended.
        job_part = batch_id.rsplit("_", 1)[0]
        assert len(job_part) <= 32

    def test_fallback_job_name_when_empty(self) -> None:
        batch_id = build_batch_id(run_date="2026-01-01", job_name="   ")
        # Falls back to ``batch`` placeholder.
        assert batch_id.startswith("batch_")

    def test_deterministic_for_same_inputs(self) -> None:
        first = build_batch_id(run_date="2026-01-01", job_name="daily")
        second = build_batch_id(run_date="2026-01-01", job_name="daily")
        assert first == second
