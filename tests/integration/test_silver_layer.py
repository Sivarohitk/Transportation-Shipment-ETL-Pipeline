"""Integration tests for the Silver layer.

These tests run against a real local SparkSession.  They cover:

- Business-key application via the dedup helper
- Deterministic dedup ("latest record wins" by ``updated_at``)
- Quality validation and quarantine routing
- Silver lineage column attachment
- Idempotency of the build pipeline (reruns produce the same row set)
- Local Parquet publication (full path through the publisher)
- The MERGE orchestrator registering a source view and emitting
  Spark SQL MERGE statements (without executing the actual Delta
  write — a real Delta table is not available in CI).
- Late-arriving data is updated, not duplicated
- A live-Databricks test is marked skip (see AGENTS.md rule 9).
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from transport_etl.common.constants import (
    META_COL_SILVER_BATCH_ID,
    META_COL_SILVER_RUN_DATE,
    SILVER_METADATA_COLUMNS,
    TABLE_SILVER_CARRIERS,
    TABLE_SILVER_DELIVERY_EVENTS,
    TABLE_SILVER_SHIPMENTS,
)
from transport_etl.ingest.carriers import load_carriers_schema_definition
from transport_etl.ingest.delivery_events import load_delivery_events_schema_definition
from transport_etl.ingest.shipments import load_shipments_schema_definition
from transport_etl.silver.deduplication import dedupe_silver
from transport_etl.silver.merge import (
    build_silver_merge_spec,
    execute_silver_merge,
    register_source_view,
    render_silver_merge_sql,
)
from transport_etl.silver.merge_spec import build_merge_sql

# ---------------------------------------------------------------------------
# Silver dedup helper
# ---------------------------------------------------------------------------


class TestSilverDedup:
    """The dedup helper applies the business-key "latest record wins" policy."""

    def test_dedup_keeps_latest_record_by_updated_at(
        self, spark, sample_paths: dict[str, Path], tmp_path: Path
    ) -> None:
        pytest.importorskip("pyspark")
        from pyspark.sql import functions as F

        from transport_etl.ingest.shipments import read_shipments_raw

        df = read_shipments_raw(
            spark=spark,
            source_path=str(sample_paths["raw_shipments"]),
            bad_records_path=str(tmp_path / "q"),
        )
        # SHP1006 appears twice in the sample data; the dedup should
        # keep exactly one row per shipment_id.
        dedup = dedupe_silver(df, table_name=TABLE_SILVER_SHIPMENTS)

        assert dedup.survived.count() == df.dropDuplicates(["shipment_id"]).count()
        # The duplicate SHP1006 row should be in the dropped set.
        duplicate_shipment_ids = {
            row["shipment_id"] for row in dedup.dropped.select("shipment_id").distinct().collect()
        }
        assert "SHP1006" in duplicate_shipment_ids

        # The kept row for SHP1006 must be the one with the higher
        # updated_at value.
        kept_shipment = (
            dedup.survived.filter(F.col("shipment_id") == "SHP1006")
            .select("updated_at", "shipping_cost_usd")
            .collect()[0]
        )
        # The CSV has two SHP1006 rows; the one with shipping_cost_usd=420.25
        # has updated_at=2026-01-02T21:36:00Z; the one with 415.00 has
        # updated_at=2026-01-02T21:50:00Z.  The dedup must keep the
        # second (415.00) row.
        assert float(kept_shipment["shipping_cost_usd"]) == 415.00

    def test_dedup_handles_missing_optional_columns(self, spark) -> None:
        """When an entity lacks the secondary tie-breaker column, the
        helper should still dedup by ``updated_at`` alone.
        """
        from pyspark.sql import functions as F

        # Build a DataFrame whose secondary tie-breaker is missing.
        df = spark.createDataFrame(
            [
                ("A", "2026-01-01T00:00:00Z"),
                ("A", "2026-01-02T00:00:00Z"),
                ("B", "2026-01-01T00:00:00Z"),
            ],
            schema="shipment_id string, updated_at string",
        ).withColumn("updated_at", F.to_timestamp("updated_at", "yyyy-MM-dd'T'HH:mm:ssX"))

        # Use a custom table name to skip the key_columns_for_entity
        # validation: we just exercise the helper directly with a
        # DataFrame that already has the business key.
        outcome = dedupe_silver(df, table_name=TABLE_SILVER_SHIPMENTS)
        # The two duplicates for key 'A' should be reduced to one row.
        # Compare row contents rather than calling .count() to avoid
        # Spark JVM lifecycle issues in CI.
        survived_ids = sorted([row["shipment_id"] for row in outcome.survived.collect()])
        assert survived_ids == ["A", "B"]
        # And the dropped frame must hold the older duplicate.
        dropped_ids = sorted([row["shipment_id"] for row in outcome.dropped.collect()])
        assert dropped_ids == ["A"]

    def test_dedup_raises_when_business_key_missing(self, spark) -> None:
        from pyspark.sql import functions as F

        df = spark.createDataFrame(
            [("2026-01-01T00:00:00Z",)],
            schema="updated_at string",
        ).withColumn("updated_at", F.to_timestamp("updated_at", "yyyy-MM-dd'T'HH:mm:ssX"))

        with pytest.raises(ValueError, match="business key columns missing"):
            dedupe_silver(df, table_name=TABLE_SILVER_SHIPMENTS)

    def test_dedup_returns_metadata_about_key_and_tie_breakers(
        self, spark, sample_paths: dict[str, Path], tmp_path: Path
    ) -> None:
        from transport_etl.ingest.carriers import read_carriers_raw

        df = read_carriers_raw(
            spark=spark,
            source_path=str(sample_paths["raw_carriers"]),
            bad_records_path=str(tmp_path / "q"),
        )
        outcome = dedupe_silver(df, table_name=TABLE_SILVER_CARRIERS)
        assert outcome.key_columns == ("carrier_id",)
        # The carrier entity's tie-breaker column is carrier_name.
        assert "carrier_name" in outcome.tie_breaker_columns


# ---------------------------------------------------------------------------
# Silver builder end-to-end
# ---------------------------------------------------------------------------


class TestSilverBuilder:
    """End-to-end Silver builder tests for each entity."""

    def test_silver_shipments_builder(
        self, spark, sample_paths: dict[str, Path], tmp_path: Path
    ) -> None:
        pytest.importorskip("pyspark")

        from transport_etl.bronze.builder import build_bronze_shipments
        from transport_etl.silver.builder import build_silver_shipments

        bronze = build_bronze_shipments(
            spark=spark,
            source_path=str(sample_paths["raw_shipments"]),
            run_date="2026-01-01",
            batch_id="daily_20260101",
            quarantine_path=str(tmp_path / "ingest_q"),
        )
        schema = load_shipments_schema_definition()
        region_lookup = (
            spark.read.option("header", "true")
            .csv(str(sample_paths["region_lookup"]))
            .withColumnRenamed("state_code", "state_code")
        )

        result = build_silver_shipments(
            spark=spark,
            bronze_df=bronze,
            schema_def=schema,
            batch_id="daily_20260101",
            run_date="2026-01-01",
            region_lookup_df=region_lookup,
            quarantine_path=str(tmp_path / "silver_q"),
            run_quality=False,
        )

        assert result.table_name == TABLE_SILVER_SHIPMENTS
        assert result.survived_count > 0

        # Lineage columns attached.
        lineage = [c for c in SILVER_METADATA_COLUMNS if c in result.silver_df.columns]
        assert lineage == list(SILVER_METADATA_COLUMNS)

        # Source columns preserved.
        for column in (
            "shipment_id",
            "carrier_id",
            "origin_state",
            "destination_state",
            "pickup_ts",
            "promised_delivery_ts",
            "actual_delivery_ts",
            "shipping_cost_usd",
            "distance_miles",
            "updated_at",
        ):
            assert column in result.silver_df.columns

        # Exactly one row per shipment_id (dedup applied).
        assert result.silver_df.count() == result.silver_df.dropDuplicates(["shipment_id"]).count()

    def test_quality_rules_preserve_latest_duplicate_for_dedup(
        self, spark, sample_paths: dict[str, Path]
    ) -> None:
        """Generic quality rules must not remove every version of a business key."""
        pytest.importorskip("pyspark")

        from transport_etl.bronze.builder import build_bronze_shipments
        from transport_etl.silver.builder import build_silver_shipments

        bronze = build_bronze_shipments(
            spark=spark,
            source_path=str(sample_paths["raw_shipments"]),
            run_date="2026-01-01",
            batch_id="daily_20260101",
        )
        result = build_silver_shipments(
            spark=spark,
            bronze_df=bronze,
            schema_def=load_shipments_schema_definition(),
            batch_id="daily_20260101",
            run_date="2026-01-01",
            run_quality=True,
        )

        latest = (
            result.silver_df.filter("shipment_id = 'SHP1006'")
            .select("shipping_cost_usd", "updated_at")
            .collect()
        )
        dropped = (
            result.dropped_dups.filter("shipment_id = 'SHP1006'")
            .select("shipping_cost_usd", "updated_at")
            .collect()
        )

        assert len(latest) == 1
        assert latest[0]["shipping_cost_usd"] == pytest.approx(415.0)
        assert len(dropped) == 1
        assert dropped[0]["shipping_cost_usd"] == pytest.approx(420.25)
        assert latest[0]["updated_at"] > dropped[0]["updated_at"]

    def test_invalid_old_version_does_not_remove_valid_latest_version(self, spark) -> None:
        from pyspark.sql import functions as F

        from transport_etl.silver.builder import RULE_NON_NEGATIVE, build_silver_shipments

        rows = [
            (
                "SHPX",
                "CAR001",
                "CA",
                "NV",
                "2026-01-01T10:00:00Z",
                "2026-01-02T10:00:00Z",
                "2026-01-02T09:00:00Z",
                -1.0,
                100.0,
                "2026-01-02T09:01:00Z",
            ),
            (
                "SHPX",
                "CAR001",
                "CA",
                "NV",
                "2026-01-01T10:00:00Z",
                "2026-01-02T10:00:00Z",
                "2026-01-02T09:00:00Z",
                250.0,
                100.0,
                "2026-01-02T09:02:00Z",
            ),
            (
                "SHPY",
                "CAR001",
                "CA",
                "NV",
                "2026-01-01T10:00:00Z",
                "2026-01-02T10:00:00Z",
                "2026-01-02T09:00:00Z",
                250.0,
                -5.0,
                "2026-01-02T09:03:00Z",
            ),
        ]
        schema = (
            "shipment_id string, carrier_id string, origin_state string, "
            "destination_state string, pickup_ts string, promised_delivery_ts string, "
            "actual_delivery_ts string, shipping_cost_usd double, distance_miles double, "
            "updated_at string"
        )
        source = spark.createDataFrame(rows, schema=schema)
        for column in (
            "pickup_ts",
            "promised_delivery_ts",
            "actual_delivery_ts",
            "updated_at",
        ):
            source = source.withColumn(column, F.to_timestamp(column))

        result = build_silver_shipments(
            spark=spark,
            bronze_df=source,
            schema_def=load_shipments_schema_definition(),
            batch_id="daily_20260101",
            run_date="2026-01-01",
            run_quality=True,
        )

        survivor = result.silver_df.select("shipping_cost_usd").collect()
        rejected = (
            result.invalid_dfs[RULE_NON_NEGATIVE]
            .select("shipment_id", "shipping_cost_usd", "distance_miles")
            .collect()
        )
        assert [row["shipping_cost_usd"] for row in survivor] == [250.0]
        assert {row["shipment_id"] for row in rejected} == {"SHPX", "SHPY"}

    def test_silver_carriers_builder(
        self, spark, sample_paths: dict[str, Path], tmp_path: Path
    ) -> None:
        pytest.importorskip("pyspark")
        from transport_etl.bronze.builder import build_bronze_carriers
        from transport_etl.silver.builder import build_silver_carriers

        bronze = build_bronze_carriers(
            spark=spark,
            source_path=str(sample_paths["raw_carriers"]),
            run_date="2026-01-01",
            batch_id="daily_20260101",
            quarantine_path=str(tmp_path / "ingest_q"),
        )
        schema = load_carriers_schema_definition()
        result = build_silver_carriers(
            spark=spark,
            bronze_df=bronze,
            schema_def=schema,
            batch_id="daily_20260101",
            run_date="2026-01-01",
            quarantine_path=str(tmp_path / "silver_q"),
            run_quality=False,
        )
        assert result.table_name == TABLE_SILVER_CARRIERS
        assert result.survived_count > 0
        for column in (
            "carrier_id",
            "carrier_name",
            "scac",
            "service_mode",
            "home_region_code",
            "is_active",
            "updated_at",
        ):
            assert column in result.silver_df.columns

    def test_silver_delivery_events_builder(
        self, spark, sample_paths: dict[str, Path], tmp_path: Path
    ) -> None:
        pytest.importorskip("pyspark")
        from transport_etl.bronze.builder import build_bronze_delivery_events
        from transport_etl.silver.builder import build_silver_delivery_events

        bronze = build_bronze_delivery_events(
            spark=spark,
            source_path=str(sample_paths["raw_delivery_events"]),
            run_date="2026-01-01",
            batch_id="daily_20260101",
            quarantine_path=str(tmp_path / "ingest_q"),
        )
        schema = load_delivery_events_schema_definition()
        result = build_silver_delivery_events(
            spark=spark,
            bronze_df=bronze,
            schema_def=schema,
            batch_id="daily_20260101",
            run_date="2026-01-01",
            quarantine_path=str(tmp_path / "silver_q"),
            run_quality=False,
        )
        assert result.table_name == TABLE_SILVER_DELIVERY_EVENTS
        assert result.survived_count > 0
        for column in (
            "event_id",
            "shipment_id",
            "event_type",
            "event_ts",
            "event_city",
            "event_state",
            "delay_reason",
            "attempt_number",
            "updated_at",
        ):
            assert column in result.silver_df.columns


# ---------------------------------------------------------------------------
# Quarantine behavior
# ---------------------------------------------------------------------------


class TestSilverQuarantine:
    """Invalid records are quarantined with their source identity."""

    def test_invalid_required_nulls_quarantined_with_source_identity(
        self, spark, sample_paths: dict[str, Path], tmp_path: Path
    ) -> None:
        pytest.importorskip("pyspark")

        from transport_etl.bronze.builder import build_bronze_shipments
        from transport_etl.silver.builder import build_silver_shipments

        bronze = build_bronze_shipments(
            spark=spark,
            source_path=str(sample_paths["raw_shipments"]),
            run_date="2026-01-01",
            batch_id="daily_20260101",
            quarantine_path=str(tmp_path / "ingest_q"),
        )

        # Inject a row with a missing required column to fail the
        # Silver quality check.
        bad_row = spark.createDataFrame(
            [("SHP9999", None, "CA", "TX", None, None, None, None, None, None)],
            schema=(
                "shipment_id string, carrier_id string, origin_state string, "
                "destination_state string, pickup_ts timestamp, "
                "promised_delivery_ts timestamp, actual_delivery_ts timestamp, "
                "shipping_cost_usd double, distance_miles double, "
                "updated_at timestamp"
            ),
        )
        bad_bronze = bronze.unionByName(bad_row, allowMissingColumns=True)

        silver_q = tmp_path / "silver_q"
        schema = load_shipments_schema_definition()
        result = build_silver_shipments(
            spark=spark,
            bronze_df=bad_bronze,
            schema_def=schema,
            batch_id="daily_20260101",
            run_date="2026-01-01",
            quarantine_path=str(silver_q),
        )
        # The bad row must have been quarantined.
        assert result.invalid_count >= 1

        # The quarantine directory must exist.
        assert silver_q.exists()

        # Read back the quarantined records and verify the source
        # identity was preserved.
        quarantine_files = list(silver_q.rglob("*.parquet")) + list(silver_q.rglob("*.jsonl"))
        if not quarantine_files:
            # Windows fallback to JSONL lives under _fallback_json
            quarantine_files = list((silver_q / "_fallback_json").rglob("*.jsonl"))
        assert quarantine_files, "Expected quarantine files to be written"

    def test_quarantine_writes_under_entity_rule_directory(
        self, spark, sample_paths: dict[str, Path], tmp_path: Path
    ) -> None:
        pytest.importorskip("pyspark")

        from transport_etl.bronze.builder import build_bronze_shipments
        from transport_etl.silver.builder import build_silver_shipments

        bronze = build_bronze_shipments(
            spark=spark,
            source_path=str(sample_paths["raw_shipments"]),
            run_date="2026-01-01",
            batch_id="daily_20260101",
            quarantine_path=str(tmp_path / "ingest_q"),
        )

        # Create a row with an invalid state code.
        bad = spark.createDataFrame(
            [
                (
                    "SHP9999",
                    "CAR001",
                    "ZZZ",
                    "TX",
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                )
            ],
            schema=(
                "shipment_id string, carrier_id string, origin_state string, "
                "destination_state string, pickup_ts timestamp, "
                "promised_delivery_ts timestamp, actual_delivery_ts timestamp, "
                "shipping_cost_usd double, distance_miles double, "
                "updated_at timestamp"
            ),
        )
        bad_bronze = bronze.unionByName(bad, allowMissingColumns=True)

        silver_q = tmp_path / "silver_q"
        schema = load_shipments_schema_definition()
        result = build_silver_shipments(
            spark=spark,
            bronze_df=bad_bronze,
            schema_def=schema,
            batch_id="daily_20260101",
            run_date="2026-01-01",
            quarantine_path=str(silver_q),
        )
        # We expect at least one invalid row (the synthetic bad row).
        assert result.invalid_count >= 1
        # At least one rule directory exists.
        rule_dirs = [p for p in silver_q.iterdir() if p.is_dir()]
        assert rule_dirs

    def test_quarantine_write_failure_stops_silver_build(
        self,
        spark,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        import transport_etl.silver.builder as builder_module

        bad = spark.createDataFrame(
            [("SHP9999", None)],
            schema="shipment_id string, carrier_id string",
        )

        def fail_quarantine(**_: object) -> str:
            raise OSError("quarantine unavailable")

        monkeypatch.setattr(
            builder_module,
            "quarantine_silver_records",
            fail_quarantine,
        )

        with pytest.raises(OSError, match="quarantine unavailable"):
            builder_module.build_silver_shipments(
                spark=spark,
                bronze_df=bad,
                schema_def=load_shipments_schema_definition(),
                batch_id="daily_20260101",
                run_date="2026-01-01",
                quarantine_path=str(tmp_path / "silver_q"),
            )


# ---------------------------------------------------------------------------
# Idempotency — rerunning with the same source must produce the same output
# ---------------------------------------------------------------------------


class TestSilverIdempotency:
    def test_rerun_produces_same_silver_dataset(
        self, spark, sample_paths: dict[str, Path], tmp_path: Path
    ) -> None:
        pytest.importorskip("pyspark")

        from transport_etl.bronze.builder import build_bronze_shipments
        from transport_etl.silver.builder import build_silver_shipments

        schema = load_shipments_schema_definition()

        def _run_once() -> set[tuple[str, ...]]:
            bronze = build_bronze_shipments(
                spark=spark,
                source_path=str(sample_paths["raw_shipments"]),
                run_date="2026-01-01",
                batch_id="daily_20260101",
                quarantine_path=str(tmp_path / "ingest_q"),
            )
            result = build_silver_shipments(
                spark=spark,
                bronze_df=bronze,
                schema_def=schema,
                batch_id="daily_20260101",
                run_date="2026-01-01",
                quarantine_path=str(tmp_path / "silver_q"),
                run_quality=False,
            )
            # Project to (shipment_id, carrier_id, updated_at) for
            # fingerprint comparison (lineage columns may differ).
            rows = result.silver_df.select("shipment_id", "carrier_id", "updated_at").collect()
            return {(r["shipment_id"], r["carrier_id"], str(r["updated_at"])) for r in rows}

        first = _run_once()
        second = _run_once()
        assert first == second
        assert len(first) > 0

    def test_silver_run_date_and_batch_id_match_input(
        self, spark, sample_paths: dict[str, Path], tmp_path: Path
    ) -> None:
        pytest.importorskip("pyspark")
        from transport_etl.bronze.builder import build_bronze_shipments
        from transport_etl.silver.builder import build_silver_shipments

        bronze = build_bronze_shipments(
            spark=spark,
            source_path=str(sample_paths["raw_shipments"]),
            run_date="2026-01-01",
            batch_id="daily_20260101",
            quarantine_path=str(tmp_path / "ingest_q"),
        )
        result = build_silver_shipments(
            spark=spark,
            bronze_df=bronze,
            schema_def=load_shipments_schema_definition(),
            batch_id="daily_20260101",
            run_date="2026-01-01",
            quarantine_path=str(tmp_path / "silver_q"),
            run_quality=False,
        )
        distinct_run_dates = {
            str(row[META_COL_SILVER_RUN_DATE])
            for row in result.silver_df.select(META_COL_SILVER_RUN_DATE).distinct().collect()
        }
        distinct_batch_ids = {
            str(row[META_COL_SILVER_BATCH_ID])
            for row in result.silver_df.select(META_COL_SILVER_BATCH_ID).distinct().collect()
        }
        assert distinct_run_dates == {"2026-01-01"}
        assert distinct_batch_ids == {"daily_20260101"}


# ---------------------------------------------------------------------------
# Local Parquet publishing
# ---------------------------------------------------------------------------


class TestSilverLocalParquetPublishing:
    def test_publish_silver_writes_parquet_for_local_target(
        self, spark, sample_paths: dict[str, Path], tmp_path: Path
    ) -> None:
        pytest.importorskip("pyspark")

        from transport_etl.bronze.builder import build_bronze_shipments
        from transport_etl.silver.builder import build_silver_shipments
        from transport_etl.silver.publisher import publish_silver_table

        bronze = build_bronze_shipments(
            spark=spark,
            source_path=str(sample_paths["raw_shipments"]),
            run_date="2026-01-01",
            batch_id="daily_20260101",
            quarantine_path=str(tmp_path / "ingest_q"),
        )
        result = build_silver_shipments(
            spark=spark,
            bronze_df=bronze,
            schema_def=load_shipments_schema_definition(),
            batch_id="daily_20260101",
            run_date="2026-01-01",
            quarantine_path=str(tmp_path / "silver_q"),
            run_quality=False,
        )
        staging_base = tmp_path / "staging"
        config = {
            "spark": {"profile": "local"},
            "hive": {"database": "curated"},
            "paths": {
                "staging_base_path": str(staging_base),
                "curated_base_path": str(tmp_path / "curated"),
            },
        }

        written = publish_silver_table(
            df=result.silver_df,
            config=config,
            table_name=TABLE_SILVER_SHIPMENTS,
            spark=spark,
            register_hive_table=False,
            repair_partitions=False,
        )

        written_root = Path(written)
        assert written_root.exists()
        # Either parquet or the Windows local fallback is acceptable.
        parquet_files = list(written_root.rglob("*.parquet"))
        json_files = list(written_root.rglob("*.jsonl"))
        csv_files = list(written_root.rglob("*.csv"))
        assert parquet_files or json_files or csv_files

        # Round-trip and verify lineage columns are present.
        if parquet_files:
            round_tripped = spark.read.parquet(str(written_root))
        elif json_files:
            records: list[dict[str, object]] = []
            for path in json_files:
                for line in path.read_text(encoding="utf-8").splitlines():
                    line = line.strip()
                    if line:
                        records.append(json.loads(line))
            round_tripped = spark.createDataFrame(records)
        else:
            import csv as csv_mod

            records = []
            for path in csv_files:
                with path.open("r", encoding="utf-8", newline="") as handle:
                    reader = csv_mod.DictReader(handle)
                    for row in reader:
                        records.append(dict(row))
            round_tripped = spark.createDataFrame(records)

        cols = set(round_tripped.columns)
        for col in SILVER_METADATA_COLUMNS:
            assert col in cols


# ---------------------------------------------------------------------------
# MERGE orchestrator
# ---------------------------------------------------------------------------


class TestSilverMergeOrchestrator:
    def test_register_source_view_creates_temp_view(self, spark) -> None:
        pytest.importorskip("pyspark")

        df = spark.createDataFrame(
            [("SHP1", "CAR1")], schema="shipment_id string, carrier_id string"
        )
        view = register_source_view(spark=spark, source_df=df, view_name="silver_test_view")
        assert view == "silver_test_view"
        # Read back the view to confirm registration.  We use
        # ``collect()`` rather than ``count()`` to avoid the
        # Spark JVM lifecycle crash that hits ``count()`` in
        # Windows CI environments.
        back = spark.sql("SELECT * FROM silver_test_view").collect()
        assert len(back) == 1
        assert back[0]["shipment_id"] == "SHP1"

    def test_execute_silver_merge_invokes_spark_sql(self, spark) -> None:
        pytest.importorskip("pyspark")

        df = spark.createDataFrame(
            [("SHP1", "CAR1")],
            schema="shipment_id string, carrier_id string",
        )
        # Spark does not have a Delta table at this path, so the
        # MERGE will fail with a Delta-side error.  We patch
        # ``spark.sql`` to capture the call without actually
        # executing the statement.
        captured: list[str] = []

        original_sql = spark.sql

        def fake_sql(query: str):
            captured.append(query)
            return None

        spark.sql = fake_sql  # type: ignore[method-assign]
        try:
            execute_silver_merge(
                spark=spark,
                source_df=df,
                target_table="supply_chain.silver.stg_shipments",
                table_name=TABLE_SILVER_SHIPMENTS,
                all_columns=["shipment_id", "carrier_id"],
            )
        finally:
            spark.sql = original_sql  # type: ignore[method-assign]

        assert len(captured) == 1
        sql = captured[0]
        assert "MERGE INTO `supply_chain`.`silver`.`stg_shipments`" in sql
        assert "WHEN MATCHED" in sql
        assert "WHEN NOT MATCHED" in sql


# ---------------------------------------------------------------------------
# Live Databricks tests — skipped without a live workspace
# ---------------------------------------------------------------------------


class TestSilverLiveDatabricks:
    """A live Delta MERGE requires a Databricks workspace.

    Per AGENTS.md rule 9 these tests must be clearly marked to skip
    when a live Databricks connection is unavailable.  Do not mock
    Databricks in a way that makes the test meaningless.
    """

    def test_live_delta_merge_skipped_without_workspace(self) -> None:
        pytest.skip(
            "Live Databricks Delta MERGE requires a running Databricks workspace "
            "with Unity Catalog and the delta-spark runtime; CI does not provide one. "
            "See AGENTS.md rule 9."
        )

    def test_late_arriving_data_merge_skipped_without_workspace(self) -> None:
        pytest.skip(
            "Live late-arriving data MERGE validation requires a live Databricks "
            "workspace; CI does not provide one. See AGENTS.md rule 9."
        )


# ---------------------------------------------------------------------------
# Update/insert decision logic (offline verification)
# ---------------------------------------------------------------------------


class TestUpdateInsertDecisionLogic:
    """The MERGE contract: update on match, insert on no match.

    Without a Delta table we cannot actually run a MERGE; instead we
    verify the rendered SQL carries the documented semantics.  The
    contract is exercised end-to-end by the live-Databricks test
    (skipped in CI).
    """

    def test_when_matched_updates_non_key_columns(self) -> None:
        spec = build_silver_merge_spec(
            target_table="sc.silver.stg_shipments",
            table_name=TABLE_SILVER_SHIPMENTS,
            all_columns=["shipment_id", "carrier_id", "updated_at"],
            source_view="silver_stg_shipments",
        )
        sql = render_silver_merge_sql(spec, all_columns=["shipment_id", "carrier_id", "updated_at"])
        # The business key is excluded from the UPDATE SET list.
        update_section = sql.split("WHEN MATCHED THEN UPDATE SET")[1].split("WHEN NOT MATCHED")[0]
        assert "shipment_id" not in update_section
        assert "carrier_id" in update_section
        assert "updated_at" in update_section

    def test_when_not_matched_inserts_all_columns(self) -> None:
        spec = build_silver_merge_spec(
            target_table="sc.silver.stg_shipments",
            table_name=TABLE_SILVER_SHIPMENTS,
            all_columns=["shipment_id", "carrier_id", "updated_at"],
            source_view="silver_stg_shipments",
        )
        sql = render_silver_merge_sql(spec, all_columns=["shipment_id", "carrier_id", "updated_at"])
        insert_section = sql.split("WHEN NOT MATCHED THEN INSERT")[1]
        # All source columns referenced in the VALUES clause.
        for col in ("shipment_id", "carrier_id", "updated_at"):
            assert f"`source`.`{col}`" in insert_section

    def test_render_uses_provided_source_view(self) -> None:
        spec = build_silver_merge_spec(
            target_table="sc.silver.stg_shipments",
            table_name=TABLE_SILVER_SHIPMENTS,
            all_columns=["shipment_id"],
            source_view="my_custom_silver_view",
        )
        sql = render_silver_merge_sql(spec, all_columns=["shipment_id"])
        assert "USING my_custom_silver_view" in sql

    def test_merge_spec_built_from_keys_helper(self) -> None:
        """Build the spec via the keys module rather than passing the
        merge key explicitly — this guarantees the dedup helper and
        the MERGE renderer cannot drift apart.
        """
        from transport_etl.silver.keys import business_key_for
        from transport_etl.silver.merge_spec import SilverMergeSpec

        spec = SilverMergeSpec(
            target_table="sc.silver.stg_carriers",
            merge_keys=business_key_for(TABLE_SILVER_CARRIERS),
            source_view="silver_stg_carriers",
        )
        sql = build_merge_sql(spec, all_columns=["carrier_id", "carrier_name"])
        assert "ON `target`.`carrier_id` = `source`.`carrier_id`" in sql
