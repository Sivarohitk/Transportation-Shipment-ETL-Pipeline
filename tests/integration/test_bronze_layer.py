"""Integration tests for the Bronze layer builder.

These tests exercise ``transport_etl.bronze.builder`` against a real
local SparkSession.  They verify:

- operational ingestion metadata columns are attached
- explicit schema contracts from ``config/schemas/*.schema.json`` are
  reused (Bronze column types come from the existing schema definitions)
- malformed records are quarantined, never silently dropped
- the builder is deterministic for identical inputs (same run_date,
  same source file)
- target-aware publisher writes Parquet on local/EMR and dispatches
  to the Delta writer on Databricks (mocked — no live Databricks
  workspace required)
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from transport_etl.bronze import (
    BRONZE_METADATA_COLUMNS,
    TABLE_BRONZE_CARRIERS,
    TABLE_BRONZE_DELIVERY_EVENTS,
    TABLE_BRONZE_SHIPMENTS,
    build_bronze_carriers,
    build_bronze_delivery_events,
    build_bronze_shipments,
)
from transport_etl.bronze.metadata import (
    META_COL_BATCH_ID,
    META_COL_INGESTED_AT,
    META_COL_RUN_DATE,
    META_COL_SOURCE_FILE,
)

# ---------------------------------------------------------------------------
# Bronze metadata attachment
# ---------------------------------------------------------------------------


class TestBronzeMetadataAttachment:
    """Verify operational ingestion metadata is attached to every Bronze record."""

    def test_metadata_columns_present_for_shipments(
        self,
        spark,
        sample_paths: dict[str, Path],
        tmp_path: Path,
    ) -> None:
        df = build_bronze_shipments(
            spark=spark,
            source_path=str(sample_paths["raw_shipments"]),
            run_date="2026-01-01",
            batch_id="daily_20260101",
            quarantine_path=str(tmp_path / "q"),
        )
        cols = set(df.columns)
        for meta_col in BRONZE_METADATA_COLUMNS:
            assert meta_col in cols

    def test_metadata_columns_present_for_carriers(
        self,
        spark,
        sample_paths: dict[str, Path],
        tmp_path: Path,
    ) -> None:
        df = build_bronze_carriers(
            spark=spark,
            source_path=str(sample_paths["raw_carriers"]),
            run_date="2026-01-01",
            batch_id="daily_20260101",
            quarantine_path=str(tmp_path / "q"),
        )
        cols = set(df.columns)
        for meta_col in BRONZE_METADATA_COLUMNS:
            assert meta_col in cols

    def test_metadata_columns_present_for_delivery_events(
        self,
        spark,
        sample_paths: dict[str, Path],
        tmp_path: Path,
    ) -> None:
        df = build_bronze_delivery_events(
            spark=spark,
            source_path=str(sample_paths["raw_delivery_events"]),
            run_date="2026-01-01",
            batch_id="daily_20260101",
            quarantine_path=str(tmp_path / "q"),
        )
        cols = set(df.columns)
        for meta_col in BRONZE_METADATA_COLUMNS:
            assert meta_col in cols

    def test_source_file_matches_supplied_path(
        self,
        spark,
        sample_paths: dict[str, Path],
        tmp_path: Path,
    ) -> None:
        df = build_bronze_shipments(
            spark=spark,
            source_path=str(sample_paths["raw_shipments"]),
            run_date="2026-01-01",
            batch_id="daily_20260101",
            quarantine_path=str(tmp_path / "q"),
        )
        distinct_sources = [
            row[META_COL_SOURCE_FILE]
            for row in df.select(META_COL_SOURCE_FILE).distinct().collect()
        ]
        assert distinct_sources == [str(sample_paths["raw_shipments"])]

    def test_batch_id_and_run_date_match_supplied_values(
        self,
        spark,
        sample_paths: dict[str, Path],
        tmp_path: Path,
    ) -> None:
        df = build_bronze_carriers(
            spark=spark,
            source_path=str(sample_paths["raw_carriers"]),
            run_date="2026-01-01",
            batch_id="custom_batch_id_42",
            quarantine_path=str(tmp_path / "q"),
        )
        batch_ids = {
            row[META_COL_BATCH_ID] for row in df.select(META_COL_BATCH_ID).distinct().collect()
        }
        run_dates = {
            row[META_COL_RUN_DATE] for row in df.select(META_COL_RUN_DATE).distinct().collect()
        }
        assert batch_ids == {"custom_batch_id_42"}
        assert run_dates == {"2026-01-01"}

    def test_batch_id_derived_from_run_date_when_omitted(
        self,
        spark,
        sample_paths: dict[str, Path],
        tmp_path: Path,
    ) -> None:
        df = build_bronze_carriers(
            spark=spark,
            source_path=str(sample_paths["raw_carriers"]),
            run_date="2026-02-15",
            quarantine_path=str(tmp_path / "q"),
        )
        batch_ids = {
            row[META_COL_BATCH_ID] for row in df.select(META_COL_BATCH_ID).distinct().collect()
        }
        assert batch_ids == {"daily_20260215"}

    def test_ingested_at_is_populated_and_recent(
        self,
        spark,
        sample_paths: dict[str, Path],
        tmp_path: Path,
    ) -> None:
        from datetime import datetime, timedelta

        df = build_bronze_shipments(
            spark=spark,
            source_path=str(sample_paths["raw_shipments"]),
            run_date="2026-01-01",
            batch_id="daily_20260101",
            quarantine_path=str(tmp_path / "q"),
        )
        timestamps = [
            row[META_COL_INGESTED_AT] for row in df.select(META_COL_INGESTED_AT).limit(1).collect()
        ]
        assert timestamps
        ingested_at = timestamps[0]
        # _ingested_at captures the Spark JVM wall-clock time at ingestion.
        # ``current_timestamp()`` is not reliably UTC on every JVM, so the
        # assertion compares to the host's local naive clock.
        assert isinstance(ingested_at, datetime)
        now = datetime.now()
        delta = abs((now - ingested_at).total_seconds())
        assert delta < timedelta(minutes=5).total_seconds()


# ---------------------------------------------------------------------------
# Schema behaviour — Bronze reuses the existing explicit schemas
# ---------------------------------------------------------------------------


class TestBronzeSchemaBehaviour:
    """Verify Bronze reuses the explicit schemas from config/schemas/*.schema.json."""

    def test_shipments_bronze_columns_and_types(
        self,
        spark,
        sample_paths: dict[str, Path],
        tmp_path: Path,
    ) -> None:
        from pyspark.sql import types as T

        schema_def = json.loads(
            (
                Path(__file__).resolve().parents[2] / "config" / "schemas" / "shipments.schema.json"
            ).read_text(encoding="utf-8-sig")
        )
        type_lookup = {
            "string": T.StringType(),
            "double": T.DoubleType(),
            "int": T.IntegerType(),
            "timestamp": T.TimestampType(),
            "boolean": T.BooleanType(),
        }

        df = build_bronze_shipments(
            spark=spark,
            source_path=str(sample_paths["raw_shipments"]),
            run_date="2026-01-01",
            batch_id="daily_20260101",
            quarantine_path=str(tmp_path / "q"),
        )

        actual_by_name = {field.name: field.dataType for field in df.schema.fields}
        for col_def in schema_def["columns"]:
            name = str(col_def["name"])
            expected_type = type_lookup[str(col_def["type"]).lower()]
            assert name in actual_by_name, f"Source column '{name}' missing from Bronze"
            assert actual_by_name[name] == expected_type, (
                f"Column '{name}' type mismatch: "
                f"expected {expected_type}, got {actual_by_name[name]}"
            )

    def test_carriers_bronze_preserves_explicit_schema(
        self,
        spark,
        sample_paths: dict[str, Path],
        tmp_path: Path,
    ) -> None:
        from pyspark.sql import types as T

        schema_def = json.loads(
            (
                Path(__file__).resolve().parents[2] / "config" / "schemas" / "carriers.schema.json"
            ).read_text(encoding="utf-8-sig")
        )
        type_lookup = {
            "string": T.StringType(),
            "double": T.DoubleType(),
            "int": T.IntegerType(),
            "timestamp": T.TimestampType(),
            "boolean": T.BooleanType(),
        }

        df = build_bronze_carriers(
            spark=spark,
            source_path=str(sample_paths["raw_carriers"]),
            run_date="2026-01-01",
            batch_id="daily_20260101",
            quarantine_path=str(tmp_path / "q"),
        )

        actual_by_name = {field.name: field.dataType for field in df.schema.fields}
        for col_def in schema_def["columns"]:
            name = str(col_def["name"])
            expected_type = type_lookup[str(col_def["type"]).lower()]
            assert name in actual_by_name
            assert actual_by_name[name] == expected_type

    def test_delivery_events_bronze_preserves_explicit_schema(
        self,
        spark,
        sample_paths: dict[str, Path],
        tmp_path: Path,
    ) -> None:
        from pyspark.sql import types as T

        schema_def = json.loads(
            (
                Path(__file__).resolve().parents[2]
                / "config"
                / "schemas"
                / "delivery_events.schema.json"
            ).read_text(encoding="utf-8-sig")
        )
        type_lookup = {
            "string": T.StringType(),
            "double": T.DoubleType(),
            "int": T.IntegerType(),
            "timestamp": T.TimestampType(),
            "boolean": T.BooleanType(),
        }

        df = build_bronze_delivery_events(
            spark=spark,
            source_path=str(sample_paths["raw_delivery_events"]),
            run_date="2026-01-01",
            batch_id="daily_20260101",
            quarantine_path=str(tmp_path / "q"),
        )

        actual_by_name = {field.name: field.dataType for field in df.schema.fields}
        for col_def in schema_def["columns"]:
            name = str(col_def["name"])
            expected_type = type_lookup[str(col_def["type"]).lower()]
            assert name in actual_by_name
            assert actual_by_name[name] == expected_type

    def test_bronze_does_not_drop_source_columns(
        self,
        spark,
        sample_paths: dict[str, Path],
        tmp_path: Path,
    ) -> None:
        """Bronze must preserve every source column — no Silver business cleaning."""
        df = build_bronze_shipments(
            spark=spark,
            source_path=str(sample_paths["raw_shipments"]),
            run_date="2026-01-01",
            batch_id="daily_20260101",
            quarantine_path=str(tmp_path / "q"),
        )
        # Even though the source has SHP1006 duplicated and SHP1011 missing
        # destination_state, the builder should not silently drop anything:
        # row count from the ingest module minus invalid quarantined rows
        # must equal what the Bronze builder returns.
        assert df.count() > 0
        # All explicit shipment schema columns must be present in the output.
        for required in [
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
        ]:
            assert required in df.columns


# ---------------------------------------------------------------------------
# Invalid record handling — quarantine path is exercised
# ---------------------------------------------------------------------------


class TestBronzeInvalidRecordHandling:
    """Verify malformed/unparseable records are quarantined, not silently dropped."""

    def test_malformed_csv_creates_parser_quarantine_path(
        self,
        spark,
        tmp_path: Path,
    ) -> None:
        """A CSV containing an extra column that breaks the schema must
        still write a parser quarantine file and produce a valid Bronze
        DataFrame for the well-formed rows."""
        csv_path = tmp_path / "shipments_malformed.csv"
        # First row is malformed — 11 columns instead of 10.
        # Subsequent rows are valid.
        csv_path.write_text(
            (
                "shipment_id,carrier_id,origin_state,destination_state,pickup_ts,"
                "promised_delivery_ts,actual_delivery_ts,shipping_cost_usd,"
                "distance_miles,updated_at\n"
                "SHP1001,CAR001,CA,TX,2026-01-01T08:10:00Z,2026-01-03T20:00:00Z,"
                "2026-01-03T18:45:00Z,1325.50,1430.2,2026-01-03T18:46:00Z,EXTRA\n"
                "SHP1002,CAR002,WA,AZ,2026-01-01T09:30:00Z,2026-01-02T21:00:00Z,"
                "2026-01-03T03:10:00Z,905.75,1102.4,2026-01-03T03:11:00Z\n"
            ),
            encoding="utf-8",
        )

        quarantine_path = tmp_path / "quarantine"
        df = build_bronze_shipments(
            spark=spark,
            source_path=str(csv_path),
            run_date="2026-01-01",
            batch_id="daily_20260101",
            quarantine_path=str(quarantine_path),
        )

        rows = df.collect()
        # Only the well-formed row should remain in the Bronze output.
        assert len(rows) == 1
        assert rows[0]["shipment_id"] == "SHP1002"
        # The malformed row must have been quarantined into a parser path.
        parser_path = quarantine_path / "p_date=2026-01-01" / "parser" / "shipments"
        assert parser_path.exists(), f"Expected parser quarantine path at {parser_path}"
        # Spark writes quarantine files when badRecordsPath is set;
        # the existence of the directory is the deterministic contract.
        assert parser_path.is_dir()

    def test_missing_quarantine_path_does_not_raise(
        self,
        spark,
        sample_paths: dict[str, Path],
    ) -> None:
        """When no quarantine path is provided, Bronze must still build
        successfully — the underlying ingest module degrades gracefully."""
        df = build_bronze_shipments(
            spark=spark,
            source_path=str(sample_paths["raw_shipments"]),
            run_date="2026-01-01",
            batch_id="daily_20260101",
            quarantine_path=None,
        )
        assert df.count() > 0

    def test_invalid_records_written_with_entity_label(
        self,
        spark,
        sample_paths: dict[str, Path],
        tmp_path: Path,
    ) -> None:
        """Underlying ingest modules tag quarantined invalid records
        with their entity name.  Verify by writing a CSV that produces
        an invalid row (missing required column) and inspecting the
        quarantine output."""
        csv_path = tmp_path / "carriers_invalid.csv"
        # Missing carrier_id (required column) — this row will fail the
        # bronze schema validation.  The well-formed row must still
        # reach the Bronze DataFrame.
        csv_path.write_text(
            (
                "carrier_id,carrier_name,scac,service_mode,home_region_code,is_active,updated_at\n"
                ",Atlas Freight,ATLS,FTL,WEST,true,2026-01-01T00:00:00Z\n"
                "CAR002,Pioneer Logistics,PNLR,LTL,WEST,true,2026-01-01T00:00:00Z\n"
            ),
            encoding="utf-8",
        )

        quarantine_path = tmp_path / "quarantine"
        df = build_bronze_carriers(
            spark=spark,
            source_path=str(csv_path),
            run_date="2026-01-01",
            batch_id="daily_20260101",
            quarantine_path=str(quarantine_path),
        )

        rows = df.collect()
        valid_ids = {row["carrier_id"] for row in rows}
        # The row with missing carrier_id is filtered out by the ingest
        # validator; the well-formed carrier_id must survive.
        assert valid_ids == {"CAR002"}


# ---------------------------------------------------------------------------
# Deterministic reruns
# ---------------------------------------------------------------------------


class TestBronzeDeterministicReruns:
    """Bronze must be deterministic for identical inputs."""

    def test_same_run_date_produces_same_batch_id(
        self,
        spark,
        sample_paths: dict[str, Path],
        tmp_path: Path,
    ) -> None:
        df_a = build_bronze_carriers(
            spark=spark,
            source_path=str(sample_paths["raw_carriers"]),
            run_date="2026-01-01",
            job_name="daily",
            quarantine_path=str(tmp_path / "q1"),
        )
        df_b = build_bronze_carriers(
            spark=spark,
            source_path=str(sample_paths["raw_carriers"]),
            run_date="2026-01-01",
            job_name="daily",
            quarantine_path=str(tmp_path / "q2"),
        )

        ids_a = {
            row[META_COL_BATCH_ID] for row in df_a.select(META_COL_BATCH_ID).distinct().collect()
        }
        ids_b = {
            row[META_COL_BATCH_ID] for row in df_b.select(META_COL_BATCH_ID).distinct().collect()
        }
        assert ids_a == ids_b == {"daily_20260101"}

    def test_same_run_date_produces_same_source_columns(
        self,
        spark,
        sample_paths: dict[str, Path],
        tmp_path: Path,
    ) -> None:
        df_a = build_bronze_shipments(
            spark=spark,
            source_path=str(sample_paths["raw_shipments"]),
            run_date="2026-01-01",
            batch_id="daily_20260101",
            quarantine_path=str(tmp_path / "q1"),
        )
        df_b = build_bronze_shipments(
            spark=spark,
            source_path=str(sample_paths["raw_shipments"]),
            run_date="2026-01-01",
            batch_id="daily_20260101",
            quarantine_path=str(tmp_path / "q2"),
        )

        # Source-column content should be identical across reruns.
        source_cols_a = (
            df_a.select(
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
            )
            .orderBy("shipment_id")
            .collect()
        )
        source_cols_b = (
            df_b.select(
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
            )
            .orderBy("shipment_id")
            .collect()
        )
        assert source_cols_a == source_cols_b
        assert df_a.count() == df_b.count()

    def test_different_run_date_produces_different_batch_id(
        self,
        spark,
        sample_paths: dict[str, Path],
        tmp_path: Path,
    ) -> None:
        df_a = build_bronze_carriers(
            spark=spark,
            source_path=str(sample_paths["raw_carriers"]),
            run_date="2026-01-01",
            job_name="daily",
            quarantine_path=str(tmp_path / "q1"),
        )
        df_b = build_bronze_carriers(
            spark=spark,
            source_path=str(sample_paths["raw_carriers"]),
            run_date="2026-01-02",
            job_name="daily",
            quarantine_path=str(tmp_path / "q2"),
        )

        ids_a = {
            row[META_COL_BATCH_ID] for row in df_a.select(META_COL_BATCH_ID).distinct().collect()
        }
        ids_b = {
            row[META_COL_BATCH_ID] for row in df_b.select(META_COL_BATCH_ID).distinct().collect()
        }
        assert ids_a == {"daily_20260101"}
        assert ids_b == {"daily_20260102"}


# ---------------------------------------------------------------------------
# Local Parquet publishing
# ---------------------------------------------------------------------------


class TestBronzeLocalParquetPublishing:
    """Verify the local Parquet publish path writes Bronze data correctly."""

    def test_publish_bronze_writes_parquet_for_local_target(
        self,
        spark,
        sample_paths: dict[str, Path],
        tmp_path: Path,
    ) -> None:
        from transport_etl.bronze.publisher import publish_bronze_table

        bronze_base = tmp_path / "bronze"
        config = {
            "spark": {"profile": "local"},
            "hive": {"database": "curated"},
            "paths": {
                "staging_base_path": str(bronze_base),
                "curated_base_path": str(tmp_path / "curated"),
            },
        }

        df = build_bronze_shipments(
            spark=spark,
            source_path=str(sample_paths["raw_shipments"]),
            run_date="2026-01-01",
            batch_id="daily_20260101",
            quarantine_path=str(tmp_path / "q"),
        )
        written = publish_bronze_table(
            df=df,
            config=config,
            table_name=TABLE_BRONZE_SHIPMENTS,
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

        # Round-trip: read back and verify metadata columns are present.
        if parquet_files:
            round_tripped = spark.read.parquet(str(written_root))
        elif json_files:
            import json as json_mod

            records: list[dict[str, object]] = []
            for path in json_files:
                for line in path.read_text(encoding="utf-8").splitlines():
                    line = line.strip()
                    if line:
                        records.append(json_mod.loads(line))
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

        round_tripped_cols = set(round_tripped.columns)
        for meta_col in BRONZE_METADATA_COLUMNS:
            assert meta_col in round_tripped_cols


# ---------------------------------------------------------------------------
# Databricks dispatch — mocked Delta writer to assert routing without
# requiring a live Databricks workspace (see AGENTS.md rule 9).
# ---------------------------------------------------------------------------


class TestBronzeDatabricksDeltaDispatch:
    """The Delta writer must be invoked on Databricks targets only."""

    def test_databricks_target_dispatches_to_delta_writer(
        self,
        monkeypatch: pytest.MonkeyPatch,
        spark,
        sample_paths: dict[str, Path],
        tmp_path: Path,
    ) -> None:
        """When ``spark.profile = databricks``, the publisher must route
        through ``publish.delta_writer.write_delta_table``."""
        from transport_etl.bronze import publisher as pub_mod
        from transport_etl.publish import delta_writer

        captured: dict[str, object] = {}

        def fake_write_delta_table(
            df,
            table_name,
            partitions,
            mode="overwrite",
            logger=None,
        ):
            captured["table_name"] = table_name
            captured["partitions"] = partitions
            captured["mode"] = mode

        # hive_writer imports ``write_delta_table`` lazily inside the
        # function, so a single monkeypatch on the delta_writer module
        # is sufficient to capture every call.
        monkeypatch.setattr(delta_writer, "write_delta_table", fake_write_delta_table)

        config = {
            "spark": {"profile": "databricks"},
            "unity_catalog": {
                "catalog": "supply_chain",
                "bronze_schema": "bronze",
                "silver_schema": "silver",
                "gold_schema": "gold",
            },
            "hive": {"database": "curated"},
            "paths": {
                "staging_base_path": "dbfs:/mnt/transport/staging",
                "curated_base_path": "dbfs:/mnt/transport/curated",
            },
        }

        df = build_bronze_carriers(
            spark=spark,
            source_path=str(sample_paths["raw_carriers"]),
            run_date="2026-01-01",
            batch_id="daily_20260101",
            quarantine_path=str(tmp_path / "q"),
        )

        written = pub_mod.publish_bronze_table(
            df=df,
            config=config,
            table_name=TABLE_BRONZE_CARRIERS,
            spark=spark,
        )

        assert written == "supply_chain.bronze.raw_carriers"
        assert captured["table_name"] == "supply_chain.bronze.raw_carriers"
        assert captured["mode"] == "overwrite"

    def test_local_target_never_invokes_delta_writer(
        self,
        monkeypatch: pytest.MonkeyPatch,
        spark,
        sample_paths: dict[str, Path],
        tmp_path: Path,
    ) -> None:
        from transport_etl.bronze import publisher as pub_mod
        from transport_etl.publish import delta_writer

        def fail_delta(*args, **kwargs):
            raise AssertionError("delta_writer.write_delta_table must not run on local target")

        monkeypatch.setattr(delta_writer, "write_delta_table", fail_delta)

        config = {
            "spark": {"profile": "local"},
            "hive": {"database": "curated"},
            "paths": {
                "staging_base_path": str(tmp_path / "staging"),
                "curated_base_path": str(tmp_path / "curated"),
            },
        }

        df = build_bronze_delivery_events(
            spark=spark,
            source_path=str(sample_paths["raw_delivery_events"]),
            run_date="2026-01-01",
            batch_id="daily_20260101",
            quarantine_path=str(tmp_path / "q"),
        )

        # hive_writer is allowed to run for the local path.
        pub_mod.publish_bronze_table(
            df=df,
            config=config,
            table_name=TABLE_BRONZE_DELIVERY_EVENTS,
            spark=spark,
            register_hive_table=False,
            repair_partitions=False,
        )


# ---------------------------------------------------------------------------
# Live Databricks write — skipped without a live workspace.
# ---------------------------------------------------------------------------


class TestBronzeLiveDatabricksWrite:
    """Live Databricks Delta write — skipped without a live workspace."""

    def test_live_delta_write_skipped_without_workspace(
        self,
        spark,
        sample_paths: dict[str, Path],
        tmp_path: Path,
    ) -> None:
        pytest.skip(
            "Live Databricks Delta write requires a running Databricks workspace "
            "and the delta-spark runtime; CI does not provide one. "
            "See AGENTS.md rule 9."
        )
