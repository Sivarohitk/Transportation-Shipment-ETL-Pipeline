"""Unit tests for the Bronze publisher format dispatch.

These tests verify the ``output_format`` and target-aware table name
resolution in ``transport_etl.bronze.publisher`` without invoking any
Spark writer.  The actual Parquet/Delta write paths are covered by
integration tests and by the underlying ``publish.hive_writer`` /
``publish.delta_writer`` unit tests.
"""

from __future__ import annotations

import pytest

from transport_etl.bronze.publisher import publish_bronze_table


def _databricks_config(catalog: str = "supply_chain", bronze: str = "bronze") -> dict:
    """Return a minimal Databricks-targeted config dict."""
    return {
        "spark": {"profile": "databricks"},
        "unity_catalog": {
            "catalog": catalog,
            "bronze_schema": bronze,
            "silver_schema": "silver",
            "gold_schema": "gold",
        },
        "hive": {"database": "curated"},
        "paths": {
            "raw_base_path": "dbfs:/mnt/transport/raw",
            "staging_base_path": "dbfs:/mnt/transport/staging",
            "curated_base_path": "dbfs:/mnt/transport/curated",
        },
    }


def _local_config() -> dict:
    """Return a minimal local-targeted config dict."""
    return {
        "spark": {"profile": "local"},
        "hive": {"database": "curated"},
        "paths": {
            "raw_base_path": "data/sample/raw",
            "staging_base_path": "data/local/staging",
            "curated_base_path": "data/local/curated",
        },
    }


def _emr_config() -> dict:
    """Return a minimal EMR-targeted config dict."""
    return {
        "spark": {"profile": "emr"},
        "hive": {"database": "curated"},
        "paths": {
            "raw_base_path": "s3://bucket/transport/raw",
            "staging_base_path": "s3://bucket/transport/staging",
            "curated_base_path": "s3://bucket/transport/curated",
        },
    }


class _FakeDf:
    """Minimal DataFrame stub that satisfies ``hasattr(df, 'columns')``."""

    columns = ["shipment_id", "_ingested_at", "_source_file", "_batch_id", "_run_date"]

    def __init__(self) -> None:
        self.sparkSession = None


class TestPublishBronzeTableTargetDispatch:
    """Target-aware output format and table name resolution."""

    def test_local_config_resolves_to_parquet(self) -> None:
        """Local target must resolve to ``parquet`` output format."""
        from transport_etl.bronze.publisher import _resolve_bronze_output_format

        assert _resolve_bronze_output_format(_local_config()) == "parquet"

    def test_emr_config_resolves_to_parquet(self) -> None:
        from transport_etl.bronze.publisher import _resolve_bronze_output_format

        assert _resolve_bronze_output_format(_emr_config()) == "parquet"

    def test_databricks_config_resolves_to_delta(self) -> None:
        from transport_etl.bronze.publisher import _resolve_bronze_output_format

        assert _resolve_bronze_output_format(_databricks_config()) == "delta"

    def test_databricks_table_name_uses_three_part_unity_catalog(self) -> None:
        from transport_etl.bronze.publisher import _resolve_bronze_output_format

        cfg = _databricks_config(catalog="acme", bronze="raw")
        assert _resolve_bronze_output_format(cfg) == "delta"
        from transport_etl.common.catalog import resolve_table_name

        assert resolve_table_name(cfg, "bronze", "raw_shipments") == "acme.raw.raw_shipments"

    def test_local_table_name_uses_hive_database(self) -> None:
        from transport_etl.common.catalog import resolve_table_name

        cfg = _local_config()
        assert resolve_table_name(cfg, "bronze", "raw_shipments") == "curated.raw_shipments"

    def test_emr_table_name_uses_hive_database(self) -> None:
        from transport_etl.common.catalog import resolve_table_name

        cfg = _emr_config()
        assert resolve_table_name(cfg, "bronze", "raw_carriers") == "curated.raw_carriers"

    def test_unknown_table_name_rejected(self) -> None:
        with pytest.raises(ValueError, match="Unknown Bronze table name"):
            publish_bronze_table(
                df=_FakeDf(),
                config=_local_config(),
                table_name="not_a_bronze_table",
            )


class TestPublishBronzeTableDatabricksDispatch:
    """Databricks path must produce a ``catalog.schema.table`` identifier."""

    def test_databricks_writes_three_part_identifier(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """The publisher must call ``write_partitioned_table`` with the
        Unity-Catalog-resolved identifier when targeting Databricks."""
        from transport_etl.bronze import publisher as pub_mod

        captured: dict[str, object] = {}

        def fake_write_partitioned_table(**kwargs):
            captured.update(kwargs)
            return kwargs["table_name"]

        monkeypatch.setattr(pub_mod, "write_partitioned_table", fake_write_partitioned_table)

        result = publish_bronze_table(
            df=_FakeDf(),
            config=_databricks_config(),
            table_name="raw_shipments",
            spark=None,
        )

        assert result == "supply_chain.bronze.raw_shipments"
        assert captured["output_format"] == "delta"
        assert captured["table_name"] == "supply_chain.bronze.raw_shipments"
        # Hive registration must be skipped on Databricks targets because
        # Unity Catalog manages metadata.
        assert captured["register_hive_table"] is False
        assert captured["repair_partitions"] is False

    def test_databricks_custom_catalog_and_bronze_schema(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from transport_etl.bronze import publisher as pub_mod

        captured: dict[str, object] = {}

        def fake_write_partitioned_table(**kwargs):
            captured.update(kwargs)
            return kwargs["table_name"]

        monkeypatch.setattr(pub_mod, "write_partitioned_table", fake_write_partitioned_table)

        cfg = _databricks_config(catalog="acme", bronze="ingest")
        result = publish_bronze_table(
            df=_FakeDf(),
            config=cfg,
            table_name="raw_delivery_events",
            spark=None,
        )

        assert result == "acme.ingest.raw_delivery_events"
        assert captured["output_format"] == "delta"


class TestPublishBronzeTableLocalDispatch:
    """Local/EMR path must produce a Parquet write with hive-style table name."""

    def test_local_writes_two_part_identifier(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from transport_etl.bronze import publisher as pub_mod

        captured: dict[str, object] = {}

        def fake_write_partitioned_table(**kwargs):
            captured.update(kwargs)
            return kwargs["output_path"]

        monkeypatch.setattr(pub_mod, "write_partitioned_table", fake_write_partitioned_table)

        result = publish_bronze_table(
            df=_FakeDf(),
            config=_local_config(),
            table_name="raw_shipments",
            spark=None,
        )

        assert result == "data/local/staging/raw_shipments"
        assert captured["output_format"] == "parquet"
        assert captured["table_name"] == "curated.raw_shipments"
        assert captured["register_hive_table"] is True
        assert captured["repair_partitions"] is True

    def test_local_falls_back_to_curated_base_when_staging_missing(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from transport_etl.bronze import publisher as pub_mod

        captured: dict[str, object] = {}

        def fake_write_partitioned_table(**kwargs):
            captured.update(kwargs)
            return kwargs["output_path"]

        monkeypatch.setattr(pub_mod, "write_partitioned_table", fake_write_partitioned_table)

        cfg = {
            "spark": {"profile": "local"},
            "hive": {"database": "curated"},
            "paths": {"curated_base_path": "data/local/curated"},
        }

        result = publish_bronze_table(
            df=_FakeDf(),
            config=cfg,
            table_name="raw_carriers",
            spark=None,
        )

        assert result == "data/local/curated/raw_carriers"
        assert captured["output_format"] == "parquet"
