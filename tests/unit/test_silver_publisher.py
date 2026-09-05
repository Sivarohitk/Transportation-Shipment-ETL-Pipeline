"""Unit tests for the Silver publisher target-aware dispatch.

These tests verify the ``publish_silver_table`` function's
target-awareness without invoking any Spark writer.  The actual
Parquet / Delta write paths are covered by the integration tests
and by the underlying ``publish.hive_writer`` / ``publish.delta_writer``
unit tests.
"""

from __future__ import annotations

import pytest

from transport_etl.common.constants import (
    TABLE_SILVER_CARRIERS,
    TABLE_SILVER_DELIVERY_EVENTS,
    TABLE_SILVER_SHIPMENTS,
)


def _databricks_config() -> dict:
    return {
        "spark": {"profile": "databricks"},
        "unity_catalog": {
            "catalog": "supply_chain",
            "bronze_schema": "bronze",
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
    return {
        "spark": {"profile": "emr"},
        "hive": {"database": "curated"},
        "paths": {
            "raw_base_path": "s3://bucket/transport/raw",
            "staging_base_path": "s3://bucket/transport/staging",
            "curated_base_path": "s3://bucket/transport/curated",
        },
    }


class _FakeDataFrame:
    """Minimal DataFrame stub that satisfies ``hasattr(df, 'columns')``."""

    columns = ["shipment_id", "_silver_valid_from"]

    def __init__(self) -> None:
        self.sparkSession = None


class TestPublishSilverTableTargetDispatch:
    """Target-aware output format and table name resolution."""

    def test_local_config_resolves_to_parquet(self) -> None:
        from transport_etl.silver.publisher import _resolve_silver_database

        assert _resolve_silver_database(_local_config()) == "curated"

    def test_emr_config_uses_hive_database(self) -> None:
        from transport_etl.silver.publisher import _resolve_silver_database

        cfg = _emr_config()
        cfg["hive"]["database"] = "prod_curated"
        assert _resolve_silver_database(cfg) == "prod_curated"

    def test_local_silver_base_path_resolution(self) -> None:
        from transport_etl.silver.publisher import _resolve_silver_base_path

        assert _resolve_silver_base_path(_local_config()) == "data/local/staging"

    def test_local_silver_base_path_fallback_to_curated(self) -> None:
        from transport_etl.silver.publisher import _resolve_silver_base_path

        cfg = {"spark": {"profile": "local"}, "paths": {"curated_base_path": "data/curated"}}
        assert _resolve_silver_base_path(cfg) == "data/curated"

    def test_databricks_silver_table_name_uses_unity_catalog(self) -> None:
        from transport_etl.common.catalog import resolve_table_name

        cfg = _databricks_config()
        assert (
            resolve_table_name(cfg, "silver", "stg_shipments")
            == "supply_chain.silver.stg_shipments"
        )

    def test_local_silver_table_name_uses_hive_database(self) -> None:
        from transport_etl.common.catalog import resolve_table_name

        assert (
            resolve_table_name(_local_config(), "silver", "stg_shipments")
            == "curated.stg_shipments"
        )

    def test_unknown_silver_table_rejected(self) -> None:
        from transport_etl.silver.publisher import publish_silver_table

        with pytest.raises(ValueError, match="Unknown Silver table name"):
            publish_silver_table(
                df=_FakeDataFrame(),
                config=_local_config(),
                table_name="not_a_silver_table",
            )


class TestPublishSilverTableLocalDispatch:
    """Local/EMR path must produce a Parquet write with hive-style table name."""

    def test_local_writes_two_part_identifier(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from transport_etl.silver import publisher as pub_mod

        captured: dict[str, object] = {}

        def fake_write_partitioned_table(**kwargs):
            captured.update(kwargs)
            return kwargs["output_path"]

        monkeypatch.setattr(pub_mod, "write_partitioned_table", fake_write_partitioned_table)

        result = pub_mod.publish_silver_table(
            df=_FakeDataFrame(),
            config=_local_config(),
            table_name=TABLE_SILVER_SHIPMENTS,
            spark=None,
        )

        assert result == "data/local/staging/stg_shipments"
        assert captured["output_format"] == "parquet"
        assert captured["table_name"] == "curated.stg_shipments"
        assert captured["register_hive_table"] is True
        assert captured["repair_partitions"] is True

    def test_local_routes_all_three_silver_tables(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from transport_etl.silver import publisher as pub_mod

        captured: list[tuple[str, str]] = []

        def fake_write_partitioned_table(**kwargs):
            captured.append((kwargs["table_name"], kwargs["output_format"]))
            return kwargs["output_path"]

        monkeypatch.setattr(pub_mod, "write_partitioned_table", fake_write_partitioned_table)

        for table in [
            TABLE_SILVER_SHIPMENTS,
            TABLE_SILVER_CARRIERS,
            TABLE_SILVER_DELIVERY_EVENTS,
        ]:
            pub_mod.publish_silver_table(
                df=_FakeDataFrame(),
                config=_local_config(),
                table_name=table,
                spark=None,
            )

        rendered = {tbl for tbl, _ in captured}
        assert rendered == {
            "curated.stg_shipments",
            "curated.stg_carriers",
            "curated.stg_delivery_events",
        }
        assert all(fmt == "parquet" for _, fmt in captured)


class TestPublishSilverTableDatabricksDispatch:
    """Databricks path must invoke the Silver MERGE orchestrator."""

    def test_databricks_target_invokes_silver_merge(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from transport_etl.silver import merge as merge_mod
        from transport_etl.silver import publisher as pub_mod

        captured: dict[str, object] = {}

        def fake_execute_silver_merge(
            spark,
            source_df,
            *,
            target_table,
            table_name,
            all_columns,
        ):
            captured["target_table"] = target_table
            captured["table_name"] = table_name
            captured["all_columns"] = list(all_columns)
            return "MERGE INTO ..."

        # The publisher does an in-function import; patch the
        # original module so the lazy import picks up the fake.
        monkeypatch.setattr(merge_mod, "execute_silver_merge", fake_execute_silver_merge)

        # Hive writer must NOT be called on the Databricks path.
        def fail_hive(**kwargs):
            raise AssertionError("write_partitioned_table should not run on Databricks")

        monkeypatch.setattr(pub_mod, "write_partitioned_table", fail_hive)

        result = pub_mod.publish_silver_table(
            df=_FakeDataFrame(),
            config=_databricks_config(),
            table_name=TABLE_SILVER_SHIPMENTS,
            spark=object(),  # type: ignore[arg-type]
        )
        assert result == "MERGE INTO ..."
        assert captured["target_table"] == "supply_chain.silver.stg_shipments"
        assert captured["table_name"] == TABLE_SILVER_SHIPMENTS

    def test_databricks_target_requires_spark(self) -> None:
        from transport_etl.silver.publisher import publish_silver_table

        with pytest.raises(ValueError, match="requires a SparkSession"):
            publish_silver_table(
                df=_FakeDataFrame(),
                config=_databricks_config(),
                table_name=TABLE_SILVER_SHIPMENTS,
                spark=None,
            )
