"""Unit tests for the Silver MERGE orchestrator helpers.

These tests focus on the pure-Python helpers in
``transport_etl.silver.merge`` — the ``build_silver_merge_spec``
factory and the ``render_silver_merge_sql`` function.  The functions
that touch a live Spark session (``register_source_view`` and
``execute_silver_merge``) are exercised in the integration tests
where a real SparkSession is available.
"""

from __future__ import annotations

import pytest

from transport_etl.common.constants import (
    TABLE_SILVER_CARRIERS,
    TABLE_SILVER_DELIVERY_EVENTS,
    TABLE_SILVER_SHIPMENTS,
)
from transport_etl.silver.merge import (
    build_silver_merge_spec,
    execute_silver_merge,
    render_silver_merge_sql,
)
from transport_etl.silver.merge_spec import SilverMergeSpec

# ---------------------------------------------------------------------------
# build_silver_merge_spec
# ---------------------------------------------------------------------------


class TestBuildSilverMergeSpec:
    def test_uses_canonical_business_key_for_shipments(self) -> None:
        spec = build_silver_merge_spec(
            target_table="supply_chain.silver.stg_shipments",
            table_name=TABLE_SILVER_SHIPMENTS,
            all_columns=["shipment_id", "carrier_id", "updated_at"],
            source_view="silver_stg_shipments",
        )
        assert spec.merge_keys == ("shipment_id",)
        assert spec.target_table == "supply_chain.silver.stg_shipments"
        assert spec.source_view == "silver_stg_shipments"

    def test_uses_canonical_business_key_for_carriers(self) -> None:
        spec = build_silver_merge_spec(
            target_table="sc.silver.stg_carriers",
            table_name=TABLE_SILVER_CARRIERS,
            all_columns=["carrier_id", "carrier_name"],
            source_view="silver_stg_carriers",
        )
        assert spec.merge_keys == ("carrier_id",)

    def test_uses_canonical_business_key_for_delivery_events(self) -> None:
        spec = build_silver_merge_spec(
            target_table="sc.silver.stg_delivery_events",
            table_name=TABLE_SILVER_DELIVERY_EVENTS,
            all_columns=["event_id", "shipment_id", "event_type"],
            source_view="silver_stg_delivery_events",
        )
        assert spec.merge_keys == ("event_id",)

    def test_explicit_update_and_insert_columns(self) -> None:
        spec = build_silver_merge_spec(
            target_table="sc.silver.stg_shipments",
            table_name=TABLE_SILVER_SHIPMENTS,
            all_columns=["shipment_id", "carrier_id"],
            source_view="silver_stg_shipments",
            update_columns=("carrier_id",),
            insert_columns=("shipment_id", "carrier_id"),
        )
        assert spec.update_columns == ("carrier_id",)
        assert spec.insert_columns == ("shipment_id", "carrier_id")

    def test_returns_immutable_spec(self) -> None:
        spec = build_silver_merge_spec(
            target_table="sc.silver.stg_shipments",
            table_name=TABLE_SILVER_SHIPMENTS,
            all_columns=["shipment_id"],
            source_view="silver_stg_shipments",
        )
        assert isinstance(spec, SilverMergeSpec)
        with pytest.raises(Exception):
            spec.target_table = "changed"  # type: ignore[misc]

    def test_unknown_table_raises(self) -> None:
        with pytest.raises(ValueError, match="Unknown Silver table name"):
            build_silver_merge_spec(
                target_table="sc.silver.stg_unknown",
                table_name="stg_unknown",
                all_columns=["x"],
                source_view="v",
            )


class TestExecuteSilverMerge:
    def test_first_load_creates_delta_table_before_merge_is_needed(self) -> None:
        class FakeCatalog:
            @staticmethod
            def tableExists(table_name: str) -> bool:
                assert table_name == "supply_chain.silver.stg_shipments"
                return False

        class FakeSpark:
            catalog = FakeCatalog()

            def __init__(self) -> None:
                self.statements: list[str] = []

            def sql(self, statement: str) -> None:
                self.statements.append(statement)

        class FakeDataFrame:
            def __init__(self) -> None:
                self.views: list[str] = []

            def createOrReplaceTempView(self, name: str) -> None:
                self.views.append(name)

        spark = FakeSpark()
        source = FakeDataFrame()

        sql = execute_silver_merge(
            spark,
            source,
            target_table="supply_chain.silver.stg_shipments",
            table_name=TABLE_SILVER_SHIPMENTS,
            all_columns=["shipment_id", "carrier_id"],
        )

        assert source.views == ["silver_stg_shipments"]
        assert spark.statements == [sql]
        assert sql == (
            "CREATE TABLE `supply_chain`.`silver`.`stg_shipments` USING DELTA "
            "AS SELECT * FROM `silver_stg_shipments`"
        )
        assert "MERGE INTO" not in sql


# ---------------------------------------------------------------------------
# render_silver_merge_sql
# ---------------------------------------------------------------------------


class TestRenderSilverMergeSql:
    def test_renders_valid_sql(self) -> None:
        sql = render_silver_merge_sql(
            build_silver_merge_spec(
                target_table="supply_chain.silver.stg_shipments",
                table_name=TABLE_SILVER_SHIPMENTS,
                all_columns=["shipment_id", "carrier_id"],
                source_view="silver_stg_shipments",
            ),
            all_columns=["shipment_id", "carrier_id"],
        )
        assert sql.startswith("MERGE INTO")
        assert "WHEN MATCHED" in sql
        assert "WHEN NOT MATCHED" in sql

    def test_idempotent(self) -> None:
        spec = build_silver_merge_spec(
            target_table="sc.silver.stg_carriers",
            table_name=TABLE_SILVER_CARRIERS,
            all_columns=["carrier_id", "carrier_name"],
            source_view="silver_stg_carriers",
        )
        first = render_silver_merge_sql(spec, all_columns=["carrier_id", "carrier_name"])
        second = render_silver_merge_sql(spec, all_columns=["carrier_id", "carrier_name"])
        assert first == second

    def test_passes_all_columns_through(self) -> None:
        spec = build_silver_merge_spec(
            target_table="sc.silver.stg_shipments",
            table_name=TABLE_SILVER_SHIPMENTS,
            all_columns=["shipment_id", "carrier_id", "origin_state", "destination_state"],
            source_view="silver_stg_shipments",
        )
        sql = render_silver_merge_sql(
            spec, all_columns=["shipment_id", "carrier_id", "origin_state", "destination_state"]
        )
        # Every non-key column must appear in the UPDATE SET list.
        for column in ("carrier_id", "origin_state", "destination_state"):
            assert f"`{column}` = `source`.`{column}`" in sql
