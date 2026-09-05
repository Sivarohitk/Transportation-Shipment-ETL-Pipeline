"""Unit tests for Delta Lake publishing logic.

These tests cover all pure-Python logic in ``publish/delta_writer.py`` and
the ``output_format`` dispatch in ``publish/hive_writer.py``.

Tests that require a live Databricks session (actual Delta writes, MERGE
execution) are explicitly skipped when ``delta-spark`` is not installed.
No mocking is used to simulate Databricks — see AGENTS.md rule 9.
"""

from __future__ import annotations

import pytest

# ---------------------------------------------------------------------------
# DeltaMergeSpec and build_merge_spec — no Spark dependency
# ---------------------------------------------------------------------------
from transport_etl.publish.delta_writer import (
    DeltaMergeSpec,
    build_merge_spec,
    resolve_delta_write_mode,
)


class TestDeltaMergeSpec:
    def test_merge_condition_single_key(self) -> None:
        spec = DeltaMergeSpec(
            target_table="sc.gold.fct_shipment",
            merge_keys=("shipment_id",),
        )
        assert spec.merge_condition() == "target.shipment_id = source.shipment_id"

    def test_merge_condition_composite_key(self) -> None:
        spec = DeltaMergeSpec(
            target_table="sc.gold.fct_delivery_event",
            merge_keys=("event_id", "shipment_id"),
        )
        cond = spec.merge_condition()
        assert "target.event_id = source.event_id" in cond
        assert "target.shipment_id = source.shipment_id" in cond
        assert " AND " in cond

    def test_merge_condition_raises_when_no_keys(self) -> None:
        spec = DeltaMergeSpec(
            target_table="sc.gold.fct_shipment",
            merge_keys=(),
        )
        with pytest.raises(ValueError, match="merge_keys must not be empty"):
            spec.merge_condition()

    def test_custom_aliases_used_in_condition(self) -> None:
        spec = DeltaMergeSpec(
            target_table="sc.gold.dim_carrier",
            source_alias="src",
            target_alias="tgt",
            merge_keys=("carrier_id",),
        )
        assert spec.merge_condition() == "tgt.carrier_id = src.carrier_id"

    def test_immutability(self) -> None:
        spec = DeltaMergeSpec(
            target_table="sc.gold.fct_shipment",
            merge_keys=("shipment_id",),
        )
        with pytest.raises(Exception):
            spec.target_table = "changed"  # type: ignore[misc]

    def test_update_columns_none_means_all(self) -> None:
        spec = DeltaMergeSpec(
            target_table="sc.gold.fct_shipment",
            merge_keys=("shipment_id",),
            update_columns=None,
        )
        assert spec.update_columns is None

    def test_update_columns_stored_as_tuple(self) -> None:
        spec = DeltaMergeSpec(
            target_table="sc.gold.fct_shipment",
            merge_keys=("shipment_id",),
            update_columns=("carrier_id", "region_code"),
        )
        assert spec.update_columns == ("carrier_id", "region_code")

    def test_insert_all_on_not_matched_default_true(self) -> None:
        spec = DeltaMergeSpec(
            target_table="sc.gold.fct_shipment",
            merge_keys=("shipment_id",),
        )
        assert spec.insert_all_on_not_matched is True


class TestBuildMergeSpec:
    def test_returns_delta_merge_spec(self) -> None:
        spec = build_merge_spec(
            target_table="supply_chain.gold.fct_shipment",
            merge_keys=["shipment_id"],
        )
        assert isinstance(spec, DeltaMergeSpec)

    def test_target_table_stored_stripped(self) -> None:
        spec = build_merge_spec(
            target_table="  supply_chain.gold.fct_shipment  ",
            merge_keys=["shipment_id"],
        )
        assert spec.target_table == "supply_chain.gold.fct_shipment"

    def test_merge_keys_stored_as_tuple(self) -> None:
        spec = build_merge_spec(
            target_table="supply_chain.gold.fct_shipment",
            merge_keys=["shipment_id", "carrier_id"],
        )
        assert spec.merge_keys == ("shipment_id", "carrier_id")

    def test_update_columns_as_tuple_when_provided(self) -> None:
        spec = build_merge_spec(
            target_table="supply_chain.gold.fct_shipment",
            merge_keys=["shipment_id"],
            update_columns=["carrier_id", "region_code"],
        )
        assert spec.update_columns == ("carrier_id", "region_code")

    def test_update_columns_none_when_omitted(self) -> None:
        spec = build_merge_spec(
            target_table="supply_chain.gold.fct_shipment",
            merge_keys=["shipment_id"],
        )
        assert spec.update_columns is None

    def test_raises_on_empty_target_table(self) -> None:
        with pytest.raises(ValueError, match="target_table must not be empty"):
            build_merge_spec(target_table="", merge_keys=["shipment_id"])

    def test_raises_on_whitespace_target_table(self) -> None:
        with pytest.raises(ValueError, match="target_table must not be empty"):
            build_merge_spec(target_table="   ", merge_keys=["shipment_id"])

    def test_raises_on_empty_merge_keys(self) -> None:
        with pytest.raises(ValueError, match="merge_keys must not be empty"):
            build_merge_spec(
                target_table="supply_chain.gold.fct_shipment",
                merge_keys=[],
            )

    def test_custom_aliases_forwarded(self) -> None:
        spec = build_merge_spec(
            target_table="supply_chain.gold.dim_carrier",
            merge_keys=["carrier_id"],
            source_alias="incoming",
            target_alias="existing",
        )
        assert spec.source_alias == "incoming"
        assert spec.target_alias == "existing"

    def test_insert_all_on_not_matched_default(self) -> None:
        spec = build_merge_spec(
            target_table="supply_chain.gold.fct_shipment",
            merge_keys=["shipment_id"],
        )
        assert spec.insert_all_on_not_matched is True

    def test_insert_all_on_not_matched_can_be_false(self) -> None:
        spec = build_merge_spec(
            target_table="supply_chain.gold.fct_shipment",
            merge_keys=["shipment_id"],
            insert_all_on_not_matched=False,
        )
        assert spec.insert_all_on_not_matched is False


# ---------------------------------------------------------------------------
# resolve_delta_write_mode — pure logic
# ---------------------------------------------------------------------------


class TestResolveDeltaWriteMode:
    def test_overwrite_accepted(self) -> None:
        assert resolve_delta_write_mode("overwrite") == "overwrite"

    def test_append_accepted(self) -> None:
        assert resolve_delta_write_mode("append") == "append"

    def test_case_insensitive(self) -> None:
        assert resolve_delta_write_mode("Overwrite") == "overwrite"
        assert resolve_delta_write_mode("APPEND") == "append"

    def test_strips_whitespace(self) -> None:
        assert resolve_delta_write_mode("  overwrite  ") == "overwrite"

    def test_raises_on_unsupported_mode(self) -> None:
        with pytest.raises(ValueError, match="Unsupported Delta write mode"):
            resolve_delta_write_mode("merge")

    def test_raises_on_empty_string(self) -> None:
        with pytest.raises(ValueError, match="Unsupported Delta write mode"):
            resolve_delta_write_mode("")


# ---------------------------------------------------------------------------
# write_delta_table — ImportError guard (no live Spark needed)
# ---------------------------------------------------------------------------


class TestWriteDeltaTableImportGuard:
    def test_raises_import_error_without_pyspark(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """write_delta_table must raise ImportError when PySpark is absent."""
        import sys

        # Remove pyspark from sys.modules to simulate absence.
        pyspark_modules = {k: v for k, v in sys.modules.items() if k.startswith("pyspark")}
        for mod in pyspark_modules:
            monkeypatch.delitem(sys.modules, mod, raising=False)

        # Force the delta_writer module to re-check availability.
        import importlib

        import transport_etl.publish.delta_writer as dw

        importlib.reload(dw)

        with pytest.raises(ImportError, match="requires PySpark"):
            dw.write_delta_table(
                df=None,  # type: ignore[arg-type]
                table_name="supply_chain.gold.fct_shipment",
                partitions=["p_date"],
                mode="overwrite",
            )


# ---------------------------------------------------------------------------
# hive_writer.write_partitioned_table — format dispatch (no live Spark)
# ---------------------------------------------------------------------------


class TestWritePartitionedTableFormatDispatch:
    """Test the output_format parameter without running Spark."""

    def test_unsupported_format_raises(self) -> None:
        from transport_etl.publish.hive_writer import write_partitioned_table

        # Pass a minimal fake df that satisfies the hasattr(df, "columns") guard
        # but fails before any Spark call so we get a clean ValueError.
        class _FakeDf:
            columns = ["shipment_id"]

        with pytest.raises(ValueError, match="Unsupported output_format"):
            write_partitioned_table(
                df=_FakeDf(),
                table_name="curated.fct_shipment",
                output_path="/tmp/test",
                partitions=["p_date"],
                output_format="csv",  # not a supported format
            )

    def test_none_df_returns_output_path_unchanged(self) -> None:
        from transport_etl.publish.hive_writer import write_partitioned_table

        result = write_partitioned_table(
            df=None,
            table_name="curated.fct_shipment",
            output_path="/tmp/sentinel",
            partitions=["p_date"],
            output_format="parquet",
        )
        assert result == "/tmp/sentinel"

    def test_none_df_returns_output_path_for_delta(self) -> None:
        from transport_etl.publish.hive_writer import write_partitioned_table

        result = write_partitioned_table(
            df=None,
            table_name="supply_chain.gold.fct_shipment",
            output_path="/tmp/sentinel",
            partitions=["p_date"],
            output_format="delta",
        )
        assert result == "/tmp/sentinel"

    def test_format_case_insensitive(self) -> None:
        """Format validation must be case-insensitive."""
        from transport_etl.publish.hive_writer import _resolve_output_format

        assert _resolve_output_format("Parquet") == "parquet"
        assert _resolve_output_format("DELTA") == "delta"
        assert _resolve_output_format("  parquet  ") == "parquet"


# ---------------------------------------------------------------------------
# is_dbfs_path — common/io helper
# ---------------------------------------------------------------------------


class TestIsDbfsPath:
    def test_dbfs_uri_recognised(self) -> None:
        from transport_etl.common.io import is_dbfs_path

        assert is_dbfs_path("dbfs:/mnt/transport/raw") is True

    def test_dbfs_uri_case_insensitive(self) -> None:
        from transport_etl.common.io import is_dbfs_path

        assert is_dbfs_path("DBFS:/mnt/transport/raw") is True

    def test_s3_uri_not_dbfs(self) -> None:
        from transport_etl.common.io import is_dbfs_path

        assert is_dbfs_path("s3://my-bucket/raw") is False

    def test_local_path_not_dbfs(self) -> None:
        from transport_etl.common.io import is_dbfs_path

        assert is_dbfs_path("/data/local/raw") is False
        assert is_dbfs_path("data/sample/raw") is False

    def test_path_exists_treats_dbfs_as_existing(self) -> None:
        from transport_etl.common.io import path_exists

        # DBFS paths are assumed reachable (Spark/Hadoop manages them).
        assert path_exists("dbfs:/mnt/transport/raw") is True

    def test_ensure_local_dir_skips_dbfs(self, tmp_path) -> None:
        """ensure_local_dir must not attempt mkdir on a DBFS path."""
        from pathlib import Path

        from transport_etl.common.io import ensure_local_dir

        dbfs_path = "dbfs:/mnt/transport/curated"
        # Should return without raising (would error if it tried os.mkdir on Windows).
        result = ensure_local_dir(dbfs_path)
        # Returns a Path wrapping the original string; the directory is NOT created.
        assert isinstance(result, Path)
        assert not result.exists()
