"""Unit tests for Delta Lake publishing logic.

These tests cover all pure-Python logic in ``publish/delta_writer.py`` and
the ``output_format`` dispatch in ``publish/hive_writer.py``.

Actual Delta writes require a live Databricks session and are covered by
workspace validation rather than mocked here.
"""

from __future__ import annotations

import pytest

from transport_etl.publish.delta_writer import resolve_delta_write_mode

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
    def test_rejects_non_dataframe_input_when_pyspark_is_installed(self) -> None:
        import transport_etl.publish.delta_writer as dw

        with pytest.raises(TypeError, match="Spark DataFrame"):
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
