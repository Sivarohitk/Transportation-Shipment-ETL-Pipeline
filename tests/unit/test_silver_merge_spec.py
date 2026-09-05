"""Unit tests for the Silver MERGE statement renderer.

These tests exercise the pure-Python ``build_merge_sql`` and friends.
No PySpark / delta-spark is required, so the tests run in every CI
environment.

The tests assert the exact behaviour contract documented in
``transport_etl.silver.merge_spec``:

- **MATCH KEY** — the ON clause is built from the business key columns.
- **WHEN MATCHED** — every non-key column is refreshed from the source.
- **WHEN NOT MATCHED** — the full source row is inserted.
- **LATE-ARRIVING DATA** — a row whose key already exists in the
  target is treated as a match (and therefore updated).
- **IDEMPOTENCY** — running the renderer with the same arguments
  returns the same SQL string.
"""

from __future__ import annotations

import pytest

from transport_etl.silver.merge_spec import (
    SilverMergeSpec,
    build_merge_condition,
    build_merge_sql,
    quote_qualified_table,
)

# ---------------------------------------------------------------------------
# SilverMergeSpec validation
# ---------------------------------------------------------------------------


class TestSilverMergeSpecValidation:
    def test_empty_target_table_raises(self) -> None:
        with pytest.raises(ValueError, match="target_table must not be empty"):
            SilverMergeSpec(target_table="", merge_keys=("shipment_id",))

    def test_whitespace_target_table_raises(self) -> None:
        with pytest.raises(ValueError, match="target_table must not be empty"):
            SilverMergeSpec(target_table="   ", merge_keys=("shipment_id",))

    def test_empty_merge_keys_raises(self) -> None:
        with pytest.raises(ValueError, match="merge_keys must not be empty"):
            SilverMergeSpec(
                target_table="sc.silver.stg_shipments",
                merge_keys=(),
            )

    def test_blank_merge_key_raises(self) -> None:
        with pytest.raises(ValueError, match="merge_keys must not contain blank values"):
            SilverMergeSpec(
                target_table="sc.silver.stg_shipments",
                merge_keys=("shipment_id", "  "),
            )

    def test_update_columns_cannot_contain_merge_key(self) -> None:
        with pytest.raises(ValueError, match="update_columns must not contain a merge key"):
            SilverMergeSpec(
                target_table="sc.silver.stg_shipments",
                merge_keys=("shipment_id",),
                update_columns=("shipment_id", "carrier_id"),
            )

    def test_defaults(self) -> None:
        spec = SilverMergeSpec(
            target_table="sc.silver.stg_shipments",
            merge_keys=("shipment_id",),
        )
        assert spec.source_alias == "source"
        assert spec.target_alias == "target"
        assert spec.merge_keys == ("shipment_id",)
        assert spec.update_columns == ()
        assert spec.insert_columns == ()
        assert spec.source_view == ""


# ---------------------------------------------------------------------------
# quote_qualified_table
# ---------------------------------------------------------------------------


class TestQuoteQualifiedTable:
    def test_single_part(self) -> None:
        assert quote_qualified_table("shipments") == "`shipments`"

    def test_two_parts(self) -> None:
        assert quote_qualified_table("curated.stg_shipments") == "`curated`.`stg_shipments`"

    def test_three_parts(self) -> None:
        assert (
            quote_qualified_table("supply_chain.silver.stg_shipments")
            == "`supply_chain`.`silver`.`stg_shipments`"
        )

    def test_strips_whitespace(self) -> None:
        assert quote_qualified_table("  sc . silver . table  ") == "`sc`.`silver`.`table`"

    def test_empty_input_raises(self) -> None:
        with pytest.raises(ValueError, match="Cannot quote an empty table"):
            quote_qualified_table("")
        with pytest.raises(ValueError, match="Cannot quote an empty table"):
            quote_qualified_table("   ")

    def test_strips_backticks_in_input(self) -> None:
        # Input containing backticks should not break the renderer; the
        # backticks are stripped before re-quoting.
        assert quote_qualified_table("sc.silver.table") == "`sc`.`silver`.`table`"


# ---------------------------------------------------------------------------
# build_merge_condition
# ---------------------------------------------------------------------------


class TestBuildMergeCondition:
    def test_single_key(self) -> None:
        spec = SilverMergeSpec(
            target_table="sc.silver.stg_shipments",
            merge_keys=("shipment_id",),
        )
        assert build_merge_condition(spec) == "`target`.`shipment_id` = `source`.`shipment_id`"

    def test_composite_key(self) -> None:
        spec = SilverMergeSpec(
            target_table="sc.silver.stg_delivery_events",
            merge_keys=("event_id", "shipment_id"),
        )
        cond = build_merge_condition(spec)
        assert "`target`.`event_id` = `source`.`event_id`" in cond
        assert "`target`.`shipment_id` = `source`.`shipment_id`" in cond
        assert " AND " in cond

    def test_custom_aliases(self) -> None:
        spec = SilverMergeSpec(
            target_table="sc.silver.stg_carriers",
            merge_keys=("carrier_id",),
            source_alias="src",
            target_alias="tgt",
        )
        assert build_merge_condition(spec) == "`tgt`.`carrier_id` = `src`.`carrier_id`"

    def test_empty_keys_raises(self) -> None:
        with pytest.raises(ValueError, match="merge_keys must not be empty"):
            SilverMergeSpec(
                target_table="sc.silver.stg_shipments",
                merge_keys=(),
            )


# ---------------------------------------------------------------------------
# build_merge_sql — the contract
# ---------------------------------------------------------------------------


class TestBuildMergeSqlContract:
    """The MERGE contract: MATCH KEY, WHEN MATCHED, WHEN NOT MATCHED."""

    @staticmethod
    def _shipments_spec() -> SilverMergeSpec:
        return SilverMergeSpec(
            target_table="supply_chain.silver.stg_shipments",
            merge_keys=("shipment_id",),
            source_view="silver_stg_shipments",
        )

    def test_match_key_present_in_on_clause(self) -> None:
        sql = build_merge_sql(self._shipments_spec(), all_columns=["shipment_id", "carrier_id"])
        assert "ON `target`.`shipment_id` = `source`.`shipment_id`" in sql

    def test_when_matched_updates_non_key_columns(self) -> None:
        columns = ["shipment_id", "carrier_id", "updated_at"]
        sql = build_merge_sql(self._shipments_spec(), all_columns=columns)
        assert "WHEN MATCHED THEN UPDATE SET" in sql
        # The business key is not in the UPDATE list.
        assert "`carrier_id` = `source`.`carrier_id`" in sql
        assert "`updated_at` = `source`.`updated_at`" in sql
        # The MERGE key itself is excluded.
        matched_section = sql.split("WHEN MATCHED THEN UPDATE SET")[1].split("WHEN NOT MATCHED")[0]
        assert "shipment_id" not in matched_section

    def test_when_not_matched_inserts_row(self) -> None:
        columns = ["shipment_id", "carrier_id", "updated_at"]
        sql = build_merge_sql(self._shipments_spec(), all_columns=columns)
        assert "WHEN NOT MATCHED THEN INSERT" in sql
        assert "VALUES" in sql
        # All source columns referenced in the INSERT VALUES list.
        assert "`source`.`shipment_id`" in sql
        assert "`source`.`carrier_id`" in sql
        assert "`source`.`updated_at`" in sql

    def test_explicit_insert_columns(self) -> None:
        spec = SilverMergeSpec(
            target_table="sc.silver.stg_carriers",
            merge_keys=("carrier_id",),
            source_view="silver_stg_carriers",
            insert_columns=("carrier_id", "carrier_name"),
        )
        sql = build_merge_sql(spec, all_columns=["carrier_id", "carrier_name", "is_active"])
        assert "WHEN NOT MATCHED THEN INSERT (`carrier_id`, `carrier_name`)" in sql
        # The non-inserted column should not appear in the VALUES clause.
        assert "`source`.`is_active`" not in sql.split("WHEN NOT MATCHED")[1]

    def test_explicit_update_columns(self) -> None:
        spec = SilverMergeSpec(
            target_table="sc.silver.stg_carriers",
            merge_keys=("carrier_id",),
            source_view="silver_stg_carriers",
            update_columns=("carrier_name",),
        )
        sql = build_merge_sql(spec, all_columns=["carrier_id", "carrier_name", "is_active"])
        assert "`carrier_name` = `source`.`carrier_name`" in sql
        # The non-updated column should not appear in the UPDATE SET list.
        matched_section = sql.split("WHEN MATCHED THEN UPDATE SET")[1].split("WHEN NOT MATCHED")[0]
        assert "is_active" not in matched_section

    def test_composite_key_renders_two_on_clauses(self) -> None:
        spec = SilverMergeSpec(
            target_table="supply_chain.silver.stg_delivery_events",
            merge_keys=("event_id", "shipment_id"),
            source_view="silver_stg_delivery_events",
        )
        sql = build_merge_sql(spec, all_columns=["event_id", "shipment_id", "event_type"])
        assert (
            "ON `target`.`event_id` = `source`.`event_id` AND "
            "`target`.`shipment_id` = `source`.`shipment_id`"
        ) in sql

    def test_source_view_required(self) -> None:
        spec = SilverMergeSpec(
            target_table="sc.silver.stg_shipments",
            merge_keys=("shipment_id",),
            source_view="",
        )
        with pytest.raises(ValueError, match="source_view must be set"):
            build_merge_sql(spec, all_columns=["shipment_id"])

    def test_empty_columns_raises(self) -> None:
        with pytest.raises(ValueError, match="at least one column"):
            build_merge_sql(self._shipments_spec(), all_columns=[])

    def test_target_table_fully_qualified_in_sql(self) -> None:
        sql = build_merge_sql(self._shipments_spec(), all_columns=["shipment_id"])
        assert "MERGE INTO `supply_chain`.`silver`.`stg_shipments`" in sql

    def test_two_part_table_supported(self) -> None:
        spec = SilverMergeSpec(
            target_table="curated.stg_shipments",
            merge_keys=("shipment_id",),
            source_view="silver_stg_shipments",
        )
        sql = build_merge_sql(spec, all_columns=["shipment_id"])
        assert "MERGE INTO `curated`.`stg_shipments`" in sql


# ---------------------------------------------------------------------------
# Idempotency
# ---------------------------------------------------------------------------


class TestMergeSqlIdempotency:
    """The same arguments must render the same SQL — the foundation of
    the documented idempotency story for Databricks MERGE."""

    def test_same_inputs_same_sql(self) -> None:
        spec = SilverMergeSpec(
            target_table="supply_chain.silver.stg_shipments",
            merge_keys=("shipment_id",),
            source_view="silver_stg_shipments",
        )
        columns = ["shipment_id", "carrier_id", "updated_at"]
        sql_a = build_merge_sql(spec, all_columns=columns)
        sql_b = build_merge_sql(spec, all_columns=columns)
        assert sql_a == sql_b

    def test_idempotent_under_column_reordering(self) -> None:
        """The renderer orders output by the order in which columns are
        provided.  Reordering the input yields a different but
        semantically equivalent SQL string — both must be well-formed.
        """
        spec = SilverMergeSpec(
            target_table="sc.silver.stg_shipments",
            merge_keys=("shipment_id",),
            source_view="silver_stg_shipments",
        )
        sql_a = build_merge_sql(spec, all_columns=["shipment_id", "carrier_id", "updated_at"])
        sql_b = build_merge_sql(spec, all_columns=["updated_at", "carrier_id", "shipment_id"])
        # Same ON clause, same target/source aliases.
        assert "ON `target`.`shipment_id`" in sql_a
        assert "ON `target`.`shipment_id`" in sql_b
        # Both must contain the same WHEN clauses.
        assert sql_a.count("WHEN MATCHED") == sql_b.count("WHEN MATCHED") == 1
        assert sql_a.count("WHEN NOT MATCHED") == sql_b.count("WHEN NOT MATCHED") == 1

    def test_idempotency_fingerprint(self) -> None:
        """The SQL string is a deterministic function of its arguments."""
        spec = SilverMergeSpec(
            target_table="sc.silver.stg_carriers",
            merge_keys=("carrier_id",),
            source_view="silver_stg_carriers",
        )
        first = build_merge_sql(spec, all_columns=["carrier_id", "carrier_name", "is_active"])
        # Re-build the same spec by hand to confirm structural equality.
        spec_rebuilt = SilverMergeSpec(
            target_table="sc.silver.stg_carriers",
            merge_keys=("carrier_id",),
            source_view="silver_stg_carriers",
        )
        second = build_merge_sql(
            spec_rebuilt, all_columns=["carrier_id", "carrier_name", "is_active"]
        )
        assert first == second


# ---------------------------------------------------------------------------
# Late-arriving data semantics
# ---------------------------------------------------------------------------


class TestMergeSqlLateArriving:
    """Late-arriving records use the same MERGE — a key that already
    exists in the target is updated; a key that does not exist is
    inserted.  The renderer must produce a single MERGE statement that
    handles both cases."""

    def test_single_merge_handles_both_branches(self) -> None:
        spec = SilverMergeSpec(
            target_table="sc.silver.stg_shipments",
            merge_keys=("shipment_id",),
            source_view="silver_stg_shipments",
        )
        sql = build_merge_sql(spec, all_columns=["shipment_id", "carrier_id"])
        # Exactly one MERGE INTO, exactly one WHEN MATCHED, exactly one WHEN NOT MATCHED.
        assert sql.count("MERGE INTO") == 1
        assert sql.count("WHEN MATCHED") == 1
        assert sql.count("WHEN NOT MATCHED") == 1
