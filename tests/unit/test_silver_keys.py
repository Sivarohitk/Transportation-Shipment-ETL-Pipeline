"""Unit tests for Silver business-key definitions.

Pure-Python tests; no PySpark required.  These guard the canonical
mapping between Silver logical table names and their business keys so
the dedup helper, the MERGE renderer, and the quarantine writer
cannot drift apart.
"""

from __future__ import annotations

import pytest

from transport_etl.common.constants import (
    SILVER_BUSINESS_KEYS,
    SILVER_TABLE_NAMES,
    TABLE_SILVER_CARRIERS,
    TABLE_SILVER_DELIVERY_EVENTS,
    TABLE_SILVER_SHIPMENTS,
)
from transport_etl.silver.keys import (
    business_key_for,
    business_key_str_for,
    is_silver_table,
    known_silver_tables,
    source_identity_for,
)


class TestKnownSilverTables:
    def test_returns_canonical_tuple(self) -> None:
        assert known_silver_tables() == (
            TABLE_SILVER_SHIPMENTS,
            TABLE_SILVER_CARRIERS,
            TABLE_SILVER_DELIVERY_EVENTS,
        )

    def test_silver_table_names_constant_matches(self) -> None:
        assert known_silver_tables() == SILVER_TABLE_NAMES


class TestIsSilverTable:
    def test_recognises_known_tables(self) -> None:
        assert is_silver_table(TABLE_SILVER_SHIPMENTS) is True
        assert is_silver_table(TABLE_SILVER_CARRIERS) is True
        assert is_silver_table(TABLE_SILVER_DELIVERY_EVENTS) is True

    def test_rejects_unknown_tables(self) -> None:
        assert is_silver_table("not_a_silver_table") is False
        assert is_silver_table("fct_shipment") is False
        assert is_silver_table("raw_shipments") is False

    def test_rejects_non_string_values(self) -> None:
        assert is_silver_table(None) is False  # type: ignore[arg-type]
        assert is_silver_table(123) is False  # type: ignore[arg-type]
        assert is_silver_table("") is False


class TestBusinessKeyFor:
    def test_shipments_business_key(self) -> None:
        assert business_key_for(TABLE_SILVER_SHIPMENTS) == ("shipment_id",)

    def test_carriers_business_key(self) -> None:
        assert business_key_for(TABLE_SILVER_CARRIERS) == ("carrier_id",)

    def test_delivery_events_business_key(self) -> None:
        assert business_key_for(TABLE_SILVER_DELIVERY_EVENTS) == ("event_id",)

    def test_matches_constants_module(self) -> None:
        """The keys module must agree with the constants module."""
        for table in SILVER_TABLE_NAMES:
            assert business_key_for(table) == SILVER_BUSINESS_KEYS[table]

    def test_each_key_has_at_least_one_column(self) -> None:
        for table in SILVER_TABLE_NAMES:
            assert len(business_key_for(table)) >= 1

    def test_keys_are_strings(self) -> None:
        for table in SILVER_TABLE_NAMES:
            for column in business_key_for(table):
                assert isinstance(column, str)
                assert column.strip() == column
                assert column  # not empty

    def test_unknown_table_raises(self) -> None:
        with pytest.raises(ValueError, match="Unknown Silver table name"):
            business_key_for("fct_shipment")

    def test_empty_table_raises(self) -> None:
        with pytest.raises(ValueError, match="must not be empty"):
            business_key_for("")


class TestBusinessKeyStrFor:
    def test_single_column_key(self) -> None:
        assert business_key_str_for(TABLE_SILVER_SHIPMENTS) == "shipment_id"
        assert business_key_str_for(TABLE_SILVER_CARRIERS) == "carrier_id"
        assert business_key_str_for(TABLE_SILVER_DELIVERY_EVENTS) == "event_id"

    def test_string_form_is_compatible_with_sql_where_clause(self) -> None:
        # The ON clause uses `target`.`key` = `source`.`key`; the
        # business_key_str_for output is the column-list half.
        rendered = business_key_str_for(TABLE_SILVER_SHIPMENTS)
        assert "`target`.`shipment_id`" not in rendered
        assert "shipment_id" in rendered


class TestSourceIdentityFor:
    def test_shipments_source_identity(self) -> None:
        assert source_identity_for(TABLE_SILVER_SHIPMENTS) == ("shipment_id",)

    def test_carriers_source_identity(self) -> None:
        assert source_identity_for(TABLE_SILVER_CARRIERS) == ("carrier_id",)

    def test_delivery_events_source_identity(self) -> None:
        assert source_identity_for(TABLE_SILVER_DELIVERY_EVENTS) == ("event_id",)

    def test_source_identity_matches_business_key(self) -> None:
        """For all Silver entities the source identity is the business key
        because the ingest module already enforces uniqueness on it."""
        for table in SILVER_TABLE_NAMES:
            assert source_identity_for(table) == business_key_for(table)

    def test_unknown_table_raises(self) -> None:
        with pytest.raises(ValueError, match="Unknown Silver table name"):
            source_identity_for("not_a_table")
