"""Tests for the leakage audit and the no-leakage contract.

The leakage audit is the strongest defence against accidental
fabrication of model metrics.  These tests assert:

- the forbidden column list is non-empty,
- each forbidden column is a real string,
- :func:`assert_no_leakage` raises when forbidden columns are
  present,
- :func:`assert_no_leakage` is silent on clean frames,
- the forbidden list is large enough to be defensive.
"""

from __future__ import annotations

import pytest

from transport_etl.ml.leakage_audit import (
    ALLOWED_FEATURE_COLUMNS,
    FORBIDDEN_FEATURE_COLUMNS,
    LABEL_AND_META_COLUMNS,
    LeakageAuditError,
    assert_no_leakage,
)


class TestLeakageAudit:
    def test_forbidden_list_is_non_empty(self) -> None:
        assert len(FORBIDDEN_FEATURE_COLUMNS) > 0

    def test_forbidden_columns_are_strings(self) -> None:
        for column in FORBIDDEN_FEATURE_COLUMNS:
            assert isinstance(column, str)
            assert column.strip() == column
            assert column  # not empty

    def test_allowed_list_is_non_empty(self) -> None:
        assert len(ALLOWED_FEATURE_COLUMNS) > 0

    def test_no_overlap_between_forbidden_and_allowed(self) -> None:
        overlap = set(FORBIDDEN_FEATURE_COLUMNS) & set(ALLOWED_FEATURE_COLUMNS)
        assert not overlap, "forbidden and allowed columns overlap: " + ", ".join(sorted(overlap))

    def test_label_and_meta_columns_are_strings(self) -> None:
        for column in LABEL_AND_META_COLUMNS:
            assert isinstance(column, str)
            assert column.strip() == column

    def test_assert_no_leakage_passes_on_clean_columns(self) -> None:
        # Should not raise.
        assert_no_leakage(["shipment_id", "pickup_ts", "carrier_id", "distance_miles"])

    def test_assert_no_leakage_passes_on_empty_iterable(self) -> None:
        # An empty iterable has no columns — trivially no leakage.
        assert_no_leakage([])

    def test_assert_no_leakage_raises_on_forbidden(self) -> None:
        with pytest.raises(LeakageAuditError, match="actual_delivery_ts"):
            assert_no_leakage(["shipment_id", "actual_delivery_ts", "carrier_id"])

    def test_assert_no_leakage_raises_on_multiple_forbidden(self) -> None:
        with pytest.raises(LeakageAuditError) as exc:
            assert_no_leakage(["actual_delivery_ts", "transit_time_hours", "delay_minutes"])
        message = str(exc.value)
        assert "actual_delivery_ts" in message
        assert "transit_time_hours" in message
        assert "delay_minutes" in message

    def test_assert_no_leakage_raises_on_aggregated_aggregations(self) -> None:
        # The Gold-layer aggregate columns are explicitly forbidden.
        with pytest.raises(LeakageAuditError):
            assert_no_leakage(["shipment_id", "agg_total_shipments", "agg_delivered_shipments"])

    def test_custom_forbidden_override(self) -> None:
        with pytest.raises(LeakageAuditError, match="actual_delivery_ts"):
            assert_no_leakage(
                ["actual_delivery_ts"],
                forbidden=("actual_delivery_ts",),
            )
