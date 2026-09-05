"""Unit tests for the carrier_performance Gold builder."""

from __future__ import annotations

from transport_etl.transform.build_carrier_performance import build_carrier_performance


def test_build_carrier_performance_returns_identity_without_spark() -> None:
    """Without Spark the builder returns its input unchanged."""
    payload = object()
    assert build_carrier_performance(payload) is payload


def test_build_carrier_performance_returns_identity_when_no_event_fact() -> None:
    """Without an event fact, the builder still returns its input."""
    payload = object()
    assert build_carrier_performance(payload, fct_delivery_event_df=None) is payload
