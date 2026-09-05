"""Unit tests for the route_performance Gold builder."""

from __future__ import annotations

from transport_etl.transform.build_route_performance import build_route_performance


def test_build_route_performance_returns_identity_without_spark() -> None:
    payload = object()
    assert build_route_performance(payload) is payload
