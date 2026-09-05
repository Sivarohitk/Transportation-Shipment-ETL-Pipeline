"""Unit tests for the delivery_exception_summary Gold builder."""

from __future__ import annotations

from transport_etl.transform.build_delivery_exception_summary import (
    build_delivery_exception_summary,
)


def test_build_delivery_exception_summary_returns_identity_without_spark() -> None:
    payload = object()
    assert build_delivery_exception_summary(payload) is payload
