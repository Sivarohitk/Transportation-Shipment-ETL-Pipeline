"""Leakage audit for the late-shipment risk model.

This module documents the leakage contract for the Phase 8 model and
provides a small runtime guard that rejects features that would leak
the target.  It is intentionally simple: the model code must not
silently introduce a leaky column.

Why this exists
----------------

AGENTS.md rule 11 is "Do not fabricate model metrics".  The strongest
defence against accidental fabrication is a strict no-leakage
contract.  Rather than rely on a code review, this module exposes a
single :data:`FORBIDDEN_FEATURE_COLUMNS` tuple.  The feature
engineering pipeline and any future model code can call
:func:`assert_no_leakage` to fail loudly if a forbidden column is
introduced into the feature matrix.

Forbidden columns
----------------

The following columns are **explicitly excluded** from the feature
matrix because they would reveal the target ``is_late`` (which is
defined as ``actual_delivery_ts > promised_delivery_ts``):

- ``actual_delivery_ts`` — the source of the label
- ``delay_minutes`` — derived from ``actual - promised``
- ``on_time_delivery_flag`` — a 0/1 alias of the label
- ``transit_time_hours`` — only known after delivery
- ``exception_flag`` — derived from delivery events
- ``delivered_flag`` — derived from ``actual_delivery_ts`` presence
- ``shipment_updated_at`` — the shipment's ``updated_at`` column can
  equal ``actual_delivery_ts`` after delivery
- ``out_for_delivery_ts`` / ``delivered_event_ts`` — only known
  after delivery

Allowed (prediction-time) columns
---------------------------------

The feature matrix is allowed to use:

- ``shipment_id`` (identifier only — excluded from training but kept
  for output joins)
- ``carrier_id``
- ``service_mode`` (joined from ``dim_carrier``)
- ``is_active`` (joined from ``dim_carrier``)
- ``home_region_code`` (joined from ``dim_carrier``)
- ``origin_state`` / ``destination_state``
- ``region_code`` (destination region from Silver enrichment)
- ``pickup_ts`` (and derived ``pickup_dow``, ``pickup_hour``,
  ``pickup_month``)
- ``promised_delivery_ts`` (the SLA, known at booking)
- ``promised_transit_hours`` (derived)
- ``distance_miles`` (known at booking)
- ``shipping_cost_usd`` (known at booking)
- ``carrier_historical_late_rate`` (computed only from shipments
  whose pickup and observed outcome precede the current pickup)
- ``route_historical_late_rate`` (same as-of rule)
- ``carrier_historical_shipment_count`` (same)
- ``route_historical_shipment_count`` (same)

How the guard works
-------------------

:func:`assert_no_leakage` is called by the feature engineering
pipeline and by the test suite.  It accepts a pandas DataFrame and
raises :class:`LeakageAuditError` if any column in
:data:`FORBIDDEN_FEATURE_COLUMNS` is present.  The test suite also
asserts that the public list of forbidden columns is non-empty and
that each entry is a string.
"""

from __future__ import annotations

from typing import Iterable

# ---------------------------------------------------------------------------
# Forbidden columns
# ---------------------------------------------------------------------------

FORBIDDEN_FEATURE_COLUMNS: tuple[str, ...] = (
    # The label source.
    "actual_delivery_ts",
    # 0/1 alias of the label.
    "on_time_delivery_flag",
    # Derived from the actual delivery time.
    "delay_minutes",
    "transit_time_hours",
    "delivered_flag",
    "exception_flag",
    # Updated at may equal actual_delivery_ts after delivery.
    "shipment_updated_at",
    # Delivery-event timestamps are only known during/after transit.
    "out_for_delivery_ts",
    "delivered_event_ts",
    "delivered_event_attempt_number",
    "exception_event_count",
    # Aggregates that depend on actual delivery (would be retrospective).
    "agg_total_shipments",
    "agg_delivered_shipments",
    "agg_on_time_shipments",
    "agg_late_shipments",
    "agg_exception_shipments",
)


# ---------------------------------------------------------------------------
# Allowed columns
# ---------------------------------------------------------------------------

ALLOWED_FEATURE_COLUMNS: tuple[str, ...] = (
    "shipment_id",
    "pickup_ts",
    "pickup_dow",
    "pickup_hour",
    "pickup_month",
    "carrier_id",
    "service_mode",
    "is_active",
    "home_region_code",
    "origin_state",
    "destination_state",
    "region_code",
    "origin_region_code",
    "promised_delivery_ts",
    "promised_transit_hours",
    "distance_miles",
    "shipping_cost_usd",
    "carrier_historical_late_rate",
    "carrier_historical_shipment_count",
    "route_historical_late_rate",
    "route_historical_shipment_count",
)


#: The list of columns that must appear in the labelled dataset but
#: are NOT model features (kept for evaluation / output joining).
LABEL_AND_META_COLUMNS: tuple[str, ...] = (
    "shipment_id",
    "pickup_ts",
    "is_late",
    "risk_probability",
    "risk_band",
    "predicted_late",
    "split",
)


class LeakageAuditError(ValueError):
    """Raised when a feature column would leak the target."""


def assert_no_leakage(
    column_names: Iterable[str],
    *,
    forbidden: tuple[str, ...] = FORBIDDEN_FEATURE_COLUMNS,
) -> None:
    """Raise :class:`LeakageAuditError` if any forbidden column is present.

    Args:
        column_names: Iterable of column names that would appear in
            the feature matrix.
        forbidden: Override the default forbidden column list (used
            by tests).

    Raises:
        LeakageAuditError: when at least one forbidden column is
            present.
    """
    column_set = {str(name) for name in column_names}
    leaks = sorted(column_set & set(forbidden))
    if leaks:
        raise LeakageAuditError(
            "Feature matrix contains forbidden column(s) that would "
            "leak the late-delivery target: " + ", ".join(leaks)
        )


__all__ = [
    "ALLOWED_FEATURE_COLUMNS",
    "FORBIDDEN_FEATURE_COLUMNS",
    "LABEL_AND_META_COLUMNS",
    "LeakageAuditError",
    "assert_no_leakage",
]
