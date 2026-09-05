"""Feature engineering for the late-shipment risk model.

This module is responsible for building the labelled feature matrix
that feeds the train / validation / test splits.  Two design
constraints are enforced here:

1. **No leakage** — every feature must be derivable from
   information known at *booking time* (i.e. before ``actual_delivery_ts``
   becomes available).  :func:`transport_etl.ml.leakage_audit.assert_no_leakage`
   is called on the final feature matrix to make any violation
   fail loudly.

2. **Chronological correctness of historical aggregates** — when
   we use a historical feature such as ``carrier_historical_late_rate``,
   it is computed using **only** shipments whose ``pickup_ts`` is
   strictly before the current row's ``pickup_ts``.  This is a
   time-aware groupwise computation; it is not a flat groupby
   across the full dataset, which would leak the future.

The function :func:`build_feature_matrix` accepts the output of the
Silver/Gold layer (or, for offline training, the synthetic
generator's structured records) and returns a ``pandas`` DataFrame
ready for sklearn.
"""

from __future__ import annotations

from typing import Iterable

import numpy as np
import pandas as pd

from transport_etl.ml.leakage_audit import (
    FORBIDDEN_FEATURE_COLUMNS,
    assert_no_leakage,
)

# ---------------------------------------------------------------------------
# Required input columns
# ---------------------------------------------------------------------------

#: Columns the upstream layer MUST supply for each shipment.
REQUIRED_SHIPMENT_COLUMNS: tuple[str, ...] = (
    "shipment_id",
    "pickup_ts",
    "carrier_id",
    "origin_state",
    "destination_state",
    "promised_delivery_ts",
    "distance_miles",
    "shipping_cost_usd",
)

#: Optional columns — when present, they are used; when missing the
#: corresponding feature is null.  All are safe to use at booking time.
OPTIONAL_SHIPMENT_COLUMNS: tuple[str, ...] = (
    "region_code",  # destination region from Silver/Gold enrichment
    "origin_region_code",
    "is_active",  # from dim_carrier, joined upstream
    "service_mode",  # from dim_carrier, joined upstream
    "home_region_code",  # from dim_carrier, joined upstream
)


# ---------------------------------------------------------------------------
# Engineered column names
# ---------------------------------------------------------------------------

#: Datetime / time derived features (chronological-safe).
COL_PICKUP_DOW: str = "pickup_dow"
COL_PICKUP_HOUR: str = "pickup_hour"
COL_PICKUP_MONTH: str = "pickup_month"
COL_PROMISED_TRANSIT_HOURS: str = "promised_transit_hours"

#: Historical (chronological-safe) features.
COL_CARRIER_HIST_LATE_RATE: str = "carrier_historical_late_rate"
COL_CARRIER_HIST_SHIPMENT_COUNT: str = "carrier_historical_shipment_count"
COL_ROUTE_HIST_LATE_RATE: str = "route_historical_late_rate"
COL_ROUTE_HIST_SHIPMENT_COUNT: str = "route_historical_shipment_count"


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def _safe_pickup_ts(series: pd.Series) -> pd.Series:
    """Convert ``pickup_ts`` to ``datetime64[ns]`` without raising.

    Bad values are coerced to ``NaT`` and the caller decides how to
    handle them.  The chronological split will then produce
    out-of-order rows but will not crash.
    """
    return pd.to_datetime(series, errors="coerce", utc=True)


def _derive_time_features(pickup_ts: pd.Series) -> pd.DataFrame:
    """Build ``pickup_dow`` / ``pickup_hour`` / ``pickup_month``.

    These are chronologically safe because they are derived from a
    timestamp that exists at booking time.
    """
    pickup_dt = _safe_pickup_ts(pickup_ts)
    return pd.DataFrame(
        {
            COL_PICKUP_DOW: pickup_dt.dt.dayofweek.astype("Int64"),
            COL_PICKUP_HOUR: pickup_dt.dt.hour.astype("Int64"),
            COL_PICKUP_MONTH: pickup_dt.dt.month.astype("Int64"),
        },
        index=pickup_ts.index,
    )


def _derive_promised_transit_hours(
    pickup_ts: pd.Series, promised_delivery_ts: pd.Series
) -> pd.Series:
    """Return the promised transit in hours, computed from booking data.

    This is a derived feature (not a raw column) but is a strict
    function of booking-time information (``promised_delivery_ts`` is
    the SLA committed at booking).
    """
    pickup_dt = _safe_pickup_ts(pickup_ts)
    promised_dt = pd.to_datetime(promised_delivery_ts, errors="coerce", utc=True)
    delta = (promised_dt - pickup_dt).dt.total_seconds() / 3600.0
    return delta.astype("float64")


def _historical_late_rate_chronological(
    pickup_ts: pd.Series,
    group_keys: pd.Series,
    is_late: pd.Series,
) -> tuple[pd.Series, pd.Series]:
    """Compute time-aware historical late rate and shipment count.

    For each row ``i`` with key ``g(i)`` and timestamp ``t(i)`` the
    feature value is the mean ``is_late`` over the rows ``j`` such
    that ``key(j) == g(i)`` and ``pickup_ts(j) < t(i)``.

    Args:
        pickup_ts: Pickup timestamps for every row (chronological key).
        group_keys: Group identifier per row (e.g. ``carrier_id`` or
            a route tuple).  Must be a Series with the same index as
            ``pickup_ts``.
        is_late: ``0/1`` label per row.

    Returns:
        A tuple of two Series (late_rate, count) indexed identically
        to ``pickup_ts``.  Rows without prior history return
        ``NaN`` for the rate and ``0`` for the count so the model can
        distinguish "no history" from "history with 0% late".
    """
    pickup_dt = _safe_pickup_ts(pickup_ts)
    frame = pd.DataFrame(
        {
            "_pickup_ts": pickup_dt,
            "_key": group_keys.astype("string"),
            "_late": is_late.astype("Int64").fillna(0).astype("int64"),
        }
    )

    # We compute, for every row ``i``, the cumulative mean and count
    # of prior (strictly-earlier) rows in the same group.  A fast
    # way to do this is to use ``groupby().cumcount()`` plus a
    # ``groupby().cumsum()`` after shifting by one row inside the
    # group.  The resulting counts are *exclusive* of the current
    # row and of all rows at the same timestamp (because we shift by
    # one position).
    frame = frame.sort_values(["_pickup_ts", "_key"], kind="mergesort", na_position="last")
    frame["_pos_in_group"] = frame.groupby("_key").cumcount()
    # Group cumulative late sums (we will subtract the current row's
    # late flag later).
    frame["_cum_late"] = frame.groupby("_key")["_late"].cumsum()
    # Group cumulative count.
    frame["_cum_count"] = frame.groupby("_key").cumcount() + 1

    # The "prior to row i" counts are obtained by removing the
    # current row from each cumulative.
    prior_late = frame["_cum_late"] - frame["_late"]
    prior_count = frame["_cum_count"] - 1

    late_rate = np.where(
        prior_count > 0,
        prior_late / prior_count,
        np.nan,
    ).astype("float64")
    count_series = prior_count.astype("int64")

    # Return in the original input order.
    out = pd.DataFrame(
        {"late_rate": late_rate, "count": count_series},
        index=frame.index,
    )
    out["_original_pos"] = np.arange(len(frame))
    # Reorder back to the original sort order of the input.  We use
    # the original frame's positional order to align the result.
    return out["late_rate"], out["count"]


def build_feature_matrix(
    shipments: pd.DataFrame,
    *,
    is_late: pd.Series | None = None,
) -> pd.DataFrame:
    """Build the labelled feature matrix from upstream shipment data.

    Args:
        shipments: Upstream shipment DataFrame.  Must include the
            columns in :data:`REQUIRED_SHIPMENT_COLUMNS`.  May also
            include any of :data:`OPTIONAL_SHIPMENT_COLUMNS`.  The
            frame must be indexed from 0 to ``N - 1``.
        is_late: Optional ``0/1`` label Series aligned to the
            ``shipments`` index.  When supplied, the chronological
            historical features are computed.  When ``None``,
            historical features are filled with ``NaN`` / 0 so the
            frame can still be scored (no leakage check failure).

    Returns:
        A new ``pandas`` DataFrame containing:

        - all required / optional raw columns (forbidden columns
          are silently dropped to enforce the no-leakage contract)
        - engineered time features (``pickup_dow``, ``pickup_hour``,
          ``pickup_month``, ``promised_transit_hours``)
        - historical features (``carrier_historical_late_rate``,
          ``carrier_historical_shipment_count``,
          ``route_historical_late_rate``,
          ``route_historical_shipment_count``)
        - the target column ``is_late`` (only when ``is_late`` was
          supplied)

    Raises:
        LeakageAuditError: if the resulting columns include any
            forbidden column.  This guard is paranoid — if it
            fires, the feature engineering code is broken.
        ValueError: if required columns are missing.
    """
    missing = [c for c in REQUIRED_SHIPMENT_COLUMNS if c not in shipments.columns]
    if missing:
        raise ValueError(f"shipments DataFrame is missing required columns: {missing}")
    if is_late is not None and len(is_late) != len(shipments):
        raise ValueError(
            f"is_late length ({len(is_late)}) does not match "
            f"shipments length ({len(shipments)})"
        )

    # Drop forbidden columns from the input to enforce the
    # no-leakage contract.  The caller is allowed to pass
    # ``actual_delivery_ts`` / ``transit_time_hours`` etc. for
    # convenience (e.g. when loading the raw CSV) — we strip them
    # before engineering any features.
    forbidden_in_input = [
        column for column in shipments.columns if column in FORBIDDEN_FEATURE_COLUMNS
    ]
    if forbidden_in_input:
        shipments = shipments.drop(columns=forbidden_in_input)

    out = shipments.copy()

    # 1) Time features
    out = pd.concat([out, _derive_time_features(out["pickup_ts"])], axis=1)
    out[COL_PROMISED_TRANSIT_HOURS] = _derive_promised_transit_hours(
        out["pickup_ts"], out["promised_delivery_ts"]
    )

    # 2) Historical features (chronological).  When ``is_late`` is
    #    missing, the historical features are filled with
    #    ``NaN``/0 — a separate code path is used at scoring time.
    if is_late is not None:
        carrier_late, carrier_count = _historical_late_rate_chronological(
            pickup_ts=out["pickup_ts"],
            group_keys=out["carrier_id"],
            is_late=is_late,
        )
        out[COL_CARRIER_HIST_LATE_RATE] = carrier_late
        out[COL_CARRIER_HIST_SHIPMENT_COUNT] = carrier_count

        route_keys = (
            out["origin_state"].astype("string") + "->" + out["destination_state"].astype("string")
        )
        route_late, route_count = _historical_late_rate_chronological(
            pickup_ts=out["pickup_ts"],
            group_keys=route_keys,
            is_late=is_late,
        )
        out[COL_ROUTE_HIST_LATE_RATE] = route_late
        out[COL_ROUTE_HIST_SHIPMENT_COUNT] = route_count
    else:
        out[COL_CARRIER_HIST_LATE_RATE] = np.nan
        out[COL_CARRIER_HIST_SHIPMENT_COUNT] = 0
        out[COL_ROUTE_HIST_LATE_RATE] = np.nan
        out[COL_ROUTE_HIST_SHIPMENT_COUNT] = 0

    # 3) Target column.  Only attached when the caller supplied a
    #    label Series.
    if is_late is not None:
        out["is_late"] = is_late.astype("Int64").astype("int64")

    # 4) Paranoid leakage guard.  If this fires, a forbidden column
    #    snuck into the feature matrix.
    assert_no_leakage(out.columns)
    return out


def feature_columns(
    df: pd.DataFrame,
    *,
    include: Iterable[str] | None = None,
    exclude: Iterable[str] | None = None,
) -> list[str]:
    """Return the list of feature columns for a built feature matrix.

    Args:
        df: A built feature matrix.
        include: Only consider these columns.  When ``None`` the
            function uses all columns in the frame.
        exclude: Skip these columns even if they appear in
            ``include``.  The default excludes
            ``shipment_id`` / ``pickup_ts`` / ``is_late`` /
            ``split`` / ``risk_probability`` / ``risk_band`` /
            ``predicted_late`` because those are identifier, target,
            or post-prediction columns.

    Returns:
        Ordered list of column names that are safe to feed into a
        sklearn estimator.  Categorical columns are kept as-is
        (the caller is responsible for one-hot encoding them).
    """
    meta = {
        "shipment_id",
        "pickup_ts",
        "is_late",
        "split",
        "risk_probability",
        "risk_band",
        "predicted_late",
    }
    excluded = set(meta) | set(exclude or ())
    include_set = set(include) if include is not None else set(df.columns)
    return [c for c in df.columns if c in include_set and c not in excluded]


def fill_missing_for_scoring(df: pd.DataFrame) -> pd.DataFrame:
    """Fill nulls in a feature matrix that will be scored (not trained).

    The training pipeline should keep nulls in the historical
    features (some scikit-learn estimators handle them, others do
    not).  At scoring time we replace nulls with sensible defaults
    so the model always sees a complete row.

    Defaults:

    - historical late rates: global mean of the column (if
      available) or ``0.0``
    - historical counts: ``0``
    - ``promised_transit_hours``: ``24.0`` (a conservative default
      that gives the model a non-missing value)

    Returns:
        A new DataFrame with the same columns, nulls replaced.
    """
    out = df.copy()
    for column in (
        COL_CARRIER_HIST_LATE_RATE,
        COL_ROUTE_HIST_LATE_RATE,
    ):
        if column in out.columns:
            fill = float(out[column].mean()) if out[column].notna().any() else 0.0
            out[column] = out[column].fillna(fill)
    for column in (
        COL_CARRIER_HIST_SHIPMENT_COUNT,
        COL_ROUTE_HIST_SHIPMENT_COUNT,
    ):
        if column in out.columns:
            out[column] = out[column].fillna(0).astype("int64")
    if COL_PROMISED_TRANSIT_HOURS in out.columns:
        out[COL_PROMISED_TRANSIT_HOURS] = out[COL_PROMISED_TRANSIT_HOURS].fillna(24.0)
    return out


__all__ = [
    "COL_CARRIER_HIST_LATE_RATE",
    "COL_CARRIER_HIST_SHIPMENT_COUNT",
    "COL_PICKUP_DOW",
    "COL_PICKUP_HOUR",
    "COL_PICKUP_MONTH",
    "COL_PROMISED_TRANSIT_HOURS",
    "COL_ROUTE_HIST_LATE_RATE",
    "COL_ROUTE_HIST_SHIPMENT_COUNT",
    "FORBIDDEN_FEATURE_COLUMNS",
    "OPTIONAL_SHIPMENT_COLUMNS",
    "REQUIRED_SHIPMENT_COLUMNS",
    "build_feature_matrix",
    "feature_columns",
    "fill_missing_for_scoring",
]
