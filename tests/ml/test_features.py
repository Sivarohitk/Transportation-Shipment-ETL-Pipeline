"""Tests for the feature-engineering module.

These tests assert:

- the chronological correctness of the historical aggregate
  features (a row only uses *prior* shipments for its history);
- the feature matrix never contains a forbidden column;
- the engineered columns are populated and well-typed;
- the scoring-time fill helper replaces nulls with sane defaults.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from transport_etl.ml.features import (
    COL_CARRIER_HIST_LATE_RATE,
    COL_CARRIER_HIST_SHIPMENT_COUNT,
    COL_PICKUP_DOW,
    COL_PICKUP_HOUR,
    COL_PROMISED_TRANSIT_HOURS,
    COL_ROUTE_HIST_LATE_RATE,
    COL_ROUTE_HIST_SHIPMENT_COUNT,
    REQUIRED_SHIPMENT_COLUMNS,
    build_feature_matrix,
    feature_columns,
    fill_missing_for_scoring,
)
from transport_etl.ml.leakage_audit import (
    FORBIDDEN_FEATURE_COLUMNS,
    assert_no_leakage,
)


def _build_minimal_shipments(n: int, *, seed: int = 0) -> pd.DataFrame:
    """Return a frame with the required columns and a tiny schedule."""
    rng = np.random.default_rng(seed)
    base = pd.Timestamp("2025-01-01", tz="UTC")
    pickup_ts = [base + pd.Timedelta(hours=int(i)) for i in range(n)]
    return pd.DataFrame(
        {
            "shipment_id": [f"SHP{i:06d}" for i in range(n)],
            "pickup_ts": pickup_ts,
            "carrier_id": ["CAR001" if i % 2 == 0 else "CAR002" for i in range(n)],
            "origin_state": ["CA" if i % 3 == 0 else "TX" for i in range(n)],
            "destination_state": ["NV" if i % 3 == 0 else "OH" for i in range(n)],
            "promised_delivery_ts": [t + pd.Timedelta(hours=24) for t in pickup_ts],
            "distance_miles": rng.uniform(100, 1500, size=n),
            "shipping_cost_usd": rng.uniform(50, 2000, size=n),
        }
    )


def _build_labels(df: pd.DataFrame, *, seed: int = 0) -> pd.Series:
    """Produce a ``0/1`` is_late label Series aligned to the frame."""
    rng = np.random.default_rng(seed)
    n = len(df)
    return pd.Series(rng.integers(0, 2, size=n), index=df.index, name="is_late")


class TestBuildFeatureMatrix:
    def test_returns_features_for_each_required_column(self) -> None:
        df = _build_minimal_shipments(20)
        labels = _build_labels(df)
        features = build_feature_matrix(df, is_late=labels)
        for column in REQUIRED_SHIPMENT_COLUMNS:
            assert column in features.columns
        # Engineered columns are present.
        for column in (
            COL_PICKUP_DOW,
            COL_PICKUP_HOUR,
            COL_PROMISED_TRANSIT_HOURS,
            COL_CARRIER_HIST_LATE_RATE,
            COL_CARRIER_HIST_SHIPMENT_COUNT,
            COL_ROUTE_HIST_LATE_RATE,
            COL_ROUTE_HIST_SHIPMENT_COUNT,
        ):
            assert column in features.columns
        # The label is attached.
        assert "is_late" in features.columns

    def test_never_includes_forbidden_columns(self) -> None:
        df = _build_minimal_shipments(50)
        labels = _build_labels(df)
        features = build_feature_matrix(df, is_late=labels)
        for column in FORBIDDEN_FEATURE_COLUMNS:
            assert column not in features.columns
        # Also exercise the paranoid guard.
        assert_no_leakage(features.columns)

    def test_rejects_missing_required_columns(self) -> None:
        df = _build_minimal_shipments(20).drop(columns=["carrier_id"])
        with pytest.raises(ValueError, match="missing required columns"):
            build_feature_matrix(df, is_late=_build_labels(df))

    def test_historical_features_chronological(self) -> None:
        """A row's historical features must be built only from rows
        whose ``pickup_ts`` is strictly before its own."""
        df = _build_minimal_shipments(50)
        labels = _build_labels(df)
        features = build_feature_matrix(df, is_late=labels)

        # For each row ``i``, the historical carrier count must equal
        # the number of rows in the same carrier whose ``pickup_ts``
        # is strictly less than ``pickup_ts[i]``.
        expected = []
        df_sorted = df.sort_values("pickup_ts", kind="mergesort").reset_index(drop=True)
        for i, row in df_sorted.iterrows():
            prior = df_sorted.iloc[:i]
            count = (prior["carrier_id"] == row["carrier_id"]).sum()
            expected.append(count)
        expected_series = pd.Series(expected, index=df_sorted.index)

        # The historical count column in the feature matrix must be
        # aligned to the same row order.
        actual = (
            features.sort_values("pickup_ts", kind="mergesort")
            .set_index("shipment_id")["carrier_historical_shipment_count"]
            .reindex(df_sorted["shipment_id"])
            .reset_index(drop=True)
        )
        np.testing.assert_array_equal(actual.to_numpy(), expected_series.to_numpy())

    def test_historical_rate_equals_prior_late_over_prior_total(self) -> None:
        """The historical late rate is the prior cumulative late
        count divided by the prior cumulative total count."""
        df = _build_minimal_shipments(60)
        labels = _build_labels(df)
        features = build_feature_matrix(df, is_late=labels)

        # Compute the expected per-carrier rate from the same input.
        merged = df.merge(labels.rename("is_late"), left_index=True, right_index=True)
        merged = merged.sort_values("pickup_ts", kind="mergesort").reset_index(drop=True)
        merged["_is_late_int"] = merged["is_late"].astype(int)
        merged["_cum_late"] = merged.groupby("carrier_id")["_is_late_int"].cumsum()
        merged["_pos"] = merged.groupby("carrier_id").cumcount() + 1
        merged["_prior_late"] = merged["_cum_late"] - merged["_is_late_int"]
        merged["_prior_total"] = merged["_pos"] - 1
        # The feature matrix should match.
        actual = features.sort_values("pickup_ts", kind="mergesort")
        merged = merged.set_index("shipment_id").loc[actual["shipment_id"]]
        np.testing.assert_allclose(
            actual["carrier_historical_late_rate"].to_numpy(),
            np.where(
                merged["_prior_total"].to_numpy() > 0,
                merged["_prior_late"].to_numpy() / merged["_prior_total"].to_numpy(),
                np.nan,
            ),
            equal_nan=True,
        )

    def test_first_shipment_per_carrier_has_no_history(self) -> None:
        """The first shipment for each carrier must have NaN / 0
        history values (no prior shipments exist)."""
        df = _build_minimal_shipments(8)
        labels = _build_labels(df)
        features = build_feature_matrix(df, is_late=labels)
        # Sort by pickup_ts to find the earliest row per carrier.
        sorted_features = features.sort_values("pickup_ts", kind="mergesort")
        first_per_carrier = sorted_features.groupby("carrier_id").head(1)
        for column in (
            COL_CARRIER_HIST_LATE_RATE,
            COL_CARRIER_HIST_SHIPMENT_COUNT,
            COL_ROUTE_HIST_LATE_RATE,
            COL_ROUTE_HIST_SHIPMENT_COUNT,
        ):
            if column.endswith("count"):
                assert int(first_per_carrier[column].iloc[0]) == 0
            else:
                assert np.isnan(first_per_carrier[column].iloc[0])

    def test_promised_transit_hours_is_positive(self) -> None:
        df = _build_minimal_shipments(20)
        labels = _build_labels(df)
        features = build_feature_matrix(df, is_late=labels)
        # The frame was built with 24h promised transit, so the
        # feature should be near 24.0 everywhere.
        assert (features[COL_PROMISED_TRANSIT_HOURS] > 23.5).all()
        assert (features[COL_PROMISED_TRANSIT_HOURS] < 24.5).all()

    def test_pickup_time_features_are_in_expected_range(self) -> None:
        df = _build_minimal_shipments(20)
        labels = _build_labels(df)
        features = build_feature_matrix(df, is_late=labels)
        # ``pickup_dow`` is pandas ``dayofweek``: Monday=0 … Sunday=6.
        assert features[COL_PICKUP_DOW].between(0, 6).all()
        assert features[COL_PICKUP_HOUR].between(0, 23).all()

    def test_no_label_no_label_column(self) -> None:
        """When called without ``is_late``, the frame does not include
        the label column.  Historical features are filled with
        NaN / 0 (no prior history)."""
        df = _build_minimal_shipments(10)
        features = build_feature_matrix(df)  # no is_late
        assert "is_late" not in features.columns
        for column in (
            COL_CARRIER_HIST_LATE_RATE,
            COL_CARRIER_HIST_SHIPMENT_COUNT,
            COL_ROUTE_HIST_LATE_RATE,
            COL_ROUTE_HIST_SHIPMENT_COUNT,
        ):
            assert column in features.columns
        # Historical counts are zero; rates are NaN.
        assert (features[COL_CARRIER_HIST_SHIPMENT_COUNT] == 0).all()
        assert features[COL_CARRIER_HIST_LATE_RATE].isna().all()


class TestFeatureColumns:
    def test_excludes_meta_columns(self) -> None:
        df = _build_minimal_shipments(20)
        features = build_feature_matrix(df, is_late=_build_labels(df))
        feats = feature_columns(features)
        # Default-excluded columns are not returned.
        for meta in ("shipment_id", "is_late", "split", "risk_probability"):
            assert meta not in feats

    def test_includes_engineered_columns(self) -> None:
        df = _build_minimal_shipments(20)
        features = build_feature_matrix(df, is_late=_build_labels(df))
        feats = feature_columns(features)
        for column in (
            "pickup_dow",
            "pickup_hour",
            "promised_transit_hours",
            "carrier_historical_late_rate",
        ):
            assert column in feats


class TestFillMissingForScoring:
    def test_fills_nulls_with_mean(self) -> None:
        df = pd.DataFrame(
            {
                "carrier_historical_late_rate": [0.1, np.nan, 0.3, np.nan],
                "route_historical_late_rate": [0.2, 0.4, np.nan, np.nan],
                "carrier_historical_shipment_count": [1.0, np.nan, 3.0, 2.0],
                "promised_transit_hours": [24.0, np.nan, 12.0, 48.0],
            }
        )
        out = fill_missing_for_scoring(df)
        # Means are 0.2 and 0.3.
        assert out["carrier_historical_late_rate"].iloc[1] == pytest.approx(0.2)
        assert out["route_historical_late_rate"].iloc[2] == pytest.approx(0.3)
        # Counts become 0 (integer).
        assert out["carrier_historical_shipment_count"].iloc[1] == 0
        # Promised transit gets 24.0 default.
        assert out["promised_transit_hours"].iloc[1] == 24.0

    def test_preserves_existing_values(self) -> None:
        df = pd.DataFrame(
            {
                "carrier_historical_late_rate": [0.42, 0.5, 0.6],
                "carrier_historical_shipment_count": [1, 2, 3],
            }
        )
        out = fill_missing_for_scoring(df)
        assert out["carrier_historical_late_rate"].tolist() == [0.42, 0.5, 0.6]
        assert out["carrier_historical_shipment_count"].tolist() == [1, 2, 3]
