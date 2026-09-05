"""Tests for the scoring output schema and the risk-band output.

These tests assert:

- the scoring output has exactly the documented columns;
- the ``risk_probability`` column lies in ``[0, 1]``;
- the ``risk_band`` column is one of the documented bands;
- the ``predicted_late`` column is a 0/1 integer;
- the ``score_summary`` helper returns a well-formed dict;
- forbidden columns are still rejected at the scoring boundary.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from transport_etl.ml.constants import (
    ALL_RISK_BANDS,
    ALL_SCORE_COLUMNS,
    RISK_BAND_CRITICAL,
    RISK_BAND_HIGH,
    RISK_BAND_LOW,
    RISK_BAND_MEDIUM,
    SCORE_COLUMN_ACTUAL,
    SCORE_COLUMN_BAND,
    SCORE_COLUMN_PICKUP_TS,
    SCORE_COLUMN_PREDICTED,
    SCORE_COLUMN_PROBABILITY,
    SCORE_COLUMN_SHIPMENT_ID,
)
from transport_etl.ml.features import build_feature_matrix
from transport_etl.ml.leakage_audit import (
    FORBIDDEN_FEATURE_COLUMNS,
)
from transport_etl.ml.scoring import score_shipments, score_summary
from transport_etl.ml.training import train_model


def _build_dataset(n: int, *, seed: int = 0) -> tuple[pd.DataFrame, pd.Series]:
    rng = np.random.default_rng(seed)
    base = pd.Timestamp("2025-01-01", tz="UTC")
    pickup_ts = [base + pd.Timedelta(hours=int(i)) for i in range(n)]
    shipments = pd.DataFrame(
        {
            "shipment_id": [f"SHP{i:06d}" for i in range(n)],
            "pickup_ts": pickup_ts,
            "carrier_id": rng.choice(["CAR001", "CAR002"], size=n),
            "origin_state": rng.choice(["CA", "TX"], size=n),
            "destination_state": rng.choice(["NV", "OH"], size=n),
            "promised_delivery_ts": [t + pd.Timedelta(hours=24) for t in pickup_ts],
            "distance_miles": rng.uniform(100, 1500, size=n),
            "shipping_cost_usd": rng.uniform(50, 2000, size=n),
            "region_code": "UNKNOWN",
            "origin_region_code": "UNKNOWN",
            "service_mode": "LTL",
            "is_active": True,
            "home_region_code": "UNKNOWN",
        }
    )
    is_late = pd.Series(rng.integers(0, 2, size=n), index=shipments.index, name="is_late")
    features = build_feature_matrix(shipments, is_late=is_late)
    return features, is_late


class TestScoreOutputSchema:
    def test_score_columns_are_documented(self) -> None:
        assert set(ALL_SCORE_COLUMNS) == {
            SCORE_COLUMN_SHIPMENT_ID,
            SCORE_COLUMN_PICKUP_TS,
            SCORE_COLUMN_PROBABILITY,
            SCORE_COLUMN_BAND,
            SCORE_COLUMN_PREDICTED,
            SCORE_COLUMN_ACTUAL,
        }

    def test_score_output_has_documented_columns(self) -> None:
        features, is_late = _build_dataset(100)
        model = train_model(features, model_name="logistic_regression")
        scored = score_shipments(
            model,
            features,
            actual_late=is_late,
        )
        # The output must include every documented column.
        for column in ALL_SCORE_COLUMNS:
            assert column in scored.columns
        # No extra columns.
        assert set(scored.columns) == set(ALL_SCORE_COLUMNS)

    def test_score_probabilities_in_unit_interval(self) -> None:
        features, is_late = _build_dataset(50)
        model = train_model(features, model_name="logistic_regression")
        scored = score_shipments(model, features, actual_late=is_late)
        proba = scored[SCORE_COLUMN_PROBABILITY].to_numpy()
        assert ((proba >= 0.0) & (proba <= 1.0)).all()

    def test_score_bands_are_canonical(self) -> None:
        features, is_late = _build_dataset(50)
        model = train_model(features, model_name="logistic_regression")
        scored = score_shipments(model, features, actual_late=is_late)
        bands = set(scored[SCORE_COLUMN_BAND].unique())
        assert bands.issubset(set(ALL_RISK_BANDS))

    def test_score_predicted_late_is_0_or_1(self) -> None:
        features, is_late = _build_dataset(50)
        model = train_model(features, model_name="logistic_regression")
        scored = score_shipments(model, features, actual_late=is_late)
        predicted = scored[SCORE_COLUMN_PREDICTED].to_numpy()
        assert set(np.unique(predicted)).issubset({0, 1})

    def test_score_actual_late_is_preserved(self) -> None:
        features, is_late = _build_dataset(50)
        model = train_model(features, model_name="logistic_regression")
        scored = score_shipments(model, features, actual_late=is_late)
        np.testing.assert_array_equal(scored[SCORE_COLUMN_ACTUAL].to_numpy(), is_late.to_numpy())

    def test_score_band_matches_risk_probability(self) -> None:
        features, is_late = _build_dataset(50)
        model = train_model(features, model_name="logistic_regression")
        scored = score_shipments(model, features, actual_late=is_late)
        for _, row in scored.iterrows():
            p = float(row[SCORE_COLUMN_PROBABILITY])
            band = str(row[SCORE_COLUMN_BAND])
            expected = (
                RISK_BAND_LOW
                if p < 0.10
                else (
                    RISK_BAND_MEDIUM
                    if p < 0.25
                    else RISK_BAND_HIGH if p < 0.50 else RISK_BAND_CRITICAL
                )
            )
            assert band == expected

    def test_score_predicted_late_matches_threshold(self) -> None:
        features, is_late = _build_dataset(50)
        model = train_model(features, model_name="logistic_regression")
        scored = score_shipments(model, features, actual_late=is_late, decision_threshold=0.30)
        for _, row in scored.iterrows():
            p = float(row[SCORE_COLUMN_PROBABILITY])
            predicted = int(row[SCORE_COLUMN_PREDICTED])
            expected = 1 if p >= 0.30 else 0
            assert predicted == expected


class TestScoreSummary:
    def test_summary_keys(self) -> None:
        features, is_late = _build_dataset(50)
        model = train_model(features, model_name="logistic_regression")
        scored = score_shipments(model, features, actual_late=is_late)
        summary = score_summary(scored)
        for key in (
            "row_count",
            "mean_risk_probability",
            "predicted_late_count",
            "band_low_count",
            "band_medium_count",
            "band_high_count",
            "band_critical_count",
        ):
            assert key in summary
        assert summary["row_count"] == 50
        assert isinstance(summary["mean_risk_probability"], float)

    def test_summary_band_counts_sum_to_row_count(self) -> None:
        features, is_late = _build_dataset(50)
        model = train_model(features, model_name="logistic_regression")
        scored = score_shipments(model, features, actual_late=is_late)
        summary = score_summary(scored)
        total = (
            summary["band_low_count"]
            + summary["band_medium_count"]
            + summary["band_high_count"]
            + summary["band_critical_count"]
        )
        assert total == summary["row_count"]


class TestScoreLeakageGuard:
    def test_output_frame_never_contains_forbidden_columns(self) -> None:
        features, is_late = _build_dataset(20)
        model = train_model(features, model_name="logistic_regression")
        # The score input may have many columns; the OUTPUT must
        # only contain the documented score columns.  The
        # ``assert_no_leakage`` guard is enforced on the output.
        bad = features.copy()
        bad["actual_delivery_ts"] = pd.Timestamp("2025-01-01", tz="UTC")
        scored = score_shipments(model, bad, actual_late=is_late)
        for column in scored.columns:
            assert (
                column not in FORBIDDEN_FEATURE_COLUMNS
            ), f"Output frame contains forbidden column {column!r}"
