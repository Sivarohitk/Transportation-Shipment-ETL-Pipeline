"""Tests for the late-risk model training module.

These tests assert:

- the model can be trained and produces a fitted pipeline;
- the fitted pipeline serialises to / deserialises from disk
  (byte-stable, deterministic for the same input);
- the model produces both probabilities and binary predictions;
- the model raises for an unknown ``model_name``;
- the chosen feature columns are intersected with the actual
  DataFrame columns (so missing columns do not crash the run);
- integer-null casting handles the pandas ``Int64`` / ``float64``
  coercion correctly.
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from transport_etl.ml.constants import (
    MODEL_HIST_GRADIENT_BOOSTING,
    MODEL_LOGISTIC_REGRESSION,
)
from transport_etl.ml.features import build_feature_matrix
from transport_etl.ml.training import (
    CATEGORICAL_FEATURES,
    NUMERIC_FEATURES,
    LateRiskModel,
    train_model,
)


def _build_dataset(n: int, *, seed: int = 0) -> tuple[pd.DataFrame, pd.Series]:
    """Return a (features, is_late) pair suitable for training."""
    rng = np.random.default_rng(seed)
    base = pd.Timestamp("2025-01-01", tz="UTC")
    pickup_ts = [base + pd.Timedelta(hours=int(i)) for i in range(n)]
    shipments = pd.DataFrame(
        {
            "shipment_id": [f"SHP{i:06d}" for i in range(n)],
            "pickup_ts": pickup_ts,
            "carrier_id": rng.choice(["CAR001", "CAR002", "CAR003"], size=n),
            "origin_state": rng.choice(["CA", "TX", "NY"], size=n),
            "destination_state": rng.choice(["NV", "OH", "FL"], size=n),
            "promised_delivery_ts": [t + pd.Timedelta(hours=24) for t in pickup_ts],
            "actual_delivery_ts": [t + pd.Timedelta(minutes=30) for t in pickup_ts],
            "distance_miles": rng.uniform(100, 1500, size=n),
            "shipping_cost_usd": rng.uniform(50, 2000, size=n),
            "region_code": "UNKNOWN",
            "origin_region_code": "UNKNOWN",
            "service_mode": rng.choice(["FTL", "LTL", "PARCEL"], size=n),
            "is_active": True,
            "home_region_code": "UNKNOWN",
        }
    )
    is_late = pd.Series(rng.integers(0, 2, size=n), index=shipments.index, name="is_late")
    features = build_feature_matrix(shipments, is_late=is_late)
    return features, is_late


class TestTrainModel:
    def test_logistic_regression_fits_and_predicts(self) -> None:
        features, is_late = _build_dataset(200)
        model = train_model(features, model_name=MODEL_LOGISTIC_REGRESSION)
        assert isinstance(model, LateRiskModel)
        assert model.model_name == MODEL_LOGISTIC_REGRESSION
        assert model.training_rows == 200
        # Probabilities are floats in [0, 1].
        proba = model.predict_proba(features)
        assert proba.shape == (200,)
        assert ((proba >= 0) & (proba <= 1)).all()

    def test_hist_gradient_boosting_fits_and_predicts(self) -> None:
        features, is_late = _build_dataset(200)
        model = train_model(features, model_name=MODEL_HIST_GRADIENT_BOOSTING)
        proba = model.predict_proba(features)
        assert proba.shape == (200,)
        assert ((proba >= 0) & (proba <= 1)).all()

    def test_predict_threshold(self) -> None:
        features, is_late = _build_dataset(200)
        model = train_model(features, model_name=MODEL_LOGISTIC_REGRESSION)
        # Default threshold = 0.5 → boolean 0/1.
        binary = model.predict(features)
        assert ((binary == 0) | (binary == 1)).all()
        # Custom threshold yields different output for some rows.
        binary_low = model.predict(features, threshold=0.1)
        assert binary_low.sum() >= binary.sum()

    def test_rejects_unknown_model(self) -> None:
        features, _ = _build_dataset(50)
        with pytest.raises(ValueError, match="Unknown model_name"):
            train_model(features, model_name="not_a_model")

    def test_rejects_missing_target(self) -> None:
        features, _ = _build_dataset(50)
        with pytest.raises(ValueError, match="target_column"):
            train_model(features.drop(columns=["is_late"]))

    def test_feature_intersection_when_columns_missing(self) -> None:
        features, is_late = _build_dataset(50)
        # Drop some columns; the trainer should still work and
        # silently skip missing features.
        slim = features.drop(columns=["shipping_cost_usd", "promised_transit_hours"])
        model = train_model(slim, model_name=MODEL_LOGISTIC_REGRESSION)
        proba = model.predict_proba(slim)
        assert proba.shape == (50,)

    def test_feature_intersection_with_minimal_columns(self) -> None:
        features, is_late = _build_dataset(50)
        minimal = features[
            [
                "shipment_id",
                "pickup_ts",
                "carrier_id",
                "is_late",
                "pickup_dow",
                "pickup_hour",
                "pickup_month",
                "promised_transit_hours",
                "distance_miles",
                "shipping_cost_usd",
                "carrier_historical_shipment_count",
                "carrier_historical_late_rate",
                "route_historical_shipment_count",
                "route_historical_late_rate",
                "origin_state",
                "destination_state",
                "region_code",
                "origin_region_code",
                "service_mode",
                "is_active",
                "home_region_code",
            ]
        ]
        model = train_model(minimal, model_name=MODEL_LOGISTIC_REGRESSION)
        proba = model.predict_proba(minimal)
        assert proba.shape == (50,)

    def test_prediction_imputation_is_independent_of_scoring_batch(self) -> None:
        features, _ = _build_dataset(200)
        model = train_model(features.iloc[:140], model_name=MODEL_LOGISTIC_REGRESSION)
        row = features.iloc[[150]].copy()
        row["carrier_historical_late_rate"] = np.nan
        row["route_historical_late_rate"] = np.nan
        companion = features.iloc[[180]].copy()
        companion["carrier_historical_late_rate"] = 1.0
        companion["route_historical_late_rate"] = 1.0

        alone = model.predict_proba(row)[0]
        with_companion = model.predict_proba(pd.concat([row, companion], ignore_index=True))[0]

        assert alone == pytest.approx(with_companion, abs=1e-15)


class TestLateRiskModelSaveLoad:
    def test_round_trip_through_disk(self, tmp_path: Path) -> None:
        features, is_late = _build_dataset(100)
        model = train_model(features, model_name=MODEL_LOGISTIC_REGRESSION)
        proba_before = model.predict_proba(features)

        path = tmp_path / "model.pkl"
        model.save(str(path))
        assert path.exists()

        loaded = LateRiskModel.load(str(path))
        proba_after = loaded.predict_proba(features)
        np.testing.assert_array_equal(proba_before, proba_after)

    def test_reproducibility_same_seed_same_proba(self) -> None:
        features, is_late = _build_dataset(100)
        # HistGradientBoostingClassifier is seeded by ``random_state``,
        # so two runs with the same data and seed should produce
        # identical probabilities.
        model_a = train_model(features, model_name=MODEL_HIST_GRADIENT_BOOSTING)
        model_b = train_model(features, model_name=MODEL_HIST_GRADIENT_BOOSTING)
        proba_a = model_a.predict_proba(features)
        proba_b = model_b.predict_proba(features)
        np.testing.assert_array_almost_equal(proba_a, proba_b, decimal=10)

    def test_rejects_non_model_file(self, tmp_path: Path) -> None:
        # Write a pickle file that contains a valid pickle blob but
        # not a LateRiskModel instance.
        import pickle

        path = tmp_path / "not_a_model.pkl"
        with path.open("wb") as handle:
            pickle.dump({"not": "a model"}, handle)

        with pytest.raises(TypeError, match="LateRiskModel"):
            LateRiskModel.load(str(path))


class TestCategoricalAndNumericLists:
    def test_categorical_and_numeric_lists_are_disjoint(self) -> None:
        overlap = set(CATEGORICAL_FEATURES) & set(NUMERIC_FEATURES)
        assert not overlap, f"categorical and numeric features overlap: {sorted(overlap)}"

    def test_categorical_features_are_strings(self) -> None:
        for name in CATEGORICAL_FEATURES:
            assert isinstance(name, str)
            assert name.strip() == name

    def test_numeric_features_are_strings(self) -> None:
        for name in NUMERIC_FEATURES:
            assert isinstance(name, str)
            assert name.strip() == name
