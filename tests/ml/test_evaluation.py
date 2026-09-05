"""Tests for the evaluation module and the risk-band classifier.

These tests assert:

- the evaluation report is computed correctly on a simple
  deterministic example;
- risk-band classification matches the documented thresholds;
- the threshold trade-off table is well-formed;
- calibration buckets are produced when probabilities vary;
- degenerate inputs (all zero, all one) produce NaN metrics
  without crashing.
"""

from __future__ import annotations

import math

import numpy as np
import pytest

from transport_etl.ml.constants import (
    RISK_BAND_CRITICAL,
    RISK_BAND_HIGH,
    RISK_BAND_LOW,
    RISK_BAND_MEDIUM,
    classify_risk_band,
)
from transport_etl.ml.evaluation import (
    evaluate_dataframe,
    evaluate_predictions,
)


def test_classify_risk_band_thresholds() -> None:
    # Boundary values.
    assert classify_risk_band(0.0) == RISK_BAND_LOW
    assert classify_risk_band(0.0999) == RISK_BAND_LOW
    assert classify_risk_band(0.10) == RISK_BAND_MEDIUM
    assert classify_risk_band(0.2499) == RISK_BAND_MEDIUM
    assert classify_risk_band(0.25) == RISK_BAND_HIGH
    assert classify_risk_band(0.4999) == RISK_BAND_HIGH
    assert classify_risk_band(0.50) == RISK_BAND_CRITICAL
    assert classify_risk_band(0.9999) == RISK_BAND_CRITICAL
    # Out-of-range values are clipped.
    assert classify_risk_band(-0.5) == RISK_BAND_LOW
    assert classify_risk_band(1.5) == RISK_BAND_CRITICAL


def test_classify_risk_band_returns_string() -> None:
    for value in (0.0, 0.1, 0.5, 0.99):
        band = classify_risk_band(value)
        assert isinstance(band, str)
        assert band in {RISK_BAND_LOW, RISK_BAND_MEDIUM, RISK_BAND_HIGH, RISK_BAND_CRITICAL}


class TestEvaluatePredictions:
    def test_simple_perfect_predictions(self) -> None:
        y_true = np.array([0, 0, 0, 0, 1, 1, 1, 1])
        # Perfectly ranked predictions (all zeros low, all ones high).
        proba = np.array([0.1, 0.2, 0.3, 0.4, 0.6, 0.7, 0.8, 0.9])
        report = evaluate_predictions(y_true, proba, split_name="test", default_threshold=0.5)
        assert report.row_count == 8
        assert report.positive_count == 4
        assert report.negative_count == 4
        assert report.positive_rate == 0.5
        # Perfect AUC.
        assert report.auc_roc == 1.0
        # At threshold 0.5, all 1s are flagged.
        assert report.precision_at_default == 1.0
        assert report.recall_at_default == 1.0
        assert report.f1_at_default == 1.0
        # Confusion matrix is [[4, 0], [0, 4]].
        assert report.confusion_matrix_at_default == [[4, 0], [0, 4]]
        # Optimal threshold is at or below the lowest positive proba.
        assert math.isfinite(report.threshold_opt_f1)
        assert report.f1_opt == 1.0

    def test_simple_random_predictions(self) -> None:
        # 50/50 mix, 50% positive rate with random probabilities.
        y_true = np.array([0] * 50 + [1] * 50)
        rng = np.random.default_rng(0)
        proba = rng.uniform(0, 1, size=100)
        report = evaluate_predictions(y_true, proba, split_name="validation", default_threshold=0.5)
        assert report.row_count == 100
        assert report.positive_count == 50
        # AUC is around 0.5 (random).
        assert 0.3 < report.auc_roc < 0.7
        # The trade-off table is non-empty.
        assert report.threshold_tradeoffs
        # At threshold 0.5, the predicted_late set is roughly half
        # the rows, so precision and recall should be in (0, 1).
        assert 0.0 <= report.precision_at_default <= 1.0
        assert 0.0 <= report.recall_at_default <= 1.0

    def test_all_positive_predictions(self) -> None:
        y_true = np.array([1, 1, 1, 1])
        proba = np.array([0.6, 0.7, 0.8, 0.9])
        report = evaluate_predictions(y_true, proba, default_threshold=0.5)
        # AUC and PR-AUC are undefined when all labels are the same.
        assert math.isnan(report.auc_roc)
        assert math.isnan(report.auc_pr)
        # Precision / recall are 1.0 because every row is flagged.
        assert report.precision_at_default == 1.0
        assert report.recall_at_default == 1.0

    def test_all_negative_predictions(self) -> None:
        y_true = np.array([0, 0, 0, 0])
        proba = np.array([0.1, 0.2, 0.3, 0.4])
        report = evaluate_predictions(y_true, proba, default_threshold=0.5)
        # AUC and PR-AUC are undefined when there are no positives.
        assert math.isnan(report.auc_roc)
        assert math.isnan(report.auc_pr)
        # Everything is predicted as on-time.
        assert report.precision_at_default == 0.0
        assert report.recall_at_default == 0.0
        # Confusion matrix is [[4, 0], [0, 0]].
        assert report.confusion_matrix_at_default == [[4, 0], [0, 0]]

    def test_threshold_tradeoffs_cover_a_range(self) -> None:
        y_true = np.array([0] * 50 + [1] * 50)
        rng = np.random.default_rng(1)
        proba = rng.uniform(0, 1, size=100)
        report = evaluate_predictions(y_true, proba, default_threshold=0.5)
        thresholds = [t.threshold for t in report.threshold_tradeoffs]
        assert min(thresholds) < 0.5 < max(thresholds)
        # Each row has a finite f1.
        for t in report.threshold_tradeoffs:
            assert math.isfinite(t.precision) or math.isnan(t.precision)
            assert math.isfinite(t.recall) or math.isnan(t.recall)
            assert math.isfinite(t.f1) or math.isnan(t.f1)

    def test_calibration_buckets_present_when_probabilities_vary(self) -> None:
        y_true = np.array([0] * 50 + [1] * 50)
        rng = np.random.default_rng(2)
        proba = rng.uniform(0, 1, size=100)
        report = evaluate_predictions(y_true, proba, default_threshold=0.5)
        # At least one bucket should be reported.
        assert report.calibration
        # Each bucket has finite avg_predicted and actual_positive_rate.
        for bucket in report.calibration:
            assert math.isfinite(bucket.avg_predicted) or math.isnan(bucket.avg_predicted)
            assert math.isfinite(bucket.actual_positive_rate) or math.isnan(
                bucket.actual_positive_rate
            )
            assert 0.0 <= bucket.lower < bucket.upper <= 1.0

    def test_calibration_empty_when_probabilities_constant(self) -> None:
        y_true = np.array([0, 0, 1, 1])
        proba = np.array([0.5, 0.5, 0.5, 0.5])
        report = evaluate_predictions(y_true, proba, default_threshold=0.5)
        # Calibration is suppressed when there is no probability
        # variation.
        assert report.calibration == []

    def test_to_dict_is_json_serialisable(self) -> None:
        y_true = np.array([0, 0, 1, 1])
        proba = np.array([0.1, 0.2, 0.7, 0.8])
        report = evaluate_predictions(y_true, proba, default_threshold=0.5)
        d = report.to_dict()
        # Round-trip through JSON.
        import json

        text = json.dumps(d)
        assert text  # non-empty
        d_round = json.loads(text)
        assert d_round["row_count"] == 4
        assert d_round["confusion_matrix_at_default"] == [[2, 0], [0, 2]]


class TestEvaluateDataframe:
    def test_dataframe_wrapper_matches_array_path(self) -> None:
        import pandas as pd

        df = pd.DataFrame(
            {
                "risk_probability": [0.1, 0.4, 0.6, 0.9],
                "is_late": [0, 0, 1, 1],
            }
        )
        report = evaluate_dataframe(df, default_threshold=0.5)
        assert report.row_count == 4
        assert report.positive_count == 2
        # Perfect ranking.
        assert report.auc_roc == 1.0

    def test_rejects_missing_columns(self) -> None:
        import pandas as pd

        df = pd.DataFrame({"risk_probability": [0.1, 0.9]})
        with pytest.raises(ValueError, match="target_column"):
            evaluate_dataframe(df, default_threshold=0.5)

        df2 = pd.DataFrame({"is_late": [0, 1]})
        with pytest.raises(ValueError, match="probability_column"):
            evaluate_dataframe(df2, default_threshold=0.5)
