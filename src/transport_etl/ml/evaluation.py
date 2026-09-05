"""Evaluation of the late-shipment risk model.

This module computes the standard binary-classification metrics
on a labelled evaluation DataFrame:

- class distribution
- precision, recall, F1
- ROC-AUC, PR-AUC
- confusion matrix
- calibration bucket counts (basic, for diagnostics)
- threshold tradeoffs

All metrics are computed against the actual ``is_late`` column;
no synthetic numbers are produced.  When the underlying data is
sparse or the model fails entirely, the metrics will be reported
honestly (e.g. ``NaN`` for PR-AUC when there are no positive
predictions in the window).
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from typing import Any

import numpy as np
import pandas as pd
from sklearn.metrics import (
    average_precision_score,
    confusion_matrix,
    f1_score,
    precision_recall_curve,
    precision_score,
    recall_score,
    roc_auc_score,
)

# ---------------------------------------------------------------------------
# Public dataclasses
# ---------------------------------------------------------------------------


@dataclass
class ThresholdTradeoff:
    """A single (threshold, precision, recall, f1) point on the PR curve."""

    threshold: float
    precision: float
    recall: float
    f1: float


@dataclass
class CalibrationBucket:
    """A single calibration bucket: predicted probability band vs actual rate."""

    lower: float
    upper: float
    count: int
    avg_predicted: float
    actual_positive_rate: float


@dataclass
class EvaluationReport:
    """The full set of metrics for a single evaluation split.

    Attributes:
        split_name: ``"validation"`` or ``"test"`` (or any user label).
        row_count: Number of rows in the evaluation set.
        positive_count: Number of ``is_late == 1`` rows.
        negative_count: Number of ``is_late == 0`` rows.
        positive_rate: Fraction of ``is_late == 1`` rows.
        auc_roc: ROC-AUC (or ``float("nan")`` if undefined).
        auc_pr: Precision-Recall AUC (or ``NaN``).
        precision_at_default: Precision at the default
            ``DEFAULT_DECISION_THRESHOLD`` (``0.25``).
        recall_at_default: Recall at the default threshold.
        f1_at_default: F1 at the default threshold.
        precision_at_opt_f1: Precision at the threshold that
            maximises F1 on the evaluation set.
        recall_at_opt_f1: Recall at the threshold that maximises
            F1.
        f1_opt: The maximum F1 attainable on the evaluation set.
        threshold_opt_f1: The threshold that attains ``f1_opt``.
        confusion_matrix_at_default: ``[[tn, fp], [fn, tp]]`` at the
            default threshold.
        threshold_tradeoffs: A list of evenly-spaced PR points for
            threshold trade-off discussion.
        calibration: Per-bucket calibration (predicted band vs
            actual positive rate).  An empty list when the
            probability column has no variation.
    """

    split_name: str
    row_count: int
    positive_count: int
    negative_count: int
    positive_rate: float
    auc_roc: float
    auc_pr: float
    precision_at_default: float
    recall_at_default: float
    f1_at_default: float
    precision_at_opt_f1: float
    recall_at_opt_f1: float
    f1_opt: float
    threshold_opt_f1: float
    confusion_matrix_at_default: list[list[int]]
    threshold_tradeoffs: list[ThresholdTradeoff] = field(default_factory=list)
    calibration: list[CalibrationBucket] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-serialisable dict (e.g. for logging)."""
        d = asdict(self)
        d["threshold_tradeoffs"] = [asdict(t) for t in self.threshold_tradeoffs]
        d["calibration"] = [asdict(c) for c in self.calibration]
        return d

    def to_json(self) -> str:
        """Return a pretty-printed JSON string."""
        return json.dumps(self.to_dict(), indent=2)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _safe_metric(value: float | None) -> float:
    """Coerce ``None`` / NaN to ``float("nan")`` and round to 6dp."""
    if value is None:
        return float("nan")
    try:
        f = float(value)
    except (TypeError, ValueError):
        return float("nan")
    if np.isnan(f) or np.isinf(f):
        return f
    return round(f, 6)


def _format_confusion(y_true: np.ndarray, y_pred: np.ndarray) -> list[list[int]]:
    """Return ``[[tn, fp], [fn, tp]]`` as plain Python ints.

    When the prediction set is degenerate (e.g. all zeros), the
    confusion matrix still returns a 2x2 array with the
    appropriate values.
    """
    cm = confusion_matrix(y_true, y_pred, labels=[0, 1])
    return [[int(cm[0, 0]), int(cm[0, 1])], [int(cm[1, 0]), int(cm[1, 1])]]


def _pr_curve_thresholds(
    y_true: np.ndarray, proba: np.ndarray
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Return ``(precisions, recalls, thresholds)`` from sklearn.

    Sklearn's :func:`precision_recall_curve` returns arrays of
    length ``n_thresholds + 1``.  The first point uses a threshold
    of ``1.0 + eps`` and is not actionable; we drop it.
    """
    precisions, recalls, thresholds = precision_recall_curve(y_true, proba)
    return precisions[1:], recalls[1:], thresholds


def _threshold_tradeoffs(y_true: np.ndarray, proba: np.ndarray) -> list[ThresholdTradeoff]:
    """Compute a small list of (threshold, precision, recall, f1) points."""
    precisions, recalls, thresholds = _pr_curve_thresholds(y_true, proba)
    if len(thresholds) == 0:
        return []
    # Pick a fixed schedule of candidate thresholds.
    candidates = np.unique(
        np.concatenate(
            [
                thresholds,
                np.array([0.05, 0.10, 0.15, 0.20, 0.25, 0.30, 0.40, 0.50]),
            ]
        )
    )
    candidates = candidates[candidates > 0.0]
    candidates = candidates[candidates < 1.0]
    rows: list[ThresholdTradeoff] = []
    for threshold in candidates:
        y_pred = (proba >= float(threshold)).astype(int)
        precision = precision_score(y_true, y_pred, zero_division=0)
        recall = recall_score(y_true, y_pred, zero_division=0)
        f1 = f1_score(y_true, y_pred, zero_division=0)
        rows.append(
            ThresholdTradeoff(
                threshold=float(round(threshold, 4)),
                precision=_safe_metric(precision),
                recall=_safe_metric(recall),
                f1=_safe_metric(f1),
            )
        )
    return rows


def _best_f1_threshold(y_true: np.ndarray, proba: np.ndarray) -> tuple[float, float, float, float]:
    """Return ``(threshold, precision, recall, f1)`` at the F1 optimum."""
    precisions, recalls, thresholds = _pr_curve_thresholds(y_true, proba)
    if len(thresholds) == 0:
        return float("nan"), float("nan"), float("nan"), float("nan")
    f1s = np.where(
        (precisions + recalls) > 0,
        2 * precisions * recalls / np.clip(precisions + recalls, 1e-12, None),
        0.0,
    )
    best = int(np.argmax(f1s))
    return (
        float(thresholds[best]),
        _safe_metric(precisions[best]),
        _safe_metric(recalls[best]),
        _safe_metric(f1s[best]),
    )


def _calibration_buckets(
    y_true: np.ndarray, proba: np.ndarray, num_buckets: int = 10
) -> list[CalibrationBucket]:
    """Compute basic calibration buckets.

    Each bucket is a fixed-width bin of the predicted probability
    range; we report the bucket's count, average predicted
    probability, and the actual positive rate.  Empty buckets are
    omitted from the output.
    """
    if proba.size == 0 or np.allclose(proba, proba[0]):
        # Degenerate: skip calibration.
        return []
    edges = np.linspace(0.0, 1.0, num_buckets + 1)
    out: list[CalibrationBucket] = []
    for lower, upper in zip(edges[:-1], edges[1:]):
        if lower == upper:
            continue
        # ``(lower, upper]`` semantics.  The first bucket also
        # includes 0.0 by using ``<=`` on the lower edge.
        if lower <= 0.0:
            mask = (proba >= lower) & (proba <= upper)
        else:
            mask = (proba > lower) & (proba <= upper)
        count = int(mask.sum())
        if count == 0:
            continue
        out.append(
            CalibrationBucket(
                lower=float(round(lower, 4)),
                upper=float(round(upper, 4)),
                count=count,
                avg_predicted=_safe_metric(float(proba[mask].mean())),
                actual_positive_rate=_safe_metric(float(y_true[mask].mean())),
            )
        )
    return out


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def evaluate_predictions(
    y_true: np.ndarray,
    proba: np.ndarray,
    *,
    split_name: str = "validation",
    default_threshold: float = 0.25,
) -> EvaluationReport:
    """Compute the full set of evaluation metrics.

    Args:
        y_true: Ground-truth ``0/1`` labels.
        proba: Predicted ``P(is_late)`` values.
        split_name: Free-text label for the report.
        default_threshold: Decision threshold for
            ``precision_at_default`` / ``recall_at_default`` /
            ``f1_at_default`` and the default confusion matrix.

    Returns:
        An :class:`EvaluationReport` with all metrics.
    """
    y_true = np.asarray(y_true).astype(int)
    proba = np.asarray(proba, dtype=float)
    if y_true.size == 0:
        raise ValueError("y_true is empty; nothing to evaluate")

    positive_count = int(y_true.sum())
    negative_count = int(y_true.size - positive_count)
    positive_rate = positive_count / y_true.size

    # Area-under-the-curve metrics.  Both are NaN when the
    # prediction set is degenerate (e.g. all-positive or
    # all-negative labels).
    if positive_count == 0 or positive_count == y_true.size:
        auc_roc = float("nan")
        auc_pr = float("nan")
    else:
        try:
            auc_roc = _safe_metric(roc_auc_score(y_true, proba))
        except ValueError:
            auc_roc = float("nan")
        try:
            auc_pr = _safe_metric(average_precision_score(y_true, proba))
        except ValueError:
            auc_pr = float("nan")

    # Default-threshold metrics.
    y_pred_default = (proba >= default_threshold).astype(int)
    precision_default = precision_score(y_true, y_pred_default, zero_division=0)
    recall_default = recall_score(y_true, y_pred_default, zero_division=0)
    f1_default = f1_score(y_true, y_pred_default, zero_division=0)

    # Best-F1 operating point on this evaluation set.
    (
        threshold_opt,
        precision_opt,
        recall_opt,
        f1_opt,
    ) = _best_f1_threshold(y_true, proba)

    return EvaluationReport(
        split_name=split_name,
        row_count=int(y_true.size),
        positive_count=positive_count,
        negative_count=negative_count,
        positive_rate=_safe_metric(positive_rate),
        auc_roc=auc_roc,
        auc_pr=auc_pr,
        precision_at_default=_safe_metric(precision_default),
        recall_at_default=_safe_metric(recall_default),
        f1_at_default=_safe_metric(f1_default),
        precision_at_opt_f1=precision_opt,
        recall_at_opt_f1=recall_opt,
        f1_opt=f1_opt,
        threshold_opt_f1=threshold_opt,
        confusion_matrix_at_default=_format_confusion(y_true, y_pred_default),
        threshold_tradeoffs=_threshold_tradeoffs(y_true, proba),
        calibration=_calibration_buckets(y_true, proba),
    )


def evaluate_dataframe(
    df: pd.DataFrame,
    *,
    probability_column: str = "risk_probability",
    target_column: str = "is_late",
    split_name: str = "validation",
    default_threshold: float = 0.25,
) -> EvaluationReport:
    """Convenience wrapper around :func:`evaluate_predictions`.

    Args:
        df: DataFrame containing both ``probability_column`` and
            ``target_column``.
        probability_column: Column with predicted probabilities.
        target_column: Column with the ground-truth ``0/1`` label.
        split_name: Free-text label for the report.
        default_threshold: Decision threshold for the report.

    Returns:
        An :class:`EvaluationReport`.
    """
    if probability_column not in df.columns:
        raise ValueError(f"probability_column {probability_column!r} not found in df")
    if target_column not in df.columns:
        raise ValueError(f"target_column {target_column!r} not found in df")
    return evaluate_predictions(
        y_true=df[target_column].to_numpy(),
        proba=df[probability_column].to_numpy(),
        split_name=split_name,
        default_threshold=default_threshold,
    )


__all__ = [
    "CalibrationBucket",
    "EvaluationReport",
    "ThresholdTradeoff",
    "evaluate_dataframe",
    "evaluate_predictions",
]
