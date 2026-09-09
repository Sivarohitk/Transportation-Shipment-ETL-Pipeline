"""End-to-end orchestration of the late-shipment risk model pipeline.

This module wires together the smaller building blocks in
:mod:`transport_etl.ml.features`, :mod:`transport_etl.ml.splits`,
:mod:`transport_etl.ml.training`, :mod:`transport_etl.ml.evaluation`,
and :mod:`transport_etl.ml.scoring` into a single function that can
be invoked from a CLI / notebook / orchestration layer:

- :func:`run_training_and_score` — fits a model on a chronological
  train/val/test split and produces the score frame and a
  per-split evaluation report.

The orchestration is intentionally simple: it is a single Python
function, not a framework.  The CLI in
:mod:`transport_etl.ml.cli` is a thin wrapper.
"""

from __future__ import annotations

from dataclasses import dataclass, field

import pandas as pd

from transport_etl.ml.constants import (
    DEFAULT_DECISION_THRESHOLD,
    DEFAULT_TEST_FRACTION,
    DEFAULT_TRAIN_FRACTION,
    DEFAULT_VALIDATION_FRACTION,
    MODEL_LOGISTIC_REGRESSION,
)
from transport_etl.ml.evaluation import EvaluationReport, evaluate_dataframe
from transport_etl.ml.features import build_feature_matrix
from transport_etl.ml.scoring import score_shipments, score_summary
from transport_etl.ml.splits import (
    SPLIT_TEST,
    SPLIT_TRAIN,
    SPLIT_VALIDATION,
    chronological_split,
    slice_split,
)
from transport_etl.ml.training import LateRiskModel, train_model


@dataclass
class TrainingAndScoreResult:
    """Container for the output of :func:`run_training_and_score`.

    Attributes:
        model: Fitted :class:`LateRiskModel`.
        train_report: Evaluation report on the training split.  This
            is reported for completeness; the *primary* reported
            metrics are the test report because the test split is
            chronologically the latest and most realistic.
        validation_report: Evaluation report on the validation split,
            retained for model comparison; this pipeline does not tune a
            threshold on it.
        test_report: Evaluation report on the test split.  This is
            the *primary* reported result.
        scored: Output frame produced by :func:`score_shipments`.
        scored_summary: Small dict summary of the scored frame.
    """

    model: LateRiskModel
    train_report: EvaluationReport
    validation_report: EvaluationReport
    test_report: EvaluationReport
    scored: pd.DataFrame
    scored_summary: dict[str, float | int] = field(default_factory=dict)


def run_training_and_score(
    shipments: pd.DataFrame,
    *,
    is_late: pd.Series,
    model_name: str = MODEL_LOGISTIC_REGRESSION,
    train_fraction: float = DEFAULT_TRAIN_FRACTION,
    validation_fraction: float = DEFAULT_VALIDATION_FRACTION,
    test_fraction: float = DEFAULT_TEST_FRACTION,
    decision_threshold: float = DEFAULT_DECISION_THRESHOLD,
) -> TrainingAndScoreResult:
    """Train on a chronological train split, score everything, and report.

    Args:
        shipments: Upstream shipment DataFrame (booking-time
            columns plus ``is_late`` label if you want a single-call
            end-to-end run).  The function does *not* require the
            feature matrix — it will run the feature engineering
            step itself, taking care of the chronological correctness
            of the historical aggregates.
        is_late: ``0/1`` label Series aligned to ``shipments``.
        model_name: One of :data:`ALL_MODEL_NAMES` (see
            :mod:`transport_etl.ml.constants`).
        train_fraction: Fraction of rows for the training split
            (chronologically earliest).
        validation_fraction: Fraction for validation.
        test_fraction: Fraction for test (chronologically latest).
        decision_threshold: Threshold for the
            ``predicted_late`` output.

    Returns:
        A :class:`TrainingAndScoreResult` containing the fitted
        model, the per-split evaluation reports, and the score frame.
    """
    if len(shipments) != len(is_late):
        raise ValueError(
            f"shipments length ({len(shipments)}) does not match "
            f"is_late length ({len(is_late)})"
        )

    if "actual_delivery_ts" not in shipments.columns:
        raise ValueError(
            "actual_delivery_ts is required to establish when historical outcomes became observable"
        )
    actual_delivery = pd.to_datetime(shipments["actual_delivery_ts"], errors="coerce", utc=True)
    if actual_delivery.isna().any():
        raise ValueError(
            "shipments contains unobserved outcomes; exclude or censor them before training"
        )

    # 1) Build the feature matrix.  Historical aggregates use only
    #    completed outcomes observable before each pickup timestamp.
    features = build_feature_matrix(
        shipments,
        is_late=is_late,
        outcome_available_ts=actual_delivery,
    )

    # 2) Chronological train / validation / test split.  The split
    #    column is added to the feature matrix.
    features = chronological_split(
        features,
        train_fraction=train_fraction,
        validation_fraction=validation_fraction,
        test_fraction=test_fraction,
    )

    train_df = slice_split(features, SPLIT_TRAIN)
    val_df = slice_split(features, SPLIT_VALIDATION)
    test_df = slice_split(features, SPLIT_TEST)

    # 3) Train on the training split only.
    model = train_model(train_df, model_name=model_name)

    # 4) Score every split using the pre-computed as-of features.
    scored = score_shipments(
        model,
        features,
        decision_threshold=decision_threshold,
    )

    # 5) Evaluate per split.  We compute the score probability per
    #    split and pass it through :func:`evaluate_dataframe`.
    def _report(name: str, split_df: pd.DataFrame) -> EvaluationReport:
        split_proba = model.predict_proba(split_df)
        # Build a temp frame that has the predicted probability and
        # the ground-truth label so :func:`evaluate_dataframe` can
        # pick them up.
        tmp = pd.DataFrame(
            {
                "risk_probability": split_proba,
                "is_late": split_df["is_late"].astype(int).to_numpy(),
            }
        )
        return evaluate_dataframe(
            tmp,
            split_name=name,
            default_threshold=decision_threshold,
        )

    return TrainingAndScoreResult(
        model=model,
        train_report=_report("train", train_df),
        validation_report=_report("validation", val_df),
        test_report=_report("test", test_df),
        scored=scored,
        scored_summary=score_summary(scored),
    )


__all__ = [
    "TrainingAndScoreResult",
    "run_training_and_score",
]
