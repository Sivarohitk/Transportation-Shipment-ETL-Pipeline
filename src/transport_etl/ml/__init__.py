"""Late-shipment risk model (Phase 8).

This package implements an explainable decision-support component
that predicts whether a shipment is likely to miss its promised
delivery time.  All predictions use only booking-time features so
the model is safe to use before the actual delivery is known.

Key design choices
------------------

- **Two estimators only**: scikit-learn's
  :class:`~sklearn.linear_model.LogisticRegression` and
  :class:`~sklearn.ensemble.HistGradientBoostingClassifier`.  No
  extra ML dependencies are introduced (AGENTS.md rule 13).
- **Chronological splits** are mandatory.  Random splits would let
  future shipments leak into the training set, producing
  optimistic evaluation metrics.
- **Strict no-leakage contract** documented in
  :mod:`transport_etl.ml.leakage_audit` and enforced at every
  feature matrix and score frame boundary.
- **Documented risk bands** in :mod:`transport_etl.ml.constants`
  (LOW / MEDIUM / HIGH / CRITICAL) with a rationale recorded in
  ``docs/late_risk_model.md``.

Public API
----------

- :func:`transport_etl.ml.features.build_feature_matrix` — build the
  feature matrix from upstream shipment data.
- :func:`transport_etl.ml.splits.chronological_split` — assign
  chronological train / validation / test labels.
- :func:`transport_etl.ml.training.train_model` — fit a
  :class:`LateRiskModel` on the training split.
- :func:`transport_etl.ml.evaluation.evaluate_predictions` /
  :func:`evaluate_dataframe` — produce an
  :class:`EvaluationReport` for a labelled frame.
- :func:`transport_etl.ml.scoring.score_shipments` — produce the
  decision-support output (``shipment_id``, ``pickup_ts``,
  ``risk_probability``, ``risk_band``, ``predicted_late``).
- :func:`transport_etl.ml.pipeline.run_training_and_score` — the
  one-call end-to-end orchestrator used by the CLI.

CLI
---

::

    python -m transport_etl.ml.cli train-and-score \\
        --shipments data/generated/shipments_2025-07-01.csv \\
        --output-dir data/scored \\
        --model logistic_regression

The CLI writes:

- ``scored_shipments.csv`` — the decision-support frame
- ``evaluation_reports.json`` — train / validation / test metrics
- ``model.pkl`` — the fitted model (loadable via
  :func:`LateRiskModel.load`)
"""

from __future__ import annotations

from transport_etl.ml.constants import (
    ALL_MODEL_NAMES,
    ALL_RISK_BANDS,
    ALL_SCORE_COLUMNS,
    DEFAULT_DECISION_THRESHOLD,
    MODEL_HIST_GRADIENT_BOOSTING,
    MODEL_LOGISTIC_REGRESSION,
    RISK_BAND_CRITICAL,
    RISK_BAND_HIGH,
    RISK_BAND_LOW,
    RISK_BAND_MEDIUM,
    classify_risk_band,
)
from transport_etl.ml.evaluation import (
    CalibrationBucket,
    EvaluationReport,
    ThresholdTradeoff,
    evaluate_dataframe,
    evaluate_predictions,
)
from transport_etl.ml.features import (
    build_feature_matrix,
    feature_columns,
    fill_missing_for_scoring,
)
from transport_etl.ml.leakage_audit import (
    ALLOWED_FEATURE_COLUMNS,
    FORBIDDEN_FEATURE_COLUMNS,
    LABEL_AND_META_COLUMNS,
    LeakageAuditError,
    assert_no_leakage,
)
from transport_etl.ml.pipeline import TrainingAndScoreResult, run_training_and_score
from transport_etl.ml.scoring import score_shipments, score_summary
from transport_etl.ml.splits import (
    ALL_SPLITS,
    SPLIT_TEST,
    SPLIT_TRAIN,
    SPLIT_VALIDATION,
    chronological_split,
    slice_split,
    split_indices,
)
from transport_etl.ml.training import (
    CATEGORICAL_FEATURES,
    NUMERIC_FEATURES,
    LateRiskModel,
    train_model,
)

__all__ = [
    "ALLOWED_FEATURE_COLUMNS",
    "ALL_MODEL_NAMES",
    "ALL_RISK_BANDS",
    "ALL_SCORE_COLUMNS",
    "ALL_SPLITS",
    "CATEGORICAL_FEATURES",
    "CalibrationBucket",
    "DEFAULT_DECISION_THRESHOLD",
    "EvaluationReport",
    "FORBIDDEN_FEATURE_COLUMNS",
    "LABEL_AND_META_COLUMNS",
    "LeakageAuditError",
    "LateRiskModel",
    "MODEL_HIST_GRADIENT_BOOSTING",
    "MODEL_LOGISTIC_REGRESSION",
    "NUMERIC_FEATURES",
    "RISK_BAND_CRITICAL",
    "RISK_BAND_HIGH",
    "RISK_BAND_LOW",
    "RISK_BAND_MEDIUM",
    "SPLIT_TEST",
    "SPLIT_TRAIN",
    "SPLIT_VALIDATION",
    "ThresholdTradeoff",
    "TrainingAndScoreResult",
    "assert_no_leakage",
    "build_feature_matrix",
    "chronological_split",
    "classify_risk_band",
    "evaluate_dataframe",
    "evaluate_predictions",
    "feature_columns",
    "fill_missing_for_scoring",
    "run_training_and_score",
    "score_shipments",
    "score_summary",
    "slice_split",
    "split_indices",
    "train_model",
]
