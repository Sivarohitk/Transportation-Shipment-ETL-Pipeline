"""Scoring of new shipments using a trained late-risk model.

This module is the production-facing entry point: it accepts a
fitted :class:`transport_etl.ml.training.LateRiskModel` and a
shipment DataFrame, and returns a decision-support frame with the
following columns:

- ``shipment_id``
- ``pickup_ts``
- ``risk_probability`` (predicted ``P(is_late)``)
- ``risk_band`` (one of ``LOW`` / ``MEDIUM`` / ``HIGH`` / ``CRITICAL``)
- ``predicted_late`` (``0/1`` based on the decision threshold)
- ``actual_late`` (optional; present when the caller supplies it)

The risk bands are documented in
:mod:`transport_etl.ml.constants` and the threshold rationale is
reproduced in ``docs/late_risk_model.md``.
"""

from __future__ import annotations

import pandas as pd

from transport_etl.ml.constants import (
    DEFAULT_DECISION_THRESHOLD,
    SCORE_COLUMN_ACTUAL,
    SCORE_COLUMN_BAND,
    SCORE_COLUMN_PICKUP_TS,
    SCORE_COLUMN_PREDICTED,
    SCORE_COLUMN_PROBABILITY,
    SCORE_COLUMN_SHIPMENT_ID,
    classify_risk_band,
)
from transport_etl.ml.features import fill_missing_for_scoring
from transport_etl.ml.leakage_audit import assert_no_leakage
from transport_etl.ml.training import LateRiskModel


def score_shipments(
    model: LateRiskModel,
    shipments: pd.DataFrame,
    *,
    decision_threshold: float = DEFAULT_DECISION_THRESHOLD,
    actual_late: pd.Series | None = None,
) -> pd.DataFrame:
    """Score ``shipments`` with ``model`` and produce the output frame.

    Args:
        model: Fitted :class:`LateRiskModel`.
        shipments: DataFrame of shipment records.  Must contain
            the engineered feature columns used during training.
            This is typically the output of
            :func:`transport_etl.ml.features.build_feature_matrix`
            (or, for end-to-end convenience, the
            :func:`transport_etl.ml.pipeline.run_training_and_score`
            helper that wires feature engineering, training, and
            scoring together).
        decision_threshold: Probability above which a shipment is
            flagged as ``predicted_late = 1``.  Defaults to the
            documented :data:`DEFAULT_DECISION_THRESHOLD` (0.25).
        actual_late: Optional ``0/1`` Series aligned to the
            ``shipments`` index.  When supplied the output frame
            includes the actual label so it can be merged with the
            validation report.

    Returns:
        A new DataFrame with the columns documented in
        :data:`ALL_SCORE_COLUMNS`.  Rows are in the same order as the
        input.
    """
    if shipments.empty:
        raise ValueError("shipments DataFrame is empty; nothing to score")

    # The feature engineering step is required because the model
    # pipeline expects all engineered columns.  We rebuild them
    # from the booking-time information alone — the historical
    # features cannot be rebuilt without the historical label, so
    # callers that want historical features at scoring time must
    # pass a frame that already has them (e.g. from a feature
    # store).
    feature_frame = fill_missing_for_scoring(shipments)

    # Predict the probability of being late.
    proba = model.predict_proba(feature_frame)
    bands = [_classify_band(p) for p in proba]
    predicted = (proba >= float(decision_threshold)).astype(int)

    out = pd.DataFrame(
        {
            SCORE_COLUMN_SHIPMENT_ID: feature_frame["shipment_id"].to_numpy(),
            SCORE_COLUMN_PICKUP_TS: pd.to_datetime(
                feature_frame["pickup_ts"], errors="coerce", utc=True
            ),
            SCORE_COLUMN_PROBABILITY: proba,
            SCORE_COLUMN_BAND: bands,
            SCORE_COLUMN_PREDICTED: predicted,
        }
    )
    if actual_late is not None:
        if len(actual_late) != len(feature_frame):
            raise ValueError(
                f"actual_late length ({len(actual_late)}) does not match "
                f"shipments length ({len(feature_frame)})"
            )
        out[SCORE_COLUMN_ACTUAL] = actual_late.astype(int).to_numpy()
    elif "is_late" in feature_frame.columns:
        # Convenience: when the upstream pipeline passed ``is_late``
        # through, propagate it to the output so the report can
        # pick it up without re-joining.
        out[SCORE_COLUMN_ACTUAL] = feature_frame["is_late"].astype(int).to_numpy()

    # Final paranoid leakage check on the output frame.
    assert_no_leakage(out.columns)
    return out


def _classify_band(probability: float) -> str:
    """Wrap :func:`classify_risk_band` so the test suite can patch it."""
    return classify_risk_band(probability)


def score_summary(scored: pd.DataFrame) -> dict[str, float | int]:
    """Return a small summary dict for the scored frame.

    The summary is useful for logging and for the model card
    documented in ``docs/late_risk_model.md``.
    """
    if scored.empty:
        return {"row_count": 0}
    summary: dict[str, float | int] = {
        "row_count": int(len(scored)),
        "mean_risk_probability": float(scored[SCORE_COLUMN_PROBABILITY].mean()),
        "predicted_late_count": int(scored[SCORE_COLUMN_PREDICTED].sum()),
    }
    for band in ("LOW", "MEDIUM", "HIGH", "CRITICAL"):
        summary[f"band_{band.lower()}_count"] = int((scored[SCORE_COLUMN_BAND] == band).sum())
    if SCORE_COLUMN_ACTUAL in scored.columns:
        summary["actual_late_count"] = int(scored[SCORE_COLUMN_ACTUAL].sum())
    return summary


__all__ = [
    "score_shipments",
    "score_summary",
]
