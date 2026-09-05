"""Constants for the late-shipment risk model (Phase 8).

All values that are referenced by more than one module live here so
that test code and production code share the same constants.  No
string literals for storage paths or table identifiers exist outside
this module and the YAML config files.
"""

from __future__ import annotations

# ---------------------------------------------------------------------------
# Risk bands
# ---------------------------------------------------------------------------
#
# Bands are defined on the predicted probability of a late delivery.
# The thresholds are documented in ``docs/late_risk_model.md``; the
# rationale is:
#
# - LOW        (< 0.10)  the carrier/route historical late rate is
#                          below 10% and the shipment looks routine.
# - MEDIUM     (0.10 to   the late rate is comparable to the industry
#                0.25)     average; flag for normal review.
# - HIGH       (0.25 to   one in four shipments of this kind is late
#                0.50)     historically; manual follow-up recommended.
# - CRITICAL   (>= 0.50)  majority of comparable shipments are late;
#                          proactive intervention warranted.
#
# The thresholds are intentionally not "tuned" to maximise any
# specific metric on the synthetic data.  They are documented
# business rules, not magic numbers.

RISK_BAND_LOW_MAX_EXCLUSIVE: float = 0.10
RISK_BAND_MEDIUM_MAX_EXCLUSIVE: float = 0.25
RISK_BAND_HIGH_MAX_EXCLUSIVE: float = 0.50
# Anything >= RISK_BAND_HIGH_MAX_EXCLUSIVE is CRITICAL.

RISK_BAND_LOW = "LOW"
RISK_BAND_MEDIUM = "MEDIUM"
RISK_BAND_HIGH = "HIGH"
RISK_BAND_CRITICAL = "CRITICAL"

ALL_RISK_BANDS: tuple[str, ...] = (
    RISK_BAND_LOW,
    RISK_BAND_MEDIUM,
    RISK_BAND_HIGH,
    RISK_BAND_CRITICAL,
)


def classify_risk_band(probability: float) -> str:
    """Map a late-delivery probability to a discrete risk band.

    Args:
        probability: Predicted ``P(is_late)`` from the model.  Must
            lie in ``[0, 1]``; values outside that range are clipped.

    Returns:
        One of ``LOW``, ``MEDIUM``, ``HIGH``, ``CRITICAL``.
    """
    clipped = max(0.0, min(1.0, float(probability)))
    if clipped < RISK_BAND_LOW_MAX_EXCLUSIVE:
        return RISK_BAND_LOW
    if clipped < RISK_BAND_MEDIUM_MAX_EXCLUSIVE:
        return RISK_BAND_MEDIUM
    if clipped < RISK_BAND_HIGH_MAX_EXCLUSIVE:
        return RISK_BAND_HIGH
    return RISK_BAND_CRITICAL


# ---------------------------------------------------------------------------
# Default decision threshold
# ---------------------------------------------------------------------------
#
# The default operating threshold for converting a probability into
# the ``predicted_late`` boolean.  Set to the boundary between MEDIUM
# and HIGH bands (0.25) because:
#
# - it matches the documented risk-band scheme,
# - it errs slightly toward flagging MEDIUM as predicted_late=False
#   (the false-negative cost is mitigated by the human review of
#   MEDIUM-band shipments in operations),
# - and it is documented, not a hidden constant.
#
# Users who want a different operating point can pass an explicit
# ``decision_threshold`` to :func:`transport_etl.ml.scoring.score_dataset`.
DEFAULT_DECISION_THRESHOLD: float = RISK_BAND_MEDIUM_MAX_EXCLUSIVE


# ---------------------------------------------------------------------------
# Chronological split defaults
# ---------------------------------------------------------------------------

#: Default fraction of (chronologically earliest) shipments assigned
#: to the training set.
DEFAULT_TRAIN_FRACTION: float = 0.70
#: Default fraction of the middle window assigned to validation.
DEFAULT_VALIDATION_FRACTION: float = 0.15
#: Default fraction of the latest window assigned to test.
DEFAULT_TEST_FRACTION: float = 0.15

#: Minimum shipment count per split.  Splits below this are
#: rejected by the splitter to avoid degenerate evaluations.
MIN_SPLIT_SIZE: int = 30

# ---------------------------------------------------------------------------
# Model identifiers
# ---------------------------------------------------------------------------

MODEL_LOGISTIC_REGRESSION: str = "logistic_regression"
MODEL_HIST_GRADIENT_BOOSTING: str = "hist_gradient_boosting"

ALL_MODEL_NAMES: tuple[str, ...] = (
    MODEL_LOGISTIC_REGRESSION,
    MODEL_HIST_GRADIENT_BOOSTING,
)

#: The default decision threshold in the ``scoring`` output is
#: identical to ``DEFAULT_DECISION_THRESHOLD``; declared separately
#: so the scoring module does not need to import the training
#: defaults.
SCORE_COLUMN_SHIPMENT_ID: str = "shipment_id"
SCORE_COLUMN_PICKUP_TS: str = "pickup_ts"
SCORE_COLUMN_PROBABILITY: str = "risk_probability"
SCORE_COLUMN_BAND: str = "risk_band"
SCORE_COLUMN_PREDICTED: str = "predicted_late"
SCORE_COLUMN_ACTUAL: str = "actual_late"

ALL_SCORE_COLUMNS: tuple[str, ...] = (
    SCORE_COLUMN_SHIPMENT_ID,
    SCORE_COLUMN_PICKUP_TS,
    SCORE_COLUMN_PROBABILITY,
    SCORE_COLUMN_BAND,
    SCORE_COLUMN_PREDICTED,
    SCORE_COLUMN_ACTUAL,
)

__all__ = [
    "ALL_MODEL_NAMES",
    "ALL_RISK_BANDS",
    "ALL_SCORE_COLUMNS",
    "DEFAULT_DECISION_THRESHOLD",
    "DEFAULT_TEST_FRACTION",
    "DEFAULT_TRAIN_FRACTION",
    "DEFAULT_VALIDATION_FRACTION",
    "MIN_SPLIT_SIZE",
    "MODEL_HIST_GRADIENT_BOOSTING",
    "MODEL_LOGISTIC_REGRESSION",
    "RISK_BAND_CRITICAL",
    "RISK_BAND_HIGH",
    "RISK_BAND_LOW",
    "RISK_BAND_HIGH_MAX_EXCLUSIVE",
    "RISK_BAND_LOW_MAX_EXCLUSIVE",
    "RISK_BAND_MEDIUM_MAX_EXCLUSIVE",
    "RISK_BAND_MEDIUM",
    "SCORE_COLUMN_ACTUAL",
    "SCORE_COLUMN_BAND",
    "SCORE_COLUMN_PICKUP_TS",
    "SCORE_COLUMN_PREDICTED",
    "SCORE_COLUMN_PROBABILITY",
    "SCORE_COLUMN_SHIPMENT_ID",
    "classify_risk_band",
]
