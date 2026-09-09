"""Training of the late-shipment risk model.

This module provides a single :func:`train_model` entry point that
fits one of the two supported estimators from scikit-learn:

- :data:`MODEL_LOGISTIC_REGRESSION` — the linear baseline
- :data:`MODEL_HIST_GRADIENT_BOOSTING` — the tree-based model

No new ML dependencies are introduced.  We use the following
scikit-learn primitives:

- :class:`sklearn.linear_model.LogisticRegression`
- :class:`sklearn.ensemble.HistGradientBoostingClassifier`
- :class:`sklearn.pipeline.Pipeline`
- :class:`sklearn.compose.ColumnTransformer`
- :class:`sklearn.preprocessing.OneHotEncoder`

The two estimators are wrapped in a small :class:`LateRiskModel`
container that knows how to:

- fit on a chronological-train DataFrame
- predict probabilities on a validation / test DataFrame
- serialise / deserialise the fitted estimator to a portable
  ``joblib`` blob (so scoring can run without retraining)

Why two models
--------------

The task requires an explainable, decision-support model.  We
provide:

- A **logistic regression** baseline that is fast, deterministic,
  and easy to interpret (per-feature coefficients).  It is the
  reference model — any non-trivial model must beat it.
- A **gradient boosting** model that captures non-linear
  interactions.  This is the "production" candidate.

We do not add LightGBM, XGBoost, CatBoost, or any other dependency
(AGENTS.md rule 13).  scikit-learn is already a transitive
dependency via PySpark for many environments, and it is the
minimum sufficient library for the chosen model classes.
"""

from __future__ import annotations

import logging
import os
import pickle
from dataclasses import dataclass, field
from typing import Any, Sequence

import numpy as np
import pandas as pd
from sklearn.base import ClassifierMixin
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder

from transport_etl.ml.constants import (
    ALL_MODEL_NAMES,
    MODEL_HIST_GRADIENT_BOOSTING,
    MODEL_LOGISTIC_REGRESSION,
)
from transport_etl.ml.features import (
    feature_columns,
    fill_missing_for_scoring,
    scoring_fill_values,
)

LOGGER = logging.getLogger("transport_etl.ml.training")


#: Columns that should be one-hot encoded rather than fed as
#: numerics.  ``service_mode`` and ``is_active`` are categorical in
#: the upstream schema; the state / region fields are also
#: categorical even though they are 2-letter strings.
CATEGORICAL_FEATURES: tuple[str, ...] = (
    "carrier_id",
    "service_mode",
    "origin_state",
    "destination_state",
    "region_code",
    "origin_region_code",
    "home_region_code",
    "is_active",
)

#: Columns that should be passed as numerics.  The model is given
#: the raw numeric value; missing values are imputed at scoring
#: time by :func:`fill_missing_for_scoring`.
NUMERIC_FEATURES: tuple[str, ...] = (
    "distance_miles",
    "shipping_cost_usd",
    "promised_transit_hours",
    "pickup_dow",
    "pickup_hour",
    "pickup_month",
    "carrier_historical_late_rate",
    "carrier_historical_shipment_count",
    "route_historical_late_rate",
    "route_historical_shipment_count",
)


@dataclass
class LateRiskModel:
    """A trained late-shipment risk model.

    Attributes:
        model_name: One of :data:`ALL_MODEL_NAMES`.
        pipeline: Fitted scikit-learn ``Pipeline`` (preprocessor +
            estimator).
        categorical_features: Categorical feature names actually
            used during training (after intersection with the
            available columns).
        numeric_features: Numeric feature names actually used
            during training.
        training_rows: Number of training rows seen by ``fit``.
        positive_rate: Fraction of positive labels in the training
            set.  Recorded for diagnostics.
    """

    model_name: str
    pipeline: Pipeline
    categorical_features: list[str] = field(default_factory=list)
    numeric_features: list[str] = field(default_factory=list)
    training_rows: int = 0
    positive_rate: float = 0.0
    imputation_values: dict[str, float] = field(default_factory=dict)

    def predict_proba(self, X: pd.DataFrame) -> np.ndarray:
        """Return the predicted ``P(is_late)`` for each row of ``X``.

        The output is a 1-D array of probabilities; only the positive
        class probability is returned.  Nulls in the chronological
        historical features are filled with the scoring-time
        defaults (see :func:`fill_missing_for_scoring`) so the
        underlying scikit-learn pipeline never sees NaN.
        """
        if not isinstance(self.pipeline, ClassifierMixin) and not hasattr(
            self.pipeline, "predict_proba"
        ):
            raise RuntimeError(
                f"Underlying pipeline of model {self.model_name!r} does "
                f"not support predict_proba"
            )
        # Apply only defaults learned from the chronological training split.
        filled = fill_missing_for_scoring(X, fill_values=self.imputation_values)
        proba = self.pipeline.predict_proba(filled)
        # ``predict_proba`` returns a 2-D array of shape (n, 2); the
        # positive class is at index 1.
        return np.asarray(proba)[:, 1]

    def predict(self, X: pd.DataFrame, *, threshold: float = 0.5) -> np.ndarray:
        """Return the boolean ``predicted_late`` for each row of ``X``."""
        proba = self.predict_proba(X)
        return (proba >= threshold).astype(int)

    def save(self, path: str) -> None:
        """Persist the model to ``path`` using :mod:`pickle`."""
        directory = os.path.dirname(path)
        if directory:
            os.makedirs(directory, exist_ok=True)
        with open(path, "wb") as handle:
            pickle.dump(self, handle)

    @staticmethod
    def load(path: str) -> "LateRiskModel":
        """Load a previously persisted model from ``path``."""
        with open(path, "rb") as handle:
            model = pickle.load(handle)
        if not isinstance(model, LateRiskModel):
            raise TypeError(f"File {path!r} does not contain a LateRiskModel instance")
        return model


def _select_feature_columns(
    df: pd.DataFrame,
    categorical: Sequence[str],
    numeric: Sequence[str],
) -> tuple[list[str], list[str]]:
    """Intersect requested feature lists with the actual frame columns.

    Returns:
        A tuple ``(cat_cols, num_cols)`` of features that are
        actually present.
    """
    cat = [c for c in categorical if c in df.columns]
    num = [c for c in numeric if c in df.columns]
    return cat, num


def _build_preprocessor(cat_cols: list[str], num_cols: list[str]) -> ColumnTransformer:
    """Construct a ColumnTransformer for the two estimators.

    - Categorical columns: ``OneHotEncoder(handle_unknown="ignore")``
    - Numeric columns: passthrough (the GBM handles nulls natively;
      LR is fed a numeric matrix that may contain nulls which the
      pipeline drops through ``pandas`` ``dropna`` before fitting).
    """
    transformers: list[tuple[str, Any, list[str]]] = []
    if cat_cols:
        transformers.append(
            (
                "cat",
                OneHotEncoder(handle_unknown="ignore", sparse_output=False),
                cat_cols,
            )
        )
    if num_cols:
        transformers.append(("num", "passthrough", num_cols))
    return ColumnTransformer(
        transformers=transformers,
        remainder="drop",
        verbose_feature_names_out=False,
    )


def _build_estimator(model_name: str) -> Any:
    """Construct the underlying scikit-learn estimator.

    Hyperparameters are chosen to give stable training on
    small-to-medium sample sizes and to avoid overfitting on the
    5,000-shipment synthetic dataset.  They are documented here
    rather than as hidden constants so any reviewer can trace the
    model behaviour.
    """
    if model_name == MODEL_LOGISTIC_REGRESSION:
        return LogisticRegression(
            max_iter=2_000,
            solver="liblinear",  # Robust on small / sparse data.
            C=1.0,
            class_weight="balanced",  # Compensate for ~18% positive rate.
        )
    if model_name == MODEL_HIST_GRADIENT_BOOSTING:
        return HistGradientBoostingClassifier(
            max_iter=200,
            learning_rate=0.05,
            max_depth=5,
            min_samples_leaf=20,
            l2_regularization=1.0,
            random_state=20260101,
        )
    raise ValueError(f"Unknown model_name {model_name!r}; expected one of {ALL_MODEL_NAMES}")


def train_model(
    train_df: pd.DataFrame,
    *,
    target_column: str = "is_late",
    model_name: str = MODEL_LOGISTIC_REGRESSION,
    feature_override: Sequence[str] | None = None,
) -> LateRiskModel:
    """Fit a :class:`LateRiskModel` on ``train_df``.

    Args:
        train_df: Training-set DataFrame.  Must include the target
            column and the engineered feature columns (typically the
            output of :func:`transport_etl.ml.splits.chronological_split`
            filtered to ``split == "train"``).
        target_column: Name of the target column.  Defaults to
            ``"is_late"``.
        model_name: One of :data:`ALL_MODEL_NAMES`.  Defaults to
            :data:`MODEL_LOGISTIC_REGRESSION`.
        feature_override: Optional explicit list of feature
            columns.  When omitted the function uses
            :func:`transport_etl.ml.features.feature_columns`.

    Returns:
        A fitted :class:`LateRiskModel`.
    """
    if model_name not in ALL_MODEL_NAMES:
        raise ValueError(f"Unknown model_name {model_name!r}; expected one of {ALL_MODEL_NAMES}")
    if target_column not in train_df.columns:
        raise ValueError(f"target_column {target_column!r} not found in train_df")

    if feature_override is None:
        feat_cols = feature_columns(train_df)
    else:
        feat_cols = list(feature_override)
    if target_column in feat_cols:
        feat_cols = [c for c in feat_cols if c != target_column]

    cat_cols, num_cols = _select_feature_columns(train_df, CATEGORICAL_FEATURES, NUMERIC_FEATURES)
    feat_cols = [c for c in feat_cols if c in cat_cols or c in num_cols]
    if not feat_cols:
        raise ValueError(
            "No usable feature columns found.  Check that the input "
            "frame contains the engineered feature columns from "
            "build_feature_matrix()."
        )

    X_train = train_df[feat_cols].copy()
    # scikit-learn estimators (LR, OneHotEncoder) cannot accept
    # pandas ``Int64`` / ``string`` with nulls.  Coerce to plain
    # ``float64`` / ``object`` so the pipeline is happy.  The
    # HistGradientBoostingClassifier can handle nulls natively but
    # the LR preprocessor cannot.
    for column in feat_cols:
        if pd.api.types.is_integer_dtype(X_train[column]):
            X_train[column] = X_train[column].astype("float64")
    # NaN imputation for the chronological historical features.
    # These columns can be NaN for shipments that have no prior
    # history (e.g. the first shipment for a new carrier).  We
    # fill with the column mean (or 0.0 as a conservative default)
    # so both estimators see a complete training matrix.  The same
    # imputation is applied at predict time in
    # :meth:`LateRiskModel.predict_proba` to keep the contract
    # symmetric.
    imputation_values = scoring_fill_values(X_train)
    X_train = fill_missing_for_scoring(X_train, fill_values=imputation_values)
    y_train = train_df[target_column].astype(int).to_numpy()

    preprocessor = _build_preprocessor(cat_cols, num_cols)
    estimator = _build_estimator(model_name)
    pipeline = Pipeline(
        steps=[
            ("preprocess", preprocessor),
            ("estimator", estimator),
        ]
    )

    LOGGER.info(
        "Training model=%s rows=%d features=%d (cat=%d num=%d)",
        model_name,
        len(train_df),
        len(feat_cols),
        len(cat_cols),
        len(num_cols),
    )
    pipeline.fit(X_train, y_train)

    positive_rate = float(np.mean(y_train))
    return LateRiskModel(
        model_name=model_name,
        pipeline=pipeline,
        categorical_features=cat_cols,
        numeric_features=num_cols,
        training_rows=len(train_df),
        positive_rate=positive_rate,
        imputation_values=imputation_values,
    )


__all__ = [
    "CATEGORICAL_FEATURES",
    "LateRiskModel",
    "NUMERIC_FEATURES",
    "train_model",
]
