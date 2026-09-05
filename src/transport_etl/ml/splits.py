"""Chronological train / validation / test split.

Why chronological
-----------------

AGENTS.md rule 11 requires that the model evaluation be honest.
Random train/test splits in supply-chain data would let future
shipments leak into the training set, producing overly optimistic
metrics.  This module enforces a strict chronological split by
sorting the dataset on ``pickup_ts`` and assigning the earliest
fraction to train, the next to validation, and the latest to test.

Contract
--------

- The splitter never shuffles the input.  The order of
  ``pickup_ts`` values in the input is irrelevant; the splitter
  produces a stable ordering by sorting on the timestamp.
- The split fractions sum to 1.0 (no rows are dropped).
- Each split is non-overlapping and the union covers the input.
- Splits below :data:`constants.MIN_SPLIT_SIZE` rows are rejected
  because a tiny split produces meaningless metrics.
- A single ``split`` column is added to the labelled DataFrame
  with values in ``{"train", "validation", "test"}`` so that
  downstream code can group on it.
"""

from __future__ import annotations

import math

import pandas as pd

from transport_etl.ml.constants import (
    DEFAULT_TEST_FRACTION,
    DEFAULT_TRAIN_FRACTION,
    DEFAULT_VALIDATION_FRACTION,
    MIN_SPLIT_SIZE,
)

#: Sentinel values for the ``split`` column added to the labelled
#: dataset.
SPLIT_TRAIN: str = "train"
SPLIT_VALIDATION: str = "validation"
SPLIT_TEST: str = "test"
ALL_SPLITS: tuple[str, ...] = (SPLIT_TRAIN, SPLIT_VALIDATION, SPLIT_TEST)


def _normalise_fractions(
    train_fraction: float,
    validation_fraction: float,
    test_fraction: float,
) -> tuple[float, float, float]:
    """Normalise three split fractions to sum to 1.0.

    The validation is "tolerant" — when callers pass values that do
    not sum to exactly 1.0 (e.g. due to floating point), we
    rescale.  When the values are nonsensical (e.g. any negative
    fraction) we raise.
    """
    if train_fraction < 0 or validation_fraction < 0 or test_fraction < 0:
        raise ValueError("Split fractions must be non-negative")
    total = train_fraction + validation_fraction + test_fraction
    if total <= 0:
        raise ValueError("At least one split fraction must be > 0")
    if math.isclose(total, 1.0, abs_tol=1e-9):
        return train_fraction, validation_fraction, test_fraction
    return (
        train_fraction / total,
        validation_fraction / total,
        test_fraction / total,
    )


def _validate_columns(df: pd.DataFrame, pickup_ts_column: str) -> None:
    if pickup_ts_column not in df.columns:
        raise ValueError(
            f"Column {pickup_ts_column!r} not found in DataFrame. "
            f"Available columns: {list(df.columns)}"
        )


def chronological_split(
    df: pd.DataFrame,
    *,
    pickup_ts_column: str = "pickup_ts",
    train_fraction: float = DEFAULT_TRAIN_FRACTION,
    validation_fraction: float = DEFAULT_VALIDATION_FRACTION,
    test_fraction: float = DEFAULT_TEST_FRACTION,
    min_split_size: int = MIN_SPLIT_SIZE,
) -> pd.DataFrame:
    """Assign each row to ``train`` / ``validation`` / ``test`` by chronology.

    Args:
        df: Input feature/labelled DataFrame.
        pickup_ts_column: Name of the timestamp column to sort on.
            The column must contain values that ``pandas`` can
            convert to ``datetime64[ns]``.
        train_fraction: Fraction of rows assigned to ``train`` after
            sorting.  The default is ``0.70``.
        validation_fraction: Fraction of rows assigned to
            ``validation``.  The default is ``0.15``.
        test_fraction: Fraction of rows assigned to ``test``.  The
            default is ``0.15``.
        min_split_size: Minimum number of rows per split.  When any
            split is smaller than this, the function raises
            ``ValueError`` to prevent meaningless evaluations.

    Returns:
        A copy of ``df`` sorted by ``pickup_ts_column`` with a new
        ``split`` column.
    """
    _validate_columns(df, pickup_ts_column)
    if df.empty:
        raise ValueError("Input DataFrame is empty; cannot split")

    train_fraction, validation_fraction, test_fraction = _normalise_fractions(
        train_fraction, validation_fraction, test_fraction
    )

    sorted_df = df.copy()
    sorted_df[pickup_ts_column] = pd.to_datetime(sorted_df[pickup_ts_column])
    sorted_df = sorted_df.sort_values(pickup_ts_column, kind="mergesort").reset_index(drop=True)

    n = len(sorted_df)
    train_end = int(round(n * train_fraction))
    # Guarantee the validation split is non-empty when ``n >= 3``.
    val_end = min(n, train_end + int(round(n * validation_fraction)))
    # Guarantee the test split is non-empty when ``n >= 3``.
    if val_end == train_end and n > train_end:
        val_end = train_end + 1
    if val_end >= n:
        val_end = n

    labels: list[str] = [""] * n
    for i in range(train_end):
        labels[i] = SPLIT_TRAIN
    for i in range(train_end, val_end):
        labels[i] = SPLIT_VALIDATION
    for i in range(val_end, n):
        labels[i] = SPLIT_TEST

    sorted_df = sorted_df.copy()
    sorted_df["split"] = labels

    counts = {
        SPLIT_TRAIN: labels.count(SPLIT_TRAIN),
        SPLIT_VALIDATION: labels.count(SPLIT_VALIDATION),
        SPLIT_TEST: labels.count(SPLIT_TEST),
    }
    for split_name, count in counts.items():
        if count < min_split_size:
            raise ValueError(
                f"Chronological split would produce a {split_name!r} "
                f"split with only {count} rows (minimum: {min_split_size}). "
                "Increase the dataset size or lower the min_split_size."
            )

    return sorted_df


def split_indices(
    n: int,
    *,
    train_fraction: float = DEFAULT_TRAIN_FRACTION,
    validation_fraction: float = DEFAULT_VALIDATION_FRACTION,
    test_fraction: float = DEFAULT_TEST_FRACTION,
) -> tuple[int, int, int]:
    """Return ``(train_end, val_end, n)`` for a sorted dataset of size ``n``.

    Exposed separately so the test suite can verify the boundary
    arithmetic in isolation.
    """
    train_fraction, validation_fraction, test_fraction = _normalise_fractions(
        train_fraction, validation_fraction, test_fraction
    )
    train_end = int(round(n * train_fraction))
    val_end = min(n, train_end + int(round(n * validation_fraction)))
    if val_end == train_end and n > train_end:
        val_end = train_end + 1
    if val_end >= n:
        val_end = n
    return train_end, val_end, n


def slice_split(df: pd.DataFrame, split_label: str) -> pd.DataFrame:
    """Filter a labelled DataFrame down to a single split.

    Args:
        df: The labelled DataFrame produced by
            :func:`chronological_split`.
        split_label: One of ``"train"``, ``"validation"``, ``"test"``.

    Returns:
        A copy of ``df`` with only the rows whose ``split`` column
        equals ``split_label``.
    """
    if split_label not in ALL_SPLITS:
        raise ValueError(f"Unknown split label {split_label!r}; expected one of {ALL_SPLITS}")
    if "split" not in df.columns:
        raise ValueError("DataFrame has no 'split' column; run chronological_split first")
    return df.loc[df["split"] == split_label].copy()


__all__ = [
    "ALL_SPLITS",
    "SPLIT_TEST",
    "SPLIT_TRAIN",
    "SPLIT_VALIDATION",
    "chronological_split",
    "slice_split",
    "split_indices",
]
