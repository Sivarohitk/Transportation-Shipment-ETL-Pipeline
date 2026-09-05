"""Tests for the chronological splitter and the split-indices helper.

The chronological split is the contract that protects the model
from accidental future leakage.  These tests assert:

- the split labels the earliest rows ``train`` and the latest
  rows ``test``;
- the split never shuffles the rows that have the same
  ``pickup_ts`` (stable sort);
- the split rejects a DataFrame with too few rows per split;
- the three subsets are non-overlapping and their union covers
  the input;
- :func:`split_indices` is consistent with
  :func:`chronological_split`;
- the test split always includes the chronologically latest rows.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from transport_etl.ml.splits import (
    SPLIT_TEST,
    SPLIT_TRAIN,
    SPLIT_VALIDATION,
    chronological_split,
    slice_split,
    split_indices,
)


def _make_frame(n: int, start: str = "2025-01-01", seed: int = 0) -> pd.DataFrame:
    """Build a frame with ``n`` rows of chronologically ordered pickups."""
    rng = np.random.default_rng(seed)
    base = pd.Timestamp(start, tz="UTC")
    seconds = np.arange(n) * 3600  # one shipment per hour
    jitter = rng.integers(0, 60, size=n)
    pickup_ts = pd.to_datetime(
        [base + pd.Timedelta(seconds=int(s + j)) for s, j in zip(seconds, jitter)],
        utc=True,
    )
    return pd.DataFrame(
        {
            "shipment_id": [f"SHP{i:06d}" for i in range(n)],
            "pickup_ts": pickup_ts,
            "is_late": (np.arange(n) % 3 == 0).astype(int),
        }
    )


class TestChronologicalSplit:
    def test_train_is_earliest_and_test_is_latest(self) -> None:
        df = _make_frame(300)
        split = chronological_split(df)
        train = slice_split(split, SPLIT_TRAIN)
        val = slice_split(split, SPLIT_VALIDATION)
        test = slice_split(split, SPLIT_TEST)
        assert train["pickup_ts"].max() <= val["pickup_ts"].min()
        assert val["pickup_ts"].max() <= test["pickup_ts"].min()

    def test_split_covers_input_without_gaps(self) -> None:
        df = _make_frame(300)
        split = chronological_split(df)
        # Union of sizes equals input size.
        assert len(split) == len(df)
        # No row is dropped or duplicated.
        assert split["shipment_id"].nunique() == df["shipment_id"].nunique()

    def test_split_is_non_overlapping(self) -> None:
        df = _make_frame(300)
        split = chronological_split(df)
        for split_a, split_b in (
            (SPLIT_TRAIN, SPLIT_VALIDATION),
            (SPLIT_TRAIN, SPLIT_TEST),
            (SPLIT_VALIDATION, SPLIT_TEST),
        ):
            a_ids = set(split.loc[split["split"] == split_a, "shipment_id"])
            b_ids = set(split.loc[split["split"] == split_b, "shipment_id"])
            assert a_ids.isdisjoint(b_ids), f"{split_a} and {split_b} overlap"

    def test_split_fractions_sum_to_one(self) -> None:
        df = _make_frame(300)
        split = chronological_split(df)
        counts = split["split"].value_counts()
        total = int(counts.sum())
        assert total == 300
        # Each split is non-empty by construction.
        for label in (SPLIT_TRAIN, SPLIT_VALIDATION, SPLIT_TEST):
            assert int(counts.get(label, 0)) > 0

    def test_normalises_fractions_in_test(self) -> None:
        df = _make_frame(300)
        # 50 / 20 / 20 = 90 → normalised; the test set must still
        # cover the latest rows.
        split = chronological_split(
            df,
            train_fraction=50,
            validation_fraction=20,
            test_fraction=20,
        )
        assert int(split["split"].value_counts().sum()) == 300

    def test_does_not_shuffle_rows_with_same_pickup_ts(self) -> None:
        # All rows share the same pickup_ts; the splitter must keep
        # the input order.  Use a tiny min_split_size to avoid the
        # 30-row-per-split guard.
        df = _make_frame(50)
        df["pickup_ts"] = pd.Timestamp("2025-01-01", tz="UTC")
        split = chronological_split(df, min_split_size=1)
        assert list(split["shipment_id"]) == list(df["shipment_id"])

    def test_rejects_too_small_frame(self) -> None:
        df = _make_frame(5)
        with pytest.raises(ValueError, match="minimum"):
            chronological_split(df, min_split_size=10)

    def test_rejects_empty_frame(self) -> None:
        with pytest.raises(ValueError, match="empty"):
            chronological_split(pd.DataFrame({"shipment_id": [], "pickup_ts": []}))

    def test_rejects_missing_pickup_ts_column(self) -> None:
        with pytest.raises(ValueError, match="pickup_ts"):
            chronological_split(pd.DataFrame({"shipment_id": ["a"]}))

    def test_rejects_negative_fractions(self) -> None:
        df = _make_frame(100)
        with pytest.raises(ValueError, match="non-negative"):
            chronological_split(df, train_fraction=-0.1)

    def test_normalises_fractions_that_do_not_sum_to_one(self) -> None:
        df = _make_frame(300)
        # 50 / 20 / 20 = 90 → normalised to 50/90, 20/90, 20/90
        split = chronological_split(
            df,
            train_fraction=50,
            validation_fraction=20,
            test_fraction=20,
        )
        counts = split["split"].value_counts()
        assert counts.sum() == 300

    def test_split_indices_matches_split(self) -> None:
        n = 300
        train_end, val_end, total = split_indices(
            n, train_fraction=0.7, validation_fraction=0.15, test_fraction=0.15
        )
        df = _make_frame(n)
        split = chronological_split(
            df, train_fraction=0.7, validation_fraction=0.15, test_fraction=0.15
        )
        assert total == n
        assert (split["split"] == SPLIT_TRAIN).sum() == train_end
        assert (split["split"] == SPLIT_VALIDATION).sum() == val_end - train_end
        assert (split["split"] == SPLIT_TEST).sum() == total - val_end

    def test_each_split_is_sorted_by_pickup_ts(self) -> None:
        df = _make_frame(300)
        split = chronological_split(df)
        for label in (SPLIT_TRAIN, SPLIT_VALIDATION, SPLIT_TEST):
            subset = split.loc[split["split"] == label]
            assert subset["pickup_ts"].is_monotonic_increasing

    def test_test_split_contains_latest_pickup(self) -> None:
        df = _make_frame(200)
        split = chronological_split(df)
        test = slice_split(split, SPLIT_TEST)
        # The test set must include the chronologically latest row.
        assert test["pickup_ts"].max() == df["pickup_ts"].max()
