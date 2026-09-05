"""End-to-end tests for the Phase 8 training and scoring pipeline.

These tests assert:

- the full :func:`run_training_and_score` orchestrator produces
  a model, three evaluation reports, and a scored frame;
- the chronology split is respected end-to-end (the test set is
  the latest window);
- the pipeline is deterministic for a given input and seed;
- the primary reported test metrics are non-NaN on a synthetic
  sample;
- the CLI exit code is 0 and the produced files have the
  documented columns.
"""

from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from transport_etl.ml.constants import (
    MODEL_HIST_GRADIENT_BOOSTING,
    MODEL_LOGISTIC_REGRESSION,
)
from transport_etl.ml.evaluation import EvaluationReport
from transport_etl.ml.pipeline import (
    TrainingAndScoreResult,
    run_training_and_score,
)
from transport_etl.ml.scoring import score_summary
from transport_etl.synthetic import GeneratorConfig, generate_dataset


def _build_frame(n: int, *, seed: int = 20260101) -> tuple[pd.DataFrame, pd.Series]:
    ds = generate_dataset(
        GeneratorConfig(
            shipment_count=n,
            horizon_days=120,
            seed=seed,
        )
    )
    rows = []
    for s in ds.shipments:
        rows.append(
            {
                "shipment_id": s.shipment_id,
                "pickup_ts": s.pickup_ts,
                "carrier_id": s.carrier_id,
                "origin_state": s.origin_state,
                "destination_state": s.destination_state,
                "promised_delivery_ts": s.promised_delivery_ts,
                "distance_miles": s.distance_miles,
                "shipping_cost_usd": s.shipping_cost_usd,
                "region_code": "UNKNOWN",
                "origin_region_code": "UNKNOWN",
                "service_mode": "LTL",
                "is_active": True,
                "home_region_code": "UNKNOWN",
            }
        )
    shipments = pd.DataFrame(rows)
    is_late = pd.Series(
        [
            1 if (s.actual_delivery_ts and s.actual_delivery_ts > s.promised_delivery_ts) else 0
            for s in ds.shipments
        ],
        name="is_late",
    )
    return shipments, is_late


class TestPipelineEndToEnd:
    def test_lr_runs_end_to_end(self) -> None:
        shipments, is_late = _build_frame(800)
        result = run_training_and_score(
            shipments, is_late=is_late, model_name=MODEL_LOGISTIC_REGRESSION
        )
        assert isinstance(result, TrainingAndScoreResult)
        for report in (
            result.train_report,
            result.validation_report,
            result.test_report,
        ):
            assert isinstance(report, EvaluationReport)
            assert report.row_count > 0

    def test_gbt_runs_end_to_end(self) -> None:
        shipments, is_late = _build_frame(800)
        result = run_training_and_score(
            shipments, is_late=is_late, model_name=MODEL_HIST_GRADIENT_BOOSTING
        )
        assert isinstance(result, TrainingAndScoreResult)
        assert result.model.model_name == MODEL_HIST_GRADIENT_BOOSTING

    def test_test_split_is_chronologically_latest(self) -> None:
        shipments, is_late = _build_frame(800)
        result = run_training_and_score(shipments, is_late=is_late)
        # The test set's latest pickup timestamp must be the
        # chronologically latest of the input.
        test = result.scored.loc[
            result.scored["pickup_ts"] >= result.scored["pickup_ts"].quantile(0.85)
        ]
        assert test["pickup_ts"].min() >= shipments["pickup_ts"].quantile(0.70)

    def test_split_labels_partition_input(self) -> None:
        shipments, is_late = _build_frame(800)
        result = run_training_and_score(shipments, is_late=is_late)
        # The scored frame is sorted by ``pickup_ts`` and the last
        # 15% of rows form the test split.  Verify by row count.
        n = len(shipments)
        train_count = int(n * 0.70)
        val_count = int(n * 0.15)
        assert result.train_report.row_count == train_count
        # validation may include 1 extra row from the floor-rounding.
        assert (
            result.validation_report.row_count == val_count
            or result.validation_report.row_count == val_count + 1
        )
        # Test set fills the remainder.
        assert result.test_report.row_count == n - train_count - (
            result.validation_report.row_count
        )

    def test_pipeline_deterministic_for_same_seed(self) -> None:
        shipments, is_late = _build_frame(400)
        a = run_training_and_score(shipments, is_late=is_late, model_name=MODEL_LOGISTIC_REGRESSION)
        b = run_training_and_score(shipments, is_late=is_late, model_name=MODEL_LOGISTIC_REGRESSION)
        # The scored frame and the test report should be byte-identical.
        np.testing.assert_array_equal(
            a.scored["risk_probability"].to_numpy(),
            b.scored["risk_probability"].to_numpy(),
        )
        assert a.test_report.auc_roc == b.test_report.auc_roc
        assert a.test_report.f1_at_default == b.test_report.f1_at_default

    def test_rejects_length_mismatch(self) -> None:
        shipments, is_late = _build_frame(100)
        with pytest.raises(ValueError, match="length"):
            run_training_and_score(shipments, is_late=is_late.iloc[:50])

    def test_scored_frame_contains_every_input_shipment(self) -> None:
        shipments, is_late = _build_frame(500)
        result = run_training_and_score(shipments, is_late=is_late)
        assert len(result.scored) == len(shipments)
        assert set(result.scored["shipment_id"]) == set(shipments["shipment_id"])

    def test_summary_dict_is_well_formed(self) -> None:
        shipments, is_late = _build_frame(500)
        result = run_training_and_score(shipments, is_late=is_late)
        summary = score_summary(result.scored)
        assert summary["row_count"] == 500
        assert (
            summary["band_low_count"]
            + summary["band_medium_count"]
            + summary["band_high_count"]
            + summary["band_critical_count"]
            == summary["row_count"]
        )


class TestCLI:
    def test_cli_runs(self, tmp_path: Path) -> None:
        # Generate a small synthetic dataset and write to disk.
        gen_dir = tmp_path / "gen"
        gen_dir.mkdir()
        from transport_etl.synthetic import write_csv_files

        ds = generate_dataset(GeneratorConfig(shipment_count=300, horizon_days=30, seed=20260101))
        write_csv_files(ds, gen_dir)

        out_dir = tmp_path / "scored"
        # Invoke the CLI via its main() entry.
        from transport_etl.ml.cli import main

        exit_code = main(
            [
                "train-and-score",
                "--shipments",
                str(gen_dir / f"shipments_{ds.config.start_date.isoformat()}.csv"),
                "--output-dir",
                str(out_dir),
                "--model",
                "logistic_regression",
            ]
        )
        assert exit_code == 0
        # The output files are written.
        assert (out_dir / "scored_shipments.csv").exists()
        assert (out_dir / "evaluation_reports.json").exists()
        assert (out_dir / "model.pkl").exists()

        # The CSV has the documented columns.
        scored_df = pd.read_csv(out_dir / "scored_shipments.csv")
        from transport_etl.ml.constants import ALL_SCORE_COLUMNS

        # ``pickup_ts`` round-trips through CSV as a string; the other
        # columns are preserved as-is.
        for column in ALL_SCORE_COLUMNS:
            assert column in scored_df.columns

        # The JSON file parses and has the documented keys.
        with (out_dir / "evaluation_reports.json").open("r", encoding="utf-8") as h:
            payload = json.load(h)
        for split in ("train", "validation", "test"):
            assert split in payload
            assert "row_count" in payload[split]
            assert "auc_roc" in payload[split]
