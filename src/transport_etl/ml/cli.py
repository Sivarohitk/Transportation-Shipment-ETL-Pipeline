"""Command-line entry point for the late-shipment risk model pipeline.

Usage:

    python -m transport_etl.ml.cli train-and-score \\
        --shipments data/generated/shipments_2025-07-01.csv \\
        --output-dir data/scored \\
        --model logistic_regression

The CLI loads the CSV file via :mod:`pandas`, derives the target
``is_late`` from the public schema, then runs
:func:`transport_etl.ml.pipeline.run_training_and_score` end-to-end.

This module is deliberately thin.  All of the work is in
:mod:`transport_etl.ml.pipeline`.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

import numpy as np
import pandas as pd

from transport_etl.ml.constants import (
    ALL_MODEL_NAMES,
    DEFAULT_DECISION_THRESHOLD,
    DEFAULT_TEST_FRACTION,
    DEFAULT_TRAIN_FRACTION,
    DEFAULT_VALIDATION_FRACTION,
    MODEL_LOGISTIC_REGRESSION,
)
from transport_etl.ml.pipeline import run_training_and_score

LOGGER = logging.getLogger("transport_etl.ml.cli")


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="transport_etl.ml.cli",
        description=(
            "Train a late-shipment risk model on a chronological split "
            "and produce a scored decision-support frame.  All input "
            "data must be SYNTHETIC or clearly marked production data; "
            "the model is intended for portfolio / decision-support use."
        ),
    )
    sub = parser.add_subparsers(dest="command", required=True)

    train_p = sub.add_parser(
        "train-and-score",
        help="Train a model and produce the scored frame + reports.",
    )
    train_p.add_argument(
        "--shipments",
        required=True,
        help="Path to a shipments CSV produced by the synthetic generator or pipeline.",
    )
    train_p.add_argument(
        "--output-dir",
        required=True,
        help="Directory to write the scored frame, reports, and the fitted model.",
    )
    train_p.add_argument(
        "--model",
        choices=ALL_MODEL_NAMES,
        default=MODEL_LOGISTIC_REGRESSION,
        help="Model to train.  Default: logistic_regression.",
    )
    train_p.add_argument(
        "--train-fraction",
        type=float,
        default=DEFAULT_TRAIN_FRACTION,
        help="Chronological train split fraction.  Default: 0.70.",
    )
    train_p.add_argument(
        "--validation-fraction",
        type=float,
        default=DEFAULT_VALIDATION_FRACTION,
        help="Chronological validation split fraction.  Default: 0.15.",
    )
    train_p.add_argument(
        "--test-fraction",
        type=float,
        default=DEFAULT_TEST_FRACTION,
        help="Chronological test split fraction.  Default: 0.15.",
    )
    train_p.add_argument(
        "--decision-threshold",
        type=float,
        default=DEFAULT_DECISION_THRESHOLD,
        help="Decision threshold for the predicted_late output.  Default: 0.25.",
    )
    return parser


def _load_shipments(path: Path) -> tuple[pd.DataFrame, pd.Series]:
    """Read the shipments CSV and derive the ``is_late`` label.

    The label is **always** derived from the public schema.  The
    caller cannot inject a fabricated label — the function reads
    ``actual_delivery_ts`` and ``promised_delivery_ts`` and computes
    the boolean.
    """
    df = pd.read_csv(path)
    if "shipment_id" not in df.columns:
        raise ValueError(f"shipments file {path} is missing the 'shipment_id' column")
    if "pickup_ts" not in df.columns:
        raise ValueError(f"shipments file {path} is missing the 'pickup_ts' column")
    if "actual_delivery_ts" not in df.columns:
        raise ValueError(f"shipments file {path} is missing the 'actual_delivery_ts' column")
    if "promised_delivery_ts" not in df.columns:
        raise ValueError(f"shipments file {path} is missing the 'promised_delivery_ts' column")
    actual = pd.to_datetime(df["actual_delivery_ts"], errors="coerce", utc=True)
    promised = pd.to_datetime(df["promised_delivery_ts"], errors="coerce", utc=True)
    is_late = (actual > promised).astype(int)
    is_late[actual.isna()] = 0  # unknown outcome -> not late
    return df, is_late


def _cmd_train_and_score(args: argparse.Namespace) -> int:
    shipments_path = Path(args.shipments)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    LOGGER.info("Loading shipments from %s", shipments_path)
    shipments, is_late = _load_shipments(shipments_path)
    LOGGER.info("Loaded %d shipments (%d late)", len(shipments), int(is_late.sum()))

    LOGGER.info("Running model=%s with chronological split", args.model)
    result = run_training_and_score(
        shipments,
        is_late=is_late,
        model_name=args.model,
        train_fraction=args.train_fraction,
        validation_fraction=args.validation_fraction,
        test_fraction=args.test_fraction,
        decision_threshold=args.decision_threshold,
    )

    scored_path = output_dir / "scored_shipments.csv"
    result.scored.to_csv(scored_path, index=False)
    LOGGER.info("Wrote scored frame to %s", scored_path)

    reports_path = output_dir / "evaluation_reports.json"
    with reports_path.open("w", encoding="utf-8") as handle:
        json.dump(
            {
                "train": result.train_report.to_dict(),
                "validation": result.validation_report.to_dict(),
                "test": result.test_report.to_dict(),
                "scored_summary": result.scored_summary,
            },
            handle,
            indent=2,
        )
    LOGGER.info("Wrote evaluation reports to %s", reports_path)

    model_path = output_dir / "model.pkl"
    result.model.save(str(model_path))
    LOGGER.info("Wrote fitted model to %s", model_path)

    summary = {
        "scored_rows": int(len(result.scored)),
        "train_report": _short_summary(result.train_report),
        "validation_report": _short_summary(result.validation_report),
        "test_report": _short_summary(result.test_report),
        "scored_summary": result.scored_summary,
        "model_path": str(model_path),
        "scored_path": str(scored_path),
        "reports_path": str(reports_path),
    }
    print(json.dumps(summary, indent=2, default=_json_default))
    return 0


def _short_summary(report) -> dict[str, float | int]:
    return {
        "row_count": report.row_count,
        "positive_count": report.positive_count,
        "positive_rate": report.positive_rate,
        "auc_roc": report.auc_roc,
        "auc_pr": report.auc_pr,
        "precision_at_default": report.precision_at_default,
        "recall_at_default": report.recall_at_default,
        "f1_at_default": report.f1_at_default,
        "precision_at_opt_f1": report.precision_at_opt_f1,
        "recall_at_opt_f1": report.recall_at_opt_f1,
        "f1_opt": report.f1_opt,
        "threshold_opt_f1": report.threshold_opt_f1,
    }


def _json_default(obj):
    """JSON encoder for numpy / pandas types."""
    if isinstance(obj, (np.integer,)):
        return int(obj)
    if isinstance(obj, (np.floating,)):
        return float(obj)
    if isinstance(obj, (np.ndarray, pd.Index, pd.Series)):
        return obj.tolist()
    if isinstance(obj, pd.Timestamp):
        return obj.isoformat()
    return str(obj)


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )
    parser = _build_arg_parser()
    args = parser.parse_args(argv)
    if args.command == "train-and-score":
        return _cmd_train_and_score(args)
    parser.error(f"Unknown command {args.command!r}")
    return 2  # pragma: no cover - defensive


if __name__ == "__main__":  # pragma: no cover - CLI entry
    sys.exit(main())
