"""Command-line entry point for the synthetic data generator.

The CLI is intentionally thin: it parses a small set of
configuration overrides, generates the dataset, and writes the
three CSV files into ``data/generated/`` (git-ignored by
``.gitignore``).  The committed sample under ``data/sample/raw`` is
never overwritten.

Usage:

    python -m transport_etl.synthetic.cli \\
        --output-dir data/generated \\
        --shipment-count 5000 \\
        --horizon-days 180 \\
        --seed 20260101

Defaults produce a 5,000-shipment, 180-day dataset that is large
enough to support a chronologically valid train/validation/test
split for late-delivery risk modeling.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import date
from pathlib import Path

from transport_etl.synthetic import (
    DEFAULT_SEED,
    GeneratedDataset,
    GeneratorConfig,
    generate_dataset,
    write_csv_files,
)

LOGGER = logging.getLogger("transport_etl.synthetic.cli")


def _parse_date(text: str) -> date:
    """Parse ``YYYY-MM-DD`` into a :class:`date`."""
    from datetime import datetime

    return datetime.strptime(text, "%Y-%m-%d").date()


def _build_arg_parser() -> argparse.ArgumentParser:
    """Return the CLI argument parser."""
    parser = argparse.ArgumentParser(
        prog="transport_etl.synthetic.cli",
        description=(
            "Generate a deterministic synthetic operational dataset "
            "for late-delivery risk model evaluation.  Output is "
            "written to --output-dir and the data is git-ignored."
        ),
    )
    parser.add_argument(
        "--output-dir",
        default="data/generated",
        help="Directory to write the CSV files into (default: data/generated).",
    )
    parser.add_argument(
        "--shipment-count",
        type=int,
        default=GeneratorConfig.shipment_count,
        help=f"Total shipment rows to generate (default: {GeneratorConfig.shipment_count}).",
    )
    parser.add_argument(
        "--start-date",
        type=_parse_date,
        default=GeneratorConfig.start_date,
        help=(
            "First pickup date (YYYY-MM-DD). " f"Default: {GeneratorConfig.start_date.isoformat()}."
        ),
    )
    parser.add_argument(
        "--horizon-days",
        type=int,
        default=GeneratorConfig.horizon_days,
        help=(
            "Number of days covered by the dataset. " f"Default: {GeneratorConfig.horizon_days}."
        ),
    )
    parser.add_argument(
        "--late-rate",
        type=float,
        default=GeneratorConfig.late_rate,
        help=("Target fraction of late shipments. " f"Default: {GeneratorConfig.late_rate}."),
    )
    parser.add_argument(
        "--first-attempt-rate",
        type=float,
        default=GeneratorConfig.first_attempt_rate,
        help=(
            "Target fraction of deliveries that succeed on the first attempt. "
            f"Default: {GeneratorConfig.first_attempt_rate}."
        ),
    )
    parser.add_argument(
        "--exception-event-rate",
        type=float,
        default=GeneratorConfig.exception_event_rate,
        help=(
            "Probability that a non-delivery event is an exception. "
            f"Default: {GeneratorConfig.exception_event_rate}."
        ),
    )
    parser.add_argument(
        "--carrier-count",
        type=int,
        default=GeneratorConfig.carrier_count,
        help=(
            "Number of distinct carriers in the carrier dimension. "
            f"Default: {GeneratorConfig.carrier_count}."
        ),
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=DEFAULT_SEED,
        help=("Random seed for reproducibility. " f"Default: {DEFAULT_SEED}."),
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    """CLI entry point.  Returns a UNIX exit code."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )
    parser = _build_arg_parser()
    args = parser.parse_args(argv)

    config = GeneratorConfig(
        shipment_count=args.shipment_count,
        start_date=args.start_date,
        horizon_days=args.horizon_days,
        late_rate=args.late_rate,
        first_attempt_rate=args.first_attempt_rate,
        exception_event_rate=args.exception_event_rate,
        carrier_count=args.carrier_count,
        seed=args.seed,
    )

    dataset: GeneratedDataset = generate_dataset(config)
    written = write_csv_files(dataset, Path(args.output_dir))

    summary = dataset.summary()
    print(
        json.dumps(
            {
                "config": config.__dict__,
                "summary": summary,
                "written": {k: str(v) for k, v in written.items()},
            },
            default=str,
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":  # pragma: no cover - CLI entry
    sys.exit(main())
