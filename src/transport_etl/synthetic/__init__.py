"""Deterministic synthetic operational-data generator.

This package produces realistic-but-synthetic shipment, carrier, and
delivery-event data for portfolio demonstration and downstream ML
evaluation.  It is **not** production data and must not be presented
as such.

The generator is invoked by:

- The CLI entry point :mod:`transport_etl.synthetic.cli` (a thin
  wrapper that writes the generated CSVs to ``data/generated/``).
- The library API :func:`transport_etl.synthetic.generator.generate_dataset`.

The committed sample in ``data/sample/raw`` is still used for
fast, deterministic unit-test fixtures.  The generated dataset
is the foundation for late-delivery risk model training and
chronological evaluation in Phase 8.
"""

from __future__ import annotations

from transport_etl.synthetic.generator import (
    CITY_TO_STATE,
    DEFAULT_SEED,
    DELAY_REASONS,
    EVENT_TYPES,
    EXCEPTION_EVENT_TYPES,
    SERVICE_MODES,
    STATE_TO_REGION,
    GeneratedDataset,
    GeneratorConfig,
    generate_dataset,
    write_csv_files,
)

__all__ = [
    "CITY_TO_STATE",
    "DEFAULT_SEED",
    "DELAY_REASONS",
    "EVENT_TYPES",
    "EXCEPTION_EVENT_TYPES",
    "GeneratedDataset",
    "GeneratorConfig",
    "SERVICE_MODES",
    "STATE_TO_REGION",
    "generate_dataset",
    "write_csv_files",
]
