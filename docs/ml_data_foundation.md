# ML Data Foundation — Synthetic Operational Data Generator

## Purpose

The committed sample under `data/sample/raw` is **too small** for
chronologically valid late-delivery risk model training:

- 12 shipment rows, 6 carriers, 43 events
- 5-day pickup window
- A 70/15/15 chronological split would give roughly 8 / 2 / 2 rows,
  which produces meaningless evaluation metrics

Phase 7 adds a **deterministic synthetic operational-data
generator** that produces a much larger dataset on demand.  The
generator is committed; the generated CSVs are written to
`data/generated/` and are **git-ignored** so the repository stays
reviewable.

> **SYNTHETIC DATA — NOT PRODUCTION.**  The generator is for
> portfolio demonstration and ML model training.  Do not present
> generated records as real operational data.

## Layout

```
data/
├── sample/                       # committed deterministic fixture (12 shipments)
│   └── raw/
│       ├── carriers_2026-01-01.csv
│       ├── shipments_2026-01-01.csv
│       └── delivery_events_2026-01-01.csv
└── generated/                    # git-ignored; CLI output goes here
    ├── .gitkeep
    ├── carriers_2025-07-01.csv
    ├── shipments_2025-07-01.csv
    └── delivery_events_2025-07-01.csv
```

## Usage

### Library API

```python
from transport_etl.synthetic import (
    GeneratorConfig,
    generate_dataset,
    write_csv_files,
)

config = GeneratorConfig(
    shipment_count=5_000,   # total rows
    horizon_days=180,        # pickup-date window
    seed=20260101,           # deterministic random seed
    late_rate=0.18,          # target late-delivery fraction
)
dataset = generate_dataset(config)
write_csv_files(dataset, "data/generated")
```

`GeneratedDataset.summary()` returns a small dict you can log:

```python
{"carriers": 8, "shipments": 5000, "events": 24282, "late_shipments": 944}
```

### CLI

```bash
python -m transport_etl.synthetic.cli \
    --output-dir data/generated \
    --shipment-count 5000 \
    --horizon-days 180 \
    --seed 20260101
```

## Default Configuration

| Knob | Default | Notes |
|------|---------|-------|
| `shipment_count` | 5,000 | Enough to support a 70/15/15 split with several hundred late records per split |
| `start_date` | 2025-07-01 | First pickup day |
| `horizon_days` | 180 | Six months — supports a chronological train/validation/test split |
| `late_rate` | 0.18 | Realistic operational late rate |
| `first_attempt_rate` | 0.78 | Realistic first-attempt success rate |
| `exception_event_rate` | 0.12 | Probability of a non-delivery event being DELAYED / EXCEPTION / HOLD |
| `carrier_count` | 8 | Distinct carriers, each with a different service-mode bias and home region |
| `seed` | 20260101 | Default seed for reproducibility |
| `scac_pool` | 12 four-letter codes | Realistic-looking SCAC values |

All defaults are configurable; the dataclass is frozen so the
generator cannot mutate the config mid-run.

## Reproducibility

The generator is **deterministic** with respect to the seed:

- All randomness flows through a single `random.Random(seed)`
  instance — there is no hidden global state, no Numpy / Spark
  randomness, no time-of-day or hostname leakage.
- The same `(config, seed)` pair always produces byte-identical CSVs
  (verified by `tests/unit/test_synthetic_generator.py::TestCSVFidelity`).
- The default seed (`20260101`) is exported as
  `transport_etl.synthetic.DEFAULT_SEED`.

## Target Label

The late-delivery target is **derived**, not stored:

```python
is_late = actual_delivery_ts > promised_delivery_ts
```

The CSV output contains only the source columns; the model
training code derives the target from the public schema.  This
matches the existing ingest contract and keeps the generator
honest — the same expression produces the label in both training
and evaluation.

## Leakage Controls

The generator is designed so the target cannot be derived from
any single CSV column at pickup time:

| Feature available at prediction time | Used? | Why |
|---------------------------------------|-------|-----|
| `carrier_id`                          | yes   | Operational |
| `origin_state`, `destination_state`   | yes   | Operational |
| `pickup_ts`                           | yes   | Operational |
| `promised_delivery_ts`                | yes   | Operational (the SLA) |
| `service_mode` (joined from `dim_carrier`) | yes | Operational |
| `distance_miles`                      | yes   | Operational |
| `shipping_cost_usd`                   | yes   | Operational |
| `home_region_code` (joined)            | yes   | Operational |
| `is_active` (joined)                   | yes   | Operational |
| `pickup_dow`, `pickup_hour` (derived)  | yes   | Operational |
| `promised_transit_hours` (derived)    | yes   | Operational |

| Feature NOT used (would leak)           | Why excluded |
|------------------------------------------|--------------|
| `actual_delivery_ts`                     | Only known after delivery — the label source |
| `delay_minutes`                          | Derived from `actual - promised` |
| `on_time_delivery_flag`                  | Same as the label |
| `transit_time_hours`                     | Only known after delivery |
| `exception_flag`                         | Derived from delivery events |
| Delivery events except `PICKED_UP`      | Only known during transit |

The dedicated leakage test
(`tests/unit/test_synthetic_generator.py::TestNoTargetLeakage`)
verifies that no *non-target* column deterministically predicts the
label on a 500-shipment sample (it allows up to 25% of
high-support groups to be homogeneous to absorb small-sample
noise).

## Variance Sources

Each shipment's late outcome is the result of several
independent random draws:

1. **Carrier pick** — uniform over the active carrier pool
2. **State pair** — uniform over the 19 state set
3. **Distance** — lognormal around 1,100 miles with a 18% short-haul
   fraction
4. **Service-mode speed** — FTL 55 mph, LTL 50 mph, PARCEL 40 mph
5. **Promised transit** — 85–95% of the sampled transit, scaled by
   the carrier reliability multiplier
6. **Carrier reliability multiplier** — uniform 0.6–1.6 per carrier
7. **Realized transit** — overrun sampled from a uniform-on-fraction
   distribution; on-time records get a small underrun
8. **Per-shipment noise** — uniform in `[-1, +1]` on the logit

The global late rate is approximately held to
`config.late_rate` on a 1,000-shipment sample.  See
`test_configured_late_rate_holds_in_aggregate`.

## Tests

| File | Coverage |
|------|----------|
| `tests/unit/test_synthetic_generator.py` | Determinism, schema validity, chronology, target presence, leakage, CSV fidelity, CLI smoke test, public API |
| `tests/data_quality/test_synthetic_data_quality.py` | FK integrity, late-rate band, non-negative numerics, realised-vs-promised transit relationship |

Live-Databricks tests are not required for Phase 7 because no
Delta write happens.  The generator only writes local CSV files.

## Why Not Just Commit a Large Dataset?

A 5,000-shipment, 180-day CSV dump is on the order of a few
megabytes.  That is not large in absolute terms, but:

- It would bloat every clone and every PR diff forever
- It would be tempting for reviewers to skim the data instead of
  the generator
- It would make the synthetic data look "real" enough that
  someone might present it as production data
- The generator + the deterministic default seed is a strictly
  better asset: the same `(seed, config)` always produces the
  same data, so any reviewer can regenerate locally in seconds

For these reasons the generated CSVs are git-ignored.  The
generator is the source of truth.
