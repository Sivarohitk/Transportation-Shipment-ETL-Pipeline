# Testing Plan

This project uses a layered testing approach to validate both engineering correctness and data quality outcomes.

## Objectives
- Validate config-driven behavior and helper modules
- Validate schema and quality controls before curation
- Validate curated model grain and KPI math
- Validate end-to-end local batch run behavior
- Keep tests deterministic with synthetic sample data

## Test Layers

### 1. Unit Tests (`tests/unit`)
- Focus: pure logic and small module contracts
- Areas covered:
  - Config load/merge/env expansion
  - Schema drift detection
  - Null and duplicate helper functions
  - Shipment fact and KPI builder behavior

### 2. Integration Tests (`tests/integration`)
- Focus: runnable pipeline behavior and publish mechanics
- Areas covered:
  - Local daily batch orchestration
  - Partitioned Parquet writes
  - Partition contract enforcement

### 3. Data Quality Tests (`tests/data_quality`)
- Focus: curated-data reliability checks
- Areas covered:
  - Null policy for curated key columns
  - Uniqueness by declared table grain
  - Rowcount reconciliation across stages

### 4. SQL Validation Tests (`tests/sql`)
- Focus: parity between SQL KPI model and Python KPI builder
- Areas covered:
  - Metric-level equivalence for all KPI outputs

## Fixtures and Test Data
- Shared fixtures are defined in `tests/conftest.py`
- Synthetic input data is read from:
  - `data/sample/raw/*.csv`
  - `data/sample/reference/region_lookup.csv`
- Fixtures build staged and curated DataFrames in-memory for deterministic assertions

## Execution Commands
```bash
# From project root
pytest -q
```

```bash
# Optional: run only integration tests
pytest -q tests/integration
```

```bash
# Optional: run only data-quality + SQL checks
pytest -q tests/data_quality tests/sql
```

## Runtime Assumptions
- Spark-dependent tests require `pyspark` installed
- In environments without `pyspark`, Spark tests are skipped (not failed)
- All tests avoid external credentials and external systems

## CI/CD Recommendations
- Run `pytest -q` on every PR
- Add lint/type checks (`ruff`, `black --check`, `mypy`) as separate gates
- Keep synthetic data snapshots versioned for reproducible assertions
- Add a nightly EMR smoke test in non-production AWS account using placeholder-safe config
