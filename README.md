# Transportation Shipment ETL Pipeline

Production-style PySpark ETL project for transportation shipment analytics, with local execution and EMR deployment artifacts.

## What This Project Implements
- Config-driven ingestion for `shipments`, `carriers`, and `delivery_events`
- Schema enforcement, bad-record handling, and quality checks
- Standardization and region enrichment transforms
- Curated Hive-style outputs:
  - `dim_carrier`
  - `fct_shipment`
  - `fct_delivery_event`
  - `agg_shipment_daily`
  - `kpi_delivery_daily`
- Partitioned Parquet publish by `p_date`, `region_code`, `carrier_id`
- CLI entrypoints for daily and backfill batch runs
- Unit, integration, SQL, and data-quality tests

## Tech Stack
- Python 3.10+
- PySpark
- Spark SQL
- Hive-style catalog patterns
- AWS EMR deployment artifacts
- Pytest

## Repository Layout
- `src/transport_etl`: Python package source
- `config`: YAML runtime configs + JSON schemas
- `sql`: staging, quality, curated, and KPI SQL models
- `data/sample`: synthetic raw/reference test data
- `deploy/emr`: bootstrap/config/steps/scripts for EMR runs
- `docs`: architecture, model, KPI, testing, and EMR runbook docs
- `tests`: unit, integration, SQL, and data quality tests

## Setup
```bash
python -m venv .venv
. .venv/Scripts/activate
pip install -r requirements.txt
pip install -r requirements-dev.txt
pip install -e .
```

## Local Run Commands
```bash
# Show CLI options
transport-etl --help
```

```bash
# Daily run on sample date
transport-etl --job daily --config config/dev.yaml --run-date 2026-01-01 --no-register-hive
```

```bash
# Backfill run (inclusive window)
transport-etl --job backfill --config config/dev.yaml --start-date 2026-01-01 --end-date 2026-01-03 --no-register-hive
```

## Windows Local Development Notes
- Spark local writes on Windows may fail with `HADOOP_HOME/hadoop.home.dir` and `winutils.exe` errors when writing Parquet.
- In `config/dev.yaml`, local fallback is enabled for:
  - ingestion/quality quarantine writes (`io.invalid_records`)
  - curated table writes (`io.curated_outputs`)
- Fallback behavior:
  - still attempts Parquet first
  - only falls back when running local profile on native Windows and the failure matches winutils/Hadoop-home errors
  - fallback format defaults to JSON (configurable to CSV)
- Fallback output paths:
  - ingest quarantine: `.../quarantine/ingest/.../<entity>/_fallback_json`
  - quality quarantine: `.../quarantine/quality/.../<entity>/<rule_name>/_fallback_json`
  - curated tables: `.../curated/<table_name>/_fallback_json/p_date=.../region_code=.../carrier_id=...`
- Hive registration:
  - if curated fallback format is used (JSON/CSV), Hive Parquet table registration is skipped for that write.
- EMR/prod behavior is unchanged: Parquet remains default and fallback is disabled by config.

## Test Commands
```bash
# Full suite
pytest -q
```

```bash
# Layered runs
pytest -q tests/unit
pytest -q tests/integration
pytest -q tests/data_quality tests/sql
```

## Make Targets
- `make install`
- `make install-dev`
- `make run-local`
- `make test`
- `make format`
- `make lint`

## Configuration
- Base: `config/base.yaml`
- Env overrides: `config/dev.yaml`, `config/prod.yaml`
- Spark profiles: `config/spark/local.conf`, `config/spark/emr.conf`
- Schemas: `config/schemas/*.schema.json`

## EMR
Use [EMR run instructions](docs/emr_run_instructions.md) with scripts in `deploy/emr/scripts/`.
All AWS identifiers in repo artifacts are placeholders.

## Notes
- Spark-dependent runtime and tests require `pyspark`.
- In environments without Spark dependencies, Spark tests are skipped.
