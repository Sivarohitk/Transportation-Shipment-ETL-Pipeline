# Transportation Shipment ETL Pipeline

Portfolio-ready PySpark ETL project for transportation shipment analytics. The pipeline ingests shipment, carrier, and delivery-event data, applies schema and data-quality checks, enriches region context, and publishes Hive-style curated datasets plus KPI outputs.

Run the commands below from the repository root.

## Project Status
- End-to-end local batch execution is implemented for daily and backfill runs.
- Editable install and CLI entrypoints are supported.
- Unit, integration, SQL, and data-quality tests are included.
- EMR deployment artifacts and configuration templates are included with placeholder-only AWS values.

## What's Included
- Ingestion for `shipments`, `carriers`, and `delivery_events`
- Explicit schema enforcement from `config/schemas/*.schema.json`
- Null, duplicate, schema-drift, and business-rule quality checks
- Staging SQL models in `sql/staging`
- Standardization and region enrichment transforms
- Curated outputs:
  - `dim_carrier`
  - `fct_shipment`
  - `fct_delivery_event`
  - `agg_shipment_daily`
  - `kpi_delivery_daily`
- Partitioned outputs by `p_date`, `region_code`, and `carrier_id`
- Local sample data for deterministic runs and tests

## Repository Layout
- `src/transport_etl`: Python package source
- `config`: YAML runtime configs, Spark profiles, and JSON schemas
- `sql`: staging, quality, curated, and KPI SQL definitions
- `data/sample`: synthetic raw and reference input data
- `tests`: unit, integration, SQL, and data-quality tests
- `deploy/emr`: portfolio-safe EMR scripts and config templates
- `docs`: architecture, curated model, KPI, testing, and EMR run docs

## Quick Start
```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements-dev.txt
python -m pip install -e .
transport-etl --help
python -m transport_etl.main --help
```

If you are not activating a virtual environment, use `python -m transport_etl.main ...` instead of relying on the console script being on `PATH`.

## How To Run Locally
```powershell
python -m transport_etl.main --job daily --config dev --run-date 2026-01-01
```

```powershell
python -m transport_etl.main --job backfill --config dev --start-date 2026-01-01 --end-date 2026-01-02
```

Equivalent Make targets:
- `make install`
- `make install-dev`
- `make run-local`
- `make run-backfill`
- `make test`
- `make lint`
- `make format`

## Testing
```powershell
pytest -q
ruff check .
black --check .
```

Layer-specific runs:
```powershell
pytest -q tests/unit
pytest -q tests/integration
pytest -q tests/data_quality tests/sql
```

## Sample Outputs
Curated outputs are written under `data/local/curated/<table_name>/` for local runs.

Expected curated tables:
- `dim_carrier`
- `fct_shipment`
- `fct_delivery_event`
- `agg_shipment_daily`
- `kpi_delivery_daily`

Partition contract:
- `p_date`
- `region_code`
- `carrier_id`

## Configuration
- Base config: `config/base.yaml`
- Local/dev overrides: `config/dev.yaml`
- EMR/prod overrides: `config/prod.yaml`
- Spark profiles: `config/spark/local.conf`, `config/spark/emr.conf`

Local development defaults are safe for sample-data runs:
- `config/dev.yaml` disables Hive table registration by default
- Windows local fallback is enabled for invalid-record and curated-output writes

## Windows Local Development Notes
- Native Windows Spark writes can fail without `HADOOP_HOME` and `winutils.exe`.
- This project keeps Parquet as the default output format, but local Windows dev mode can fall back to JSON for invalid-record and curated writes when that specific Hadoop error occurs.
- EMR and normal Linux-style runs still target Parquet.

## CI
GitHub Actions runs:
- editable install
- `transport-etl --help`
- `python -m transport_etl.main --help`
- `pytest -q`
- `ruff check .`
- `black --check .`

## Documentation
- [Architecture](docs/architecture.md)
- [Curated Tables](docs/curated_tables.md)
- [KPI Definitions](docs/kpi_definitions.md)
- [Testing Plan](docs/testing_plan.md)
- [EMR Run Instructions](docs/emr_run_instructions.md)
