# AGENTS.md

## Project
Build a Transportation Shipment ETL Pipeline using PySpark, Spark SQL, Hive-style curated tables, and EMR-oriented batch processing.

## Goals
- Ingest shipment, carrier, and delivery-event data
- Clean and deduplicate records
- Build curated analytical tables
- Partition curated outputs by date, region, and carrier
- Output Parquet datasets
- Include tests and documentation
- Keep code modular and production-style

## Tech Stack
- Python
- PySpark
- Spark SQL
- Hive-style tables
- Amazon EMR deployment artifacts
- SQL
- Pytest

## Coding Rules
- Use clear folder structure
- Add docstrings
- Prefer config-driven paths
- Write reusable functions
- Add validation checks for nulls, duplicates, and schema drift
- Do not hardcode credentials
- Use synthetic sample data

## Deliverables
- Working ETL pipeline
- Sample raw data
- Curated tables
- Tests
- README
- EMR run instructions