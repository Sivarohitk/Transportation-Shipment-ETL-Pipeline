# AGENTS.md

## Project

**Supply Chain Transportation Lakehouse & Decision Support**

A production-style supply chain analytics platform built on PySpark, Spark SQL, and a Bronze/Silver/Gold medallion lakehouse architecture. The pipeline ingests shipment, carrier, and delivery-event operational data; enforces schema contracts and data quality; builds curated dimensional and fact models; computes carrier and route performance KPIs; and publishes Power BI-ready Gold outputs.

The platform targets three execution environments:
- **Local** — development and testing with synthetic sample data
- **Amazon EMR** — batch processing on S3-backed Parquet/Hive tables
- **Databricks** — incremental Delta Lake processing with Unity Catalog and Lakeflow Jobs orchestration

Databricks is an **additional** execution target. It does not replace local or EMR behavior.

## Goals

- Ingest shipment, carrier, and delivery-event data from CSV sources
- Enforce explicit schema contracts; quarantine invalid records
- Clean, deduplicate, and validate records at each layer
- Apply Bronze/Silver/Gold medallion architecture with documented data lineage
- Build curated dimensional, fact, and aggregate models
- Partition outputs by `p_date`, `region_code`, and `carrier_id`
- Output Parquet datasets (local and EMR) and Delta tables (Databricks)
- Compute carrier performance, route performance, and delivery exception KPIs
- Produce Power BI-ready Gold outputs
- Support incremental and idempotent processing; use Delta MERGE/upsert for changing operational records on Databricks
- Provide a late-shipment risk model with chronologically valid ML evaluation (future phase)
- Document DMAIC process-improvement case study (future phase)
- Support Lakeflow Jobs orchestration and Declarative Automation Bundles (future phase)
- Include meaningful tests and documentation
- Keep code modular, production-style, and explainable in an interview

## Tech Stack

- Python
- PySpark
- Spark SQL
- Hive-style tables (local and EMR)
- Delta Lake (Databricks)
- Unity Catalog-compatible `catalog.schema.table` naming (Databricks)
- Amazon EMR deployment artifacts
- Databricks Asset Bundles / Lakeflow Jobs (future phase)
- SQL
- Pytest

## Architecture: Medallion Layers

### Bronze
- Raw ingestion from CSV sources
- Explicit schema enforcement from `config/schemas/*.schema.json`
- Row-level validation; invalid records quarantined to a dedicated path, never silently discarded
- Append-only or overwrite depending on execution target
- Maps to existing `src/transport_etl/ingest/` modules

### Silver
- Deduplication, standardization, and region enrichment
- Data quality checks: schema drift, required nulls, duplicate business keys, allowed values, timestamp ordering, non-negative metrics
- Staging SQL views: `stg_shipments`, `stg_carriers`, `stg_delivery_events`
- Maps to existing `src/transport_etl/quality/` and `src/transport_etl/transform/` modules

### Gold
- Curated dimensional and fact models: `dim_carrier`, `fct_shipment`, `fct_delivery_event`
- Aggregate models: `agg_shipment_daily`
- KPI outputs: `kpi_delivery_daily` (carrier performance, route performance, delivery exception metrics)
- Power BI-ready outputs at `(p_date, region_code, carrier_id)` grain
- Maps to existing `src/transport_etl/transform/build_*.py` modules and `sql/kpi/`

## Unity Catalog Naming Convention (Databricks only)

Tables are named using the three-level Unity Catalog convention:

```
{catalog}.{schema}.{table}
```

Example:
```
supply_chain.bronze.raw_shipments
supply_chain.silver.stg_shipments
supply_chain.gold.fct_shipment
supply_chain.gold.kpi_delivery_daily
```

Catalog, schema, and table names are always **configuration-driven**. They must never be hardcoded. On local and EMR targets, the existing `hive.database` convention (`curated.fct_shipment`) is preserved unchanged.

## Coding Rules

1. **Preserve existing local and EMR behavior** unless a later prompt explicitly changes it. Do not alter how local or EMR execution works when adding Databricks support.
2. **Never globally replace Parquet with Delta.** Parquet remains the default format for local and EMR targets. Delta is used only when the execution target is explicitly Databricks.
3. **Databricks is an additional target.** All new Databricks-specific code paths must be gated behind a `databricks` Spark profile or an equivalent config flag. Local and EMR code paths must remain independently functional.
4. **No hardcoded credentials, tokens, hosts, account IDs, workspace URLs, or cloud resource IDs.** All environment-specific values must come from configuration files, environment variables, or runtime parameters.
5. **Prefer existing modules and abstractions over duplicated code.** Extend `common/spark.py`, `common/io.py`, `publish/catalog.py`, and `publish/hive_writer.py` before creating new equivalents.
6. **Keep paths, catalog names, schema names, and table names configuration-driven.** No string literals for storage paths or catalog identifiers outside of `config/` YAML files or `common/constants.py`.
7. **Invalid records must be quarantined or reported, never silently discarded.** Quarantine paths must be written to a configured location and logged with entity name, rule name, and row count.
8. **New functionality requires meaningful tests.** Unit tests for pure logic; integration tests for Spark-dependent behavior. Tests must pass without requiring a live Databricks workspace.
9. **Do not fake Databricks integration tests.** Tests that exercise Databricks-specific APIs must be clearly marked to skip when a live Databricks connection is unavailable. Do not mock Databricks in a way that makes the test meaningless.
10. **Do not claim successful Databricks deployment until it has actually been run in a workspace.** Document deployment status honestly in relevant docs.
11. **Do not fabricate model metrics.** ML evaluation results must come from actual model runs on held-out data. Report train/test split strategy, date of evaluation split, and all standard metrics. Do not invent or round-up numbers.
12. **Synthetic and demo data must be clearly identified as synthetic** in code comments, data files, and documentation. Never present synthetic data as production data.
13. **Do not add Kafka, Airflow, dbt, Kubernetes, Terraform, Snowflake, BigQuery, Redshift, LLMs, RAG, agents, or vector databases.** These are out of scope for this project.
14. **Do not add unnecessary files, abstractions, or technologies.** Every new file must serve a clearly stated purpose. Prefer extending existing modules.
15. **Keep the repository production-style and explainable in an interview.** Code must be readable, documented with docstrings, and structured so that any data engineering or data science interviewer can follow the logic from raw ingestion to final KPI output.

## Deliverables

- Working ETL pipeline with local, EMR, and Databricks execution paths
- Synthetic sample data clearly labeled as synthetic
- Curated Bronze, Silver, and Gold tables
- Documented data lineage across all layers
- KPI outputs: carrier performance, route performance, delivery exception analysis
- Late-shipment risk model with chronologically valid evaluation (future phase)
- DMAIC process-improvement case study (future phase)
- Lakeflow Jobs workflow definition (future phase)
- Declarative Automation Bundle configuration (future phase)
- Power BI data model documentation
- Meaningful tests (unit, integration, data quality, SQL)
- README with quickstart, architecture diagram, and execution instructions for all targets
- EMR run instructions
- Databricks deployment instructions (once workspace-validated)
