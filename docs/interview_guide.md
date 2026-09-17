# Interview guide: transportation ETL

## Business problem and outputs

Shipment, carrier, and delivery-event files arrive at different grains and can contain invalid or repeated records. The daily job turns them into shipment and event facts, a dated carrier dimension, and daily carrier/region KPIs. Analysts can compare delivery timeliness, exceptions, transit, distance, and cost using the [Gold models](curated_tables.md) and [metric definitions](kpi_definitions.md). The sample files are synthetic; there is no deployed customer report.

## Architecture decisions

The pipeline separates raw preservation (Bronze), validation and latest-record selection (Silver), and reporting models (Gold). [`run_daily_batch.py`](../src/transport_etl/jobs/run_daily_batch.py) shares ingestion, quality, and transforms across local, EMR-oriented, and Glue Spark entrypoints. Publication differs by profile: Parquet/Hive-compatible local and AWS paths, managed Delta tables on Databricks. The Databricks development workflow had an earlier live synthetic run, but the current repaired revision has not been rerun there. AWS paths have only local and fake-client tests.

### Why Spark and Spark SQL?

PySpark DataFrames handle schema enforcement, validation, deduplication, joins, and aggregations in one distributed API. The staging and KPI SQL files make relational transformations reviewable separately from orchestration. Spark is appropriate for files that may outgrow one machine, but the checked-in workload is small and no EMR/Glue scale result exists. A simpler single-node implementation could be cheaper for the current sample size.

### Why Parquet and partitioning?

The local/EMR/Glue target defaults to Snappy-compressed Parquet. Columns can be read selectively, and date/region/carrier partitions allow readers to avoid unrelated directories when they filter on those keys. The tradeoff is many small files or skew when a few carriers dominate. [`docs/performance.md`](performance.md) records the settings and measurements still needed. The Windows smoke artifact used a JSON fallback, so it does not measure Parquet throughput. Databricks uses Delta rather than replacing the Parquet path.

### Why Glue Data Catalog and Redshift?

Spark Hive registration remains available for local/EMR use. The optional [`GlueCatalogAdapter`](../src/transport_etl/publish/glue_catalog.py) registers five S3 Gold Parquet tables and partitions for AWS discovery; it is separate from the Glue job runtime. The optional [`Redshift publisher`](../src/transport_etl/publish/redshift.py) exports Redshift-ready Parquet, issues Data API `COPY` into staging, and MERGEs keyed rows into analytical tables defined in [`sql/redshift/`](../sql/redshift/003_create_final_tables.sql). Redshift is useful when warehouse SQL and reporting access are required, but introduces another copy, IAM setup, and reconciliation work. Neither integration has been executed in AWS.

## Incremental strategy and reruns

The batch date resolves dated shipment, carrier, and delivery-event files. [`pipeline_state.py`](../src/transport_etl/common/pipeline_state.py) stores their paths and fingerprints, source modification metadata, processing configuration fingerprint, run ID, status, and output locations. An identical successful batch skips; a changed source/configuration or prior failure retries; `--force` reprocesses a success. Backfill applies this check date by date. Local state uses atomic replacement; S3 uses a checksummed object. Success is recorded only after the ETL, required audit write, and configured critical publishers complete. The process needs external serialization for concurrent runs of the same date. It is not event-time watermarking, database CDC, streaming, or row-level change capture.

Idempotency has limits by target. Silver resolves duplicate business keys deterministically. Databricks Silver uses keyed Delta MERGE; local/EMR/Glue Parquet publication uses deterministic overwrite/dynamic-partition behavior. Redshift uses staging plus keyed MERGE in a Data API transaction, tested locally with fake clients. An AWS rerun has not yet verified the Redshift contract, and run/audit timestamps need not be byte-identical.

## Failure handling and dataset quality

JSON schema contracts, drift checks, required-field checks, allowed values, non-negative values, timestamp order, and duplicate handling run before publication. Invalid and superseded rows go to configured quarantine paths with rule context. Configured blocking failures stop the run rather than marking its checkpoint successful. [`aws_retry.py`](../src/transport_etl/common/aws_retry.py) retries classified transient Glue, Redshift, CloudWatch, and S3 requests with bounded backoff. It does not retry invalid credentials, authorization, SQL syntax, Spark transformations, or quality failures. Glue registration has an explicit `fail`/`warn` policy; `warn` is non-critical and requires `--force` for a later retry after a successful batch.

[`monitor/audit.py`](../src/transport_etl/monitor/audit.py) persists local or S3 JSON run records with counts, duration, publish status, and sanitized errors. [`monitor/metrics.py`](../src/transport_etl/monitor/metrics.py) can send CloudWatch metrics with Job/Environment/Entity dimensions; run IDs are excluded from metric dimensions. Failure audit is attempted even if a stage fails. CloudWatch is disabled locally and is best-effort, not an exactly-once ledger. No AWS dashboard or alert has been validated.

## Scaling considerations and tradeoffs

The code uses native Spark operations, a broadcast candidate for the small region lookup, adaptive execution, configurable shuffle partitions, and Parquet compression. The [performance review](performance.md) identifies repeated actions already removed and remaining shuffle, skew, and small-file risks. A synthetic generator can produce larger CSV fixtures, but only a small local smoke artifact is checked in. The next credible scale claim would require a representative EMR or Glue benchmark recording input size, Spark plan/stage metrics, S3 output files, duration, cost, and data-quality reconciliation. Redshift sort/distribution choices currently use automatic settings until a real query workload can justify tuning.

## What would change at Amazon-scale?

This is a design discussion, not a claim that the project handles that volume. First measure representative workload size and skew. Then size cluster/serverless capacity, tune partitions and file sizes from Spark UI and S3 evidence, plan compaction and table maintenance, isolate late-arriving corrections, add concurrency control to batch state, and establish operational SLOs, alerting, IAM reviews, recovery drills, and warehouse reconciliation. Depending on upstream systems, a real change feed or event-time window might replace file manifests. None of those production controls is demonstrated by the local smoke result.

## Honest limitations

- AWS deployment, permissions, Glue catalog discovery, Redshift COPY/MERGE, CloudWatch delivery, and EMR/Glue performance remain manually unvalidated.
- The dated file manifest is not CDC or an event-time watermark. Concurrent same-date runs require external coordination.
- The Power BI document is a build specification, not a PBIX, published semantic model, or scheduled report.
- The late-shipment model has weak held-out discrimination on synthetic data; its scorer reads a configured CSV rather than Gold directly. It is not ready for autonomous decisions.
- No production data, customers, employment, Amazon-scale throughput, or measured business improvement is claimed.
