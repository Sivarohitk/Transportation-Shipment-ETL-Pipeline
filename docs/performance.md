# Scalability review and reproducible benchmark

This is a design and measurement record, not a production-scale performance
claim. The committed synthetic sample is tiny. A real EMR or Glue benchmark
with representative S3 files, cluster sizing, and workload concurrency is
still required.

## What changed, and why

| Area | Observation | Change and boundary |
| --- | --- | --- |
| Region joins | State-to-region is a bounded reference mapping, while shipments and events can be large. The old window sorted duplicate state mappings and the join had no explicit build side. | Use `min(region_code)` for the same deterministic tie-break and broadcast the lookup. A local Spark physical-plan test confirms `BroadcastHashJoin` even with automatic broadcast disabled. This is appropriate only while the reference remains small. |
| Diagnostic actions | Bronze builders counted each frame just to log it. The daily Gold analytics log counted three derived frames immediately before writing them. | Removed those six log-only actions. Required quality, audit, and output-count actions remain; no row-count metric was invented. |
| Redshift physical design | DDL already used `DISTSTYLE AUTO`, but pinned sort keys without a measured warehouse query workload. | New tables use `DISTSTYLE AUTO`, `SORTKEY AUTO`, and `ENCODE AUTO`. Redshift can choose from observed workload. `CREATE TABLE IF NOT EXISTS` does **not** migrate existing tables; inspect and alter existing tables deliberately after real query evidence. |

## Configuration and unresolved costs

- The base Spark configuration enables adaptive query execution (AQE), dynamic
  partition overwrite, and Snappy Parquet. Local/dev use 4 shuffle partitions;
  base and Glue inherit 8; EMR/prod and Databricks configuration currently use 200. These
  are starting settings, **not** measured optima. AQE coalescing and skew-join
  flags are captured by the benchmark script. Do not copy a local value to EMR
  or Glue without a representative run.
- Curated Parquet remains partitioned by `p_date`, `region_code`, and
  `carrier_id`. This supports date/region/carrier pruning, but a single date
  with many carriers can make small files; a dominant carrier can skew tasks.
  We have not forced `repartition()` or `coalesce()` without file-size and
  task-skew evidence. Inspect output file counts/sizes and Spark UI task times
  on a real S3 benchmark before choosing a target file size or compaction job.
- Daily CSV inputs do not benefit from Parquet predicate pushdown. Batch-file
  resolution selects one date before reading. Downstream Parquet consumers
  should filter on physical partition columns, for example
  `spark.read.parquet(path).where(F.col("p_date") == run_date)`, and confirm
  `PartitionFilters` in `explain("formatted")`.
- Quality rules and audit/output row counts still execute Spark actions. The
  daily flow also re-reads raw files for Bronze after initial quality checks,
  preserving existing ingest/quarantine behavior. Caching large frames could
  increase memory pressure; this is a measured future trade-off, not a blanket
  optimization. Silver/Gold builders reuse logical plans and Spark may re-scan
  them for separate writes.
- Production transforms use native Spark SQL/DataFrame expressions, not Python
  UDFs or `toPandas()`. The only `collect()` calls are scalar null-rule
  aggregation and the explicitly Windows-local JSON/CSV write fallback. That
  fallback materializes data in driver memory and is **not** a scale path;
  a benchmark artifact reporting `json_fallback` cannot establish Parquet/S3
  throughput.

## Generate an uncommitted workload

The existing ML generator materializes rows in Python lists. The separate
`transport_etl.synthetic.scale` generator streams one row at a time and uses
O(1) row buffering. It writes schema-compatible CSVs under the ignored
`data/generated/` tree. The data distributions are deliberately simple and
synthetic; they are not representative of a real carrier network.

```powershell
.\.venv\Scripts\python.exe -m transport_etl.synthetic.scale --output-root data/generated/scale-10k --shipments 10000 --events 20000 --carriers 32 --run-date 2026-01-01
.\.venv\Scripts\python.exe -m transport_etl.synthetic.scale --output-root data/generated/scale-100k --shipments 100000 --events 200000 --carriers 64 --run-date 2026-01-01
.\.venv\Scripts\python.exe -m transport_etl.synthetic.scale --output-root data/generated/scale-1m --shipments 1000000 --events 2000000 --carriers 128 --run-date 2026-01-01
```

The 1m option is a supported generation configuration, not a claim that a
1m-row end-to-end run has completed.

## Benchmark one actual daily run

The benchmark invokes the shared daily job and captures its real audit counts.
It disables state-based skipping, Hive registration, Glue, Redshift, and
CloudWatch so the timing covers the local/EMR Spark ETL rather than remote
publication. Elapsed time includes Spark session startup and shutdown;
throughput is **input CSV rows divided by elapsed seconds**, not a warehouse
query rate. `output_rows` sums the five core Gold audit counts and can exceed
shipment count because it includes multiple model tables. The result records
Spark/Python versions, profile, selected effective Spark settings, row counts,
rejections, status, and observed output format.

```powershell
.\.venv\Scripts\python.exe -m transport_etl.benchmark --config dev --run-date 2026-01-01 --raw-base-path data/generated/scale-10k/raw --reference-base-path data/generated/scale-10k/reference --result-json data/generated/benchmark/10k.json
```

On EMR, run the same package module with `spark-submit`, `--config prod`, and
S3 overrides for all five paths; upload the resulting JSON from the driver.
For Glue, adapt the same measurement contract in the Glue job/runtime rather
than claiming this local CLI exercised Glue. Keep generated CSVs and temporary
outputs out of Git. Commit a small JSON result under `docs/results/` only
after the command truly completes and the configuration/output format has
been inspected.

## Explain-plan checks

Use `df.explain(mode="formatted")` before a costly write. For the region join,
expect `BroadcastHashJoin` and a `BroadcastExchange` of the lookup; the
integration test asserts the former. For shipment/event joins, examine
`Exchange` and join type, then compare stage shuffle bytes and skewed task
durations in the Spark UI. For curated Parquet reads filtered by `p_date`,
confirm `PartitionFilters`. An explain plan describes optimizer choice; it
does not prove end-to-end runtime or cost.

The local smoke result in [results/local-smoke-60.json](results/local-smoke-60.json)
was produced by the benchmark command on 60 synthetic shipments and 120
synthetic events. It reports `json_fallback` due the existing Windows Hadoop
write limitation. It proves the harness and audit-count schema, **not**
scalability, Parquet throughput, or EMR/Glue performance.

## Redshift validation still needed

AWS documentation recommends automatic [sort-key](https://docs.aws.amazon.com/redshift/latest/dg/t_Sorting_data.html),
[distribution](https://docs.aws.amazon.com/redshift/latest/dg/t_Distributing_data.html),
and [compression](https://docs.aws.amazon.com/redshift/latest/dg/c_Compression_encodings.html)
selection as the starting point. Validate `COPY`, `MERGE`, query plans,
distribution/skew, storage, and representative date/carrier filters in a real
warehouse before pinning a manual key or encoding. Redshift informational
primary keys are not enforced; the pipeline must maintain source uniqueness.
