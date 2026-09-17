# AWS Glue Spark execution (deployment guide)

This is an **optional execution adapter**, not another ETL implementation.
`job_entrypoint.py` creates a Glue-managed `GlueContext`/`SparkSession`, then
calls the existing `transport_etl.jobs.run_daily_batch.run_daily_batch` with
that session. The daily runner still owns ingestion, Bronze/Silver/Gold
transforms, data quality, state, audit, and optional Glue Data Catalog and
Redshift publication. It does not stop Glue's session. Glue-only imports are
inside the runtime factory; local imports and tests need no `awsglue` package.

The code has been tested locally with fake Glue runtime seams. **No real AWS
Glue run has been performed or claimed.**

## Artifacts and runtime

Use an AWS Glue **Spark ETL** job, not a Python-shell job. Glue 5.0 is the
recommended first smoke-test target: AWS documents Spark 3.5.4 and Python
3.11 for that runtime. This differs from the verified local Spark 3.5.2 /
Python 3.12 environment, so the live smoke test must confirm compatibility.
[AWS Glue version table](https://docs.aws.amazon.com/glue/latest/dg/release-notes.html).

Run from the repository root:

```powershell
.\.venv\Scripts\python.exe deploy/glue/build_artifacts.py --output-dir dist/glue
```

The deterministic builder creates:

- `dist/glue/transport_etl_code.zip`: Python files from `src/transport_etl`, for
  Glue's `--extra-py-files`. It contains no virtual environment or test data.
- `dist/glue/transport_etl_resources.zip`: `config/` YAML/schema/profile files
  and `sql/` files, for Glue's `--extra-files`. The entrypoint validates and
  extracts it on the driver; `runtime.resource_base_path` then points to those
  files. Redshift bootstrap SQL uses the same resource root when enabled.

Upload the two zips and `deploy/glue/job_entrypoint.py` to an artifact prefix
in S3. Glue loads the Python zip from `--extra-py-files` and downloads the
resource zip through `--extra-files`; those are the documented Glue Spark job
parameters. [AWS job arguments](https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-glue-arguments.html),
[Python library packaging](https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-python-libraries.html).

The ETL imports PyYAML. Set `--additional-python-modules` to pinned
`PyYAML==6.0.2` for the smoke test, or supply a compatible wheel from S3 in
a network-restricted environment. Do not install a second PySpark into Glue.
Keep Redshift disabled for the first smoke run; enable it only after the
warehouse, IAM role, and credentials are provisioned. Job arguments and logs
must never contain a password or access key. A Redshift secret **ARN** may be
passed as a parameter, but the secret value remains in Secrets Manager.

## One manual AWS smoke test, in order

Replace every angle-bracket placeholder before using a command. Use a
non-production account/bucket and the repository's **synthetic** sample CSVs.
The job role, bucket policy, region, and optional KMS policy must agree.

1. Create or select private S3 artifact, data, and temporary prefixes. Upload
   the four synthetic inputs:

   ```text
   data/sample/raw/shipments_2026-01-01.csv       -> s3://<DATA_BUCKET>/transport/raw/
   data/sample/raw/carriers_2026-01-01.csv         -> s3://<DATA_BUCKET>/transport/raw/
   data/sample/raw/delivery_events_2026-01-01.csv  -> s3://<DATA_BUCKET>/transport/raw/
   data/sample/reference/region_lookup.csv         -> s3://<DATA_BUCKET>/transport/reference/
   ```

2. Build the two artifacts with the command above. Upload these exact files:

   ```powershell
   aws s3 cp deploy/glue/job_entrypoint.py s3://<ARTIFACT_BUCKET>/transport-etl/glue/job_entrypoint.py
   aws s3 cp dist/glue/transport_etl_code.zip s3://<ARTIFACT_BUCKET>/transport-etl/glue/transport_etl_code.zip
   aws s3 cp dist/glue/transport_etl_resources.zip s3://<ARTIFACT_BUCKET>/transport-etl/glue/transport_etl_resources.zip
   ```

3. Create a Glue service role trusted by `glue.amazonaws.com`. Grant only the
   scoped actions below, plus access to the named S3/KMS resources. The
   operator creating/running the job needs separate `glue:CreateJob`,
   `glue:StartJob`, `glue:GetJobRun`, and `iam:PassRole` permissions scoped to
   this job/role. [AWS Glue IAM setup](https://docs.aws.amazon.com/glue/latest/dg/configure-iam-for-glue.html).

4. Copy `example_job_parameters.json` to a private, untracked deployment
   location and replace the bucket, region, and database placeholders. Keep
   `--redshift_enabled=false` initially. The file is a Glue
   `DefaultArguments` JSON object; it intentionally omits `--JOB_NAME`, which
   Glue supplies. Set all five S3 path arguments to real prefixes: the
   entrypoint rejects `s3://your-bucket` and unresolved placeholders before
   starting Spark. Set `--glue_enabled=true`, `--glue_region`, and
   `--glue_database` to exercise catalog registration.

5. Create a Glue **Spark** job with Glue 5.0, Python 3, script location
   `s3://<ARTIFACT_BUCKET>/transport-etl/glue/job_entrypoint.py`, the service
   role, a bounded timeout, and `DefaultArguments` from the edited JSON file.
   Start with a small worker allocation appropriate to the synthetic data.
   Use the console or the CLI, for example:

   ```text
   aws glue create-job --name <JOB_NAME> --role <GLUE_ROLE_ARN> --glue-version 5.0 --command Name=glueetl,ScriptLocation=s3://<ARTIFACT_BUCKET>/transport-etl/glue/job_entrypoint.py,PythonVersion=3 --worker-type G.1X --number-of-workers 2 --timeout 30 --default-arguments file://<EDITED_PARAMETERS_JSON>
   ```

6. Start one run with `--run_date=2026-01-01` (already present in the example
   defaults, or override it on the run). Record its JobRunId. Wait for
   `SUCCEEDED`; `FAILED` is not a successful smoke test. The entrypoint calls
   `job.commit()` only after the shared daily runner returns zero. Use
   `aws glue get-job-run --job-name <JOB_NAME> --run-id <JOB_RUN_ID>` or the
   Glue console to inspect status. Follow the job-run links to CloudWatch
   `/aws-glue/jobs/output` and `/aws-glue/jobs/error` logs.
   [AWS job-run logs](https://docs.aws.amazon.com/glue/latest/dg/view-job-runs.html).

7. Verify S3 Gold outputs under `curated/dim_carrier/`, `fct_shipment/`,
   `fct_delivery_event/`, `agg_shipment_daily/`, and `kpi_delivery_daily/`.
   Inspect the run audit JSON under `audit/pipeline_audit/` and the date's
   `audit/pipeline_state/batches/2026-01-01.json`; it must say `success`.
   Inspect source/clean/rejected row counts. Check staging quarantine prefixes
   under `staging/quarantine/{ingest,quality,silver}/`; clean synthetic inputs
   may produce no quarantine objects, which is not itself a failure.

8. If catalog registration was enabled, inspect `<GLUE_DATABASE>` and the
   five Gold tables, S3 locations, schema columns, and registered partitions
   in the Glue Data Catalog (console or `aws glue get-table` /
   `get-partitions`). The job uses the existing boto3 catalog adapter after
   curated S3 publication; Spark Hive registration remains disabled in this
   Glue profile.

9. Optional second test: provision Redshift and its COPY role, then set
   `--redshift_enabled=true` with region, database, exactly one workgroup or
   cluster identifier, a Secrets Manager secret ARN or supported database
   user, COPY role ARN, and Redshift-ready S3 prefix. Run the same date with
   `--force=true` if the prior success checkpoint would skip it. Verify the five
   analytical tables and `etl_load_audit` in Redshift. The Glue job uses the
   existing Redshift Data API publisher; it does not use a JDBC connection.
   No Redshift result is claimed until that test is actually performed.

10. Start the same date again without `--force=true`. Verify the successful
    file-manifest checkpoint causes a deterministic skip, no new Gold load,
    and a skipped audit record. Finally delete the disposable Glue job,
    synthetic objects, artifact/temp prefixes, optional test catalog/database
    and Redshift objects, and any test-only IAM role/policy. Confirm the
    intended objects before deletion.

## IAM actions to scope by purpose

Scope S3 permissions to the exact buckets/prefixes and Glue catalog actions
to the intended catalog/database/tables where the action supports it. This is
an action inventory, **not** an unrestricted policy or a real ARN template.
The final policy also depends on bucket encryption and network design.
[AWS least-privilege job guidance](https://docs.aws.amazon.com/glue/latest/dg/getting-started-min-privs-job.html).

| Purpose | Service actions for the Glue execution role |
| --- | --- |
| Read script/zips/raw/reference | `s3:ListBucket`, `s3:GetObject` |
| Write staging, curated, quarantine, audit/state, TempDir | `s3:ListBucket`, `s3:GetObject`, `s3:PutObject`, `s3:DeleteObject` for overwrite/cleanup; multipart actions only if the selected S3 committer requires them |
| Glue-managed logs | `logs:CreateLogGroup`, `logs:CreateLogStream`, `logs:PutLogEvents` (and `logs:DescribeLogStreams` if the logging configuration requires it) |
| Existing optional Data Catalog adapter | `glue:GetDatabase`, `glue:CreateDatabase`, `glue:GetTable`, `glue:CreateTable`, `glue:UpdateTable`; with partition registration, `glue:BatchCreatePartition`, `glue:UpdatePartition` |
| Optional application metrics | `cloudwatch:PutMetricData`, constrained to the configured namespace |
| Optional Redshift Data API | `redshift-data:BatchExecuteStatement`, `redshift-data:DescribeStatement`, `redshift-data:CancelStatement`; `redshift-data:ExecuteStatement` only if the single-statement helper is used |
| Redshift Secrets Manager authentication, if selected | `secretsmanager:GetSecretValue` on the selected secret; required database/serverless authentication rights depend on the selected mode |
| SSE-KMS, if configured | `kms:Decrypt`, `kms:Encrypt`, `kms:GenerateDataKey` on the specific key, as needed for those S3/secret resources |

Do not grant `redshift-data:GetStatementResult` solely for this pipeline: the
current publisher does not call it. Redshift's **COPY IAM role is separate**
from the Glue execution role and needs read access to the Redshift-ready S3
prefix (`s3:ListBucket`, `s3:GetObject`, and KMS decrypt if encrypted). Restrict
the database user's SQL rights to the configured staging/analytics/audit
schemas. [Redshift Data API IAM guidance](https://docs.aws.amazon.com/redshift/latest/mgmt/data-api-iam.html).

## Validation boundary

Local tests prove parsing, resource-archive safety, artifact contents,
external-session ownership, call ordering, failure propagation, and the
shared daily runner seam. They do **not** prove Glue runtime compatibility,
S3 permissions, PyYAML installation, Data Catalog visibility, Redshift IAM,
or production sizing. Record the JobRunId and observed artifacts after the
manual run before claiming AWS validation.
