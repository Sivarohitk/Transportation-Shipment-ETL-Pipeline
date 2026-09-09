# Databricks deployment

This project adds Databricks as an execution target without changing the
local or Amazon EMR paths. Local and EMR continue to use Parquet and the
existing Hive-compatible behavior. Databricks uses a Declarative Automation
Bundle, serverless Lakeflow Jobs, managed Delta tables, and Unity Catalog
Volumes for file-based inputs and non-table outputs.

## Phase 10 live validation status

The development bundle was validated, deployed, and run end to end on
2026-09-08 in a Databricks Free Edition serverless workspace. The final
three-task Lakeflow run completed successfully, and each task had its own
`SUCCESS` result:

```text
ingest_bronze_silver_gold  SUCCESS
score_late_risk            SUCCESS
data_quality_checks        SUCCESS (11 passed)
```

The run produced and queried 14 managed Delta tables: 3 Bronze, 3 Silver, and
8 Gold. Observed row counts were 11 shipments, 6 carriers, and 43 delivery
events at Bronze/Silver fact grain; the corresponding Gold facts retained
those counts. All tested business/grain keys were unique after repeated runs.
A deterministic shipment update was applied through the Silver MERGE and
remained present after the final rerun.

The live ML validation used 5,000 deterministic synthetic shipments. All
5,000 were scored, including two deliberately supplied rows with a null route
component. Probabilities were finite and within `[0, 1]`; `risk_band` and
`predicted_late` were populated for every row. Evaluation values in the task
output came from that actual chronological train/validation/test run.

This record describes the Phase 10 workspace run. Phase 15 subsequently
repaired outcome-availability history, Gold-from-Silver lineage, and KPI
cohort alignment. Those changes are locally regression-tested and the bundle
is validated, but this repaired revision has not been redeployed or executed
in the workspace.

## Bundle layout and path rules

The bundle root is `deploy/databricks/`; the repository root is `../..` from
that directory.

```text
deploy/databricks/
|-- databricks.yml
|-- resources/transport_etl_job.yml
|-- targets/dev.yml
|-- targets/prod.yml
`-- dist/
```

Top-level `include` contains only bundle configuration fragments. The Python
application is built as a wheel from the repository root through the current
`artifacts` schema. Runtime configuration, synthetic sample data, staging SQL,
and the existing data-quality tests are uploaded through `sync.paths`.

The artifact configuration is equivalent to:

```yaml
artifacts:
  python_wheel:
    type: whl
    path: ../..
    build: python -m pip wheel --no-deps --wheel-dir deploy/databricks/dist .
```

This preserves the package and console entry points declared in the root
`pyproject.toml`; `deploy/databricks/` is not treated as a Python project.

The wheel intentionally does not contain repository configuration, schema
JSON, SQL, or test files. The ETL task therefore passes both
`--config-dir ${workspace.file_path}/config` and
`--resource-base-path ${workspace.file_path}`. The first makes
`databricks.yaml` merge with the synced `base.yaml`, including its logging
configuration. The second resolves schema JSON and the staging SQL directory
from the synced bundle root instead of from the installed wheel. Local and EMR
omit these flags and retain the existing repository-root defaults.

The data-quality entry point runs pytest in the wheel-task process so its
fixtures can reuse the serverless managed Spark session. It receives the same
resource root through `TRANSPORT_ETL_RESOURCE_BASE_PATH`, disables cache and
bytecode writes beside read-only Workspace Files, and uses a configured UC
Volume path for test quarantine writes. A nonzero pytest result raises from
the entry point and therefore fails the Lakeflow task. Curated, Gold, KPI, and
quality SQL files are documentation/test assets in the current pipeline; the
running daily job only reads the three files under `sql/staging/`.

## Serverless Lakeflow Job

Databricks Free Edition is serverless-only. The job therefore has no
`job_clusters`, `new_cluster`, `node_type_id`, `spark_version`,
`data_security_mode`, or manually provisioned compute setting. Each Python
wheel task references the job's `serverless` environment with
`environment_version: "4"`, as supported by the installed CLI schema.

The task dependency graph remains:

```text
ingest_bronze_silver_gold
        |
        v
score_late_risk
        |
        v
data_quality_checks
```

All tasks invoke console scripts from the existing wheel. No ETL logic is
copied into notebooks.

## Storage and Unity Catalog

Bronze, Silver, and Gold outputs use managed Delta tables. Catalog and schema
names are bundle variables and are forwarded to the ETL CLI as application
configuration overrides:

- `catalog_name`
- `bronze_schema`
- `silver_schema`
- `gold_schema`

The development defaults use the Free Edition `workspace` catalog and
separate `bronze_dev`, `silver_dev`, and `gold_dev` schema names. Production
uses `bronze`, `silver`, and `gold`. These objects must already exist before a
job can run; the bundle does not create or delete them.

File-based inputs, quarantined records, audit output, and the existing
file-based ML interface use paths shaped as:

```text
/Volumes/${catalog_name}/${file_schema}/${volume_name}/raw
/Volumes/${catalog_name}/${file_schema}/${volume_name}/reference
/Volumes/${catalog_name}/${file_schema}/${volume_name}/staging
/Volumes/${catalog_name}/${file_schema}/${volume_name}/audit
/Volumes/${catalog_name}/${file_schema}/${volume_name}/ml
```

The defaults name `workspace.default.transport_etl`, but the Volume is a
prerequisite, not an assumption: override `file_schema` and `volume_name` when
using a different existing Volume. The bundle does not require DBFS mounts,
external locations, storage credentials, S3 buckets, or Azure storage.

The `curated_base_path` variable remains for compatibility with the shared
local/EMR pipeline interface. On the Databricks Delta path, managed table
writers ignore that path and call `saveAsTable` with a configured Unity
Catalog identifier.

## Authentication

Use Databricks CLI OAuth and a named local profile. Never place the workspace
host, profile name, username, token, or other credentials in bundle files.

```powershell
databricks auth login --host https://<workspace-host> --profile <profile>
databricks auth profiles
databricks current-user me --profile <profile>
```

The selected profile supplies the workspace host and OAuth credentials. A
personal access token is not required by this workflow.

## Validate without deploying

Run validation from the bundle root:

```powershell
Set-Location deploy/databricks
databricks bundle validate --target dev --profile <profile>
```

Validation is read-only with respect to bundle resources. Do not substitute
`bundle deploy`, `bundle run`, or `jobs run-now` when performing the Phase 9.1
validation gate.

## Prerequisites for another deployment or run

An operator must complete and verify these items manually:

1. Select an existing writable Unity Catalog catalog.
2. Create or select the configured Bronze, Silver, and Gold schemas.
3. Create or select the configured Unity Catalog Volume.
4. Upload the clearly labeled synthetic CSV inputs and region reference file
   under the configured `raw` and `reference` directories.
5. Provide a CSV at `ml_shipments_path` for the existing file-based ML CLI.
6. Confirm that serverless environment version 4 contains or can install the
   wheel dependencies from `pyproject.toml`.
7. Re-run bundle validation with the intended target and profile.

The Phase 10 development validation satisfied these prerequisites in its test
workspace. Other targets and workspaces must provision their own objects and
inputs before deployment or execution.

## Honest runtime limitations

- The late-risk CLI currently consumes a CSV, while the ETL Gold layer writes
  a managed Delta table. The bundle keeps `ml_shipments_path` configurable;
  producing that CSV from the managed fact table remains a manual prerequisite
  until a separate, tested integration is implemented.
- The data-quality task runs the repository's existing synthetic
  `tests/data_quality/` suite. It does not yet query the newly written Unity
  Catalog tables as a post-run reconciliation suite.
- Production-target deployment, scheduling, and production-data behavior were
  not validated; the verified live execution used the development target and
  clearly labeled synthetic data.

## Local and EMR behavior

No local or EMR storage setting was changed. Continue using the existing local
and EMR commands and configuration files documented in the project README.
Databricks-specific Delta, Unity Catalog, Volume, and serverless settings are
gated by the Databricks profile and bundle target.
