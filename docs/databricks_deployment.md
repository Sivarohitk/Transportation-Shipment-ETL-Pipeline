# Databricks Deployment (Phase 9)

This document describes how to deploy the **Transport Shipment ETL**
to a Databricks workspace using a **Lakeflow Job** orchestrated by
a **Declarative Automation Bundle** (formerly "Databricks Asset
Bundles" / DAB).

> **Status (Phase 9 commit).**  The bundle and job YAML are
> committed and validated by the test suite in
> ``tests/databricks/``.  The **Databricks CLI is not installed in
> this development environment**, so ``databricks bundle validate``
> was not executed.  Validation **must** be performed by an operator
> with a configured Databricks CLI before any ``databricks bundle
> deploy`` command is run.  See
> [Required manual commands](#required-manual-commands).

## Bundle layout

```
deploy/
└── databricks/
    ├── databricks.yml                      # top-level bundle config
    ├── resources/
    │   └── transport_etl_job.yml           # Lakeflow job definition
    ├── targets/
    │   ├── dev.yml                         # dev-target overrides
    │   └── prod.yml                        # prod-target overrides
    ├── dist/                                # wheel build output (git-ignored)
    └── .gitignore                           # dist/ is excluded
```

The wheel is built locally by ``python.build_commands`` in
``databricks.yml`` (``pip wheel --no-deps --wheel-dir dist .``) and
is uploaded to the workspace by ``databricks bundle deploy``.

## Terminology note

"Databricks Asset Bundles" was renamed to
"**Declarative Automation Bundles**" in the Databricks product
documentation.  The YAML schema in this repository is unchanged;
only the marketing name moved.  This is the current terminology
the operator will see in the Databricks CLI output.

## Lakeflow Job DAG

The job runs **three sequential tasks** in a single Lakeflow Job:

```
ingest_bronze_silver_gold  (transport-etl, --job daily)
        │
        ▼
score_late_risk            (transport-etl-ml, train-and-score)
        │
        ▼
data_quality_checks        (pytest -q tests/data_quality/)
```

### Why one job with three tasks (not three separate jobs)

A single Lakeflow Job keeps the dependencies explicit and lets the
operator re-run a backfill in a single click.  All three tasks
share the same on-demand job cluster (``transport_etl_cluster``)
so there is no cluster-provisioning overhead between tasks.  The
cluster is torn down on completion; the next run creates a fresh
one.

### Task details

| Task | Entry point | What it does |
|---|---|---|
| ``ingest_bronze_silver_gold`` | ``transport-etl --job daily`` | Ingest today's CSVs, validate against the explicit schemas, and write Bronze + Silver + Gold tables.  This is the existing ``run_daily_batch`` end-to-end flow. |
| ``score_late_risk`` | ``transport-etl-ml train-and-score`` | Train the late-risk model on the most recent historical shipments, score today's shipments, and emit a decision-support frame with ``risk_probability`` / ``risk_band`` / ``predicted_late``. |
| ``data_quality_checks`` | ``sh -lc pytest -q tests/data_quality/`` | Run the data-quality test suite against the freshly-written Silver and Gold tables.  Failures surface as Lakeflow task failures. |

### Task dependencies

- ``ingest_bronze_silver_gold`` → no predecessor.
- ``score_late_risk`` → depends on ``ingest_bronze_silver_gold``.
- ``data_quality_checks`` → depends on both upstream tasks.

### Cluster configuration

The job uses a single on-demand cluster with:

- ``spark_version: 14.3.x-scala2.12`` (Databricks Runtime LTS that
  ships PySpark 3.5.x).
- ``num_workers: 0`` (single-node; the dataset is small).
- ``data_security_mode: SINGLE_USER`` (the documented
  production setting for Lakeflow jobs).
- The runtime Spark conf mirrors ``config/spark/databricks.conf``
  in the repository.

## Why no notebook copies of the pipeline

AGENTS.md rule 1 forbids duplicating the existing local/EMR
behaviour.  The bundle invokes the existing
``transport_shipment_etl`` Python wheel — every task is a
``python_wheel_task`` that runs the project's own console scripts.
There is **no** ``.ipynb`` file in the bundle.

## Configuration variables (no credentials)

| Variable | Default | Where to override |
|---|---|---|
| ``wheel_path`` | ``./dist/transport_shipment_etl-0.1.0-py3-none-any.whl`` | Re-build with ``pip wheel`` (no override needed) |
| ``etl_entry_point`` | ``transport-etl`` | Fixed; do not change |
| ``ml_entry_point`` | ``transport-etl-ml`` | Fixed; do not change |
| ``config_path`` | ``../config/databricks.yaml`` | Per-target |
| ``raw_base_path`` | ``dbfs:/mnt/transport/<env>/raw`` | Per-target |
| ``reference_base_path`` | ``dbfs:/mnt/transport/<env>/reference`` | Per-target |
| ``staging_base_path`` | ``dbfs:/mnt/transport/<env>/staging`` | Per-target |
| ``curated_base_path`` | ``dbfs:/mnt/transport/<env>/curated`` | Per-target |
| ``audit_base_path`` | ``dbfs:/mnt/transport/<env>/logs`` | Per-target |
| ``spark_profile`` | ``databricks`` | Fixed; do not change |
| ``hive_database`` | ``curated`` (dev: ``curated_dev``) | Per-target |
| ``ml_decision_threshold`` | ``0.25`` | Per-target |

The **workspace host** and **credential** are intentionally
absent from the YAML.  The Databricks CLI profile
(``databricks configure``) supplies them at deploy time.
The bundle's ``workspace.host`` field is a placeholder
(``https://<your-workspace>.cloud.databricks.com``) that the
operator replaces per target.

## Required manual commands

The following commands must be run by an operator with a
configured Databricks CLI.  The bundle itself does **not** run
them.

### 1. Configure the Databricks CLI

```bash
databricks configure --host https://<your-workspace>.cloud.databricks.com \
    --token <your-personal-access-token>
```

(or use a service-principal profile if your workspace enforces
SCIM-only auth).

### 2. Validate the bundle (run from ``deploy/databricks/``)

```bash
cd deploy/databricks
databricks bundle validate
```

The expected output is a summary of the resolved bundle
configuration.  **The CI test suite cannot replace this step** —
it only verifies the YAML schema and structural contracts.  An
operator must run ``databricks bundle validate`` on a workstation
where the Databricks CLI is installed and authenticated.

### 3. (Optional) Override the workspace host for the target

```bash
databricks bundle validate --target dev \
    --var="workspace_host=https://acme.cloud.databricks.com"
```

### 4. Deploy (manual gate)

```bash
cd deploy/databricks
databricks bundle deploy --target dev
```

This is **irreversible on the target workspace** — the bundle
creates the Lakeflow Job, the cluster spec, and uploads the
Python wheel.  Confirm the deploy target with your team before
running it.

### 5. Trigger a run

```bash
databricks bundle run transport_etl_job --target dev
```

(``--target`` is optional; default is the ``dev`` target.)

### 6. Trigger a backfill for a specific date

```bash
databricks bundle run transport_etl_job --target dev --params="run_date=2026-01-15"
```

## Local-equivalent (no Databricks workspace)

When the Databricks CLI is not available (the development
environment for this portfolio project), the pipeline can be
exercised locally with:

```bash
# Daily ETL (Bronze + Silver + Gold)
python -m transport_etl.main --job daily --config config/databricks.yaml \
    --run-date 2026-01-15 \
    --raw-base-path data/sample/raw \
    --staging-base-path data/local/staging \
    --curated-base-path data/local/curated \
    --reference-base-path data/sample/reference \
    --spark-profile databricks \
    --no-register-hive

# ML scoring
python -m transport_etl.ml.cli train-and-score \
    --shipments data/local/curated/fct_shipment \
    --output-dir data/local/scored \
    --model logistic_regression
```

These commands exercise the same Python modules the bundle
invokes; only the cluster / Unity Catalog integration is
Databricks-specific.

## Why ``databricks bundle validate`` is not auto-run in CI

- The Databricks CLI is not installed in this development
  environment.
- Even if it were, ``validate`` requires a configured workspace
  profile (``databricks configure``) which is operator-specific
  and **must not** be committed.
- AGENTS.md rule 9 explicitly forbids mocking Databricks APIs
  in tests.  Auto-running ``validate`` with a stub would be a
  silent fake.
- The test suite in ``tests/databricks/`` asserts the bundle
  schema (YAML, structure, dependencies) which is what
  ``validate`` does mechanically; the workspace-resolution part
  is operator-specific and out of scope.

## What is verified in CI

The CI test suite (``tests/databricks/test_bundle_structure.py``
and ``tests/databricks/test_wheel_build.py``) covers:

- YAML files are syntactically valid and loadable.
- The top-level bundle has the required sections (``bundle``,
  ``include``, ``exclude``, ``variables``, ``targets``, ``python``).
- The Lakeflow Job has exactly the three documented tasks
  (``ingest_bronze_silver_gold``, ``score_late_risk``,
  ``data_quality_checks``).
- Task dependencies match the documented DAG.
- Every task is a ``python_wheel_task`` (no notebook copies).
- The ``entry_point`` values match the ``[project.scripts]``
  block in ``pyproject.toml``.
- No real credentials, no real workspace URLs (only placeholders).
- The ``include`` patterns cover the pipeline source.
- The wheel name in ``databricks.yml`` matches what
  ``pyproject.toml`` produces.
- The ML CLI exposes the ``train-and-score`` subcommand that the
  bundle invokes.
- The Databricks CLI absence is documented (not a silent failure).

## What is **not** verified in CI

- The bundle's actual deploy to a real workspace.
- Cluster startup time, runtime behaviour, or cost.
- Databricks-specific feature compatibility (Unity Catalog
  privileges, runtime versions, etc.).
- The exact ``databricks bundle validate`` exit message.

These require a configured workspace and a human operator.

## Live-Databricks skip policy

There is no test in this repository that auto-runs
``databricks bundle validate``, ``databricks bundle deploy``, or
``databricks jobs run-now``.  The test file
``tests/databricks/test_bundle_structure.py::TestDatabricksCliAvailability``
explicitly records the absence of the CLI and skips; it does **not**
mock or fabricate a successful validation.
