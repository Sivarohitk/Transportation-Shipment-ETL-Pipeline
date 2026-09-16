# Windows local development setup

The supported Windows development and test baseline is:

- Python 3.12.x
- PySpark 3.5.2
- JDK 17
- A project-local `.venv`

Keep other installed Python and Java versions if they are needed by other
projects. These instructions change only the current PowerShell process; they
do not edit machine-wide environment variables.

## Automated setup

From the repository root, inspect the available Python installations and run
the bootstrap script:

```powershell
py -0p
.\scripts\setup_windows_dev.ps1
```

The script requires `py -3.12`, creates or reuses `.venv`, installs the
editable project with its development dependencies, locates a JDK 17 in common
Windows install directories, and runs the environment diagnostic. It will not
delete an existing incompatible `.venv`, install Java, uninstall Java 21, or
change system-wide environment variables.

If JDK 17 is not installed, install a JDK 17 distribution such as Eclipse
Temurin 17. After installation, open a new PowerShell window or set the current
shell to the actual installation path:

```powershell
$env:JAVA_HOME = "<actual JDK17 path>"
$env:PATH = "$env:JAVA_HOME\bin;$env:PATH"
.\scripts\setup_windows_dev.ps1
```

## Manual setup

Run these commands from the repository root:

```powershell
py -0p
py -3.12 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -e ".[dev]"

$env:JAVA_HOME = "<actual JDK17 path>"
$env:PATH = "$env:JAVA_HOME\bin;$env:PATH"
```

Verify that every command resolves through the intended environment:

```powershell
python --version
python -c "import pyspark; print('PySpark:', pyspark.__version__)"
java -version
python -m pytest --version
.\scripts\check_environment.ps1
```

The diagnostic exits with a nonzero status when Python is not 3.12, PySpark is
missing or not 3.5.2, Java is not 17, `.venv` is inactive, or pytest resolves
outside `.venv`.

PySpark 3.5.2 has an upstream Windows simple-worker flush defect with Python
3.12 (SPARK-53759). The local Spark profile and test fixture select the
project's narrow worker-entrypoint backport automatically; local and EMR
business logic is unchanged.

## Test and quality commands

Always invoke Python tools as modules so they use the same interpreter:

```powershell
python -m pytest -q
python -m ruff check .
python -m black --check .
```

Focused Spark checks:

```powershell
python -m pytest tests/unit/test_null_rules.py -q
python -m pytest tests/unit/test_duplicate_rules.py -q
python -m pytest tests/integration/test_silver_layer.py -q
python -m pytest tests/integration/test_hive_partition_writes.py -q
```

Do not diagnose Spark fixture or ETL behavior until
`scripts/check_environment.ps1` reports a passing environment.
