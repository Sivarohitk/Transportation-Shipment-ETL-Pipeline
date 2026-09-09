"""Delta Lake publishing backend for the Databricks execution target.

This module is the Databricks-specific counterpart to ``hive_writer.py``.
It provides managed Delta overwrite/append publishing for Bronze and Gold.
Silver's production MERGE implementation lives in ``silver.merge``.

Design constraints (see AGENTS.md):
- This module must import cleanly in environments without PySpark or
  Databricks.  All Spark-dependent code is inside functions or guarded by
  ``try/except ModuleNotFoundError``.
- Only the Databricks execution target uses Delta.  Parquet remains the
  default for local and EMR.
- No credentials, workspace URLs, or account IDs are referenced here.
- Table names must arrive as pre-resolved ``catalog.schema.table`` strings
  produced by ``common/catalog.py``.
"""

from __future__ import annotations

import logging
from typing import Any

LOGGER = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Write mode helpers — pure logic, no Spark dependency
# ---------------------------------------------------------------------------

# Supported Spark write modes for Delta.
# ``overwrite`` replaces the specified partitions (dynamic partition overwrite).
# ``append``    adds rows without touching existing data.
# Both are safe for incremental processing when used with the right
# ``spark.sql.sources.partitionOverwriteMode=dynamic`` setting.
_SUPPORTED_DELTA_WRITE_MODES = {"overwrite", "append"}


def resolve_delta_write_mode(mode: str) -> str:
    """Return a validated, normalised Delta write mode string.

    Args:
        mode: Caller-supplied write mode (case-insensitive).

    Returns:
        Normalised lowercase mode string.

    Raises:
        ValueError: If ``mode`` is not supported for Delta writes.
    """
    normalised = str(mode).strip().lower()
    if normalised not in _SUPPORTED_DELTA_WRITE_MODES:
        raise ValueError(
            f"Unsupported Delta write mode: '{mode}'. "
            f"Expected one of {sorted(_SUPPORTED_DELTA_WRITE_MODES)}."
        )
    return normalised


# ---------------------------------------------------------------------------
# Delta write — requires PySpark; guarded so the module imports without it
# ---------------------------------------------------------------------------


def write_delta_table(
    df: Any,
    table_name: str,
    partitions: list[str],
    mode: str = "overwrite",
    logger: Any | None = None,
) -> None:
    """Write a Spark DataFrame as a managed Delta table on Databricks.

    The table is written using ``saveAsTable`` so the Delta table is
    registered in Unity Catalog automatically.  The caller must pass a
    fully-qualified ``catalog.schema.table`` name produced by
    ``common/catalog.resolve_table_name``.

    Partition columns are applied explicitly.  Dynamic partition overwrite
    mode (``spark.sql.sources.partitionOverwriteMode=dynamic``) must be
    enabled at the session or cluster level — this is set in
    ``config/databricks.yaml`` and ``config/spark/databricks.conf``.

    This function requires an active PySpark session.  It must only be called
    when the Databricks Spark profile is active.  Importing this module in a
    non-Spark environment is safe; calling this function without PySpark will
    raise ``ImportError``.

    Args:
        df:         Spark DataFrame to persist.
        table_name: Fully-qualified ``catalog.schema.table`` identifier.
        partitions: Ordered list of partition column names.
        mode:       Write mode — ``"overwrite"`` or ``"append"``.
        logger:     Optional Python logger for progress messages.
    """
    try:
        from pyspark.sql import DataFrame  # noqa: F401
    except ModuleNotFoundError as exc:
        raise ImportError(
            "write_delta_table requires PySpark. " "Install pyspark before calling this function."
        ) from exc

    if df is None or not hasattr(df, "write"):
        raise TypeError("df must be a Spark DataFrame")

    validated_mode = resolve_delta_write_mode(mode)

    log = logger or LOGGER
    log.info(
        "Writing Delta table table=%s partitions=%s mode=%s",
        table_name,
        partitions,
        validated_mode,
    )

    writer = df.write.format("delta").mode(validated_mode)
    if partitions:
        writer = writer.partitionBy(*partitions)

    writer.saveAsTable(table_name)

    log.info("Delta write complete table=%s", table_name)
