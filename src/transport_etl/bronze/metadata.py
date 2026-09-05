"""Bronze-layer operational metadata definitions.

The Bronze layer captures raw source records with the smallest possible
transformation footprint.  In addition to the source columns, every Bronze
record carries a small set of operational metadata columns that describe
**how** the record entered the lakehouse.  These columns are intentionally
limited to fields that can be populated reliably from the job runner
context (timestamp, source path, batch identifier, and run date).

The columns defined here are the single source of truth used by the
Bronze builder and publisher modules.  Tests assert against these
constants so a column rename is a deliberate, reviewable change.
"""

from __future__ import annotations

from transport_etl.common.constants import (
    BRONZE_METADATA_COLUMNS,
    META_COL_BATCH_ID,
    META_COL_INGESTED_AT,
    META_COL_RUN_DATE,
    META_COL_SOURCE_FILE,
)

__all__ = [
    "BRONZE_METADATA_COLUMNS",
    "META_COL_BATCH_ID",
    "META_COL_INGESTED_AT",
    "META_COL_RUN_DATE",
    "META_COL_SOURCE_FILE",
    "build_batch_id",
]


def build_batch_id(run_date: str, job_name: str = "daily") -> str:
    """Return a deterministic, human-readable Bronze batch identifier.

    The batch id is stable for a given ``run_date`` and ``job_name``
    combination so reruns of the same day produce the same value.  Job
    names longer than 32 characters are truncated to keep the value
    safe for use as a partition folder token and a Delta metadata
    attribute.

    Args:
        run_date: Batch run date in ``YYYY-MM-DD`` format.
        job_name: Job name (e.g. ``"daily"`` or ``"backfill"``).

    Returns:
        Batch identifier of the form ``<job>_<YYYYMMDD>``.
    """
    normalized_date = str(run_date).strip().replace("-", "")
    safe_job = str(job_name).strip().lower().replace(" ", "_")
    if len(safe_job) > 32:
        safe_job = safe_job[:32]
    if not safe_job:
        safe_job = "batch"
    if not normalized_date:
        normalized_date = "undated"
    return f"{safe_job}_{normalized_date}"
