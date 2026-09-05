"""Silver-layer quarantine writer for invalid records.

Silver-layer quality failures must be:

- Quarantined (not silently discarded), per AGENTS.md rule 7.
- Tagged with the rule that caught them (``__rule_name``,
  ``__rule_reason``).
- Tagged with the source identity (business key) so an operator can
  trace a bad row back to the source system without scanning the entire
  table.
- Tagged with batch / run metadata so the bad record can be correlated
  with the Bronze / Silver write that produced it.

The writer is a thin wrapper over the existing
:func:`transport_etl.common.io.write_invalid_records_with_fallback`
helper so the Windows local fallback and Spark Parquet semantics
continue to work unchanged.

The same quarantine directory layout is used as the existing
``quality.rules`` quarantine writer:

    <quarantine_path>/<entity>/<rule_name>/part-*.parquet

The writer accepts any Spark DataFrame; the caller (the Silver builder)
is responsible for attaching the standardised ``__rule_name`` and
``__rule_reason`` columns before calling :func:`quarantine_silver_records`.
"""

from __future__ import annotations

import logging
from typing import Any, Mapping

try:
    from pyspark.sql import DataFrame
    from pyspark.sql import functions as F
except ModuleNotFoundError:  # pragma: no cover
    DataFrame = Any  # type: ignore[assignment]
    F = None  # type: ignore[assignment]

from transport_etl.common.io import write_invalid_records_with_fallback
from transport_etl.silver.keys import source_identity_for

LOGGER = logging.getLogger("transport_etl.silver.quarantine")

# Rule names emitted by the Silver layer.  Centralised so tests can
# assert on the exact strings instead of using brittle literals.
RULE_REQUIRED_NULLS = "silver_required_nulls"
RULE_DUPLICATE_KEYS = "silver_duplicate_keys"
RULE_INVALID_ALLOWED_VALUE = "silver_invalid_allowed_value"
RULE_NON_NEGATIVE = "silver_non_negative_metric"
RULE_TIMESTAMP_ORDER = "silver_invalid_timestamp_order"
RULE_SCHEMA_DRIFT = "silver_schema_drift"
RULE_DROPPED_BY_DEDUP = "silver_dropped_by_dedup"

ALL_SILVER_RULE_NAMES: tuple[str, ...] = (
    RULE_REQUIRED_NULLS,
    RULE_DUPLICATE_KEYS,
    RULE_INVALID_ALLOWED_VALUE,
    RULE_NON_NEGATIVE,
    RULE_TIMESTAMP_ORDER,
    RULE_SCHEMA_DRIFT,
    RULE_DROPPED_BY_DEDUP,
)


def _require_spark() -> None:
    """Ensure pyspark is available before executing Spark operations.

    Uses the same guard pattern as ``quality.rules``: a non-None
    ``pyspark.sql.functions`` module means pyspark is importable.
    """
    if F is None:
        raise ImportError("pyspark is required for Silver quarantine writer")


def _enrich_with_source_identity(
    df: DataFrame,
    table_name: str,
    batch_id: str | None,
    run_date: str | None,
    default_rule_label: str,
) -> DataFrame:
    """Attach the standardised Silver quarantine metadata columns.

    Every quarantined record receives the following audit columns:

    - ``__quarantine_rule`` — same value as ``__rule_name`` (added here
      for discoverability when the source frame was built by a
      different code path).  When the source frame lacks
      ``__rule_name``, the value falls back to ``default_rule_label``.
    - ``__source_entity`` — the Silver table name.
    - ``__source_identity`` — comma-separated business-key values.
    - ``__batch_id`` / ``__run_date`` — operational lineage.

    The function is idempotent: if any of these columns already exist
    on the frame, they are overwritten in place.
    """
    _require_spark()
    identity_columns = list(source_identity_for(table_name))
    if identity_columns:
        identity_expr = F.concat_ws(
            ":",
            *[F.coalesce(F.col(col).cast("string"), F.lit("")) for col in identity_columns],
        )
    else:
        identity_expr = F.lit("UNKNOWN")

    enriched = df
    if "__rule_name" in df.columns:
        enriched = enriched.withColumn("__quarantine_rule", F.col("__rule_name"))
    else:
        enriched = enriched.withColumn("__quarantine_rule", F.lit(str(default_rule_label)))
    enriched = enriched.withColumn("__source_entity", F.lit(str(table_name)))
    enriched = enriched.withColumn("__source_identity", identity_expr)
    if batch_id is not None:
        enriched = enriched.withColumn("__batch_id", F.lit(str(batch_id)))
    if run_date is not None:
        enriched = enriched.withColumn("__run_date", F.lit(str(run_date)))
    enriched = enriched.withColumn("__quarantined_at", F.current_timestamp())

    return enriched


def quarantine_silver_records(
    invalid_df: DataFrame,
    *,
    quarantine_path: str,
    table_name: str,
    rule_name: str,
    batch_id: str | None = None,
    run_date: str | None = None,
    write_config: Mapping[str, Any] | None = None,
) -> str:
    """Write invalid Silver records to the configured quarantine path.

    Args:
        invalid_df: Spark DataFrame containing invalid rows.  Must
            include the standardised ``__rule_name`` / ``__rule_reason``
            columns populated by the Silver builder.
        quarantine_path: Base quarantine path.  Records are written to
            ``<quarantine_path>/<table_name>/<rule_name>/``.
        table_name: Silver table the records came from.
        rule_name: Logical rule that flagged the records.
        batch_id: Optional batch identifier for lineage.
        run_date: Optional run date for lineage.
        write_config: Optional write-fallback configuration forwarded
            to the underlying writer.

    Returns:
        The destination path where records were written.
    """
    _require_spark()
    if not quarantine_path or not str(quarantine_path).strip():
        # Per AGENTS.md rule 7, quarantine paths must be configured.
        raise ValueError("quarantine_path must be configured for Silver quarantine writes")

    if rule_name not in ALL_SILVER_RULE_NAMES:
        raise ValueError(
            f"Unknown Silver rule name '{rule_name}'. "
            f"Expected one of {list(ALL_SILVER_RULE_NAMES)}."
        )

    destination = str(quarantine_path).rstrip("/\\") + f"/{table_name}/{rule_name}"

    enriched = _enrich_with_source_identity(
        invalid_df,
        table_name=table_name,
        batch_id=batch_id,
        run_date=run_date,
        default_rule_label=rule_name,
    )

    LOGGER.warning(
        "Silver quarantine entity=%s rule=%s destination=%s",
        table_name,
        rule_name,
        destination,
    )

    write_invalid_records_with_fallback(
        df=enriched,
        destination=destination,
        mode="append",
        write_config=write_config,
        logger=LOGGER,
    )

    return destination


__all__ = [
    "ALL_SILVER_RULE_NAMES",
    "RULE_DROPPED_BY_DEDUP",
    "RULE_DUPLICATE_KEYS",
    "RULE_INVALID_ALLOWED_VALUE",
    "RULE_NON_NEGATIVE",
    "RULE_REQUIRED_NULLS",
    "RULE_SCHEMA_DRIFT",
    "RULE_TIMESTAMP_ORDER",
    "quarantine_silver_records",
]
