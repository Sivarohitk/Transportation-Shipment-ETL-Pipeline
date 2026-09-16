"""Bind audit persistence and optional metrics to a single run attempt."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Mapping

from transport_etl.monitor.audit import (
    AuditStore,
    build_audit_record,
    create_audit_store,
    sanitize_error,
)
from transport_etl.monitor.metrics import MetricsSink, create_metrics_sink


class PipelineRunMonitor:
    """Collect partial stage facts and finalize audit/metrics on every outcome."""

    def __init__(
        self,
        config: Mapping[str, Any],
        *,
        run_id: str,
        job: str,
        batch_date: str | None,
        audit_store: AuditStore | None = None,
        metrics_sink: MetricsSink | None = None,
        s3_client: Any | None = None,
        cloudwatch_client: Any | None = None,
        logger: Any | None = None,
    ) -> None:
        self.config = config
        self.run_id = run_id
        self.job = job
        self.batch_date = batch_date
        self.environment = str(config.get("app", {}).get("env", "unknown"))
        self.started_at = datetime.now(timezone.utc).isoformat()
        self.audit_store = (
            audit_store if audit_store is not None else create_audit_store(config, client=s3_client)
        )
        self.metrics_sink = (
            metrics_sink
            if metrics_sink is not None
            else create_metrics_sink(config, client=cloudwatch_client)
        )
        self.logger = logger
        self.progress: dict[str, Any] = {
            "source_rows": {},
            "clean_rows": {},
            "rejected_rows": {},
            "curated_rows": {},
            "quality_failures": {},
            "redshift_status": (
                "pending" if config.get("redshift", {}).get("enabled") else "disabled"
            ),
            "glue_status": "pending" if config.get("glue", {}).get("enabled") else "disabled",
        }

    def _record(
        self,
        status: str,
        *,
        result: Mapping[str, Any] | None = None,
        error: Exception | None = None,
    ) -> dict[str, Any]:
        return build_audit_record(
            run_id=self.run_id,
            job=self.job,
            environment=self.environment,
            batch_date=self.batch_date,
            started_at=self.started_at,
            completed_at=datetime.now(timezone.utc).isoformat(),
            status=status,
            source_rows=self.progress["source_rows"],
            clean_rows=self.progress["clean_rows"],
            rejected_rows=self.progress["rejected_rows"],
            curated_rows=self.progress["curated_rows"],
            quality_failures=self.progress["quality_failures"],
            redshift_status=self.progress["redshift_status"],
            glue_status=self.progress["glue_status"],
            outputs=(result or {}).get("outputs", {}),
            error=error,
            config=self.config,
        )

    def _write(self, record: Mapping[str, Any]) -> None:
        if self.audit_store is not None:
            self.audit_store.write(record)

    def _emit(self, record: Mapping[str, Any]) -> None:
        try:
            self.metrics_sink.emit(record)
        except Exception as exc:
            if self.logger is not None:
                self.logger.warning(
                    "Metrics emission failed type=%s message=%s",
                    type(exc).__name__,
                    sanitize_error(exc, self.config),
                )

    def persist_success(self, result: Mapping[str, Any]) -> dict[str, Any]:
        """Persist success before the batch checkpoint is marked successful."""
        record = self._record("success", result=result)
        self._write(record)
        return record

    def emit_success(self, record: Mapping[str, Any]) -> None:
        """Emit best-effort metrics after the authoritative state commit."""
        self._emit(record)

    def persist_skipped(self) -> None:
        """Record an unchanged successful-batch skip without success metrics."""
        self._write(self._record("skipped"))

    def fail(self, error: Exception) -> dict[str, Any]:
        """Attempt a failure audit even if an earlier audit write failed."""
        record = self._record("failed", error=error)
        try:
            self._write(record)
        except Exception as audit_exc:
            if self.logger is not None:
                self.logger.error(
                    "Failure audit persistence failed type=%s message=%s",
                    type(audit_exc).__name__,
                    sanitize_error(audit_exc, self.config),
                )
        self._emit(record)
        return record
