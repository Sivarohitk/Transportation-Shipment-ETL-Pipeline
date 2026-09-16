"""Monitoring follows real daily/backfill entry-point outcomes without Spark or AWS."""

from __future__ import annotations

from transport_etl.jobs import run_backfill_batch as backfill
from transport_etl.jobs import run_daily_batch as daily


class _Audit:
    def __init__(self):
        self.records = []

    def write(self, record):
        self.records.append(dict(record))
        return "audit.json"


class _Metrics:
    def __init__(self):
        self.records = []

    def emit(self, record):
        self.records.append(dict(record))


def test_daily_entrypoint_audits_and_emits_success(monkeypatch) -> None:
    audit = _Audit()
    metrics = _Metrics()
    monkeypatch.setattr(daily, "create_spark_session_from_config", lambda config: object())
    monkeypatch.setattr(daily, "stop_spark_session", lambda spark: None)

    def execute(**kwargs):
        kwargs["monitor"].progress["source_rows"] = {"shipments": 5}
        kwargs["monitor"].progress["curated_rows"] = {"fct_shipment": 4}
        return {"outputs": {"fct_shipment": "gold"}}

    monkeypatch.setattr(daily, "_execute_daily_flow", execute)
    assert (
        daily.run_daily_batch(
            "dev",
            "2026-01-01",
            {"pipeline_state.enabled": False},
            audit_store=audit,
            metrics_sink=metrics,
        )
        == 0
    )
    assert audit.records[0]["status"] == "success"
    assert audit.records[0]["source_rows"] == {"shipments": 5}
    assert metrics.records[0]["curated_rows"] == {"fct_shipment": 4}


def test_daily_partial_redshift_failure_audited_and_retryable(monkeypatch) -> None:
    audit = _Audit()
    metrics = _Metrics()
    monkeypatch.setattr(daily, "create_spark_session_from_config", lambda config: object())
    monkeypatch.setattr(daily, "stop_spark_session", lambda spark: None)

    def execute(**kwargs):
        kwargs["monitor"].progress["glue_status"] = "success"
        kwargs["monitor"].progress["redshift_status"] = "failed"
        raise RuntimeError("COPY failed password=topsecret")

    monkeypatch.setattr(daily, "_execute_daily_flow", execute)
    assert (
        daily.run_daily_batch(
            "dev",
            "2026-01-01",
            {"pipeline_state.enabled": False},
            audit_store=audit,
            metrics_sink=metrics,
        )
        == 1
    )
    record = audit.records[0]
    assert record["status"] == "failed"
    assert record["glue_status"] == "success"
    assert record["redshift_status"] == "failed"
    assert "topsecret" not in record["error_message"]
    assert metrics.records[0]["status"] == "failed"


def test_backfill_summary_audits_failure(monkeypatch) -> None:
    audit = _Audit()
    metrics = _Metrics()
    statuses = iter([0, 1])
    monkeypatch.setattr(backfill, "run_daily_batch", lambda **kwargs: next(statuses))
    assert (
        backfill.run_backfill_batch(
            "dev",
            "2026-01-01",
            "2026-01-02",
            audit_store=audit,
            metrics_sink=metrics,
        )
        == 1
    )
    assert audit.records[0]["job"] == "backfill"
    assert audit.records[0]["status"] == "failed"
    assert metrics.records[0]["status"] == "failed"
