"""Audit persistence, CloudWatch payloads, and secret-safe monitoring tests."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from transport_etl.monitor.audit import (
    LocalJsonAuditStore,
    S3AuditStore,
    build_audit_record,
    create_audit_store,
    sanitize_error,
)
from transport_etl.monitor.metrics import (
    CloudWatchMetricsSink,
    NoOpMetricsSink,
    build_metric_data,
    create_metrics_sink,
)
from transport_etl.monitor.runner import PipelineRunMonitor


def _record(status: str = "success") -> dict:
    return build_audit_record(
        run_id="daily_2026-01-01_run1",
        job="daily",
        environment="dev",
        batch_date="2026-01-01",
        started_at="2026-01-01T00:00:00+00:00",
        completed_at="2026-01-01T00:00:02+00:00",
        status=status,
        source_rows={"shipments": 10},
        clean_rows={"shipments": 8},
        rejected_rows={"shipments": 2},
        curated_rows={"fct_shipment": 8},
        quality_failures={"shipments": ["schema_drift", "required_nulls"]},
        redshift_status="failed",
        glue_status="warning",
        outputs={"fct_shipment": "curated/fct_shipment"},
    )


def test_local_audit_persists_success_and_failure(tmp_path: Path) -> None:
    store = LocalJsonAuditStore(tmp_path)
    success = _record()
    path = Path(store.write(success))
    assert json.loads(path.read_text(encoding="utf-8")) == success
    failure = _record("failed")
    failure["error_type"] = "RuntimeError"
    store.write(failure)
    assert json.loads(path.read_text(encoding="utf-8"))["status"] == "failed"


class _FakeS3:
    def __init__(self):
        self.calls = []

    def put_object(self, **kwargs):
        self.calls.append(kwargs)


def test_s3_audit_store_uses_injected_client() -> None:
    client = _FakeS3()
    store = S3AuditStore("s3://test-bucket/audit", client=client)
    store.write(_record())
    assert client.calls[0]["Bucket"] == "test-bucket"
    assert client.calls[0]["Key"] == "audit/daily_2026-01-01_run1.json"
    assert client.calls[0]["ContentMD5"]


def test_audit_and_cloudwatch_factories_keep_local_runs_aws_free(tmp_path: Path) -> None:
    config = {
        "paths": {"audit_base_path": str(tmp_path)},
        "audit": {"enabled": True, "backend": "auto", "path": ""},
        "cloudwatch": {"enabled": False},
    }
    assert isinstance(create_audit_store(config), LocalJsonAuditStore)
    assert isinstance(create_metrics_sink(config), NoOpMetricsSink)


def test_metric_inventory_uses_low_cardinality_dimensions() -> None:
    data = build_metric_data(_record())
    names = {item["MetricName"] for item in data}
    assert names == {
        "PipelineSuccess",
        "PipelineDurationSeconds",
        "RowsRead",
        "RowsWritten",
        "RowsRejected",
        "DataQualityFailures",
        "SchemaDriftFailures",
        "RedshiftLoadFailures",
        "GlueCatalogFailures",
    }
    assert all(dim["Name"] != "RunId" for item in data for dim in item["Dimensions"])
    assert all(
        dim["Name"] in {"Job", "Environment", "Entity"}
        for item in data
        for dim in item["Dimensions"]
    )
    assert any(item["MetricName"] == "RowsRead" and item["Value"] == 10 for item in data)


class _FakeCloudWatch:
    def __init__(self, fail: bool = False):
        self.calls = []
        self.fail = fail

    def put_metric_data(self, **kwargs):
        self.calls.append(kwargs)
        if self.fail:
            raise RuntimeError("CloudWatch unavailable")


def test_cloudwatch_success_failure_and_disabled_sink() -> None:
    client = _FakeCloudWatch()
    sink = CloudWatchMetricsSink("TransportETL", "us-east-1", client=client)
    sink.emit(_record())
    sink.emit(_record("failed"))
    assert client.calls[0]["Namespace"] == "TransportETL"
    assert "PipelineSuccess" in {x["MetricName"] for x in client.calls[0]["MetricData"]}
    assert "PipelineFailure" in {x["MetricName"] for x in client.calls[1]["MetricData"]}
    NoOpMetricsSink().emit(_record())


def test_cloudwatch_client_failure_surfaces_to_caller() -> None:
    sink = CloudWatchMetricsSink("TransportETL", "us-east-1", client=_FakeCloudWatch(True))
    with pytest.raises(RuntimeError, match="CloudWatch unavailable"):
        sink.emit(_record())


def test_sensitive_error_redaction() -> None:
    config = {"redshift": {"secret_arn": "arn:aws:secret:test", "password": "topsecret"}}
    message = (
        "connect postgres://user:pass@host/db password=topsecret "
        "arn:aws:secret:test AKIA1234567890ABCDEF"
    )
    cleaned = sanitize_error(message, config)
    for value in (
        "user:pass@",
        "postgres://",
        "topsecret",
        "arn:aws:secret:test",
        "AKIA1234567890ABCDEF",
    ):
        assert value not in cleaned


def test_run_monitor_audits_success_failure_and_partial_publish() -> None:
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

    audit = _Audit()
    metrics = _Metrics()
    monitor = PipelineRunMonitor(
        {"app": {"env": "dev"}},
        run_id="daily_2026-01-01_run1",
        job="daily",
        batch_date="2026-01-01",
        audit_store=audit,
        metrics_sink=metrics,
    )
    monitor.progress["source_rows"] = {"shipments": 10}
    record = monitor.persist_success({"outputs": {"fct_shipment": "gold"}})
    monitor.emit_success(record)
    assert audit.records[0]["status"] == "success"
    assert metrics.records[0]["status"] == "success"

    monitor.progress["redshift_status"] = "failed"
    monitor.fail(RuntimeError("COPY failed"))
    assert audit.records[-1]["status"] == "failed"
    assert audit.records[-1]["redshift_status"] == "failed"
    assert metrics.records[-1]["status"] == "failed"


def test_cloudwatch_api_failure_does_not_hide_success(tmp_path: Path) -> None:
    audit = LocalJsonAuditStore(tmp_path)
    sink = CloudWatchMetricsSink("TransportETL", "us-east-1", client=_FakeCloudWatch(True))
    monitor = PipelineRunMonitor(
        {"app": {"env": "dev"}},
        run_id="daily_2026-01-01_run1",
        job="daily",
        batch_date="2026-01-01",
        audit_store=audit,
        metrics_sink=sink,
    )
    record = monitor.persist_success({"outputs": {}})
    monitor.emit_success(record)
    assert json.loads((tmp_path / "daily_2026-01-01_run1.json").read_text())["status"] == "success"
