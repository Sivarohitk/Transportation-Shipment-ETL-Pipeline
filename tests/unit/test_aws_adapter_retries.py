"""Adapter retries exercise real adapter boundaries with fake remote clients."""

from __future__ import annotations

import json

import pytest

from transport_etl.common.aws_retry import RetryPolicy
from transport_etl.common.pipeline_state import S3PipelineStateStore, process_batch_with_state
from transport_etl.monitor.audit import S3AuditStore
from transport_etl.monitor.metrics import CloudWatchMetricsSink
from transport_etl.publish.glue_catalog import GlueCatalogAdapter, GlueCatalogConfig
from transport_etl.publish.redshift import RedshiftConfig, RedshiftDataApi, RedshiftStatementError


class _AwsError(Exception):
    def __init__(self, code: str, status: int = 400):
        super().__init__("credential=never-log")
        self.response = {"Error": {"Code": code}, "ResponseMetadata": {"HTTPStatusCode": status}}


_POLICY = RetryPolicy(max_attempts=3, initial_delay=0, max_delay=0, jitter=0)


def test_glue_retries_transient_get_but_not_access_denied() -> None:
    class Client:
        calls = 0

        def get_database(self, **kwargs):
            self.calls += 1
            if self.calls == 1:
                raise _AwsError("ThrottlingException", 429)
            return {"Database": {"Name": kwargs["Name"]}}

    client = Client()
    adapter = GlueCatalogAdapter(
        GlueCatalogConfig(enabled=True, region="us-east-1", database="curated"),
        client=client,
        retry_policy=_POLICY,
        sleep=lambda _: None,
    )
    assert adapter.ensure_database() == "existing"
    assert client.calls == 2

    def denied(**kwargs):
        raise _AwsError("AccessDeniedException", 403)

    client.get_database = denied
    with pytest.raises(_AwsError):
        adapter.ensure_database()


def test_redshift_submit_retries_with_same_idempotency_token_and_sql_error_does_not() -> None:
    class Client:
        requests = []

        def execute_statement(self, **kwargs):
            self.requests.append(kwargs)
            if len(self.requests) == 1:
                raise _AwsError("ServiceUnavailableException", 503)
            return {"Id": "statement-1"}

        def describe_statement(self, **kwargs):
            return {"Status": "FINISHED", "ResultRows": 1}

    config = RedshiftConfig(enabled=True, database="dev", workgroup_name="test", timeout_seconds=10)
    client = Client()
    api = RedshiftDataApi(config=config, client=client, retry_policy=_POLICY, sleep=lambda _: None)
    assert api.execute_sql("SELECT 1").status == "FINISHED"
    assert len(client.requests) == 2
    assert client.requests[0]["ClientToken"] == client.requests[1]["ClientToken"]

    client.describe_statement = lambda **kwargs: {"Status": "FAILED", "Error": "syntax error"}
    with pytest.raises(RedshiftStatementError):
        api.poll_statement("statement-1")


def test_cloudwatch_and_s3_audit_retry_transient_calls() -> None:
    class Client:
        calls = 0

        def put_metric_data(self, **kwargs):
            self.calls += 1
            if self.calls == 1:
                raise _AwsError("ThrottlingException", 429)

    client = Client()
    sink = CloudWatchMetricsSink(
        "TransportETL", "us-east-1", client=client, retry_policy=_POLICY, sleep=lambda _: None
    )
    sink.emit({"status": "success", "job": "daily", "environment": "dev"})
    assert client.calls == 2

    class S3:
        calls = 0
        objects = {}

        def put_object(self, **kwargs):
            self.calls += 1
            if self.calls == 1:
                raise _AwsError("ServiceUnavailableException", 503)
            self.objects[kwargs["Key"]] = kwargs["Body"]

    s3 = S3()
    store = S3AuditStore("s3://bucket/audit", s3, retry_policy=_POLICY, sleep=lambda _: None)
    store.write({"run_id": "run-1", "status": "success"})
    assert s3.calls == 2
    assert len(s3.objects) == 1


def test_s3_state_retry_does_not_duplicate_final_checkpoint() -> None:
    class S3:
        objects = {}
        put_calls = 0

        def get_object(self, **kwargs):
            if kwargs["Key"] not in self.objects:
                raise _AwsError("NoSuchKey", 404)

            class Body:
                def read(self):
                    return self.objects[kwargs["Key"]]

            body = Body()
            body.objects = self.objects
            return {"Body": body}

        def put_object(self, **kwargs):
            self.put_calls += 1
            self.objects[kwargs["Key"]] = kwargs["Body"]
            if self.put_calls == 2:
                # Commit succeeded remotely, but the response was lost.
                raise _AwsError("ServiceUnavailableException", 503)

    s3 = S3()
    store = S3PipelineStateStore(
        "s3://bucket/state", s3, retry_policy=_POLICY, sleep=lambda _: None
    )
    executions = 0

    def execute():
        nonlocal executions
        executions += 1
        return {"outputs": {"fct_shipment": "s3://bucket/gold"}}

    first = process_batch_with_state(store, "2026-01-01", "run-1", [], "same", execute)
    second = process_batch_with_state(store, "2026-01-01", "run-2", [], "same", execute)
    assert first.skipped is False
    assert second.skipped is True
    assert executions == 1
    assert len(s3.objects) == 1
    assert json.loads(next(iter(s3.objects.values())))["status"] == "success"
