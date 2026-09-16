"""Integration-style coverage for the daily job's optional Redshift seam."""

from __future__ import annotations

from transport_etl.jobs.run_daily_batch import _publish_redshift_if_enabled
from transport_etl.publish.redshift import REDSHIFT_TABLE_ORDER


class _Writer:
    def __init__(self) -> None:
        self.mode_name = ""
        self.destinations: list[str] = []

    def mode(self, value: str):
        self.mode_name = value
        return self

    def parquet(self, destination: str) -> None:
        assert self.mode_name == "overwrite"
        self.destinations.append(destination)


class _Frame:
    def __init__(self) -> None:
        self.write = _Writer()


class _DataApiClient:
    def __init__(self) -> None:
        self.batch_calls: list[dict[str, object]] = []

    def batch_execute_statement(self, **kwargs):
        self.batch_calls.append(kwargs)
        return {"Id": f"statement-{len(self.batch_calls)}"}

    def describe_statement(self, **kwargs):
        return {"Status": "FINISHED", "ResultRows": 3, "Duration": 50_000_000}

    def execute_statement(self, **kwargs):  # pragma: no cover - workflow uses batches
        raise AssertionError("Unexpected single-statement execution")

    def cancel_statement(self, **kwargs):  # pragma: no cover - all calls finish
        raise AssertionError("Unexpected cancellation")


class _Logger:
    def __init__(self) -> None:
        self.messages: list[tuple[object, ...]] = []

    def info(self, *args) -> None:
        self.messages.append(args)


def test_daily_redshift_seam_runs_full_workflow_with_fake_client(project_root) -> None:
    config = {
        "redshift": {
            "enabled": True,
            "region": "us-east-1",
            "database": "analytics",
            "workgroup_name": "transport-workgroup",
            "iam_role_arn": "arn:aws:iam::123456789012:role/redshift-copy",
            "source_s3_path": "s3://transport-bucket/redshift-ready",
            "staging_schema": "transport_staging",
            "target_schema": "transport_analytics",
            "audit_schema": "transport_audit",
            "poll_interval_seconds": 1,
            "timeout_seconds": 10,
        }
    }
    frames = {table: _Frame() for table in REDSHIFT_TABLE_ORDER}
    client = _DataApiClient()
    logger = _Logger()

    results = _publish_redshift_if_enabled(
        config=config,
        dataframes=frames,
        batch_date="2026-01-01",
        logger=logger,
        client=client,
        sql_dir=project_root / "sql" / "redshift",
    )

    assert [result["table"] for result in results] == list(REDSHIFT_TABLE_ORDER)
    assert all(result["status"] == "FINISHED" for result in results)
    assert all(result["statement_id"].startswith("statement-") for result in results)
    assert len(client.batch_calls) == 6  # one bootstrap plus five table transactions
    assert all(frame.write.destinations for frame in frames.values())
    load_sql = [call["Sqls"] for call in client.batch_calls[1:]]
    assert all(any("COPY" in sql for sql in statements) for statements in load_sql)
    assert all(any("MERGE INTO" in sql for sql in statements) for statements in load_sql)
    assert logger.messages
