"""Unit tests for optional Amazon Redshift publication."""

from __future__ import annotations

from pathlib import Path

import pytest

from transport_etl.publish.redshift import (
    REDSHIFT_TABLE_ORDER,
    RedshiftConfig,
    RedshiftConfigurationError,
    RedshiftDataApi,
    RedshiftStatementError,
    RedshiftStatementTimeout,
    build_copy_sql,
    build_merge_sql,
    publish_curated_to_redshift,
    quote_identifier,
)


def _enabled_config() -> dict[str, object]:
    return {
        "redshift": {
            "enabled": True,
            "region": "us-east-1",
            "database": "dev",
            "workgroup_name": "transport-workgroup",
            "cluster_identifier": "",
            "secret_arn": "",
            "database_user": "",
            "iam_role_arn": "arn:aws:iam::123456789012:role/redshift-copy",
            "source_s3_path": "s3://transport-bucket/redshift-ready",
            "staging_schema": "transport_staging",
            "target_schema": "transport_analytics",
            "audit_schema": "transport_audit",
            "poll_interval_seconds": 1,
            "timeout_seconds": 10,
        }
    }


class _FakeDataApiClient:
    def __init__(self, descriptions: list[dict[str, object]] | None = None) -> None:
        self.descriptions = list(descriptions or [])
        self.execute_calls: list[dict[str, object]] = []
        self.batch_calls: list[dict[str, object]] = []
        self.cancel_calls: list[dict[str, object]] = []
        self._counter = 0

    def execute_statement(self, **kwargs):
        self.execute_calls.append(kwargs)
        self._counter += 1
        return {"Id": f"statement-{self._counter}"}

    def batch_execute_statement(self, **kwargs):
        self.batch_calls.append(kwargs)
        self._counter += 1
        return {"Id": f"statement-{self._counter}"}

    def describe_statement(self, **kwargs):
        if self.descriptions:
            return self.descriptions.pop(0)
        return {"Status": "FINISHED", "ResultRows": 0, "Duration": 1_000_000}

    def cancel_statement(self, **kwargs):
        self.cancel_calls.append(kwargs)
        return {"Status": True}


class _FakeClock:
    def __init__(self) -> None:
        self.value = 0.0

    def monotonic(self) -> float:
        return self.value

    def sleep(self, seconds: float) -> None:
        self.value += seconds


class _FakeWriter:
    def __init__(self, destinations: list[str]) -> None:
        self.destinations = destinations
        self.write_mode = ""

    def mode(self, value: str):
        self.write_mode = value
        return self

    def parquet(self, destination: str) -> None:
        assert self.write_mode == "overwrite"
        self.destinations.append(destination)


class _FakeDataFrame:
    def __init__(self, destinations: list[str]) -> None:
        self.write = _FakeWriter(destinations)


def test_redshift_config_disabled_tolerates_empty_aws_values() -> None:
    settings = RedshiftConfig.from_mapping({"redshift": {"enabled": False}})

    assert settings.enabled is False
    assert settings.target_schema == "transport_analytics"


def test_redshift_config_parses_enabled_serverless_target() -> None:
    settings = RedshiftConfig.from_mapping(_enabled_config())

    assert settings.enabled is True
    assert settings.workgroup_name == "transport-workgroup"
    assert settings.cluster_identifier == ""


@pytest.mark.parametrize(
    "change",
    [
        {"database": ""},
        {"workgroup_name": "", "cluster_identifier": ""},
        {"cluster_identifier": "cluster", "workgroup_name": "workgroup"},
        {"source_s3_path": "https://example.com/data"},
        {"iam_role_arn": "not-an-arn"},
        {"target_schema": "bad-schema"},
        {"database": "${UNRESOLVED_DATABASE}"},
    ],
)
def test_redshift_config_rejects_invalid_enabled_values(change: dict[str, str]) -> None:
    config = _enabled_config()
    redshift = config["redshift"]
    assert isinstance(redshift, dict)
    redshift.update(change)

    with pytest.raises(RedshiftConfigurationError):
        RedshiftConfig.from_mapping(config)


@pytest.mark.parametrize("value", ["valid_name", "Name123", "_private"])
def test_quote_identifier_accepts_safe_names(value: str) -> None:
    assert quote_identifier(value) == f'"{value}"'


@pytest.mark.parametrize("value", ["bad-name", "schema.table", "name;DROP TABLE x", ""])
def test_quote_identifier_rejects_unsafe_names(value: str) -> None:
    with pytest.raises(ValueError):
        quote_identifier(value)


def test_build_copy_sql_uses_parquet_and_escapes_source_literal() -> None:
    sql = build_copy_sql(
        "transport_staging",
        "fct_shipment",
        "s3://transport-bucket/redshift-ready/it's-here",
        "arn:aws:iam::123456789012:role/redshift-copy",
    )

    assert 'COPY "transport_staging"."fct_shipment"' in sql
    assert "FROM 's3://transport-bucket/redshift-ready/it''s-here'" in sql
    assert "FORMAT AS PARQUET" in sql
    assert "REGION" not in sql


def test_build_merge_sql_uses_expected_business_key() -> None:
    sql = build_merge_sql(
        table="dim_carrier",
        staging_schema="transport_staging",
        target_schema="transport_analytics",
    )

    assert 'MERGE INTO "transport_analytics"."dim_carrier"' in sql
    assert '"dim_carrier"."carrier_id" = source."carrier_id"' in sql
    assert '"dim_carrier"."p_date" = source."p_date"' in sql
    assert sql.rstrip().endswith("REMOVE DUPLICATES;")


def test_data_api_polls_until_success() -> None:
    client = _FakeDataApiClient(
        [
            {"Status": "SUBMITTED"},
            {"Status": "STARTED"},
            {"Status": "FINISHED", "ResultRows": 7, "Duration": 2_000_000_000},
        ]
    )
    clock = _FakeClock()
    api = RedshiftDataApi(
        client=client,
        config=RedshiftConfig.from_mapping(_enabled_config()),
        sleep=clock.sleep,
        monotonic=clock.monotonic,
    )

    result = api.execute_sql("SELECT 1")

    assert result.status == "FINISHED"
    assert result.rows == 7
    assert result.duration_seconds == pytest.approx(2.0)
    assert client.execute_calls[0]["WorkgroupName"] == "transport-workgroup"


@pytest.mark.parametrize("status", ["FAILED", "ABORTED"])
def test_data_api_surfaces_failure(status: str) -> None:
    client = _FakeDataApiClient([{"Status": status, "Error": "warehouse rejected SQL"}])
    api = RedshiftDataApi(
        client=client,
        config=RedshiftConfig.from_mapping(_enabled_config()),
        sleep=lambda _: None,
    )

    with pytest.raises(RedshiftStatementError, match="warehouse rejected SQL"):
        api.execute_sql("SELECT 1")


def test_data_api_times_out_and_cancels_statement() -> None:
    client = _FakeDataApiClient([{"Status": "STARTED"}] * 20)
    clock = _FakeClock()
    config = _enabled_config()
    redshift = config["redshift"]
    assert isinstance(redshift, dict)
    redshift["timeout_seconds"] = 2
    api = RedshiftDataApi(
        client=client,
        config=RedshiftConfig.from_mapping(config),
        sleep=clock.sleep,
        monotonic=clock.monotonic,
    )

    with pytest.raises(RedshiftStatementTimeout):
        api.execute_sql("SELECT pg_sleep(20)")

    assert client.cancel_calls == [{"Id": "statement-1"}]


def test_data_api_batch_is_transactional() -> None:
    client = _FakeDataApiClient()
    api = RedshiftDataApi(
        client=client,
        config=RedshiftConfig.from_mapping(_enabled_config()),
        sleep=lambda _: None,
    )

    api.execute_transaction(["DELETE FROM x", "INSERT INTO x VALUES (1)"])

    assert client.batch_calls[0]["ExecutionMode"] == "TRANSACTION"
    assert client.batch_calls[0]["Sqls"] == ["DELETE FROM x", "INSERT INTO x VALUES (1)"]


def test_publish_disabled_does_not_touch_frames_or_client(tmp_path: Path) -> None:
    client = _FakeDataApiClient()

    results = publish_curated_to_redshift(
        {"redshift": {"enabled": False}},
        dataframes=None,
        batch_id="daily_2026-01-01",
        client=client,
        sql_dir=tmp_path,
    )

    assert results == []
    assert client.execute_calls == []
    assert client.batch_calls == []


def test_publish_uses_expected_order_and_is_rerunnable(project_root: Path) -> None:
    destinations: list[str] = []
    frames = {table: _FakeDataFrame(destinations) for table in REDSHIFT_TABLE_ORDER}
    client = _FakeDataApiClient()

    first = publish_curated_to_redshift(
        _enabled_config(),
        dataframes=frames,
        batch_id="daily_2026-01-01",
        client=client,
        sql_dir=project_root / "sql" / "redshift",
        sleep=lambda _: None,
    )
    first_load_calls = list(client.batch_calls[1:])
    second = publish_curated_to_redshift(
        _enabled_config(),
        dataframes=frames,
        batch_id="daily_2026-01-01",
        client=client,
        sql_dir=project_root / "sql" / "redshift",
        sleep=lambda _: None,
    )
    second_load_calls = client.batch_calls[len(first_load_calls) + 2 :]

    assert [result.table for result in first] == list(REDSHIFT_TABLE_ORDER)
    assert [result.table for result in second] == list(REDSHIFT_TABLE_ORDER)
    assert len(destinations) == 10
    assert all("batch_id=daily_2026-01-01" in path for path in destinations)
    assert [call["Sqls"] for call in first_load_calls] == [
        call["Sqls"] for call in second_load_calls
    ]
    assert all(any("MERGE INTO" in sql for sql in call["Sqls"]) for call in first_load_calls)


def test_publish_reads_bootstrap_sql_from_runtime_resource_root(tmp_path: Path) -> None:
    sql_root = tmp_path / "sql" / "redshift"
    sql_root.mkdir(parents=True)
    (sql_root / "001_bootstrap.sql").write_text("SELECT 1;", encoding="utf-8")
    config = _enabled_config()
    config["runtime"] = {"resource_base_path": str(tmp_path)}
    frames = {table: _FakeDataFrame([]) for table in REDSHIFT_TABLE_ORDER}
    client = _FakeDataApiClient()

    publish_curated_to_redshift(
        config,
        dataframes=frames,
        batch_id="daily_2026-01-01",
        client=client,
        sleep=lambda _: None,
    )

    assert client.batch_calls[0]["Sqls"] == ["SELECT 1"]


def test_sql_assets_define_all_warehouse_tables(project_root: Path) -> None:
    sql_root = project_root / "sql" / "redshift"
    combined = "\n".join(
        path.read_text(encoding="utf-8") for path in sorted(sql_root.glob("*.sql"))
    )

    for table in REDSHIFT_TABLE_ORDER:
        assert f"{{{{staging_schema}}}}.{table}" in combined
        assert f"{{{{target_schema}}}}.{table}" in combined
    assert "{{audit_schema}}.etl_load_audit" in combined
