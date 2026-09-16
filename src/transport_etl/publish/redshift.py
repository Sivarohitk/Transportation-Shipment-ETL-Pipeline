"""Optional Amazon Redshift publication through the Redshift Data API.

The canonical Gold outputs remain partitioned for local/EMR consumers.  When
enabled, this module writes a separate unpartitioned Parquet snapshot so every
logical column is present in the files consumed by Redshift ``COPY``.
"""

from __future__ import annotations

import re
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Mapping, Protocol, Sequence
from urllib.parse import urlsplit

from transport_etl.common.constants import (
    TABLE_AGG_SHIPMENT_DAILY,
    TABLE_DIM_CARRIER,
    TABLE_FCT_DELIVERY_EVENT,
    TABLE_FCT_SHIPMENT,
    TABLE_KPI_DELIVERY_DAILY,
)
from transport_etl.monitor.audit import sanitize_error

REDSHIFT_TABLE_ORDER = (
    TABLE_DIM_CARRIER,
    TABLE_FCT_SHIPMENT,
    TABLE_FCT_DELIVERY_EVENT,
    TABLE_AGG_SHIPMENT_DAILY,
    TABLE_KPI_DELIVERY_DAILY,
)

_MERGE_KEYS: dict[str, tuple[str, ...]] = {
    TABLE_DIM_CARRIER: ("carrier_id", "p_date"),
    TABLE_FCT_SHIPMENT: ("shipment_id",),
    TABLE_FCT_DELIVERY_EVENT: ("event_id",),
    TABLE_AGG_SHIPMENT_DAILY: ("p_date", "region_code", "carrier_id"),
    TABLE_KPI_DELIVERY_DAILY: ("p_date", "region_code", "carrier_id"),
}
_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,126}$")
_IAM_ROLE_PATTERN = re.compile(
    r"^arn:(?:aws|aws-us-gov|aws-cn):iam::[0-9]{12}:role/[A-Za-z0-9+=,.@_/-]+$"
)
_PENDING_STATUSES = frozenset({"SUBMITTED", "PICKED", "STARTED"})
_FAILURE_STATUSES = frozenset({"FAILED", "ABORTED"})


class RedshiftConfigurationError(ValueError):
    """Raised when enabled Redshift configuration is incomplete or unsafe."""


class RedshiftStatementError(RuntimeError):
    """Raised when the Redshift Data API reports a failed statement."""

    def __init__(self, statement_id: str, status: str, message: str) -> None:
        super().__init__(f"Redshift statement {statement_id} ended with {status}: {message}")
        self.statement_id = statement_id
        self.status = status


class RedshiftStatementTimeout(TimeoutError):
    """Raised when a Redshift Data API statement exceeds its configured timeout."""

    def __init__(self, statement_id: str, timeout_seconds: float) -> None:
        super().__init__(
            f"Redshift statement {statement_id} did not finish within {timeout_seconds:g} seconds"
        )
        self.statement_id = statement_id
        self.timeout_seconds = timeout_seconds


class RedshiftDataApiClient(Protocol):
    """Small injectable surface used from boto3's Redshift Data API client."""

    def execute_statement(self, **kwargs: Any) -> Mapping[str, Any]: ...

    def batch_execute_statement(self, **kwargs: Any) -> Mapping[str, Any]: ...

    def describe_statement(self, **kwargs: Any) -> Mapping[str, Any]: ...

    def cancel_statement(self, **kwargs: Any) -> Mapping[str, Any]: ...


def _as_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def _contains_placeholder(value: str) -> bool:
    return "${" in value or value.startswith("%") and value.endswith("%")


def validate_identifier(value: str) -> str:
    """Validate a Redshift identifier without silently rewriting it."""
    if not _IDENTIFIER_PATTERN.fullmatch(value):
        raise ValueError(f"Unsafe Redshift identifier: {value!r}")
    return value


def quote_identifier(value: str) -> str:
    """Return a validated, quoted Redshift identifier."""
    return f'"{validate_identifier(value)}"'


def quote_qualified_identifier(schema: str, table: str) -> str:
    """Return a validated, quoted ``schema.table`` identifier."""
    return f"{quote_identifier(schema)}.{quote_identifier(table)}"


def _quote_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _validate_s3_uri(value: str) -> str:
    parsed = urlsplit(value)
    if parsed.scheme != "s3" or not parsed.netloc or not parsed.path.strip("/"):
        raise RedshiftConfigurationError(
            "redshift.source_s3_path must be an s3:// URI with a bucket and prefix"
        )
    if parsed.query or parsed.fragment:
        raise RedshiftConfigurationError(
            "redshift.source_s3_path cannot contain a query string or fragment"
        )
    return value.rstrip("/")


@dataclass(frozen=True)
class RedshiftConfig:
    """Validated configuration for an optional Redshift publication target."""

    enabled: bool = False
    region: str = ""
    database: str = ""
    workgroup_name: str = ""
    cluster_identifier: str = ""
    secret_arn: str = ""
    database_user: str = ""
    iam_role_arn: str = ""
    source_s3_path: str = ""
    staging_schema: str = "transport_staging"
    target_schema: str = "transport_analytics"
    audit_schema: str = "transport_audit"
    poll_interval_seconds: float = 2.0
    timeout_seconds: float = 900.0

    @classmethod
    def from_mapping(cls, config: Mapping[str, Any]) -> RedshiftConfig:
        """Build settings from either the full app config or its Redshift section."""
        raw = config.get("redshift", config)
        if not isinstance(raw, Mapping):
            raise RedshiftConfigurationError("redshift configuration must be a mapping")

        try:
            settings = cls(
                enabled=_as_bool(raw.get("enabled", False)),
                region=str(raw.get("region", "")).strip(),
                database=str(raw.get("database", "")).strip(),
                workgroup_name=str(raw.get("workgroup_name", "")).strip(),
                cluster_identifier=str(raw.get("cluster_identifier", "")).strip(),
                secret_arn=str(raw.get("secret_arn", "")).strip(),
                database_user=str(raw.get("database_user", "")).strip(),
                iam_role_arn=str(raw.get("iam_role_arn", "")).strip(),
                source_s3_path=str(raw.get("source_s3_path", "")).strip(),
                staging_schema=str(raw.get("staging_schema", "transport_staging")).strip(),
                target_schema=str(raw.get("target_schema", "transport_analytics")).strip(),
                audit_schema=str(raw.get("audit_schema", "transport_audit")).strip(),
                poll_interval_seconds=float(raw.get("poll_interval_seconds", 2)),
                timeout_seconds=float(raw.get("timeout_seconds", 900)),
            )
        except (TypeError, ValueError) as exc:
            raise RedshiftConfigurationError(f"Invalid Redshift configuration: {exc}") from exc

        if not settings.enabled:
            return settings
        settings._validate_enabled()
        return settings

    def _validate_enabled(self) -> None:
        required = {
            "region": self.region,
            "database": self.database,
            "iam_role_arn": self.iam_role_arn,
            "source_s3_path": self.source_s3_path,
        }
        missing = [name for name, value in required.items() if not value]
        if missing:
            raise RedshiftConfigurationError(
                "Enabled Redshift configuration is missing: " + ", ".join(sorted(missing))
            )
        unresolved = [name for name, value in required.items() if _contains_placeholder(value)]
        if unresolved:
            raise RedshiftConfigurationError(
                "Enabled Redshift configuration has unresolved placeholders: "
                + ", ".join(sorted(unresolved))
            )
        if bool(self.workgroup_name) == bool(self.cluster_identifier):
            raise RedshiftConfigurationError(
                "Configure exactly one of redshift.workgroup_name or cluster_identifier"
            )
        if self.secret_arn and self.database_user:
            raise RedshiftConfigurationError(
                "secret_arn and database_user are alternative authentication modes"
            )
        if self.workgroup_name and self.database_user:
            raise RedshiftConfigurationError(
                "database_user is only supported for a provisioned cluster"
            )
        if not _IAM_ROLE_PATTERN.fullmatch(self.iam_role_arn):
            raise RedshiftConfigurationError("redshift.iam_role_arn is not a valid IAM role ARN")
        _validate_s3_uri(self.source_s3_path)
        for value in (self.staging_schema, self.target_schema, self.audit_schema):
            try:
                validate_identifier(value)
            except ValueError as exc:
                raise RedshiftConfigurationError(str(exc)) from exc
        if self.poll_interval_seconds <= 0 or self.timeout_seconds <= 0:
            raise RedshiftConfigurationError("Redshift poll interval and timeout must be positive")


@dataclass(frozen=True)
class RedshiftStatementResult:
    """Result metadata returned after a Data API statement finishes."""

    statement_id: str
    status: str
    rows: int | None
    duration_seconds: float


@dataclass(frozen=True)
class RedshiftLoadResult:
    """Structured orchestration result for one curated table load."""

    table: str
    source_path: str
    statement_id: str
    rows: int | None
    duration_seconds: float
    status: str


class RedshiftDataApi:
    """Execute and poll Redshift Data API statements with injectable timing."""

    def __init__(
        self,
        *,
        config: RedshiftConfig,
        client: RedshiftDataApiClient | None = None,
        sleep: Callable[[float], None] = time.sleep,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        self.config = config
        self._sleep = sleep
        self._monotonic = monotonic
        self.client = client if client is not None else self._create_client()

    def _create_client(self) -> RedshiftDataApiClient:
        try:
            import boto3
        except ModuleNotFoundError as exc:  # pragma: no cover - depends on optional install
            raise ModuleNotFoundError(
                "Redshift publishing requires the optional AWS dependency: pip install .[aws]"
            ) from exc
        return boto3.client("redshift-data", region_name=self.config.region)

    def _connection_args(self) -> dict[str, str]:
        args = {"Database": self.config.database}
        if self.config.workgroup_name:
            args["WorkgroupName"] = self.config.workgroup_name
        else:
            args["ClusterIdentifier"] = self.config.cluster_identifier
        if self.config.secret_arn:
            args["SecretArn"] = self.config.secret_arn
        elif self.config.database_user:
            args["DbUser"] = self.config.database_user
        return args

    def execute_sql(self, sql: str, statement_name: str | None = None) -> RedshiftStatementResult:
        """Execute one SQL statement and wait for its terminal state."""
        request: dict[str, Any] = {"Sql": sql, **self._connection_args()}
        if statement_name:
            request["StatementName"] = statement_name
        response = self.client.execute_statement(**request)
        return self.poll_statement(str(response["Id"]))

    def execute_transaction(
        self, sqls: Sequence[str], statement_name: str | None = None
    ) -> RedshiftStatementResult:
        """Execute ordered statements in one Data API transaction."""
        statements = [str(sql).strip() for sql in sqls if str(sql).strip()]
        if not statements:
            raise ValueError("At least one SQL statement is required")
        request: dict[str, Any] = {
            "Sqls": statements,
            "ExecutionMode": "TRANSACTION",
            **self._connection_args(),
        }
        if statement_name:
            request["StatementName"] = statement_name
        response = self.client.batch_execute_statement(**request)
        return self.poll_statement(str(response["Id"]))

    def poll_statement(self, statement_id: str) -> RedshiftStatementResult:
        """Poll a statement until success, failure, or configured timeout."""
        started = self._monotonic()
        while True:
            detail = self.client.describe_statement(Id=statement_id)
            status = str(detail.get("Status", ""))
            if status == "FINISHED":
                result_rows = detail.get("ResultRows")
                rows = (
                    int(result_rows) if result_rows is not None and int(result_rows) >= 0 else None
                )
                api_duration = detail.get("Duration")
                duration = (
                    float(api_duration) / 1_000_000_000
                    if api_duration is not None and float(api_duration) >= 0
                    else self._monotonic() - started
                )
                return RedshiftStatementResult(statement_id, status, rows, duration)
            if status in _FAILURE_STATUSES:
                raise RedshiftStatementError(
                    statement_id, status, str(detail.get("Error") or "No error detail returned")
                )
            if status not in _PENDING_STATUSES:
                raise RedshiftStatementError(
                    statement_id, status or "UNKNOWN", "Unexpected Data API statement status"
                )

            elapsed = self._monotonic() - started
            if elapsed >= self.config.timeout_seconds:
                try:
                    self.client.cancel_statement(Id=statement_id)
                except Exception:
                    pass
                raise RedshiftStatementTimeout(statement_id, self.config.timeout_seconds)
            self._sleep(
                min(self.config.poll_interval_seconds, self.config.timeout_seconds - elapsed)
            )


def build_copy_sql(
    schema: str,
    table: str,
    source_path: str,
    iam_role_arn: str,
) -> str:
    """Build a safe Redshift Parquet COPY statement."""
    return (
        f"COPY {quote_qualified_identifier(schema, table)}\n"
        f"FROM {_quote_literal(source_path)}\n"
        f"IAM_ROLE {_quote_literal(iam_role_arn)}\n"
        "FORMAT AS PARQUET;"
    )


def build_merge_sql(table: str, staging_schema: str, target_schema: str) -> str:
    """Build an idempotent simplified MERGE for a trusted curated table."""
    if table not in _MERGE_KEYS:
        raise ValueError(f"Unsupported Redshift curated table: {table}")
    target = quote_qualified_identifier(target_schema, table)
    source = quote_qualified_identifier(staging_schema, table)
    target_name = quote_identifier(table)
    predicates = " AND ".join(
        f"{target_name}.{quote_identifier(key)} = source.{quote_identifier(key)}"
        for key in _MERGE_KEYS[table]
    )
    return (
        f"MERGE INTO {target}\n"
        f"USING {source} AS source\n"
        f"ON {predicates}\n"
        "REMOVE DUPLICATES;"
    )


def _split_sql_statements(sql: str) -> list[str]:
    without_comments = re.sub(r"(?m)^\s*--.*$", "", sql)
    return [part.strip() for part in without_comments.split(";") if part.strip()]


def _load_bootstrap_sql(sql_dir: Path, config: RedshiftConfig) -> list[str]:
    tokens = {
        "{{staging_schema}}": quote_identifier(config.staging_schema),
        "{{target_schema}}": quote_identifier(config.target_schema),
        "{{audit_schema}}": quote_identifier(config.audit_schema),
    }
    statements: list[str] = []
    for path in sorted(sql_dir.glob("*.sql")):
        rendered = path.read_text(encoding="utf-8")
        for token, value in tokens.items():
            rendered = rendered.replace(token, value)
        if "{{" in rendered or "}}" in rendered:
            raise RedshiftConfigurationError(f"Unresolved SQL template token in {path}")
        statements.extend(_split_sql_statements(rendered))
    if not statements:
        raise FileNotFoundError(f"No Redshift SQL assets found in {sql_dir}")
    return statements


def _source_path(root: str, table: str, batch_id: str) -> str:
    validate_identifier(table)
    safe_batch = re.sub(r"[^A-Za-z0-9_.-]", "_", batch_id)
    if not safe_batch:
        raise ValueError("batch_id must contain at least one safe character")
    return f"{root.rstrip('/')}/{table}/batch_id={safe_batch}"


def export_curated_for_redshift(
    dataframes: Mapping[str, Any], config: RedshiftConfig, batch_id: str
) -> dict[str, str]:
    """Write full-schema, unpartitioned Parquet snapshots for Redshift COPY."""
    destinations: dict[str, str] = {}
    for table in REDSHIFT_TABLE_ORDER:
        if table not in dataframes:
            raise KeyError(f"Missing curated DataFrame for Redshift table: {table}")
        destination = _source_path(config.source_s3_path, table, batch_id)
        dataframes[table].write.mode("overwrite").parquet(destination)
        destinations[table] = destination
    return destinations


def _build_audit_sql(
    config: RedshiftConfig, *, batch_id: str, table: str, source_path: str
) -> tuple[str, str]:
    audit_table = quote_qualified_identifier(config.audit_schema, "etl_load_audit")
    load_id = f"{batch_id}:{table}"
    delete_sql = f"DELETE FROM {audit_table} WHERE load_id = {_quote_literal(load_id)};"
    insert_sql = (
        f"INSERT INTO {audit_table} "
        "(load_id, batch_id, table_name, source_s3_path, status, started_at, finished_at) "
        f"VALUES ({_quote_literal(load_id)}, {_quote_literal(batch_id)}, "
        f"{_quote_literal(table)}, {_quote_literal(source_path)}, "
        "'FINISHED', GETDATE(), GETDATE());"
    )
    return delete_sql, insert_sql


def publish_curated_to_redshift(
    config: Mapping[str, Any],
    *,
    dataframes: Mapping[str, Any] | None,
    batch_id: str,
    client: RedshiftDataApiClient | None = None,
    sql_dir: str | Path | None = None,
    sleep: Callable[[float], None] = time.sleep,
    monotonic: Callable[[], float] = time.monotonic,
    logger: Any | None = None,
) -> list[RedshiftLoadResult]:
    """Publish the five core curated tables when Redshift is enabled."""
    settings = RedshiftConfig.from_mapping(config)
    if not settings.enabled:
        return []
    if dataframes is None:
        raise ValueError("dataframes are required when Redshift publishing is enabled")

    resource_root = Path(__file__).resolve().parents[3]
    resolved_sql_dir = Path(sql_dir) if sql_dir is not None else resource_root / "sql" / "redshift"
    destinations = export_curated_for_redshift(dataframes, settings, batch_id)
    api = RedshiftDataApi(
        client=client,
        config=settings,
        sleep=sleep,
        monotonic=monotonic,
    )

    api.execute_transaction(
        _load_bootstrap_sql(resolved_sql_dir, settings),
        statement_name="transport-redshift-bootstrap",
    )
    results: list[RedshiftLoadResult] = []
    for table in REDSHIFT_TABLE_ORDER:
        source_path = destinations[table]
        audit_delete, audit_insert = _build_audit_sql(
            settings, batch_id=batch_id, table=table, source_path=source_path
        )
        execution = api.execute_transaction(
            [
                f"DELETE FROM {quote_qualified_identifier(settings.staging_schema, table)};",
                build_copy_sql(settings.staging_schema, table, source_path, settings.iam_role_arn),
                build_merge_sql(table, settings.staging_schema, settings.target_schema),
                audit_delete,
                audit_insert,
            ],
            statement_name=f"transport-redshift-{table}",
        )
        result = RedshiftLoadResult(
            table=table,
            source_path=source_path,
            statement_id=execution.statement_id,
            rows=execution.rows,
            duration_seconds=execution.duration_seconds,
            status=execution.status,
        )
        results.append(result)
        if logger is not None:
            logger.info(
                "Redshift load completed table=%s source_path=%s statement_id=%s rows=%s "
                "duration_seconds=%s status=%s",
                result.table,
                sanitize_error(result.source_path, config),
                result.statement_id,
                result.rows,
                result.duration_seconds,
                result.status,
            )
    return results
