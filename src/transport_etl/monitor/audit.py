"""Secret-safe run audit records and local/S3 persistence adapters."""

from __future__ import annotations

import base64
import hashlib
import json
import os
import re
import tempfile
import time
from contextlib import suppress
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Mapping, Protocol
from urllib.parse import urlsplit

from transport_etl.common.aws_retry import RetryPolicy, retry_aws_call

_RUN_ID = re.compile(r"^[A-Za-z0-9_.-]{1,160}$")
_SENSITIVE_KEY = re.compile(r"secret|password|token|credential|access.key|private.key", re.I)
_SECRET_ASSIGNMENT = re.compile(
    r"(?i)\b(password|passwd|pwd|secret|token|access[_-]?key|credential)\s*[:=]\s*[^\s,;]+"
)
_CREDENTIAL_URI = re.compile(r"\b[\w+.-]+://[^\s,;]*@[^\s,;]+")
_AWS_ACCESS_KEY = re.compile(r"\b(?:AKIA|ASIA)[A-Z0-9]{16}\b")


def _sensitive_values(config: Mapping[str, Any]) -> list[str]:
    values: list[str] = []

    def visit(node: Mapping[str, Any]) -> None:
        for key, value in node.items():
            if isinstance(value, Mapping):
                visit(value)
            elif _SENSITIVE_KEY.search(str(key)) and value not in (None, ""):
                values.append(str(value))

    visit(config)
    return sorted(set(values), key=len, reverse=True)


def sanitize_error(message: Any, config: Mapping[str, Any] | None = None) -> str:
    """Remove configured secrets and common credential forms from error text."""
    cleaned = str(message)
    for value in _sensitive_values(config or {}):
        cleaned = cleaned.replace(value, "[REDACTED]")
    cleaned = _CREDENTIAL_URI.sub("[REDACTED_CONNECTION]", cleaned)
    cleaned = _SECRET_ASSIGNMENT.sub(lambda match: f"{match.group(1)}=[REDACTED]", cleaned)
    cleaned = _AWS_ACCESS_KEY.sub("[REDACTED]", cleaned)
    return cleaned[:500]


def build_audit_record(
    *,
    run_id: str,
    job: str,
    environment: str,
    batch_date: str | None,
    started_at: str,
    completed_at: str,
    status: str,
    source_rows: Mapping[str, int] | None = None,
    clean_rows: Mapping[str, int] | None = None,
    rejected_rows: Mapping[str, int] | None = None,
    curated_rows: Mapping[str, int] | None = None,
    quality_failures: Mapping[str, list[str]] | None = None,
    redshift_status: str = "disabled",
    glue_status: str = "disabled",
    outputs: Mapping[str, str] | None = None,
    error: Exception | None = None,
    config: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Build one completed attempt record with count and publisher context."""
    elapsed = (
        datetime.fromisoformat(completed_at) - datetime.fromisoformat(started_at)
    ).total_seconds()
    return {
        "run_id": run_id,
        "job": job,
        "environment": environment,
        "batch_date": batch_date,
        "started_at": started_at,
        "completed_at": completed_at,
        "status": status,
        "duration_seconds": max(0.0, elapsed),
        "source_rows": dict(source_rows or {}),
        "clean_rows": dict(clean_rows or {}),
        "rejected_rows": dict(rejected_rows or {}),
        "curated_rows": dict(curated_rows or {}),
        "quality_failures": dict(quality_failures or {}),
        "redshift_status": redshift_status,
        "glue_status": glue_status,
        "outputs": {key: sanitize_error(value, config) for key, value in (outputs or {}).items()},
        "error_type": type(error).__name__ if error is not None else None,
        "error_message": sanitize_error(error, config) if error is not None else None,
    }


class AuditStore(Protocol):
    """Persist one run-attempt audit record."""

    def write(self, record: Mapping[str, Any]) -> str: ...


def _run_id(record: Mapping[str, Any]) -> str:
    value = str(record.get("run_id", ""))
    if not _RUN_ID.fullmatch(value):
        raise ValueError(f"Invalid audit run ID: {value!r}")
    return value


def _encoded(record: Mapping[str, Any]) -> bytes:
    return json.dumps(record, sort_keys=True, separators=(",", ":")).encode("utf-8")


class LocalJsonAuditStore:
    """Write one JSON file per run using an atomic same-directory replace."""

    def __init__(self, root: str | Path) -> None:
        self.root = Path(root)

    def write(self, record: Mapping[str, Any]) -> str:
        path = self.root / f"{_run_id(record)}.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary: str | None = None
        try:
            with tempfile.NamedTemporaryFile(
                mode="wb", prefix=f".{path.stem}-", suffix=".tmp", dir=path.parent, delete=False
            ) as handle:
                temporary = handle.name
                handle.write(_encoded(record))
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, path)
        finally:
            if temporary:
                with suppress(FileNotFoundError):
                    os.unlink(temporary)
        return str(path)


class S3AuditStore:
    """Write one checksummed S3 object per run with an injectable client."""

    def __init__(
        self,
        root: str,
        client: Any | None = None,
        *,
        retry_policy: RetryPolicy | None = None,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        parsed = urlsplit(root)
        if parsed.scheme != "s3" or not parsed.netloc or parsed.query or parsed.fragment:
            raise ValueError(f"Invalid S3 audit root: {root!r}")
        self.bucket = parsed.netloc
        self.prefix = parsed.path.strip("/")
        if client is None:
            try:
                import boto3
            except ModuleNotFoundError as exc:  # pragma: no cover - optional dependency
                raise ModuleNotFoundError("S3 audit requires pip install .[aws]") from exc
            client = boto3.client("s3")
        self.client = client
        self.retry_policy = retry_policy or RetryPolicy()
        self.sleep = sleep

    def write(self, record: Mapping[str, Any]) -> str:
        key = "/".join(filter(None, (self.prefix, f"{_run_id(record)}.json")))
        body = _encoded(record)
        checksum = base64.b64encode(hashlib.md5(body).digest()).decode("ascii")  # nosec B324
        retry_aws_call(
            lambda: self.client.put_object(
                Bucket=self.bucket,
                Key=key,
                Body=body,
                ContentType="application/json",
                ContentMD5=checksum,
            ),
            operation="s3.put_object",
            policy=self.retry_policy,
            sleep=self.sleep,
        )
        return f"s3://{self.bucket}/{key}"


def create_audit_store(
    config: Mapping[str, Any], *, client: Any | None = None
) -> AuditStore | None:
    """Build the configured audit adapter; disabled profiles do no I/O."""
    settings = config.get("audit", {})
    if not isinstance(settings, Mapping):
        raise ValueError("audit configuration must be a mapping")
    if not bool(settings.get("enabled", False)):
        return None
    paths = config.get("paths", {})
    if not isinstance(paths, Mapping):
        raise ValueError("paths configuration must be a mapping")
    configured = str(settings.get("path", "")).strip()
    base = configured or str(paths.get("audit_base_path", ""))
    if not base:
        raise ValueError("audit.path or paths.audit_base_path is required")
    root = configured or (
        base.rstrip("/") + "/pipeline_audit"
        if base.startswith("s3://")
        else str(Path(base) / "pipeline_audit")
    )
    backend = str(settings.get("backend", "auto")).lower()
    if backend == "auto":
        backend = "s3" if root.startswith("s3://") else "local"
    if backend == "s3":
        return S3AuditStore(root, client=client, retry_policy=RetryPolicy.from_config(config))
    if backend == "local" and "://" not in root and not root.startswith("dbfs:/"):
        return LocalJsonAuditStore(root)
    raise ValueError(f"Invalid audit backend/path: {backend!r}, {root!r}")
