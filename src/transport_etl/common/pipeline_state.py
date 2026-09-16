"""Durable batch manifests for date-based, file-level incremental processing.

This is a control-plane checkpoint, not database change data capture. Each
batch date has one authoritative record; only a fully successful run is
eligible for an unchanged-source skip.
"""

from __future__ import annotations

import base64
import hashlib
import json
import os
import re
import tempfile
from contextlib import suppress
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Mapping, Protocol
from urllib.parse import urlsplit

_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def _timestamp() -> str:
    return datetime.now(timezone.utc).isoformat()


def _date_token(value: str) -> str:
    if not _DATE.fullmatch(value):
        raise ValueError(f"Invalid batch date for pipeline state: {value!r}")
    datetime.strptime(value, "%Y-%m-%d")
    return value


def _s3_parts(uri: str) -> tuple[str, str]:
    parsed = urlsplit(uri)
    if parsed.scheme != "s3" or not parsed.netloc or parsed.query or parsed.fragment:
        raise ValueError(f"Expected an S3 URI: {uri!r}")
    return parsed.netloc, parsed.path.lstrip("/")


def _s3_client(client: Any | None) -> Any:
    if client is not None:
        return client
    try:
        import boto3
    except ModuleNotFoundError as exc:  # pragma: no cover - optional dependency
        raise ModuleNotFoundError("S3 pipeline state requires pip install .[aws]") from exc
    return boto3.client("s3")


def _missing_object(exc: Exception) -> bool:
    response = getattr(exc, "response", None)
    if not isinstance(response, Mapping):
        return False
    error = response.get("Error", {})
    return isinstance(error, Mapping) and str(error.get("Code")) in {
        "NoSuchKey",
        "NotFound",
        "404",
    }


def _decode_record(raw: bytes, location: str) -> dict[str, Any]:
    try:
        payload = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"Invalid pipeline state at {location}") from exc
    if not isinstance(payload, dict) or not isinstance(payload.get("status"), str):
        raise ValueError(f"Invalid pipeline state at {location}")
    return payload


class PipelineStateStore(Protocol):
    """Storage-independent per-date checkpoint interface."""

    def load(self, batch_date: str) -> dict[str, Any] | None: ...

    def save(self, batch_date: str, record: Mapping[str, Any]) -> None: ...


class LocalPipelineStateStore:
    """Persist each checkpoint with fsync and atomic same-directory replace."""

    def __init__(self, root: str | Path) -> None:
        self.root = Path(root)

    def _path(self, batch_date: str) -> Path:
        return self.root / "batches" / f"{_date_token(batch_date)}.json"

    def load(self, batch_date: str) -> dict[str, Any] | None:
        path = self._path(batch_date)
        try:
            return _decode_record(path.read_bytes(), str(path))
        except FileNotFoundError:
            return None

    def save(self, batch_date: str, record: Mapping[str, Any]) -> None:
        path = self._path(batch_date)
        path.parent.mkdir(parents=True, exist_ok=True)
        encoded = json.dumps(record, sort_keys=True, separators=(",", ":")).encode("utf-8")
        temporary: str | None = None
        try:
            with tempfile.NamedTemporaryFile(
                mode="wb", prefix=f".{path.stem}-", suffix=".tmp", dir=path.parent, delete=False
            ) as handle:
                temporary = handle.name
                handle.write(encoded)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, path)
        finally:
            if temporary:
                with suppress(FileNotFoundError):
                    os.unlink(temporary)


class S3PipelineStateStore:
    """Persist each checkpoint as one checksummed S3 PUT, never multipart."""

    def __init__(self, root: str, client: Any | None = None) -> None:
        self.bucket, prefix = _s3_parts(root)
        self.prefix = prefix.rstrip("/")
        self.client = _s3_client(client)

    def _key(self, batch_date: str) -> str:
        name = f"batches/{_date_token(batch_date)}.json"
        return f"{self.prefix}/{name}" if self.prefix else name

    def load(self, batch_date: str) -> dict[str, Any] | None:
        key = self._key(batch_date)
        try:
            response = self.client.get_object(Bucket=self.bucket, Key=key)
        except Exception as exc:
            if _missing_object(exc):
                return None
            raise
        return _decode_record(response["Body"].read(), f"s3://{self.bucket}/{key}")

    def save(self, batch_date: str, record: Mapping[str, Any]) -> None:
        encoded = json.dumps(record, sort_keys=True, separators=(",", ":")).encode("utf-8")
        checksum = base64.b64encode(hashlib.md5(encoded).digest()).decode("ascii")  # nosec B324
        self.client.put_object(
            Bucket=self.bucket,
            Key=self._key(batch_date),
            Body=encoded,
            ContentType="application/json",
            ContentMD5=checksum,
        )


def create_state_store(
    root: str, backend: str = "auto", *, s3_client: Any | None = None
) -> PipelineStateStore:
    """Select local or S3 persistence from explicit backend or root URI."""
    selected = backend.lower()
    if selected == "auto":
        selected = "s3" if root.startswith("s3://") else "local"
    if selected == "s3":
        return S3PipelineStateStore(root, client=s3_client)
    if selected == "local" and not root.startswith(("s3://", "dbfs:/")):
        return LocalPipelineStateStore(root)
    raise ValueError(f"Invalid pipeline state backend/root: {backend!r}, {root!r}")


def source_client_for_paths(paths: list[str], client: Any | None = None) -> Any | None:
    """Reuse one injected or lazily created S3 client for source metadata reads."""
    if client is not None:
        return client
    if any(path.startswith("s3://") for path in paths):
        return _s3_client(None)
    return None


@dataclass(frozen=True)
class SourceFile:
    """One resolved source file and stable identity for a batch date."""

    entity: str
    path: str
    fingerprint: str
    modified_at: str | None


def source_file_metadata(entity: str, path: str, *, s3_client: Any | None = None) -> SourceFile:
    """Fingerprint local content, or S3 object version/ETag and metadata."""
    if path.startswith("s3://"):
        bucket, key = _s3_parts(path)
        try:
            metadata = _s3_client(s3_client).head_object(Bucket=bucket, Key=key)
        except Exception as exc:
            if _missing_object(exc):
                raise FileNotFoundError(path) from exc
            raise
        modified = metadata.get("LastModified")
        modified_text = modified.isoformat() if hasattr(modified, "isoformat") else str(modified)
        identity = {
            "version": str(metadata.get("VersionId", "")),
            "etag": str(metadata.get("ETag", "")),
            "size": int(metadata.get("ContentLength", 0)),
            "modified": modified_text,
        }
        digest = hashlib.sha256(json.dumps(identity, sort_keys=True).encode("utf-8")).hexdigest()
        return SourceFile(entity, path, f"s3meta:{digest}", modified_text)

    file_path = Path(path)
    digest = hashlib.sha256()
    with file_path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    modified = datetime.fromtimestamp(file_path.stat().st_mtime, timezone.utc).isoformat()
    return SourceFile(entity, path, f"sha256:{digest.hexdigest()}", modified)


@dataclass(frozen=True)
class BatchOutcome:
    """Whether work was skipped and, if executed, its ETL result."""

    skipped: bool
    result: Mapping[str, Any] | None = None


def process_batch_with_state(
    store: PipelineStateStore,
    batch_date: str,
    run_id: str,
    sources: list[SourceFile],
    config_fingerprint: str,
    execute: Callable[[], Mapping[str, Any]],
    *,
    force: bool = False,
) -> BatchOutcome:
    """Skip only identical successes; persist attempts and final outcomes."""
    _date_token(batch_date)
    manifest = [asdict(source) for source in sources]
    previous = store.load(batch_date)
    if (
        not force
        and previous is not None
        and previous.get("status") == "success"
        and previous.get("sources") == manifest
        and previous.get("config_fingerprint") == config_fingerprint
    ):
        return BatchOutcome(skipped=True)

    record: dict[str, Any] = {
        "batch_date": batch_date,
        "run_id": run_id,
        "sources": manifest,
        "config_fingerprint": config_fingerprint,
        "status": "running",
        "processed_at": _timestamp(),
        "outputs": {},
    }
    store.save(batch_date, record)
    try:
        result = execute()
        record.update(
            status="success",
            processed_at=_timestamp(),
            outputs=dict(result.get("outputs", {})),
            bronze_outputs=dict(result.get("bronze", {})),
            silver_outputs=dict(result.get("silver", {})),
        )
        store.save(batch_date, record)
    except Exception:
        record.update(status="failed", processed_at=_timestamp())
        store.save(batch_date, record)
        raise
    return BatchOutcome(skipped=False, result=result)
