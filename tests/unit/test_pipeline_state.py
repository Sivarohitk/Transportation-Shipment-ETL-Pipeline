"""File-manifest incremental state and durable state-store behavior."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from transport_etl.common.pipeline_state import (
    LocalPipelineStateStore,
    S3PipelineStateStore,
    SourceFile,
    create_state_store,
    process_batch_with_state,
    source_client_for_paths,
    source_file_metadata,
)


def _source() -> list[SourceFile]:
    return [SourceFile("shipments", "shipments_2026-01-01.csv", "sha256:abc", None)]


def test_first_run_and_successful_rerun_skip(tmp_path: Path) -> None:
    store = LocalPipelineStateStore(tmp_path)
    calls = []

    def execute():
        calls.append("run")
        return {"outputs": {"fct_shipment": "curated/fct_shipment"}}

    first = process_batch_with_state(store, "2026-01-01", "run-1", _source(), "config-a", execute)
    second = process_batch_with_state(store, "2026-01-01", "run-2", _source(), "config-a", execute)

    assert first.skipped is False
    assert second.skipped is True
    assert calls == ["run"]
    record = store.load("2026-01-01")
    assert record["status"] == "success"
    assert record["run_id"] == "run-1"
    assert record["sources"][0]["entity"] == "shipments"
    assert record["outputs"] == {"fct_shipment": "curated/fct_shipment"}
    assert record["bronze_outputs"] == {}
    assert record["silver_outputs"] == {}
    assert record["processed_at"]


def test_failure_is_retryable_and_partial_state_is_not_skipped(tmp_path: Path) -> None:
    store = LocalPipelineStateStore(tmp_path)
    with pytest.raises(RuntimeError, match="publish failed"):
        process_batch_with_state(
            store,
            "2026-01-01",
            "run-1",
            _source(),
            "config-a",
            lambda: (_ for _ in ()).throw(RuntimeError("publish failed")),
        )
    assert store.load("2026-01-01")["status"] == "failed"

    store.save("2026-01-01", {**store.load("2026-01-01"), "status": "running"})
    retried = process_batch_with_state(
        store,
        "2026-01-01",
        "run-2",
        _source(),
        "config-a",
        lambda: {"outputs": {}},
    )
    assert retried.skipped is False
    assert store.load("2026-01-01")["status"] == "success"


def test_force_and_changed_source_reprocess(tmp_path: Path) -> None:
    store = LocalPipelineStateStore(tmp_path)
    calls = []

    def execute():
        calls.append(1)
        return {"outputs": {}}

    for source, force in [(_source(), False), (_source(), True), (_source(), False)]:
        process_batch_with_state(
            store, "2026-01-01", f"run-{len(calls)}", source, "config-a", execute, force=force
        )
    modified = [SourceFile("shipments", "shipments_2026-01-01.csv", "sha256:new", None)]
    process_batch_with_state(store, "2026-01-01", "run-3", modified, "config-a", execute)
    assert len(calls) == 3


def test_configuration_change_reprocesses_for_new_publish_target(tmp_path: Path) -> None:
    store = LocalPipelineStateStore(tmp_path)

    def execute():
        return {"outputs": {}}

    process_batch_with_state(store, "2026-01-01", "run-1", _source(), "before", execute)
    outcome = process_batch_with_state(store, "2026-01-01", "run-2", _source(), "after", execute)
    assert outcome.skipped is False


def test_local_store_rejects_corrupt_state(tmp_path: Path) -> None:
    store = LocalPipelineStateStore(tmp_path)
    store.save("2026-01-01", {"status": "success"})
    assert json.loads((tmp_path / "batches" / "2026-01-01.json").read_text())["status"] == "success"
    (tmp_path / "batches" / "2026-01-01.json").write_text("{", encoding="utf-8")
    with pytest.raises(ValueError, match="Invalid pipeline state"):
        store.load("2026-01-01")


def test_local_replace_failure_keeps_previous_checkpoint(monkeypatch, tmp_path: Path) -> None:
    from transport_etl.common import pipeline_state

    store = LocalPipelineStateStore(tmp_path)
    store.save("2026-01-01", {"status": "success"})

    def fail_replace(source, target):
        raise OSError("replace failed")

    monkeypatch.setattr(pipeline_state.os, "replace", fail_replace)
    with pytest.raises(OSError, match="replace failed"):
        store.save("2026-01-01", {"status": "running"})
    assert store.load("2026-01-01") == {"status": "success"}
    assert not list((tmp_path / "batches").glob("*.tmp"))


def test_local_source_fingerprint_detects_content_change(tmp_path: Path) -> None:
    path = tmp_path / "shipments.csv"
    path.write_text("a", encoding="utf-8")
    first = source_file_metadata("shipments", str(path))
    path.write_text("b", encoding="utf-8")
    second = source_file_metadata("shipments", str(path))
    assert first.fingerprint != second.fingerprint
    assert first.modified_at is not None


class _FakeS3:
    def __init__(self):
        self.objects = {}
        self.calls = []

    def get_object(self, **kwargs):
        key = (kwargs["Bucket"], kwargs["Key"])
        if key not in self.objects:
            raise _NotFound()
        return {"Body": _Body(self.objects[key])}

    def put_object(self, **kwargs):
        self.calls.append(kwargs)
        self.objects[(kwargs["Bucket"], kwargs["Key"])] = kwargs["Body"]

    def head_object(self, **kwargs):
        return {"ETag": '"etag"', "ContentLength": 4, "LastModified": "2026-01-01"}


class _Body:
    def __init__(self, body):
        self.body = body

    def read(self):
        return self.body


class _NotFound(Exception):
    response = {"Error": {"Code": "NoSuchKey"}}


def test_s3_state_store_single_object_put_and_read() -> None:
    client = _FakeS3()
    store = S3PipelineStateStore("s3://test-bucket/state", client=client)
    assert store.load("2026-01-01") is None
    store.save("2026-01-01", {"status": "success"})
    assert store.load("2026-01-01") == {"status": "success"}
    assert client.calls[0]["Key"] == "state/batches/2026-01-01.json"
    assert client.calls[0]["ContentMD5"]


def test_s3_source_uses_object_metadata_without_download() -> None:
    client = _FakeS3()
    source = source_file_metadata(
        "shipments", "s3://test-bucket/raw/shipments_2026-01-01.csv", s3_client=client
    )
    assert source.fingerprint.startswith("s3meta:")
    assert source.modified_at == "2026-01-01"


def test_s3_state_read_error_is_not_treated_as_missing() -> None:
    class _Denied(_FakeS3):
        def get_object(self, **kwargs):
            raise PermissionError("denied")

    store = S3PipelineStateStore("s3://test-bucket/state", client=_Denied())
    with pytest.raises(PermissionError, match="denied"):
        store.load("2026-01-01")


def test_state_store_failure_prevents_success_mark(tmp_path: Path) -> None:
    class _FailingStore(LocalPipelineStateStore):
        def save(self, batch_date, record):
            if record["status"] == "success":
                raise OSError("state unavailable")
            super().save(batch_date, record)

    store = _FailingStore(tmp_path)
    with pytest.raises(OSError, match="state unavailable"):
        process_batch_with_state(
            store, "2026-01-01", "run-1", _source(), "config-a", lambda: {"outputs": {}}
        )
    assert store.load("2026-01-01")["status"] != "success"


def test_create_store_auto_backend(tmp_path: Path) -> None:
    assert isinstance(create_state_store(str(tmp_path), "auto"), LocalPipelineStateStore)
    assert isinstance(
        create_state_store("s3://test-bucket/state", "auto", s3_client=_FakeS3()),
        S3PipelineStateStore,
    )


def test_source_client_is_created_once_for_s3_paths(monkeypatch) -> None:
    from transport_etl.common import pipeline_state

    client = _FakeS3()
    calls = []

    def make_client(existing):
        calls.append(existing)
        return client

    monkeypatch.setattr(pipeline_state, "_s3_client", make_client)
    selected = source_client_for_paths(["s3://test-bucket/raw", "s3://test-bucket/state"])
    assert selected is client
    assert calls == [None]
    assert source_client_for_paths(["s3://test-bucket/raw"], selected) is client
    assert calls == [None]
