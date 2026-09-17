"""Daily entry-point state integration without starting Spark."""

from __future__ import annotations

from pathlib import Path

from transport_etl.common.pipeline_state import LocalPipelineStateStore
from transport_etl.jobs import run_daily_batch as daily
from transport_etl.jobs.run_backfill_batch import run_backfill_batch


def _raw_inputs(tmp_path: Path) -> tuple[Path, Path, Path]:
    raw = tmp_path / "raw"
    reference = tmp_path / "reference"
    raw.mkdir()
    reference.mkdir()
    for entity in ("shipments", "carriers", "delivery_events"):
        (raw / f"{entity}_2026-01-01.csv").write_text(entity, encoding="utf-8")
    (reference / "region_lookup.csv").write_text("region", encoding="utf-8")
    return raw, reference, tmp_path / "state"


def test_daily_state_skips_success_and_reprocesses_modified_file(
    monkeypatch, tmp_path: Path
) -> None:
    raw, reference, state_root = _raw_inputs(tmp_path)
    calls = []
    monkeypatch.setattr(daily, "create_spark_session_from_config", lambda config: object())
    monkeypatch.setattr(daily, "stop_spark_session", lambda spark: None)

    def execute(**kwargs):
        calls.append(kwargs["batch_date"])
        return {"outputs": {"fct_shipment": str(tmp_path / "gold")}}

    monkeypatch.setattr(daily, "_execute_daily_flow", execute)
    overrides = {
        "paths.raw_base_path": str(raw),
        "paths.reference_base_path": str(reference),
        "paths.audit_base_path": str(tmp_path / "audit"),
        "pipeline_state.root_path": str(state_root),
    }
    store = LocalPipelineStateStore(state_root)
    assert daily.run_daily_batch("dev", "2026-01-01", overrides, state_store=store) == 0
    assert daily.run_daily_batch("dev", "2026-01-01", overrides, state_store=store) == 0
    assert calls == ["2026-01-01"]

    (raw / "shipments_2026-01-01.csv").write_text("modified", encoding="utf-8")
    assert daily.run_daily_batch("dev", "2026-01-01", overrides, state_store=store) == 0
    assert calls == ["2026-01-01", "2026-01-01"]


def test_daily_accepts_external_spark_without_creating_or_stopping_it(monkeypatch) -> None:
    spark = object()
    seen = []
    monkeypatch.setattr(
        daily,
        "create_spark_session_from_config",
        lambda config: (_ for _ in ()).throw(AssertionError("created Spark")),
    )
    monkeypatch.setattr(daily, "stop_spark_session", lambda session: seen.append("stopped"))
    monkeypatch.setattr(
        daily,
        "_execute_daily_flow",
        lambda **kwargs: (seen.append(kwargs["spark"]), {"outputs": {}})[1],
    )
    assert (
        daily.run_daily_batch(
            "base",
            "2026-01-01",
            {"pipeline_state.enabled": False},
            spark_session=spark,
        )
        == 0
    )
    assert seen == [spark]


def test_daily_state_failed_publish_retries_then_force_reprocesses(
    monkeypatch, tmp_path: Path
) -> None:
    raw, reference, state_root = _raw_inputs(tmp_path)
    calls = []
    monkeypatch.setattr(daily, "create_spark_session_from_config", lambda config: object())
    monkeypatch.setattr(daily, "stop_spark_session", lambda spark: None)

    def execute(**kwargs):
        calls.append(1)
        if len(calls) == 1:
            raise RuntimeError("critical Redshift publish failed")
        return {"outputs": {"fct_shipment": "gold"}}

    monkeypatch.setattr(daily, "_execute_daily_flow", execute)
    overrides = {
        "paths.raw_base_path": str(raw),
        "paths.reference_base_path": str(reference),
        "pipeline_state.root_path": str(state_root),
    }
    store = LocalPipelineStateStore(state_root)
    assert daily.run_daily_batch("dev", "2026-01-01", overrides, state_store=store) == 1
    assert store.load("2026-01-01")["status"] == "failed"
    assert daily.run_daily_batch("dev", "2026-01-01", overrides, state_store=store) == 0
    assert (
        daily.run_daily_batch(
            "dev", "2026-01-01", {**overrides, "pipeline_state.force": True}, state_store=store
        )
        == 0
    )
    assert len(calls) == 3


def test_backfill_tracks_each_date_and_skips_second_window(monkeypatch, tmp_path: Path) -> None:
    raw, reference, state_root = _raw_inputs(tmp_path)
    for entity in ("shipments", "carriers", "delivery_events"):
        (raw / f"{entity}_2026-01-02.csv").write_text(entity, encoding="utf-8")
    calls = []
    monkeypatch.setattr(daily, "create_spark_session_from_config", lambda config: object())
    monkeypatch.setattr(daily, "stop_spark_session", lambda spark: None)

    def execute(**kwargs):
        calls.append(kwargs["batch_date"])
        return {"outputs": {"fct_shipment": "gold"}}

    monkeypatch.setattr(daily, "_execute_daily_flow", execute)
    overrides = {
        "paths.raw_base_path": str(raw),
        "paths.reference_base_path": str(reference),
        "pipeline_state.root_path": str(state_root),
    }
    for _ in range(2):
        assert run_backfill_batch("dev", "2026-01-01", "2026-01-02", overrides) == 0
    assert calls == ["2026-01-01", "2026-01-02"]
    store = LocalPipelineStateStore(state_root)
    assert store.load("2026-01-01")["status"] == "success"
    assert store.load("2026-01-02")["status"] == "success"


def test_s3_manifest_falls_back_to_undated_file() -> None:
    class _NotFound(Exception):
        response = {"Error": {"Code": "404"}}

    class _S3:
        def head_object(self, **kwargs):
            key = kwargs["Key"]
            if key.endswith("_2026-01-01.csv"):
                raise _NotFound()
            return {"ETag": '"etag"', "ContentLength": 1, "LastModified": "2026-01-01"}

    manifest = daily._resolve_source_manifest(
        "s3://test-bucket/raw", "s3://test-bucket/reference", "2026-01-01", _S3()
    )
    assert [source.entity for source in manifest] == [
        "shipments",
        "carriers",
        "delivery_events",
        "region_lookup",
    ]
    assert all(source.path.endswith(f"{source.entity}.csv") for source in manifest)


def test_source_changed_during_execution_cannot_be_marked_success(
    monkeypatch, tmp_path: Path
) -> None:
    raw, reference, state_root = _raw_inputs(tmp_path)
    monkeypatch.setattr(daily, "create_spark_session_from_config", lambda config: object())
    monkeypatch.setattr(daily, "stop_spark_session", lambda spark: None)

    def execute(**kwargs):
        (raw / "shipments_2026-01-01.csv").write_text("changed during run", encoding="utf-8")
        return {"outputs": {"fct_shipment": "gold"}}

    monkeypatch.setattr(daily, "_execute_daily_flow", execute)
    overrides = {
        "paths.raw_base_path": str(raw),
        "paths.reference_base_path": str(reference),
        "pipeline_state.root_path": str(state_root),
    }
    assert daily.run_daily_batch("dev", "2026-01-01", overrides) == 1
    assert LocalPipelineStateStore(state_root).load("2026-01-01")["status"] == "failed"
