"""Streaming scale fixtures match the daily CSV contract without buffering rows."""

from __future__ import annotations

import csv
from datetime import date

import pytest

from transport_etl.synthetic.scale import ScaleConfig, generate_scale_csv


def _rows(path):
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def test_scale_generator_writes_exact_counts_and_linked_events(tmp_path):
    config = ScaleConfig(
        shipment_count=37,
        event_count=89,
        carrier_count=4,
        batch_date=date(2026, 1, 1),
        seed=7,
    )
    manifest = generate_scale_csv(config, tmp_path)
    shipments = _rows(manifest.paths["shipments"])
    events = _rows(manifest.paths["delivery_events"])
    carriers = _rows(manifest.paths["carriers"])

    assert manifest.counts == {"shipments": 37, "delivery_events": 89, "carriers": 4}
    assert len(shipments) == 37
    assert len(events) == 89
    assert len(carriers) == 4
    assert {row["shipment_id"] for row in events} <= {row["shipment_id"] for row in shipments}
    assert {row["carrier_id"] for row in shipments} <= {row["carrier_id"] for row in carriers}
    assert manifest.paths["region_lookup"].is_file()


def test_scale_generator_is_deterministic(tmp_path):
    config = ScaleConfig(shipment_count=12, event_count=21, carrier_count=3, seed=99)
    first = generate_scale_csv(config, tmp_path / "first")
    second = generate_scale_csv(config, tmp_path / "second")
    for entity in first.paths:
        assert first.paths[entity].read_bytes() == second.paths[entity].read_bytes()


@pytest.mark.parametrize(
    "field,value", [("shipment_count", 0), ("event_count", -1), ("carrier_count", 0)]
)
def test_scale_generator_rejects_invalid_counts(field, value):
    values = {"shipment_count": 10, "event_count": 20, "carrier_count": 2}
    values[field] = value
    with pytest.raises(ValueError):
        ScaleConfig(**values)
