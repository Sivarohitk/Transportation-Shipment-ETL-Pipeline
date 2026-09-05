"""Data quality test for the synthetic operational-data generator.

The generator is the data foundation for late-delivery risk model
training (Phase 8).  This test asserts the dataset satisfies the
basic data-quality contract expected by downstream consumers:

- every shipment has a unique ``shipment_id`` and a non-null
  ``carrier_id`` that resolves to a real carrier record
- every delivery event has a unique ``event_id`` and a non-null
  ``shipment_id`` that resolves to a real shipment
- the realized late rate lies in a realistic operational range
- all numeric columns are non-negative where the schema requires it

The test runs the small deterministic generator configuration so
it stays fast on CI.
"""

from __future__ import annotations

import pytest

from transport_etl.synthetic import (
    GeneratorConfig,
    generate_dataset,
    write_csv_files,
)


def _build_test_dataset(tmp_path) -> tuple:
    """Generate a small dataset and write the CSVs to ``tmp_path``."""
    config = GeneratorConfig(
        shipment_count=300,
        horizon_days=60,
        seed=20260101,
    )
    dataset = generate_dataset(config)
    write_csv_files(dataset, tmp_path)
    return dataset, tmp_path


class TestSyntheticDataQuality:
    """End-to-end data quality assertions for the generated dataset."""

    def test_carrier_id_is_unique_and_resolvable(self, tmp_path) -> None:
        pytest.importorskip("pyspark")

        dataset, tmp_path = _build_test_dataset(tmp_path)
        carrier_ids = {c.carrier_id for c in dataset.carriers}
        assert len(carrier_ids) == len(dataset.carriers), "carrier_id values must be unique"

        shipment_carrier_ids = {s.carrier_id for s in dataset.shipments}
        assert shipment_carrier_ids.issubset(carrier_ids), (
            "shipments reference carrier_ids that do not exist in " "the carrier dimension"
        )

    def test_shipment_id_is_unique(self, tmp_path) -> None:
        dataset, _ = _build_test_dataset(tmp_path)
        shipment_ids = [s.shipment_id for s in dataset.shipments]
        assert len(set(shipment_ids)) == len(shipment_ids), "shipment_id values must be unique"

    def test_event_id_is_unique(self, tmp_path) -> None:
        dataset, _ = _build_test_dataset(tmp_path)
        event_ids = [e.event_id for e in dataset.events]
        assert len(set(event_ids)) == len(event_ids), "event_id values must be unique"

    def test_event_shipment_id_resolves(self, tmp_path) -> None:
        dataset, _ = _build_test_dataset(tmp_path)
        shipment_ids = {s.shipment_id for s in dataset.shipments}
        event_shipment_ids = {e.shipment_id for e in dataset.events}
        assert event_shipment_ids.issubset(
            shipment_ids
        ), "delivery events reference shipment_ids that do not exist"

    def test_late_rate_in_realistic_range(self, tmp_path) -> None:
        """Operational data usually sits in the 5–40% late range.

        A late rate of 0% or 100% would indicate a synthetic
        generator bug (or a target-leakage bug downstream).
        """
        dataset, _ = _build_test_dataset(tmp_path)
        late = sum(
            1
            for s in dataset.shipments
            if s.actual_delivery_ts and s.actual_delivery_ts > s.promised_delivery_ts
        )
        late_rate = late / len(dataset.shipments)
        assert (
            0.05 <= late_rate <= 0.40
        ), f"Late rate {late_rate:.1%} outside realistic [5%, 40%] range"

    def test_distance_and_cost_non_negative(self, tmp_path) -> None:
        dataset, _ = _build_test_dataset(tmp_path)
        for shipment in dataset.shipments:
            assert shipment.distance_miles is not None
            assert shipment.distance_miles >= 0.0
            assert shipment.shipping_cost_usd is not None
            assert shipment.shipping_cost_usd >= 0.0

    def test_promised_transit_is_shorter_than_realised_transit_quartile(self, tmp_path) -> None:
        """Most late shipments should have realised transit longer
        than promised transit.  This is a sanity check on the
        relationship between the generated demand ratio and the
        realised transit time.
        """
        dataset, _ = _build_test_dataset(tmp_path)
        late = [
            s
            for s in dataset.shipments
            if s.actual_delivery_ts and s.actual_delivery_ts > s.promised_delivery_ts
        ]
        assert late, "Test data must include at least one late shipment"
        longer = sum(
            1
            for s in late
            if (s.actual_delivery_ts - s.pickup_ts).total_seconds() / 3600.0
            > (s.promised_delivery_ts - s.pickup_ts).total_seconds() / 3600.0
        )
        # The vast majority of late shipments must have a longer
        # realised transit; the generator adds noise to some
        # on-time records so a small minority may have realised
        # transit shorter than promised but still be "late" because
        # they are right at the boundary.
        assert longer / len(late) >= 0.80, (
            f"Only {longer/len(late):.0%} of late shipments have "
            f"realised transit > promised transit; the generator has "
            f"an inconsistency bug."
        )

    def test_every_shipment_has_at_least_one_delivery_event(self, tmp_path) -> None:
        dataset, _ = _build_test_dataset(tmp_path)
        shipment_ids = {s.shipment_id for s in dataset.shipments}
        shipment_ids_with_delivery = {
            e.shipment_id for e in dataset.events if e.event_type == "DELIVERED"
        }
        assert (
            shipment_ids == shipment_ids_with_delivery
        ), "Every shipment must end with a DELIVERED event"
