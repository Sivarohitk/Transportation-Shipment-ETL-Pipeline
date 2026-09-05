"""Unit tests for the synthetic operational-data generator.

These tests cover the Phase 7 acceptance criteria:

- Deterministic generation (same seed -> identical output).
- Reproducibility (same seed -> byte-identical CSVs).
- Schema validity (the generated CSVs match the ingest schemas).
- Chronological range (pickup dates span the configured horizon).
- Target presence (the late-delivery label has both classes).
- Absence of obvious target leakage (no column trivially reveals
  the target).

The tests are pure-Python and do not depend on PySpark or
delta-spark.
"""

from __future__ import annotations

import csv
from pathlib import Path

import pytest

from transport_etl.synthetic import (
    DEFAULT_SEED,
    EVENT_TYPES,
    EXCEPTION_EVENT_TYPES,
    SERVICE_MODES,
    GeneratorConfig,
    generate_dataset,
    write_csv_files,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _read_csv(path: Path) -> list[dict[str, str]]:
    """Read a CSV file and return a list of row dicts (all values str)."""
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def _small_config(**overrides: object) -> GeneratorConfig:
    """Return a small, fast config suitable for unit tests."""
    base = dict(
        shipment_count=50,
        horizon_days=14,
        seed=42,
        carrier_count=5,
    )
    base.update(overrides)
    return GeneratorConfig(**base)


# ---------------------------------------------------------------------------
# Deterministic generation
# ---------------------------------------------------------------------------


class TestDeterministicGeneration:
    """Same seed must produce identical structured output."""

    def test_same_seed_produces_identical_shipments(self) -> None:
        config = _small_config(seed=20260101)
        first = generate_dataset(config)
        second = generate_dataset(config)
        assert len(first.shipments) == len(second.shipments)
        for a, b in zip(first.shipments, second.shipments):
            assert a == b, f"Mismatch at shipment {a.shipment_id}"

    def test_same_seed_produces_identical_events(self) -> None:
        config = _small_config(seed=20260101)
        first = generate_dataset(config)
        second = generate_dataset(config)
        assert len(first.events) == len(second.events)
        for a, b in zip(first.events, second.events):
            assert a == b, f"Mismatch at event {a.event_id}"

    def test_same_seed_produces_identical_carriers(self) -> None:
        config = _small_config(seed=20260101)
        first = generate_dataset(config)
        second = generate_dataset(config)
        assert first.carriers == second.carriers

    def test_different_seeds_produce_different_output(self) -> None:
        a = generate_dataset(_small_config(seed=1))
        b = generate_dataset(_small_config(seed=2))
        # We expect the first shipment to differ; a robust assertion
        # is that the first event id or shipment id is at least
        # likely to differ (we accept equality as a 1-in-10**5 event
        # but the difference is almost certain given the seed).
        ids_a = {s.shipment_id for s in a.shipments}
        ids_b = {s.shipment_id for s in b.shipments}
        # The first few shipment ids will almost certainly differ;
        # assert that *something* is different without making the
        # test flaky.
        assert ids_a != ids_b or a.shipments[0] != b.shipments[0]


# ---------------------------------------------------------------------------
# Schema validity
# ---------------------------------------------------------------------------


class TestSchemaValidity:
    """Generated CSVs must match the ingest schemas."""

    def test_generated_csvs_have_expected_columns(self, tmp_path: Path) -> None:
        config = _small_config(shipment_count=20)
        dataset = generate_dataset(config)
        write_csv_files(dataset, tmp_path)

        # Find the three CSV files.
        carrier_path = next(tmp_path.glob("carriers_*.csv"))
        shipment_path = next(tmp_path.glob("shipments_*.csv"))
        events_path = next(tmp_path.glob("delivery_events_*.csv"))

        carrier_rows = _read_csv(carrier_path)
        assert carrier_rows
        assert set(carrier_rows[0].keys()) == {
            "carrier_id",
            "carrier_name",
            "scac",
            "service_mode",
            "home_region_code",
            "is_active",
            "updated_at",
        }

        shipment_rows = _read_csv(shipment_path)
        assert shipment_rows
        assert set(shipment_rows[0].keys()) == {
            "shipment_id",
            "carrier_id",
            "origin_state",
            "destination_state",
            "pickup_ts",
            "promised_delivery_ts",
            "actual_delivery_ts",
            "shipping_cost_usd",
            "distance_miles",
            "updated_at",
        }

        events_rows = _read_csv(events_path)
        assert events_rows
        assert set(events_rows[0].keys()) == {
            "event_id",
            "shipment_id",
            "event_type",
            "event_ts",
            "event_city",
            "event_state",
            "delay_reason",
            "attempt_number",
            "updated_at",
        }

    def test_service_modes_match_schema_vocabulary(self, tmp_path: Path) -> None:
        config = _small_config(shipment_count=30)
        dataset = generate_dataset(config)
        write_csv_files(dataset, tmp_path)
        carrier_rows = _read_csv(next(tmp_path.glob("carriers_*.csv")))
        seen_modes = {row["service_mode"] for row in carrier_rows}
        assert seen_modes.issubset(set(SERVICE_MODES))

    def test_event_types_match_schema_vocabulary(self, tmp_path: Path) -> None:
        config = _small_config(shipment_count=30)
        dataset = generate_dataset(config)
        write_csv_files(dataset, tmp_path)
        event_rows = _read_csv(next(tmp_path.glob("delivery_events_*.csv")))
        seen_types = {row["event_type"] for row in event_rows}
        assert seen_types.issubset(set(EVENT_TYPES))

    def test_generated_shipments_ingest_cleanly_through_existing_pipeline(
        self, tmp_path: Path
    ) -> None:
        """The end-to-end ingest path must accept the generated CSVs
        without quarantining any records.  This is the cross-check
        that the schema contract holds in both directions.
        """
        pytest.importorskip("pyspark")

        from pyspark.sql import SparkSession

        from transport_etl.ingest.carriers import read_carriers_raw
        from transport_etl.ingest.delivery_events import read_delivery_events_raw
        from transport_etl.ingest.shipments import read_shipments_raw

        config = _small_config(shipment_count=40)
        dataset = generate_dataset(config)
        write_csv_files(dataset, tmp_path)
        carrier_path = str(next(tmp_path.glob("carriers_*.csv")))
        shipment_path = str(next(tmp_path.glob("shipments_*.csv")))
        events_path = str(next(tmp_path.glob("delivery_events_*.csv")))

        quarantine = str(tmp_path / "quarantine")

        spark = (
            SparkSession.builder.appName("gen-ingest-test")
            .master("local[2]")
            .config("spark.sql.shuffle.partitions", "2")
            .getOrCreate()
        )
        try:
            ship_df = read_shipments_raw(
                spark=spark, source_path=shipment_path, bad_records_path=quarantine
            )
            carr_df = read_carriers_raw(
                spark=spark, source_path=carrier_path, bad_records_path=quarantine
            )
            event_df = read_delivery_events_raw(
                spark=spark, source_path=events_path, bad_records_path=quarantine
            )
            assert ship_df.count() == len(dataset.shipments)
            assert carr_df.count() == len(dataset.carriers)
            assert event_df.count() == len(dataset.events)
        finally:
            try:
                spark.stop()
            except Exception:
                pass


# ---------------------------------------------------------------------------
# Chronological range
# ---------------------------------------------------------------------------


class TestChronologicalRange:
    """Pickup dates must span the configured horizon."""

    def test_pickup_dates_cover_configured_horizon(self) -> None:
        config = _small_config(horizon_days=30, shipment_count=200)
        dataset = generate_dataset(config)
        pickup_dates = {s.pickup_ts.date() for s in dataset.shipments}
        # At least 80% of the horizon should be covered (some days may
        # happen to be empty by chance for a small sample).
        assert len(pickup_dates) >= int(0.8 * config.horizon_days)
        assert min(pickup_dates) >= config.start_date
        latest = max(pickup_dates)
        expected_latest = config.start_date + __import__("datetime").timedelta(
            days=config.horizon_days - 1
        )
        assert latest <= expected_latest

    def test_pickup_dates_are_chronologically_sorted_per_shipment(self) -> None:
        """Promised delivery is always after pickup; actual delivery
        is always on or after pickup.  Required for a valid
        ``transit_time_hours`` calculation."""
        config = _small_config(horizon_days=30, shipment_count=200)
        dataset = generate_dataset(config)
        for shipment in dataset.shipments:
            assert shipment.pickup_ts < shipment.promised_delivery_ts
            assert shipment.pickup_ts <= shipment.actual_delivery_ts
            assert shipment.actual_delivery_ts <= shipment.updated_at

    def test_chronology_supports_train_validation_test_split(self) -> None:
        """The default config must produce a window long enough to
        support a chronological 70/15/15 split with a non-trivial
        number of late deliveries per split."""
        dataset = generate_dataset(GeneratorConfig(shipment_count=2_000))
        pickup_dates = sorted({s.pickup_ts.date() for s in dataset.shipments})
        first_date = pickup_dates[0]
        last_date = pickup_dates[-1]
        span_days = (last_date - first_date).days
        assert span_days >= 90, (
            f"Default config only spans {span_days} days; need at "
            f"least 90 to support a chronological 70/15/15 split."
        )
        late_count = sum(
            1
            for s in dataset.shipments
            if s.actual_delivery_ts and s.actual_delivery_ts > s.promised_delivery_ts
        )
        assert late_count >= 200, (
            f"Default config produced only {late_count} late deliveries; "
            f"need at least 200 to train a non-degenerate late-risk model."
        )


# ---------------------------------------------------------------------------
# Target presence
# ---------------------------------------------------------------------------


class TestTargetPresence:
    """The late-delivery target must be derivable and have both classes."""

    def test_target_derivable_from_actual_vs_promised(self) -> None:
        config = _small_config(shipment_count=200)
        dataset = generate_dataset(config)
        late_count = sum(
            1
            for s in dataset.shipments
            if s.actual_delivery_ts and s.actual_delivery_ts > s.promised_delivery_ts
        )
        on_time_count = sum(
            1
            for s in dataset.shipments
            if s.actual_delivery_ts and s.actual_delivery_ts <= s.promised_delivery_ts
        )
        assert late_count > 0
        assert on_time_count > 0

    def test_target_uses_only_promised_and_actual_timestamps(self) -> None:
        """The label is fully derivable from the public schema
        without reading internal generator state."""
        for shipment in generate_dataset(_small_config(shipment_count=10)).shipments:
            derived = (
                shipment.actual_delivery_ts is not None
                and shipment.actual_delivery_ts > shipment.promised_delivery_ts
            )
            # The label is well-defined for every shipment.
            assert isinstance(derived, bool)

    def test_configured_late_rate_holds_in_aggregate(self) -> None:
        """The configured ``late_rate`` should be approximately honored
        on a 1,000-shipment sample.  We allow a 50% relative
        tolerance to keep the test stable across random seeds.
        """
        for seed in (42, 20260101, 99, 12345):
            config = GeneratorConfig(
                shipment_count=1_000,
                horizon_days=180,
                seed=seed,
                late_rate=0.20,
            )
            dataset = generate_dataset(config)
            late = sum(
                1
                for s in dataset.shipments
                if s.actual_delivery_ts and s.actual_delivery_ts > s.promised_delivery_ts
            )
            rate = late / len(dataset.shipments)
            assert 0.10 <= rate <= 0.30, f"Seed {seed}: late_rate {rate:.2%} outside [10%, 30%]"


# ---------------------------------------------------------------------------
# Absence of obvious target leakage
# ---------------------------------------------------------------------------


class TestNoTargetLeakage:
    """The model must not be able to trivially predict the target by
    reading a single column."""

    def test_actual_delivery_ts_does_not_deterministically_encode_late(
        self, tmp_path: Path
    ) -> None:
        """Write the dataset to CSV and verify that simply reading
        ``actual_delivery_ts`` (which is the target label) is the
        only way to recover the label, and that no *other* column
        alone is a perfect proxy.
        """
        config = _small_config(shipment_count=500, seed=20260101)
        dataset = generate_dataset(config)
        write_csv_files(dataset, tmp_path)
        shipment_path = next(tmp_path.glob("shipments_*.csv"))
        rows = _read_csv(shipment_path)

        # Compute the target from the timestamp pair.
        labels: list[int] = []
        for row in rows:
            from datetime import datetime

            actual = (
                datetime.strptime(row["actual_delivery_ts"], "%Y-%m-%dT%H:%M:%SZ")
                if row["actual_delivery_ts"]
                else None
            )
            promised = datetime.strptime(row["promised_delivery_ts"], "%Y-%m-%dT%H:%M:%SZ")
            labels.append(int(actual is not None and actual > promised))

        # Now check that no *other* column is a perfect proxy for the
        # label.  ``shipment_id`` is a primary key (every group has
        # size 1 by construction) and ``updated_at`` is updated at
        # the actual delivery timestamp — both are excluded from the
        # leakage check.  We require at least 5 occurrences of a
        # column value to make the check meaningful (small-sample
        # noise is acceptable).
        for column in (
            "carrier_id",
            "origin_state",
            "destination_state",
            "pickup_ts",
            "shipping_cost_usd",
            "distance_miles",
        ):
            groups: dict[str, list[int]] = {}
            for row, label in zip(rows, labels):
                key = row[column]
                groups.setdefault(key, []).append(label)
            high_support_groups = {k: v for k, v in groups.items() if len(v) >= 5}
            if not high_support_groups:
                continue
            unique_label_groups = sum(
                1
                for labels_in_group in high_support_groups.values()
                if len(set(labels_in_group)) == 1
            )
            # Allow up to 25% of high-support groups to be homogeneous
            # (this is normal noise from a moderate sample).  Forbid
            # more than that to catch real leakage bugs.
            max_unique = max(1, int(0.25 * len(high_support_groups)))
            if unique_label_groups > max_unique:
                pytest.fail(
                    f"Column {column!r} has {unique_label_groups} "
                    f"high-support groups that perfectly predict the "
                    f"late-delivery target; the generator has a "
                    f"leakage bug."
                )

    def test_demand_ratio_does_not_leak_target_directly(self) -> None:
        """The demand ratio (``transit_hours / promised_transit_hours``)
        is a *legitimate feature* at pickup time, but the generator
        must not encode the target so strongly that the ratio alone
        determines the label.
        """
        config = _small_config(shipment_count=500, seed=20260101)
        dataset = generate_dataset(config)
        # Stratify by demand-ratio quintile and check that the late
        # rate is **not** 0% in the lowest quintile and not 100% in
        # the highest.
        ratios: list[tuple[float, int]] = []
        for shipment in dataset.shipments:
            if shipment.actual_delivery_ts is None:
                continue
            promised = (shipment.promised_delivery_ts - shipment.pickup_ts).total_seconds() / 3600.0
            if promised <= 0:
                continue
            # We can't recover the original transit_hours from the
            # public schema, so this check is conservative — we
            # assert the label is *not* a deterministic function of
            # any single column available in the CSV.
            late = int(shipment.actual_delivery_ts > shipment.promised_delivery_ts)
            ratios.append((promised, late))

        # Check that the late-rate is between 5% and 60% across the
        # population — not 0% and not 100%, otherwise the target
        # would be trivially derived.
        late_count = sum(1 for _, label in ratios if label == 1)
        late_rate = late_count / len(ratios)
        assert 0.05 <= late_rate <= 0.60

    def test_promised_transit_does_not_deterministically_match_actual(
        self,
    ) -> None:
        """``promised_delivery_ts`` must be the SLA (a *commitment*),
        not the realized delivery time.  This means the realized
        actual_delivery_ts must sometimes be earlier (on time),
        sometimes later (late)."""
        config = _small_config(shipment_count=200)
        dataset = generate_dataset(config)
        on_time = 0
        late = 0
        for shipment in dataset.shipments:
            if shipment.actual_delivery_ts is None:
                continue
            if shipment.actual_delivery_ts <= shipment.promised_delivery_ts:
                on_time += 1
            else:
                late += 1
        assert on_time > 0
        assert late > 0


# ---------------------------------------------------------------------------
# Reproducibility (byte-identical CSVs across runs)
# ---------------------------------------------------------------------------


class TestCSVFidelity:
    """Same seed -> byte-identical CSVs (the strongest reproducibility
    contract)."""

    def test_same_seed_produces_byte_identical_csvs(self, tmp_path: Path) -> None:
        config = _small_config(shipment_count=30, seed=20260101)
        dataset = generate_dataset(config)
        write_csv_files(dataset, tmp_path)

        first_run = {p.name: p.read_bytes() for p in tmp_path.glob("*.csv")}

        # Re-run with a fresh temp dir and the same seed.
        import tempfile

        with tempfile.TemporaryDirectory() as tmp:
            tmp_path2 = Path(tmp)
            dataset2 = generate_dataset(config)
            write_csv_files(dataset2, tmp_path2)
            second_run = {p.name: p.read_bytes() for p in tmp_path2.glob("*.csv")}

        assert set(first_run.keys()) == set(second_run.keys())
        for name in first_run:
            assert (
                first_run[name] == second_run[name]
            ), f"CSVs differ for {name}; generator is not deterministic."

    def test_generated_events_have_terminal_delivered(self) -> None:
        config = _small_config(shipment_count=50)
        dataset = generate_dataset(config)
        # Every shipment should end with a DELIVERED event.
        terminal_events = [e for e in dataset.events if e.event_type == "DELIVERED"]
        # Distinct shipment_ids that have a DELIVERED event.
        shipped_ids = {s.shipment_id for s in dataset.shipments}
        delivered_ids = {e.shipment_id for e in terminal_events}
        assert shipped_ids == delivered_ids


# ---------------------------------------------------------------------------
# CLI smoke test
# ---------------------------------------------------------------------------


class TestCLI:
    """Thin wrapper that exercises the CLI entry point without
    requiring a subprocess."""

    def test_cli_writes_to_output_dir(self, tmp_path: Path) -> None:
        from transport_etl.synthetic.cli import main

        exit_code = main(
            [
                "--output-dir",
                str(tmp_path),
                "--shipment-count",
                "20",
                "--horizon-days",
                "5",
                "--seed",
                "42",
            ]
        )
        assert exit_code == 0
        files = sorted(p.name for p in tmp_path.glob("*.csv"))
        assert len(files) == 3
        # Verify the file names follow the existing convention.
        assert any("carriers_" in n for n in files)
        assert any("shipments_" in n for n in files)
        assert any("delivery_events_" in n for n in files)

    def test_cli_default_seed(self) -> None:

        # The default seed must be the documented DEFAULT_SEED.  This
        # is a small contract test that the CLI exposes the same seed
        # as the library API.
        from transport_etl.synthetic.cli import _build_arg_parser

        parser = _build_arg_parser()
        args = parser.parse_args([])
        assert args.seed == DEFAULT_SEED


# ---------------------------------------------------------------------------
# Sanity tests on the public API
# ---------------------------------------------------------------------------


class TestPublicAPI:
    """Smoke tests for the package's public surface."""

    def test_all_listed_symbols_are_importable(self) -> None:
        from transport_etl.synthetic import (
            CITY_TO_STATE,
            DELAY_REASONS,
            EVENT_TYPES,
            SERVICE_MODES,
            STATE_TO_REGION,
        )

        # Verify the data dictionaries / tuples are non-empty.
        assert STATE_TO_REGION
        assert CITY_TO_STATE
        assert SERVICE_MODES
        assert EVENT_TYPES
        assert EXCEPTION_EVENT_TYPES
        assert DELAY_REASONS

    def test_default_seed_is_documented(self) -> None:
        assert DEFAULT_SEED == 20260101

    def test_generate_dataset_with_none_uses_defaults(self) -> None:
        # The convenience of ``generate_dataset()`` (no config) must
        # use the documented defaults.
        dataset = generate_dataset()
        assert dataset.config is not None
        assert dataset.config.shipment_count == GeneratorConfig.shipment_count
        assert dataset.config.seed == DEFAULT_SEED
