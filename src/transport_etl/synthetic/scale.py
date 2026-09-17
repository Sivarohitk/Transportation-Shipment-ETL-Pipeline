"""Stream synthetic daily CSV fixtures without holding the dataset in memory.

This generator is for load testing, not the in-memory ML demonstration data.
Its simple distributions are deliberate and are not production-like evidence.
"""

from __future__ import annotations

import argparse
import csv
import json
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

STATES = ("CA", "TX", "NY", "IL", "GA", "WA")
REGIONS = ("WEST", "SOUTH", "NORTHEAST", "MIDWEST", "SOUTH", "WEST")
SHIPMENT_COLUMNS = (
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
)
CARRIER_COLUMNS = (
    "carrier_id",
    "carrier_name",
    "scac",
    "service_mode",
    "home_region_code",
    "is_active",
    "updated_at",
)
EVENT_COLUMNS = (
    "event_id",
    "shipment_id",
    "event_type",
    "event_ts",
    "event_city",
    "event_state",
    "delay_reason",
    "attempt_number",
    "updated_at",
)
EVENT_TYPES = ("PICKED_UP", "IN_TRANSIT", "OUT_FOR_DELIVERY", "DELIVERED", "DELAYED")


@dataclass(frozen=True)
class ScaleConfig:
    """Exact row counts for one synthetic daily batch."""

    shipment_count: int = 10_000
    event_count: int = 20_000
    carrier_count: int = 32
    batch_date: date = date(2026, 1, 1)
    seed: int = 20260101

    def __post_init__(self) -> None:
        if self.shipment_count < 1 or self.event_count < 0 or self.carrier_count < 1:
            raise ValueError(
                "shipment_count and carrier_count must be positive; event_count nonnegative"
            )


@dataclass(frozen=True)
class ScaleManifest:
    """Paths and expected input rows for a generated batch."""

    paths: dict[str, Path]
    counts: dict[str, int]


def _timestamp(value: datetime) -> str:
    return value.strftime("%Y-%m-%dT%H:%M:%SZ")


def _pickup(config: ScaleConfig, index: int) -> datetime:
    return datetime.combine(config.batch_date, datetime.min.time(), timezone.utc) + timedelta(
        hours=8, minutes=(index + config.seed) % 600
    )


def _shipment_row(config: ScaleConfig, index: int) -> tuple[str, ...]:
    pickup = _pickup(config, index)
    promised = pickup + timedelta(hours=48)
    actual = pickup + timedelta(hours=52 if index % 5 == 0 else 44)
    return (
        f"SHP{index + 1:012d}",
        f"CAR{(index + config.seed) % config.carrier_count + 1:06d}",
        STATES[(index * 3 + config.seed) % len(STATES)],
        STATES[(index + config.seed + 1) % len(STATES)],
        _timestamp(pickup),
        _timestamp(promised),
        _timestamp(actual),
        f"{100 + index % 1000:.2f}",
        f"{50 + index % 2000:.2f}",
        _timestamp(actual + timedelta(minutes=1)),
    )


def _event_row(config: ScaleConfig, index: int) -> tuple[str, ...]:
    shipment_index = index % config.shipment_count
    event_type = EVENT_TYPES[index % len(EVENT_TYPES)]
    pickup = _pickup(config, shipment_index)
    event_time = (
        pickup + timedelta(hours=52 if shipment_index % 5 == 0 else 44)
        if event_type == "DELIVERED"
        else pickup + timedelta(hours=1 + index % 24)
    )
    return (
        f"EVT{index + 1:012d}",
        f"SHP{shipment_index + 1:012d}",
        event_type,
        _timestamp(event_time),
        "Synthetic City",
        STATES[(shipment_index + config.seed + 1) % len(STATES)],
        "WEATHER" if event_type == "DELAYED" else "",
        "1" if event_type == "DELIVERED" else "",
        _timestamp(event_time + timedelta(minutes=1)),
    )


def generate_scale_csv(config: ScaleConfig, output_root: str | Path) -> ScaleManifest:
    """Write four schema-compatible CSVs using O(1) row buffering.

    The generated data is synthetic. A single uncompressed file per entity
    preserves the daily job's filename contract and remains splittable by Spark.
    """
    root = Path(output_root)
    raw = root / "raw"
    reference = root / "reference"
    raw.mkdir(parents=True, exist_ok=True)
    reference.mkdir(parents=True, exist_ok=True)
    label = config.batch_date.isoformat()
    paths = {
        "shipments": raw / f"shipments_{label}.csv",
        "carriers": raw / f"carriers_{label}.csv",
        "delivery_events": raw / f"delivery_events_{label}.csv",
        "region_lookup": reference / "region_lookup.csv",
    }
    with paths["carriers"].open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(CARRIER_COLUMNS)
        for index in range(config.carrier_count):
            writer.writerow(
                (
                    f"CAR{index + 1:06d}",
                    f"Synthetic Carrier {index + 1}",
                    f"S{index % 1000:03d}",
                    ("FTL", "LTL", "PARCEL")[index % 3],
                    REGIONS[index % len(REGIONS)],
                    "true",
                    _timestamp(_pickup(config, 0)),
                )
            )
    with paths["shipments"].open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(SHIPMENT_COLUMNS)
        for index in range(config.shipment_count):
            writer.writerow(_shipment_row(config, index))
    with paths["delivery_events"].open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(EVENT_COLUMNS)
        for index in range(config.event_count):
            writer.writerow(_event_row(config, index))
    with paths["region_lookup"].open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(("state_code", "region_code"))
        writer.writerows(zip(STATES, REGIONS))
    return ScaleManifest(
        paths=paths,
        counts={
            "shipments": config.shipment_count,
            "delivery_events": config.event_count,
            "carriers": config.carrier_count,
        },
    )


def main(argv: list[str] | None = None) -> int:
    """Generate a reproducible, git-ignored scale fixture from the CLI."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-root", type=Path, default=Path("data/generated/scale"))
    parser.add_argument("--shipments", type=int, default=10_000)
    parser.add_argument("--events", type=int, default=20_000)
    parser.add_argument("--carriers", type=int, default=32)
    parser.add_argument("--run-date", type=date.fromisoformat, default=date(2026, 1, 1))
    parser.add_argument("--seed", type=int, default=20260101)
    args = parser.parse_args(argv)
    config = ScaleConfig(args.shipments, args.events, args.carriers, args.run_date, args.seed)
    manifest = generate_scale_csv(config, args.output_root)
    print(
        json.dumps(
            {
                "synthetic": True,
                "counts": manifest.counts,
                "paths": {key: str(path) for key, path in manifest.paths.items()},
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":  # pragma: no cover - exercised through main()
    raise SystemExit(main())
