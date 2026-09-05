"""Deterministic synthetic operational-data generator.

The generator produces realistic-but-synthetic shipment history that
matches the existing ingest schemas in
``config/schemas/*.schema.json``.  The output is intended for portfolio
demonstration and late-delivery risk model evaluation; it is **not**
production data.

**Why a generator instead of a committed large dataset?**

The committed sample in ``data/sample/raw`` contains 12 shipments
spanning 5 days.  That is far too small for a chronologically valid
train/validation/test split on late-delivery risk (the positive class
would be roughly 4 records, producing meaningless metrics).

The generator produces the additional rows on demand and writes them
to ``data/generated/``, which is git-ignored.  Committed artefacts
remain small (a deterministic 20-shipment fixture and the generator
itself) so the repository stays reviewable.

**Reproducibility**

- All randomness flows through a single ``random.Random(seed)``
  instance — there is no hidden global state.
- The default seed (``DEFAULT_SEED = 20260101``) is fixed in
  ``constants``.  The CLI / API accept an override.
- The same seed produces byte-identical output for the same
  configuration (verified by ``test_deterministic_generation``).
"""

from __future__ import annotations

import csv
import json
import logging
import random
from dataclasses import dataclass, field, replace
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

LOGGER = logging.getLogger("transport_etl.synthetic")

# ---------------------------------------------------------------------------
# Reference data — same as the existing sample fixtures
# ---------------------------------------------------------------------------

#: State → region mapping, identical to ``data/sample/reference/region_lookup.csv``.
STATE_TO_REGION: dict[str, str] = {
    "AZ": "WEST",
    "CA": "WEST",
    "CO": "WEST",
    "ID": "WEST",
    "NV": "WEST",
    "NM": "WEST",
    "WA": "WEST",
    "FL": "SOUTH",
    "GA": "SOUTH",
    "NC": "SOUTH",
    "TN": "SOUTH",
    "TX": "SOUTH",
    "IL": "MIDWEST",
    "OH": "MIDWEST",
    "CT": "NORTHEAST",
    "MA": "NORTHEAST",
    "NJ": "NORTHEAST",
    "NY": "NORTHEAST",
    "PA": "NORTHEAST",
}

#: Allowed service modes (from the carriers schema).
SERVICE_MODES: tuple[str, ...] = ("FTL", "LTL", "PARCEL")

#: Event types (from the delivery_events schema).
EVENT_TYPES: tuple[str, ...] = (
    "PICKED_UP",
    "IN_TRANSIT",
    "OUT_FOR_DELIVERY",
    "DELIVERY_ATTEMPT",
    "DELIVERED",
    "DELAYED",
    "EXCEPTION",
    "HOLD",
)

#: Terminal event types (delivery completed or abandoned).
TERMINAL_EVENT_TYPES: frozenset[str] = frozenset({"DELIVERED"})

#: Operational exception event types.
EXCEPTION_EVENT_TYPES: frozenset[str] = frozenset({"DELAYED", "EXCEPTION", "HOLD"})

#: Top-N delay reasons observed in the source sample.
DELAY_REASONS: tuple[str, ...] = (
    "WEATHER",
    "MECHANICAL",
    "ROAD_CLOSURE",
    "ADDRESS_ISSUE",
    "NO_RECIPIENT",
    "BUSINESS_CLOSED",
    "MISSING_DESTINATION",
    "AWAITING_ADDRESS_UPDATE",
)

#: City → state mapping for synthetic events.  Values are biased to
#: the same regions as STATE_TO_REGION so the resulting shipment
#: and event datasets reconcile.  Multiple cities per state for
#: variety.
CITY_TO_STATE: dict[str, str] = {
    "Los Angeles": "CA",
    "San Diego": "CA",
    "San Francisco": "CA",
    "Sacramento": "CA",
    "Phoenix": "AZ",
    "Tucson": "AZ",
    "Las Vegas": "NV",
    "Reno": "NV",
    "Denver": "CO",
    "Colorado Springs": "CO",
    "Albuquerque": "NM",
    "Santa Fe": "NM",
    "Boise": "ID",
    "Seattle": "WA",
    "Tacoma": "WA",
    "Spokane": "WA",
    "Portland": "OR",
    "Eugene": "OR",
    "Houston": "TX",
    "Dallas": "TX",
    "Austin": "TX",
    "San Antonio": "TX",
    "El Paso": "TX",
    "Amarillo": "TX",
    "Raton": "TX",
    "New Orleans": "LA",
    "Baton Rouge": "LA",
    "Miami": "FL",
    "Orlando": "FL",
    "Jacksonville": "FL",
    "Tampa": "FL",
    "Atlanta": "GA",
    "Savannah": "GA",
    "Charlotte": "NC",
    "Raleigh": "NC",
    "Nashville": "TN",
    "Memphis": "TN",
    "Knoxville": "TN",
    "Birmingham": "AL",
    "Chicago": "IL",
    "Springfield": "IL",
    "Indianapolis": "IN",
    "Columbus": "OH",
    "Cleveland": "OH",
    "Cincinnati": "OH",
    "Detroit": "MI",
    "Lansing": "MI",
    "St. Louis": "MO",
    "Kansas City": "MO",
    "Minneapolis": "MN",
    "Milwaukee": "WI",
    "New York": "NY",
    "Buffalo": "NY",
    "Boston": "MA",
    "Hartford": "CT",
    "Providence": "RI",
    "Newark": "NJ",
    "Philadelphia": "PA",
    "Pittsburgh": "PA",
    "Baltimore": "MD",
    "Washington": "DC",
    "Richmond": "VA",
}


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

#: Default seed used when the caller does not override.
DEFAULT_SEED: int = 20260101


@dataclass(frozen=True)
class GeneratorConfig:
    """Configuration knobs for the synthetic dataset.

    The defaults produce ~5,000 shipments over 180 days, which is
    large enough to support a 70/15/15 chronological split with
    hundreds of late-delivery positives per split.

    Attributes:
        shipment_count: Total number of shipment rows to generate.
        start_date: First pickup day (inclusive).  Pickup dates are
            drawn uniformly over ``[start_date, start_date + horizon_days]``.
        horizon_days: Number of days covered by the dataset.
        late_rate: Target fraction of shipments whose actual
            delivery is after the promised delivery timestamp.
            Realistic operations sit in the 0.10–0.30 range.
        first_attempt_rate: Target fraction of delivered shipments
            whose DELIVERED event has ``attempt_number = 1``.
        exception_event_rate: Probability that a non-delivery event
            in the lifecycle is an exception type (DELAYED /
            EXCEPTION / HOLD).
        carrier_count: Number of distinct carriers in the carrier
            dimension.
        seed: Random seed for reproducibility.
        scac_pool: Optional iterable of four-letter SCAC codes; when
            ``None`` the generator draws from a fixed pool of
            plausible codes.
        csv_timestamp_format: Timestamp format used in the CSV
            output to match the existing ingest options
            (``yyyy-MM-dd'T'HH:mm:ssX``).
    """

    shipment_count: int = 5_000
    start_date: date = date(2025, 7, 1)
    horizon_days: int = 180
    late_rate: float = 0.18
    first_attempt_rate: float = 0.78
    exception_event_rate: float = 0.12
    carrier_count: int = 8
    seed: int = DEFAULT_SEED
    scac_pool: tuple[str, ...] = (
        "ATLS",
        "PNLR",
        "MSTR",
        "CPLX",
        "BLTX",
        "RDRX",
        "NXGN",
        "ECHO",
        "YMAX",
        "JBHT",
        "OLDX",
        "SAIA",
    )
    csv_timestamp_format: str = "yyyy-MM-dd'T'HH:mm:ssX"

    def replace(self, **overrides: object) -> "GeneratorConfig":
        """Return a new config with the given overrides applied."""
        return replace(self, **overrides)


# ---------------------------------------------------------------------------
# Internal data structures
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class CarrierProfile:
    """Synthesized carrier record."""

    carrier_id: str
    carrier_name: str
    scac: str
    service_mode: str
    home_region_code: str
    is_active: bool
    updated_at: datetime


@dataclass(frozen=True)
class ShipmentRecord:
    """Synthesized shipment row.  Matches the shipments schema."""

    shipment_id: str
    carrier_id: str
    origin_state: str
    destination_state: str
    pickup_ts: datetime
    promised_delivery_ts: datetime
    actual_delivery_ts: datetime | None
    shipping_cost_usd: float | None
    distance_miles: float | None
    updated_at: datetime


@dataclass(frozen=True)
class EventRecord:
    """Synthesized delivery-event row.  Matches the events schema."""

    event_id: str
    shipment_id: str
    event_type: str
    event_ts: datetime
    event_city: str
    event_state: str
    delay_reason: str | None
    attempt_number: int | None
    updated_at: datetime


# ---------------------------------------------------------------------------
# Output container
# ---------------------------------------------------------------------------


@dataclass
class GeneratedDataset:
    """Container for the three CSV outputs of the generator.

    The dataset is intentionally split into three frames that mirror
    the existing ingest schemas.  No additional target / label column
    is added to these CSV files — the late-delivery label is derived
    later by the model training code from
    ``actual_delivery_ts > promised_delivery_ts``.

    Attributes:
        carriers: Carrier dimension rows.
        shipments: Shipment fact rows.
        events: Delivery-event fact rows.
        config: The configuration that produced the dataset.
    """

    carriers: list[CarrierProfile] = field(default_factory=list)
    shipments: list[ShipmentRecord] = field(default_factory=list)
    events: list[EventRecord] = field(default_factory=list)
    config: GeneratorConfig | None = None

    def summary(self) -> dict[str, int]:
        """Return a small summary dict for logging / tests."""
        late = sum(
            1
            for s in self.shipments
            if s.actual_delivery_ts is not None and s.actual_delivery_ts > s.promised_delivery_ts
        )
        return {
            "carriers": len(self.carriers),
            "shipments": len(self.shipments),
            "events": len(self.events),
            "late_shipments": late,
        }


# ---------------------------------------------------------------------------
# Generator
# ---------------------------------------------------------------------------


def _iso(ts: datetime) -> str:
    """Render a timestamp in the CSV format used by the ingest module.

    The existing sample uses ``yyyy-MM-dd'T'HH:mm:ssX`` (an ISO 8601
    variant with a trailing ``Z`` for UTC).  The helper below
    reproduces that exact text so the generated CSVs round-trip
    cleanly through :func:`read_csv`.
    """
    return ts.strftime("%Y-%m-%dT%H:%M:%SZ")


def _weighted_choice(rng: random.Random, values: list, weights: list):
    """Deterministic weighted choice using the supplied ``rng``.

    The standard library's :func:`random.choices` uses the global
    RNG, so we implement a small replacement that draws from the
    supplied :class:`random.Random` instance.  Weights are
    normalised; zero / negative weights are treated as zero.
    """
    if not values:
        raise ValueError("values must be non-empty")
    if len(values) != len(weights):
        raise ValueError("values and weights must have the same length")
    total = 0.0
    cleaned: list[float] = []
    for w in weights:
        value = float(w) if w is not None else 0.0
        if value < 0:
            value = 0.0
        cleaned.append(value)
        total += value
    if total <= 0:
        # Fall back to uniform selection when all weights are zero.
        return values[rng.randrange(len(values))]
    pick = rng.random() * total
    cumulative = 0.0
    for value, weight in zip(values, cleaned):
        cumulative += weight
        if pick < cumulative:
            return value
    return values[-1]


def _date_range(start: date, days: int) -> list[date]:
    """Return ``[start, start + 1, ..., start + days - 1]``."""
    return [start + timedelta(days=offset) for offset in range(days)]


def _random_pickup_datetime(rng: random.Random, pickup_day: date) -> datetime:
    """Return a realistic pickup timestamp in the 06:00–22:00 window.

    Pickups concentrate in the morning and are bounded so a
    reasonable per-shipment lifecycle fits inside a 24h window
    (with the rare exception of a next-day DELIVERY).  The window
    also keeps the chronology test deterministic.
    """
    # Hour: 6–22 weighted toward business hours.
    hour_weights = [
        0,
        0,
        0,
        0,
        0,
        0,  # 00–05
        1,
        2,
        3,
        3,
        2,
        2,  # 06–11
        2,
        2,
        2,
        3,
        3,
        3,  # 12–17
        2,
        1,
        1,
        1,
        0,
        0,  # 18–23
    ]
    hour = _weighted_choice(rng, list(range(24)), hour_weights)
    minute = rng.randint(0, 59)
    second = rng.randint(0, 59)
    return datetime(
        pickup_day.year,
        pickup_day.month,
        pickup_day.day,
        hour,
        minute,
        second,
        tzinfo=timezone.utc,
    )


def _random_transit_hours(rng: random.Random, distance_miles: float, service_mode: str) -> float:
    """Return a realistic pickup-to-delivery transit duration.

    Approximates a linear relationship between distance and time at
    ~50–60 mph average speed, with mode-specific adjustments and
    multiplicative noise.
    """
    base_speed_mph = {"FTL": 55.0, "LTL": 50.0, "PARCEL": 40.0}[service_mode]
    base_hours = max(distance_miles, 50.0) / base_speed_mph
    # Add ±25% noise so transit times are not deterministic.
    return max(2.0, base_hours * rng.uniform(0.75, 1.25))


def _random_distance(rng: random.Random, origin: str, destination: str) -> float:
    """Return a plausible long-haul distance for a state pair.

    We avoid the (small) state-pair lookup to keep the generator
    self-contained: each pair draws from a distribution centered on
    1,200 miles with realistic spread, and a small fraction of
    shipments are short-haul (< 500 miles).
    """
    if origin == destination:
        return float(rng.randint(50, 250))
    long_haul = rng.lognormvariate(7.0, 0.35)  # median ≈ 1100 miles
    if rng.random() < 0.18:
        long_haul = float(rng.randint(200, 600))  # short-haul fraction
    return float(min(long_haul, 2_800.0))


def _random_cost(rng: random.Random, distance_miles: float, service_mode: str) -> float:
    """Return a synthetic shipping cost.

    Uses a per-mile rate that varies by service mode plus a small
    fixed pickup surcharge.
    """
    rate_per_mile = {
        "FTL": rng.uniform(2.2, 3.0),
        "LTL": rng.uniform(1.4, 2.0),
        "PARCEL": rng.uniform(0.9, 1.6),
    }[service_mode]
    surcharge = rng.uniform(35.0, 110.0)
    return round(surcharge + rate_per_mile * distance_miles, 2)


def _carrier_reliability(rng: random.Random) -> float:
    """Per-call multiplicative shock for delivery variance.

    Some shipments get a "bad day" with extra delay; some get a
    "lucky day".  Most are near 1.0.  This is *not* the carrier-level
    lateness profile — that lives on the carrier record.
    """
    return rng.lognormvariate(0.0, 0.20)  # median = 1.0


def _carrier_late_multiplier(carrier_id: str, profiles: dict[str, float]) -> float:
    """Map a carrier to a base lateness multiplier."""
    return profiles.get(carrier_id, 1.0)


def _make_carrier_id(rng: random.Random, index: int) -> str:
    """Stable carrier id like ``CAR001``."""
    return f"CAR{index:03d}"


def _generate_carriers(rng: random.Random, config: GeneratorConfig) -> list[CarrierProfile]:
    """Generate the carrier dimension rows.

    Each carrier gets:
    - a unique id and name
    - one of the three allowed service modes
    - a home region drawn from the four canonical regions
    - a SCAC drawn from ``config.scac_pool``
    - a per-carrier lateness multiplier (some carriers more reliable
      than others) that the shipment generator consumes
    """
    carrier_prefixes = [
        "Atlas",
        "Pioneer",
        "MidStates",
        "Coastal",
        "Sunbelt",
        "BoltEx",
        "RoadEx",
        "Northstar",
        "Echo",
        "YMax",
        "JBHunt",
        "OldDom",
        "Saia",
        "Reliant",
        "Continental",
    ]
    carrier_suffixes = [
        "Freight",
        "Logistics",
        "Trucking",
        "Parcel",
        "Express",
        "Carriers",
        "Lines",
        "Transport",
    ]
    regions = sorted(set(STATE_TO_REGION.values()))
    generated: list[CarrierProfile] = []
    for i in range(config.carrier_count):
        carrier_id = _make_carrier_id(rng, i + 1)
        name = f"{rng.choice(carrier_prefixes)} {rng.choice(carrier_suffixes)}"
        scac = rng.choice(config.scac_pool)
        service_mode = rng.choice(SERVICE_MODES)
        home_region = rng.choice(regions)
        # One carrier (deterministically the first by index when we
        # choose to) may be inactive for realism.  Keep the share
        # small (1 carrier out of ``carrier_count`` is enough).
        is_active = not (i == config.carrier_count - 1 and config.carrier_count >= 6)
        updated_at = datetime(
            config.start_date.year,
            config.start_date.month,
            config.start_date.day,
            0,
            0,
            0,
            tzinfo=timezone.utc,
        )
        generated.append(
            CarrierProfile(
                carrier_id=carrier_id,
                carrier_name=name,
                scac=scac,
                service_mode=service_mode,
                home_region_code=home_region,
                is_active=is_active,
                updated_at=updated_at,
            )
        )
    return generated


def _assign_carrier_lateness_profile(
    rng: random.Random, carriers: list[CarrierProfile]
) -> dict[str, float]:
    """Return a mapping ``carrier_id -> base late multiplier``.

    Most carriers cluster around 1.0; a small fraction are clearly
    more reliable or less reliable.  Used to inject realistic
    carrier-level variation into the target label without leaking
    the target itself into the feature set.
    """
    profile: dict[str, float] = {}
    for carrier in carriers:
        # Centre around 1.0; range 0.6 (very reliable) to 1.6 (poor).
        profile[carrier.carrier_id] = float(rng.uniform(0.6, 1.6))
    return profile


def _lateness_probability(
    rng: random.Random,
    config: GeneratorConfig,
    *,
    carrier_multiplier: float,
    transit_hours: float,
    promised_transit_hours: float,
    distance_miles: float,
) -> float:
    """Map operational features to a probability of being late.

    The model is intentionally *noisy* and depends only on features
    that are known at pickup time.  It does **not** read
    ``actual_delivery_ts`` or any column that would leak the
    target.

    The shape is:
    - promised transit is derived from the sampled transit hours
      with a per-shipment buffer, so the demand ratio is a
      *deliberately tight* margin (not a leak).  Realized overrun
      is generated separately via multiplicative noise; the
      probability of overrun is what the logistic models.
    - longer distance = slightly lower risk
      (long-haul carriers have more routine / buffer)
    - carrier reliability multiplier adds per-carrier bias

    All of these are tunable; the final label is sampled
    Bernoulli-style with realistic class imbalance calibrated to
    ``config.late_rate`` on average.
    """
    from math import exp

    # Ratio of demand vs promise.  Because we set promised transit
    # to ``transit_hours * uniform(0.85, 0.95) * carrier_multiplier``,
    # the ratio is centred on ~1.10 with a long right tail.  This is
    # the operational margin carriers actually use.
    if promised_transit_hours <= 0:
        promised_transit_hours = 1.0
    demand_ratio = transit_hours / promised_transit_hours

    # Convert carrier multiplier to a centered reliability signal:
    #   - 0.6 (very reliable)  ->  -0.4
    #   - 1.0 (median)          ->   0.0
    #   - 1.6 (poor)            ->  +0.6
    carrier_reliability = (carrier_multiplier - 1.0) * 1.0

    # Logit anchored so the average across the synthetic population
    # matches ``config.late_rate``.  Calibrated to give a result
    # near ``late_rate`` on the median carrier / median demand.
    logit = (
        -2.0  # base anchor: keeps overall class near late_rate
        - 3.0 * (demand_ratio - 1.0)  # overrun penalty (the main signal)
        + 0.3 * (1.0 - min(1.0, distance_miles / 2_000.0))  # shorter-haul slightly riskier
        + 1.5 * carrier_reliability  # carrier-level effect
    )

    # Logistic with a moderate slope to convert the logit to a
    # probability.  The slope is small so the late_rate is
    # approximately respected when averaged over the population.
    raw = 1.0 / (1.0 + exp(-1.4 * logit))

    # Blend toward the configured base so a poor random seed does
    # not destroy the class balance.
    base = max(0.005, min(0.995, config.late_rate))
    return float(max(0.0, min(1.0, 0.3 * raw + 0.7 * base)))


def _generate_shipments(
    rng: random.Random,
    config: GeneratorConfig,
    carriers: list[CarrierProfile],
    carrier_late_profile: dict[str, float],
) -> list[ShipmentRecord]:
    """Generate the shipment fact rows."""
    if not carriers:
        raise ValueError("At least one carrier is required to generate shipments")

    active_carriers = [c for c in carriers if c.is_active] or carriers
    shipment_pool = _date_range(config.start_date, config.horizon_days)

    shipments: list[ShipmentRecord] = []
    for index in range(config.shipment_count):
        shipment_id = f"SHP{index + 1:06d}"
        carrier = rng.choice(active_carriers)
        carrier_id = carrier.carrier_id
        carrier_multiplier = _carrier_late_multiplier(carrier_id, carrier_late_profile)

        # Pickup date uniform across the horizon.
        pickup_day = rng.choice(shipment_pool)
        pickup_ts = _random_pickup_datetime(rng, pickup_day)

        # Origin / destination sampled from the state set.  Avoid
        # same-state as a small fraction.
        states = list(STATE_TO_REGION.keys())
        origin = rng.choice(states)
        destination = rng.choice(states)
        if destination == origin and rng.random() < 0.95:
            destination = rng.choice([s for s in states if s != origin])

        distance = _random_distance(rng, origin, destination)
        transit_hours = _random_transit_hours(rng, distance, carrier.service_mode)

        # Promised transit: a fraction of the sampled transit plus a
        # carrier-specific buffer.  This is the *operational* SLA the
        # carrier commits to at booking time — known at pickup.
        promised_transit = max(
            4.0,
            transit_hours * rng.uniform(0.85, 0.95) * carrier_multiplier,
        )
        promised_delivery_ts = pickup_ts + timedelta(hours=promised_transit)

        # Late probability and realization.
        late_prob = _lateness_probability(
            rng,
            config,
            carrier_multiplier=carrier_multiplier,
            transit_hours=transit_hours,
            promised_transit_hours=promised_transit,
            distance_miles=distance,
        )
        is_late = rng.random() < late_prob

        if is_late:
            overrun = rng.uniform(0.05, 0.65) * promised_transit
            actual_transit = promised_transit + overrun
        else:
            underrun = rng.uniform(0.0, 0.20) * promised_transit
            actual_transit = max(1.0, promised_transit - underrun)

        actual_delivery_ts = pickup_ts + timedelta(hours=actual_transit)

        # Updated-at is set to the actual delivery timestamp when
        # present, else to the promised delivery + 1h (a typical
        # operational follow-up cadence for in-flight shipments).
        updated_at = actual_delivery_ts

        cost = _random_cost(rng, distance, carrier.service_mode)

        shipments.append(
            ShipmentRecord(
                shipment_id=shipment_id,
                carrier_id=carrier_id,
                origin_state=origin,
                destination_state=destination,
                pickup_ts=pickup_ts,
                promised_delivery_ts=promised_delivery_ts,
                actual_delivery_ts=actual_delivery_ts,
                shipping_cost_usd=cost,
                distance_miles=distance,
                updated_at=updated_at,
            )
        )
    return shipments


def _generate_events(
    rng: random.Random,
    config: GeneratorConfig,
    shipments: list[ShipmentRecord],
    carrier_late_profile: dict[str, float],
) -> list[EventRecord]:
    """Generate the delivery-event rows.

    Each shipment's lifecycle is a function of:
    - whether it was late (controls the terminal DELIVERED event time)
    - whether it had an exception (DELAYED / EXCEPTION / HOLD)
    - whether the first delivery attempt succeeded
    - per-shipment noise on the number of intermediate events
    """
    event_seq = 0
    rows: list[EventRecord] = []

    def _next_event_id() -> str:
        nonlocal event_seq
        event_seq += 1
        return f"EVT{event_seq:07d}"

    for shipment in shipments:
        carrier_mult = _carrier_late_profile_lookup(carrier_late_profile, shipment.carrier_id)
        is_late = (
            shipment.actual_delivery_ts is not None
            and shipment.actual_delivery_ts > shipment.promised_delivery_ts
        )
        is_first_attempt_success = rng.random() < config.first_attempt_rate

        # Pickup is always present, at the origin state.
        rows.append(
            _make_event(
                rng,
                shipment,
                _next_event_id(),
                "PICKED_UP",
                shipment.pickup_ts + timedelta(minutes=rng.randint(5, 60)),
                shipment.origin_state,
                None,
                None,
            )
        )

        # 0–3 IN_TRANSIT events between pickup and delivery.  The
        # state for each event is a random state (drawn from the
        # same state set so the city lookup always succeeds); the
        # city is then resolved consistently.
        in_transit_count = _weighted_choice(rng, [0, 1, 2, 3], [0.05, 0.45, 0.40, 0.10])
        in_transit_span_seconds = max(
            1.0,
            (
                (shipment.actual_delivery_ts or shipment.promised_delivery_ts) - shipment.pickup_ts
            ).total_seconds(),
        )
        for i in range(in_transit_count):
            fraction = (i + 1) / (in_transit_count + 1)
            ts = shipment.pickup_ts + timedelta(seconds=in_transit_span_seconds * fraction)
            transit_state = rng.choice(list(STATE_TO_REGION.keys()))
            rows.append(
                _make_event(
                    rng,
                    shipment,
                    _next_event_id(),
                    "IN_TRANSIT",
                    ts,
                    transit_state,
                    None,
                    None,
                )
            )

        # Optional DELAYED / EXCEPTION / HOLD event when the
        # shipment had a hiccup.  Probability is amplified for late
        # shipments so the exception summary is informative but
        # exceptions are not deterministic.
        exception_prob = config.exception_event_rate * (1.0 + 0.7 * carrier_mult)
        if is_late and rng.random() < min(0.95, exception_prob + 0.20):
            delay_ts = shipment.pickup_ts + timedelta(
                seconds=in_transit_span_seconds * rng.uniform(0.4, 0.8)
            )
            event_type = _weighted_choice(
                rng,
                ["DELAYED", "EXCEPTION", "HOLD"],
                [0.55, 0.30, 0.15],
            )
            reason = rng.choice(DELAY_REASONS)
            rows.append(
                _make_event(
                    rng,
                    shipment,
                    _next_event_id(),
                    event_type,
                    delay_ts,
                    shipment.destination_state,
                    reason,
                    None,
                )
            )

        # OUT_FOR_DELIVERY typically ~30–90 minutes before delivery,
        # at the destination state.
        delivery_ts = shipment.actual_delivery_ts or shipment.promised_delivery_ts
        ofd_ts = delivery_ts - timedelta(minutes=rng.randint(30, 180))
        rows.append(
            _make_event(
                rng,
                shipment,
                _next_event_id(),
                "OUT_FOR_DELIVERY",
                ofd_ts,
                shipment.destination_state,
                None,
                None,
            )
        )

        # First delivery attempt.
        attempt = 1
        if not is_first_attempt_success:
            attempt_ts = ofd_ts + timedelta(minutes=rng.randint(10, 60))
            reason = rng.choice(["NO_RECIPIENT", "BUSINESS_CLOSED", "ADDRESS_ISSUE"])
            rows.append(
                _make_event(
                    rng,
                    shipment,
                    _next_event_id(),
                    "DELIVERY_ATTEMPT",
                    attempt_ts,
                    shipment.destination_state,
                    reason,
                    attempt,
                )
            )
            attempt += 1

        # Final DELIVERED.
        rows.append(
            _make_event(
                rng,
                shipment,
                _next_event_id(),
                "DELIVERED",
                delivery_ts + timedelta(minutes=rng.randint(0, 30)),
                shipment.destination_state,
                None,
                attempt,
            )
        )

    return rows


def _carrier_late_profile_lookup(profiles: dict[str, float], carrier_id: str) -> float:
    """Return the carrier's late multiplier, defaulting to 1.0."""
    return profiles.get(carrier_id, 1.0)


def _event_city_for(rng: random.Random, state_code: str) -> str:
    """Return a plausible city for the given state code."""
    candidates = [city for city, st in CITY_TO_STATE.items() if st == state_code]
    if not candidates:
        candidates = [city for city, st in CITY_TO_STATE.items()]
    return rng.choice(candidates)


def _make_event(
    rng: random.Random,
    shipment: ShipmentRecord,
    event_id: str,
    event_type: str,
    event_ts: datetime,
    event_state: str,
    delay_reason: str | None,
    attempt_number: int | None,
) -> EventRecord:
    """Build an EventRecord, keeping the schema contract in mind.

    The ``event_state`` argument is the *resolved* state for this
    event — we use it to pick a city and copy it directly so the
    CSV row always has a coherent ``(city, state)`` pair.  Callers
    that pass ``"UNKNOWN"`` are responsible for resolving the state
    before calling this helper.
    """
    city = _event_city_for(rng, event_state)
    return EventRecord(
        event_id=event_id,
        shipment_id=shipment.shipment_id,
        event_type=event_type,
        event_ts=event_ts,
        event_city=city,
        event_state=event_state,
        delay_reason=delay_reason,
        attempt_number=attempt_number,
        updated_at=event_ts,
    )


# ---------------------------------------------------------------------------
# Public entry points
# ---------------------------------------------------------------------------


def generate_dataset(config: GeneratorConfig | None = None) -> GeneratedDataset:
    """Generate a deterministic synthetic dataset.

    Args:
        config: Optional :class:`GeneratorConfig`.  When ``None`` the
            module defaults are used (5,000 shipments, 180-day
            horizon, seed ``DEFAULT_SEED``).

    Returns:
        A :class:`GeneratedDataset` with carriers, shipments, and
        delivery events.  The structure is identical to the
        committed sample so the same ingest pipeline can process
        the output.
    """
    if config is None:
        config = GeneratorConfig()

    rng = random.Random(config.seed)

    carriers = _generate_carriers(rng, config)
    carrier_late_profile = _assign_carrier_lateness_profile(rng, carriers)
    shipments = _generate_shipments(rng, config, carriers, carrier_late_profile)
    events = _generate_events(rng, config, shipments, carrier_late_profile)

    dataset = GeneratedDataset(
        carriers=carriers,
        shipments=shipments,
        events=events,
        config=config,
    )
    LOGGER.info(
        "Generated synthetic dataset: %s",
        json.dumps(dataset.summary(), default=str),
    )
    return dataset


def write_csv_files(
    dataset: GeneratedDataset,
    output_dir: str | Path,
) -> dict[str, Path]:
    """Persist a generated dataset to ``output_dir`` as three CSV files.

    The output filenames follow the existing convention
    (``carriers_<date>.csv``, ``shipments_<date>.csv``,
    ``delivery_events_<date>.csv``) so the existing ingest path can
    consume the generated files without code changes.

    Args:
        dataset: Dataset produced by :func:`generate_dataset`.
        output_dir: Directory to write the CSVs into.  Will be
            created if it does not exist.

    Returns:
        A dict mapping entity name to the written file path.
    """
    if dataset.config is None:
        raise ValueError("dataset.config is required to write_csv_files")

    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    date_label = dataset.config.start_date.strftime("%Y-%m-%d")
    written: dict[str, Path] = {}

    # --- carriers ---------------------------------------------------------
    carrier_path = output_path / f"carriers_{date_label}.csv"
    with carrier_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            [
                "carrier_id",
                "carrier_name",
                "scac",
                "service_mode",
                "home_region_code",
                "is_active",
                "updated_at",
            ]
        )
        for carrier in dataset.carriers:
            writer.writerow(
                [
                    carrier.carrier_id,
                    carrier.carrier_name,
                    carrier.scac,
                    carrier.service_mode,
                    carrier.home_region_code,
                    "true" if carrier.is_active else "false",
                    _iso(carrier.updated_at),
                ]
            )
    written["carriers"] = carrier_path

    # --- shipments --------------------------------------------------------
    shipment_path = output_path / f"shipments_{date_label}.csv"
    with shipment_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            [
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
            ]
        )
        for shipment in dataset.shipments:
            writer.writerow(
                [
                    shipment.shipment_id,
                    shipment.carrier_id,
                    shipment.origin_state,
                    shipment.destination_state,
                    _iso(shipment.pickup_ts),
                    _iso(shipment.promised_delivery_ts),
                    _iso(shipment.actual_delivery_ts) if shipment.actual_delivery_ts else "",
                    (
                        f"{shipment.shipping_cost_usd:.2f}"
                        if shipment.shipping_cost_usd is not None
                        else ""
                    ),
                    f"{shipment.distance_miles:.1f}" if shipment.distance_miles is not None else "",
                    _iso(shipment.updated_at),
                ]
            )
    written["shipments"] = shipment_path

    # --- delivery_events -------------------------------------------------
    events_path = output_path / f"delivery_events_{date_label}.csv"
    with events_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            [
                "event_id",
                "shipment_id",
                "event_type",
                "event_ts",
                "event_city",
                "event_state",
                "delay_reason",
                "attempt_number",
                "updated_at",
            ]
        )
        for event in dataset.events:
            writer.writerow(
                [
                    event.event_id,
                    event.shipment_id,
                    event.event_type,
                    _iso(event.event_ts),
                    event.event_city,
                    event.event_state,
                    event.delay_reason or "",
                    event.attempt_number if event.attempt_number is not None else "",
                    _iso(event.updated_at),
                ]
            )
    written["delivery_events"] = events_path

    LOGGER.info("Wrote synthetic dataset to %s", output_path)
    return written


__all__ = [
    "DEFAULT_SEED",
    "GeneratorConfig",
    "GeneratedDataset",
    "STATE_TO_REGION",
    "CITY_TO_STATE",
    "SERVICE_MODES",
    "EVENT_TYPES",
    "EXCEPTION_EVENT_TYPES",
    "DELAY_REASONS",
    "generate_dataset",
    "write_csv_files",
]
