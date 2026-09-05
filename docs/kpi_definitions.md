# KPI Definitions

All Phase-6 analytics build on the existing Gold fact / dimension
tables and use the documented metrics below.

## Phase-6 Gold Analytics Tables

The following three additional Gold tables are produced alongside
the existing `dim_carrier`, `fct_shipment`, `fct_delivery_event`,
`agg_shipment_daily`, and `kpi_delivery_daily`.  Each is a thin
analytical roll-up of the existing Gold facts; no fields are
invented that are not already present in the source data.

### `carrier_performance`

- **Grain:** one row per `(p_date, carrier_id, service_mode)`
- **Source tables:** `fct_shipment`, `fct_delivery_event`, `dim_carrier`
- **Supported metrics:**

  | Metric | Formula | Business Meaning |
  | --- | --- | --- |
  | `shipment_volume` | `COUNT(DISTINCT shipment_id)` | Daily shipment throughput per carrier/service mode |
  | `delivered_shipments` | `SUM(delivered_flag)` | Shipments with a non-null actual delivery |
  | `on_time_delivery_rate` | `on_time_shipments / delivered_shipments` | Share of delivered shipments that met the promise time |
  | `late_delivery_rate` | `late_shipments / delivered_shipments` | Share of delivered shipments arriving late |
  | `exception_rate` | `exception_shipments / shipment_volume` | Share of shipments with operational exceptions |
  | `first_attempt_success_rate` | `first_attempt_success_shipments / delivered_shipments` | Share of delivered shipments completed on attempt 1 |
  | `avg_transit_hours` | `AVG(transit_time_hours)` | Typical pickup-to-delivery duration |
  | `avg_cost_per_mile` | `total_shipping_cost_usd / total_distance_miles` | Cost efficiency metric |
  | `total_shipping_cost_usd` | `SUM(shipping_cost_usd)` | Sum of shipment costs |
  | `total_distance_miles` | `SUM(distance_miles)` | Sum of route distances |
  | `total_delay_minutes` | `SUM(delay_minutes)` | Sum of delay minutes |

- **Metrics NOT produced (and why):** revenue / margin (no source
  field), customer satisfaction score (not in source data), carbon /
  fuel metrics (not in source data).

### `route_performance`

- **Grain:** one row per `(p_date, origin_region_code, destination_region_code, carrier_id)`
- **Source tables:** `fct_shipment`
- **Supported metrics:**

  | Metric | Formula | Business Meaning |
  | --- | --- | --- |
  | `shipment_count` | `COUNT(DISTINCT shipment_id)` | Volume per route/carrier/day |
  | `on_time_rate` | `on_time_shipments / delivered_shipments` | Share of on-time deliveries on the route |
  | `late_rate` | `late_shipments / delivered_shipments` | Share of late deliveries on the route |
  | `exception_rate` | `exception_shipments / shipment_count` | Share of shipments with operational exceptions |
  | `avg_transit_hours` | `AVG(transit_time_hours)` | Typical pickup-to-delivery duration on the route |
  | `avg_cost_per_mile` | `total_shipping_cost_usd / total_distance_miles` | Cost efficiency on the route |
  | `total_shipping_cost_usd` | `SUM(shipping_cost_usd)` | Sum of shipment costs on the route |
  | `total_distance_miles` | `SUM(distance_miles)` | Sum of route distances |

- **Metrics NOT produced (and why):** lane volume vs prior period
  (requires a wider time window), carrier exclusivity (would require a
  second pass over the same grain), SLA attainment (no SLA targets
  in source data).

### `delivery_exception_summary`

- **Grain:** one row per `(p_date, event_type, carrier_id, region_code)`
- **Source tables:** `fct_delivery_event`, `fct_shipment` (used for
  the per-grain shipment denominator of the `rate` metric)
- **Supported metrics:**

  | Metric | Formula | Business Meaning |
  | --- | --- | --- |
  | `event_count` | `COUNT(1)` | Number of events of this type |
  | `shipment_count` | `COUNT(DISTINCT shipment_id)` | Distinct shipments touched by events of this type |
  | `exception_event_count` | `SUM(exception_flag)` | Events of this type flagged as exception |
  | `exception_shipment_count` | `COUNT(DISTINCT CASE WHEN exception_flag = 1 THEN shipment_id END)` | Distinct shipments with at least one exception event of this type |
  | `rate` | `event_count / total_shipments` (per-grain denominator) | Frequency of this event type per shipment |
  | `avg_delay_minutes` | `AVG(delay_minutes)` | Average event-level delay vs the shipment promise |
  | `is_exception_event_type` | `event_type IN ('DELAYED', 'EXCEPTION', 'HOLD')` | Whether this event type is treated as an operational exception |
  | `total_shipments` | `COUNT(DISTINCT shipment_id)` from `fct_shipment` | Per-grain shipment denominator |

- **Metrics NOT produced (and why):** exception cost impact (would
  require shipment-level cost data joined on the event), time-to-
  recovery (requires a window join between exception events and the
  next DELIVERED event for the same shipment — documented as a
  Phase-7 / future enhancement).

## Existing Gold KPI Model

All existing KPI outputs are produced at grain:
- `p_date`
- `region_code`
- `carrier_id`

Source table: `curated.kpi_delivery_daily`

## KPI Formula Reference

| KPI | Formula | Business Meaning |
| --- | --- | --- |
| `on_time_delivery_rate` | `on_time_shipments / delivered_denominator` | Share of delivered shipments that met promise time |
| `avg_transit_hours` | `AVG(transit_time_hours)` from shipment/event facts | Typical pickup-to-delivery duration |
| `late_delivery_rate` | `delayed_shipments / delivered_denominator` | Share of delivered shipments arriving late |
| `first_attempt_success_rate` | `first_attempt_success_shipments / delivered_denominator` | Share of delivered shipments completed on attempt 1 |
| `exception_rate` | `exception_shipments / total_shipments` | Share of shipments with operational exceptions |
| `avg_cost_per_mile` | `total_shipping_cost_usd / total_distance_miles` | Cost efficiency metric |
| `volume_by_carrier` | `total_shipments` | Daily shipment throughput per carrier |
| `delivery_event_density` | `total_delivery_events / total_shipments` | Operational touch intensity per shipment |

### Denominator Convention
`delivered_denominator = GREATEST(delivered_shipments, delivered_event_shipments)`

This avoids undercounting when delivery events and shipment facts arrive on slightly different schedules.

## Metric Behavior and Edge Cases
- All rates default to `0.0` when denominator is `0`.
- `avg_cost_per_mile` defaults to `0.0` when distance is zero/null.
- `avg_transit_hours` defaults to `0.0` when no transit durations are available.
- `delay_minutes` is clamped to non-negative values before KPI rollups.

## Suggested Alert Thresholds (Portfolio Example)
- `on_time_delivery_rate < 0.90` for 3 consecutive days
- `late_delivery_rate > 0.10` day-over-day spike > 30%
- `first_attempt_success_rate < 0.85` for parcel carriers
- `exception_rate > 0.08` sustained for 7 days
- `avg_cost_per_mile` weekly increase > 15%

## SQL Consumption Example
```sql
SELECT
  p_date,
  region_code,
  carrier_id,
  on_time_delivery_rate,
  late_delivery_rate,
  first_attempt_success_rate,
  exception_rate
FROM curated.kpi_delivery_daily
WHERE p_date BETWEEN DATE '2026-01-01' AND DATE '2026-01-31'
ORDER BY p_date, region_code, carrier_id;
```
