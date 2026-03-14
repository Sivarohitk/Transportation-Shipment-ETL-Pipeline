# KPI Definitions

All KPI outputs are produced at grain:
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
