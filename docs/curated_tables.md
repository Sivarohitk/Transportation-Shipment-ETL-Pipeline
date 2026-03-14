# Curated Tables

This document defines the curated analytical model produced by the ETL pipeline.

## Partition Strategy
All curated outputs are partitioned by:
- `p_date`
- `region_code`
- `carrier_id`

This enables fast date and segment filtering for KPI and operational analysis.

## Table Catalog

### `curated.dim_carrier`
- Grain: one row per `carrier_id` per snapshot `p_date`
- Purpose: carrier attributes and current operational status
- Primary business key: (`carrier_id`, `p_date`)
- Source: `stg_carriers`

| Column | Type | Meaning |
| --- | --- | --- |
| `carrier_id` | string | Unique carrier identifier |
| `carrier_name` | string | Carrier display name |
| `scac` | string | Standard Carrier Alpha Code |
| `service_mode` | string | Service type (`FTL`, `LTL`, `PARCEL`) |
| `region_code` | string | Carrier home/operating region |
| `is_active` | boolean | Active flag from source |
| `updated_at` | timestamp | Last source update time |
| `p_date` | date | Snapshot partition date |

### `curated.fct_shipment`
- Grain: one row per `shipment_id` (latest state)
- Purpose: shipment-level fact table for delivery and cost analytics
- Primary business key: `shipment_id`
- Source: `stg_shipments` + region enrichment

| Column | Type | Meaning |
| --- | --- | --- |
| `shipment_id` | string | Shipment identifier |
| `carrier_id` | string | Owning carrier |
| `origin_state` | string | Shipment origin state |
| `destination_state` | string | Shipment destination state |
| `origin_region_code` | string | Enriched origin region |
| `region_code` | string | Enriched destination region |
| `pickup_ts` | timestamp | Pickup timestamp |
| `promised_delivery_ts` | timestamp | SLA delivery timestamp |
| `actual_delivery_ts` | timestamp | Actual delivery timestamp |
| `shipping_cost_usd` | double | Shipment cost in USD |
| `distance_miles` | double | Shipment route distance |
| `delivered_flag` | int | Delivered indicator |
| `on_time_delivery_flag` | int | Delivery on/before promise |
| `delay_minutes` | double | Positive minutes late |
| `exception_flag` | int | Shipment exception indicator |
| `transit_time_hours` | double | Pickup-to-delivery duration |
| `p_date` | date | Shipment partition date |

### `curated.fct_delivery_event`
- Grain: one row per `event_id` (latest event update)
- Purpose: event-level timeline for delivery behavior and exception analysis
- Primary business key: `event_id`
- Source: `stg_delivery_events` + `stg_shipments` + region enrichment

| Column | Type | Meaning |
| --- | --- | --- |
| `event_id` | string | Delivery event identifier |
| `shipment_id` | string | Related shipment ID |
| `carrier_id` | string | Carrier from shipment context |
| `event_type` | string | Event classification |
| `event_ts` | timestamp | Event occurrence timestamp |
| `event_city` | string | Event city |
| `event_state` | string | Event state |
| `delay_reason` | string | Delay/exception reason |
| `attempt_number` | int | Delivery attempt ordinal |
| `updated_at` | timestamp | Last event update timestamp |
| `region_code` | string | Event/shipment derived region |
| `p_date` | date | Event partition date |
| `on_time_delivery_flag` | int | Delivered event on time |
| `delay_minutes` | double | Event lateness vs promise |
| `exception_flag` | int | Exception/HOLD event flag |
| `transit_time_hours` | double | Pickup-to-event duration |

### `curated.agg_shipment_daily`
- Grain: one row per (`p_date`, `region_code`, `carrier_id`)
- Purpose: daily aggregate facts to support KPI computation
- Source: `fct_shipment` (+ optional delivery event counts)

| Column | Type | Meaning |
| --- | --- | --- |
| `total_shipments` | long | Distinct shipment volume |
| `delivered_shipments` | long | Delivered shipment count |
| `on_time_shipments` | long | On-time delivered count |
| `delayed_shipments` | long | Delayed shipment count |
| `exception_shipments` | long | Exception shipment count |
| `total_delay_minutes` | double | Sum of delay minutes |
| `avg_delay_minutes` | double | Average delay minutes (delayed only) |
| `avg_transit_time_hours` | double | Average transit duration |
| `total_shipping_cost_usd` | double | Total shipping cost |
| `total_distance_miles` | double | Total distance |
| `total_delivery_events` | long | Related event count |
| `on_time_delivery_rate` | double | On-time ratio |
| `exception_rate` | double | Exception ratio |
| `avg_cost_per_mile` | double | Cost per mile |

### `curated.kpi_delivery_daily`
- Grain: one row per (`p_date`, `region_code`, `carrier_id`)
- Purpose: final KPI output for dashboards and reporting
- Source: `agg_shipment_daily` + `fct_delivery_event`

| Column | Type | Meaning |
| --- | --- | --- |
| `on_time_delivery_rate` | double | On-time success ratio |
| `avg_transit_hours` | double | Average transit duration |
| `late_delivery_rate` | double | Late delivery ratio |
| `first_attempt_success_rate` | double | Delivered-on-first-attempt ratio |
| `exception_rate` | double | Exception ratio |
| `avg_cost_per_mile` | double | Cost efficiency metric |
| `volume_by_carrier` | long | Shipment volume |
| `delivery_event_density` | double | Events per shipment |

## Example Queries
```sql
-- Daily KPI by carrier in SOUTH region
SELECT p_date, carrier_id, on_time_delivery_rate, late_delivery_rate
FROM curated.kpi_delivery_daily
WHERE region_code = 'SOUTH'
ORDER BY p_date, carrier_id;
```

```sql
-- Shipment exception trends
SELECT p_date, region_code, SUM(exception_flag) AS exception_events
FROM curated.fct_delivery_event
GROUP BY p_date, region_code
ORDER BY p_date, region_code;
```
