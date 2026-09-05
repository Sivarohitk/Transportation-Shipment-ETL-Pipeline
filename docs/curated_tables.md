# Curated Tables

This document defines the curated analytical model produced by the ETL pipeline.

## Silver Tables (Phase 5)

The Silver layer produces validated, deduplicated, region-enriched
datasets that the Gold models consume.  Silver lives in
`src/transport_etl/silver/`.

### `curated.stg_shipments` / `supply_chain.silver.stg_shipments`
- Grain: one row per `shipment_id` (deduplicated by `updated_at`)
- Source: `curated.raw_shipments` (Bronze)
- Business key: `shipment_id`
- Reuses `standardize_columns` + `enrich_shipments_with_region`

### `curated.stg_carriers` / `supply_chain.silver.stg_carriers`
- Grain: one row per `carrier_id` (deduplicated by `updated_at`)
- Source: `curated.raw_carriers` (Bronze)
- Business key: `carrier_id`

### `curated.stg_delivery_events` / `supply_chain.silver.stg_delivery_events`
- Grain: one row per `event_id` (deduplicated by `updated_at`)
- Source: `curated.raw_delivery_events` (Bronze)
- Business key: `event_id`

### Silver MERGE Behavior (Databricks)

| Aspect            | Value |
|-------------------|-------|
| MATCH KEY         | `shipment_id` / `carrier_id` / `event_id` (business key) |
| WHEN MATCHED      | `UPDATE SET <all non-key columns> = source.<...>` |
| WHEN NOT MATCHED  | `INSERT (<all columns>) VALUES (source.<...>)` |
| Late-arriving data | Same MERGE; existing keys are updated, new keys are inserted |
| Idempotency       | Same source data produces identical target rows |

The renderer is a pure-Python function
(`transport_etl.silver.merge_spec.build_merge_sql`); the
orchestrator (`transport_etl.silver.merge.execute_silver_merge`)
registers the source as a Spark temp view and runs the rendered
SQL through `spark.sql`.  No `delta-spark` Python import is
required for unit tests; the live Delta write happens on the
Databricks runtime.

### Silver Quarantine

Quarantined records are written to
`<quarantine>/<table>/<rule>/part-*.parquet` with these audit
columns preserved:

- `__quarantine_rule` — Silver rule label
- `__source_entity` — Silver table name
- `__source_identity` — `<business_key>:<value>`
- `__batch_id` / `__run_date` — operational lineage
- `__quarantined_at` — current timestamp

## Bronze Tables (Phase 4)

The Bronze layer preserves raw source records with the smallest possible
transformation footprint.  It lives in `src/transport_etl/bronze/`.

### `curated.raw_shipments` / `supply_chain.bronze.raw_shipments`
- Grain: one row per source shipment record (with operational metadata)
- Source: `data/sample/raw/shipments_YYYY-MM-DD.csv`
- Reuses the explicit schema in `config/schemas/shipments.schema.json`

### `curated.raw_carriers` / `supply_chain.bronze.raw_carriers`
- Grain: one row per source carrier record
- Source: `data/sample/raw/carriers_YYYY-MM-DD.csv`
- Reuses the explicit schema in `config/schemas/carriers.schema.json`

### `curated.raw_delivery_events` / `supply_chain.bronze.raw_delivery_events`
- Grain: one row per source delivery event record
- Source: `data/sample/raw/delivery_events_YYYY-MM-DD.csv`
- Reuses the explicit schema in `config/schemas/delivery_events.schema.json`

### Operational Ingestion Metadata

Every Bronze record carries the same metadata columns:

| Column         | Description                                              |
|----------------|----------------------------------------------------------|
| `_ingested_at` | Spark `current_timestamp()` at ingestion (JVM local time) |
| `_source_file` | Absolute source CSV path supplied by the job runner       |
| `_batch_id`    | Deterministic id `<job>_<YYYYMMDD>` derived from run_date |
| `_run_date`    | Batch run date (`YYYY-MM-DD`) supplied by the job runner |

Bronze preserves **all** source columns exactly as the underlying ingest
modules return them; no Silver business cleaning happens in Bronze.

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

### `curated.carrier_performance` / `supply_chain.gold.carrier_performance`
- Grain: one row per (`p_date`, `carrier_id`, `service_mode`)
- Purpose: daily carrier-level performance roll-up
- Source: `fct_shipment` + `fct_delivery_event` + `dim_carrier`
- Defined in `sql/gold/carrier_performance.sql` and
  `transport_etl.transform.build_carrier_performance`

| Column | Type | Meaning |
| --- | --- | --- |
| `p_date` | date | Snapshot date |
| `carrier_id` | string | Carrier identifier |
| `service_mode` | string | Service mode (FTL / LTL / PARCEL / UNKNOWN) |
| `shipment_volume` | long | Distinct shipment count |
| `delivered_shipments` | long | Shipments with a non-null actual delivery |
| `on_time_shipments` | long | On-time delivery count |
| `late_shipments` | long | Late delivery count |
| `exception_shipments` | long | Shipments with operational exceptions |
| `first_attempt_success_shipments` | long | Delivered on attempt 1 |
| `on_time_delivery_rate` | double | `on_time_shipments / delivered_shipments` |
| `late_delivery_rate` | double | `late_shipments / delivered_shipments` |
| `exception_rate` | double | `exception_shipments / shipment_volume` |
| `first_attempt_success_rate` | double | `first_attempt_success_shipments / delivered_shipments` |
| `avg_transit_hours` | double | `AVG(transit_time_hours)` |
| `total_delay_minutes` | double | `SUM(delay_minutes)` |
| `avg_cost_per_mile` | double | `total_shipping_cost_usd / total_distance_miles` |
| `total_shipping_cost_usd` | double | `SUM(shipping_cost_usd)` |
| `total_distance_miles` | double | `SUM(distance_miles)` |

### `curated.route_performance` / `supply_chain.gold.route_performance`
- Grain: one row per (`p_date`, `origin_region_code`,
  `destination_region_code`, `carrier_id`)
- Purpose: daily route-level performance roll-up
- Source: `fct_shipment`
- Defined in `sql/gold/route_performance.sql` and
  `transport_etl.transform.build_route_performance`

| Column | Type | Meaning |
| --- | --- | --- |
| `p_date` | date | Snapshot date |
| `origin_region_code` | string | Origin region |
| `destination_region_code` | string | Destination region |
| `carrier_id` | string | Carrier identifier |
| `shipment_count` | long | Distinct shipment count |
| `delivered_shipments` | long | Shipments with a non-null actual delivery |
| `on_time_shipments` | long | On-time delivery count |
| `late_shipments` | long | Late delivery count |
| `exception_shipments` | long | Shipments with operational exceptions |
| `on_time_rate` | double | `on_time_shipments / delivered_shipments` |
| `late_rate` | double | `late_shipments / delivered_shipments` |
| `exception_rate` | double | `exception_shipments / shipment_count` |
| `avg_transit_hours` | double | `AVG(transit_time_hours)` |
| `avg_cost_per_mile` | double | `total_shipping_cost_usd / total_distance_miles` |
| `total_shipping_cost_usd` | double | `SUM(shipping_cost_usd)` |
| `total_distance_miles` | double | `SUM(distance_miles)` |

### `curated.delivery_exception_summary` / `supply_chain.gold.delivery_exception_summary`
- Grain: one row per (`p_date`, `event_type`, `carrier_id`,
  `region_code`)
- Purpose: daily event-type exception summary
- Source: `fct_delivery_event` + `fct_shipment` (for the denominator)
- Defined in `sql/gold/delivery_exception_summary.sql` and
  `transport_etl.transform.build_delivery_exception_summary`

| Column | Type | Meaning |
| --- | --- | --- |
| `p_date` | date | Snapshot date |
| `event_type` | string | Event classification (e.g. DELIVERED, DELAYED, EXCEPTION) |
| `carrier_id` | string | Carrier identifier |
| `region_code` | string | Region |
| `event_count` | long | Number of events of this type |
| `shipment_count` | long | Distinct shipments touched by events of this type |
| `exception_event_count` | long | Events of this type flagged as exception |
| `exception_shipment_count` | long | Distinct shipments with at least one exception event of this type |
| `rate` | double | `event_count / total_shipments` (per-grain denominator) |
| `avg_delay_minutes` | double | `AVG(delay_minutes)` |
| `is_exception_event_type` | boolean | `event_type IN ('DELAYED','EXCEPTION','HOLD')` |
| `total_shipments` | long | Per-grain shipment denominator (from `fct_shipment`) |

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
