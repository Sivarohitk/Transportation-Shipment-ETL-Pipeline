# Data Dictionary

This document defines the synthetic sample input data used for ETL development and testing.
All timestamps are in UTC (`Z` suffix) and follow ISO 8601 format.

## Dataset: `data/sample/raw/shipments_2026-01-01.csv`

**Business grain:** shipment-level records from the source shipment system.

| Column | Type | Business meaning | Notes / edge cases in sample |
|---|---|---|---|
| `shipment_id` | string | Unique shipment identifier from source system. | Intentional duplicate: `SHP1006` appears twice (late-arriving update scenario). |
| `carrier_id` | string | Carrier assigned to transport the shipment. | Maps to `carriers.carrier_id`. |
| `origin_state` | string | Origin state where shipment was picked up. | Used for route analytics. |
| `destination_state` | string | Destination state for final delivery. | Missing value present (`SHP1011`) to test null handling. |
| `pickup_ts` | timestamp | Timestamp when shipment left origin. | Multiple pickup dates included. |
| `promised_delivery_ts` | timestamp | SLA target delivery timestamp. | Used to compute on-time/late status. |
| `actual_delivery_ts` | timestamp | Actual delivered timestamp. | Missing for in-progress/exception shipments (`SHP1004`, `SHP1011`). |
| `shipping_cost_usd` | decimal | Total shipment cost in USD. | Missing value present (`SHP1007`). |
| `distance_miles` | decimal | Route distance in miles. | Missing value present (`SHP1009`). |
| `updated_at` | timestamp | Last source update timestamp for this row. | Supports deduplication by latest record. |

## Dataset: `data/sample/raw/carriers_2026-01-01.csv`

**Business grain:** carrier master records.

| Column | Type | Business meaning | Notes / edge cases in sample |
|---|---|---|---|
| `carrier_id` | string | Unique carrier identifier. | Referenced by shipments. |
| `carrier_name` | string | Carrier legal/trade name. | Multiple active carriers plus one inactive carrier. |
| `scac` | string | Standard Carrier Alpha Code. | Missing value present (`CAR005`). |
| `service_mode` | string | Service category (FTL/LTL/PARCEL). | Used for operational slicing. |
| `home_region_code` | string | Carrier's primary operating region. | Missing value present (`CAR006`). |
| `is_active` | boolean | Whether the carrier is actively contracted. | Includes `false` for inactive carrier. |
| `updated_at` | timestamp | Last source update timestamp. | Enables future SCD/snapshot logic. |

## Dataset: `data/sample/raw/delivery_events_2026-01-01.csv`

**Business grain:** event-level tracking history for shipments.

| Column | Type | Business meaning | Notes / edge cases in sample |
|---|---|---|---|
| `event_id` | string | Unique event record identifier. | One row per shipment event. |
| `shipment_id` | string | Shipment to which the event belongs. | Joins to shipments. |
| `event_type` | string | Tracking event category (e.g., `PICKED_UP`, `DELAYED`, `DELIVERED`). | Includes exception/hold/attempt states. |
| `event_ts` | timestamp | Event occurrence timestamp. | Spans multiple days per shipment lifecycle. |
| `event_city` | string | City where event occurred. | Missing city present (`EVT2016`). |
| `event_state` | string | State where event occurred. | Supports regional rollups. |
| `delay_reason` | string | Standardized delay/exception reason. | Values include `WEATHER`, `NO_RECIPIENT`, `ADDRESS_ISSUE`, etc. |
| `attempt_number` | integer | Delivery attempt sequence number. | Includes first-attempt and non-first-attempt delivery patterns. |
| `updated_at` | timestamp | Last source update timestamp for event row. | Useful for event dedup/order handling. |

## Dataset: `data/sample/reference/region_lookup.csv`

**Business grain:** state-to-region mapping reference.

| Column | Type | Business meaning | Notes |
|---|---|---|---|
| `state_code` | string | Two-letter state code. | Includes states seen in synthetic sample data. |
| `region_code` | string | Standard region bucket (`WEST`, `SOUTH`, `MIDWEST`, `NORTHEAST`). | Used for partitioning and analytics. |

## Intentional Edge Cases Included

- Duplicate shipment records (`SHP1006`).
- Missing values across critical fields (`destination_state`, `actual_delivery_ts`, `shipping_cost_usd`, `distance_miles`, `scac`, `home_region_code`, `event_city`).
- Delayed deliveries (`actual_delivery_ts > promised_delivery_ts`) and explicit `DELAYED` events.
- Exception workflows (`EXCEPTION`, `HOLD`).
- Multiple carriers and service modes.
- Multiple regions and state mappings.
- Multi-day event timelines across several dates.
- Both first-attempt and later-attempt successful deliveries.
