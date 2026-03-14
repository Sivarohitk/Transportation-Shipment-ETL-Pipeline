-- Purpose: Stage shipment records after ingestion-time schema enforcement.
-- Grain: One row per raw shipment record (deduplication is handled in curated models).

WITH normalized AS (
  SELECT
    UPPER(TRIM(shipment_id)) AS shipment_id,
    UPPER(TRIM(carrier_id)) AS carrier_id,
    UPPER(TRIM(origin_state)) AS origin_state,
    UPPER(TRIM(destination_state)) AS destination_state,
    pickup_ts,
    promised_delivery_ts,
    actual_delivery_ts,
    CAST(shipping_cost_usd AS DOUBLE) AS shipping_cost_usd,
    CAST(distance_miles AS DOUBLE) AS distance_miles,
    updated_at
  FROM raw_shipments
)
SELECT
  shipment_id,
  carrier_id,
  origin_state,
  destination_state,
  pickup_ts,
  promised_delivery_ts,
  actual_delivery_ts,
  shipping_cost_usd,
  distance_miles,
  updated_at
FROM normalized
WHERE shipment_id IS NOT NULL
  AND carrier_id IS NOT NULL;
