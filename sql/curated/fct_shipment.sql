-- Curated model: fct_shipment
-- Grain: one row per shipment_id (latest shipment record).

WITH standardized AS (
  SELECT
    UPPER(TRIM(shipment_id)) AS shipment_id,
    UPPER(TRIM(carrier_id)) AS carrier_id,
    UPPER(TRIM(origin_state)) AS origin_state,
    UPPER(TRIM(destination_state)) AS destination_state,
    pickup_ts,
    promised_delivery_ts,
    actual_delivery_ts,
    shipping_cost_usd,
    distance_miles,
    updated_at
  FROM stg_shipments
),
latest_per_shipment AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY shipment_id
      ORDER BY updated_at DESC NULLS LAST, pickup_ts DESC NULLS LAST
    ) AS rn
  FROM standardized
),
shipment_base AS (
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
  FROM latest_per_shipment
  WHERE rn = 1
),
lookup AS (
  SELECT
    UPPER(TRIM(state_code)) AS state_code,
    UPPER(TRIM(region_code)) AS region_code
  FROM region_lookup
)
SELECT
  s.shipment_id,
  COALESCE(s.carrier_id, 'UNKNOWN') AS carrier_id,
  s.origin_state,
  s.destination_state,
  s.pickup_ts,
  s.promised_delivery_ts,
  s.actual_delivery_ts,
  s.updated_at,
  s.shipping_cost_usd,
  s.distance_miles,
  CASE WHEN s.actual_delivery_ts IS NOT NULL THEN 1 ELSE 0 END AS delivered_flag,
  CASE
    WHEN s.actual_delivery_ts IS NOT NULL
     AND s.promised_delivery_ts IS NOT NULL
     AND s.actual_delivery_ts <= s.promised_delivery_ts
      THEN 1
    ELSE 0
  END AS on_time_delivery_flag,
  CASE
    WHEN s.actual_delivery_ts IS NOT NULL
     AND s.promised_delivery_ts IS NOT NULL
      THEN GREATEST((unix_timestamp(s.actual_delivery_ts) - unix_timestamp(s.promised_delivery_ts)) / 60.0, 0.0)
    ELSE NULL
  END AS delay_minutes,
  CASE
    WHEN s.destination_state IS NULL THEN 1
    WHEN s.actual_delivery_ts IS NULL
     AND s.promised_delivery_ts IS NOT NULL
     AND s.updated_at IS NOT NULL
     AND s.updated_at > s.promised_delivery_ts
      THEN 1
    ELSE 0
  END AS exception_flag,
  CASE
    WHEN s.actual_delivery_ts IS NOT NULL AND s.pickup_ts IS NOT NULL
      THEN (unix_timestamp(s.actual_delivery_ts) - unix_timestamp(s.pickup_ts)) / 3600.0
    ELSE NULL
  END AS transit_time_hours,
  COALESCE(CAST(s.pickup_ts AS DATE), CAST(s.promised_delivery_ts AS DATE), CAST(s.updated_at AS DATE), current_date()) AS p_date,
  COALESCE(l.region_code, 'UNKNOWN') AS region_code
FROM shipment_base s
LEFT JOIN lookup l
  ON s.destination_state = l.state_code;

-- Example query: delayed shipment count by carrier
-- SELECT carrier_id, COUNT(*) AS delayed_shipments
-- FROM curated.fct_shipment
-- WHERE delay_minutes > 0
-- GROUP BY carrier_id;
