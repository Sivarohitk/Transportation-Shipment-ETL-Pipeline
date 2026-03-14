-- Curated model: fct_delivery_event
-- Grain: one row per event_id (latest event record).

WITH events_standardized AS (
  SELECT
    UPPER(TRIM(event_id)) AS event_id,
    UPPER(TRIM(shipment_id)) AS shipment_id,
    UPPER(TRIM(event_type)) AS event_type,
    event_ts,
    TRIM(event_city) AS event_city,
    UPPER(TRIM(event_state)) AS event_state,
    UPPER(TRIM(delay_reason)) AS delay_reason,
    attempt_number,
    updated_at
  FROM stg_delivery_events
),
shipments_standardized AS (
  SELECT
    UPPER(TRIM(shipment_id)) AS shipment_id,
    UPPER(TRIM(carrier_id)) AS carrier_id,
    UPPER(TRIM(destination_state)) AS destination_state,
    pickup_ts,
    promised_delivery_ts,
    actual_delivery_ts,
    updated_at
  FROM stg_shipments
),
latest_event AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY event_id
      ORDER BY updated_at DESC NULLS LAST, event_ts DESC NULLS LAST
    ) AS rn
  FROM events_standardized
),
latest_shipment AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY shipment_id
      ORDER BY updated_at DESC NULLS LAST
    ) AS rn
  FROM shipments_standardized
),
lookup AS (
  SELECT
    UPPER(TRIM(state_code)) AS state_code,
    UPPER(TRIM(region_code)) AS region_code
  FROM region_lookup
)
SELECT
  e.event_id,
  e.shipment_id,
  COALESCE(s.carrier_id, 'UNKNOWN') AS carrier_id,
  e.event_type,
  e.event_ts,
  e.event_city,
  e.event_state,
  e.delay_reason,
  e.attempt_number,
  e.updated_at,
  COALESCE(CAST(e.event_ts AS DATE), CAST(e.updated_at AS DATE), current_date()) AS p_date,
  COALESCE(l_event.region_code, l_ship.region_code, 'UNKNOWN') AS region_code,
  CASE
    WHEN e.event_type = 'DELIVERED'
     AND e.event_ts IS NOT NULL
     AND s.promised_delivery_ts IS NOT NULL
     AND e.event_ts <= s.promised_delivery_ts
      THEN 1
    ELSE 0
  END AS on_time_delivery_flag,
  CASE
    WHEN e.event_ts IS NOT NULL
     AND s.promised_delivery_ts IS NOT NULL
      THEN GREATEST((unix_timestamp(e.event_ts) - unix_timestamp(s.promised_delivery_ts)) / 60.0, 0.0)
    ELSE NULL
  END AS delay_minutes,
  CASE
    WHEN e.event_type IN ('EXCEPTION', 'HOLD') THEN 1
    ELSE 0
  END AS exception_flag,
  CASE
    WHEN e.event_type = 'DELIVERED'
     AND e.event_ts IS NOT NULL
     AND s.pickup_ts IS NOT NULL
      THEN (unix_timestamp(e.event_ts) - unix_timestamp(s.pickup_ts)) / 3600.0
    ELSE NULL
  END AS transit_time_hours
FROM latest_event e
LEFT JOIN latest_shipment s
  ON e.shipment_id = s.shipment_id
 AND s.rn = 1
LEFT JOIN lookup l_event
  ON e.event_state = l_event.state_code
LEFT JOIN lookup l_ship
  ON s.destination_state = l_ship.state_code
WHERE e.rn = 1;

-- Example query: exception events by day
-- SELECT p_date, COUNT(*) AS exception_events
-- FROM curated.fct_delivery_event
-- WHERE exception_flag = 1
-- GROUP BY p_date
-- ORDER BY p_date;
