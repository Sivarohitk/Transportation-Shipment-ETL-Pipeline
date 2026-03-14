-- Purpose: Stage delivery-event records with normalized business tokens.
-- Grain: One row per raw delivery event record.

WITH normalized AS (
  SELECT
    UPPER(TRIM(event_id)) AS event_id,
    UPPER(TRIM(shipment_id)) AS shipment_id,
    UPPER(TRIM(event_type)) AS event_type,
    event_ts,
    TRIM(event_city) AS event_city,
    UPPER(TRIM(event_state)) AS event_state,
    UPPER(TRIM(delay_reason)) AS delay_reason,
    CAST(attempt_number AS INT) AS attempt_number,
    updated_at
  FROM raw_delivery_events
)
SELECT
  event_id,
  shipment_id,
  event_type,
  event_ts,
  event_city,
  event_state,
  delay_reason,
  attempt_number,
  updated_at
FROM normalized
WHERE event_id IS NOT NULL
  AND shipment_id IS NOT NULL
  AND event_type IS NOT NULL;
