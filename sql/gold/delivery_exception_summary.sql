-- Gold analytics: delivery_exception_summary
-- Grain: one row per (p_date, event_type, carrier_id, region_code).
-- Source tables: fct_delivery_event, fct_shipment
-- Metrics:
--   event_count
--   shipment_count
--   exception_event_count
--   exception_shipment_count
--   rate
--   avg_delay_minutes
--   is_exception_event_type
--   total_shipments

WITH event_base AS (
  SELECT
    p_date,
    COALESCE(event_type, 'UNKNOWN') AS event_type,
    COALESCE(carrier_id, 'UNKNOWN') AS carrier_id,
    COALESCE(region_code, 'UNKNOWN') AS region_code,
    shipment_id,
    exception_flag,
    delay_minutes
  FROM fct_delivery_event
),
event_agg AS (
  SELECT
    p_date,
    event_type,
    carrier_id,
    region_code,
    COUNT(1) AS event_count,
    COUNT(DISTINCT shipment_id) AS shipment_count,
    COALESCE(SUM(exception_flag), 0) AS exception_event_count,
    COUNT(DISTINCT CASE WHEN exception_flag = 1 THEN shipment_id END)
      AS exception_shipment_count,
    AVG(delay_minutes) AS avg_delay_minutes,
    CASE
      WHEN event_type IN ('DELAYED', 'EXCEPTION', 'HOLD') THEN TRUE
      ELSE FALSE
    END AS is_exception_event_type
  FROM event_base
  GROUP BY p_date, event_type, carrier_id, region_code
),
shipment_denominator AS (
  SELECT
    p_date,
    carrier_id,
    COALESCE(region_code, 'UNKNOWN') AS region_code,
    COUNT(DISTINCT shipment_id) AS total_shipments
  FROM fct_shipment
  GROUP BY p_date, carrier_id, region_code
)
SELECT
  e.p_date,
  e.event_type,
  e.carrier_id,
  e.region_code,
  e.event_count,
  e.shipment_count,
  e.exception_event_count,
  e.exception_shipment_count,
  CASE WHEN COALESCE(s.total_shipments, 0) > 0
       THEN e.event_count * 1.0 / s.total_shipments
       ELSE 0.0 END AS rate,
  COALESCE(e.avg_delay_minutes, 0.0) AS avg_delay_minutes,
  e.is_exception_event_type,
  COALESCE(s.total_shipments, 0) AS total_shipments
FROM event_agg e
LEFT JOIN shipment_denominator s
  ON e.p_date = s.p_date
 AND e.carrier_id = s.carrier_id
 AND e.region_code = s.region_code;
