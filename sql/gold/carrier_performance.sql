-- Gold analytics: carrier_performance
-- Grain: one row per (p_date, carrier_id, service_mode).
-- Source tables: fct_shipment, fct_delivery_event, dim_carrier
-- Metrics:
--   shipment_volume
--   delivered_shipments
--   on_time_shipments
--   late_shipments
--   exception_shipments
--   first_attempt_success_shipments
--   on_time_delivery_rate
--   late_delivery_rate
--   exception_rate
--   first_attempt_success_rate
--   avg_transit_hours
--   total_delay_minutes
--   avg_cost_per_mile
--   total_shipping_cost_usd
--   total_distance_miles

WITH shipment_base AS (
  SELECT
    s.shipment_id,
    s.carrier_id,
    s.p_date,
    s.on_time_delivery_flag,
    s.exception_flag,
    s.transit_time_hours,
    s.shipping_cost_usd,
    s.distance_miles,
    s.delivered_flag,
    s.delay_minutes,
    COALESCE(c.service_mode, 'UNKNOWN') AS service_mode
  FROM fct_shipment s
  LEFT JOIN dim_carrier c
    ON s.carrier_id = c.carrier_id
   AND s.p_date = c.p_date
),
first_attempt AS (
  SELECT
    p_date,
    carrier_id,
    COUNT(DISTINCT CASE WHEN event_type = 'DELIVERED' AND attempt_number = 1 THEN shipment_id END)
      AS first_attempt_success_shipments
  FROM fct_delivery_event
  GROUP BY p_date, carrier_id
),
agg AS (
  SELECT
    p_date,
    carrier_id,
    service_mode,
    COUNT(DISTINCT shipment_id) AS shipment_volume,
    COALESCE(SUM(delivered_flag), 0) AS delivered_shipments,
    COALESCE(SUM(on_time_delivery_flag), 0) AS on_time_shipments,
    COALESCE(SUM(CASE WHEN delay_minutes > 0 THEN 1 ELSE 0 END), 0) AS late_shipments,
    COALESCE(SUM(exception_flag), 0) AS exception_shipments,
    COALESCE(SUM(delay_minutes), 0.0) AS total_delay_minutes,
    AVG(transit_time_hours) AS avg_transit_hours,
    COALESCE(SUM(shipping_cost_usd), 0.0) AS total_shipping_cost_usd,
    COALESCE(SUM(distance_miles), 0.0) AS total_distance_miles
  FROM shipment_base
  GROUP BY p_date, carrier_id, service_mode
)
SELECT
  a.p_date,
  a.carrier_id,
  a.service_mode,
  a.shipment_volume,
  a.delivered_shipments,
  a.on_time_shipments,
  a.late_shipments,
  a.exception_shipments,
  COALESCE(f.first_attempt_success_shipments, 0) AS first_attempt_success_shipments,
  CASE WHEN a.delivered_shipments > 0
       THEN a.on_time_shipments * 1.0 / a.delivered_shipments
       ELSE 0.0 END AS on_time_delivery_rate,
  CASE WHEN a.delivered_shipments > 0
       THEN a.late_shipments * 1.0 / a.delivered_shipments
       ELSE 0.0 END AS late_delivery_rate,
  CASE WHEN a.shipment_volume > 0
       THEN a.exception_shipments * 1.0 / a.shipment_volume
       ELSE 0.0 END AS exception_rate,
  CASE WHEN a.delivered_shipments > 0
       THEN COALESCE(f.first_attempt_success_shipments, 0) * 1.0 / a.delivered_shipments
       ELSE 0.0 END AS first_attempt_success_rate,
  COALESCE(a.avg_transit_hours, 0.0) AS avg_transit_hours,
  a.total_delay_minutes,
  CASE WHEN a.total_distance_miles > 0
       THEN a.total_shipping_cost_usd / a.total_distance_miles
       ELSE 0.0 END AS avg_cost_per_mile,
  a.total_shipping_cost_usd,
  a.total_distance_miles
FROM agg a
LEFT JOIN first_attempt f
  ON a.p_date = f.p_date
 AND a.carrier_id = f.carrier_id;
