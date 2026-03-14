-- Curated model: agg_shipment_daily
-- Grain: one row per (p_date, region_code, carrier_id).

WITH shipment_agg AS (
  SELECT
    p_date,
    region_code,
    carrier_id,
    COUNT(DISTINCT shipment_id) AS total_shipments,
    SUM(COALESCE(delivered_flag, 0)) AS delivered_shipments,
    SUM(COALESCE(on_time_delivery_flag, 0)) AS on_time_shipments,
    SUM(CASE WHEN COALESCE(delay_minutes, 0.0) > 0 THEN 1 ELSE 0 END) AS delayed_shipments,
    SUM(COALESCE(exception_flag, 0)) AS exception_shipments,
    SUM(COALESCE(delay_minutes, 0.0)) AS total_delay_minutes,
    AVG(CASE WHEN COALESCE(delay_minutes, 0.0) > 0 THEN delay_minutes END) AS avg_delay_minutes,
    AVG(transit_time_hours) AS avg_transit_time_hours,
    SUM(COALESCE(shipping_cost_usd, 0.0)) AS total_shipping_cost_usd,
    SUM(COALESCE(distance_miles, 0.0)) AS total_distance_miles
  FROM fct_shipment
  GROUP BY p_date, region_code, carrier_id
),
event_agg AS (
  SELECT
    p_date,
    region_code,
    carrier_id,
    COUNT(*) AS total_delivery_events
  FROM fct_delivery_event
  GROUP BY p_date, region_code, carrier_id
)
SELECT
  s.p_date,
  s.region_code,
  s.carrier_id,
  s.total_shipments,
  s.delivered_shipments,
  s.on_time_shipments,
  s.delayed_shipments,
  s.exception_shipments,
  s.total_delay_minutes,
  s.avg_delay_minutes,
  s.avg_transit_time_hours,
  s.total_shipping_cost_usd,
  s.total_distance_miles,
  COALESCE(e.total_delivery_events, 0) AS total_delivery_events,
  CASE
    WHEN s.delivered_shipments > 0 THEN s.on_time_shipments * 1.0 / s.delivered_shipments
    ELSE 0.0
  END AS on_time_delivery_rate,
  CASE
    WHEN s.total_shipments > 0 THEN s.exception_shipments * 1.0 / s.total_shipments
    ELSE 0.0
  END AS exception_rate,
  CASE
    WHEN s.total_distance_miles > 0 THEN s.total_shipping_cost_usd / s.total_distance_miles
    ELSE 0.0
  END AS avg_cost_per_mile
FROM shipment_agg s
LEFT JOIN event_agg e
  ON s.p_date = e.p_date
 AND s.region_code = e.region_code
 AND s.carrier_id = e.carrier_id;

-- Example query: top 10 lane-day volumes
-- SELECT p_date, region_code, carrier_id, total_shipments
-- FROM curated.agg_shipment_daily
-- ORDER BY total_shipments DESC
-- LIMIT 10;
