-- Gold analytics: route_performance
-- Grain: one row per (p_date, origin_region_code, destination_region_code, carrier_id).
-- Source tables: fct_shipment
-- Metrics:
--   shipment_count
--   delivered_shipments
--   on_time_shipments
--   late_shipments
--   exception_shipments
--   on_time_rate
--   late_rate
--   exception_rate
--   avg_transit_hours
--   avg_cost_per_mile
--   total_shipping_cost_usd
--   total_distance_miles

WITH base AS (
  SELECT
    p_date,
    COALESCE(origin_region_code, 'UNKNOWN') AS origin_region_code,
    COALESCE(region_code, 'UNKNOWN') AS destination_region_code,
    COALESCE(carrier_id, 'UNKNOWN') AS carrier_id,
    shipment_id,
    on_time_delivery_flag,
    exception_flag,
    transit_time_hours,
    shipping_cost_usd,
    distance_miles,
    delivered_flag,
    delay_minutes
  FROM fct_shipment
),
agg AS (
  SELECT
    p_date,
    origin_region_code,
    destination_region_code,
    carrier_id,
    COUNT(DISTINCT shipment_id) AS shipment_count,
    COALESCE(SUM(delivered_flag), 0) AS delivered_shipments,
    COALESCE(SUM(on_time_delivery_flag), 0) AS on_time_shipments,
    COALESCE(SUM(CASE WHEN delay_minutes > 0 THEN 1 ELSE 0 END), 0) AS late_shipments,
    COALESCE(SUM(exception_flag), 0) AS exception_shipments,
    AVG(transit_time_hours) AS avg_transit_hours,
    COALESCE(SUM(shipping_cost_usd), 0.0) AS total_shipping_cost_usd,
    COALESCE(SUM(distance_miles), 0.0) AS total_distance_miles
  FROM base
  GROUP BY p_date, origin_region_code, destination_region_code, carrier_id
)
SELECT
  p_date,
  origin_region_code,
  destination_region_code,
  carrier_id,
  shipment_count,
  delivered_shipments,
  on_time_shipments,
  late_shipments,
  exception_shipments,
  CASE WHEN delivered_shipments > 0
       THEN on_time_shipments * 1.0 / delivered_shipments
       ELSE 0.0 END AS on_time_rate,
  CASE WHEN delivered_shipments > 0
       THEN late_shipments * 1.0 / delivered_shipments
       ELSE 0.0 END AS late_rate,
  CASE WHEN shipment_count > 0
       THEN exception_shipments * 1.0 / shipment_count
       ELSE 0.0 END AS exception_rate,
  COALESCE(avg_transit_hours, 0.0) AS avg_transit_hours,
  CASE WHEN total_distance_miles > 0
       THEN total_shipping_cost_usd / total_distance_miles
       ELSE 0.0 END AS avg_cost_per_mile,
  total_shipping_cost_usd,
  total_distance_miles
FROM agg;
