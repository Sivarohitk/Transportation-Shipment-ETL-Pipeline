-- KPI model: kpi_delivery_daily
-- Grain: one row per (p_date, region_code, carrier_id).
-- Metrics:
--   on_time_delivery_rate
--   avg_transit_hours
--   late_delivery_rate
--   first_attempt_success_rate
--   exception_rate
--   avg_cost_per_mile
--   volume_by_carrier
--   delivery_event_density

WITH agg AS (
  SELECT
    p_date,
    region_code,
    carrier_id,
    total_shipments,
    delivered_shipments,
    on_time_shipments,
    delayed_shipments,
    exception_shipments,
    avg_transit_time_hours,
    total_shipping_cost_usd,
    total_distance_miles
  FROM agg_shipment_daily
),
first_attempt AS (
  SELECT
    p_date,
    region_code,
    carrier_id,
    COUNT(1) AS total_delivery_events,
    COUNT(DISTINCT CASE WHEN event_type = 'DELIVERED' THEN shipment_id END) AS delivered_event_shipments,
    COUNT(DISTINCT CASE WHEN event_type = 'DELIVERED' AND attempt_number = 1 THEN shipment_id END) AS first_attempt_success_shipments
  FROM fct_delivery_event
  GROUP BY p_date, region_code, carrier_id
),
base AS (
  SELECT
    a.p_date,
    a.region_code,
    a.carrier_id,
    a.total_shipments,
    a.delivered_shipments,
    a.on_time_shipments,
    a.delayed_shipments,
    a.exception_shipments,
    a.avg_transit_time_hours,
    a.total_shipping_cost_usd,
    a.total_distance_miles,
    COALESCE(f.total_delivery_events, 0) AS total_delivery_events,
    COALESCE(f.delivered_event_shipments, 0) AS delivered_event_shipments,
    COALESCE(f.first_attempt_success_shipments, 0) AS first_attempt_success_shipments,
    GREATEST(COALESCE(a.delivered_shipments, 0), COALESCE(f.delivered_event_shipments, 0)) AS delivered_denominator
  FROM agg a
  LEFT JOIN first_attempt f
    ON a.p_date = f.p_date
   AND a.region_code = f.region_code
   AND a.carrier_id = f.carrier_id
)
SELECT
  p_date,
  region_code,
  carrier_id,
  CASE
    WHEN delivered_denominator > 0 THEN on_time_shipments * 1.0 / delivered_denominator
    ELSE 0.0
  END AS on_time_delivery_rate,
  COALESCE(avg_transit_time_hours, 0.0) AS avg_transit_hours,
  CASE
    WHEN delivered_denominator > 0 THEN delayed_shipments * 1.0 / delivered_denominator
    ELSE 0.0
  END AS late_delivery_rate,
  CASE
    WHEN delivered_denominator > 0 THEN first_attempt_success_shipments * 1.0 / delivered_denominator
    ELSE 0.0
  END AS first_attempt_success_rate,
  CASE
    WHEN total_shipments > 0 THEN exception_shipments * 1.0 / total_shipments
    ELSE 0.0
  END AS exception_rate,
  CASE
    WHEN total_distance_miles > 0 THEN total_shipping_cost_usd / total_distance_miles
    ELSE 0.0
  END AS avg_cost_per_mile,
  total_shipments AS volume_by_carrier,
  CASE
    WHEN total_shipments > 0 THEN total_delivery_events * 1.0 / total_shipments
    ELSE 0.0
  END AS delivery_event_density
FROM base;

-- Query examples
-- 1) Top carriers by on-time rate for a day
-- SELECT carrier_id, on_time_delivery_rate
-- FROM curated.kpi_delivery_daily
-- WHERE p_date = DATE '2026-01-03'
-- ORDER BY on_time_delivery_rate DESC
-- LIMIT 10;

-- 2) Regions with highest exception rate in a week
-- SELECT region_code, AVG(exception_rate) AS avg_exception_rate
-- FROM curated.kpi_delivery_daily
-- WHERE p_date BETWEEN DATE '2026-01-01' AND DATE '2026-01-07'
-- GROUP BY region_code
-- ORDER BY avg_exception_rate DESC;

-- 3) Carrier event density vs volume trend
-- SELECT p_date, carrier_id, volume_by_carrier, delivery_event_density
-- FROM curated.kpi_delivery_daily
-- ORDER BY p_date, carrier_id;
