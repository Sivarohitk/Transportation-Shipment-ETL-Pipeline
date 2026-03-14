-- Purpose: Reconcile row counts between staging and curated layers.

WITH staging_counts AS (
  SELECT 'shipments' AS entity, COUNT(DISTINCT shipment_id) AS staging_count FROM stg_shipments
  UNION ALL
  SELECT 'carriers' AS entity, COUNT(DISTINCT carrier_id) AS staging_count FROM stg_carriers
  UNION ALL
  SELECT 'delivery_events' AS entity, COUNT(DISTINCT event_id) AS staging_count FROM stg_delivery_events
),
curated_counts AS (
  SELECT 'shipments' AS entity, COUNT(DISTINCT shipment_id) AS curated_count FROM fct_shipment
  UNION ALL
  SELECT 'carriers' AS entity, COUNT(DISTINCT carrier_id) AS curated_count FROM dim_carrier
  UNION ALL
  SELECT 'delivery_events' AS entity, COUNT(DISTINCT event_id) AS curated_count FROM fct_delivery_event
)
SELECT
  s.entity,
  s.staging_count,
  c.curated_count,
  c.curated_count - s.staging_count AS row_delta,
  CASE
    WHEN s.staging_count = 0 THEN 0.0
    ELSE (c.curated_count - s.staging_count) * 1.0 / s.staging_count
  END AS variance_ratio
FROM staging_counts s
JOIN curated_counts c
  ON s.entity = c.entity
ORDER BY s.entity;
