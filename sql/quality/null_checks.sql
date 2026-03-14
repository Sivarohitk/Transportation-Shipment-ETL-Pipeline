-- Purpose: Detect required-null and blank-string violations in curated tables.

WITH null_metrics AS (
  SELECT
    'fct_shipment' AS table_name,
    'shipment_id' AS column_name,
    COUNT(*) AS null_count
  FROM fct_shipment
  WHERE shipment_id IS NULL OR TRIM(CAST(shipment_id AS STRING)) = ''

  UNION ALL

  SELECT
    'fct_shipment' AS table_name,
    'carrier_id' AS column_name,
    COUNT(*) AS null_count
  FROM fct_shipment
  WHERE carrier_id IS NULL OR TRIM(CAST(carrier_id AS STRING)) = ''

  UNION ALL

  SELECT
    'fct_shipment' AS table_name,
    'pickup_ts' AS column_name,
    COUNT(*) AS null_count
  FROM fct_shipment
  WHERE pickup_ts IS NULL

  UNION ALL

  SELECT
    'dim_carrier' AS table_name,
    'carrier_id' AS column_name,
    COUNT(*) AS null_count
  FROM dim_carrier
  WHERE carrier_id IS NULL OR TRIM(CAST(carrier_id AS STRING)) = ''

  UNION ALL

  SELECT
    'dim_carrier' AS table_name,
    'carrier_name' AS column_name,
    COUNT(*) AS null_count
  FROM dim_carrier
  WHERE carrier_name IS NULL OR TRIM(CAST(carrier_name AS STRING)) = ''

  UNION ALL

  SELECT
    'fct_delivery_event' AS table_name,
    'event_id' AS column_name,
    COUNT(*) AS null_count
  FROM fct_delivery_event
  WHERE event_id IS NULL OR TRIM(CAST(event_id AS STRING)) = ''

  UNION ALL

  SELECT
    'fct_delivery_event' AS table_name,
    'shipment_id' AS column_name,
    COUNT(*) AS null_count
  FROM fct_delivery_event
  WHERE shipment_id IS NULL OR TRIM(CAST(shipment_id AS STRING)) = ''

  UNION ALL

  SELECT
    'fct_delivery_event' AS table_name,
    'event_ts' AS column_name,
    COUNT(*) AS null_count
  FROM fct_delivery_event
  WHERE event_ts IS NULL
)
SELECT
  table_name,
  column_name,
  null_count
FROM null_metrics
WHERE null_count > 0
ORDER BY table_name, column_name;
