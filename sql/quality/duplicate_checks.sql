-- Purpose: Detect duplicate keys in curated tables.

WITH duplicate_metrics AS (
  SELECT
    'fct_shipment' AS table_name,
    shipment_id AS key_value,
    CAST(NULL AS STRING) AS key_value_2,
    COUNT(*) AS row_count
  FROM fct_shipment
  GROUP BY shipment_id
  HAVING COUNT(*) > 1

  UNION ALL

  SELECT
    'dim_carrier' AS table_name,
    carrier_id AS key_value,
    CAST(p_date AS STRING) AS key_value_2,
    COUNT(*) AS row_count
  FROM dim_carrier
  GROUP BY carrier_id, p_date
  HAVING COUNT(*) > 1

  UNION ALL

  SELECT
    'fct_delivery_event' AS table_name,
    event_id AS key_value,
    CAST(NULL AS STRING) AS key_value_2,
    COUNT(*) AS row_count
  FROM fct_delivery_event
  GROUP BY event_id
  HAVING COUNT(*) > 1
)
SELECT
  table_name,
  key_value,
  key_value_2,
  row_count
FROM duplicate_metrics
ORDER BY table_name, row_count DESC, key_value;
