-- Curated model: dim_carrier
-- Grain: one row per carrier per snapshot date (p_date).

WITH standardized AS (
  SELECT
    UPPER(TRIM(carrier_id)) AS carrier_id,
    TRIM(carrier_name) AS carrier_name,
    UPPER(TRIM(scac)) AS scac,
    UPPER(TRIM(service_mode)) AS service_mode,
    UPPER(TRIM(home_region_code)) AS home_region_code,
    is_active,
    updated_at
  FROM stg_carriers
),
latest_per_carrier AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY carrier_id
      ORDER BY updated_at DESC NULLS LAST, carrier_name ASC NULLS LAST
    ) AS rn
  FROM standardized
)
SELECT
  carrier_id,
  carrier_name,
  scac,
  service_mode,
  COALESCE(home_region_code, 'UNKNOWN') AS region_code,
  is_active,
  updated_at,
  CAST(current_date() AS DATE) AS p_date
FROM latest_per_carrier
WHERE rn = 1;

-- Example query: active carrier count by region
-- SELECT region_code, COUNT(*) AS active_carriers
-- FROM curated.dim_carrier
-- WHERE is_active = true
-- GROUP BY region_code;
