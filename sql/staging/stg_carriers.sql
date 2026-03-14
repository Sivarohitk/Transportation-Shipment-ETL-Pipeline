-- Purpose: Stage carrier records with lightweight normalization.
-- Grain: One row per raw carrier record.

WITH normalized AS (
  SELECT
    UPPER(TRIM(carrier_id)) AS carrier_id,
    TRIM(carrier_name) AS carrier_name,
    UPPER(TRIM(scac)) AS scac,
    UPPER(TRIM(service_mode)) AS service_mode,
    UPPER(TRIM(home_region_code)) AS home_region_code,
    CAST(is_active AS BOOLEAN) AS is_active,
    updated_at
  FROM raw_carriers
)
SELECT
  carrier_id,
  carrier_name,
  scac,
  service_mode,
  home_region_code,
  is_active,
  updated_at
FROM normalized
WHERE carrier_id IS NOT NULL
  AND carrier_name IS NOT NULL
  AND service_mode IS NOT NULL;
