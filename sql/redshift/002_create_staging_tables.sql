-- Persistent Redshift staging tables used as COPY and MERGE sources.
--
-- Each staging table deliberately has the same columns, order, nullability,
-- and data types as its corresponding final table. This permits Redshift's
-- simplified `MERGE ... REMOVE DUPLICATES` mode. The publisher clears a
-- staging table transactionally before loading the next batch.

CREATE TABLE IF NOT EXISTS {{staging_schema}}.dim_carrier (
    carrier_id VARCHAR(128) NOT NULL,
    carrier_name VARCHAR(256) NOT NULL,
    scac VARCHAR(4),
    service_mode VARCHAR(32) NOT NULL,
    region_code VARCHAR(32) NOT NULL,
    is_active BOOLEAN,
    updated_at TIMESTAMP NOT NULL,
    p_date DATE NOT NULL
)
DISTSTYLE AUTO
SORTKEY AUTO
ENCODE AUTO;

CREATE TABLE IF NOT EXISTS {{staging_schema}}.fct_shipment (
    shipment_id VARCHAR(128) NOT NULL,
    carrier_id VARCHAR(128) NOT NULL,
    origin_state VARCHAR(2) NOT NULL,
    destination_state VARCHAR(2) NOT NULL,
    origin_region_code VARCHAR(32),
    region_code VARCHAR(32) NOT NULL,
    pickup_ts TIMESTAMP NOT NULL,
    promised_delivery_ts TIMESTAMP NOT NULL,
    actual_delivery_ts TIMESTAMP,
    updated_at TIMESTAMP NOT NULL,
    shipping_cost_usd DOUBLE PRECISION,
    distance_miles DOUBLE PRECISION,
    delivered_flag INTEGER,
    on_time_delivery_flag INTEGER,
    delay_minutes DOUBLE PRECISION,
    exception_flag INTEGER,
    transit_time_hours DOUBLE PRECISION,
    p_date DATE NOT NULL
)
DISTSTYLE AUTO
SORTKEY AUTO
ENCODE AUTO;

CREATE TABLE IF NOT EXISTS {{staging_schema}}.fct_delivery_event (
    event_id VARCHAR(128) NOT NULL,
    shipment_id VARCHAR(128) NOT NULL,
    carrier_id VARCHAR(128) NOT NULL,
    event_type VARCHAR(32) NOT NULL,
    event_ts TIMESTAMP NOT NULL,
    event_city VARCHAR(256),
    event_state VARCHAR(2),
    delay_reason VARCHAR(512),
    attempt_number INTEGER,
    updated_at TIMESTAMP NOT NULL,
    region_code VARCHAR(32) NOT NULL,
    p_date DATE NOT NULL,
    on_time_delivery_flag INTEGER,
    delay_minutes DOUBLE PRECISION,
    exception_flag INTEGER,
    transit_time_hours DOUBLE PRECISION
)
DISTSTYLE AUTO
SORTKEY AUTO
ENCODE AUTO;

CREATE TABLE IF NOT EXISTS {{staging_schema}}.agg_shipment_daily (
    p_date DATE NOT NULL,
    region_code VARCHAR(32) NOT NULL,
    carrier_id VARCHAR(128) NOT NULL,
    total_shipments BIGINT,
    delivered_shipments BIGINT,
    on_time_shipments BIGINT,
    delayed_shipments BIGINT,
    exception_shipments BIGINT,
    total_delay_minutes DOUBLE PRECISION,
    avg_delay_minutes DOUBLE PRECISION,
    avg_transit_time_hours DOUBLE PRECISION,
    total_shipping_cost_usd DOUBLE PRECISION,
    total_distance_miles DOUBLE PRECISION,
    on_time_delivery_rate DOUBLE PRECISION,
    exception_rate DOUBLE PRECISION,
    avg_cost_per_mile DOUBLE PRECISION
)
DISTSTYLE AUTO
SORTKEY AUTO
ENCODE AUTO;

CREATE TABLE IF NOT EXISTS {{staging_schema}}.kpi_delivery_daily (
    p_date DATE NOT NULL,
    region_code VARCHAR(32) NOT NULL,
    carrier_id VARCHAR(128) NOT NULL,
    on_time_delivery_rate DOUBLE PRECISION,
    avg_transit_hours DOUBLE PRECISION,
    late_delivery_rate DOUBLE PRECISION,
    first_attempt_success_rate DOUBLE PRECISION,
    exception_rate DOUBLE PRECISION,
    avg_cost_per_mile DOUBLE PRECISION,
    volume_by_carrier BIGINT,
    delivery_event_density DOUBLE PRECISION,
    total_shipments BIGINT,
    delivered_shipments BIGINT,
    on_time_shipments BIGINT,
    delayed_shipments BIGINT,
    exception_shipments BIGINT,
    total_delivery_events BIGINT
)
DISTSTYLE AUTO
SORTKEY AUTO
ENCODE AUTO;
