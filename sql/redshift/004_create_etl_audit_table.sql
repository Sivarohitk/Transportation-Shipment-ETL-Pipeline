-- One row per attempted Redshift table load.

CREATE TABLE IF NOT EXISTS {{audit_schema}}.etl_load_audit (
    load_id VARCHAR(128) NOT NULL,
    batch_id VARCHAR(128),
    table_name VARCHAR(256) NOT NULL,
    source_s3_path VARCHAR(2048) NOT NULL,
    statement_id VARCHAR(128),
    status VARCHAR(32) NOT NULL,
    rows_loaded BIGINT,
    started_at TIMESTAMP NOT NULL,
    finished_at TIMESTAMP,
    duration_seconds DOUBLE PRECISION,
    error_message VARCHAR(65535),
    created_at TIMESTAMP NOT NULL DEFAULT GETDATE(),
    PRIMARY KEY (load_id)
)
DISTSTYLE AUTO
SORTKEY (started_at, table_name);
