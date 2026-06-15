CREATE TABLE IF NOT EXISTS ingestion_log (
    ingestion_id BIGSERIAL PRIMARY KEY,
    run_id TEXT NOT NULL,
    dataset_name TEXT NOT NULL,
    dataset_id TEXT NOT NULL,
    source_url TEXT NOT NULL,
    year INTEGER NOT NULL,
    file_path TEXT NOT NULL,
    manifest_path TEXT NOT NULL,
    row_count BIGINT NOT NULL,
    expected_row_count BIGINT,
    checksum_sha256 TEXT,
    object_etag TEXT,
    status TEXT NOT NULL,
    started_at TIMESTAMP NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMP,
    error_message TEXT,
    CONSTRAINT ingestion_status_valid
        CHECK (status IN ('RUNNING', 'SUCCESS', 'FAILED')),
    CONSTRAINT ingestion_run_unique
        UNIQUE (run_id, dataset_name, year)
);

CREATE INDEX IF NOT EXISTS idx_ingestion_log_dataset_year
    ON ingestion_log (dataset_name, year, status);

CREATE TABLE IF NOT EXISTS data_quality_result (
    check_id BIGSERIAL PRIMARY KEY,
    run_id TEXT NOT NULL,
    dataset_name TEXT NOT NULL,
    layer TEXT NOT NULL,
    check_name TEXT NOT NULL,
    status TEXT NOT NULL,
    observed_value TEXT,
    expected_value TEXT,
    details TEXT,
    checked_at TIMESTAMP NOT NULL DEFAULT NOW(),
    CONSTRAINT dq_status_valid
        CHECK (status IN ('PASS', 'FAIL', 'WARN'))
);

