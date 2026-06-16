CREATE TABLE IF NOT EXISTS gold_state_year_spending (
    mart_id TEXT PRIMARY KEY,
    year INTEGER NOT NULL,
    state TEXT NOT NULL,
    total_claims BIGINT,
    total_drug_cost DOUBLE PRECISION,
    total_beneficiaries BIGINT,
    prescriber_count BIGINT,
    drug_count BIGINT,
    avg_cost_per_claim DOUBLE PRECISION
);

CREATE TABLE IF NOT EXISTS gold_drug_year_spending (
    mart_id TEXT PRIMARY KEY,
    year INTEGER NOT NULL,
    brand_name TEXT,
    generic_name TEXT,
    total_claims BIGINT,
    total_drug_cost DOUBLE PRECISION,
    prescriber_count BIGINT,
    state_count BIGINT,
    avg_cost_per_claim DOUBLE PRECISION
);

CREATE TABLE IF NOT EXISTS gold_specialty_state_spending (
    mart_id TEXT PRIMARY KEY,
    year INTEGER NOT NULL,
    state TEXT NOT NULL,
    specialty TEXT,
    total_claims BIGINT,
    total_drug_cost DOUBLE PRECISION,
    prescriber_count BIGINT,
    drug_count BIGINT,
    avg_cost_per_claim DOUBLE PRECISION
);

CREATE TABLE IF NOT EXISTS gold_prescriber_summary (
    mart_id TEXT PRIMARY KEY,
    year INTEGER NOT NULL,
    npi TEXT NOT NULL,
    full_name TEXT,
    city TEXT,
    state TEXT,
    specialty TEXT,
    total_claims BIGINT,
    total_drug_cost DOUBLE PRECISION,
    drug_count BIGINT,
    avg_cost_per_claim DOUBLE PRECISION
);

CREATE TABLE IF NOT EXISTS gold_anomaly_prescribers (
    mart_id TEXT PRIMARY KEY,
    year INTEGER NOT NULL,
    npi TEXT NOT NULL,
    full_name TEXT,
    city TEXT,
    state TEXT,
    specialty TEXT,
    total_claims BIGINT,
    total_drug_cost DOUBLE PRECISION,
    avg_cost_per_claim DOUBLE PRECISION,
    peer_p95_cost_per_claim DOUBLE PRECISION,
    anomaly_reason TEXT
);

CREATE INDEX IF NOT EXISTS idx_gold_state_year_spending_year_state
    ON gold_state_year_spending (year, state);

CREATE INDEX IF NOT EXISTS idx_gold_drug_year_spending_year_cost
    ON gold_drug_year_spending (year, total_drug_cost DESC);

CREATE INDEX IF NOT EXISTS idx_gold_specialty_state_spending_year_state
    ON gold_specialty_state_spending (year, state, specialty);

CREATE INDEX IF NOT EXISTS idx_gold_prescriber_summary_year_cost
    ON gold_prescriber_summary (year, total_drug_cost DESC);

CREATE INDEX IF NOT EXISTS idx_gold_anomaly_prescribers_year_state
    ON gold_anomaly_prescribers (year, state, specialty);

