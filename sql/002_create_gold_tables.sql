CREATE TABLE IF NOT EXISTS dim_prescriber (
    prescriber_id TEXT PRIMARY KEY,
    npi TEXT NOT NULL,
    full_name TEXT,
    city TEXT,
    state TEXT,
    specialty TEXT,
    UNIQUE (npi, city, state, specialty)
);

CREATE TABLE IF NOT EXISTS dim_drug (
    drug_id TEXT PRIMARY KEY,
    brand_name TEXT,
    generic_name TEXT,
    UNIQUE (brand_name, generic_name)
);

CREATE TABLE IF NOT EXISTS dim_state (
    state_id TEXT PRIMARY KEY,
    state_abbr TEXT NOT NULL UNIQUE,
    state_fips TEXT
);

CREATE TABLE IF NOT EXISTS dim_year (
    year_id INTEGER PRIMARY KEY,
    year INTEGER NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS fact_prescriptions (
    fact_id TEXT PRIMARY KEY,
    prescriber_id TEXT NOT NULL REFERENCES dim_prescriber (prescriber_id),
    drug_id TEXT NOT NULL REFERENCES dim_drug (drug_id),
    state_id TEXT NOT NULL REFERENCES dim_state (state_id),
    year_id INTEGER NOT NULL REFERENCES dim_year (year_id),
    tot_claims BIGINT,
    tot_30day_fills DOUBLE PRECISION,
    tot_drug_cost DOUBLE PRECISION,
    tot_day_supply BIGINT,
    tot_beneficiaries BIGINT,
    ge65_claims BIGINT,
    ge65_drug_cost DOUBLE PRECISION,
    cost_per_claim DOUBLE PRECISION,
    claims_per_beneficiary DOUBLE PRECISION,
    is_tot_claims_suppressed BOOLEAN,
    is_ge65_suppressed BOOLEAN
);

CREATE INDEX IF NOT EXISTS idx_fact_prescriptions_year_state
    ON fact_prescriptions (year_id, state_id);

CREATE INDEX IF NOT EXISTS idx_fact_prescriptions_prescriber
    ON fact_prescriptions (prescriber_id);

CREATE INDEX IF NOT EXISTS idx_fact_prescriptions_drug
    ON fact_prescriptions (drug_id);

