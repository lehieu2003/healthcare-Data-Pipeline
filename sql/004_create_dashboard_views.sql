CREATE OR REPLACE VIEW dashboard_overview_kpis AS
SELECT
    year,
    SUM(total_claims) AS total_claims,
    SUM(total_drug_cost) AS total_drug_cost,
    SUM(total_beneficiaries) AS total_beneficiaries,
    SUM(prescriber_count) AS state_prescriber_count_sum,
    SUM(drug_count) AS state_drug_count_sum,
    CASE
        WHEN SUM(total_claims) = 0 THEN NULL
        ELSE SUM(total_drug_cost) / SUM(total_claims)
    END AS avg_cost_per_claim
FROM gold_state_year_spending
GROUP BY year;

CREATE OR REPLACE VIEW dashboard_state_spending AS
SELECT
    year,
    state,
    total_claims,
    total_drug_cost,
    total_beneficiaries,
    prescriber_count,
    drug_count,
    avg_cost_per_claim
FROM gold_state_year_spending;

CREATE OR REPLACE VIEW dashboard_top_drugs AS
SELECT
    year,
    COALESCE(NULLIF(brand_name, ''), 'UNKNOWN') AS brand_name,
    COALESCE(NULLIF(generic_name, ''), 'UNKNOWN') AS generic_name,
    total_claims,
    total_drug_cost,
    prescriber_count,
    state_count,
    avg_cost_per_claim
FROM gold_drug_year_spending;

CREATE OR REPLACE VIEW dashboard_specialty_state_spending AS
SELECT
    year,
    state,
    COALESCE(NULLIF(specialty, ''), 'UNKNOWN') AS specialty,
    total_claims,
    total_drug_cost,
    prescriber_count,
    drug_count,
    avg_cost_per_claim
FROM gold_specialty_state_spending;

CREATE OR REPLACE VIEW dashboard_prescriber_leaders AS
SELECT
    year,
    npi,
    COALESCE(NULLIF(full_name, ''), 'UNKNOWN') AS full_name,
    COALESCE(NULLIF(city, ''), 'UNKNOWN') AS city,
    state,
    COALESCE(NULLIF(specialty, ''), 'UNKNOWN') AS specialty,
    total_claims,
    total_drug_cost,
    drug_count,
    avg_cost_per_claim
FROM gold_prescriber_summary;

CREATE OR REPLACE VIEW dashboard_anomaly_prescribers AS
SELECT
    year,
    npi,
    COALESCE(NULLIF(full_name, ''), 'UNKNOWN') AS full_name,
    COALESCE(NULLIF(city, ''), 'UNKNOWN') AS city,
    state,
    COALESCE(NULLIF(specialty, ''), 'UNKNOWN') AS specialty,
    total_claims,
    total_drug_cost,
    avg_cost_per_claim,
    peer_p95_cost_per_claim,
    anomaly_reason
FROM gold_anomaly_prescribers;

CREATE OR REPLACE VIEW dashboard_anomaly_summary AS
SELECT
    year,
    state,
    COALESCE(NULLIF(specialty, ''), 'UNKNOWN') AS specialty,
    COUNT(*) AS anomaly_prescriber_count,
    SUM(total_claims) AS anomaly_total_claims,
    SUM(total_drug_cost) AS anomaly_total_drug_cost,
    AVG(avg_cost_per_claim) AS anomaly_avg_cost_per_claim,
    AVG(peer_p95_cost_per_claim) AS peer_p95_cost_per_claim
FROM gold_anomaly_prescribers
GROUP BY year, state, COALESCE(NULLIF(specialty, ''), 'UNKNOWN');
