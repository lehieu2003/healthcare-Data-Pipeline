# Healthcare Data Pipeline

## Medicare Part D — End-to-End Project Plan

### Junior Data Engineer Portfolio Project

## Tech Stack

| Layer         | Technology                                                 |
| ------------- | ---------------------------------------------------------- |
| Ingestion     | Python, CMS REST API                                       |
| Processing    | Apache Spark (PySpark)                                     |
| Orchestration | Apache Airflow                                             |
| Storage       | MinIO (Data Lake), PostgreSQL (Data Warehouse)             |
| Visualization | Apache Superset                                            |
| Monitoring    | Airflow UI, PostgreSQL pipeline metadata                   |
| Container     | Docker Compose                                             |
| Dataset       | Medicare Part D Prescribers by Provider & Drug (2020–2022) |

Kafka and Grafana are intentionally not part of version 1. The dataset is annual batch data, so the first implementation should focus on a reliable batch pipeline. Kafka can be added later only if the project introduces streaming or event-driven use cases.

---

# Phase 0 — Understanding the Dataset

## 0.1 Data Source

- Dataset: Medicare Part D Prescribers — by Provider and Drug
- Provider: Centers for Medicare & Medicaid Services (CMS)
- Source: data.cms.gov
- Update frequency: Annual
- Coverage: 2020–2022

## 0.2 Dataset Meaning

Each row answers:

> Which doctor prescribed which drug, how many prescriptions, for how many patients, and at what total cost?

## 0.3 Important Columns

| Column                | Type    | Description                     |
| --------------------- | ------- | ------------------------------- |
| Prscrbr_NPI           | STRING  | Doctor identifier               |
| Prscrbr_Last_Org_Name | STRING  | Doctor last name / organization |
| Prscrbr_First_Name    | STRING  | Doctor first name               |
| Prscrbr_City          | STRING  | Practice city                   |
| Prscrbr_State_Abrvtn  | STRING  | State abbreviation              |
| Prscrbr_State_FIPS    | STRING  | State FIPS code                 |
| Prscrbr_Type          | STRING  | Medical specialty               |
| Brnd_Name             | STRING  | Drug brand name                 |
| Gnrc_Name             | STRING  | Generic drug name               |
| Tot_Clms              | INTEGER | Total prescriptions             |
| Tot_30day_Fills       | FLOAT   | 30-day equivalent fills         |
| Tot_Drug_Cst          | FLOAT   | Total drug cost                 |
| Tot_Day_Suply         | INTEGER | Total days supplied             |
| Tot_Benes             | INTEGER | Unique beneficiaries            |
| GE65_Tot_Clms         | INTEGER | Claims for patients aged 65+    |
| GE65_Tot_Drug_Cst     | FLOAT   | Drug cost for patients aged 65+ |
| GE65_Sprsn_Flag       | STRING  | Suppression flag                |

> Note: CMS suppresses values when Tot_Clms < 11 for privacy reasons.

---

## 0.4 Business Questions

### Main Question

> Which doctors exhibit abnormal prescribing behavior compared to peers within the same specialty and state?

### Supporting Questions

- Which states have the highest Medicare drug costs?
- Which drugs account for the largest share of spending?
- How has brand vs generic prescribing changed over time?
- What percentage of spending comes from patients aged 65+?

### Target Users

#### CMS / Healthcare Regulators

Detect Medicare fraud and abuse.

#### Insurance Companies

Optimize pharmaceutical spending.

#### Hospitals

Benchmark physician prescribing behavior.

#### Researchers

Analyze healthcare policy and drug spending trends.

---

# Phase 1 — Architecture & Environment

## 1.1 Medallion Architecture

| Layer  | Name           | Technology         | Description           |
| ------ | -------------- | ------------------ | --------------------- |
| Source | Source         | CMS API / CSV      | Raw source data       |
| Bronze | Raw Zone       | MinIO              | Raw files             |
| Silver | Cleaned Zone   | MinIO + Spark      | Cleaned datasets      |
| Gold   | Analytics Zone | PostgreSQL         | Star schema warehouse |
| Serve  | Visualization  | Superset           | Dashboards            |

---

## 1.2 Docker Compose Stack

| Service    | Image                     | Port        |
| ---------- | ------------------------- | ----------- |
| MinIO      | minio/minio               | 9000 / 9001 |
| PostgreSQL | postgres:15               | 5432        |
| Spark      | bitnami/spark             | 8080        |
| Airflow    | apache/airflow:2.8        | 8081        |
| Superset   | apache/superset           | 8088        |

---

## 1.3 Project Structure

```text
healthcare-pipeline/
├── dags/
├── spark/jobs/
├── ingestion/
├── sql/
├── dashboard/
├── tests/
├── docker-compose.yml
└── README.md
```

---

# Phase 2 — Data Ingestion

## 2.1 CMS Data Collection

### Option 1

Manual CSV download.

### Option 2

CMS REST API

```
/data-api/v1/dataset/{id}/data
```

Supports:

- limit
- offset
- pagination

Project uses CMS REST API.

---

## 2.2 Ingestion Workflow

1. Call CMS API by year.
2. Download data page-by-page.
3. Save temporary CSV locally.
4. Upload CSV to MinIO.
5. Write metadata to PostgreSQL ingestion_log.
6. Verify file existence before next stage.

---

## 2.3 Bronze Layer Structure

```text
cms-raw/
├── year=2020/part_d_2020.csv
├── year=2021/part_d_2021.csv
└── year=2022/part_d_2022.csv

cms-silver/
└── year=2020/

cms-gold/
└── fact_prescriptions/
```

---

# Phase 3 — Data Processing

## 3.1 Bronze → Silver

### Cleaning Rules

| Problem                 | Solution                   |
| ----------------------- | -------------------------- |
| Inconsistent drug names | UPPER() normalization      |
| Suppressed values       | Keep NULL + suppression flag |
| GE65 suppression values | Map to standard categories |
| Duplicate rows          | Deduplicate                |
| Negative drug cost      | Remove or flag             |
| Missing state           | Set UNKNOWN                |
| Incorrect data types    | Cast properly              |

Suppressed CMS values must not be converted to `0`. A zero means no claims or no cost. A suppressed value means CMS intentionally hid the number for privacy reasons. Silver should preserve that distinction with nullable numeric fields and boolean flags such as `is_tot_claims_suppressed`.

---

## 3.2 Silver → Gold

### Dimension Tables

#### dim_prescriber

```text
prescriber_id
npi
full_name
city
state
specialty
specialty_src
```

#### dim_drug

```text
drug_id
brand_name
generic_name
is_generic
```

#### dim_date

```text
date_id
year
quarter
```

#### dim_state

```text
state_id
state_abbr
state_name
fips_code
region
```

### Fact Table

#### fact_prescriptions

Fact grain:

```text
one row per year + prescriber + drug + state
```

```text
fact_id
prescriber_id
drug_id
date_id
state_id

tot_claims
tot_30day_fills
tot_drug_cost
tot_day_supply
tot_beneficiaries

ge65_claims
ge65_drug_cost

is_tot_claims_suppressed
is_ge65_suppressed
cost_per_claim
claims_per_beneficiary
is_anomaly
```

---

# Phase 4 — Airflow Orchestration

## DAGs

### dag_ingest_cms

```text
download_csv
→ upload_to_minio
→ log_metadata
→ verify_file
```

### dag_bronze_to_silver

```text
spark_clean
→ validate_row_count
→ move_to_silver
```

### dag_silver_to_gold

```text
spark_transform
→ load_dim_tables
→ load_fact_table
→ run_dq_checks
```

### dag_data_quality

```text
check_null_npi
→ check_negative_cost
→ check_row_count
→ alert_if_fail
```

---

# Phase 5 — Data Quality & Testing

## Quality Checks

| Check                 | Expected Result     |
| --------------------- | ------------------- |
| Bronze Row Count      | 100% of API stats   |
| Silver Row Count      | Bronze - rejected rows |
| Gold Reconciliation   | Matches Silver fact grain |
| Null NPI              | 0 rows              |
| Negative Cost         | 0 rows              |
| Duplicate Records     | 0 duplicates        |
| State Validation      | Pass                |
| Referential Integrity | 0 orphan rows       |
| Year Completeness     | 2020–2022 available |

---

## Unit Testing

Using:

- pytest
- pyspark

Test cases:

- Null handling
- Deduplication
- Star schema transformation
- Dimension creation

---

# Phase 6 — Visualization

## Dashboard Overview

### Overview

- Total cost by year
- Total claims
- Number of prescribers

### Geography

- State spending map
- Top 10 states
- Yearly trends

### Drug Analysis

- Top 20 costly drugs
- Brand vs Generic ratio
- Average cost per claim

### Prescriber Anomaly

- Outlier doctors
- Filters by state and specialty

### Data Pipeline

- Processed rows
- DAG runtime
- Data quality metrics

---

## Anomaly Detection Logic

For each:

```text
(specialty, state, year, generic_name)
```

Compute:

```text
cost_per_claim = Tot_Drug_Cst / Tot_Clms
claims_per_beneficiary = Tot_Clms / Tot_Benes
```

Then compare each prescriber against peers in the same group using percentile, IQR, or z-score.

Version 1 recommended rule:

```text
cost_per_claim > peer_p95
AND Tot_Clms >= 30
```

Set:

```text
is_anomaly = true
```

---

# Phase 7 — README & Portfolio

## README Structure

1. Project Overview
2. Architecture Diagram
3. Tech Stack
4. Dataset
5. Data Model
6. How To Run
7. Dashboard Screenshots
8. Key Findings

### Example Findings

- Top spending states
- Most expensive drugs
- Number of anomalous doctors
- Generic prescribing growth

---

# 5-Week Roadmap

| Week   | Goal                          | Deliverable            |
| ------ | ----------------------------- | ---------------------- |
| Week 1 | Environment + Ingestion       | CMS API + Bronze Layer |
| Week 2 | Spark Cleaning                | Silver Layer           |
| Week 3 | Star Schema + Airflow         | Gold Layer             |
| Week 4 | Dashboard + Anomaly Detection | Superset Dashboard     |
| Week 5 | README + Polish               | GitHub Portfolio Ready |

> Recruiters often read the README before reviewing code.
