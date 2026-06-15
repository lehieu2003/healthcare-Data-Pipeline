CMS Medicare Part D Data
│
▼
Python Ingestion

- Download yearly CSV files
- Handle pagination / chunk download
- Validate checksum / row count
- Write Bronze manifest
  │
  ▼
  MinIO Bronze Layer
- Store raw CSV files
- Store manifest.json per year
- Keep original data unchanged
- Partition by year
  │
  ▼
  Airflow DAG: bronze_to_silver
  │
  ▼
  Apache Spark Cleaning
- Read raw CSV from MinIO
- Cast data types
- Handle null / suppressed values
- Preserve suppressed CMS values as NULL + flags
- Normalize drug names
- Remove duplicates
- Reconcile row count against Bronze minus rejected rows
  │
  ▼
  MinIO Silver Layer
- Store cleaned Parquet files
- Partition by year / state
  │
  ▼
  Airflow DAG: silver_to_gold
  │
  ▼
  Apache Spark Transformation
- Build dimension tables
- Build fact table
- Calculate business metrics
- Detect anomalies
- Enforce fact grain: year + prescriber + drug + state
  │
  ▼
  PostgreSQL Gold Layer
- dim_prescriber
- dim_drug
- dim_state
- dim_date
- fact_prescriptions
  │
  ▼
  Data Quality Checks
- Null NPI
- Negative cost
- Duplicate records
- FK integrity
- Year completeness
  │
  ▼
Superset Dashboard
- Spending overview
- Geography analysis
- Drug analysis
- Prescriber anomaly

Simple version:
CMS API / CSV
→ Python Ingestion
→ MinIO Bronze
→ Spark Clean
→ MinIO Silver (Parquet)
→ Spark Transform
→ PostgreSQL Gold (Star Schema)
→ Data Quality Checks
→ Superset Dashboard

Operational monitoring for version 1 is handled by Airflow task status and PostgreSQL pipeline metadata. Grafana can be added later if dedicated monitoring dashboards are needed.
