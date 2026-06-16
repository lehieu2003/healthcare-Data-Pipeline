# CMS Medicare Part D Data Pipeline

## Architecture Overview

```text
CMS Medicare Part D Data
│
▼
Python Ingestion
│
├─ Download yearly CSV files
├─ Handle pagination / chunk download
├─ Validate checksum and row count
└─ Write Bronze manifest
│
▼
MinIO Bronze Layer
│
├─ Store raw CSV files
├─ Store manifest.json per year
├─ Keep original data unchanged
└─ Partition by year
│
▼
Airflow DAG: bronze_to_silver
│
▼
Apache Spark Cleaning
│
├─ Read raw CSV from MinIO
├─ Cast data types
├─ Handle null and suppressed values
├─ Preserve CMS suppressed values as NULL + flags
├─ Normalize drug names
├─ Remove duplicates
└─ Reconcile row count against Bronze minus rejected rows
│
▼
MinIO Silver Layer
│
├─ Store cleaned Parquet files
└─ Partition by year and state
│
▼
Airflow DAG: silver_to_gold
│
▼
Apache Spark Transformation
│
├─ Read detailed Silver Parquet
├─ Calculate business metrics
├─ Build aggregate mart exports
└─ Detect prescriber anomalies
│
▼
PostgreSQL Gold Layer
│
├─ gold_state_year_spending
├─ gold_drug_year_spending
├─ gold_specialty_state_spending
├─ gold_prescriber_summary
└─ gold_anomaly_prescribers
│
▼
Data Quality Checks
│
├─ Null NPI
├─ Negative cost
├─ Duplicate records
├─ Aggregate reconciliation
└─ Year completeness
│
▼
Apache Superset Dashboard
│
├─ Spending Overview
├─ Geography Analysis
├─ Drug Analysis
└─ Prescriber Anomaly Detection
```

---

# Simplified Data Flow

```text
CMS API / CSV
    │
    ▼
Python Ingestion
    │
    ▼
MinIO Bronze
    │
    ▼
Spark Clean
    │
    ▼
MinIO Silver (Parquet)
    │
    ▼
Spark Transform
    │
    ▼
PostgreSQL Gold (Aggregate Marts)
    │
    ▼
Data Quality Checks
    │
    ▼
Superset Dashboard
```

---

# Layer Responsibilities

## Bronze Layer

### Purpose

Store source data exactly as received from CMS.

### Storage

- Raw CSV files
- Manifest metadata

### Characteristics

- Immutable
- Auditable
- Reproducible

### Partitioning

```text
bronze/
└── medicare_part_d/
    ├── year=2022/
    │   ├── data.csv
    │   └── manifest.json
    ├── year=2023/
    └── year=2024/
```

---

## Silver Layer

### Purpose

Create cleaned and standardized datasets.

### Transformations

- Data type casting
- Null handling
- Suppressed value handling
- Drug name normalization
- Duplicate removal
- Validation checks

### Storage Format

- Parquet
- Partitioned by year and state

### Example

```text
silver/
└── prescriptions/
    ├── year=2022/
    │   ├── state=CA/
    │   ├── state=TX/
    │   └── state=NY/
```

---

## Gold Layer

### Purpose

Provide analytics-ready aggregate datasets for BI dashboards while keeping row-level detail in Silver Parquet.

### Mart Tables

- gold_state_year_spending
- gold_drug_year_spending
- gold_specialty_state_spending
- gold_prescriber_summary
- gold_anomaly_prescribers

### Grain

The Gold layer intentionally stores aggregates instead of the full prescription fact table.

| Mart | Grain |
| --- | --- |
| `gold_state_year_spending` | year + state |
| `gold_drug_year_spending` | year + drug |
| `gold_specialty_state_spending` | year + state + specialty |
| `gold_prescriber_summary` | year + prescriber |
| `gold_anomaly_prescribers` | anomalous year + prescriber |

### Detail Retention

Full row-level prescription detail remains in Silver Parquet. Spark should be used for heavy detailed scans and reprocessing. PostgreSQL is used for BI-friendly Gold marts.

---

# Data Quality Rules

| Check             | Description                        |
| ----------------- | ---------------------------------- |
| Null NPI          | Prescriber NPI must exist          |
| Negative Cost     | Drug cost cannot be negative       |
| Duplicate Records | No duplicate prescription rows     |
| Aggregate Checks  | Gold marts must reconcile to Silver aggregates |
| Year Completeness | All expected years loaded          |

---

# Orchestration

## Airflow DAGs

### bronze_to_silver

```text
Extract Bronze
    ↓
Spark Cleaning
    ↓
Write Silver
```

### silver_to_gold

```text
Read Silver
    ↓
Build Aggregate Marts
    ↓
Export Gold Mart CSV
    ↓
Run DQ Checks
    ↓
Load PostgreSQL
```

---

# Analytics Layer

## Apache Superset Dashboards

### Spending Overview

- Total prescription spending
- Spending trends by year

### Geography Analysis

- State-level comparisons
- Regional spending hotspots

### Drug Analysis

- Top prescribed drugs
- Cost trends by drug

### Prescriber Anomaly Detection

- Outlier prescribers
- Unusual spending patterns

---

# Monitoring

Version 1 monitoring relies on:

- Airflow DAG status
- Airflow task logs
- PostgreSQL pipeline metadata tables

Future enhancements may include:

- Grafana dashboards
- Prometheus metrics
- Data quality alerting
- Slack notifications
