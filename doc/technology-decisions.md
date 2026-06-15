# Technology Decisions

## Healthcare Data Pipeline

This document explains the rationale behind each major technology choice in the project architecture.

---

# Design Principles

The technology stack was selected based on the following goals:

- Handle approximately 12GB of healthcare data efficiently
- Simulate a production-grade data engineering environment
- Follow modern data lake and warehouse architecture patterns
- Remain deployable on a local machine using Docker Compose
- Demonstrate skills commonly required in Data Engineer roles
- Keep version 1 focused on a reliable batch pipeline before adding optional streaming components

---

# Python

## Why Python?

Python was selected as the primary programming language because it is the industry standard for data engineering workflows.

Advantages:

- Strong ecosystem for data processing
- Native integration with Spark (PySpark)
- Airflow DAG development
- API ingestion support
- Large community support

Alternatives considered:

- Java
- Scala
- Go

Why not Scala?

Although Spark is written in Scala, Python provides faster development speed and is more commonly used in entry-level and mid-level data engineering teams.

---

# CMS REST API

## Why API-Based Ingestion?

The CMS dataset can be downloaded manually as CSV files.

However, production systems rarely depend on manual downloads.

Using APIs provides:

- Automation
- Repeatability
- Scheduling support
- Better data lineage

Benefits:

- Easily integrated with Airflow
- Supports pagination
- Enables future incremental ingestion

---

# MinIO

## Why MinIO?

MinIO serves as the project's Data Lake.

Benefits:

- S3-compatible storage
- Open-source
- Lightweight
- Docker-friendly
- Easy local development

The project follows a Medallion Architecture:

```text
Bronze → Silver → Gold
```

MinIO stores:

```text
Bronze:
Raw CSV

Silver:
Cleaned Parquet
```

Why not AWS S3?

S3 is ideal in production but introduces cloud costs and account management requirements.

MinIO provides the same concepts while remaining completely local.

---

# Apache Spark

## Why Spark?

The Medicare Part D dataset is relatively large:

```text
2020 ≈ 4GB
2021 ≈ 4GB
2022 ≈ 4GB

Total ≈ 12GB
```

Processing this volume with pandas becomes increasingly memory-intensive.

Spark was selected because it:

- Processes data in partitions
- Supports parallel execution
- Handles larger-than-memory workloads
- Integrates naturally with Parquet

---

## Why Not Pandas?

Pandas is excellent for analysis and small ETL jobs.

However:

- Single-machine processing
- Memory-bound execution
- Limited scalability

For datasets approaching tens of gigabytes, Spark provides a more realistic production solution.

---

# Apache Airflow

## Why Airflow?

The project contains multiple dependent stages:

```text
Ingestion
→ Cleaning
→ Transformation
→ Data Quality
→ Dashboard Consumption
```

Airflow provides:

- Workflow orchestration
- Scheduling
- Dependency management
- Retry handling
- Monitoring

Benefits for portfolio projects:

- Demonstrates production pipeline thinking
- Commonly used in industry

---

# Apache Kafka

## Why Not Kafka in Version 1?

Kafka is intentionally excluded from the initial implementation.

Current workload:

```text
Annual batch processing
```

Therefore Kafka is not required for the initial implementation.

Adding Kafka to a purely annual batch pipeline would increase operational complexity without improving the core data product.

Potential future use cases:

- Real-time prescription feeds
- Streaming anomaly detection
- Event-driven pipelines

Decision:

```text
Version 1:
No Kafka dependency

Future Version:
Kafka-enabled streaming architecture
```

---

# Apache Parquet

## Why Parquet?

CSV is useful for ingestion but inefficient for analytics.

Parquet provides:

- Columnar storage
- Compression
- Predicate pushdown
- Faster Spark execution

Workflow:

```text
CSV
→ Spark Cleaning
→ Parquet
```

This follows common Data Lake best practices.

---

# PostgreSQL

## Why PostgreSQL?

PostgreSQL acts as the analytical warehouse layer.

Benefits:

- Mature relational database
- Strong SQL support
- Easy integration with Superset
- Excellent for dimensional modeling

Used for:

```text
Dimension Tables
Fact Tables
Data Quality Metadata
Pipeline Logs
```

---

## Why Not Data Warehouse Technologies?

Alternatives:

- Snowflake
- BigQuery
- Redshift

Reason:

This project is designed to run locally without cloud infrastructure.

PostgreSQL provides a practical warehouse implementation while preserving warehouse concepts.

---

# Star Schema

## Why Star Schema?

Analytical workloads differ from transactional systems.

Star Schema provides:

- Simpler reporting queries
- Faster aggregations
- Easier dashboard development

Dimensions:

```text
dim_prescriber
dim_drug
dim_state
dim_date
```

Fact:

```text
fact_prescriptions
```

This design mirrors real-world data warehouse architecture.

---

# Apache Superset

## Why Superset?

The project requires business-facing dashboards.

Superset offers:

- SQL-based exploration
- Dashboard creation
- Open-source
- PostgreSQL integration

Dashboard categories:

- Cost Analysis
- Geographic Analysis
- Drug Analysis
- Prescriber Anomalies

---

# Grafana

## Why Not Grafana in Version 1?

Grafana is useful for operational monitoring, but it is not required for the first implementation.

Version 1 can expose enough operational visibility through:

- Airflow task status and retries
- PostgreSQL pipeline metadata tables
- Data quality result tables

Grafana can be added later if the project needs dedicated time-series monitoring for DAG runtime, row counts, or data quality metrics.

This keeps the initial Docker Compose stack smaller while preserving a clear future monitoring path.

---

# Docker Compose

## Why Docker?

The project contains multiple services:

```text
Spark
Airflow
MinIO
PostgreSQL
Superset
```

Docker provides:

- Environment consistency
- Reproducibility
- Simplified onboarding
- Easier deployment

One command startup:

```bash
docker compose up -d
```

---

# Key Architecture Decisions

| Decision                             | Rationale                                   |
| ------------------------------------ | ------------------------------------------- |
| Spark instead of Pandas              | Better scalability for ~12GB data           |
| MinIO instead of S3                  | Local development without cloud cost        |
| PostgreSQL instead of Snowflake      | Fully local architecture                    |
| Airflow instead of Cron              | Workflow orchestration and observability    |
| Parquet instead of CSV for analytics | Better performance and compression          |
| Star Schema instead of Flat Tables   | Faster analytical queries                   |
| Superset for v1 dashboards           | Business analytics with minimal local stack |
| No Kafka in v1                       | Annual batch data does not require streaming |

---

# Future Improvements

## Storage

- Apache Iceberg
- Delta Lake

## Data Quality

- Great Expectations

## Transformations

- dbt

## Streaming

- Apache Kafka
- Spark Structured Streaming

## Cloud Migration

- AWS S3
- AWS Glue
- Amazon Redshift

---

# Interview Talking Points

### Why Spark?

Because the project processes approximately 12GB of healthcare data. Spark provides partitioned processing and scales better than pandas for large analytical workloads.

### Why MinIO?

To simulate an S3-compatible Data Lake environment locally without introducing cloud costs.

### Why PostgreSQL?

To implement a dimensional warehouse model while keeping the entire project runnable through Docker Compose.

### Why Airflow?

To orchestrate ingestion, transformation, and quality validation workflows using production-style DAGs.
