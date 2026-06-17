# Healthcare Data Pipeline

Batch data pipeline for CMS Medicare Part D Prescribers by Provider and Drug data.

Version 1 focuses on a reliable batch pipeline:

```text
CMS Data API
-> Python ingestion
-> MinIO Bronze CSV + manifest
-> Spark Silver Parquet
-> Spark Gold aggregate marts
-> PostgreSQL Gold marts
-> Superset dashboard
```

Kafka and Grafana are intentionally deferred. The source data is annual batch data, so version 1 prioritizes ingestion correctness, lineage, row-count reconciliation, disk-safe local processing, and a clean analytical model.

## Current Implementation

Implemented:

- Local Docker Compose services for PostgreSQL and MinIO.
- CMS ingestion package with pagination, retries, CSV writing, checksum, and manifest generation.
- PostgreSQL metadata tables for ingestion and data quality results.
- Spark Bronze to Silver transformation for CMS Part D prescriber detail data.
- Spark Silver to Gold aggregate mart exports.
- PostgreSQL Gold mart tables and CSV loader.
- Unit tests for manifest, ingestion pagination, Silver transforms, and Gold mart transforms.
- Local Superset service and dashboard views for Gold mart analysis.

Next:

- Build and export the Superset dashboard from the dashboard views.
- Add more years after validating local disk capacity.
- Add Airflow DAGs after the manual local path is stable.

## Local Setup

Create a virtual environment and install dependencies:

```bash
python -m venv .venv
.venv\Scripts\pip install -e ".[dev]"
```

Start local infrastructure:

```bash
docker compose up -d postgres minio
docker compose --profile processing up -d spark
docker compose build superset
docker compose up -d superset
```

Run tests:

```bash
.venv\Scripts\python.exe -m pytest -q
```

## Run Pipeline

### 1. Bronze Ingestion

Run a safe ingestion smoke test without MinIO upload:

```bash
python -m ingestion.ingest_cms_part_d --years 2022 --max-pages 1 --skip-upload
```

Run a full local Bronze ingestion after infrastructure is healthy:

```bash
python -m ingestion.ingest_cms_part_d --years 2022
```

The full run can download a large dataset. Use `--max-pages` during development.

### 2. Silver Parquet

Run Bronze to Silver for the full 2022 dataset:

```bash
docker compose exec spark /opt/spark/bin/spark-submit --driver-memory 4g /app/spark/jobs/bronze_to_silver.py --year 2022 --input-path /app/data/raw/part_d_2022.csv --output-path /app/data/silver/part_d_prescribers --skip-count
```

If the local raw CSV was deleted after upload to MinIO, restore it from MinIO or rerun ingestion before this step. Silver output is the detailed Parquet layer and should remain the source of truth for row-level analysis.

Inspect Silver output:

```bash
docker compose exec spark /opt/spark/bin/spark-submit --driver-memory 2g /app/spark/jobs/inspect_parquet.py --input-path /app/data/silver/part_d_prescribers --limit 5
```

### 3. Gold Mart Export

Create PostgreSQL Gold mart tables:

```bash
docker compose exec postgres psql -U pipeline -d healthcare -f /docker-entrypoint-initdb.d/003_create_gold_marts.sql
```

Export aggregate Gold marts from Silver:

```bash
docker compose exec spark /opt/spark/bin/spark-submit --driver-memory 4g /app/spark/jobs/silver_to_gold_marts.py --input-path /app/data/silver/part_d_prescribers --output-path /app/data/gold_mart_export --skip-count
```

Load Gold mart exports into PostgreSQL:

```bash
.venv\Scripts\python.exe gold_loader.py --export-dir data\gold_mart_export --load-set marts --truncate
```

Verify loaded row counts:

```bash
docker compose exec postgres psql -U pipeline -d healthcare -c "SELECT 'gold_state_year_spending' AS table_name, count(*) FROM gold_state_year_spending UNION ALL SELECT 'gold_drug_year_spending', count(*) FROM gold_drug_year_spending UNION ALL SELECT 'gold_specialty_state_spending', count(*) FROM gold_specialty_state_spending UNION ALL SELECT 'gold_prescriber_summary', count(*) FROM gold_prescriber_summary UNION ALL SELECT 'gold_anomaly_prescribers', count(*) FROM gold_anomaly_prescribers;"
```

Expected 2022 row counts from the current local run:

| Table | Rows |
| --- | ---: |
| `gold_state_year_spending` | 61 |
| `gold_drug_year_spending` | 3,136 |
| `gold_specialty_state_spending` | 4,813 |
| `gold_prescriber_summary` | 1,057,566 |
| `gold_anomaly_prescribers` | 50,226 |

### 4. Superset Dashboard

Create the dashboard views in PostgreSQL. This command runs `psql` inside the PostgreSQL container and executes the local file `sql/004_create_dashboard_views.sql`, which is mounted in the container as `/docker-entrypoint-initdb.d/004_create_dashboard_views.sql`.

```bash
docker compose exec postgres psql -U pipeline -d healthcare -f /docker-entrypoint-initdb.d/004_create_dashboard_views.sql
```

Start Superset if it is not already running:

```bash
docker compose build superset
docker compose up -d superset
```

Open Superset:

```text
http://localhost:8088
```

Login:

```text
admin / admin
```

Add a PostgreSQL database connection in Superset with these form values:

```text
Host: postgres
Port: 5432
Database name: healthcare
Username: pipeline
Password: pipeline
Display name: Healthcare PostgreSQL
```

From Windows or VS Code, PostgreSQL is available at `localhost:55432`. From Superset, use `postgres:5432` because Superset connects from inside Docker.

Then create datasets and charts from the `dashboard_*` views. See `dashboard/README.md` for the dashboard-specific setup.

## Local Storage Notes

Do not load the full detailed prescription fact table into local PostgreSQL unless the machine has enough free disk for both staged CSV files and PostgreSQL indexes. The recommended local path is:

```text
Silver Parquet keeps row-level detail.
PostgreSQL stores aggregate Gold marts for BI.
```

See `doc/run-process-notes.md` for the low-disk issue and the design change made during the first full run.

## Services

| Service | URL | Credentials |
| --- | --- | --- |
| MinIO API | `http://localhost:9000` | `minioadmin` / `minioadmin` |
| MinIO Console | `http://localhost:9001` | `minioadmin` / `minioadmin` |
| PostgreSQL | `localhost:55432` | `pipeline` / `pipeline` |
| Spark UI | `http://localhost:4040` | available only while a Spark application is running |
| Superset | `http://localhost:8088` | `admin` / `admin` |

## Dashboard

Superset is configured as a local Docker Compose service. See `dashboard/README.md` for the connection URI, dashboard views, and recommended charts.
