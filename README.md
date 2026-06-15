# Healthcare Data Pipeline

Batch data pipeline for CMS Medicare Part D Prescribers by Provider and Drug data.

Version 1 focuses on a reliable batch pipeline:

```text
CMS Data API
-> Python ingestion
-> MinIO Bronze CSV + manifest
-> Spark Silver Parquet
-> PostgreSQL Gold star schema
-> Superset dashboard
```

Kafka and Grafana are intentionally deferred. The source data is annual batch data, so version 1 prioritizes ingestion correctness, lineage, row-count reconciliation, and a clean analytical model.

## Current Implementation

Implemented:

- Local Docker Compose services for PostgreSQL and MinIO.
- CMS ingestion package with pagination, retries, CSV writing, checksum, and manifest generation.
- PostgreSQL metadata tables for ingestion and data quality results.
- Unit tests for manifest and ingestion pagination behavior.

Next:

- Wire ingestion into MinIO and PostgreSQL with a real smoke run.
- Add Bronze to Silver Spark job.
- Add Gold dimensional model tables.
- Add Airflow DAGs after the local ingestion path is stable.

## Local Setup

Create a virtual environment and install dependencies:

```bash
python -m venv .venv
.venv\Scripts\pip install -e ".[dev]"
```

Start local infrastructure:

```bash
docker compose up -d
```

Run tests:

```bash
pytest -q
```

Run a safe ingestion smoke test without MinIO upload:

```bash
python -m ingestion.ingest_cms_part_d --years 2022 --max-pages 1 --skip-upload
```

Run a full local Bronze ingestion after infrastructure is healthy:

```bash
python -m ingestion.ingest_cms_part_d --years 2022
```

The full run can download a large dataset. Use `--max-pages` during development.

## Services

| Service | URL | Credentials |
| --- | --- | --- |
| MinIO API | `http://localhost:9000` | `minioadmin` / `minioadmin` |
| MinIO Console | `http://localhost:9001` | `minioadmin` / `minioadmin` |
| PostgreSQL | `localhost:55432` | `pipeline` / `pipeline` |
