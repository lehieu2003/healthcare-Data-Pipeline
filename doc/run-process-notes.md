# Run Process Notes

This file records operational findings from the first full local run of the CMS Medicare Part D 2022 pipeline.

## Reader

This note is for the engineer running the pipeline locally and deciding what should live in Parquet versus PostgreSQL.

## Full 2022 Run Status

The 2022 dataset was processed end to end through the disk-safe path:

```text
Bronze raw/API
-> Silver Parquet detail
-> Gold aggregate mart CSV exports
-> PostgreSQL Gold mart tables
```

Final verified PostgreSQL mart counts:

| Table | Rows |
| --- | ---: |
| `gold_state_year_spending` | 61 |
| `gold_drug_year_spending` | 3,136 |
| `gold_specialty_state_spending` | 4,813 |
| `gold_prescriber_summary` | 1,057,566 |
| `gold_anomaly_prescribers` | 50,226 |

## Bug: Local Disk Exhaustion During Detailed Gold Load

During the first Gold load attempt, the detailed Gold schema was used. The loader successfully inserted dimensions, including about 1,057,566 prescribers, then PostgreSQL terminated the connection while the next large load was running.

Observed error:

```text
psycopg.OperationalError: consuming input failed: server closed the connection unexpectedly
```

The root cause was local disk pressure. Loading a detailed prescription fact table into PostgreSQL for the full 2022 CMS Part D dataset is expensive because the machine needs space for:

- Gold export CSV files.
- PostgreSQL table heap storage.
- PostgreSQL indexes and primary keys.
- Temporary files during `COPY`, staging, and upsert.
- Existing Docker volumes and Parquet data.

For a laptop-sized local environment, this is not the right default path.

## Change In Direction

The project direction changed from "load full detailed Gold fact into PostgreSQL" to "keep detail in Silver Parquet and load aggregate Gold marts into PostgreSQL."

New local design:

```text
Silver Parquet:
- Row-level prescription detail.
- Used by Spark for heavy scans and reprocessing.

PostgreSQL Gold marts:
- Aggregated BI tables.
- Used by Superset and SQL clients for dashboard queries.
```

This keeps PostgreSQL small enough for local development while preserving the detailed data in Parquet.

## What Not To Run By Default

Avoid this detailed loader path for full 2022 local runs unless there is enough disk capacity and a clear reason to query row-level facts directly in PostgreSQL:

```bash
.venv\Scripts\python.exe gold_loader.py --export-dir data\gold_export --truncate
```

Use the mart loader instead:

```bash
.venv\Scripts\python.exe gold_loader.py --export-dir data\gold_mart_export --load-set marts --truncate
```

## Current Recommended Run Path

Create Gold mart schema:

```bash
docker compose exec postgres psql -U pipeline -d healthcare -f /docker-entrypoint-initdb.d/003_create_gold_marts.sql
```

Export marts from Silver:

```bash
docker compose exec spark /opt/spark/bin/spark-submit --driver-memory 4g /app/spark/jobs/silver_to_gold_marts.py --input-path /app/data/silver/part_d_prescribers --output-path /app/data/gold_mart_export --skip-count
```

Load marts into PostgreSQL:

```bash
.venv\Scripts\python.exe gold_loader.py --export-dir data\gold_mart_export --load-set marts --truncate
```

Verify:

```bash
docker compose exec postgres psql -U pipeline -d healthcare -c "SELECT 'gold_state_year_spending' AS table_name, count(*) FROM gold_state_year_spending UNION ALL SELECT 'gold_drug_year_spending', count(*) FROM gold_drug_year_spending UNION ALL SELECT 'gold_specialty_state_spending', count(*) FROM gold_specialty_state_spending UNION ALL SELECT 'gold_prescriber_summary', count(*) FROM gold_prescriber_summary UNION ALL SELECT 'gold_anomaly_prescribers', count(*) FROM gold_anomaly_prescribers;"
```

## Follow-Up

The next engineering step is to build Superset dashboards from the five Gold mart tables, not from detailed prescription facts.
