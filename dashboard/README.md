# Superset Dashboard

This folder contains the local Superset setup notes for the CMS Medicare Part D anomaly dashboard.

## Goal

The dashboard should answer the main project question:

```text
Which prescribers exhibit abnormal prescribing behavior compared to peers in the same specialty and state?
```

The spending charts provide context, but the anomaly charts are the core deliverable.

## 1. Start Services

Start PostgreSQL first:

```bash
docker compose up -d postgres
```

Build the local Superset image. This image extends `apache/superset:4.1.1` with `psycopg2-binary`, which Superset needs to connect to PostgreSQL:

```bash
docker compose build superset
```

Start Superset:

```bash
docker compose up -d postgres superset
```

Open Superset:

```text
http://localhost:8088
```

Default local credentials:

```text
admin / admin
```

## 2. Prepare Gold Marts

Create the Gold mart tables:

```bash
docker compose exec postgres psql -U pipeline -d healthcare -f /docker-entrypoint-initdb.d/003_create_gold_marts.sql
```

Load the Gold marts if they are not already loaded:

```bash
.venv\Scripts\python.exe gold_loader.py --export-dir data\gold_mart_export --load-set marts --truncate
```

Verify mart row counts:

```bash
docker compose exec postgres psql -U pipeline -d healthcare -c "SELECT 'gold_state_year_spending' AS table_name, count(*) FROM gold_state_year_spending UNION ALL SELECT 'gold_drug_year_spending', count(*) FROM gold_drug_year_spending UNION ALL SELECT 'gold_specialty_state_spending', count(*) FROM gold_specialty_state_spending UNION ALL SELECT 'gold_prescriber_summary', count(*) FROM gold_prescriber_summary UNION ALL SELECT 'gold_anomaly_prescribers', count(*) FROM gold_anomaly_prescribers;"
```

## 3. Create Dashboard Views

Run the dashboard view script:

```bash
docker compose exec postgres psql -U pipeline -d healthcare -f /docker-entrypoint-initdb.d/004_create_dashboard_views.sql
```

This command runs `psql` inside the PostgreSQL container. The local `sql/` folder is mounted to `/docker-entrypoint-initdb.d/`, so this local file:

```text
sql/004_create_dashboard_views.sql
```

is available inside the container as:

```text
/docker-entrypoint-initdb.d/004_create_dashboard_views.sql
```

## 4. Add PostgreSQL In Superset

In Superset, go to:

```text
Settings -> Database Connections -> + Database
```

Use these form values:

```text
Host: postgres
Port: 5432
Database name: healthcare
Username: pipeline
Password: pipeline
Display name: Healthcare PostgreSQL
Additional parameters: leave blank
```

Use `postgres:5432` in Superset because Superset runs inside Docker. Use `localhost:55432` only from Windows or VS Code database tools.

If using the SQLAlchemy URI field, use:


```text
postgresql+psycopg2://pipeline:pipeline@postgres:5432/healthcare
```

## 5. Create Datasets

Go to:

```text
Data -> Datasets -> + Dataset
```

Database:

```text
Healthcare PostgreSQL
```

Schema:

```text
public
```

Create datasets from these views:

- `dashboard_overview_kpis`
- `dashboard_state_spending`
- `dashboard_top_drugs`
- `dashboard_specialty_state_spending`
- `dashboard_prescriber_leaders`
- `dashboard_anomaly_prescribers`
- `dashboard_anomaly_summary`

If Superset says "This table already has a dataset", skip it and use the existing dataset.

## 6. Create Charts

Create one dashboard named:

```text
Medicare Part D Prescribing Anomaly Analysis
```

Recommended charts:

| Name | Dataset | Chart type | Configuration |
| --- | --- | --- | --- |
| `Total Drug Cost` | `dashboard_overview_kpis` | Big Number | Metric: `SUM(total_drug_cost)` |
| `Total Claims KPI` | `dashboard_overview_kpis` | Big Number | Metric: `SUM(total_claims)` |
| `Anomaly Prescribers` | `dashboard_anomaly_prescribers` | Big Number | Metric: `COUNT(*)` |
| `Anomaly Prescribers by State` | `dashboard_anomaly_summary` | Bar Chart | X-Axis: `state`; Metric: `SUM(anomaly_prescriber_count)`; Sort descending; Row limit: `15` |
| `Anomaly Prescribers by Specialty` | `dashboard_anomaly_summary` | Bar Chart | X-Axis: `specialty`; Metric: `SUM(anomaly_prescriber_count)`; Sort descending; Row limit: `15` |
| `Highest Cost Anomaly Prescribers` | `dashboard_anomaly_prescribers` | Table | Raw records; columns: `full_name`, `npi`, `state`, `specialty`, `total_claims`, `total_drug_cost`, `avg_cost_per_claim`, `peer_p95_cost_per_claim`, `anomaly_reason`; sort `total_drug_cost` descending; row limit `25` |
| `State Drug Cost` | `dashboard_state_spending` | Bar Chart | X-Axis: `state`; Metric: `SUM(total_drug_cost)`; Sort descending |
| `Top Drugs by Cost` | `dashboard_top_drugs` | Bar Chart | X-Axis: `brand_name`; Metric: `SUM(total_drug_cost)`; Sort descending; Row limit: `20` |

For Bar Chart, do not put the same column in both `X-Axis` and `Dimensions`; that causes duplicate label errors in Superset.

## 7. Dashboard Layout

Use this layout:

```text
Top row:
Total Drug Cost | Total Claims KPI | Anomaly Prescribers

Middle:
Anomaly Prescribers by State | Anomaly Prescribers by Specialty

Context:
State Drug Cost | Top Drugs by Cost

Bottom:
Highest Cost Anomaly Prescribers
```

Use `year`, `state`, and `specialty` as native dashboard filters.
