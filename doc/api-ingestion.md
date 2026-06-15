# API Ingestion Guide

## Medicare Part D Prescribers by Provider and Drug

This document explains how to call the CMS API to ingest Medicare Part D Prescribers data into the Healthcare Data Pipeline.

---

# 0. Dataset Version

Each Medicare Part D Prescribers dataset version has a unique CMS dataset UUID.

| Year | Dataset UUID                           |
| ---- | -------------------------------------- |
| 2024 | `d5aa71a8-dcc0-4570-8bcf-bd39deac69fe` |
| 2023 | `e54db557-cd82-4e91-a0fe-61aad5865d69` |
| 2022 | `b101b457-ffa4-49bb-8fd9-27c1266086e2` |
| 2021 | `f68114ed-f854-4ffc-9c6e-ed78b5e2f8d0` |
| 2020 | `7795fe20-e80e-435a-a9ed-d2d65e05feeb` |
| 2019 | `2a6705e6-7a1e-460c-ba22-35249a531918` |
| 2018 | `802fe556-311f-4962-8d75-d5f4ff405884` |
| 2017 | `05f108dd-76c4-49f4-9fdc-788d8f4251ec` |
| 2016 | `25106f9d-0eb8-4ba7-b237-486ee87d910a` |
| 2015 | `1d650894-8afe-4056-ba31-a85cb0e3cee6` |
| 2014 | `0779bc8d-18dd-40b8-9d61-7addc8b0daf1` |
| 2013 | `c6905d43-45de-470d-897c-9ed8e75e256d` |

For this project, the initial scope uses:
2020
2021
2022

# 1. API Overview

The project uses the CMS Data API to retrieve Medicare Part D prescription data programmatically.

Instead of manually downloading CSV files, the pipeline uses API-based ingestion so the process can be:

- Automated
- Scheduled by Airflow
- Re-runnable
- Logged with metadata
- Integrated with MinIO Bronze storage

---

# 2. Base API Pattern

CMS dataset APIs follow this pattern:

```text
https://data.cms.gov/data-api/v1/dataset/{dataset_id}/data
```

Replace:

```text
{dataset_id}
```

with the dataset ID from the CMS API documentation page.

Example:

```text
https://data.cms.gov/data-api/v1/dataset/{dataset_id}/data?size=5000&offset=0
```

---

# 3. Pagination

CMS API does not return the full dataset in one request.

The API should be called page by page using:

```text
size
offset
```

Example:

```text
size=5000
offset=0
```

Next page:

```text
size=5000
offset=5000
```

Next page:

```text
size=5000
offset=10000
```

General rule:

```text
next_offset = current_offset + size
```

The ingestion script should stop when the API returns an empty response or when the total row count has been reached.

---

# 4. Get Dataset Statistics

Before downloading all data, call the stats endpoint to estimate the total number of rows.

```text
https://data.cms.gov/data-api/v1/dataset/{dataset_id}/data/stats
```

This helps the ingestion job know:

- Total number of rows
- Expected row count
- Download progress
- Whether ingestion is complete

---

# 5. Filtering Data

The CMS API supports filters.

For this project, each year already has its own CMS dataset UUID. Version 1 should select the correct `dataset_id` for the target year and download that dataset directly. Do not depend on a `filter[Year]` parameter unless the specific dataset exposes a `Year` column and the behavior has been tested.

Example filter by year if the dataset exposes a `Year` column:

```text
https://data.cms.gov/data-api/v1/dataset/{dataset_id}/data?filter[Year]=2022&size=5000&offset=0
```

Example filter by state:

```text
https://data.cms.gov/data-api/v1/dataset/{dataset_id}/data?filter[Prscrbr_State_Abrvtn]=CA&size=5000&offset=0
```

In this project, the recommended approach is:

```text
Download one CMS dataset UUID per year
```

because the pipeline stores files in MinIO using year-based partitions.

---

# 6. Selecting Columns

To reduce API response size, the API can request only selected columns.

Example:

```text
https://data.cms.gov/data-api/v1/dataset/{dataset_id}/data?column=Prscrbr_NPI,Prscrbr_State_Abrvtn,Gnrc_Name,Tot_Clms,Tot_Drug_Cst&size=5000&offset=0
```

For the first version of the project, it is better to keep all required columns for downstream processing.

---

# 7. Recommended Columns for This Project

The ingestion job should keep at least these columns:

```text
Prscrbr_NPI
Prscrbr_Last_Org_Name
Prscrbr_First_Name
Prscrbr_City
Prscrbr_State_Abrvtn
Prscrbr_State_FIPS
Prscrbr_Type
Brnd_Name
Gnrc_Name
Tot_Clms
Tot_30day_Fills
Tot_Drug_Cst
Tot_Day_Suply
Tot_Benes
GE65_Tot_Clms
GE65_Tot_Drug_Cst
GE65_Sprsn_Flag
```

These fields are required for:

- Prescriber dimension
- Drug dimension
- State analysis
- Cost analysis
- Anomaly detection
- Data quality checks

---

# 8. Ingestion Workflow

The API ingestion job follows this workflow:

```text
Start
  ↓
Read config
  ↓
Call CMS stats endpoint
  ↓
Loop through pages using size + offset
  ↓
Write each response page to local temporary file
  ↓
Merge or stream records into yearly CSV
  ↓
Upload CSV to MinIO Bronze
  ↓
Write manifest with row count and checksum
  ↓
Log metadata to PostgreSQL
  ↓
Verify row count and checksum
  ↓
End
```

---

# 9. MinIO Bronze Path Design

Raw data should be stored without modification.

Recommended path:

```text
cms-raw/
├── year=2020/
│   ├── part_d_2020.csv
│   └── manifest.json
├── year=2021/
│   ├── part_d_2021.csv
│   └── manifest.json
└── year=2022/
    ├── part_d_2022.csv
    └── manifest.json
```

The Bronze layer must preserve the original data as much as possible.

Do not clean, rename, or transform fields during ingestion.

The manifest records ingestion metadata for reproducibility:

```json
{
  "dataset_name": "medicare_part_d_prescribers_by_provider_and_drug",
  "dataset_id": "b101b457-ffa4-49bb-8fd9-27c1266086e2",
  "year": 2022,
  "source_url": "https://data.cms.gov/data-api/v1/dataset/b101b457-ffa4-49bb-8fd9-27c1266086e2/data",
  "row_count": 10000000,
  "expected_row_count": 10000000,
  "page_size": 5000,
  "checksum_sha256": "example",
  "ingested_at": "2026-06-15T00:00:00Z"
}
```

---

# 10. Python Example

```python
import csv
import time
import requests
from pathlib import Path


BASE_URL = "https://data.cms.gov/data-api/v1/dataset/{dataset_id}/data"

DATASET_IDS = {
    2020: "7795fe20-e80e-435a-a9ed-d2d65e05feeb",
    2021: "f68114ed-f854-4ffc-9c6e-ed78b5e2f8d0",
    2022: "b101b457-ffa4-49bb-8fd9-27c1266086e2",
}

PAGE_SIZE = 5000
YEAR = 2022

OUTPUT_DIR = Path("data/raw")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def fetch_page(dataset_id: str, size: int, offset: int):
    url = BASE_URL.format(dataset_id=dataset_id)

    params = {
        "size": size,
        "offset": offset,
    }

    response = requests.get(url, params=params, timeout=60)
    response.raise_for_status()

    return response.json()


def ingest_year(year: int):
    dataset_id = DATASET_IDS[year]
    output_file = OUTPUT_DIR / f"part_d_{year}.csv"
    offset = 0
    total_rows = 0
    header_written = False

    with output_file.open("w", newline="", encoding="utf-8") as f:
        writer = None

        while True:
            rows = fetch_page(
                dataset_id=dataset_id,
                size=PAGE_SIZE,
                offset=offset,
            )

            if not rows:
                break

            if not header_written:
                writer = csv.DictWriter(f, fieldnames=rows[0].keys())
                writer.writeheader()
                header_written = True

            writer.writerows(rows)

            total_rows += len(rows)

            print(f"Downloaded {total_rows} rows for year={year}")

            offset += PAGE_SIZE

            time.sleep(0.2)

    print(f"Ingestion completed: {output_file}")
    print(f"Total rows: {total_rows}")


if __name__ == "__main__":
    ingest_year(YEAR)
```

---

# 11. Python Example with Multiple Years

```python
YEARS = [2020, 2021, 2022]

for year in YEARS:
    ingest_year(year)
```

---

# 12. Upload to MinIO

After downloading the file, upload it to MinIO Bronze.

Target path:

```text
cms-raw/year=2022/part_d_2022.csv
```

Example pseudo-code:

```python
from minio import Minio


client = Minio(
    "localhost:9000",
    access_key="minioadmin",
    secret_key="minioadmin",
    secure=False,
)


bucket_name = "cms-raw"
object_name = "year=2022/part_d_2022.csv"
file_path = "data/raw/part_d_2022.csv"

client.fput_object(
    bucket_name=bucket_name,
    object_name=object_name,
    file_path=file_path,
)
```

---

# 13. Metadata Logging

Each ingestion run should be logged into PostgreSQL.

Recommended table:

```sql
CREATE TABLE IF NOT EXISTS ingestion_log (
    ingestion_id SERIAL PRIMARY KEY,
    run_id TEXT NOT NULL,
    dataset_name TEXT NOT NULL,
    dataset_id TEXT NOT NULL,
    source_url TEXT NOT NULL,
    year INTEGER NOT NULL,
    file_path TEXT NOT NULL,
    manifest_path TEXT NOT NULL,
    row_count BIGINT NOT NULL,
    expected_row_count BIGINT,
    checksum_sha256 TEXT,
    object_etag TEXT,
    status TEXT NOT NULL,
    started_at TIMESTAMP NOT NULL,
    completed_at TIMESTAMP,
    error_message TEXT,
    CONSTRAINT ingestion_status_valid
        CHECK (status IN ('RUNNING', 'SUCCESS', 'FAILED')),
    CONSTRAINT ingestion_run_unique
        UNIQUE (run_id, dataset_name, year)
);
```

Idempotency rule:

```text
If a successful run already exists for the same dataset_id + year + checksum,
do not upload a duplicate Bronze object.
```

Example metadata:

| Field        | Example                                          |
| ------------ | ------------------------------------------------ |
| run_id       | scheduled__2026-01-01                            |
| dataset_name | medicare_part_d_prescribers_by_provider_and_drug |
| dataset_id   | b101b457-ffa4-49bb-8fd9-27c1266086e2             |
| year         | 2022                                             |
| file_path    | cms-raw/year=2022/part_d_2022.csv                |
| manifest_path | cms-raw/year=2022/manifest.json                 |
| row_count    | 10000000                                         |
| status       | SUCCESS                                          |

---

# 14. Error Handling

The ingestion script should handle:

- Network timeout
- API rate limit
- Empty response
- Invalid JSON response
- Partial download
- MinIO upload failure
- Row count mismatch
- Checksum mismatch
- Duplicate successful ingestion

Recommended strategy:

```text
Retry failed request
Wait before retrying
Log error message
Fail the Airflow task if retries are exhausted
```

---

# 15. Airflow Task Design

The ingestion DAG should contain these tasks:

```text
check_api_available
→ get_dataset_stats
→ download_yearly_data
→ upload_to_minio
→ write_manifest
→ log_ingestion_metadata
→ verify_minio_file
→ verify_row_count_and_checksum
```

Recommended DAG name:

```text
dag_ingest_cms_part_d
```

Recommended schedule:

```text
@yearly
```

For development:

```text
manual trigger
```

---

# 16. Why API Ingestion Instead of Manual Download?

Manual download is acceptable for exploration.

However, API ingestion is better for a data engineering portfolio because it demonstrates:

- Automation
- Pagination handling
- Metadata logging
- Reproducibility
- Pipeline orchestration
- Production-style ingestion design

---

# 17. Notes

For the first implementation, focus on:

```text
API → Local CSV → MinIO Bronze
```

After that, connect the ingestion script to Airflow.

Do not clean data during ingestion.

Cleaning belongs to the Bronze → Silver Spark job.
