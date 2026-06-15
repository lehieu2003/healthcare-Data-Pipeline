from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


DATASET_NAME = "medicare_part_d_prescribers_by_provider_and_drug"

DATASET_IDS_BY_YEAR: dict[int, str] = {
    2020: "7795fe20-e80e-435a-a9ed-d2d65e05feeb",
    2021: "f68114ed-f854-4ffc-9c6e-ed78b5e2f8d0",
    2022: "b101b457-ffa4-49bb-8fd9-27c1266086e2",
}

CMS_DATA_API_URL = "https://data.cms.gov/data-api/v1/dataset/{dataset_id}/data"
CMS_STATS_API_URL = "https://data.cms.gov/data-api/v1/dataset/{dataset_id}/data/stats"


@dataclass(frozen=True)
class PipelineSettings:
    page_size: int
    minio_endpoint: str
    minio_access_key: str
    minio_secret_key: str
    minio_secure: bool
    minio_raw_bucket: str
    postgres_dsn: str | None


def load_settings() -> PipelineSettings:
    load_env_file(Path(".env"))

    return PipelineSettings(
        page_size=int(os.getenv("CMS_PAGE_SIZE", "5000")),
        minio_endpoint=os.getenv("MINIO_ENDPOINT", "localhost:9000"),
        minio_access_key=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
        minio_secret_key=os.getenv("MINIO_SECRET_KEY", "minioadmin"),
        minio_secure=os.getenv("MINIO_SECURE", "false").lower() == "true",
        minio_raw_bucket=os.getenv("MINIO_RAW_BUCKET", "cms-raw"),
        postgres_dsn=os.getenv("POSTGRES_DSN"),
    )


def load_env_file(path: Path) -> None:
    if not path.exists():
        return

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue

        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        os.environ.setdefault(key, value)
