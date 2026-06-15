from __future__ import annotations

import argparse
import csv
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Protocol

from ingestion.cms_client import CmsDataApiClient
from ingestion.config import DATASET_IDS_BY_YEAR, DATASET_NAME, load_settings
from ingestion.manifest import build_manifest, compute_sha256, write_manifest
from ingestion.metadata import insert_ingestion_log
from ingestion.storage import MinioRawStorage


class PageFetcher(Protocol):
    def fetch_page(self, dataset_id: str, size: int, offset: int) -> list[dict[str, object]]:
        ...

    def fetch_stats(self, dataset_id: str) -> dict[str, object]:
        ...


@dataclass(frozen=True)
class DownloadResult:
    year: int
    dataset_id: str
    csv_path: Path
    manifest_path: Path
    row_count: int
    expected_row_count: int | None
    checksum_sha256: str


def extract_total_rows(stats: dict[str, object]) -> int | None:
    for key in ("total_rows", "totalRows", "row_count", "rowCount", "count"):
        value = stats.get(key)
        if isinstance(value, int):
            return value
        if isinstance(value, str) and value.isdigit():
            return int(value)
    return None


def download_year(
    *,
    client: PageFetcher,
    year: int,
    output_dir: Path,
    page_size: int,
    max_pages: int | None = None,
) -> DownloadResult:
    dataset_id = DATASET_IDS_BY_YEAR[year]
    output_dir.mkdir(parents=True, exist_ok=True)

    csv_path = output_dir / f"part_d_{year}.csv"
    manifest_path = output_dir / f"part_d_{year}.manifest.json"

    stats = client.fetch_stats(dataset_id)
    expected_row_count = extract_total_rows(stats)

    offset = 0
    row_count = 0
    fieldnames: list[str] | None = None

    pages_downloaded = 0

    with csv_path.open("w", newline="", encoding="utf-8") as file_obj:
        writer: csv.DictWriter[str] | None = None

        while True:
            if max_pages is not None and pages_downloaded >= max_pages:
                break

            rows = client.fetch_page(dataset_id=dataset_id, size=page_size, offset=offset)
            if not rows:
                break

            if fieldnames is None:
                fieldnames = list(rows[0].keys())
                writer = csv.DictWriter(file_obj, fieldnames=fieldnames, extrasaction="ignore")
                writer.writeheader()

            assert writer is not None
            writer.writerows(rows)
            row_count += len(rows)
            offset += page_size
            pages_downloaded += 1

            if pages_downloaded == 1 or pages_downloaded % 25 == 0:
                if expected_row_count:
                    pct = (row_count / expected_row_count) * 100
                    print(
                        f"year={year} pages={pages_downloaded} rows={row_count}/{expected_row_count} ({pct:.2f}%)",
                        flush=True,
                    )
                else:
                    print(f"year={year} pages={pages_downloaded} rows={row_count}", flush=True)

            if expected_row_count is not None and row_count >= expected_row_count:
                break

    checksum = compute_sha256(csv_path)
    manifest = build_manifest(
        dataset_id=dataset_id,
        year=year,
        row_count=row_count,
        expected_row_count=expected_row_count,
        page_size=page_size,
        checksum_sha256=checksum,
    )
    write_manifest(manifest_path, manifest)

    is_partial_download = max_pages is not None
    if expected_row_count is not None and row_count != expected_row_count and not is_partial_download:
        raise ValueError(f"Row count mismatch for year={year}: expected {expected_row_count}, got {row_count}")

    return DownloadResult(
        year=year,
        dataset_id=dataset_id,
        csv_path=csv_path,
        manifest_path=manifest_path,
        row_count=row_count,
        expected_row_count=expected_row_count,
        checksum_sha256=checksum,
    )


def upload_bronze(result: DownloadResult) -> tuple[str, str, str | None]:
    settings = load_settings()
    storage = MinioRawStorage(
        endpoint=settings.minio_endpoint,
        access_key=settings.minio_access_key,
        secret_key=settings.minio_secret_key,
        secure=settings.minio_secure,
        bucket_name=settings.minio_raw_bucket,
    )
    storage.ensure_bucket()

    csv_object = f"year={result.year}/part_d_{result.year}.csv"
    manifest_object = f"year={result.year}/manifest.json"

    csv_etag = storage.upload_file(result.csv_path, csv_object)
    storage.upload_file(result.manifest_path, manifest_object)
    return csv_object, manifest_object, csv_etag


def log_success(result: DownloadResult, csv_object: str, manifest_object: str, object_etag: str | None) -> None:
    settings = load_settings()
    if not settings.postgres_dsn:
        return

    completed_at = datetime.utcnow()
    insert_ingestion_log(
        settings.postgres_dsn,
        {
            "run_id": f"manual__{result.year}",
            "dataset_name": DATASET_NAME,
            "dataset_id": result.dataset_id,
            "source_url": f"https://data.cms.gov/data-api/v1/dataset/{result.dataset_id}/data",
            "year": result.year,
            "file_path": f"{settings.minio_raw_bucket}/{csv_object}",
            "manifest_path": f"{settings.minio_raw_bucket}/{manifest_object}",
            "row_count": result.row_count,
            "expected_row_count": result.expected_row_count,
            "checksum_sha256": result.checksum_sha256,
            "object_etag": object_etag,
            "status": "SUCCESS",
            "started_at": completed_at,
            "completed_at": completed_at,
            "error_message": None,
        },
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ingest CMS Medicare Part D data to local CSV and optional MinIO.")
    parser.add_argument("--years", nargs="+", type=int, default=[2020, 2021, 2022])
    parser.add_argument("--output-dir", type=Path, default=Path("data/raw"))
    parser.add_argument("--max-pages", type=int, help="Limit pages per year for local smoke tests.")
    parser.add_argument("--skip-upload", action="store_true", help="Only download CSV and manifest locally.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    settings = load_settings()
    client = CmsDataApiClient()

    for year in args.years:
        if year not in DATASET_IDS_BY_YEAR:
            raise ValueError(f"Unsupported year: {year}")

        result = download_year(
            client=client,
            year=year,
            output_dir=args.output_dir,
            page_size=settings.page_size,
            max_pages=args.max_pages,
        )
        print(f"Downloaded year={year} rows={result.row_count} csv={result.csv_path}")

        if args.skip_upload:
            continue

        csv_object, manifest_object, object_etag = upload_bronze(result)
        log_success(result, csv_object, manifest_object, object_etag)
        print(f"Uploaded year={year} to MinIO objects: {csv_object}, {manifest_object}")


if __name__ == "__main__":
    main()
