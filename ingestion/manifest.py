from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ingestion.config import CMS_DATA_API_URL, DATASET_NAME


def compute_sha256(path: Path) -> str:
    digest = hashlib.sha256()

    with path.open("rb") as file_obj:
        for chunk in iter(lambda: file_obj.read(1024 * 1024), b""):
            digest.update(chunk)

    return digest.hexdigest()


def build_manifest(
    *,
    dataset_id: str,
    year: int,
    row_count: int,
    expected_row_count: int | None,
    page_size: int,
    checksum_sha256: str,
) -> dict[str, Any]:
    return {
        "dataset_name": DATASET_NAME,
        "dataset_id": dataset_id,
        "year": year,
        "source_url": CMS_DATA_API_URL.format(dataset_id=dataset_id),
        "row_count": row_count,
        "expected_row_count": expected_row_count,
        "page_size": page_size,
        "checksum_sha256": checksum_sha256,
        "ingested_at": datetime.now(timezone.utc).isoformat(),
    }


def write_manifest(path: Path, manifest: dict[str, Any]) -> None:
    path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")

