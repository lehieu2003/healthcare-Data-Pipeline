from __future__ import annotations

import csv
import json
from pathlib import Path

import pytest

from ingestion.config import DATASET_IDS_BY_YEAR
from ingestion.ingest_cms_part_d import download_year, extract_total_rows


class FakeCmsClient:
    def __init__(self, pages: list[list[dict[str, object]]], total_rows: int | None):
        self.pages = pages
        self.total_rows = total_rows
        self.calls: list[tuple[str, int, int]] = []

    def fetch_stats(self, dataset_id: str) -> dict[str, object]:
        if self.total_rows is None:
            return {}
        return {"total_rows": self.total_rows}

    def fetch_page(self, dataset_id: str, size: int, offset: int) -> list[dict[str, object]]:
        self.calls.append((dataset_id, size, offset))
        index = offset // size
        if index >= len(self.pages):
            return []
        return self.pages[index]


def test_extract_total_rows_accepts_common_stats_keys() -> None:
    assert extract_total_rows({"total_rows": 12}) == 12
    assert extract_total_rows({"rowCount": "13"}) == 13
    assert extract_total_rows({"unknown": 1}) is None


def test_download_year_paginates_writes_csv_and_manifest(tmp_path: Path) -> None:
    client = FakeCmsClient(
        pages=[
            [{"Prscrbr_NPI": "1", "Tot_Clms": "10"}],
            [{"Prscrbr_NPI": "2", "Tot_Clms": "20"}],
        ],
        total_rows=2,
    )

    result = download_year(client=client, year=2022, output_dir=tmp_path, page_size=1)

    assert result.row_count == 2
    assert result.expected_row_count == 2
    assert result.dataset_id == DATASET_IDS_BY_YEAR[2022]
    assert client.calls == [
        (DATASET_IDS_BY_YEAR[2022], 1, 0),
        (DATASET_IDS_BY_YEAR[2022], 1, 1),
    ]

    with result.csv_path.open(newline="", encoding="utf-8") as file_obj:
        rows = list(csv.DictReader(file_obj))

    assert rows == [
        {"Prscrbr_NPI": "1", "Tot_Clms": "10"},
        {"Prscrbr_NPI": "2", "Tot_Clms": "20"},
    ]

    manifest = json.loads(result.manifest_path.read_text(encoding="utf-8"))
    assert manifest["row_count"] == 2
    assert manifest["expected_row_count"] == 2
    assert manifest["checksum_sha256"] == result.checksum_sha256


def test_download_year_fails_on_row_count_mismatch(tmp_path: Path) -> None:
    client = FakeCmsClient(
        pages=[[{"Prscrbr_NPI": "1", "Tot_Clms": "10"}]],
        total_rows=2,
    )

    with pytest.raises(ValueError, match="Row count mismatch"):
        download_year(client=client, year=2022, output_dir=tmp_path, page_size=1)


def test_download_year_allows_partial_smoke_download_with_max_pages(tmp_path: Path) -> None:
    client = FakeCmsClient(
        pages=[
            [{"Prscrbr_NPI": "1", "Tot_Clms": "10"}],
            [{"Prscrbr_NPI": "2", "Tot_Clms": "20"}],
        ],
        total_rows=2,
    )

    result = download_year(client=client, year=2022, output_dir=tmp_path, page_size=1, max_pages=1)

    assert result.row_count == 1
    assert result.expected_row_count == 2
    assert client.calls == [(DATASET_IDS_BY_YEAR[2022], 1, 0)]
