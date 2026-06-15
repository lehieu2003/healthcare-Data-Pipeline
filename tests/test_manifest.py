from pathlib import Path

from ingestion.manifest import build_manifest, compute_sha256


def test_compute_sha256(tmp_path: Path) -> None:
    path = tmp_path / "sample.csv"
    path.write_text("a,b\n1,2\n", encoding="utf-8")

    assert compute_sha256(path) == "ea14f99c47575613ab22111122c847728c61007f6bfd7b062d02fcb99df3feb0"


def test_build_manifest_contains_lineage_fields() -> None:
    manifest = build_manifest(
        dataset_id="dataset-123",
        year=2022,
        row_count=10,
        expected_row_count=10,
        page_size=5000,
        checksum_sha256="abc",
    )

    assert manifest["dataset_name"] == "medicare_part_d_prescribers_by_provider_and_drug"
    assert manifest["dataset_id"] == "dataset-123"
    assert manifest["year"] == 2022
    assert manifest["row_count"] == 10
    assert manifest["expected_row_count"] == 10
    assert manifest["checksum_sha256"] == "abc"
    assert "source_url" in manifest
    assert "ingested_at" in manifest
