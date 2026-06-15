from __future__ import annotations

from datetime import datetime
from typing import Any


def insert_ingestion_log(postgres_dsn: str, record: dict[str, Any]) -> None:
    import psycopg

    sql = """
        INSERT INTO ingestion_log (
            run_id,
            dataset_name,
            dataset_id,
            source_url,
            year,
            file_path,
            manifest_path,
            row_count,
            expected_row_count,
            checksum_sha256,
            object_etag,
            status,
            started_at,
            completed_at,
            error_message
        )
        VALUES (
            %(run_id)s,
            %(dataset_name)s,
            %(dataset_id)s,
            %(source_url)s,
            %(year)s,
            %(file_path)s,
            %(manifest_path)s,
            %(row_count)s,
            %(expected_row_count)s,
            %(checksum_sha256)s,
            %(object_etag)s,
            %(status)s,
            %(started_at)s,
            %(completed_at)s,
            %(error_message)s
        )
        ON CONFLICT (run_id, dataset_name, year)
        DO UPDATE SET
            row_count = EXCLUDED.row_count,
            expected_row_count = EXCLUDED.expected_row_count,
            checksum_sha256 = EXCLUDED.checksum_sha256,
            object_etag = EXCLUDED.object_etag,
            status = EXCLUDED.status,
            completed_at = EXCLUDED.completed_at,
            error_message = EXCLUDED.error_message;
    """

    normalized = {
        **record,
        "started_at": record.get("started_at") or datetime.utcnow(),
        "completed_at": record.get("completed_at"),
        "error_message": record.get("error_message"),
    }

    with psycopg.connect(postgres_dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, normalized)
        conn.commit()

