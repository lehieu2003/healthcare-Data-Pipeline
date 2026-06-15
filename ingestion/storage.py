from __future__ import annotations

from pathlib import Path


class MinioRawStorage:
    def __init__(
        self,
        *,
        endpoint: str,
        access_key: str,
        secret_key: str,
        secure: bool,
        bucket_name: str,
    ):
        from minio import Minio

        self.bucket_name = bucket_name
        self.client = Minio(
            endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=secure,
        )

    def ensure_bucket(self) -> None:
        if not self.client.bucket_exists(self.bucket_name):
            self.client.make_bucket(self.bucket_name)

    def upload_file(self, local_path: Path, object_name: str) -> str | None:
        result = self.client.fput_object(
            bucket_name=self.bucket_name,
            object_name=object_name,
            file_path=str(local_path),
        )
        return getattr(result, "etag", None)

