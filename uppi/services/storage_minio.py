from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from minio.error import S3Error

from uppi.domain.object_storage import ObjectStorage
from uppi.services.retry import retry


@dataclass(frozen=True)
class StorageUploadResult:
    bucket: str
    object_name: str


class StorageService:
    def __init__(self, storage: Optional[ObjectStorage] = None):
        self.storage = storage or ObjectStorage()

    def object_exists(self, bucket: str, object_name: str) -> bool:
        return retry(
            lambda: self.storage.object_exists(bucket, object_name),
            exceptions=(S3Error, Exception),
            attempts=3,
            base_delay=0.5,
            logger=None,
            context=f"object_exists {bucket}/{object_name}",
        )

    def upload_file(self, bucket: str, object_name: str, path: Path, content_type: str) -> StorageUploadResult:
        retry(
            lambda: self.storage.upload_file(bucket, object_name, path, content_type=content_type),
            exceptions=(S3Error, Exception),
            attempts=3,
            base_delay=0.5,
            logger=None,
            context=f"upload {bucket}/{object_name}",
        )
        return StorageUploadResult(bucket=bucket, object_name=object_name)
