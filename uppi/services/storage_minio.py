from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from minio.error import S3Error

from uppi.config.env import MinioConfig, load_minio_config
from uppi.domain.object_storage import ObjectStorage
from uppi.utils.retry import retry_call

logger = logging.getLogger(__name__)


@dataclass
class UploadResult:
    bucket: str
    object_name: str
    content_type: str


class StorageService:
    def __init__(self, config: Optional[MinioConfig] = None):
        self.config = config or load_minio_config()
        self.storage = ObjectStorage()
        self.storage.cfg = self.storage.cfg.__class__(
            endpoint=self.config.endpoint,
            access_key=self.config.access_key,
            secret_key=self.config.secret_key,
            secure=self.config.secure,
            visure_bucket=self.config.visure_bucket,
            attestazioni_bucket=self.config.attestazioni_bucket,
        )

    def object_exists(self, bucket: str, object_name: str) -> bool:
        return retry_call(
            lambda: self.storage.object_exists(bucket, object_name),
            retries=2,
            backoff_s=0.5,
            retry_exceptions=(S3Error, Exception),
        )

    def upload_file(self, bucket: str, object_name: str, file_path: Path, content_type: str) -> UploadResult:
        def _upload():
            self.storage.upload_file(bucket, object_name, file_path, content_type)
            return UploadResult(bucket=bucket, object_name=object_name, content_type=content_type)

        return retry_call(
            _upload,
            retries=2,
            backoff_s=0.5,
            retry_exceptions=(S3Error, Exception),
        )

    def visura_object_name(self, cf: str) -> str:
        return self.storage.visura_object_name(cf)

    def attestazione_object_name(self, cf: str, contract_id: str) -> str:
        return self.storage.attestazione_object_name(cf, contract_id)
