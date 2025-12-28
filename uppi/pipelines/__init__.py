from __future__ import annotations

import logging
from pathlib import Path

from decouple import config
from itemadapter import ItemAdapter

from uppi.services import DatabaseRepository, DbConnectionManager
from uppi.services.storage_minio import StorageService
from uppi.services.visura_processing import VisuraProcessingService

logger = logging.getLogger(__name__)

AE_USERNAME = config("AE_USERNAME", default="").strip()
TEMPLATE_VERSION = config("TEMPLATE_VERSION", default="pescara2018_v1").strip()

PRUNE_OLD_IMMOBILI_WITHOUT_CONTRACTS = (
    config("PRUNE_OLD_IMMOBILI_WITHOUT_CONTRACTS", default="True").strip().lower() == "true"
)
DELETE_LOCAL_VISURA_AFTER_UPLOAD = (
    config("DELETE_LOCAL_VISURA_AFTER_UPLOAD", default="False").strip().lower() == "true"
)


class UppiPipeline:
    """
    Scrapy pipeline: мінімальний glue між Scrapy item та сервісним шаром.
    """

    def __init__(self):
        template_path = (
            Path(__file__).resolve().parents[1] / "attestazione_template" / "template_attestazione_pescara.docx"
        )
        repo = DatabaseRepository(ae_username=AE_USERNAME, template_version=TEMPLATE_VERSION)
        storage = StorageService()

        self.service = VisuraProcessingService(
            repo=repo,
            storage=storage,
            template_path=template_path,
            delete_local_visura_after_upload=DELETE_LOCAL_VISURA_AFTER_UPLOAD,
            prune_old_immobili_without_contracts=PRUNE_OLD_IMMOBILI_WITHOUT_CONTRACTS,
        )

    def process_item(self, item, spider):
        adapter = ItemAdapter(item)
        payload = dict(adapter.asdict())

        try:
            with DbConnectionManager() as conn:
                self.service.process_item(payload, conn, spider.logger)
        except Exception as exc:
            spider.logger.exception("[PIPELINE] Fatal error: %s", exc)

        return item
