# uppi/pipelines.py
from __future__ import annotations

from uppi.services.visura_processor import VisuraProcessor


class UppiPipeline:
    """
    Minimal glue: delegate item processing to VisuraProcessor service.
    """

    def __init__(self):
        self.processor = VisuraProcessor()

    def process_item(self, item, spider):
        return self.processor.process_item(item, spider)
