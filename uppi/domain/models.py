from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass(frozen=True)
class Visura:
    cf: str
    pdf_bucket: str
    pdf_object: str
    checksum_sha256: Optional[str] = None
    fetched_at: Optional[datetime] = None


@dataclass(frozen=True)
class Contract:
    contract_id: str
    immobile_id: int
    contract_kind: Optional[str] = None
    start_date: Optional[str] = None
    durata_anni: Optional[int] = None
    arredato: Optional[str] = None
    energy_class: Optional[str] = None
    canone_contrattuale_mensile: Optional[float] = None
