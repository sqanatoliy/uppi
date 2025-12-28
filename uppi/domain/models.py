from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, Optional


@dataclass(frozen=True)
class ClientConfig:
    locatore_cf: str
    force_update_visura: bool = False
    comune: str = "PESCARA"
    tipo_catasto: str = "F"
    ufficio_label: str = "PESCARA Territorio"

    locatore_comune_res: Optional[str] = None
    locatore_via: Optional[str] = None
    locatore_civico: Optional[str] = None

    immobile_comune: Optional[str] = None
    immobile_via: Optional[str] = None
    immobile_civico: Optional[str] = None
    immobile_piano: Optional[str] = None
    immobile_interno: Optional[str] = None
    foglio: Optional[str] = None
    numero: Optional[str] = None
    sub: Optional[str] = None
    rendita: Optional[str] = None
    superficie_totale: Optional[str] = None
    categoria: Optional[str] = None

    contratto_data: Optional[str] = None
    conduttore_nome: Optional[str] = None
    conduttore_cf: Optional[str] = None
    conduttore_comune: Optional[str] = None
    conduttore_via: Optional[str] = None

    decorrenza_data: Optional[str] = None
    registrazione_data: Optional[str] = None
    registrazione_num: Optional[str] = None
    agenzia_entrate_sede: Optional[str] = None

    contract_kind: Optional[str] = None
    arredato: Optional[str] = None
    energy_class: Optional[str] = None
    canone_contrattuale_mensile: Optional[str] = None
    durata_anni: Optional[str] = None

    a1: Optional[str] = None
    a2: Optional[str] = None
    b1: Optional[str] = None
    b2: Optional[str] = None
    b3: Optional[str] = None
    b4: Optional[str] = None
    b5: Optional[str] = None
    c1: Optional[str] = None
    c2: Optional[str] = None
    c3: Optional[str] = None
    c4: Optional[str] = None
    c5: Optional[str] = None
    c6: Optional[str] = None
    c7: Optional[str] = None
    d1: Optional[str] = None
    d2: Optional[str] = None
    d3: Optional[str] = None
    d4: Optional[str] = None
    d5: Optional[str] = None
    d6: Optional[str] = None
    d7: Optional[str] = None
    d8: Optional[str] = None
    d9: Optional[str] = None
    d10: Optional[str] = None
    d11: Optional[str] = None
    d12: Optional[str] = None
    d13: Optional[str] = None

    extra: Dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class VisuraMetadata:
    cf: str
    pdf_bucket: str
    pdf_object: str
    checksum_sha256: Optional[str]
    fetched_at: Optional[datetime]


@dataclass(frozen=True)
class Contract:
    contract_id: str
    immobile_id: int
    contract_kind: Optional[str]
    start_date: Optional[datetime]
    durata_anni: Optional[int]
    arredato: Optional[bool]
    energy_class: Optional[str]
    canone_contrattuale_mensile: Optional[float]
