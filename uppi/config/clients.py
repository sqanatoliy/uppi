from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict


@dataclass(frozen=True)
class ClientConfig:
    locatore_cf: str
    comune: str
    tipo_catasto: str
    ufficio_label: str
    force_update_visura: bool

    locatore_comune_res: str | None = None
    locatore_via: str | None = None
    locatore_civico: str | None = None

    immobile_comune: str | None = None
    immobile_via: str | None = None
    immobile_civico: str | None = None
    immobile_piano: str | None = None
    immobile_interno: str | None = None

    foglio: str | None = None
    numero: str | None = None
    sub: str | None = None
    rendita: str | None = None
    superficie_totale: str | None = None
    categoria: str | None = None

    contratto_data: str | None = None

    conduttore_nome: str | None = None
    conduttore_cf: str | None = None
    conduttore_comune: str | None = None
    conduttore_via: str | None = None

    decorrenza_data: str | None = None
    registrazione_data: str | None = None
    registrazione_num: str | None = None
    agenzia_entrate_sede: str | None = None

    contract_kind: str | None = None
    arredato: str | None = None
    energy_class: str | None = None
    canone_contrattuale_mensile: str | None = None
    durata_anni: str | None = None

    elements: Dict[str, Any] = field(default_factory=dict)
    extra: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_raw(
        cls,
        raw: Dict[str, Any],
        *,
        default_comune: str,
        default_tipo_catasto: str,
        default_ufficio_label: str,
    ) -> "ClientConfig":
        if not isinstance(raw, dict):
            raise ValueError("Client entry must be a mapping")

        locatore_cf = (raw.get("LOCATORE_CF") or raw.get("locatore_cf") or "").strip()
        if not locatore_cf:
            raise ValueError("LOCATORE_CF is required")

        force_update_raw = raw.get("FORCE_UPDATE_VISURA", raw.get("force_update_visura"))
        force_update = _parse_bool(force_update_raw)

        elements = {k.lower(): v for k, v in raw.items() if str(k).strip().upper() in _ELEMENT_KEYS}

        return cls(
            locatore_cf=locatore_cf,
            comune=str(raw.get("COMUNE", raw.get("comune", default_comune)) or default_comune),
            tipo_catasto=str(raw.get("TIPO_CATASTO", raw.get("tipo_catasto", default_tipo_catasto)) or default_tipo_catasto),
            ufficio_label=str(
                raw.get(
                    "UFFICIO_PROVINCIALE_LABEL",
                    raw.get("ufficio_label", default_ufficio_label),
                )
                or default_ufficio_label
            ),
            force_update_visura=force_update,
            locatore_comune_res=_opt_str(raw.get("LOCATORE_COMUNE_RES", raw.get("locatore_comune_res"))),
            locatore_via=_opt_str(raw.get("LOCATORE_VIA", raw.get("locatore_via"))),
            locatore_civico=_opt_str(raw.get("LOCATORE_CIVICO", raw.get("locatore_civico"))),
            immobile_comune=_opt_str(raw.get("IMMOBILE_COMUNE", raw.get("immobile_comune"))),
            immobile_via=_opt_str(raw.get("IMMOBILE_VIA", raw.get("immobile_via"))),
            immobile_civico=_opt_str(raw.get("IMMOBILE_CIVICO", raw.get("immobile_civico"))),
            immobile_piano=_opt_str(raw.get("IMMOBILE_PIANO", raw.get("immobile_piano"))),
            immobile_interno=_opt_str(raw.get("IMMOBILE_INTERNO", raw.get("immobile_interno"))),
            foglio=_opt_str(raw.get("FOGLIO", raw.get("foglio"))),
            numero=_opt_str(raw.get("NUMERO", raw.get("numero"))),
            sub=_opt_str(raw.get("SUB", raw.get("sub"))),
            rendita=_opt_str(raw.get("RENDITA", raw.get("rendita"))),
            superficie_totale=_opt_str(raw.get("SUPERFICIE_TOTALE", raw.get("superficie_totale"))),
            categoria=_opt_str(raw.get("CATEGORIA", raw.get("categoria"))),
            contratto_data=_opt_str(raw.get("CONTRATTO_DATA", raw.get("contratto_data"))),
            conduttore_nome=_opt_str(raw.get("CONDUTTORE_NOME", raw.get("conduttore_nome"))),
            conduttore_cf=_opt_str(raw.get("CONDUTTORE_CF", raw.get("conduttore_cf"))),
            conduttore_comune=_opt_str(raw.get("CONDUTTORE_COMUNE", raw.get("conduttore_comune"))),
            conduttore_via=_opt_str(raw.get("CONDUTTORE_VIA", raw.get("conduttore_via"))),
            decorrenza_data=_opt_str(raw.get("DECORRENZA_DATA", raw.get("decorrenza_data"))),
            registrazione_data=_opt_str(raw.get("REGISTRAZIONE_DATA", raw.get("registrazione_data"))),
            registrazione_num=_opt_str(raw.get("REGISTRAZIONE_NUM", raw.get("registrazione_num"))),
            agenzia_entrate_sede=_opt_str(raw.get("AGENZIA_ENTRATE_SEDE", raw.get("agenzia_entrate_sede"))),
            contract_kind=_opt_str(raw.get("CONTRACT_KIND", raw.get("contract_kind"))),
            arredato=_opt_str(raw.get("ARREDATO", raw.get("arredato"))),
            energy_class=_opt_str(raw.get("ENERGY_CLASS", raw.get("energy_class"))),
            canone_contrattuale_mensile=_opt_str(
                raw.get("CANONE_CONTRATTUALE_MENSILE", raw.get("canone_contrattuale_mensile"))
            ),
            durata_anni=_opt_str(raw.get("DURATA_ANNI", raw.get("durata_anni"))),
            elements=elements,
            extra=_extract_extra(raw),
        )

    def to_item_dict(self) -> Dict[str, Any]:
        base = {
            "locatore_cf": self.locatore_cf,
            "comune": self.comune,
            "tipo_catasto": self.tipo_catasto,
            "ufficio_label": self.ufficio_label,
            "force_update_visura": self.force_update_visura,
            "locatore_comune_res": self.locatore_comune_res,
            "locatore_via": self.locatore_via,
            "locatore_civico": self.locatore_civico,
            "immobile_comune": self.immobile_comune,
            "immobile_via": self.immobile_via,
            "immobile_civico": self.immobile_civico,
            "immobile_piano": self.immobile_piano,
            "immobile_interno": self.immobile_interno,
            "foglio": self.foglio,
            "numero": self.numero,
            "sub": self.sub,
            "rendita": self.rendita,
            "superficie_totale": self.superficie_totale,
            "categoria": self.categoria,
            "contratto_data": self.contratto_data,
            "conduttore_nome": self.conduttore_nome,
            "conduttore_cf": self.conduttore_cf,
            "conduttore_comune": self.conduttore_comune,
            "conduttore_via": self.conduttore_via,
            "decorrenza_data": self.decorrenza_data,
            "registrazione_data": self.registrazione_data,
            "registrazione_num": self.registrazione_num,
            "agenzia_entrate_sede": self.agenzia_entrate_sede,
            "contract_kind": self.contract_kind,
            "arredato": self.arredato,
            "energy_class": self.energy_class,
            "canone_contrattuale_mensile": self.canone_contrattuale_mensile,
            "durata_anni": self.durata_anni,
        }
        base.update(self.elements)
        if self.extra:
            base.setdefault("extra", {}).update(self.extra)
        return base


def _opt_str(value: Any) -> str | None:
    if value is None:
        return None
    value = str(value).strip()
    return value or None


def _parse_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() in {"1", "true", "yes", "y", "t", "on", "si", "sì"}


def _extract_extra(raw: Dict[str, Any]) -> Dict[str, Any]:
    known = {k.upper() for k in _BASE_KEYS} | _ELEMENT_KEYS
    return {k: v for k, v in raw.items() if str(k).strip().upper() not in known}


_BASE_KEYS = {
    "LOCATORE_CF",
    "COMUNE",
    "TIPO_CATASTO",
    "UFFICIO_PROVINCIALE_LABEL",
    "FORCE_UPDATE_VISURA",
    "LOCATORE_COMUNE_RES",
    "LOCATORE_VIA",
    "LOCATORE_CIVICO",
    "IMMOBILE_COMUNE",
    "IMMOBILE_VIA",
    "IMMOBILE_CIVICO",
    "IMMOBILE_PIANO",
    "IMMOBILE_INTERNO",
    "FOGLIO",
    "NUMERO",
    "SUB",
    "RENDITA",
    "SUPERFICIE_TOTALE",
    "CATEGORIA",
    "CONTRATTO_DATA",
    "CONDUTTORE_NOME",
    "CONDUTTORE_CF",
    "CONDUTTORE_COMUNE",
    "CONDUTTORE_VIA",
    "DECORRENZA_DATA",
    "REGISTRAZIONE_DATA",
    "REGISTRAZIONE_NUM",
    "AGENZIA_ENTRATE_SEDE",
    "CONTRACT_KIND",
    "ARREDATO",
    "ENERGY_CLASS",
    "CANONE_CONTRATTUALE_MENSILE",
    "DURATA_ANNI",
}

_ELEMENT_KEYS = {f"{prefix}{num}" for prefix in ["A", "B", "C", "D"] for num in range(1, 14)}
