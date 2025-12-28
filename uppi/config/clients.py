from __future__ import annotations

from dataclasses import asdict
from pathlib import Path
from typing import Any, Dict, List

import logging
import yaml
from decouple import config

from uppi.domain.models import ClientConfig
from uppi.utils.item_mapper import YAML_TO_ITEM_MAP

logger = logging.getLogger(__name__)


CLIENTS_YAML = Path(config("UPPI_CLIENTS_YAML", default="clients/clients.yml"))


def _normalize_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on", "si", "sì"}


def parse_client_row(raw: Dict[str, Any]) -> ClientConfig:
    mapped: Dict[str, Any] = {}
    extra: Dict[str, Any] = {}

    for raw_key, value in raw.items():
        if raw_key is None:
            continue
        key = str(raw_key).strip().upper()
        target = YAML_TO_ITEM_MAP.get(key)
        if target:
            mapped[target] = value
        else:
            extra[raw_key] = value

    mapped.setdefault("comune", ClientConfig.comune)
    mapped.setdefault("tipo_catasto", ClientConfig.tipo_catasto)
    mapped.setdefault("ufficio_label", ClientConfig.ufficio_label)

    locatore_cf = str(mapped.get("locatore_cf") or "").strip()
    if not locatore_cf:
        raise ValueError("LOCATORE_CF is required")

    force = _normalize_bool(mapped.get("force_update_visura"))

    return ClientConfig(
        locatore_cf=locatore_cf,
        force_update_visura=force,
        comune=str(mapped.get("comune") or ClientConfig.comune),
        tipo_catasto=str(mapped.get("tipo_catasto") or ClientConfig.tipo_catasto),
        ufficio_label=str(mapped.get("ufficio_label") or ClientConfig.ufficio_label),
        locatore_comune_res=mapped.get("locatore_comune_res"),
        locatore_via=mapped.get("locatore_via"),
        locatore_civico=mapped.get("locatore_civico"),
        immobile_comune=mapped.get("immobile_comune"),
        immobile_via=mapped.get("immobile_via"),
        immobile_civico=mapped.get("immobile_civico"),
        immobile_piano=mapped.get("immobile_piano"),
        immobile_interno=mapped.get("immobile_interno"),
        foglio=mapped.get("foglio"),
        numero=mapped.get("numero"),
        sub=mapped.get("sub"),
        rendita=mapped.get("rendita"),
        superficie_totale=mapped.get("superficie_totale"),
        categoria=mapped.get("categoria"),
        contratto_data=mapped.get("contratto_data"),
        conduttore_nome=mapped.get("conduttore_nome"),
        conduttore_cf=mapped.get("conduttore_cf"),
        conduttore_comune=mapped.get("conduttore_comune"),
        conduttore_via=mapped.get("conduttore_via"),
        decorrenza_data=mapped.get("decorrenza_data"),
        registrazione_data=mapped.get("registrazione_data"),
        registrazione_num=mapped.get("registrazione_num"),
        agenzia_entrate_sede=mapped.get("agenzia_entrate_sede"),
        contract_kind=mapped.get("contract_kind"),
        arredato=mapped.get("arredato"),
        energy_class=mapped.get("energy_class"),
        canone_contrattuale_mensile=mapped.get("canone_contrattuale_mensile"),
        durata_anni=mapped.get("durata_anni"),
        a1=mapped.get("a1"),
        a2=mapped.get("a2"),
        b1=mapped.get("b1"),
        b2=mapped.get("b2"),
        b3=mapped.get("b3"),
        b4=mapped.get("b4"),
        b5=mapped.get("b5"),
        c1=mapped.get("c1"),
        c2=mapped.get("c2"),
        c3=mapped.get("c3"),
        c4=mapped.get("c4"),
        c5=mapped.get("c5"),
        c6=mapped.get("c6"),
        c7=mapped.get("c7"),
        d1=mapped.get("d1"),
        d2=mapped.get("d2"),
        d3=mapped.get("d3"),
        d4=mapped.get("d4"),
        d5=mapped.get("d5"),
        d6=mapped.get("d6"),
        d7=mapped.get("d7"),
        d8=mapped.get("d8"),
        d9=mapped.get("d9"),
        d10=mapped.get("d10"),
        d11=mapped.get("d11"),
        d12=mapped.get("d12"),
        d13=mapped.get("d13"),
        extra=extra,
    )


def load_clients() -> List[ClientConfig]:
    if not CLIENTS_YAML.exists():
        logger.error("[CLIENTS] Файл clients.yml не знайдено: %s", CLIENTS_YAML)
        return []

    try:
        with CLIENTS_YAML.open("r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or []
    except Exception as exc:
        logger.exception("[CLIENTS] Неможливо прочитати %s: %s", CLIENTS_YAML, exc)
        return []

    if not isinstance(data, list):
        logger.error("[CLIENTS] Некоректна структура YAML (очікується список): %s", CLIENTS_YAML)
        return []

    clients: List[ClientConfig] = []
    for row in data:
        if not isinstance(row, dict):
            continue
        try:
            clients.append(parse_client_row(row))
        except Exception as exc:
            logger.warning("[CLIENTS] Пропущено запис через помилку: %s", exc)

    return clients


def client_to_item_payload(client: ClientConfig) -> Dict[str, Any]:
    payload = asdict(client)
    payload["LOCATORE_CF"] = client.locatore_cf
    payload["FORCE_UPDATE_VISURA"] = client.force_update_visura
    payload["COMUNE"] = client.comune
    payload["TIPO_CATASTO"] = client.tipo_catasto
    payload["UFFICIO_PROVINCIALE_LABEL"] = client.ufficio_label
    return payload
