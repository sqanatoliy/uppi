"""
Маппер полів з clients.yml у структуру UppiItem.

Бере сирий dict з YAML (ключі типу LOCATORE_CF, COMUNE, A1, B2, ...)
і перетворює його у нормалізований dict під UppiItem
(locatore_cf, comune, tipo_catasto, a1, b2, ...).

Невідомі ключі не викидаємо, а складаємо в extra.
"""

from typing import Any, Dict, Mapping
import logging

logger = logging.getLogger(__name__)

# Мапа: YAML-ключ (UPPERCASE) → поле UppiItem
YAML_TO_ITEM_MAP: Dict[str, str] = {
    # Базові / SISTER
    "LOCATORE_CF": "locatore_cf",
    "FORCE_UPDATE_VISURA": "force_update_visura",

    "COMUNE": "comune",
    "TIPO_CATASTO": "tipo_catasto",
    "UFFICIO_PROVINCIALE_LABEL": "ufficio_label",
    "UFFICIO_LABEL": "ufficio_label",

    # Адреса локатора
    "LOCATORE_COMUNE_RES": "locatore_comune_res",
    "LOCATORE_VIA": "locatore_via",
    "LOCATORE_CIVICO": "locatore_civico",

    # Дані по договору / шаблону
    "CONTRATTO_DATA": "contratto_data",

    "CONDUTTORE_NOME": "conduttore_nome",
    "CONDUTTORE_CF": "conduttore_cf",
    "CONDUTTORE_COMUNE": "conduttore_comune",
    "CONDUTTORE_VIA": "conduttore_via",

    "DECORRENZA_DATA": "decorrenza_data",
    "REGISTRAZIONE_DATA": "registrazione_data",
    "REGISTRAZIONE_NUM": "registrazione_num",
    "AGENZIA_ENTRATE_SEDE": "agenzia_entrate_sede",

    # Опціональні поля нерухомості з YAML (якщо руками щось задаєш)
    "IMMOBILE_COMUNE": "immobile_comune",
    "IMMOBILE_VIA": "immobile_via",
    "IMMOBILE_CIVICO": "immobile_civico",
    "IMMOBILE_PIANO": "immobile_piano",
    "IMMOBILE_INTERNO": "immobile_interno",
    "FOGLIO": "foglio",
    "NUMERO": "numero",
    "SUB": "sub",
    "CATEGORIA": "categoria",
    "RENDITA": "rendita",
    "SUPERFICIE_TOTALE": "superficie_totale",

    # Поля для розрахунку канону (pescara2018.py): Тип договору, Мебльованість, Енергоклас, Фактичний canone з договору, Тривалість договору.
    "CONTRACT_KIND": "contract_kind",
    "ARREDATO": "arredato",
    "ENERGY_CLASS": "energy_class",
    "CANONE_CONTRATTUALE_MENSILE": "canone_contrattuale_mensile",
    "DURATA_ANNI": "durata_anni",
    "ISTAT": "istat",
    "IGNORE_SURCHARGES": "ignore_surcharges",

    # A/B/C/D — бізнес-логіка (pescara2018.py)
    "A1": "a1",
    "A2": "a2",
    "B1": "b1",
    "B2": "b2",
    "B3": "b3",
    "B4": "b4",
    "B5": "b5",
    "C1": "c1",
    "C2": "c2",
    "C3": "c3",
    "C4": "c4",
    "C5": "c5",
    "C6": "c6",
    "C7": "c7",
    "D1": "d1",
    "D2": "d2",
    "D3": "d3",
    "D4": "d4",
    "D5": "d5",
    "D6": "d6",
    "D7": "d7",
    "D8": "d8",
    "D9": "d9",
    "D10": "d10",
    "D11": "d11",
    "D12": "d12",
    "D13": "d13",
}

# Дефолти, якщо у YAML не задано
DEFAULTS: Dict[str, Any] = {
    "COMUNE": "PESCARA",
    "TIPO_CATASTO": "F",
    "UFFICIO_PROVINCIALE_LABEL": "PESCARA Territorio",
}


def map_yaml_to_item(client: Mapping[str, Any]) -> Dict[str, Any]:
    """
    Перетворює dict з clients.yml в dict під UppiItem.

    - Нормалізує ключі до UPPERCASE.
    - Перекладає відомі ключі через YAML_TO_ITEM_MAP.
    - Додає дефолти для COMUNE/TIPO_CATASTO/UFFICIO_PROVINCIALE_LABEL, якщо їх немає.
    - Невідомі ключі кладе в item["extra"].

    Повертає:
        dict, який потім передається в UppiItem(**mapped)
    """
    result: Dict[str, Any] = {}
    extra: Dict[str, Any] = {}

    for raw_key, value in client.items():
        if raw_key is None:
            continue

        key = str(raw_key).strip().upper()
        target = YAML_TO_ITEM_MAP.get(key)

        if target:
            result[target] = value
        else:
            # Невідомі ключі — в extra, але не кричимо в логах кожен раз
            extra[raw_key] = value

    # Дефолтні значення для важливих полів, якщо їх немає в YAML
    for yaml_key, default_value in DEFAULTS.items():
        item_field = YAML_TO_ITEM_MAP[yaml_key]
        if item_field not in result:
            result[item_field] = default_value

    # Переклад FORCE_UPDATE_VISURA в нормальний bool
    if "force_update_visura" in result:
        val = result["force_update_visura"]
        if isinstance(val, str):
            result["force_update_visura"] = val.strip().lower() in {
                "1",
                "true",
                "yes",
                "y",
                "on",
                "si",
                "sì",
            }
        else:
            result["force_update_visura"] = bool(val)

    if extra:
        # Складаємо усі невідомі поля в extra,
        # щоб вони не губились, але й не ламали Item.
        result.setdefault("extra", {}).update(extra)
        logger.debug(
            "map_yaml_to_item: unknown YAML keys stored in extra for LOCATORE_CF=%r: %s",
            result.get("locatore_cf"),
            list(extra.keys()),
        )

    return result