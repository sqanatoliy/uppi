from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List
import logging

import yaml

from uppi.config.clients import ClientConfig

logger = logging.getLogger(__name__)

CLIENTS_DIR = Path(__file__).resolve().parents[2] / "clients"
CLIENTS_FILE = CLIENTS_DIR / "clients.yml"

DEFAULT_COMUNE = "PESCARA"
DEFAULT_TIPO_CATASTO = "F"
DEFAULT_UFFICIO = "PESCARA Territorio"


def _parse_yaml(path: Path) -> List[Dict[str, Any]]:
    clients: List[Dict[str, Any]] = []

    if not path.exists():
        logger.error("[CLIENTS] Файл clients.yml не знайдено: %s", path)
        return clients

    try:
        with path.open("r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or []
    except Exception as e:
        logger.exception("[CLIENTS] Неможливо прочитати %s: %s", path, e)
        return clients

    if not isinstance(data, list):
        logger.error("[CLIENTS] Очікував список у %s, а отримав %r", path, type(data))
        return clients

    for raw in data:
        try:
            client_cfg = ClientConfig.from_raw(
                raw,
                default_comune=DEFAULT_COMUNE,
                default_tipo_catasto=DEFAULT_TIPO_CATASTO,
                default_ufficio_label=DEFAULT_UFFICIO,
            )
        except ValueError as e:
            logger.error("[CLIENTS] Некоректний запис у YAML: %s", e)
            continue
        except Exception as e:
            logger.exception("[CLIENTS] Неочікувана помилка при читанні YAML: %s", e)
            continue

        client_dict = client_cfg.to_item_dict()

        client_dict.update(client_cfg.extra)

        client_dict["LOCATORE_CF"] = client_cfg.locatore_cf
        client_dict["COMUNE"] = client_cfg.comune
        client_dict["TIPO_CATASTO"] = client_cfg.tipo_catasto
        client_dict["UFFICIO_PROVINCIALE_LABEL"] = client_cfg.ufficio_label
        client_dict["FORCE_UPDATE_VISURA"] = bool(client_cfg.force_update_visura)

        client_dict["locatore_cf"] = client_cfg.locatore_cf
        client_dict["comune"] = client_cfg.comune
        client_dict["tipo_catasto"] = client_cfg.tipo_catasto
        client_dict["ufficio_label"] = client_cfg.ufficio_label
        client_dict["force_update_visura"] = bool(client_cfg.force_update_visura)

        clients.append(client_dict)

    logger.info("[CLIENTS] Завантажено %d клієнтів із %s", len(clients), path)
    return clients


def load_clients() -> List[Dict[str, Any]]:
    return _parse_yaml(CLIENTS_FILE)
