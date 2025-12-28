"""Utils for file storage and path management."""

from __future__ import annotations

from pathlib import Path
import logging

from .immobile import Immobile

logger = logging.getLogger(__name__)

DOWNLOADS_DIR = Path(__file__).resolve().parents[2] / "downloads"


def slugify_immobile(imm: Immobile) -> str:
    """
    Створює стабільний slug для Immobile на основі кадастрових атрибутів.
    """
    parts = []

    if imm.foglio:
        parts.append(f"F{imm.foglio}")
    if imm.numero:
        parts.append(f"N{imm.numero}")
    if imm.sub:
        parts.append(f"S{imm.sub}")
    if imm.zona_cens:
        parts.append(f"Z{imm.zona_cens}")
    if imm.micro_zona:
        parts.append(f"MZ{imm.micro_zona}")
    if imm.categoria:
        parts.append(f"CAT{imm.categoria.replace('/', '')}")
    if imm.classe:
        parts.append(f"CL{imm.classe}")
    if imm.consistenza:
        parts.append(f"CONS{imm.consistenza}")

    slug = "_".join(parts) if parts else "IMMOBILE"
    logger.debug("[STORAGE] slugify_immobile → %s", slug)
    return slug


def get_client_dir(cf: str) -> Path:
    """Повертає шлях до каталогу для заданого CF та створює його за потреби."""
    client_dir = DOWNLOADS_DIR / cf
    client_dir.mkdir(parents=True, exist_ok=True)
    logger.debug("[STORAGE] get_client_dir(%s) → %s", cf, client_dir)
    return client_dir


def get_visura_path(cf: str) -> Path:
    """Шлях до файлу VISURA_<cf>.pdf у каталозі клієнта."""
    path = get_client_dir(cf) / f"VISURA_{cf}.pdf"
    logger.debug("[STORAGE] get_visura_path(%s) → %s", cf, path)
    return path


def get_attestazione_path(cf: str, contract_id: str, imm: Immobile) -> Path:
    """
    Шлях до файлу ATTESTAZIONE_<cf>_<contract_id>_<slug>.docx у каталозі клієнта.
    contract_id додаємо, щоб уникнути колізій (1 immobile -> N contracts).
    """
    slug = slugify_immobile(imm)
    safe_contract_id = str(contract_id).replace("/", "_")
    path = get_client_dir(cf) / f"ATTESTAZIONE_{cf}_{safe_contract_id}_{slug}.docx"
    logger.debug("[STORAGE] get_attestazione_path(%s, %s, ...) → %s", cf, contract_id, path)
    return path
