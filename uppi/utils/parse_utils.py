# uppi/utils/parse_utils.py
from __future__ import annotations
from decimal import Decimal
from enum import Enum

import re
from datetime import date, datetime
from typing import Any, Optional


def clean_str(v: Any) -> Optional[str]:
    if v is None:
        return None
    s = str(v).replace("\n", " ").strip()
    return s if s != "" else None

def clean_sub(v: Any) -> str:
    """sub з visura в БД у нас канонічно не NULL. None/порожнє -> ''."""
    if v is None:
        return ""
    s = str(v).replace("\n", " ").strip()
    return s  # може бути ''

def safe_float(v: Any) -> Optional[float]:
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return float(v)
    s = str(v).strip()
    if s == "":
        return None
    # кома як десятковий роздільник
    s = s.replace(" ", "").replace(",", ".")
    try:
        return float(s)
    except ValueError:
        return None


def to_bool_or_none(v: Any) -> Optional[bool]:
    if v is None:
        return None
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        return bool(v)
    s = str(v).strip().lower()
    if s in ("true", "yes", "y", "1"):
        return True
    if s in ("false", "no", "n", "0"):
        return False
    return None


def parse_date(v: Any) -> Optional[date]:
    """
    Підтримка:
      - YYYY-MM-DD
      - DD/MM/YYYY
    """
    s = clean_str(v)
    if not s:
        return None

    if re.match(r"^\d{4}-\d{2}-\d{2}$", s):
        try:
            y, m, d = s.split("-")
            return date(int(y), int(m), int(d))
        except Exception:
            return None

    if re.match(r"^\d{2}/\d{2}/\d{4}$", s):
        try:
            d, m, y = s.split("/")
            return date(int(y), int(m), int(d))
        except Exception:
            return None

    return None


# JSON encoder that converts Decimal and Enum to JSON-serializable formats
def prepare_for_json(obj):
    """Рекурсивно конвертує Decimal та Enum у формати, придатні для JSON."""
    if isinstance(obj, dict):
        return {k: prepare_for_json(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [prepare_for_json(i) for i in obj]
    elif isinstance(obj, Decimal):
        return float(obj)
    elif isinstance(obj, Enum):
        return obj.value
    elif isinstance(obj, (date, datetime)):
        return obj.isoformat()
    return obj


# Розділення повного імені на Прізвище та Ім'я
def split_full_name(full_name: str) -> tuple[str, str]:
    """
    Розділяє повне ім'я на Прізвище та Ім'я.
    Припускає формат: 'Прізвище Ім'я' або 'Прізвище Прізвище2 Ім'я'.
    Повертає кортеж (surname, name).
    """
    if not full_name:
        return "", ""
    
    parts = full_name.strip().split()
    if len(parts) == 0:
        return "", ""
    if len(parts) == 1:
        return parts[0], "" # Тільки прізвище
        
    # Найчастіше в документах першим йде Прізвище (Cognome), потім Ім'я (Nome)
    # Ми беремо перше слово як прізвище, решту як ім'я. 
    # Або навпаки, залежно від вашого стандарту.
    return parts[0], " ".join(parts[1:])