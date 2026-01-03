from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Dict, Optional


# ----------------------------
# Data model
# ----------------------------

@dataclass(frozen=True)
class AddressParts:
    """
    Нормалізоване представлення адреси.

    Всі поля, окрім indirizzo_raw, можуть бути None.
    via_num завжди рядок або None (ніколи int).
    """
    via_type: Optional[str]
    via_name: Optional[str]
    via_num: Optional[str]
    scala: Optional[str]
    interno: Optional[str]
    piano: Optional[str]
    indirizzo_raw: str

    def as_dict(self) -> Dict[str, Optional[str]]:
        return {
            "via_type": self.via_type,
            "via_name": self.via_name,
            "via_num": self.via_num,
            "scala": self.scala,
            "interno": self.interno,
            "piano": self.piano,
            "indirizzo_raw": self.indirizzo_raw,
        }


# ----------------------------
# Street types
# ----------------------------

_STREET_TYPES = {
    "VIA",
    "VIALE",
    "PIAZZA",
    "PZZA",
    "CORSO",
    "STRADA",
    "VICOLO",
    "LARGO",
    "BORGO",
    "LOCALITA",
    "LOCALITÀ",
    "LOC",
    "FRAZIONE",
    "FRAZ",
    "CONTRADA",
}

# Тип вулиці завжди на початку рядка
_STREET_TYPE_REGEX = re.compile(
    r"""
    ^
    (VIA|VIALE|PIAZZA|P\.?ZZA|CORSO|STRADA|VICOLO|LARGO|BORGO|
     LOCALITÀ|LOC\.?|LOCALITA|FRAZIONE|FRAZ\.?|CONTRADA)
    \s+
    (.+)
    $
    """,
    re.IGNORECASE | re.UNICODE | re.VERBOSE,
)


# ----------------------------
# Address components (tail only!)
# ----------------------------

_COMPONENT_PATTERNS = {
    "scala": re.compile(r"\b(?:SCALA|SC\.?)\s*([A-Z0-9]+)\b", re.IGNORECASE),
    "interno": re.compile(r"\b(?:INTERNO|INT\.?)\s*([A-Z0-9]+)\b", re.IGNORECASE),
    # P. або PIANO, але тільки як окремий токен
    "piano": re.compile(
        r"\b(?:PIANO|P\.)\s*(T|TERRA|RIALZATO|AMMEZZATO|S\d|[-A-Z0-9°]+)\b",
        re.IGNORECASE,
    ),
}


# ----------------------------
# Civico (house number)
# ----------------------------

# Числовий номер: група (1) ЗАВЖДИ номер
_CIVICO_REGEXES = [
    re.compile(
        r"\b(?:N\.?|NUM\.?|CIVICO)\s*([\d]+[A-Z]?([\-/\dA-Z]+)?)\b",
        re.IGNORECASE,
    ),
    re.compile(
        r"\b([\d]+[A-Z]?([\-/\dA-Z]+)?)\s*$",
        re.IGNORECASE,
    ),
]

# SNC = senza numero civico → номер відсутній
_SNC_REGEX = re.compile(r"\b(N\.?\s*)?SNC\b", re.IGNORECASE)


# ----------------------------
# Parser
# ----------------------------

def parse_address(text: str) -> AddressParts:
    """
    Парсить італійську адресу у структурований вигляд.

    Стратегія:
    1) Нормалізація тексту
    2) Визначення via_type + via_name (основа)
    3) Робота ТІЛЬКИ з хвостом (via_name)
    4) Обробка SNC (окремий семантичний кейс)
    5) Парсинг номера
    6) Парсинг scala / interno / piano
    """

    if not isinstance(text, str):
        raise TypeError("parse_address expects a string")

    raw = text.replace("\n", " ").strip()
    working = raw

    # ----------------------------
    # 1. Street type + base name
    # ----------------------------

    via_type: Optional[str] = None
    via_name: Optional[str] = None

    m = _STREET_TYPE_REGEX.match(working)
    if m:
        via_type = (
            m.group(1)
            .upper()
            .replace(".", "")
            .replace("PZZA", "PIAZZA")
        )
        via_name = m.group(2).strip()
    else:
        # Нема типу — вважаємо все назвою
        via_name = working

    # Працюємо далі ТІЛЬКИ з хвостом
    working_tail = via_name

    # ----------------------------
    # 2. Civico: SNC
    # ----------------------------

    via_num: Optional[str] = None

    if _SNC_REGEX.search(working_tail):
        # Явно вказано, що номера немає
        working_tail = _SNC_REGEX.sub(" ", working_tail).strip()
        via_num = None
    else:
        # ----------------------------
        # 3. Civico: numeric
        # ----------------------------
        for regex in _CIVICO_REGEXES:
            m = regex.search(working_tail)
            if not m:
                continue

            via_num = m.group(1)
            working_tail = regex.sub(" ", working_tail, count=1).strip()
            break

    # ----------------------------
    # 4. Other components
    # ----------------------------

    scala: Optional[str] = None
    interno: Optional[str] = None
    piano: Optional[str] = None

    for key, pattern in _COMPONENT_PATTERNS.items():
        m = pattern.search(working_tail)
        if not m:
            continue

        value = m.group(1).strip().upper()

        if key == "scala":
            scala = value
        elif key == "interno":
            interno = value
        elif key == "piano":
            piano = value

        working_tail = pattern.sub(" ", working_tail, count=1).strip()

    # ----------------------------
    # 5. Final cleanup
    # ----------------------------

    via_name = re.sub(r"\s{2,}", " ", working_tail).strip() or None

    return AddressParts(
        via_type=via_type,
        via_name=via_name,
        via_num=via_num,
        scala=scala,
        interno=interno,
        piano=piano,
        indirizzo_raw=raw,
    )