from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Dict, Optional


@dataclass(frozen=True)
class AddressParts:
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
            "civico": self.via_num,
        }


_STREET_TYPES = {
    "VIA",
    "VIALE",
    "PIAZZA",
    "P.ZZA",
    "PZZA",
    "CORSO",
    "STRADA",
    "VICOLO",
    "LARGO",
    "BORGO",
    "LOCALITA",
    "LOCALITÀ",
    "LOC.",
    "FRAZIONE",
    "FRAZ.",
    "CONTRADA",
}

_STREET_TYPE_REGEX = re.compile(
    r"^(VIA|VIALE|PIAZZA|P\.?ZZA|CORSO|STRADA|VICOLO|LARGO|BORGO|LOCALITÀ|LOC\.?|FRAZIONE|FRAZ\.?|CONTRADA)\s+(.+)$",
    re.IGNORECASE | re.UNICODE,
)

_COMPONENT_PATTERNS = {
    "scala": re.compile(r"\b(SCALA|SC\.?)[\s:]*([A-Z0-9]+)\b", re.IGNORECASE),
    "interno": re.compile(r"\b(INTERNO|INT\.?)[\s:]*([A-Z0-9]+)\b", re.IGNORECASE),
    "piano": re.compile(
        r"\b(PIANO|P\.?)[\s:]*([-A-Z0-9°]+|T|TERRA|RIALZATO|AMMEZZATO|S\d)\b",
        re.IGNORECASE,
    ),
}

_CIVICO_REGEXES = [
    re.compile(r"\b(N\.?|NUM\.?|CIVICO)\s*([\d]+[A-Z]?([\-/\dA-Z]+)?)\b", re.IGNORECASE),
    re.compile(r"\b([\d]+[A-Z]?([\-/\dA-Z]+)?)\s*(?:SNC)?$", re.IGNORECASE),
]


def parse_address(text: str) -> AddressParts:
    raw = text.replace("\n", " ").strip()
    working = raw

    via_num = None
    scala = None
    interno = None
    piano = None

    for key, pattern in _COMPONENT_PATTERNS.items():
        match = pattern.search(working)
        if not match:
            continue
        value = match.group(2).strip()
        if key == "scala":
            scala = value
        elif key == "interno":
            interno = value
        elif key == "piano":
            piano = value
        working = pattern.sub(" ", working, count=1).strip()

    for regex in _CIVICO_REGEXES:
        match = regex.search(working)
        if match:
            via_num = match.group(2).strip()
            working = regex.sub(" ", working, count=1).strip()
            break

    working = re.sub(r"\bSNC\b", " ", working, flags=re.IGNORECASE).strip()
    working = re.sub(r"\s{2,}", " ", working).strip()

    via_type = None
    via_name = None

    match = _STREET_TYPE_REGEX.match(working)
    if match:
        via_type = match.group(1).strip().upper().replace("P.ZZA", "PIAZZA")
        via_name = match.group(2).strip()
    elif working:
        via_name = working

    if via_type and via_type not in _STREET_TYPES:
        via_type = via_type.replace(".", "")

    return AddressParts(
        via_type=via_type,
        via_name=via_name,
        via_num=via_num,
        scala=scala,
        interno=interno,
        piano=piano,
        indirizzo_raw=raw,
    )
