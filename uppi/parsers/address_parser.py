from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class ParsedAddress:
    indirizzo_raw: str
    via_type: Optional[str]
    via_name: Optional[str]
    civico: Optional[str]
    scala: Optional[str]
    interno: Optional[str]
    piano: Optional[str]


_STREET_TYPE_REGEX = re.compile(
    r"^(VIA|VIALE|PIAZZA|P\.?ZZA|CORSO|STRADA|VICOLO|LARGO|BORGO|LOCALITÀ|LOC\.?|FRAZIONE|FRAZ\.?|CONTRADA)\s+(.+)",
    re.IGNORECASE | re.UNICODE,
)

_COMPONENT_PATTERNS = {
    "civico": re.compile(r"\b(?:n\.?|num\.?|numero)?\s*([\d]+[A-Z]?[-/\dA-Z]*|SNC)\b", re.IGNORECASE),
    "scala": re.compile(r"\b(?:SCALA|SC\.?)\s*([A-Z0-9]+)\b", re.IGNORECASE),
    "interno": re.compile(r"\b(?:INTERNO|INT\.?)\s*([A-Z0-9]+)\b", re.IGNORECASE),
    "piano": re.compile(r"\b(?:PIANO|P\.)\s*([-A-Z0-9°]+|T|TERRA|RIALZATO|AMMEZZATO|S\d)\b", re.IGNORECASE),
}

_NOISE_SPLIT = re.compile(r"\s{2,}|Variazione|Annotazione|Aggiornamento", re.IGNORECASE)


def _strip_noise(text: str) -> str:
    return _NOISE_SPLIT.split(text, maxsplit=1)[0].strip()


def parse_address(raw: str) -> ParsedAddress:
    normalized = " ".join((raw or "").replace("\n", " ").split()).strip()

    components = {
        "civico": None,
        "scala": None,
        "interno": None,
        "piano": None,
    }

    working = normalized
    for key, pattern in _COMPONENT_PATTERNS.items():
        match = pattern.search(working)
        if match:
            components[key] = match.group(1).strip()
            working = (working[: match.start()] + " " + working[match.end() :]).strip()

    working = _strip_noise(working)

    via_type = None
    via_name = None

    if working:
        match = _STREET_TYPE_REGEX.match(working)
        if match:
            via_type = match.group(1).strip().upper()
            via_name = _strip_noise(match.group(2)).strip()
        else:
            via_name = working

    return ParsedAddress(
        indirizzo_raw=normalized,
        via_type=via_type,
        via_name=via_name,
        civico=components["civico"],
        scala=components["scala"],
        interno=components["interno"],
        piano=components["piano"],
    )
