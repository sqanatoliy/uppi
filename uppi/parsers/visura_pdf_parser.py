from __future__ import annotations

from pathlib import Path
import logging
import re
from typing import Any, Dict, List

import fitz
import camelot

from uppi.parsers.address_parser import parse_address

logger = logging.getLogger(__name__)

DOWNLOADS_DIR = Path(__file__).resolve().parents[2] / "downloads"

PDF_PATH = DOWNLOADS_DIR / "sample_visura.pdf"


class VisuraParser:
    """
    Парсер PDF-візури.

    На виході повертає список dict'ів, один dict = один immobile.
    Ключі максимально близькі до колонок таблиці public.immobili:
        - foglio, numero, sub, categoria, classe, consistenza, rendita, zona_cens, micro_zona
        - superficie_totale, superficie_escluse, superficie_raw
        - via_type, via_name, via_num, scala, interno, piano, indirizzo_raw, dati_ulteriori
        - locatore_* та immobile_comune/immobile_comune_code (для збагачення)
    """

    NAME_CF = re.compile(
        r"^([A-ZÀÈÌÒÙ]{2,})\s+([A-Za-zÀÈÌÒÙ]+)\s+\(CF:\s*([A-Z0-9]{16})\)",
        re.UNICODE,
    )

    COMUNE_TABLE = re.compile(
        r"Immobili\s+siti\s+nel\s+Comune\s+di\s+(.+?)\s+\(Codice\s+([A-Z0-9]+)\)",
        re.IGNORECASE,
    )

    SUPERFICIE_TOTALE = re.compile(r"Totale:\s*([0-9.,]+)")
    SUPERFICIE_ESCLUSE = re.compile(r"Totale escluse aree\s*scoperte\*\*:\s*([0-9.,]+)")

    GROUPED_HEADER_KEYWORDS = ["DATI IDENTIFICATIVI", "DATI DI CLASSAMENTO", "ALTRE INFORMAZIONI"]
    INTABULAZIONE_KEYWORDS = ["DATI ANAGRAFICI", "DIRITTI E ONERI REALI"]

    REAL_ESTATE_COLUMNS = {"Foglio", "Numero", "Sub", "Categoria", "Classe"}

    def parse(self, pdf_path: str | Path) -> List[Dict[str, Any]]:
        pdf_path = str(pdf_path)
        logger.info("[VISURA_PARSER] Парсимо PDF: %s", pdf_path)

        try:
            doc = fitz.open(pdf_path)
        except Exception as e:
            logger.exception("[VISURA_PARSER] Не вдалося відкрити PDF %s: %s", pdf_path, e)
            return []

        all_immobili: List[Dict[str, Any]] = []
        try:
            name_data = self._extract_name_cf(doc)

            for page_idx in range(len(doc)):
                page = doc[page_idx]
                comune_name, comune_code = self._extract_comune_for_page(page)

                try:
                    tables = camelot.read_pdf(pdf_path, pages=str(page_idx + 1), flavor="lattice")
                except Exception as e:
                    logger.exception(
                        "[VISURA_PARSER] Помилка Camelot на сторінці %d (%s): %s",
                        page_idx + 1,
                        pdf_path,
                        e,
                    )
                    continue

                for table in tables:
                    parsed = self._process_table(table)
                    if parsed is None:
                        continue

                    immobili_list = parsed.get("immobili", [])
                    for immobile in immobili_list:
                        immobile.update(
                            {
                                "locatore_surname": name_data.get("locatore_surname"),
                                "locatore_name": name_data.get("locatore_name"),
                                "locatore_codice_fiscale": name_data.get("cf"),
                                "immobile_comune": comune_name,
                                "immobile_comune_code": comune_code,
                            }
                        )
                        all_immobili.append(immobile)

            logger.info("[VISURA_PARSER] Готово: знайдено %d immobili у %s", len(all_immobili), pdf_path)
            logger.info(f"All immobili information is: {all_immobili}")
            return all_immobili
        finally:
            doc.close()

    def _normalize_header(self, header: str) -> str:
        snake = re.sub(r"[^A-Za-z0-9]+", "_", header).strip("_").lower()

        if snake == "microzona":
            return "micro_zona"
        if snake in ("zona_cens", "zona_censuaria"):
            return "zona_cens"
        if snake == "sez_urb":
            return "sez_urbana"

        return snake

    def _extract_name_cf(self, doc) -> Dict[str, Any]:
        page = doc[0]
        blocks = page.get_text("blocks")

        for _, _, _, _, text, *_ in blocks:
            for line in text.splitlines():
                m = self.NAME_CF.match(line.strip())
                if m:
                    locatore_surname, locatore_name, cf = m.groups()
                    return {"locatore_surname": locatore_surname, "locatore_name": locatore_name, "cf": cf}

        logger.warning("[VISURA_PARSER] Не вдалося знайти ім'я/CF на першій сторінці")
        return {"locatore_surname": None, "locatore_name": None, "cf": None}

    def _extract_comune_for_page(self, page) -> tuple[str | None, str | None]:
        txt = page.get_text("text")
        for line in txt.splitlines():
            m = self.COMUNE_TABLE.search(line.strip())
            if m:
                return m.group(1), m.group(2)
        return None, None

    def _process_table(self, table):
        df = table.df
        if df.empty:
            return None

        first_row_text = " ".join(df.iloc[0]).upper()
        if any(k in first_row_text for k in self.GROUPED_HEADER_KEYWORDS):
            header_row = 1
            data_start_row = 2
        else:
            header_row = 0
            data_start_row = 1

        header = [h.replace("\n", " ").strip() for h in list(df.iloc[header_row])]
        header_join = " ".join(header).upper()
        if any(k in header_join for k in self.INTABULAZIONE_KEYWORDS):
            return None

        if not any(col in header for col in self.REAL_ESTATE_COLUMNS):
            return None

        normalized_header: List[str] = []
        for idx, col in enumerate(header):
            col_clean = col.replace("\n", " ").strip()

            if col_clean == "" and idx == 0:
                normalized_header.append("table_num_immobile")
                continue

            if col_clean == "" and idx == 8:
                normalized_header.append("classe")
                continue

            if col_clean == "Classe Consistenza":
                normalized_header.append("consistenza")
                continue

            normalized_header.append(col_clean)

        header = [self._normalize_header(col) for col in normalized_header]

        rows: List[Dict[str, Any]] = []
        for i in range(data_start_row, len(df)):
            row_dict: Dict[str, Any] = {}

            for col_index, col_name in enumerate(header):
                raw_value = df.iloc[i, col_index].strip()

                if "indirizzo" in col_name:
                    row_dict.update(parse_address(raw_value).as_dict())
                    continue

                if col_name == "superficie_catastale":
                    row_dict.update(self._parse_superficie(raw_value))
                    continue

                if col_name == "rendita":
                    row_dict.update(self._parse_rendita(raw_value))
                    continue

                row_dict[col_name] = raw_value

            rows.append(row_dict)

        return {"immobili": rows} if rows else None

    def _parse_superficie(self, text: str) -> Dict[str, Any]:
        txt = text.replace("\n", " ")
        totale = None
        escluse = None

        m1 = self.SUPERFICIE_TOTALE.search(txt)
        if m1:
            totale = self._normalize_number(m1.group(1))

        m2 = self.SUPERFICIE_ESCLUSE.search(txt)
        if m2:
            escluse = self._normalize_number(m2.group(1))

        return {"superficie_totale": totale, "superficie_escluse": escluse, "superficie_raw": txt}

    def _parse_rendita(self, text: str) -> dict[str, str] | None:
        text = text.replace("Euro", "").strip()

        if not text:
            return None
        return {"rendita": f"€ {self._normalize_number(text)}"}

    def _normalize_number(self, numstr: str) -> float:
        return float(numstr.replace(",", ".").replace(" ", ""))


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    parser = VisuraParser()
    data = parser.parse(PDF_PATH)
    for t in data:
        print(t)
