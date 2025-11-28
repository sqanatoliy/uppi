import pprint
import fitz
import camelot
import re


PDF_PATH = "downloads/CCMMRT71S44H501X/DOC_1926488153.pdf"


class VisuraParser:
    # -------------------------------
    #  REGEX PATTERNS
    # -------------------------------
    NAME_CF = re.compile(
        r"^([A-ZÀÈÌÒÙ]{2,})\s+([A-Za-zÀÈÌÒÙ]+)\s+\(CF:\s*([A-Z0-9]{16})\)",
        re.UNICODE
    )

    COMUNE_TABLE = re.compile(
        r"Immobili\s+siti\s+nel\s+Comune\s+di\s+(.+?)\s+\(Codice\s+([A-Z0-9]+)\)",
        re.IGNORECASE
    )

    SUPERFICIE_TOTALE = re.compile(r"Totale:\s*([0-9.,]+)")
    SUPERFICIE_ESCLUSE = re.compile(r"Totale escluse aree\s*scoperte\*\*:\s*([0-9.,]+)")

    GROUPED_HEADER_KEYWORDS = [
        "DATI IDENTIFICATIVI",
        "DATI DI CLASSAMENTO",
        "ALTRE INFORMAZIONI"
    ]

    #  Intestazione tables (owners)
    INTABULAZIONE_KEYWORDS = [
        "DATI ANAGRAFICI",
        "DIRITTI E ONERI REALI"
    ]

    # key columns to identify real estate tables
    REAL_ESTATE_COLUMNS = {"Foglio", "Numero", "Sub", "Categoria", "Classe"}

    # -------------------------------
    #  MAIN ENTRYPOINT
    # -------------------------------
    def parse(self, pdf_path):
        doc = fitz.open(pdf_path)
        name_data = self._extract_name_cf(doc)

        results = []

        for page_idx in range(len(doc)):
            comune_name, comune_code = self._extract_comune_for_page(doc[page_idx])

            tables = camelot.read_pdf(
                pdf_path,
                pages=str(page_idx + 1),
                flavor="lattice"
            )

            for table in tables:
                parsed = self._process_table(table)
                if parsed is None:
                    continue

                parsed.update({
                    "surname": name_data["surname"],
                    "given_name": name_data["given_name"],
                    "codice_fiscale": name_data["cf"],
                    "comune": comune_name,
                    "comune_code": comune_code
                })

                results.append(parsed)

        return results

    # -------------------------------
    #  NAME + CF ONLY FROM FIRST PAGE
    # -------------------------------
    def _extract_name_cf(self, doc):
        """Extract surname, given name and codice fiscale from the first page."""
        page = doc[0]
        blocks = page.get_text("blocks")

        surname = given_name = cf = None

        for _, _, _, _, text, *_ in blocks:
            for line in text.splitlines():
                m = self.NAME_CF.match(line.strip())
                if m:
                    surname, given_name, cf = m.groups()
                    return {
                        "surname": surname,
                        "given_name": given_name,
                        "cf": cf
                    }

        return {"surname": None, "given_name": None, "cf": None}

    # --------------------------------------
    #  EXACT "Immobili siti nel Comune di ..."
    # --------------------------------------
    def _extract_comune_for_page(self, page):
        """Extract Comune name and code from the page."""
        txt = page.get_text("text")

        for line in txt.splitlines():
            line = line.strip()
            m = self.COMUNE_TABLE.search(line)
            if m:
                return m.group(1), m.group(2)

        return None, None

    # -------------------------------
    #  TABLE PROCESSING
    # -------------------------------
    def _process_table(self, table):
        df = table.df

        # --- 1) detect grouped header (DATI IDENTIFICATIVI etc)
        first_row_text = " ".join(df.iloc[0]).upper()
        if any(k in first_row_text for k in self.GROUPED_HEADER_KEYWORDS):
            header_row = 1
            data_start_row = 2
        else:
            header_row = 0
            data_start_row = 1

        # extract preliminary header
        header = list(df.iloc[header_row])

        # --- 2) normalize header (remove newlines)
        header = [h.replace("\n", " ").strip() for h in header]

        # --- 3) detect & skip Intestazione tables (owners)
        header_join = " ".join(header).upper()
        if any(k in header_join for k in self.INTABULAZIONE_KEYWORDS):
            return None

        # --- 4) detect if table is real estate table
        if not any(col in header for col in self.REAL_ESTATE_COLUMNS):
            return None

        # --- 5) FIX merged headers: Classe + Consistenza
        normalized_header = []
        for idx, col in enumerate(header):
            col_clean = col.replace("\n", " ").strip()

            # Fix empty first column → N.
            if col_clean == "" and idx == 0:
                normalized_header.append("N.")
                continue

            # Fix empty column at Classe position
            if col_clean == "" and idx == 8:
                normalized_header.append("Classe")
                continue

            # Fix merged Classe Consistenza
            if col_clean == "Classe Consistenza":
                normalized_header.append("Consistenza")
                continue

            normalized_header.append(col_clean)

        header = normalized_header

        # --- 6) extract rows
        rows = []
        for i in range(data_start_row, len(df)):
            row_dict = {}

            for col_index, col_name in enumerate(header):
                raw_value = df.iloc[i, col_index].strip()

                # Superficie
                if col_name == "Superficie Catastale":
                    parsed = self._parse_superficie(raw_value)
                    row_dict.update(parsed)
                    continue

                row_dict[col_name] = raw_value

            rows.append(row_dict)

        return {
            # "columns": header,
            "rows": rows
        }

    # -------------------------------
    #  SUPERFICIE PARSER
    # -------------------------------
    def _parse_superficie(self, text):
        """Витягує Totale та Totale escluse aree scoperte."""
        txt = text.replace("\n", " ")

        totale = None
        escluse = None

        m1 = self.SUPERFICIE_TOTALE.search(txt)
        if m1:
            totale = self._normalize_number(m1.group(1))

        m2 = self.SUPERFICIE_ESCLUSE.search(txt)
        if m2:
            escluse = self._normalize_number(m2.group(1))

        return {
            "superficie_totale": totale,
            "superficie_escluse": escluse,
            "superficie_raw": txt
        }

    def _normalize_number(self, numstr):
        return float(numstr.replace(",", ".").replace(" ", ""))


# --------------------------------------------------
#  Example usage
# --------------------------------------------------
if __name__ == "__main__":
    parser = VisuraParser()
    data = parser.parse(PDF_PATH)
    for t in data:
        pprint.pp(t)
