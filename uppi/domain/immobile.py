from dataclasses import dataclass
from typing import Optional

@dataclass
class Immobile:
    # --- З таблиці нерухомості у візурі ---
    table_num_immobile: str | None = None
    sez_urbana: str | None = None
    foglio: str | None = None
    numero: str | None = None
    sub: str | None = None
    zona_cens: str | None = None
    micro_zona: str | None = None
    categoria: str | None = None
    classe: str | None = None
    consistenza: str | None = None
    superficie_totale: Optional[float] = None
    superficie_escluse: Optional[float] = None
    superficie_raw: str | None = None
    rendita: str | None = None

    # Адреса з візури (зберігаємо, але НЕ використовуємо для атестаціоне)
    immobile_comune: str | None = None
    immobile_comune_code: str | None = None
    via_type: str | None = None
    via_name: str | None = None
    via_num: str | None = None
    scala: str | None = None
    interno: str | None = None
    piano: str | None = None
    indirizzo_raw: str | None = None
    dati_ulteriori: str | None = None

    # Дані орендодавця з візури
    locatore_surname: str | None = None
    locatore_name: str | None = None
    locatore_codice_fiscale: str | None = None

    # --- OVERRIDE: РЕАЛЬНА адреса об'єкта (з YAML, збережена в БД) ---
    immobile_comune_override: str | None = None
    immobile_via_override: str | None = None
    immobile_civico_override: str | None = None
    immobile_piano_override: str | None = None
    immobile_interno_override: str | None = None

    # --- Адреса орендодавця (з YAML, збережена в БД) ---
    locatore_comune_res: str | None = None
    locatore_via: str | None = None
    locatore_civico: str | None = None

    # -------------------------------------------------------------------------
    # Поля для заповнення DOCX-шаблону: Тип договору, Мебльованість, Енергоклас, Фактичний canone з договору, Тривалість договору.
    # -------------------------------------------------------------------------
    contract_kind: str | None = None
    arredato: str | None = None
    energy_class: str | None = None
    canone_contrattuale_mensile: Optional[float] = None
    durata_anni: Optional[int] = None

    # --- Елементи A/B/C/D (з YAML, збережені в БД) ---
    a1: Optional[str] = None
    a2: Optional[str] = None

    b1: Optional[str] = None
    b2: Optional[str] = None
    b3: Optional[str] = None
    b4: Optional[str] = None
    b5: Optional[str] = None

    c1: Optional[str] = None
    c2: Optional[str] = None
    c3: Optional[str] = None
    c4: Optional[str] = None
    c5: Optional[str] = None
    c6: Optional[str] = None
    c7: Optional[str] = None

    d1: Optional[str] = None
    d2: Optional[str] = None
    d3: Optional[str] = None
    d4: Optional[str] = None
    d5: Optional[str] = None
    d6: Optional[str] = None
    d7: Optional[str] = None
    d8: Optional[str] = None
    d9: Optional[str] = None
    d10: Optional[str] = None
    d11: Optional[str] = None
    d12: Optional[str] = None
    d13: Optional[str] = None

    # --- Підсумкові кількості елементів за типами ---
    a_cnt: Optional[int] = None
    b_cnt: Optional[int] = None
    c_cnt: Optional[int] = None
    d_cnt: Optional[int] = None
