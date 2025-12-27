"""

"""
import argparse
from datetime import datetime
from typing import Any, Dict, List, Optional

from decouple import config
import psycopg
from psycopg.rows import dict_row

import yaml


DB_HOST = config("DB_HOST", default="localhost")
DB_PORT = config("DB_PORT", default="5432")
DB_NAME = config("DB_NAME", default="uppi_db")
DB_USER = config("DB_USER", default="uppi_user")
DB_PASSWORD = config("DB_PASSWORD", default="uppi_password")


CLIENTS_YAML = config("UPPI_CLIENTS_YAML", default="clients/clients.yml")


# ---------------------------
#  helpers: YAML
# ---------------------------

def load_clients() -> List[Dict[str, Any]]:
    """Завантажуємо clients.yml і повертаємо список рядків (словників)."""
    try:
        with open(CLIENTS_YAML, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or []
    except FileNotFoundError:
        return []

    if not isinstance(data, list):
        return []

    return [row for row in data if isinstance(row, dict)]


def find_rows_by_cf(cf: str, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Фільтруємо рядки за LOCATORE_CF."""
    cf_norm = cf.strip().upper()
    return [
        row for row in rows
        if str(row.get("LOCATORE_CF", "")).strip().upper() == cf_norm
    ]


# ---------------------------
#  helpers: DB
# ---------------------------

def get_conn():
    """Підключаємося до Postgres за параметрами з .env."""
    if not DB_HOST or not DB_NAME or not DB_USER or not DB_PASSWORD:
        raise RuntimeError("UPPI DB DB_HOST or DB_NAME or DB_USER or DB_PASSWORD не задано в .env")
    return psycopg.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
    )


def fetch_visura(conn, cf: str) -> Optional[Dict[str, Any]]:
    """
    Таблиця visure:

        cf         TEXT PK
        pdf_bucket TEXT
        pdf_object TEXT
        updated_at TIMESTAMPTZ

    Тут просто перевіряємо, що запис є, і тягнемо метадані PDF.
    """
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT cf, pdf_bucket, pdf_object, updated_at
            FROM visure
            WHERE cf = %s
            """,
            (cf,),
        )
        return cur.fetchone()


def build_visura_address(im: Dict[str, Any]) -> str:
    """
    Будуємо адресу об'єкта, як вона є в візурі.
    Формат: [via_type via_name] [n. num_via] [Piano piano] [Int. interno]
    Якщо нічого з цього немає, повертаємо indirizzo_raw."""
    bits = []
    via_type = im.get("via_type")
    via_name = im.get("via_name")
    num_via = im.get("num_via")

    if via_type or via_name:
        bits.append(" ".join(x for x in [via_type, via_name] if x))
    if num_via:
        bits.append(f"n. {num_via}")
    if im.get("piano"):
        bits.append(f"Piano {im['piano']}")
    if im.get("interno"):
        bits.append(f"Int. {im['interno']}")

    addr = ", ".join(bits)
    if addr:
        return addr

    return im.get("indirizzo_raw") or ""


def build_real_address(im: Dict[str, Any]) -> str:
    """
    Будуємо реальну адресу об'єкта, враховуючи можливі оверрайди.
    1. Якщо є immobile_*_override, то беремо їх.
    2. Інакше беремо звичайні поля (comune).
    Формат: [comune - ] [via immobile_civico_override] [Piano immobile_piano_override] [Int. immobile_interno_override]
    Параметри:
    - immobile_via_override, 
    - immobile_civico_override, 
    - immobile_piano_override, 
    - immobile_interno_override
    """
    comune = im.get("immobile_comune_override") or im.get("comune")
    via = im.get("immobile_via_override")
    civico = im.get("immobile_civico_override")
    piano_ov = im.get("immobile_piano_override")
    interno_ov = im.get("immobile_interno_override")

    parts = []
    if via:
        if civico:
            parts.append(f"{via} {civico}")
        else:
            parts.append(via)
    if piano_ov:
        parts.append(f"Piano {piano_ov}")
    if interno_ov:
        parts.append(f"Int. {interno_ov}")

    addr = ", ".join(parts)

    if comune:
        if addr:
            addr = f"{comune} - {addr}"
        else:
            addr = comune

    return addr


def fetch_immobili(conn, cf: str) -> List[Dict[str, Any]]:
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT
                id,
                immobile_comune      AS comune,
                immobile_comune_code AS comune_code,
                foglio,
                numero,
                sub,
                categoria,
                rendita,
                superficie_totale,
                superficie_escluse,
                via_type,
                via_name,
                via_num              AS num_via,
                scala,
                interno,
                piano,
                indirizzo_raw,
                locatore_name,
                locatore_surname,
                locatore_codice_fiscale,

                immobile_comune_override,
                immobile_via_override,
                immobile_civico_override,
                immobile_piano_override,
                immobile_interno_override
            FROM immobili
            WHERE visura_cf = %s
            ORDER BY immobile_comune, foglio, numero, sub, id
            """,
            (cf,),
        )
        return cur.fetchall()


# ---------------------------
#  pretty-print
# ---------------------------

def fmt_dt(value: Any) -> str:
    if isinstance(value, datetime):
        return value.isoformat(sep=" ", timespec="seconds")
    return str(value)


def print_header_for_row(idx: int, row: Dict[str, Any]):
    cf = str(row.get("LOCATORE_CF", "")).strip().upper()
    print("=" * 80)
    print(f"[{idx}] CF: {cf}")
    print("-" * 80)

    # Те, що вже є у YAML — ти бачиш, чи заповнював щось ще
    interesting_keys = [
        "LOCATORE_CF",
        "LOCATORE_NOME",
        "LOCATORE_COGNOME",
        "CONDUTTORE_CF",
        "CONDUTTORE_NOME",
        "CONDUTTORE_COGNOME",
        "IMMOBILE_COMUNE",
        "IMMOBILE_FOGLIO",
        "IMMOBILE_NUMERO",
        "IMMOBILE_SUB",
    ]
    printed = set()
    for key in interesting_keys:
        if key in row and row[key] not in ("", None):
            print(f"  {key:20}: {row[key]}")
            printed.add(key)

    # решта полів, якщо щось є
    others = [k for k in row.keys() if k not in printed]
    if others:
        print("  --- інші поля в YAML ---")
        for k in sorted(others):
            print(f"  {k:20}: {row[k]}")


def print_visura_and_immobili(cf: str, visura: Optional[Dict[str, Any]], immobili: List[Dict[str, Any]]):
    print()
    print("=== Дані з БД (visure / immobili) ===")

    if not visura:
        print("⛔ Візури для цього CF в БД немає.")
        print("   ➜ Треба запускати спайдер, щоб отримати PDF і об'єкти.")
        return

    # Локатор (ім'я/прізвище/CF) беремо з першого immobile, якщо є
    loc_name = loc_surname = loc_cf = None
    if immobili:
        first = immobili[0]
        loc_name = first.get("locatore_name")
        loc_surname = first.get("locatore_surname")
        loc_cf = first.get("locatore_codice_fiscale") or cf

    print("✅ Візура знайдена в БД")
    if loc_cf:
        print(f"  Локатор CF    : {loc_cf}")
    if loc_name or loc_surname:
        print(f"  Локатор ім'я  : {loc_name}")
        print(f"  Локатор прізв.: {loc_surname}")

    print(f"  PDF bucket    : {visura.get('pdf_bucket')}")
    print(f"  PDF object    : {visura.get('pdf_object')}")
    print(f"  Оновлено      : {fmt_dt(visura.get('updated_at'))}")
    print(f"  К-сть об'єктів (immobili): {len(immobili)}")

    if not immobili:
        print("  ⚠️ Візура є, але в таблиці immobili записів немає.")
        return

    print()
    print("  Об'єкти:")
    for i, im in enumerate(immobili, start=1):
        visura_addr = build_visura_address(im)
        real_addr = build_real_address(im)

        print(f"  [{i}] {im.get('comune')} ({im.get('comune_code')})")
        print(
            f"       Foglio {im.get('foglio')}  Numero {im.get('numero')}  "
            f"Sub {im.get('sub')}  Cat {im.get('categoria')}"
        )
        print(
            f"       Rendita: {im.get('rendita')}  "
            f"Sup.tot: {im.get('superficie_totale')}  "
            f"Sup.escl.: {im.get('superficie_escluse')}"
        )

        if real_addr:
            print(f"       Реальна адреса  : {real_addr}")
        else:
            print("       Реальна адреса  : — (не задано, буде братися з візури)")

        print(f"       Адреса в Visura : {visura_addr}")

        print("       ➜ YAML-селектор для цього об'єкта (якщо хочеш тільки його):")
        print(
            "          IMMOBILE_COMUNE:  {comune}\n"
            "          IMMOBILE_FOGLIO:  {foglio}\n"
            "          IMMOBILE_NUMERO:  {numero}\n"
            "          IMMOBILE_SUB:    {sub}".format(
                comune=im.get("comune"),
                foglio=im.get("foglio"),
                numero=im.get("numero"),
                sub=im.get("sub"),
            )
        )
        print()


# ---------------------------
#  main
# ---------------------------

def main():
    parser = argparse.ArgumentParser(
        description=(
            "Огляд даних по клієнтах з clients.yml + Postgres.\n"
            "Використання: спочатку вписуєш LOCATORE_CF у clients.yml, потім запускаєш цю утиліту."
        )
    )
    parser.add_argument(
        "--cf",
        help="конкретний LOCATORE_CF, який вже є в clients.yml",
    )
    parser.add_argument(
        "--last",
        action="store_true",
        help="взяти останній запис із clients.yml (корисно, коли щойно додав клієнта)",
    )
    args = parser.parse_args()

    rows = load_clients()
    if not rows:
        print(f"clients.yml порожній або не знайдений ({CLIENTS_YAML})")
        return

    # Визначаємо, які рядки перевіряти
    target_rows: List[Dict[str, Any]] = rows

    if args.last:
        target_rows = [rows[-1]]

    if args.cf:
        cf_norm = args.cf.strip().upper()
        filtered = find_rows_by_cf(cf_norm, rows)
        if not filtered:
            print(f"У clients.yml немає записів з LOCATORE_CF={cf_norm}")
            return
        target_rows = filtered

    # Підключення до БД один раз
    conn = get_conn()

    try:
        for idx, row in enumerate(target_rows, start=1):
            cf = str(row.get("LOCATORE_CF", "")).strip().upper()
            if not cf:
                continue

            print_header_for_row(idx, row)

            visura = fetch_visura(conn, cf)
            immobili = fetch_immobili(conn, cf) if visura else []
            print_visura_and_immobili(cf, visura, immobili)

            print()
            print("  ➜ Інтерпретація:")
            if not visura:
                print("     - В БД нічого немає: або запускаєш спайдер, або працюєш тільки з даними орендаря/контракту.")
            else:
                if len(immobili) == 1:
                    print("     - Один об'єкт: можеш спокійно робити атестаціоне 'по CF' для цього об'єкта.")
                else:
                    print("     - Кілька об'єктів: або робиш атестаціоне по всіх,")
                    print("       або додаєш у YAML селектори IMMOBILE_COMUNE / FOGLIO / NUMERO / SUB для вибору одного.")

        print("=" * 80)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
