# uppi/services/db_repo.py
from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import psycopg2
import psycopg2.extras
from itemadapter import ItemAdapter
from psycopg2 import Error as Psycopg2Error

from uppi.domain.immobile import Immobile
from uppi.utils.db_utils.key_normalize import normalize_element_key
from uppi.utils.parse_utils import clean_str, clean_sub, parse_date, safe_float

logger = logging.getLogger(__name__)

# Константи для ключів елементів (A1..D13)
ELEMENT_KEYS = (
    ["a1", "a2"]
    + [f"b{i}" for i in range(1, 6)]
    + [f"c{i}" for i in range(1, 8)]
    + [f"d{i}" for i in range(1, 14)]
)

# Список колонок, які ми очікуємо отримати з парсера/Immobile об'єкта
# (використовується для формування словника перед вставкою)
IMMOBILI_PARSED_COLUMNS = [
    "table_num_immobile",
    "sez_urbana",
    "foglio",
    "numero",
    "sub",
    "zona_cens",
    "micro_zona",
    "categoria",
    "classe",
    "consistenza",
    "rendita",
    "superficie_totale",
    "superficie_escluse",
    "superficie_raw",
    # Адреси тепер обробляються окремо, але парсер може повертати сирі дані
    "immobile_comune",
    "via_type",
    "via_name",
    "via_num",
    "scala",
    "interno",
    "piano",
    "indirizzo_raw",
    "dati_ulteriori",
]


# ---------------------------------------------------------
# Допоміжна функція для логіки "Smart Patch"
# ---------------------------------------------------------
def resolve_patch_value(yaml_val: Any, db_val: Any, default: Any = None) -> Any:
    """
    Вирішує, яке значення використовувати:
    1. Якщо yaml_val == "-", повертає None (або default), тобто видаляє значення.
    2. Якщо yaml_val є (не None і не пустий рядок), повертає його.
    3. Якщо yaml_val пустий, повертає старе значення з БД (db_val).
    """
    s_val = str(yaml_val).strip() if yaml_val is not None else ""
    
    if s_val == "-":
        return default  # Явне видалення
    
    if s_val:
        return yaml_val # Нове значення
        
    return db_val # Старе значення (або None, якщо старого нема)


# =========================================================
# Helpers
# =========================================================

def immobile_from_parsed_dict(data: Dict[str, Any]) -> Immobile:
    """
    Створює об'єкт Immobile зі словника, отриманого від парсера.
    Виконує базове перетворення типів для площі.
    """
    # Створюємо копію, щоб не мутувати вхідний словник
    d = data.copy()
    if "superficie_totale" in d:
        d["superficie_totale"] = safe_float(d.get("superficie_totale"))
    if "superficie_escluse" in d:
        d["superficie_escluse"] = safe_float(d.get("superficie_escluse"))
    return Immobile(**d)


def immobile_db_row(imm: Immobile) -> Dict[str, Any]:
    """
    Перетворює об'єкт Immobile у словник для подальшої обробки/вставки.
    Виконує очистку рядків та нормалізацію sub/foglio/numero.
    """
    row: Dict[str, Any] = {}

    for col in IMMOBILI_PARSED_COLUMNS:
        raw = getattr(imm, col, None)

        if col == "sub":
            row[col] = clean_sub(raw)
            continue

        if col in ("superficie_totale", "superficie_escluse"):
            row[col] = safe_float(raw)
            continue

        row[col] = clean_str(raw)

    # Нормалізація критичних полів (видалення пробілів)
    if row.get("foglio") is not None:
        row["foglio"] = str(row["foglio"]).strip()
    if row.get("numero") is not None:
        row["numero"] = str(row["numero"]).strip()
    
    # Гарантуємо, що sub - це рядок (навіть порожній), а не None
    if row.get("sub") is None:
        row["sub"] = ""

    return row


# =========================================================
# 1. ADDRESSES (New)
# =========================================================

def db_upsert_address(conn, addr_data: Dict[str, Any]) -> Optional[int]:
    """
    Знаходить існуючу адресу або створює нову.
    Повертає ID адреси.
    
    addr_data очікує ключі: 
      - comune (обов'язково)
      - via_full (обов'язково, або via_name як fallback)
      - civico, piano, interno, scala (опціонально)
    """
    comune = clean_str(addr_data.get("comune"))
    # Формуємо повну назву вулиці, якщо вона розбита, або беремо вже готову
    via_full = clean_str(addr_data.get("via_full"))
    if not via_full:
        # Fallback: пробуємо склеїти тип і назву, якщо via_full немає
        via_type = clean_str(addr_data.get("via_type"))
        via_name = clean_str(addr_data.get("via_name"))
        if via_type and via_name:
            via_full = f"{via_type} {via_name}"
        elif via_name:
            via_full = via_name
            
    # Якщо критичних даних немає - адресу не створюємо
    if not comune or not via_full:
        return None

    civico = clean_str(addr_data.get("civico"))
    piano = clean_str(addr_data.get("piano"))
    interno = clean_str(addr_data.get("interno"))
    scala = clean_str(addr_data.get("scala"))

    sql = """
    INSERT INTO public.addresses (comune, via_full, civico, piano, interno, scala)
    VALUES (
        %(comune)s, 
        %(via_full)s, 
        COALESCE(%(civico)s, 'SNC'), 
        %(piano)s, 
        %(interno)s, 
        %(scala)s
    )
    ON CONFLICT (content_hash) DO UPDATE 
    SET created_at = public.addresses.created_at -- Фіктивний апдейт, щоб повернути ID
    RETURNING id;
    """
    
    params = {
        "comune": comune,
        "via_full": via_full,
        "civico": civico,
        "piano": piano,
        "interno": interno,
        "scala": scala,
    }

    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            res = cur.fetchone()
            if res:
                return res[0]
            # Теоретично сюди не має дійти, якщо ON CONFLICT працює коректно
            # Але якщо раптом, пробуємо знайти вручну за хешем (для надійності)
            cur.execute(
                "SELECT id FROM public.addresses WHERE content_hash = md5(upper(trim(%s)) || '|' || upper(trim(regexp_replace(%s, '\s+', ' ', 'g'))) || '|' || upper(trim(COALESCE(%s, 'SNC'))))", 
                (comune, via_full, civico)
            )
            res_fallback = cur.fetchone()
            return res_fallback[0] if res_fallback else None
            
    except Exception as e:
        logger.error(f"[DB] Address upsert failed: {e}")
        # Не "ковтаємо" помилку, бо без адреси може поплисти логіка
        raise


# =========================================================
# 2. PERSONS (Updated)
# =========================================================

def db_upsert_person(
    conn, 
    cf: str, 
    surname: Optional[str], 
    name: Optional[str], 
    address_id: Optional[int] = None
) -> None:
    """
    Оновлює або створює запис про особу (Locatore/Conduttore).
    Тепер підтримує лінк на адресу проживання.
    """
    if not cf:
        return

    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO public.persons (cf, surname, name, residence_address_id)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (cf) DO UPDATE
            SET
              surname = COALESCE(EXCLUDED.surname, persons.surname),
              name    = COALESCE(EXCLUDED.name, persons.name),
              residence_address_id = COALESCE(EXCLUDED.residence_address_id, persons.residence_address_id),
              updated_at = now();
            """,
            (cf, surname, name, address_id),
        )


# =========================================================
# 3. VISURE (Metadata)
# =========================================================


def db_upsert_visura(
    conn, 
    cf: str, 
    pdf_bucket: str, 
    pdf_object: str, 
    checksum_sha256: Optional[str], 
    fetched_now: bool
) -> int:
    """
    Зберігає метадані візури. Повертає ID візури.
    Використовує параметризований запит для уникнення SQL-ін'єкцій.
    """
    
    # Визначаємо час отримання: якщо fetched_now=True, ставимо поточний час, інакше None
    now = datetime.now() if fetched_now else None

    sql = """
        INSERT INTO public.visure (locatore_cf, pdf_bucket, pdf_object, checksum_sha256, fetched_at)
        VALUES (%(cf)s, %(bucket)s, %(obj)s, %(sum)s, %(now)s)
        ON CONFLICT (locatore_cf) DO UPDATE
        SET
          pdf_bucket      = EXCLUDED.pdf_bucket,
          pdf_object      = EXCLUDED.pdf_object,
          checksum_sha256 = COALESCE(EXCLUDED.checksum_sha256, visure.checksum_sha256),
          -- Оновлюємо fetched_at тільки якщо зараз відбулося завантаження нового файлу
          fetched_at      = COALESCE(EXCLUDED.fetched_at, visure.fetched_at),
          updated_at      = now()
        RETURNING id;
    """
    
    params = {
        "cf": cf,
        "bucket": pdf_bucket,
        "obj": pdf_object,
        "sum": checksum_sha256,
        "now": now
    }
    
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            result = cur.fetchone()
            if result:
                return result[0]
            raise RuntimeError(f"Failed to upsert visura for CF: {cf}")
    except Psycopg2Error as e:
        logger.error(f"[DB] db_upsert_visura error: {e}")
        raise


@dataclass(frozen=True)
class VisuraState:
    cf: str
    pdf_bucket: Optional[str]
    pdf_object: Optional[str]
    fetched_at: Optional[Any]
    id: Optional[int] = None # Додали ID

def fetch_visura_state(conn, cf: str) -> Optional[VisuraState]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT locatore_cf, pdf_bucket, pdf_object, fetched_at, id
            FROM public.visure
            WHERE locatore_cf = %s;
            """,
            (cf,),
        )
        row = cur.fetchone()
    if not row:
        return None
    return VisuraState(cf=row[0], pdf_bucket=row[1], pdf_object=row[2], fetched_at=row[3], id=row[4])


# =========================================================
# 4. IMMOBILI (Master Data - Updated)
# =========================================================


# 4.1 Upsert Immobile with new fields

def db_upsert_immobile(
    conn, 
    owner_cf: str, 
    imm: Immobile, 
    visura_addr_id: Optional[int] = None,
    source_visura_id: Optional[int] = None
) -> int:
    """
    Вставляє або оновлює Immobili (Master Data).
    Використовує нові поля: micro_zona, zona_cens, visura_address_id.
    """
    row = immobile_db_row(imm)

    # Валідація критичних полів
    foglio = row.get("foglio")
    numero = row.get("numero")
    
    if not foglio or not numero:
        raise ValueError(
            f"Cannot upsert immobile without foglio+numero. "
            f"Got foglio={foglio!r}, numero={numero!r}, owner_cf={owner_cf!r}"
        )

    sub = row.get("sub") or ""

    sql = """
    INSERT INTO public.immobili (
        owner_cf, source_visura_id, visura_address_id,
        sez_urbana, foglio, numero, sub,
        zona_cens, micro_zona, categoria, classe, consistenza, rendita,
        superficie_totale, superficie_escluse, superficie_raw
    )
    VALUES (
        %(owner_cf)s, %(source_visura_id)s, %(visura_addr_id)s,
        %(sez_urbana)s, %(foglio)s, %(numero)s, %(sub)s,
        %(zona_cens)s, %(micro_zona)s, %(categoria)s, %(classe)s, %(consistenza)s, %(rendita)s,
        %(superficie_totale)s, %(superficie_escluse)s, %(superficie_raw)s
    )
    ON CONFLICT (owner_cf, foglio, numero, sub) DO UPDATE
    SET
        -- Оновлюємо дані з візури, якщо вони змінилися
        source_visura_id   = COALESCE(EXCLUDED.source_visura_id, immobili.source_visura_id),
        visura_address_id  = COALESCE(EXCLUDED.visura_address_id, immobili.visura_address_id),
        
        zona_cens          = COALESCE(EXCLUDED.zona_cens, immobili.zona_cens),
        micro_zona         = COALESCE(EXCLUDED.micro_zona, immobili.micro_zona),
        categoria          = COALESCE(EXCLUDED.categoria, immobili.categoria),
        classe             = COALESCE(EXCLUDED.classe, immobili.classe),
        consistenza        = COALESCE(EXCLUDED.consistenza, immobili.consistenza),
        rendita            = COALESCE(EXCLUDED.rendita, immobili.rendita),
        superficie_totale  = COALESCE(EXCLUDED.superficie_totale, immobili.superficie_totale),
        superficie_escluse = COALESCE(EXCLUDED.superficie_escluse, immobili.superficie_escluse),
        superficie_raw     = COALESCE(EXCLUDED.superficie_raw, immobili.superficie_raw),
        updated_at         = now()
    RETURNING id;
    """

    params = {
        "owner_cf": owner_cf,
        "source_visura_id": source_visura_id,
        "visura_addr_id": visura_addr_id,
        
        "sez_urbana": row.get("sez_urbana"),
        "foglio": foglio,
        "numero": numero,
        "sub": sub,
        
        "zona_cens": row.get("zona_cens"),
        "micro_zona": row.get("micro_zona"),
        "categoria": row.get("categoria"),
        "classe": row.get("classe"),
        "consistenza": row.get("consistenza"),
        "rendita": row.get("rendita"),
        
        "superficie_totale": row.get("superficie_totale"),
        "superficie_escluse": row.get("superficie_escluse"),
        "superficie_raw": row.get("superficie_raw"),
    }

    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchone()[0]
    except Psycopg2Error as e:
        logger.error(f"[DB] Immobile upsert failed for CF={owner_cf} F={foglio} N={numero} S={sub}: {e}")
        raise

# 4.2 Upsert Immobile Elements
def db_upsert_immobile_elements(conn, immobile_id: int, adapter: ItemAdapter):
    """
    Розумне оновлення елементів A-D.
    - Якщо в адаптері ключ відсутній (None) -> дані в БД не чіпаємо (залишаються старі).
    - Якщо значення "-" -> видаляємо запис з БД.
    - Якщо є значення -> оновлюємо або вставляємо.
    """
    
    # Список всіх можливих ключів елементів, щоб нічого не пропустити
    all_keys = (
        ["a1", "a2"]
        + [f"b{i}" for i in range(1, 6)]
        + [f"c{i}" for i in range(1, 8)]
        + [f"d{i}" for i in range(1, 14)]
    )

    with conn.cursor() as cur:
        for key in all_keys:
            # Отримуємо "сире" значення з YAML
            raw_val = adapter.get(key)

            # 1. Якщо в YAML ключ пропущений — пропускаємо (зберігаємо те, що вже є в БД)
            if raw_val is None:
                continue

            val = str(raw_val).strip()
            
            # Парсимо групу і код (наприклад, key='d12' -> grp='D', code='12')
            grp = key[0].upper()
            code = key[1:]

            # 2. Якщо значення "-", це команда на видалення
            if val == "-":
                cur.execute(
                    """
                    DELETE FROM public.immobile_elements 
                    WHERE immobile_id = %s AND grp = %s AND code = %s
                    """,
                    (immobile_id, grp, code)
                )
            
            # 3. Якщо є будь-яке інше значення (наприклад "X"), записуємо/оновлюємо
            elif val:
                cur.execute(
                    """
                    INSERT INTO public.immobile_elements (immobile_id, grp, code, value)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (immobile_id, grp, code) 
                    DO UPDATE SET value = EXCLUDED.value
                    """,
                    (immobile_id, grp, code, val)
                )


def db_update_immobile_real_address(
    conn, 
    immobile_id: int, 
    real_address_id: Optional[int] = None, 
    energy_class: Optional[str] = None
) -> None:
    """
    Оновлює поля immobile.
    Реалізовано логіку видалення ENERGY_CLASS через "-".
    """
    updates = []
    params = []

    # 1. Адреса (проста логіка update if exists)
    if real_address_id is not None:
        updates.append("real_address_id = %s")
        params.append(real_address_id)
    
    # 2. Energy Class (Smart Patch)
    # Тут ми не робимо SELECT, тому що це простий UPDATE.
    # Але ми можемо використати NULLIF в SQL або логіку в Python.
    # Оскільки вимога "якщо нема в yml то береться з БД" - це стандартна поведінка
    # UPDATE (якщо ми не чіпаємо колонку, вона лишається старою).
    # Нам треба обробити тільки випадки "Є значення" або "-".
    if energy_class is not None:
        val = clean_str(energy_class)
        if val == "-":
            # Явне видалення
            updates.append("energy_class = NULL")
        elif val != "":
            # Оновлення
            updates.append("energy_class = %s")
            params.append(val.upper())
        # Якщо val is None (пусто в YAML) -> ми просто НЕ додаємо це поле в updates,
        # і база залишає старе значення.

    if not updates:
        return

    sql = f"""
    UPDATE public.immobili 
    SET {', '.join(updates)}, updated_at = now()
    WHERE id = %s
    """
    params.append(immobile_id)

    with conn.cursor() as cur:
        try:
            cur.execute(sql, params)
        except psycopg2.Error as e:
            logger.error(f"[DB] Помилка оновлення immobili: {e}")
            conn.rollback()
            raise


def db_load_immobili(conn, owner_cf: str) -> List[Tuple[int, Immobile]]:
    """
    Завантажує всі immobile для власника.
    Повертає список кортежів (id, Immobile).
    
    Примітка: Ця функція також підтягує дані з joined таблиць адрес, 
    щоб заповнити поля Immobile об'єкта (для сумісності з пайплайном).
    """
    sql = """
    SELECT
      i.id,
      -- Кадастрові дані
      i.sez_urbana, i.foglio, i.numero, i.sub,
      i.zona_cens, i.micro_zona, i.categoria, i.classe, i.consistenza, i.rendita,
      i.superficie_totale, i.superficie_escluse, i.superficie_raw,
      i.energy_class,
      
      -- Адреса з візури (JOIN)
      va.comune as v_comune, va.via_full as v_via, va.civico as v_civico, 
      va.piano as v_piano, va.interno as v_interno, va.scala as v_scala,
      
      -- Реальна адреса (JOIN)
      ra.comune as r_comune, ra.via_full as r_via, ra.civico as r_civico,
      ra.piano as r_piano, ra.interno as r_interno
      
    FROM public.immobili i
    LEFT JOIN public.addresses va ON i.visura_address_id = va.id
    LEFT JOIN public.addresses ra ON i.real_address_id = ra.id
    WHERE i.owner_cf = %s
    ORDER BY i.foglio, i.numero, i.sub;
    """
    
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute(sql, (owner_cf,))
        rows = cur.fetchall()

    out: List[Tuple[int, Immobile]] = []
    for r in rows:
        d = dict(r)
        imm_id = int(d.pop("id"))
        
        # Мапимо поля з БД назад у структуру Immobile для сумісності з кодом генератора
        # Пріоритет: Реальна адреса > Адреса візури
        
        imm_obj = Immobile(
            # Кадастрові
            sez_urbana=d['sez_urbana'],
            foglio=d['foglio'],
            numero=d['numero'],
            sub=d['sub'],
            zona_cens=d['zona_cens'],
            micro_zona=d['micro_zona'],
            categoria=d['categoria'],
            classe=d['classe'],
            consistenza=d['consistenza'],
            rendita=d['rendita'],
            superficie_totale=float(d['superficie_totale']) if d['superficie_totale'] else None,
            superficie_escluse=float(d['superficie_escluse']) if d['superficie_escluse'] else None,
            superficie_raw=d['superficie_raw'],
            
            # Енергоклас (Master Data)
            energy_class=d['energy_class'],
            
            # Поля адреси для візури (заповнюємо з візурної адреси)
            immobile_comune=d['v_comune'],
            via_name=d['v_via'], # Ми зберігаємо повну назву як via_full
            via_num=d['v_civico'],
            piano=d['v_piano'],
            interno=d['v_interno'],
            scala=d['v_scala'],
            
            # Override поля (реальна адреса)
            immobile_comune_override=d['r_comune'],
            immobile_via_override=d['r_via'],
            immobile_civico_override=d['r_civico'],
            immobile_piano_override=d['r_piano'],
            immobile_interno_override=d['r_interno']
        )
        out.append((imm_id, imm_obj))
        
    return out


def db_prune_old_immobili_without_contracts(conn, owner_cf: str, keep_ids: List[int], enabled: bool) -> int:
    """Видаляє старі immobile, які не мають контрактів."""
    if not enabled or not keep_ids:
        return 0

    with conn.cursor() as cur:
        cur.execute(
            """
            DELETE FROM public.immobili i
            WHERE i.owner_cf=%s
              AND NOT (i.id = ANY(%s))
              AND NOT EXISTS (
                SELECT 1 FROM public.contracts c WHERE c.immobile_id = i.id
              );
            """,
            (owner_cf, keep_ids),
        )
        return cur.rowcount


# =========================================================
# 5. IMMOBILE ELEMENTS (Details)
# =========================================================

def db_apply_immobile_elements(conn, immobile_id: int, adapter: ItemAdapter) -> None:
    """
    Оновлює елементи A/B/C/D для immobile.
    Якщо в YAML передано "-" - видаляє елемент.
    """
    with conn.cursor() as cur:
        for k in ELEMENT_KEYS:
            raw = adapter.get(k)
            # Якщо ключа немає в адаптері (None) - пропускаємо (не чіпаємо БД)
            # Щоб видалити, треба явно передати "-"
            if raw is None:
                continue
                
            val = str(raw).strip()
            if val == "":
                continue # Порожній рядок теж ігноруємо

            grp = k[0].upper()
            code = k.upper()

            if val == "-":
                cur.execute(
                    "DELETE FROM public.immobile_elements WHERE immobile_id=%s AND grp=%s AND code=%s;",
                    (immobile_id, grp, code),
                )
            else:
                cur.execute(
                    """
                    INSERT INTO public.immobile_elements (immobile_id, grp, code, value)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (immobile_id, grp, code) DO UPDATE
                    SET value = EXCLUDED.value;
                    """,
                    (immobile_id, grp, code, val),
                )


# =========================================================
# 6. CONTRACTS (Updated)
# =========================================================

def db_upsert_contract(conn, immobile_id: int, adapter: ItemAdapter) -> str:
    """
    Створює або оновлює контракт з урахуванням логіки:
    - CONTRACT_KIND: Reset to Default/YAML (no DB history).
    - ARREDATO, DURATA, ISTAT: Patch (YAML > DB > Delete via "-").
    - IGNORE_SURCHARGES: Patch logic.
    """
    
    # 1. Отримуємо поточний стан з бази (якщо є), щоб знати "старі" значення
    old_contract = {}
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute(
            """
            SELECT id, 
                   contract_kind, durata_anni, arredato_pct, istat_rate, ignore_surcharges,
                   start_date, decorrenza_data, registrazione_data, 
                   registrazione_num, agenzia_entrate_sede, canone_contrattuale_mensile,
                   conduttore_cf
            FROM public.contracts 
            WHERE immobile_id = %s 
            ORDER BY created_at DESC LIMIT 1
            """, 
            (immobile_id,)
        )
        row = cur.fetchone()
        if row:
            old_contract = dict(row)

    contract_id = old_contract.get("id")

    # 2. Підготовка нових значень
    
    # --- A. CONTRACT_KIND (RESET LOGIC) ---
    # Не дивимось в old_contract. Або YAML, або CONCORDATO.
    kind_raw = clean_str(adapter.get("contract_kind"))
    new_kind = (kind_raw or "CONCORDATO").upper()
    if new_kind not in ['CONCORDATO', 'TRANSITORIO', 'STUDENTI']:
        logger.warning(f"[DB] Unknown contract kind '{new_kind}', defaulting to CONCORDATO")
        new_kind = 'CONCORDATO'

    # --- B. ARREDATO (SMART PATCH) ---
    # YAML "-" -> 0.0 (видалено/пусто). YAML value -> float. YAML empty -> Old DB val.
    raw_arredato = adapter.get("arredato")
    old_arredato = float(old_contract.get("arredato_pct") or 0.0)
    
    if str(raw_arredato).strip() == "-":
        new_arredato = 0.0
    elif raw_arredato is not None and str(raw_arredato).strip() != "":
        new_arredato = safe_float(raw_arredato) or 0.0
    else:
        new_arredato = old_arredato

    # --- C. DURATA_ANNI (SMART PATCH) ---
    raw_durata = adapter.get("durata_anni")
    old_durata = old_contract.get("durata_anni") # int or None
    
    if str(raw_durata).strip() == "-":
        new_durata = None # NULL в базі
    elif raw_durata is not None and str(raw_durata).strip() != "":
        new_durata = int(raw_durata)
    else:
        new_durata = old_durata
        
    # Default fallback, якщо і в базі було пусто, і в YAML пусто -> 3 роки (стандарт)
    if new_durata is None:
        new_durata = 3

    # --- D. ISTAT (SMART PATCH) ---
    raw_istat = adapter.get("istat")
    old_istat = float(old_contract.get("istat_rate") or 0.0)
    
    if str(raw_istat).strip() == "-":
        new_istat = 0.0
    elif raw_istat is not None and str(raw_istat).strip() != "":
        new_istat = safe_float(raw_istat) or 0.0
    else:
        new_istat = old_istat

    # --- E. IGNORE_SURCHARGES (SMART PATCH) ---
    # Читаємо з extra або як окреме поле, якщо ви його додали в маппер
    # Припускаємо, що воно є в adapter (потрібно додати в item_mapper.py, див. нижче)
    raw_ignore = adapter.get("ignore_surcharges")
    old_ignore = bool(old_contract.get("ignore_surcharges")) if old_contract else False
    
    if str(raw_ignore).strip() == "-":
        new_ignore = False
    elif raw_ignore is not None and str(raw_ignore).strip() != "":
        # Парсинг булевого значення
        s = str(raw_ignore).lower()
        new_ignore = s in ("true", "1", "yes", "y")
    else:
        new_ignore = old_ignore

    # --- F. Інші поля (Дата, CF і т.д.) ---
    # Для них залишаємо логіку COALESCE в SQL або патчимо тут.
    # Для чистоти коду простіше підготувати повний словник params і зробити простий UPDATE/INSERT.
    
    cond_cf = clean_str(adapter.get("conduttore_cf")) or old_contract.get("conduttore_cf")
    
    # Дати: Якщо в YAML пусто -> беремо старе
    start_date = parse_date(adapter.get("contratto_data")) or old_contract.get("start_date")
    decorrenza = parse_date(adapter.get("decorrenza_data")) or old_contract.get("decorrenza_data")
    reg_data = parse_date(adapter.get("registrazione_data")) or old_contract.get("registrazione_data")
    
    reg_num = clean_str(adapter.get("registrazione_num")) or old_contract.get("registrazione_num")
    ae_sede = clean_str(adapter.get("agenzia_entrate_sede")) or old_contract.get("agenzia_entrate_sede")
    canone_val = safe_float(adapter.get("canone_contrattuale_mensile")) or old_contract.get("canone_contrattuale_mensile")

    # Якщо start_date все ще None -> сьогодні
    if not start_date:
        from datetime import date
        start_date = date.today()

    params = {
        "immobile_id": immobile_id,
        "cond_cf": cond_cf,
        "kind": new_kind,
        "start_date": start_date,
        "durata": new_durata,
        "decorrenza": decorrenza,
        "reg_data": reg_data,
        "reg_num": reg_num,
        "ae_sede": ae_sede,
        "canone": canone_val,
        "istat": new_istat,
        "arredato": new_arredato,
        "ignore_surcharges": new_ignore
    }

    with conn.cursor() as cur:
        if not contract_id:
            # INSERT
            sql = """
            INSERT INTO public.contracts (
                immobile_id, conduttore_cf, contract_kind, start_date, durata_anni,
                decorrenza_data, registrazione_data, registrazione_num, agenzia_entrate_sede,
                canone_contrattuale_mensile, istat_rate, arredato_pct, ignore_surcharges
            ) VALUES (
                %(immobile_id)s, %(cond_cf)s, %(kind)s, %(start_date)s, %(durata)s,
                %(decorrenza)s, %(reg_data)s, %(reg_num)s, %(ae_sede)s,
                %(canone)s, %(istat)s, %(arredato)s, %(ignore_surcharges)s
            ) RETURNING id;
            """
            cur.execute(sql, params)
            contract_id = cur.fetchone()[0]
        else:
            # UPDATE
            # Ми вже вирішили всі конфлікти YAML/DB на рівні Python змінних,
            # тому просто записуємо params.
            sql = """
            UPDATE public.contracts SET
                conduttore_cf = %(cond_cf)s,
                contract_kind = %(kind)s,
                start_date    = %(start_date)s,
                durata_anni   = %(durata)s,
                
                decorrenza_data      = %(decorrenza)s,
                registrazione_data   = %(reg_data)s,
                registrazione_num    = %(reg_num)s,
                agenzia_entrate_sede = %(ae_sede)s,
                
                canone_contrattuale_mensile = %(canone)s,
                istat_rate    = %(istat)s,
                arredato_pct  = %(arredato)s,
                ignore_surcharges = %(ignore_surcharges)s,
                
                updated_at = now()
            WHERE id = %(id)s;
            """
            cur.execute(sql, {**params, "id": contract_id})
            
        return str(contract_id)


def db_load_contract_context(conn, contract_id: str) -> Dict[str, Any]:
    """
    Завантажує повний контекст контракту для генерації документа.
    """
    ctx: Dict[str, Any] = {
            "contract": {},
            "overrides": {},
            "elements": {},
            "parties": {},
            "canone_calc": None,
            "immobile": {}, 
        }

    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        sql_contract = """
        SELECT 
            c.*,
            -- Власник (Locatore)
            p_own.cf as loc_cf, p_own.name as loc_name, p_own.surname as loc_surname,
            a_own.comune as loc_comune, a_own.via_full as loc_via, a_own.civico as loc_civico,
            
            -- Орендар (Conduttore)
            p_cond.cf as cond_cf, p_cond.name as cond_name, p_cond.surname as cond_surname,
            a_cond.comune as cond_comune, a_cond.via_full as cond_via,
            
            -- АДРЕСА ОБ'ЄКТА + ENERGY CLASS
            COALESCE(ra.comune, va.comune) as imm_comune,
            COALESCE(ra.via_full, va.via_full) as imm_via,
            COALESCE(ra.civico, va.civico) as imm_civico,
            COALESCE(ra.piano, va.piano) as imm_piano,
            COALESCE(ra.interno, va.interno) as imm_interno,
            i.energy_class as imm_energy_class 
            
        FROM public.contracts c
        JOIN public.immobili i ON c.immobile_id = i.id
        LEFT JOIN public.persons p_own ON i.owner_cf = p_own.cf 
        LEFT JOIN public.addresses a_own ON p_own.residence_address_id = a_own.id
        
        LEFT JOIN public.persons p_cond ON c.conduttore_cf = p_cond.cf
        LEFT JOIN public.addresses a_cond ON p_cond.residence_address_id = a_cond.id
        
        LEFT JOIN public.addresses va ON i.visura_address_id = va.id
        LEFT JOIN public.addresses ra ON i.real_address_id = ra.id
        
        WHERE c.id = %s;
        """
        cur.execute(sql_contract, (contract_id,))
        row = cur.fetchone()
        
        if row:
            ctx["contract"] = dict(row)

            # Спеціальна секція immobile для генератора та процесора
            ctx["immobile"] = {
                "comune": row["imm_comune"],
                "via": row["imm_via"],
                "civico": row["imm_civico"],
                "piano": row["imm_piano"],
                "interno": row["imm_interno"],
                "energy_class": row["imm_energy_class"] # <--- Передаємо в контекст
            }

            ctx["overrides"] = {
                "locatore_comune_res": row["loc_comune"],
                "locatore_via": row["loc_via"],
                "locatore_civico": row["loc_civico"],
            }
            
            ctx["parties"]["LOCATORE"] = {
                "cf": row["loc_cf"], "name": row["loc_name"], "surname": row["loc_surname"]
            }
            if row["cond_cf"]:
                ctx["parties"]["CONDUTTORE"] = {
                    "cf": row["cond_cf"], "name": row["cond_name"], "surname": row["cond_surname"],
                    "comune": row["cond_comune"], "via": row["cond_via"]
                }
                
            immobile_id = row["immobile_id"]

            # Завантаження елементів
            cur.execute(
                "SELECT grp, code, value FROM public.immobile_elements WHERE immobile_id=%s;",
                (immobile_id,),
            )
            elements: Dict[str, str] = {}
            for grp, code, value in cur.fetchall():
                key = normalize_element_key(str(grp or ""), str(code or ""))
                if not key:
                    continue
                elements[key] = "" if value is None else str(value)
            ctx["elements"] = elements

        # Останній розрахунок канону
        cur.execute(
            """
            SELECT inputs::text
            FROM public.canone_calcoli
            WHERE contract_id=%s
            ORDER BY calculated_at DESC
            LIMIT 1;
            """,
            (contract_id,),
        )
        r = cur.fetchone()
        if r and r[0]:
            try:
                ctx["canone_calc"] = json.loads(r[0])
            except Exception:
                ctx["canone_calc"] = None

    return ctx

# =========================================================
# 7. LOGGING & AUDIT
# =========================================================

def db_insert_canone_calc(
    conn, 
    contract_id: str, 
    method: str, 
    inputs: Dict[str, Any], 
    result_mensile: Optional[float]
) -> None:
    """
    Логує розрахунок канону. 
    """
    # Безпечно витягуємо словник 'result', якщо його немає — повертаємо порожній словник {}
    res = inputs.get("result") or {}
    
    # Витягуємо значення. Якщо ключів немає, safe_float поверне None або 0.0 (залежить від вашої реалізації)
    min_val = safe_float(res.get("base_min_euro_mq"))
    max_val = safe_float(res.get("base_max_euro_mq"))

    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO public.canone_calcoli (contract_id, inputs, min_val, max_val, result_mensile)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (contract_id, calculated_at) DO NOTHING;
            """,
            (contract_id, psycopg2.extras.Json(inputs), min_val, max_val, result_mensile),
        )


def db_insert_attestazione_log(
    conn,
    contract_id: str,
    status: str,
    output_bucket: str,
    output_object: str,
    params_snapshot: Dict[str, Any],
    error: Optional[str],
    author_login_masked: str,
    author_login_sha256: str,
    template_version: str,
) -> None:
    """
    Логує факт генерації атестації.
    """
    # Додаємо метадані про автора та помилку в snapshot, бо в новій схемі немає окремих полів для цього
    full_snapshot = params_snapshot.copy()
    full_snapshot.update({
        "error": error,
        "author_masked": author_login_masked,
        "template_version": template_version
    })

    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO public.attestazioni (
              contract_id,
              output_bucket,
              output_object,
              full_data_snapshot,
              author_hash,
              status
            )
            VALUES (%s, %s, %s, %s, %s, %s);
            """,
            (
                contract_id,
                output_bucket,
                output_object,
                psycopg2.extras.Json(full_snapshot),
                author_login_sha256,
                status,
            ),
        )