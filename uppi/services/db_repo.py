from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import psycopg2
import psycopg2.extras
from psycopg2 import Error as Psycopg2Error

from uppi.domain.immobile import Immobile
from uppi.utils.db_utils.key_normalize import normalize_element_key
from uppi.utils.parse_utils import clean_str, clean_sub, parse_date, safe_float, to_bool_or_none

logger = logging.getLogger(__name__)


IMMOBILI_DB_COLUMNS = [
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
    "immobile_comune",
    "immobile_comune_code",
    "via_type",
    "via_name",
    "via_num",
    "scala",
    "interno",
    "piano",
    "indirizzo_raw",
    "dati_ulteriori",
]

ELEMENT_KEYS = (
    ["a1", "a2"]
    + [f"b{i}" for i in range(1, 6)]
    + [f"c{i}" for i in range(1, 8)]
    + [f"d{i}" for i in range(1, 14)]
)


def immobile_from_parsed_dict(data: Dict[str, Any]) -> Immobile:
    if "superficie_totale" in data:
        data["superficie_totale"] = safe_float(data.get("superficie_totale"))
    if "superficie_escluse" in data:
        data["superficie_escluse"] = safe_float(data.get("superficie_escluse"))

    return Immobile(**data)


def immobile_db_row(imm: Immobile) -> Dict[str, Any]:
    row: Dict[str, Any] = {}

    for col in IMMOBILI_DB_COLUMNS:
        raw = getattr(imm, col, None)

        if col == "sub":
            row[col] = clean_sub(raw)
            continue

        if col in ("superficie_totale", "superficie_escluse"):
            row[col] = safe_float(raw)
            continue

        row[col] = clean_str(raw)

    if row.get("foglio") is not None:
        row["foglio"] = str(row["foglio"]).strip()
    if row.get("numero") is not None:
        row["numero"] = str(row["numero"]).strip()

    return row


def db_upsert_person(conn, cf: str, surname: Optional[str], name: Optional[str]) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO public.persons (cf, surname, name)
            VALUES (%s, %s, %s)
            ON CONFLICT (cf) DO UPDATE
            SET
              surname = COALESCE(EXCLUDED.surname, persons.surname),
              name    = COALESCE(EXCLUDED.name, persons.name);
            """,
            (cf, surname, name),
        )


def db_upsert_visura(conn, cf: str, pdf_bucket: str, pdf_object: str, checksum_sha256: Optional[str], fetched_now: bool) -> None:
    with conn.cursor() as cur:
        if fetched_now:
            cur.execute(
                """
                INSERT INTO public.visure (cf, pdf_bucket, pdf_object, checksum_sha256, fetched_at)
                VALUES (%s, %s, %s, %s, now())
                ON CONFLICT (cf) DO UPDATE
                SET
                  pdf_bucket      = EXCLUDED.pdf_bucket,
                  pdf_object      = EXCLUDED.pdf_object,
                  checksum_sha256 = COALESCE(EXCLUDED.checksum_sha256, visure.checksum_sha256),
                  fetched_at      = now();
                """,
                (cf, pdf_bucket, pdf_object, checksum_sha256),
            )
        else:
            cur.execute(
                """
                INSERT INTO public.visure (cf, pdf_bucket, pdf_object, checksum_sha256)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (cf) DO UPDATE
                SET
                  pdf_bucket      = EXCLUDED.pdf_bucket,
                  pdf_object      = EXCLUDED.pdf_object,
                  checksum_sha256 = COALESCE(EXCLUDED.checksum_sha256, visure.checksum_sha256);
                """,
                (cf, pdf_bucket, pdf_object, checksum_sha256),
            )


def db_upsert_immobile(conn, visura_cf: str, imm: Immobile) -> int:
    row: Dict[str, Any] = immobile_db_row(imm)

    sub = row.get("sub")
    row["sub"] = (sub or "").strip()

    foglio = (row.get("foglio") or "").strip()
    numero = (row.get("numero") or "").strip()
    row["foglio"] = foglio or None
    row["numero"] = numero or None

    if not foglio or not numero:
        raise ValueError(
            "Cannot upsert immobile without foglio+numero. "
            f"Got foglio={foglio!r}, numero={numero!r}, sub={row['sub']!r}, visura_cf={visura_cf!r}"
        )

    cols = ["visura_cf"] + list(row.keys())
    placeholders = ", ".join(["%s"] * len(cols))

    update_keys = [k for k in row.keys() if k not in ("created_at", "updated_at")]
    set_sql = ", ".join([f"{k} = EXCLUDED.{k}" for k in update_keys])

    sql = f"""
        INSERT INTO public.immobili ({", ".join(cols)})
        VALUES ({placeholders})
        ON CONFLICT (visura_cf, foglio, numero, sub)
        DO UPDATE SET
            {set_sql}
        RETURNING id;
    """

    params = [visura_cf] + [row[k] for k in row.keys()]

    try:
        logger.debug(
            "[DB] immobile key visura_cf=%s foglio=%r numero=%r sub=%r",
            visura_cf,
            row.get("foglio"),
            row.get("numero"),
            row.get("sub"),
        )
        with conn.cursor() as cur:
            cur.execute(sql, params)
            rec = cur.fetchone()
            if not rec:
                raise RuntimeError("UPSERT immobile did not return id (unexpected).")
            return int(rec[0])

    except Psycopg2Error as e:
        logger.exception(
            "[DB] db_upsert_immobile failed visura_cf=%s foglio=%r numero=%r sub=%r: %s",
            visura_cf,
            foglio,
            numero,
            row.get("sub"),
            e,
        )
        raise


def db_prune_old_immobili_without_contracts(conn, visura_cf: str, keep_ids: List[int], enabled: bool) -> int:
    if not enabled:
        return 0
    if not keep_ids:
        return 0

    with conn.cursor() as cur:
        cur.execute(
            """
            DELETE FROM public.immobili i
            WHERE i.visura_cf=%s
              AND NOT (i.id = ANY(%s))
              AND NOT EXISTS (
                SELECT 1 FROM public.contracts c WHERE c.immobile_id = i.id
              );
            """,
            (visura_cf, keep_ids),
        )
        return cur.rowcount


def db_load_immobili(conn, visura_cf: str) -> List[Tuple[int, Immobile]]:
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute(
            """
            SELECT
              id,
              table_num_immobile,
              sez_urbana,
              foglio,
              numero,
              sub,
              zona_cens,
              micro_zona,
              categoria,
              classe,
              consistenza,
              rendita,
              superficie_totale,
              superficie_escluse,
              superficie_raw,
              immobile_comune,
              immobile_comune_code,
              via_type,
              via_name,
              via_num,
              scala,
              interno,
              piano,
              indirizzo_raw,
              dati_ulteriori
            FROM public.immobili
            WHERE visura_cf=%s
            ORDER BY id;
            """,
            (visura_cf,),
        )
        rows = cur.fetchall()

    out: List[Tuple[int, Immobile]] = []
    for r in rows:
        d = dict(r)
        imm_id = int(d.pop("id"))
        out.append((imm_id, Immobile(**d)))
    return out


def db_get_latest_contract_id(conn, immobile_id: int) -> Optional[str]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT contract_id
            FROM public.contracts
            WHERE immobile_id=%s
            ORDER BY created_at DESC
            LIMIT 1;
            """,
            (immobile_id,),
        )
        r = cur.fetchone()
        return str(r[0]) if r else None


def db_create_contract(conn, immobile_id: int) -> str:
    with conn.cursor() as cur:
        cur.execute("INSERT INTO public.contracts (immobile_id) VALUES (%s) RETURNING contract_id;", (immobile_id,))
        return str(cur.fetchone()[0])


def db_update_contract_fields(conn, contract_id: str, adapter) -> None:
    contract_kind = clean_str(adapter.get("contract_kind"))
    if contract_kind:
        contract_kind = contract_kind.upper()

    start_date = parse_date(adapter.get("decorrenza_data")) or parse_date(adapter.get("contratto_data"))
    durata_anni = None
    if clean_str(adapter.get("durata_anni")):
        try:
            durata_anni = int(str(adapter.get("durata_anni")))
        except Exception:
            durata_anni = None

    arredato = to_bool_or_none(adapter.get("arredato"))
    energy_class = clean_str(adapter.get("energy_class"))
    if energy_class:
        energy_class = energy_class.upper()

    canone_contr = safe_float(adapter.get("canone_contrattuale_mensile"))

    updates: List[str] = []
    params: List[Any] = []

    def add(col: str, val: Any) -> None:
        if val is None:
            return
        updates.append(f"{col}=%s")
        params.append(val)

    add("contract_kind", contract_kind)
    add("start_date", start_date)
    add("durata_anni", durata_anni)
    add("arredato", arredato)
    add("energy_class", energy_class)
    add("canone_contrattuale_mensile", canone_contr)

    if not updates:
        return

    params.append(contract_id)
    with conn.cursor() as cur:
        cur.execute(f"UPDATE public.contracts SET {', '.join(updates)} WHERE contract_id=%s;", params)


def db_upsert_contract_parties(conn, contract_id: str, locatore_cf: str, conduttore_cf: Optional[str]) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO public.contract_parties (contract_id, role, person_cf)
            VALUES (%s, 'LOCATORE', %s)
            ON CONFLICT (contract_id, role) DO UPDATE
            SET person_cf = EXCLUDED.person_cf;
            """,
            (contract_id, locatore_cf),
        )

        if conduttore_cf:
            cur.execute(
                """
                INSERT INTO public.contract_parties (contract_id, role, person_cf)
                VALUES (%s, 'CONDUTTORE', %s)
                ON CONFLICT (contract_id, role) DO UPDATE
                SET person_cf = EXCLUDED.person_cf;
                """,
                (contract_id, conduttore_cf),
            )


def db_upsert_contract_overrides(conn, contract_id: str, adapter) -> None:
    data = {
        "immobile_comune_override": clean_str(adapter.get("immobile_comune")),
        "immobile_via_override": clean_str(adapter.get("immobile_via")),
        "immobile_civico_override": clean_str(adapter.get("immobile_civico")),
        "immobile_piano_override": clean_str(adapter.get("immobile_piano")),
        "immobile_interno_override": clean_str(adapter.get("immobile_interno")),
        "locatore_comune_res": clean_str(adapter.get("locatore_comune_res")),
        "locatore_via": clean_str(adapter.get("locatore_via")),
        "locatore_civico": clean_str(adapter.get("locatore_civico")),
    }

    cols = [k for k, v in data.items() if v is not None]
    if not cols:
        return

    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO public.contract_overrides (contract_id)
            VALUES (%s)
            ON CONFLICT (contract_id) DO NOTHING;
            """,
            (contract_id,),
        )

        set_sql = ", ".join([f"{c}=%s" for c in cols])
        params = [data[c] for c in cols] + [contract_id]
        cur.execute(f"UPDATE public.contract_overrides SET {set_sql} WHERE contract_id=%s;", params)


def db_apply_contract_elements(conn, contract_id: str, adapter) -> None:
    with conn.cursor() as cur:
        for k in ELEMENT_KEYS:
            raw = adapter.get(k)
            if raw is None:
                continue
            val = str(raw).strip()
            if val == "":
                continue

            grp = k[0].upper()
            code = k.upper()

            if val == "-":
                cur.execute(
                    "DELETE FROM public.contract_elements WHERE contract_id=%s AND grp=%s AND code=%s;",
                    (contract_id, grp, code),
                )
            else:
                cur.execute(
                    """
                    INSERT INTO public.contract_elements (contract_id, grp, code, value)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (contract_id, grp, code) DO UPDATE
                    SET value = EXCLUDED.value;
                    """,
                    (contract_id, grp, code, val),
                )


def db_load_contract_context(conn, contract_id: str) -> Dict[str, Any]:
    ctx: Dict[str, Any] = {
        "contract": {},
        "overrides": {},
        "elements": {},
        "parties": {},
        "canone_calc": None,
    }

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT contract_kind, start_date, durata_anni, arredato, energy_class, canone_contrattuale_mensile
            FROM public.contracts
            WHERE contract_id=%s;
            """,
            (contract_id,),
        )
        r = cur.fetchone()
        if r:
            contract_kind, start_date, durata_anni, arredato, energy_class, canone = r
            ctx["contract"] = {
                "contract_kind": contract_kind,
                "start_date": start_date.isoformat() if start_date else None,
                "durata_anni": durata_anni,
                "arredato": arredato,
                "energy_class": energy_class,
                "canone_contrattuale_mensile": str(canone) if canone is not None else None,
            }

        cur.execute(
            """
            SELECT
              immobile_comune_override,
              immobile_via_override,
              immobile_civico_override,
              immobile_piano_override,
              immobile_interno_override,
              locatore_comune_res,
              locatore_via,
              locatore_civico
            FROM public.contract_overrides
            WHERE contract_id=%s;
            """,
            (contract_id,),
        )
        o = cur.fetchone()
        if o:
            (
                ic,
                iv,
                civ,
                p,
                intr,
                lc,
                lv,
                lciv,
            ) = o
            ctx["overrides"] = {
                "immobile_comune_override": ic,
                "immobile_via_override": iv,
                "immobile_civico_override": civ,
                "immobile_piano_override": p,
                "immobile_interno_override": intr,
                "locatore_comune_res": lc,
                "locatore_via": lv,
                "locatore_civico": lciv,
            }

        cur.execute(
            "SELECT grp, code, value FROM public.contract_elements WHERE contract_id=%s;",
            (contract_id,),
        )
        elements: Dict[str, str] = {}
        for grp, code, value in cur.fetchall():
            key = normalize_element_key(str(grp or ""), str(code or ""))
            if not key:
                continue
            elements[key] = "" if value is None else str(value)
        ctx["elements"] = elements

        cur.execute(
            """
            SELECT cp.role, p.cf, p.surname, p.name
            FROM public.contract_parties cp
            JOIN public.persons p ON p.cf = cp.person_cf
            WHERE cp.contract_id = %s;
            """,
            (contract_id,),
        )
        for role, cf, surname, name in cur.fetchall():
            ctx["parties"][role] = {"cf": cf, "surname": surname, "name": name}

        cur.execute(
            """
            SELECT inputs::text
            FROM public.canone_calcoli
            WHERE contract_id=%s
            ORDER BY created_at DESC
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

    logger.info(
        "[DB] ctx loaded contract_id=%s parties=%s elements=%d overrides=%s",
        contract_id,
        list((ctx.get("parties") or {}).keys()),
        len(ctx.get("elements") or {}),
        bool(ctx.get("overrides")),
    )
    return ctx


def db_insert_canone_calc(conn, contract_id: str, method: str, inputs: Dict[str, Any], result_mensile: Optional[float]) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO public.canone_calcoli (contract_id, method, inputs, result_mensile)
            VALUES (%s, %s, %s, %s);
            """,
            (contract_id, method, psycopg2.extras.Json(inputs), result_mensile),
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
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO public.attestazioni (
              contract_id,
              author_login_masked,
              author_login_sha256,
              template_version,
              output_bucket,
              output_object,
              params_snapshot,
              status,
              error
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
            """,
            (
                contract_id,
                author_login_masked,
                author_login_sha256,
                template_version,
                output_bucket,
                output_object,
                psycopg2.extras.Json(params_snapshot),
                status,
                error,
            ),
        )


@dataclass(frozen=True)
class VisuraState:
    cf: str
    pdf_bucket: Optional[str]
    pdf_object: Optional[str]
    fetched_at: Optional[Any]


def fetch_visura_state(conn, cf: str) -> Optional[VisuraState]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT cf, pdf_bucket, pdf_object, fetched_at
            FROM public.visure
            WHERE cf = %s;
            """,
            (cf,),
        )
        row = cur.fetchone()
    if not row:
        return None
    return VisuraState(cf=row[0], pdf_bucket=row[1], pdf_object=row[2], fetched_at=row[3])
