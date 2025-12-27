"""
Scrapy pipelines for the Uppi project.

Тут відбувається "зведення" всіх шарів:
- YAML (clients.yml через map_yaml_to_item),
- SISTER/PDF (VisuraParser),
- БД (збереження/зчитування immobili),
- MinIO (зберігання PDF-візур),
- генерація DOCX-атестацйї,
- розрахунок canone за Accordo Pescara 2018.

Результат — заповнений UppiItem, з яким вже можна:
- робити DOCX,
- будувати звіти,
- дебажити весь флоу.

ВАЖЛИВО: таблиця `immobili` повинна містити, крім уже існуючих полів,
також контрактні поля:

    contract_kind TEXT,                    -- CONCORDATO / TRANSITORIO / STUDENTI
    arredato BOOLEAN,                      -- мебльованість
    energy_class TEXT,                     -- 'A'..'G'
    canone_contrattuale_mensile NUMERIC,   -- фактичний canone з договору
    durata_anni INTEGER                    -- тривалість договору в роках

І dataclass Immobile має мати відповідні атрибути з дефолтами (None).
"""

from __future__ import annotations

from pathlib import Path
from typing import Dict, List, Any

import logging

from itemadapter import ItemAdapter
from minio import Minio
from minio.error import S3Error
from decouple import config

from uppi.domain.db import db_has_visura, _get_pg_connection
from uppi.docs.visura_pdf_parser import VisuraParser
from uppi.domain.storage import get_visura_path, get_attestazione_path
from uppi.docs.attestazione_template_filler import fill_attestazione_template, underscored
from uppi.domain.immobile import Immobile
from uppi.domain.canone_models import CanoneInput, CanoneResult, ContractKind
from uppi.domain.pescara2018_calc import compute_base_canone, CanoneCalculationError

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# MinIO/S3 configuration (зберігання PDF-візур)
# ---------------------------------------------------------------------------

MINIO_ENDPOINT = config("MINIO_ENDPOINT", default="localhost:9000")
MINIO_ACCESS_KEY = config("MINIO_ACCESS_KEY", default="minioadmin")
MINIO_SECRET_KEY = config("MINIO_SECRET_KEY", default="minioadmin")
MINIO_SECURE = config("MINIO_SECURE", default="False").lower() == "true"
MINIO_BUCKET = config("MINIO_BUCKET", default="visure")

_minio_client: Minio | None = None


def _get_minio_client() -> Minio:
    """
    Повертає singleton-клієнт MinIO.
    Створює бакет, якщо він ще не існує (на "нормальному" S3).
    На R2 bucket_exists може не працювати — тоді просто логую і живу далі.
    """
    global _minio_client
    if _minio_client is not None:
        return _minio_client

    try:
        client = Minio(
            MINIO_ENDPOINT,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=MINIO_SECURE,
        )

        try:
            if not client.bucket_exists(MINIO_BUCKET):
                logger.warning(
                    "[MINIO] Bucket %r не існує, створи його вручну (наприклад, у R2 Dashboard).",
                    MINIO_BUCKET,
                )
        except Exception:
            # Cloudflare R2 може не дозволяти ListBuckets → попереджаємо й працюємо далі.
            logger.info("[MINIO] Неможливо перевірити існування bucket через API — продовжуємо всліпу.")

        _minio_client = client
        logger.info("[MINIO] Підключення до %s, bucket=%s готове", MINIO_ENDPOINT, MINIO_BUCKET)
        return _minio_client
    except S3Error as e:
        logger.exception("[MINIO] Помилка роботи з MinIO: %s", e)
        raise
    except Exception as e:
        logger.exception("[MINIO] Неочікувана помилка ініціалізації MinIO: %s", e)
        raise


# ---------------------------------------------------------------------------
# DB helpers: завантаження/збереження visura + immobili
# ---------------------------------------------------------------------------

def load_immobiles_from_db(cf: str) -> List[Immobile]:
    """
    Завантажити всі Immobile для візури конкретного CF з таблиці `immobili`.

    SQL відповідає поточній схемі таблиці `immobili` і полям dataclass `Immobile`.
    """
    conn = None
    try:
        conn = _get_pg_connection()
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    -- базові поля таблиці (з візури)
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
                    superficie_totale,
                    superficie_escluse,
                    superficie_raw,
                    rendita,

                    -- адреса з візури
                    immobile_comune,
                    immobile_comune_code,
                    via_type,
                    via_name,
                    via_num,
                    scala,
                    interno,
                    piano,
                    indirizzo_raw,
                    dati_ulteriori,

                    -- дані локатора з візури
                    locatore_surname,
                    locatore_name,
                    locatore_codice_fiscale,

                    -- OVERRIDE (реальна адреса об'єкта)
                    immobile_comune_override,
                    immobile_via_override,
                    immobile_civico_override,
                    immobile_piano_override,
                    immobile_interno_override,

                    -- адреса локатора (з YAML, збережена в БД)
                    locatore_comune_res,
                    locatore_via,
                    locatore_civico,

                    -- Елементи A/B/C/D
                    a1, a2,
                    b1, b2, b3, b4, b5,
                    c1, c2, c3, c4, c5, c6, c7,
                    d1, d2, d3, d4, d5, d6, d7, d8, d9, d10, d11, d12, d13,

                    -- Підсумкові кількості
                    a_cnt, b_cnt, c_cnt, d_cnt,

                    -- Контрактні параметри (з YAML/БД)
                    contract_kind,
                    arredato,
                    energy_class,
                    canone_contrattuale_mensile,
                    durata_anni
                FROM immobili
                WHERE visura_cf = %s
                ORDER BY id;
                """,
                (cf,),
            )
            rows = cur.fetchall()
    except Exception as e:
        logger.exception("[DB] Не вдалося прочитати immobili для %s: %s", cf, e)
        return []
    finally:
        if conn is not None:
            conn.close()

    immobiles: List[Immobile] = []
    for row in rows:
        (
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
            superficie_totale,
            superficie_escluse,
            superficie_raw,
            rendita,
            immobile_comune,
            immobile_comune_code,
            via_type,
            via_name,
            via_num,
            scala,
            interno,
            piano,
            indirizzo_raw,
            dati_ulteriori,
            locatore_surname,
            locatore_name,
            locatore_codice_fiscale,
            immobile_comune_override,
            immobile_via_override,
            immobile_civico_override,
            immobile_piano_override,
            immobile_interno_override,
            locatore_comune_res,
            locatore_via,
            locatore_civico,
            a1, a2,
            b1, b2, b3, b4, b5,
            c1, c2, c3, c4, c5, c6, c7,
            d1, d2, d3, d4, d5, d6, d7, d8, d9, d10, d11, d12, d13,
            a_cnt, b_cnt, c_cnt, d_cnt,
            contract_kind,
            arredato,
            energy_class,
            canone_contrattuale_mensile,
            durata_anni,
        ) = row

        immobiles.append(
            Immobile(
                table_num_immobile=table_num_immobile,
                sez_urbana=sez_urbana,
                foglio=foglio,
                numero=numero,
                sub=sub,
                zona_cens=zona_cens,
                micro_zona=micro_zona,
                categoria=categoria,
                classe=classe,
                consistenza=consistenza,
                superficie_totale=superficie_totale,
                superficie_escluse=superficie_escluse,
                superficie_raw=superficie_raw,
                rendita=rendita,
                immobile_comune=immobile_comune,
                immobile_comune_code=immobile_comune_code,
                via_type=via_type,
                via_name=via_name,
                via_num=via_num,
                scala=scala,
                interno=interno,
                piano=piano,
                indirizzo_raw=indirizzo_raw,
                dati_ulteriori=dati_ulteriori,
                locatore_surname=locatore_surname,
                locatore_name=locatore_name,
                locatore_codice_fiscale=locatore_codice_fiscale,
                immobile_comune_override=immobile_comune_override,
                immobile_via_override=immobile_via_override,
                immobile_civico_override=immobile_civico_override,
                immobile_piano_override=immobile_piano_override,
                immobile_interno_override=immobile_interno_override,
                locatore_comune_res=locatore_comune_res,
                locatore_via=locatore_via,
                locatore_civico=locatore_civico,
                a1=a1,
                a2=a2,
                b1=b1,
                b2=b2,
                b3=b3,
                b4=b4,
                b5=b5,
                c1=c1,
                c2=c2,
                c3=c3,
                c4=c4,
                c5=c5,
                c6=c6,
                c7=c7,
                d1=d1,
                d2=d2,
                d3=d3,
                d4=d4,
                d5=d5,
                d6=d6,
                d7=d7,
                d8=d8,
                d9=d9,
                d10=d10,
                d11=d11,
                d12=d12,
                d13=d13,
                a_cnt=a_cnt,
                b_cnt=b_cnt,
                c_cnt=c_cnt,
                d_cnt=d_cnt,
                contract_kind=contract_kind,
                arredato=arredato,
                energy_class=energy_class,
                canone_contrattuale_mensile=canone_contrattuale_mensile,
                durata_anni=durata_anni,
            )
        )

    logger.debug("[DB] Завантажено %d immobili для %s", len(immobiles), cf)
    return immobiles


def save_visura(cf: str, immobiles: List[Immobile], pdf_path: Path) -> None:
    """
    Зберігає:
    - PDF-візуру в MinIO (visure/<cf>.pdf),
    - метадані в таблиці visure,
    - усі immobili в таблиці immobili.

    Після успішного запису локальний PDF видаляється.

    при FORCE_UPDATE_VISURA старі override-и, A/B/C/D, адреса локатора та
    контрактні поля НЕ губляться — підтягуємо їх зі старих записів.
    """
    object_name = f"visure/{cf}.pdf"

    # 1. Заливаємо PDF у MinIO
    try:
        client = _get_minio_client()
        logger.info(
            "[MINIO] Завантажуємо PDF візури для %s у bucket=%s, object=%s",
            cf,
            MINIO_BUCKET,
            object_name,
        )
        client.fput_object(
            bucket_name=MINIO_BUCKET,
            object_name=object_name,
            file_path=str(pdf_path),
            content_type="application/pdf",
        )
    except S3Error as e:
        logger.exception("[MINIO] Помилка при завантаженні PDF для %s: %s", cf, e)
        raise
    except Exception as e:
        logger.exception("[MINIO] Неочікувана помилка при завантаженні PDF для %s: %s", cf, e)
        raise

    conn = None
    try:
        conn = _get_pg_connection()
        with conn:
            with conn.cursor() as cur:
                # 0. Зчитуємо старі override-и, адресу локатора, A/B/C/D та контрактні поля
                cur.execute(
                    """
                    SELECT
                        foglio,
                        numero,
                        sub,
                        immobile_comune_override,
                        immobile_via_override,
                        immobile_civico_override,
                        immobile_piano_override,
                        immobile_interno_override,
                        locatore_comune_res,
                        locatore_via,
                        locatore_civico,
                        a1, a2,
                        b1, b2, b3, b4, b5,
                        c1, c2, c3, c4, c5, c6, c7,
                        d1, d2, d3, d4, d5, d6, d7, d8, d9, d10, d11, d12, d13,
                        a_cnt, b_cnt, c_cnt, d_cnt,
                        contract_kind,
                        arredato,
                        energy_class,
                        canone_contrattuale_mensile,
                        durata_anni
                    FROM immobili
                    WHERE visura_cf = %s;
                    """,
                    (cf,),
                )
                old_rows = cur.fetchall()
                old_map: dict[tuple[str, str, str], dict[str, Any]] = {}

                for (
                    foglio,
                    numero,
                    sub,
                    immobile_com_override,
                    immobile_via_override,
                    immobile_civico_override,
                    immobile_piano_override,
                    immobile_interno_override,
                    locatore_comune_res,
                    locatore_via,
                    locatore_civico,
                    a1, a2,
                    b1, b2, b3, b4, b5,
                    c1, c2, c3, c4, c5, c6, c7,
                    d1, d2, d3, d4, d5, d6, d7, d8, d9, d10, d11, d12, d13,
                    a_cnt, b_cnt, c_cnt, d_cnt,
                    contract_kind,
                    arredato,
                    energy_class,
                    canone_contrattuale_mensile,
                    durata_anni,
                ) in old_rows:
                    key = (
                        str(foglio) if foglio is not None else "",
                        str(numero) if numero is not None else "",
                        str(sub) if sub is not None else "",
                    )
                    old_map[key] = {
                        "immobile_comune_override": immobile_com_override,
                        "immobile_via_override": immobile_via_override,
                        "immobile_civico_override": immobile_civico_override,
                        "immobile_piano_override": immobile_piano_override,
                        "immobile_interno_override": immobile_interno_override,
                        "locatore_comune_res": locatore_comune_res,
                        "locatore_via": locatore_via,
                        "locatore_civico": locatore_civico,
                        "a1": a1,
                        "a2": a2,
                        "b1": b1,
                        "b2": b2,
                        "b3": b3,
                        "b4": b4,
                        "b5": b5,
                        "c1": c1,
                        "c2": c2,
                        "c3": c3,
                        "c4": c4,
                        "c5": c5,
                        "c6": c6,
                        "c7": c7,
                        "d1": d1,
                        "d2": d2,
                        "d3": d3,
                        "d4": d4,
                        "d5": d5,
                        "d6": d6,
                        "d7": d7,
                        "d8": d8,
                        "d9": d9,
                        "d10": d10,
                        "d11": d11,
                        "d12": d12,
                        "d13": d13,
                        "a_cnt": a_cnt,
                        "b_cnt": b_cnt,
                        "c_cnt": c_cnt,
                        "d_cnt": d_cnt,
                        "contract_kind": contract_kind,
                        "arredato": arredato,
                        "energy_class": energy_class,
                        "canone_contrattuale_mensile": canone_contrattuale_mensile,
                        "durata_anni": durata_anni,
                    }

                # 1. upsert у visure (метадані PDF)
                cur.execute(
                    """
                    INSERT INTO visure (cf, pdf_bucket, pdf_object, updated_at)
                    VALUES (%s, %s, %s, now())
                    ON CONFLICT (cf) DO UPDATE
                    SET pdf_bucket = EXCLUDED.pdf_bucket,
                        pdf_object = EXCLUDED.pdf_object,
                        updated_at = EXCLUDED.updated_at;
                    """,
                    (cf, MINIO_BUCKET, object_name),
                )

                # 2. чистимо старі immobili для цього cf
                cur.execute("DELETE FROM immobili WHERE visura_cf = %s;", (cf,))

                insert_sql = """
                    INSERT INTO immobili (
                        visura_cf,
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
                        superficie_totale,
                        superficie_escluse,
                        superficie_raw,
                        rendita,
                        immobile_comune,
                        immobile_comune_code,
                        via_type,
                        via_name,
                        via_num,
                        scala,
                        interno,
                        piano,
                        indirizzo_raw,
                        dati_ulteriori,
                        locatore_surname,
                        locatore_name,
                        locatore_codice_fiscale,
                        immobile_comune_override,
                        immobile_via_override,
                        immobile_civico_override,
                        immobile_piano_override,
                        immobile_interno_override,
                        locatore_comune_res,
                        locatore_via,
                        locatore_civico,
                        a1, a2,
                        b1, b2, b3, b4, b5,
                        c1, c2, c3, c4, c5, c6, c7,
                        d1, d2, d3, d4, d5, d6, d7, d8, d9, d10, d11, d12, d13,
                        a_cnt, b_cnt, c_cnt, d_cnt,
                        contract_kind,
                        arredato,
                        energy_class,
                        canone_contrattuale_mensile,
                        durata_anni
                    ) VALUES (
                        %s,  -- visura_cf
                        %s,  -- table_num_immobile
                        %s,  -- sez_urbana
                        %s,  -- foglio
                        %s,  -- numero
                        %s,  -- sub
                        %s,  -- zona_cens
                        %s,  -- micro_zona
                        %s,  -- categoria
                        %s,  -- classe
                        %s,  -- consistenza
                        %s,  -- superficie_totale
                        %s,  -- superficie_escluse
                        %s,  -- superficie_raw
                        %s,  -- rendita
                        %s,  -- immobile_comune
                        %s,  -- immobile_comune_code
                        %s,  -- via_type
                        %s,  -- via_name
                        %s,  -- via_num
                        %s,  -- scala
                        %s,  -- interno
                        %s,  -- piano
                        %s,  -- indirizzo_raw
                        %s,  -- dati_ulteriori
                        %s,  -- locatore_surname
                        %s,  -- locatore_name
                        %s,  -- locatore_codice_fiscale
                        %s,  -- immobile_comune_override
                        %s,  -- immobile_via_override
                        %s,  -- immobile_civico_override
                        %s,  -- immobile_piano_override
                        %s,  -- immobile_interno_override
                        %s,  -- locatore_comune_res
                        %s,  -- locatore_via
                        %s,  -- locatore_civico
                        %s,  -- a1
                        %s,  -- a2
                        %s,  -- b1
                        %s,  -- b2
                        %s,  -- b3
                        %s,  -- b4
                        %s,  -- b5
                        %s,  -- c1
                        %s,  -- c2
                        %s,  -- c3
                        %s,  -- c4
                        %s,  -- c5
                        %s,  -- c6
                        %s,  -- c7
                        %s,  -- d1
                        %s,  -- d2
                        %s,  -- d3
                        %s,  -- d4
                        %s,  -- d5
                        %s,  -- d6
                        %s,  -- d7
                        %s,  -- d8
                        %s,  -- d9
                        %s,  -- d10
                        %s,  -- d11
                        %s,  -- d12
                        %s,  -- d13
                        %s,  -- a_cnt
                        %s,  -- b_cnt
                        %s,  -- c_cnt
                        %s,  -- d_cnt
                        %s,  -- contract_kind
                        %s,  -- arredato
                        %s,  -- energy_class
                        %s,  -- canone_contrattuale_mensile
                        %s   -- durata_anni
                    );
                """

                for imm in immobiles:
                    # Підтягуємо старі override-и, A/B/C/D, адресу локатора і контрактні поля, якщо вони були
                    key = (
                        str(getattr(imm, "foglio", "") or ""),
                        str(getattr(imm, "numero", "") or ""),
                        str(getattr(imm, "sub", "") or ""),
                    )
                    old = old_map.get(key)
                    if old:
                        for field in (
                            "immobile_comune_override",
                            "immobile_via_override",
                            "immobile_civico_override",
                            "immobile_piano_override",
                            "immobile_interno_override",
                            "locatore_comune_res",
                            "locatore_via",
                            "locatore_civico",
                            "a1", "a2",
                            "b1", "b2", "b3", "b4", "b5",
                            "c1", "c2", "c3", "c4", "c5", "c6", "c7",
                            "d1", "d2", "d3", "d4", "d5", "d6", "d7",
                            "d8", "d9", "d10", "d11", "d12", "d13",
                            "a_cnt", "b_cnt", "c_cnt", "d_cnt",
                            "contract_kind",
                            "arredato",
                            "energy_class",
                            "canone_contrattuale_mensile",
                            "durata_anni",
                        ):
                            current_val = getattr(imm, field, None)
                            if current_val in (None, "") and old.get(field) is not None:
                                setattr(imm, field, old[field])

                    cur.execute(
                        insert_sql,
                        (
                            cf,
                            getattr(imm, "table_num_immobile", None),
                            getattr(imm, "sez_urbana", None),
                            getattr(imm, "foglio", None),
                            getattr(imm, "numero", None),
                            getattr(imm, "sub", None),
                            getattr(imm, "zona_cens", None),
                            getattr(imm, "micro_zona", None),
                            getattr(imm, "categoria", None),
                            getattr(imm, "classe", None),
                            getattr(imm, "consistenza", None),
                            getattr(imm, "superficie_totale", None),
                            getattr(imm, "superficie_escluse", None),
                            getattr(imm, "superficie_raw", None),
                            getattr(imm, "rendita", None),
                            getattr(imm, "immobile_comune", None),
                            getattr(imm, "immobile_comune_code", None),
                            getattr(imm, "via_type", None),
                            getattr(imm, "via_name", None),
                            getattr(imm, "via_num", None),
                            getattr(imm, "scala", None),
                            getattr(imm, "interno", None),
                            getattr(imm, "piano", None),
                            getattr(imm, "indirizzo_raw", None),
                            getattr(imm, "dati_ulteriori", None),
                            getattr(imm, "locatore_surname", None),
                            getattr(imm, "locatore_name", None),
                            getattr(imm, "locatore_codice_fiscale", None),
                            getattr(imm, "immobile_comune_override", None),
                            getattr(imm, "immobile_via_override", None),
                            getattr(imm, "immobile_civico_override", None),
                            getattr(imm, "immobile_piano_override", None),
                            getattr(imm, "immobile_interno_override", None),
                            getattr(imm, "locatore_comune_res", None),
                            getattr(imm, "locatore_via", None),
                            getattr(imm, "locatore_civico", None),
                            getattr(imm, "a1", None),
                            getattr(imm, "a2", None),
                            getattr(imm, "b1", None),
                            getattr(imm, "b2", None),
                            getattr(imm, "b3", None),
                            getattr(imm, "b4", None),
                            getattr(imm, "b5", None),
                            getattr(imm, "c1", None),
                            getattr(imm, "c2", None),
                            getattr(imm, "c3", None),
                            getattr(imm, "c4", None),
                            getattr(imm, "c5", None),
                            getattr(imm, "c6", None),
                            getattr(imm, "c7", None),
                            getattr(imm, "d1", None),
                            getattr(imm, "d2", None),
                            getattr(imm, "d3", None),
                            getattr(imm, "d4", None),
                            getattr(imm, "d5", None),
                            getattr(imm, "d6", None),
                            getattr(imm, "d7", None),
                            getattr(imm, "d8", None),
                            getattr(imm, "d9", None),
                            getattr(imm, "d10", None),
                            getattr(imm, "d11", None),
                            getattr(imm, "d12", None),
                            getattr(imm, "d13", None),
                            getattr(imm, "a_cnt", None),
                            getattr(imm, "b_cnt", None),
                            getattr(imm, "c_cnt", None),
                            getattr(imm, "d_cnt", None),
                            getattr(imm, "contract_kind", None),
                            getattr(imm, "arredato", None),
                            getattr(imm, "energy_class", None),
                            getattr(imm, "canone_contrattuale_mensile", None),
                            getattr(imm, "durata_anni", None),
                        ),
                    )

        logger.info(
            "[DB] Збережено visura + %d immobili для %s (PDF: %s/%s)",
            len(immobiles),
            cf,
            MINIO_BUCKET,
            object_name,
        )
    except Exception as e:
        logger.exception("[DB] Помилка при збереженні visura для %s: %s", cf, e)
        raise
    finally:
        if conn is not None:
            conn.close()
        try:
            pdf_path.unlink()
            logger.debug("[PIPELINE] Локальний PDF видалено: %s", pdf_path)
        except FileNotFoundError:
            logger.debug("[PIPELINE] Локальний PDF вже відсутній: %s", pdf_path)
        except Exception as e:
            logger.warning("[PIPELINE] Не вдалося видалити локальний PDF %s: %s", pdf_path, e)


# ---------------------------------------------------------------------------
# YAML helpers / upserts
# ---------------------------------------------------------------------------

def _clean_str(value: Any) -> str | None:
    """
    Приводить до рядка й відкидає "порожні" значення.

    None       -> None
    "" / "  "  -> None
    інше       -> stripped str(...)
    """
    if value is None:
        return None
    s = str(value).strip()
    return s or None


def _to_bool_or_none(value: Any) -> bool | None:
    """
    Акуратне приведення до bool:

    - None / ""      -> None
    - bool           -> як є
    - 0 / 1          -> False / True
    - "true"/"yes"/"1" (регістр не важливий)  -> True
    - "false"/"no"/"0" -> False
    - інше           -> None (лог не ламаємо)
    """
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)

    s = str(value).strip().lower()
    if s in ("true", "yes", "y", "1"):
        return True
    if s in ("false", "no", "n", "0"):
        return False
    return None


def upsert_overrides_from_yaml(cf: str, adapter: ItemAdapter) -> None:
    """
    Оновлює в таблиці `immobili`:
    - реальну адресу об'єкта (immobile_*_override) — ТІЛЬКИ для конкретного immobile;
    - адресу локатора (locatore_*) — ДЛЯ ВСІХ immobili з цим CF.

    Логіка:
    1) якщо в YAML немає жодного IMMOBILE_* і жодного LOCATORE_* → нічого не робимо;
    2) якщо є IMMOBILE_*:
         - якщо у CF кілька immobili і немає FOGLIO/NUMERO/SUB → не оновлюємо override-адресу;
         - інакше оновлюємо ті рядки, що відповідають ключу (foglio/numero/sub);
    3) якщо є LOCATORE_*:
         - оновлюємо locatore_* для ВСІХ рядків visura_cf = cf.
    """

    # --- нормалізація YAML-значень ---
    immobile_comune = _clean_str(adapter.get("immobile_comune"))
    immobile_via = _clean_str(adapter.get("immobile_via"))
    immobile_civico = _clean_str(adapter.get("immobile_civico"))
    immobile_piano = _clean_str(adapter.get("immobile_piano"))
    immobile_interno = _clean_str(adapter.get("immobile_interno"))

    locatore_comune_res = _clean_str(adapter.get("locatore_comune_res"))
    locatore_via = _clean_str(adapter.get("locatore_via"))
    locatore_civico = _clean_str(adapter.get("locatore_civico"))

    any_obj_override = any(
        v is not None
        for v in (
            immobile_comune,
            immobile_via,
            immobile_civico,
            immobile_piano,
            immobile_interno,
        )
    )
    any_locatore = any(
        v is not None for v in (locatore_comune_res, locatore_via, locatore_civico)
    )

    # Немає жодних даних для оновлення
    if not any_obj_override and not any_locatore:
        return

    conn = None
    try:
        conn = _get_pg_connection()
        with conn:
            with conn.cursor() as cur:
                # Дивимось, скільки immobili є для цього CF
                cur.execute(
                    """
                    SELECT id, foglio, numero, sub
                    FROM immobili
                    WHERE visura_cf = %s;
                    """,
                    (cf,),
                )
                rows = cur.fetchall()

                if not rows:
                    logger.warning(
                        "[DB] upsert_overrides_from_yaml: для %s немає записів у immobili "
                        "(visura ще не збережена?)",
                        cf,
                    )
                    return

                # -----------------------------
                # 1) OVERRIDE РЕАЛЬНОЇ АДРЕСИ ОБ'ЄКТА
                # -----------------------------
                if any_obj_override:
                    foglio = _clean_str(adapter.get("foglio"))
                    numero = _clean_str(adapter.get("numero"))
                    sub = _clean_str(adapter.get("sub"))

                    where_clauses = ["visura_cf = %s"]
                    where_params: List[Any] = [cf]

                    if foglio is not None:
                        where_clauses.append("foglio = %s")
                        where_params.append(foglio)
                    if numero is not None:
                        where_clauses.append("numero = %s")
                        where_params.append(numero)
                    if sub is not None:
                        where_clauses.append("sub = %s")
                        where_params.append(sub)

                    # Немає FOGLIO/NUMERO/SUB, але кілька об'єктів — не знаємо, який оновлювати
                    if len(where_clauses) == 1 and len(rows) > 1:
                        logger.warning(
                            "[DB] upsert_overrides_from_yaml: для %s є %d immobili, "
                            "але FOGLIO/NUMERO/SUB у YAML не задані — "
                            "пропускаємо оновлення реальної адреси об'єкта, "
                            "щоб не присвоїти одну адресу всім об'єктам.",
                            cf,
                            len(rows),
                        )
                    else:
                        sql_obj = f"""
                            UPDATE immobili
                            SET
                                immobile_comune_override  = COALESCE(%s, immobile_comune_override),
                                immobile_via_override     = COALESCE(%s, immobile_via_override),
                                immobile_civico_override  = COALESCE(%s, immobile_civico_override),
                                immobile_piano_override   = COALESCE(%s, immobile_piano_override),
                                immobile_interno_override = COALESCE(%s, immobile_interno_override)
                            WHERE {" AND ".join(where_clauses)};
                        """
                        params_obj: List[Any] = [
                            immobile_comune,
                            immobile_via,
                            immobile_civico,
                            immobile_piano,
                            immobile_interno,
                            *where_params,
                        ]
                        cur.execute(sql_obj, params_obj)
                        logger.debug(
                            "[DB] upsert_overrides_from_yaml: для %s оновлено override-адресу об'єкта (%d рядків)",
                            cf,
                            cur.rowcount,
                        )

                # -----------------------------
                # 2) АДРЕСА ЛОКАТОРА ДЛЯ ВСІХ IMMOBILI ЦЬОГО CF
                # -----------------------------
                if any_locatore:
                    sql_loc = """
                        UPDATE immobili
                        SET
                            locatore_comune_res = COALESCE(%s, locatore_comune_res),
                            locatore_via        = COALESCE(%s, locatore_via),
                            locatore_civico     = COALESCE(%s, locatore_civico)
                        WHERE visura_cf = %s;
                    """
                    cur.execute(
                        sql_loc,
                        [locatore_comune_res, locatore_via, locatore_civico, cf],
                    )
                    logger.debug(
                        "[DB] upsert_overrides_from_yaml: для %s оновлено адресу локатора (%d рядків)",
                        cf,
                        cur.rowcount,
                    )

    except Exception as e:
        logger.exception("[DB] Помилка в upsert_overrides_from_yaml(%s): %s", cf, e)
    finally:
        if conn is not None:
            conn.close()


def upsert_elements_from_yaml(cf: str, adapter: ItemAdapter) -> None:
    """
    Оновлює в таблиці `immobili` поля A1..D13 і підсумкові A_CNT..D_CNT для заданого CF.

    Логіка:
    - значення з YAML → записуються в БД;
    - якщо замість значення передано "-" → відповідне поле в БД очищується (NULL);
    - якщо значення в YAML немає (ключ відсутній або порожній рядок) → БД не чіпаємо;
    - після оновлення A/B/C/D перераховуємо A_CNT..D_CNT на основі не-NULL полів.

    Якщо для CF кілька immobili і в YAML немає FOGLIO/NUMERO/SUB —
    нічого не оновлюємо (щоб не розмазувати одну конфігурацію по всіх об'єктах).
    """
    # Ключі елементів
    element_keys = [
        "a1", "a2",
        "b1", "b2", "b3", "b4", "b5",
        "c1", "c2", "c3", "c4", "c5", "c6", "c7",
        "d1", "d2", "d3", "d4", "d5", "d6",
        "d7", "d8", "d9", "d10", "d11", "d12", "d13",
    ]

    # Збираємо зміни з YAML
    pending: dict[str, tuple[str, str | None]] = {}  # key -> ("set"/"clear", value)

    for key in element_keys:
        raw = adapter.get(key)
        if raw is None:
            continue

        if isinstance(raw, str):
            val = raw.strip()
        else:
            val = str(raw).strip()

        if val == "":
            # Порожнє → вважаємо "немає оновлення"
            continue

        if val == "-":
            # Спеціальний випадок: видалення елемента з БД
            pending[key] = ("clear", None)
        else:
            # Будь-яке непорожнє значення → зберігаємо як є (YAML має пріоритет)
            pending[key] = ("set", val)

    if not pending:
        # У YAML немає жодних A/B/C/D → нічого не робимо
        return

    conn = None
    try:
        conn = _get_pg_connection()
        with conn:
            with conn.cursor() as cur:
                # Дивимось, скільки immobili є для цього CF
                cur.execute(
                    """
                    SELECT id, foglio, numero, sub
                    FROM immobili
                    WHERE visura_cf = %s;
                    """,
                    (cf,),
                )
                rows = cur.fetchall()

                if not rows:
                    logger.warning(
                        "[DB] upsert_elements_from_yaml: для %s немає записів у immobili "
                        "(visура ще не збережена?)",
                        cf,
                    )
                    return

                foglio = _clean_str(adapter.get("foglio"))
                numero = _clean_str(adapter.get("numero"))
                sub = _clean_str(adapter.get("sub"))

                where_clauses = ["visura_cf = %s"]
                where_params: List[Any] = [cf]

                if foglio is not None:
                    where_clauses.append("foglio = %s")
                    where_params.append(foglio)
                if numero is not None:
                    where_clauses.append("numero = %s")
                    where_params.append(numero)
                if sub is not None:
                    where_clauses.append("sub = %s")
                    where_params.append(sub)

                # Якщо є кілька об'єктів і немає foglio/numero/sub — не знаємо, що оновлювати
                if len(where_clauses) == 1 and len(rows) > 1:
                    logger.warning(
                        "[DB] upsert_elements_from_yaml: для %s є %d immobili, "
                        "але FOGLIO/NUMERO/SUB у YAML не задані — "
                        "пропускаємо оновлення A/B/C/D, "
                        "щоб не застосувати один набір елементів до всіх об'єктів.",
                        cf,
                        len(rows),
                    )
                    return

                # Будуємо SET-частину
                set_clauses: List[str] = []
                set_params: List[Any] = []

                for col, (action, value) in pending.items():
                    col_name = col  # у БД ті ж імена

                    if action == "clear":
                        set_clauses.append(f"{col_name} = NULL")
                    else:
                        set_clauses.append(f"{col_name} = %s")
                        set_params.append(value)

                if not set_clauses:
                    return

                sql_update_elements = f"""
                    UPDATE immobili
                    SET {', '.join(set_clauses)}
                    WHERE {" AND ".join(where_clauses)};
                """
                params = set_params + where_params
                cur.execute(sql_update_elements, params)
                logger.debug(
                    "[DB] upsert_elements_from_yaml: для %s оновлено A/B/C/D (%d рядків)",
                    cf,
                    cur.rowcount,
                )

                # Перераховуємо A_CNT..D_CNT для всіх immobili цього CF
                cur.execute(
                    """
                    UPDATE immobili
                    SET
                        a_cnt = (
                            CASE WHEN a1 IS NOT NULL THEN 1 ELSE 0 END +
                            CASE WHEN a2 IS NOT NULL THEN 1 ELSE 0 END
                        ),
                        b_cnt = (
                            CASE WHEN b1 IS NOT NULL THEN 1 ELSE 0 END +
                            CASE WHEN b2 IS NOT NULL THEN 1 ELSE 0 END +
                            CASE WHEN b3 IS NOT NULL THEN 1 ELSE 0 END +
                            CASE WHEN b4 IS NOT NULL THEN 1 ELSE 0 END +
                            CASE WHEN b5 IS NOT NULL THEN 1 ELSE 0 END
                        ),
                        c_cnt = (
                            CASE WHEN c1 IS NOT NULL THEN 1 ELSE 0 END +
                            CASE WHEN c2 IS NOT NULL THEN 1 ELSE 0 END +
                            CASE WHEN c3 IS NOT NULL THEN 1 ELSE 0 END +
                            CASE WHEN c4 IS NOT NULL THEN 1 ELSE 0 END +
                            CASE WHEN c5 IS NOT NULL THEN 1 ELSE 0 END +
                            CASE WHEN c6 IS NOT NULL THEN 1 ELSE 0 END +
                            CASE WHEN c7 IS NOT NULL THEN 1 ELSE 0 END
                        ),
                        d_cnt = (
                            CASE WHEN d1 IS NOT NULL THEN 1 ELSE 0 END +
                            CASE WHEN d2 IS NOT NULL THEN 1 ELSE 0 END +
                            CASE WHEN d3 IS NOT NULL THEN 1 ELSE 0 END +
                            CASE WHEN d4 IS NOT NULL THEN 1 ELSE 0 END +
                            CASE WHEN d5 IS NOT NULL THEN 1 ELSE 0 END +
                            CASE WHEN d6 IS NOT NULL THEN 1 ELSE 0 END +
                            CASE WHEN d7 IS NOT NULL THEN 1 ELSE 0 END +
                            CASE WHEN d8 IS NOT NULL THEN 1 ELSE 0 END +
                            CASE WHEN d9 IS NOT NULL THEN 1 ELSE 0 END +
                            CASE WHEN d10 IS NOT NULL THEN 1 ELSE 0 END +
                            CASE WHEN d11 IS NOT NULL THEN 1 ELSE 0 END +
                            CASE WHEN d12 IS NOT NULL THEN 1 ELSE 0 END +
                            CASE WHEN d13 IS NOT NULL THEN 1 ELSE 0 END
                        )
                    WHERE visura_cf = %s;
                    """,
                    (cf,),
                )
                logger.debug(
                    "[DB] upsert_elements_from_yaml: для %s перераховано A_CNT..D_CNT (%d рядків)",
                    cf,
                    cur.rowcount,
                )

    except Exception as e:
        logger.exception("[DB] Помилка в upsert_elements_from_yaml(%s): %s", cf, e)
    finally:
        if conn is not None:
            conn.close()


def upsert_contract_from_yaml(cf: str, adapter: ItemAdapter) -> None:
    """
    Оновлює контрактні поля в таблиці `immobili` для заданого CF:

        contract_kind                  (TEXT)
        arredato                       (BOOLEAN)
        energy_class                   (TEXT)
        canone_contrattuale_mensile    (NUMERIC)
        durata_anni                    (INTEGER)

    Логіка:
    - YAML має пріоритет над БД: якщо значення задане в YAML → воно записується;
    - якщо значення в YAML немає (ключ відсутній або порожній) → відповідне поле НЕ чіпаємо;
    - Якщо для CF кілька immobili і немає FOGLIO/NUMERO/SUB —
      оновлення НЕ виконуємо (щоб не підписати одним контрактом всі об'єкти).
    """
    # Забираємо значення з adapter
    raw_kind = _clean_str(adapter.get("contract_kind"))
    raw_arredato = adapter.get("arredato")
    raw_energy = _clean_str(adapter.get("energy_class"))
    raw_canone = adapter.get("canone_contrattuale_mensile")
    raw_durata = adapter.get("durata_anni")

    contract_kind = raw_kind.upper() if raw_kind else None
    arredato = _to_bool_or_none(raw_arredato)
    energy_class = raw_energy.upper() if raw_energy else None

    canone_contrattuale_mensile: float | None
    if raw_canone in (None, ""):
        canone_contrattuale_mensile = None
    else:
        try:
            canone_contrattuale_mensile = float(raw_canone)
        except (TypeError, ValueError):
            logger.warning(
                "[DB] upsert_contract_from_yaml: некоректне CANONE_CONTRATTUALE_MENSILE=%r — ігнорую",
                raw_canone,
            )
            canone_contrattuale_mensile = None

    durata_anni: int | None
    if raw_durata in (None, ""):
        durata_anni = None
    else:
        try:
            durata_anni = int(raw_durata)
        except (TypeError, ValueError):
            logger.warning(
                "[DB] upsert_contract_from_yaml: некоректне DURATA_ANNI=%r — ігнорую",
                raw_durata,
            )
            durata_anni = None

    # Якщо в YAML немає жодного контрактного поля — нічого не оновлюємо
    if (
        contract_kind is None
        and arredato is None
        and energy_class is None
        and canone_contrattuale_mensile is None
        and durata_anni is None
    ):
        return

    conn = None
    try:
        conn = _get_pg_connection()
        with conn:
            with conn.cursor() as cur:
                # Дивимось, скільки immobili є для цього CF
                cur.execute(
                    """
                    SELECT id, foglio, numero, sub
                    FROM immobili
                    WHERE visura_cf = %s;
                    """,
                    (cf,),
                )
                rows = cur.fetchall()
                if not rows:
                    logger.warning(
                        "[DB] upsert_contract_from_yaml: для %s немає записів у immobili "
                        "(visura ще не збережена?)",
                        cf,
                    )
                    return

                foglio = _clean_str(adapter.get("foglio"))
                numero = _clean_str(adapter.get("numero"))
                sub = _clean_str(adapter.get("sub"))

                where_clauses = ["visura_cf = %s"]
                where_params: List[Any] = [cf]

                if foglio is not None:
                    where_clauses.append("foglio = %s")
                    where_params.append(foglio)
                if numero is not None:
                    where_clauses.append("numero = %s")
                    where_params.append(numero)
                if sub is not None:
                    where_clauses.append("sub = %s")
                    where_params.append(sub)

                # Якщо кілька об'єктів і немає foglio/numero/sub — не підписуємо все під один контракт
                if len(where_clauses) == 1 and len(rows) > 1:
                    logger.warning(
                        "[DB] upsert_contract_from_yaml: для %s є %d immobili, "
                        "але FOGLIO/NUMERO/SUB у YAML не задані — "
                        "пропускаємо оновлення контрактних полів.",
                        cf,
                        len(rows),
                    )
                    return

                sql = f"""
                    UPDATE immobili
                    SET
                        contract_kind               = COALESCE(%s, contract_kind),
                        arredato                    = COALESCE(%s, arredato),
                        energy_class                = COALESCE(%s, energy_class),
                        canone_contrattuale_mensile = COALESCE(%s, canone_contrattuale_mensile),
                        durata_anni                 = COALESCE(%s, durata_anni)
                    WHERE {" AND ".join(where_clauses)};
                """
                params = [
                    contract_kind,
                    arredato,
                    energy_class,
                    canone_contrattuale_mensile,
                    durata_anni,
                    *where_params,
                ]
                cur.execute(sql, params)
                logger.debug(
                    "[DB] upsert_contract_from_yaml: для %s оновлено контрактні поля (%d рядків)",
                    cf,
                    cur.rowcount,
                )

    except Exception as e:
        logger.exception("[DB] Помилка в upsert_contract_from_yaml(%s): %s", cf, e)
    finally:
        if conn is not None:
            conn.close()


# ---------------------------------------------------------------------------
# Фільтрація Immobile за бажаними критеріями
# ---------------------------------------------------------------------------

def filter_immobiles(immobiles: List[Immobile], adapter: ItemAdapter) -> List[Immobile]:
    """
    Фільтрує список Immobile на основі параметрів в item:
    foglio, numero, sub, rendita, superficie_totale, categoria.

    Порожні/відсутні параметри ігноруються.
    """
    filtered: List[Immobile] = []
    for imm in immobiles:
        ok = True

        foglio = adapter.get("foglio")
        if foglio and imm.foglio != str(foglio):
            ok = False

        numero = adapter.get("numero")
        if numero and imm.numero != str(numero):
            ok = False

        sub = adapter.get("sub")
        if sub and imm.sub != str(sub):
            ok = False

        categoria = adapter.get("categoria")
        if categoria and imm.categoria != str(categoria):
            ok = False

        rendita = adapter.get("rendita")
        if rendita and imm.rendita != str(rendita):
            ok = False

        superficie = adapter.get("superficie_totale")
        if (
            superficie
            and imm.superficie_totale is not None
            and float(superficie) != imm.superficie_totale
        ):
            ok = False

        if ok:
            filtered.append(imm)

    logger.debug(
        "[PIPELINE] Відібрано %d/%d immobili згідно критеріїв item",
        len(filtered),
        len(immobiles),
    )
    return filtered


# ---------------------------------------------------------------------------
# Побудова params для шаблону attestazione
# ---------------------------------------------------------------------------

def _to_str(value: Any) -> str:
    """
    Безпечне приведення до рядка:
    - None -> ""
    - інші типи -> str(...)
    """
    if value is None:
        return ""
    return str(value)


def _pick_override(imm: Immobile, override_attr: str) -> str:
    """
    Для полів реальної адреси об'єкта:
    беремо ТІЛЬКИ значення override з БД (imm.immobile_*_override).
    Якщо його немає — повертаємо "" (у шаблоні буде тільки підкреслення).
    """
    val = getattr(imm, override_attr, None)
    return _to_str(val)


def _pick_locatore_field(
    imm: Immobile,
    adapter: ItemAdapter,
    yaml_field: str,
    imm_attr: str,
) -> str:
    """
    Для адреси локатора:
    1) YAML у поточному запуску (локатор може змінити адресу)
    2) якщо YAML немає — беремо те, що вже лежить у БД (imm.locatore_*)
    3) інакше "".
    """
    yaml_val = adapter.get(yaml_field)
    if yaml_val not in (None, ""):
        return _to_str(yaml_val)

    db_val = getattr(imm, imm_attr, None)
    if db_val not in (None, ""):
        return _to_str(db_val)

    return ""


def build_params(
    adapter: ItemAdapter,
    imm: Immobile,
    canone: CanoneResult | None = None,
) -> Dict[str, str]:
    """
    Формує dict params → значення для заповнення DOCX-шаблону атестації.

    ВАЖЛИВО:
    - Адреса об'єкта — ТІЛЬКИ з imm.immobile_*_override (БД), YAML сюди не лізе.
    - A/B/C/D елементи — також з БД (Immobile), YAML лише оновлює БД перед цим.
    - CALCOLO DEL CANONE (CAN_*) заповнюється на основі CanoneResult + фактичного
      canone з договору (YAML/БД).
    """
    params: Dict[str, str] = {}

    # -----------------------------
    # LOCATORE (власник)
    # -----------------------------
    loc_cf = adapter.get("locatore_cf") or getattr(imm, "locatore_codice_fiscale", None)
    params["{{LOCATORE_CF}}"] = _to_str(loc_cf)

    surname = imm.locatore_surname
    name = imm.locatore_name
    # Спочатку ім'я, потім прізвище
    locatore_nome = " ".join(p for p in (name, surname) if p)
    params["{{LOCATORE_NOME}}"] = _to_str(locatore_nome)

    params["{{LOCATORE_COMUNE_RES}}"] = _pick_locatore_field(
        imm, adapter, "locatore_comune_res", "locatore_comune_res"
    )
    params["{{LOCATORE_VIA}}"] = _pick_locatore_field(
        imm, adapter, "locatore_via", "locatore_via"
    )
    params["{{LOCATORE_CIVICO}}"] = _pick_locatore_field(
        imm, adapter, "locatore_civico", "locatore_civico"
    )

    # -----------------------------
    # IMMOBILE (реальна адреса, ТІЛЬКИ override з БД)
    # -----------------------------
    params["{{IMMOBILE_COMUNE}}"] = _pick_override(imm, "immobile_comune_override")
    params["{{IMMOBILE_VIA}}"] = _pick_override(imm, "immobile_via_override")
    params["{{IMMOBILE_CIVICO}}"] = _pick_override(imm, "immobile_civico_override")
    params["{{IMMOBILE_PIANO}}"] = _pick_override(imm, "immobile_piano_override")
    params["{{IMMOBILE_INTERNO}}"] = _pick_override(imm, "immobile_interno_override")

    # -----------------------------
    # Дані катасто (з візури, збережені в БД)
    # -----------------------------
    params["{{FOGLIO}}"] = _to_str(imm.foglio)
    params["{{NUMERO}}"] = _to_str(imm.numero)
    params["{{SUB}}"] = _to_str(imm.sub)
    params["{{RENDITA}}"] = _to_str(imm.rendita)
    params["{{SUPERFICIE_TOTALE}}"] = _to_str(imm.superficie_totale)
    params["{{CATEGORIA}}"] = _to_str(imm.categoria)

    # -----------------------------
    # DATI CATASTALI – рядок APPARTAMENTO
    # -----------------------------
    params["{{APP_FOGL}}"] = _to_str(imm.foglio)
    params["{{APP_PART}}"] = _to_str(imm.numero)
    params["{{APP_SUB}}"] = _to_str(imm.sub)
    params["{{APP_REND}}"] = _to_str(imm.rendita)
    params["{{APP_SCAT}}"] = _to_str(imm.superficie_totale)
    # Поки немає формули riparametrata — ставимо таку саму площу
    params["{{APP_SRIP}}"] = _to_str(imm.superficie_totale)
    params["{{APP_CAT}}"] = _to_str(imm.categoria)

    # -----------------------------
    # DATI CATASTALI – рядок TOTALE SUPERFICIE
    # -----------------------------
    params["{{TOT_SCAT}}"] = _to_str(imm.superficie_totale)
    params["{{TOT_SRIP}}"] = _to_str(imm.superficie_totale)
    params["{{TOT_CAT}}"] = _to_str(imm.categoria)

    # -----------------------------
    # DATI CATASTALI – GARAGE/BOX/CANTINA та POSTO MACCHINA
    #
    # Поки що окремі об'єкти типу box/posto не парсимо й не зберігаємо —
    # заповнюємо ці плейсхолдери пустими рядками, щоб у документі
    # не залишались сирі {{GAR_*}} / {{PST_*}}.
    # -----------------------------
    for prefix in ("GAR", "PST"):
        for suffix in ("FOGL", "PART", "SUB", "REND", "SCAT", "SRIP", "CAT"):
            params[f"{{{{{prefix}_{suffix}}}}}"] = ""

    # -----------------------------
    # Дані договору
    # -----------------------------
    params["{{CONTRATTO_DATA}}"] = _to_str(adapter.get("contratto_data"))

    # -----------------------------
    # CONDUTTORE (орендар)
    # -----------------------------
    params["{{CONDUTTORE_NOME}}"] = _to_str(adapter.get("conduttore_nome"))
    params["{{CONDUTTORE_CF}}"] = _to_str(adapter.get("conduttore_cf"))
    params["{{CONDUTTORE_COMUNE}}"] = _to_str(adapter.get("conduttore_comune"))
    params["{{CONDUTTORE_VIA}}"] = _to_str(adapter.get("conduttore_via"))

    # -----------------------------
    # Дані реєстрації
    # -----------------------------
    params["{{DECORRENZA_DATA}}"] = _to_str(adapter.get("decorrenza_data"))
    params["{{REGISTRAZIONE_DATA}}"] = _to_str(adapter.get("registrazione_data"))
    params["{{REGISTRAZIONE_NUM}}"] = _to_str(adapter.get("registrazione_num"))
    params["{{AGENZIA_ENTRATE_SEDE}}"] = _to_str(adapter.get("agenzia_entrate_sede"))

    # -----------------------------
    # A/B/C/D елементи — ТІЛЬКИ з БД (Immobile)
    # -----------------------------
    element_keys = [
        "a1", "a2",
        "b1", "b2", "b3", "b4", "b5",
        "c1", "c2", "c3", "c4", "c5", "c6", "c7",
        "d1", "d2", "d3", "d4", "d5", "d6",
        "d7", "d8", "d9", "d10", "d11", "d12", "d13",
    ]

    for key in element_keys:
        val = getattr(imm, key, None)
        val_str = _to_str(val)
        # Плейсхолдери в шаблоні можуть бути як {{a1}}, так і {{A1}}
        params[f"{{{{{key}}}}}"] = val_str
        params[f"{{{{{key.upper()}}}}}"] = val_str

    # -----------------------------
    # CALCOLO NUMERO ELEMENTI — {{A_CNT}}..{{D_CNT}}
    # -----------------------------
    def _count_present(values) -> int:
        return sum(1 for v in values if v not in (None, ""))

    a_cnt = _count_present([getattr(imm, "a1", None), getattr(imm, "a2", None)])
    b_cnt = _count_present([getattr(imm, "b1", None), getattr(imm, "b2", None),
                            getattr(imm, "b3", None), getattr(imm, "b4", None), getattr(imm, "b5", None)])
    c_cnt = _count_present([
        getattr(imm, "c1", None), getattr(imm, "c2", None), getattr(imm, "c3", None),
        getattr(imm, "c4", None), getattr(imm, "c5", None), getattr(imm, "c6", None),
        getattr(imm, "c7", None),
    ])
    d_cnt = _count_present([
        getattr(imm, "d1", None), getattr(imm, "d2", None), getattr(imm, "d3", None),
        getattr(imm, "d4", None), getattr(imm, "d5", None), getattr(imm, "d6", None),
        getattr(imm, "d7", None), getattr(imm, "d8", None), getattr(imm, "d9", None),
        getattr(imm, "d10", None), getattr(imm, "d11", None), getattr(imm, "d12", None),
        getattr(imm, "d13", None),
    ])

    # Можемо паралельно оновити в об'єкті (на випадок, якщо далі це ще десь знадобиться)
    imm.a_cnt = a_cnt
    imm.b_cnt = b_cnt
    imm.c_cnt = c_cnt
    imm.d_cnt = d_cnt

    # Плейсхолдери в шаблоні CALCOLO NUMERO ELEMENTI
    params["{{A_CNT}}"] = _to_str(a_cnt)
    params["{{B_CNT}}"] = _to_str(b_cnt)
    params["{{C_CNT}}"] = _to_str(c_cnt)
    params["{{D_CNT}}"] = _to_str(d_cnt)

    # -----------------------------
    # CALCOLO DEL CANONE – CAN_*
    # -----------------------------
    # Фактичний canone за договором: YAML має пріоритет, але якщо там немає —
    # беремо з БД (imm.canone_contrattuale_mensile).
    canone_contr_mens = adapter.get("canone_contrattuale_mensile")
    if canone_contr_mens in (None, ""):
        canone_contr_mens = getattr(imm, "canone_contrattuale_mensile", None)

    # Якщо canone_result відсутній (розрахунок не вдався) – секцію CAN_* заповнюємо порожнім,
    # але TOTALE MENSILE CONCORDATO показуємо, якщо він є.
    if canone is None:
        placeholders = [
            "CAN_ZONA", "CAN_SUBFASCIA",
            "CAN_MQ", "CAN_MQ_ANNUO", "CAN_TOTALE_ANNUO",
            "CAN_ARREDATO", "CAN_CLASSE_A", "CAN_CLASSE_B",
            "CAN_ENERGY", "CAN_DURATA", "CAN_TRANSITORIO", "CAN_STUDENTI",
            "CAN_ANNUO_VAR_MIN", "CAN_ANNUO_VAR_MAX",
            "CAN_MENSILE_VAR_MIN", "CAN_MENSILE_VAR_MAX",
            "CAN_MENSILE",
        ]
        for ph in placeholders:
            params[f"{{{{{ph}}}}}"] = ""

        if canone_contr_mens is not None:
            params["{{CAN_MENSILE}}"] = _to_str(canone_contr_mens)

        return params

    # Є валідний CanoneResult → заповнюємо
    params["{{CAN_ZONA}}"] = _to_str(canone.zona)
    params["{{CAN_SUBFASCIA}}"] = _to_str(canone.subfascia)

    params["{{CAN_MQ}}"] = _to_str(imm.superficie_totale)
    params["{{CAN_MQ_ANNUO}}"] = f"{canone.base_euro_mq:.2f}"
    params["{{CAN_TOTALE_ANNUO}}"] = f"{canone.canone_base_annuo:.2f}"

    # Надбавки/знижки у % — поки що в тебе в CanoneResult тільки поля
    # arredamento_delta_pct, energy_delta_pct, studenti_delta_pct.
    params["{{CAN_ARREDATO}}"] = (
        f"{canone.arredamento_delta_pct:+.0f}%"
        if canone.arredamento_delta_pct
        else ""
    )
    # Для CLASSE_A / CLASSE_B поки немає окремих формул → лишаємо порожні.
    params["{{CAN_CLASSE_A}}"] = ""
    params["{{CAN_CLASSE_B}}"] = ""
    params["{{CAN_ENERGY}}"] = (
        f"{canone.energy_delta_pct:+.0f}%" if canone.energy_delta_pct else ""
    )

    # Надбавки за тривалість / transitorio / studenti поки не реалізовані
    params["{{CAN_DURATA}}"] = ""
    params["{{CAN_TRANSITORIO}}"] = ""
    params["{{CAN_STUDENTI}}"] = ""

    # Поки немає min/max діапазону по варіаціям — ставимо однакові значення
    params["{{CAN_ANNUO_VAR_MIN}}"] = f"{canone.canone_finale_annuo:.2f}"
    params["{{CAN_ANNUO_VAR_MAX}}"] = f"{canone.canone_finale_annuo:.2f}"
    params["{{CAN_MENSILE_VAR_MIN}}"] = f"{canone.canone_finale_mensile:.2f}"
    params["{{CAN_MENSILE_VAR_MAX}}"] = f"{canone.canone_finale_mensile:.2f}"

    # TOTALE MENSILE CONCORDATO TRA LE PARTI
    params["{{CAN_MENSILE}}"] = _to_str(canone_contr_mens)

    return params


# ---------------------------------------------------------------------------
# Основний Scrapy pipeline
# ---------------------------------------------------------------------------

class UppiPipeline:
    """
    Pipeline, який:
    - для item з visura_source='sister' парсить PDF-візуру, зберігає в БД+MinIO;
    - для item з visura_source='db_cache' тягне immobili з БД;
    - генерує DOCX-атестації для вибраних об'єктів;
    - рахує базовий canone (compute_base_canone) для CALCOLO DEL CANONE.
    """

    template_path: Path

    def __init__(self):
        # Шлях до шаблону DOCX
        self.template_path = (
            Path(__file__).resolve().parents[1]
            / "attestazione_template"
            / "template_attestazione_pescara.docx"
        )

    # ------------------------------------------------------------------
    # Внутрішній helper: збір CanoneInput з Immobile + YAML
    # ------------------------------------------------------------------
    def _build_canone_input(self, adapter: ItemAdapter, imm: Immobile) -> CanoneInput:
        """
        Формує CanoneInput з Immobile (БД) + YAML.

        Пріоритет:
        - якщо значення є в Immobile (тобто вже в БД) → беремо його;
        - якщо в Immobile немає, але є в YAML → беремо з YAML;
        - інакше дефолт.

        Це відповідає флоу:
        1) YAML → upsert_* → БД;
        2) з БД беремо дані для розрахунку;
        3) YAML може "підмінити" БД тільки якщо ти явно його змінив у поточному запуску.
        """
        # 1) Superficie
        superficie = imm.superficie_totale
        if superficie is None:
            yaml_superficie = adapter.get("superficie_totale")
            try:
                superficie = float(yaml_superficie) if yaml_superficie is not None else 0.0
            except (TypeError, ValueError):
                superficie = 0.0

        # 2) micro_zona та foglio
        micro_zona = getattr(imm, "micro_zона", None)
        if not micro_zona:
            micro_zona = _clean_str(adapter.get("micro_zona"))

        foglio = _clean_str(imm.foglio) or _clean_str(adapter.get("foglio"))

        # 3) категорія / клас
        categoria = _clean_str(imm.categoria) or _clean_str(adapter.get("categoria"))
        classe = _clean_str(imm.classe) or _clean_str(adapter.get("classe"))

        # 4) Лічильники A/B/C/D — рахуємо на основі полів imm, щоб бути незалежними від БД
        def _cnt(values) -> int:
            return sum(1 for v in values if v not in (None, ""))

        count_a = _cnt([getattr(imm, "a1", None), getattr(imm, "a2", None)])
        count_b = _cnt([
            getattr(imm, "b1", None), getattr(imm, "b2", None),
            getattr(imm, "b3", None), getattr(imm, "b4", None), getattr(imm, "b5", None),
        ])
        count_c = _cnt([
            getattr(imm, "c1", None), getattr(imm, "c2", None), getattr(imm, "c3", None),
            getattr(imm, "c4", None), getattr(imm, "c5", None),
            getattr(imm, "c6", None), getattr(imm, "c7", None),
        ])
        count_d = _cnt([
            getattr(imm, "d1", None), getattr(imm, "d2", None), getattr(imm, "d3", None),
            getattr(imm, "d4", None), getattr(imm, "d5", None), getattr(imm, "d6", None),
            getattr(imm, "d7", None), getattr(imm, "d8", None), getattr(imm, "d9", None),
            getattr(imm, "d10", None), getattr(imm, "d11", None),
            getattr(imm, "d12", None), getattr(imm, "d13", None),
        ])

        # 5) contract_kind
        raw_kind = getattr(imm, "contract_kind", None)
        if not raw_kind:
            raw_kind = _clean_str(adapter.get("contract_kind")) or "CONCORDATO"
        try:
            contract_kind = ContractKind[raw_kind.strip().upper()]
        except Exception:
            logger.warning(
                "[CANONE] Невідомий CONTRACT_KIND=%r, використовую CONCORDATO", raw_kind
            )
            contract_kind = ContractKind.CONCORDATO

        # 6) arredato
        imm_arredato = getattr(imm, "arredato", None)
        yaml_arredato = adapter.get("arredato")
        arredato = (
            _to_bool_or_none(yaml_arredato)
            if yaml_arredato not in (None, "")
            else _to_bool_or_none(imm_arredato)
        ) or False

        # 7) energy_class
        energy_db = _clean_str(getattr(imm, "energy_class", None))
        energy_yaml = _clean_str(adapter.get("energy_class"))
        energy_class = (energy_yaml or energy_db or None)
        if energy_class:
            energy_class = energy_class.upper()

        # 8) durata_anni
        durata = getattr(imm, "durata_anni", None)
        if durata is None:
            raw_durata = adapter.get("durata_anni")
            try:
                durata = int(raw_durata) if raw_durata is not None else 3
            except (TypeError, ValueError):
                logger.warning(
                    "[CANONE] Невірне DURATA_ANNI=%r, використовую 3", raw_durata
                )
                durata = 3

        return CanoneInput(
            superficie_catastale=float(superficie or 0.0),
            micro_zona=micro_zona,
            foglio=foglio,
            categoria_catasto=categoria,
            classe_catasto=classe,
            count_a=count_a,
            count_b=count_b,
            count_c=count_c,
            count_d=count_d,
            arredato=arredato,
            energy_class=energy_class,
            contract_kind=contract_kind,
            durata_anni=durata,
        )

    # ------------------------------------------------------------------
    # Основний вхід Scrapy pipeline
    # ------------------------------------------------------------------
    def process_item(self, item, spider):
        adapter = ItemAdapter(item)
        cf = adapter.get("locatore_cf") or adapter.get("codice_fiscale")

        if not cf:
            spider.logger.error("[PIPELINE] Item без locatore_cf/codice_fiscale: %r", item)
            return item

        source = adapter.get("visura_source")
        visura_downloaded = bool(adapter.get("visura_downloaded"))
        visura_download_path = adapter.get("visura_download_path")
        force_update = (
            bool(adapter.get("force_update_visура"))
            or bool(adapter.get("FORCE_UPDATE_VISURA"))
            or bool(getattr(spider, "force_update_visura", False))
        )

        spider.logger.info(
            "[PIPELINE] CF=%s, visura_source=%r, downloaded=%s, force_update=%s",
            cf,
            source,
            visura_downloaded,
            force_update,
        )

        immobiles: List[Immobile] = []

        # ------------------------------------------------------------------
        # 1) Item з SISTER: очікуємо, що павук вже скачав PDF
        # ------------------------------------------------------------------
        if source == "sister":
            if not visura_downloaded or not visura_download_path:
                spider.logger.error(
                    "[PIPELINE] CF=%s, visura_source='sister', але немає PDF "
                    "(downloaded=%s, path=%r)",
                    cf,
                    visura_downloaded,
                    visura_download_path,
                )

                # fallback: якщо в БД вже щось є і force_update=False — спробуємо використати БД
                if not force_update and db_has_visura(cf):
                    spider.logger.warning(
                        "[PIPELINE] CF=%s, fallback на БД для immobili через відсутній PDF",
                        cf,
                    )
                    # YAML → БД: overrides, A/B/C/D, контрактні поля
                    upsert_overrides_from_yaml(cf, adapter)
                    upsert_elements_from_yaml(cf, adapter)
                    upsert_contract_from_yaml(cf, adapter)
                    immobiles = load_immobiles_from_db(cf)
                else:
                    return item
            else:
                pdf_path = Path(visura_download_path)
                if not pdf_path.exists():
                    spider.logger.error(
                        "[PIPELINE] CF=%s, очікуваний PDF не знайдено: %s",
                        cf,
                        pdf_path,
                    )
                    # fallback: раптом PDF лежить там, де його кладе get_visura_path
                    candidate = get_visura_path(cf)
                    if candidate.exists():
                        spider.logger.warning(
                            "[PIPELINE] CF=%s, використовую fallback шлях PDF: %s",
                            cf,
                            candidate,
                        )
                        pdf_path = candidate
                    else:
                        return item

                parser = VisuraParser()
                try:
                    imm_dicts = parser.parse(str(pdf_path))
                except Exception as e:
                    spider.logger.exception(
                        "[PIPELINE] Помилка парсингу PDF для %s (%s): %s",
                        cf,
                        pdf_path,
                        e,
                    )
                    return item

                if not imm_dicts:
                    spider.logger.warning(
                        "[PIPELINE] Після парсингу PDF для %s не знайдено жодного immobile",
                        cf,
                    )
                    return item

                # Спочатку — сирі immobiles з парсера
                raw_immobiles = [Immobile(**d) for d in imm_dicts]
                spider.logger.info(
                    "[PIPELINE] Розпарсено %d immobili з PDF для %s",
                    len(raw_immobiles),
                    cf,
                )

                # Зберігаємо в БД + MinIO
                try:
                    save_visura(cf, raw_immobiles, pdf_path)
                except Exception:
                    # помилка вже залогована всередині save_visura
                    return item

                # Після збереження — YAML → БД: overrides, A/B/C/D, контрактні поля
                upsert_overrides_from_yaml(cf, adapter)
                upsert_elements_from_yaml(cf, adapter)
                upsert_contract_from_yaml(cf, adapter)

                # І ТІЛЬКИ ТЕПЕР беремо canonical immobiles з БД
                immobiles = load_immobiles_from_db(cf)
                if not immobiles:
                    spider.logger.warning(
                        "[PIPELINE] Після save_visura/overrides в БД немає immobilі для %s",
                        cf,
                    )
                    return item

        # ------------------------------------------------------------------
        # 2) Item з DB cache: visura_source='db_cache'
        # ------------------------------------------------------------------
        elif source == "db_cache":
            spider.logger.info(
                "[PIPELINE] CF=%s, visura_source='db_cache' → тягнемо immobili з БД",
                cf,
            )

            # YAML → БД: overrides, A/B/C/D, контрактні поля
            upsert_overrides_from_yaml(cf, adapter)
            upsert_elements_from_yaml(cf, adapter)
            upsert_contract_from_yaml(cf, adapter)

            immobiles = load_immobiles_from_db(cf)
            if not immobiles:
                spider.logger.warning(
                    "[PIPELINE] В БД немає immobilі для існуючої візури %s",
                    cf,
                )
                return item

        # ------------------------------------------------------------------
        # 3) Backward-compat: item без visura_source
        # ------------------------------------------------------------------
        else:
            has_in_db = db_has_visura(cf)
            spider.logger.info(
                "[PIPELINE] CF=%s, visura_source is None, has_in_db=%s, force_update=%s",
                cf,
                has_in_db,
                force_update,
            )

            if force_update or not has_in_db:
                # Маємо перерахувати візуру з локального PDF
                pdf_path = get_visura_path(cf)
                if not pdf_path.exists():
                    spider.logger.error(
                        "[PIPELINE] CF=%s, очікуваний PDF (backward) не знайдено: %s",
                        cf,
                        pdf_path,
                    )
                    return item

                parser = VisuraParser()
                try:
                    imm_dicts = parser.parse(str(pdf_path))
                except Exception as e:
                    spider.logger.exception(
                        "[PIPELINE] Помилка парсингу PDF (backward) для %s (%s): %s",
                        cf,
                        pdf_path,
                        e,
                    )
                    return item

                if not imm_dicts:
                    spider.logger.warning(
                        "[PIPELINE] Після парсингу PDF (backward) для %s не знайдено жодного immobile",
                        cf,
                    )
                    return item

                raw_immobiles = [Immobile(**d) for d in imm_dicts]
                spider.logger.info(
                    "[PIPELINE] (backward) Розпарсено %d immobili з PDF для %s",
                    len(raw_immobiles),
                    cf,
                )

                try:
                    save_visura(cf, raw_immobiles, pdf_path)
                except Exception:
                    return item

                # Після збереження — YAML → БД
                upsert_overrides_from_yaml(cf, adapter)
                upsert_elements_from_yaml(cf, adapter)
                upsert_contract_from_yaml(cf, adapter)

                # І далі працюємо вже з canonical даними з БД
                immobiles = load_immobiles_from_db(cf)
                if not immobiles:
                    spider.logger.warning(
                        "[PIPELINE] (backward) Після save_visura/overrides в БД немає immobilі для %s",
                        cf,
                    )
                    return item
            else:
                # Візура вже є в БД, PDF не чіпаємо
                # Але YAML може містити нові overrides/A/B/C/D/контракт → оновлюємо й читаємо
                upsert_overrides_from_yaml(cf, adapter)
                upsert_elements_from_yaml(cf, adapter)
                upsert_contract_from_yaml(cf, adapter)

                immobiles = load_immobiles_from_db(cf)
                if not immobiles:
                    spider.logger.warning(
                        "[PIPELINE] (backward) В БД немає immobilі для існуючої візури %s",
                        cf,
                    )
                    return item

        # ------------------------------------------------------------------
        # 4) Фільтрація immobili згідно критеріїв і генерація DOCX
        # ------------------------------------------------------------------
        selected_immobiles = filter_immobiles(immobiles, adapter)
        if not selected_immobiles:
            spider.logger.warning(
                "[PIPELINE] Для %s жоден immobile не пройшов фільтр. Аттестації не створені.",
                cf,
            )
            return item

        for imm in selected_immobiles:
            # 1) Розрахунок canone для цього immobile
            canone_result: CanoneResult | None = None
            try:
                canone_input = self._build_canone_input(adapter, imm)
                if canone_input.superficie_catastale <= 0:
                    raise CanoneCalculationError(
                        f"superficie_catastale <= 0: {canone_input.superficie_catastale}"
                    )
                canone_result = compute_base_canone(canone_input)
            except CanoneCalculationError as e:
                spider.logger.warning(
                    "[CANONE] Логічна помилка при розрахунку canone для CF=%s imm=%s: %s",
                    cf,
                    getattr(imm, "table_num_immobile", None),
                    e,
                )
            except Exception as e:
                spider.logger.exception(
                    "[CANONE] Неочікувана помилка при розрахунку canone для CF=%s imm=%s: %s",
                    cf,
                    getattr(imm, "table_num_immobile", None),
                    e,
                )

            # 2) Формуємо params для шаблону, передаємо canone_result
            params = build_params(adapter, imm, canone_result)

            # 3) Генеруємо attestazione
            output_path = get_attestazione_path(cf, imm)
            output_folder = output_path.parent
            output_folder.mkdir(parents=True, exist_ok=True)

            spider.logger.info(
                "[PIPELINE] Генеруємо attestazione для %s → %s", cf, output_path
            )
            try:
                fill_attestazione_template(
                    template_path=str(self.template_path),
                    output_folder=str(output_folder),
                    filename=output_path.name,
                    params=params,
                    underscored=underscored,
                )
            except Exception as e:
                spider.logger.exception(
                    "[PIPELINE] Помилка при генерації attestazione для %s (%s): %s",
                    cf,
                    output_path,
                    e,
                )

        return item
