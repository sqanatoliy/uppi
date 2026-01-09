# uppi/services/visura_processor.py
from __future__ import annotations

import logging
from pathlib import Path
from typing import List, Optional, Tuple

from itemadapter import ItemAdapter
from decouple import config

from uppi.domain.db import get_pg_connection
from uppi.domain.immobile import Immobile
from uppi.domain.object_storage import ObjectStorage
from uppi.domain.storage import get_attestazione_path, get_client_dir, get_visura_path
from uppi.parsers.visura_pdf_parser import VisuraParser
from uppi.services.attestazione_generator import build_template_params
from uppi.services.db_repo import (
    db_upsert_address,
    db_upsert_immobile_elements,
    db_upsert_person,
    db_upsert_visura,
    db_upsert_immobile,
    db_update_immobile_real_address,
    db_upsert_contract,
    db_load_immobili,
    db_load_contract_context,
    db_insert_canone_calc,
    db_insert_attestazione_log,
    db_prune_old_immobili_without_contracts,
    immobile_from_parsed_dict,
    immobile_db_row,
)
from uppi.services.storage_minio import StorageService
from uppi.utils.audit import mask_username, safe_unlink, sha256_file, sha256_text
from uppi.utils.parse_utils import clean_str, prepare_for_json, safe_float, split_full_name

from uppi.docs.attestazione_template_filler import fill_attestazione_template, underscored
from uppi.domain.pescara2018_calc import compute_base_canone
from uppi.domain.canone_models import CanoneInput, ContractKind

logger = logging.getLogger(__name__)

# Налаштування з середовища
AE_USERNAME = config("AE_USERNAME", default="").strip()
TEMPLATE_VERSION = config("TEMPLATE_VERSION", default="pescara2018_v2").strip()
PRUNE_OLD_IMMOBILI_WITHOUT_CONTRACTS = config("PRUNE_OLD_IMMOBILI_WITHOUT_CONTRACTS",
                                              default="True").strip().lower() == "true"
DELETE_LOCAL_VISURA_AFTER_UPLOAD = config("DELETE_LOCAL_VISURA_AFTER_UPLOAD", default="False").strip().lower() == "true"


def find_local_visura_pdf(cf: str, adapter: ItemAdapter) -> Optional[Path]:
    """Пошук файлу візури в локальній файловій системі."""
    p = clean_str(adapter.get("visura_download_path"))
    if p:
        path = Path(p)
        if path.exists():
            return path

    fallback = get_visura_path(cf)
    if fallback.exists():
        return fallback

    client_dir = get_client_dir(cf)
    # Шукаємо за префіксом DOC_ або просто найновіший PDF
    candidates = sorted(client_dir.glob("DOC_*.pdf"), key=lambda x: x.stat().st_mtime, reverse=True)
    if candidates:
        return candidates[0]

    any_pdf = sorted(client_dir.glob("*.pdf"), key=lambda x: x.stat().st_mtime, reverse=True)
    if any_pdf:
        return any_pdf[0]

    return None


def filter_immobiles_by_yaml(immobiles: List[Tuple[int, Immobile]], adapter: ItemAdapter) -> List[Tuple[int, Immobile]]:
    """Фільтрація списку нерухомості з БД за параметрами, вказаними в YAML (item)."""
    foglio_f = clean_str(adapter.get("foglio"))
    numero_f = clean_str(adapter.get("numero"))
    sub_f = clean_str(adapter.get("sub"))

    out: List[Tuple[int, Immobile]] = []
    for imm_id, imm in immobiles:
        # Порівнюємо кадастрові ідентифікатори (основний спосіб матчингу)
        if foglio_f and str(getattr(imm, "foglio", "") or "") != foglio_f:
            continue
        if numero_f and str(getattr(imm, "numero", "") or "") != numero_f:
            continue
        if sub_f and str(getattr(imm, "sub", "") or "") != sub_f:
            continue

        out.append((imm_id, imm))
        logger.debug("[PIPELINE] Immobile ID=%s matched YAML criteria", imm_id)
    return out


class VisuraProcessor:
    def __init__(self, storage: Optional[ObjectStorage] = None, template_path: Optional[Path] = None):
        self.storage_service = StorageService(storage)
        self.storage = storage or ObjectStorage()
        self.template_path = template_path or (
                Path(__file__).resolve().parents[2] / "attestazione_template" / "template_attestazione_pescara.docx"
        )

    def process_item(self, item, spider):
        adapter = ItemAdapter(item)
        locatore_cf = clean_str(adapter.get("locatore_cf") or adapter.get("codice_fiscale"))

        if not locatore_cf:
            spider.logger.error("[PIPELINE] Missing locatore_cf for item: %r", item)
            return item

        cond_cf = clean_str(adapter.get("conduttore_cf"))
        conn = get_pg_connection()

        try:
            # --- ЕТАП 1: АДРЕСИ ТА ПЕРСОНИ (LOCATORE / CONDUTTORE) ---

            # 1.1. Адреса Locatore
            loc_addr_id = None
            if adapter.get("locatore_comune_res") and adapter.get("locatore_via"):
                loc_addr_id = db_upsert_address(conn, {
                    "comune": adapter.get("locatore_comune_res"),
                    "via_full": adapter.get("locatore_via"),
                    "civico": adapter.get("locatore_civico")
                })

            db_upsert_person(
                conn, locatore_cf,
                surname=clean_str(adapter.get("locatore_surname")),
                name=clean_str(adapter.get("locatore_name")),
                address_id=loc_addr_id
            )

            # 1.2. Адреса Conduttore
            if cond_cf:
                cond_addr_id = None
                if adapter.get("conduttore_comune"):
                    cond_addr_id = db_upsert_address(conn, {
                        "comune": adapter.get("conduttore_comune"),
                        "via_full": adapter.get("conduttore_via") or ""
                    })

                # Розділення повного імені Conduttore (якщо потрібно)
                cond_full_name = clean_str(adapter.get("conduttore_nome"))
                c_surname, c_name = split_full_name(cond_full_name)

                db_upsert_person(
                    conn, cond_cf,
                    surname=c_surname,
                    name=c_name,
                    address_id=cond_addr_id
                )

            # --- ЕТАП 2: ЗАВАНТАЖЕННЯ ТА ПАРСИНГ ВІЗУРИ ---

            visura_source = clean_str(adapter.get("visura_source"))
            visura_downloaded = bool(adapter.get("visura_downloaded"))
            pdf_path = None
            fetched_now = False
            visura_db_id = None
            pdf_to_delete: Path | None = None

            if visura_source == "sister" and visura_downloaded:
                pdf_path = find_local_visura_pdf(locatore_cf, adapter)
                if pdf_path:
                    checksum = sha256_file(pdf_path)
                    bucket = self.storage.cfg.visure_bucket
                    obj_name = self.storage.visura_object_name(locatore_cf)

                    self.storage_service.upload_file(bucket, obj_name, pdf_path, content_type="application/pdf")
                    fetched_now = True
                    visura_db_id = db_upsert_visura(conn, locatore_cf, bucket, obj_name, checksum, fetched_now=True)
                    pdf_to_delete = pdf_path
            else:
                # Навіть якщо не качали зараз, реєструємо запис або отримуємо існуючий ID
                visura_db_id = db_upsert_visura(
                    conn, locatore_cf, self.storage.cfg.visure_bucket,
                    self.storage.visura_object_name(locatore_cf), None, fetched_now=False
                )

            # --- ЕТАП 3: ОБРОБКА IMMOBILI (З ПАРСЕРА) ---

            keep_ids: List[int] = []
            if fetched_now and pdf_path:
                parser = VisuraParser()
                parsed_dicts = parser.parse(pdf_path)

                # Оновлюємо інформацію про Locatore з візури
                if parsed_dicts:
                    # Prendiamo i dati del locatore dal primo immobile trovato (sono uguali per tutti)
                    first_item = parsed_dicts[0]
                    v_name = first_item.get("locatore_name")
                    v_surname = first_item.get("locatore_surname")

                    # Aggiorniamo il database con i nomi VERI estratti dalla visura
                    # solo se non sono già stati forniti manualmente via YAML
                    db_upsert_person(
                        conn, locatore_cf,
                        surname=clean_str(adapter.get("locatore_surname")) or v_surname,
                        name=clean_str(adapter.get("locatore_name")) or v_name,
                        address_id=loc_addr_id
                    )

                for d in parsed_dicts:
                    # А. Зберігаємо адресу з візури
                    v_addr_id = db_upsert_address(conn, {
                        "comune": d.get("immobile_comune"),
                        "via_full": d.get("via_name") or d.get("indirizzo_raw"),
                        "civico": d.get("via_num"),
                        "piano": d.get("piano"),
                        "interno": d.get("interno"),
                        "scala": d.get("scala")
                    })

                    # Б. Створюємо/оновлюємо Immobile Master Data
                    imm_obj = immobile_from_parsed_dict(d)
                    imm_id = db_upsert_immobile(
                        conn, locatore_cf, imm_obj,
                        visura_addr_id=v_addr_id,
                        source_visura_id=visura_db_id
                    )
                    keep_ids.append(imm_id)

                # Очистка старих записів без контрактів
                if keep_ids:
                    db_prune_old_immobili_without_contracts(conn, locatore_cf, keep_ids,
                                                            PRUNE_OLD_IMMOBILI_WITHOUT_CONTRACTS)

            # --- ЕТАП 4: ОПЕРАЦІЙНИЙ ЦИКЛ (КОНТРАКТИ ТА ГЕНЕРАЦІЯ) ---

            immobili_db = db_load_immobili(conn, locatore_cf)
            selected = filter_immobiles_by_yaml(immobili_db, adapter)

            for immobile_id, imm in selected:
                # 4.1.1 Обробка "реальної" адреси об'єкта (якщо вказана в YAML як override)
                real_addr_id = None
                if adapter.get("immobile_comune") or adapter.get("immobile_via"):
                    real_addr_id = db_upsert_address(conn, {
                        "comune": adapter.get("immobile_comune"),
                        "via_full": adapter.get("immobile_via"),
                        "civico": adapter.get("immobile_civico"),
                        "piano": adapter.get("immobile_piano"),
                        "interno": adapter.get("immobile_interno")
                    })

                # Оновлюємо Master Data нерухомості даними з YAML
                db_update_immobile_real_address(
                    conn, immobile_id,
                    real_address_id=real_addr_id,
                    energy_class=adapter.get("energy_class")
                )

                # 4.1.2 Записуємо елементи A-D в базу перед завантаженням контексту
                db_upsert_immobile_elements(conn, immobile_id, adapter)

                # 4.2. Контракт
                # Створюємо або оновлюємо останній активний контракт
                contract_id = db_upsert_contract(conn, immobile_id, adapter)

                # Отримуємо повний контекст (включаючи джойни адрес та персон)
                contract_ctx = db_load_contract_context(conn, contract_id)
                # imm = contract_ctx['immobili']  # Оновлений Immobile з контексту

                # --- ЕТАП 5: РОЗРАХУНОК КАНОНУ ---

                canone_snapshot = {}
                canone_result_snapshot = None

                try:
                    elements = contract_ctx.get("elements") or {}

                    # Допоміжна функція для підрахунку заповнених елементів A-D
                    def cnt(keys: List[str]) -> int:
                        return sum(1 for k in keys if str(elements.get(k, "") or "").strip() != "")

                    # -------------------------------------------------------------------------
                    # 1. CONTRACT_KIND
                    # Логіка: Завжди беремо з YAML або "CONCORDATO". Ніколи з БД.
                    # -------------------------------------------------------------------------
                    kind_raw = clean_str(adapter.get("contract_kind"))
                    kind_str = (kind_raw or "CONCORDATO").upper()

                    kind_enum = ContractKind.CONCORDATO
                    try:
                        kind_enum = ContractKind[kind_str]
                    except KeyError:
                        logger.warning(f"[CANONE] Unknown contract kind '{kind_str}', defaulting to CONCORDATO")
                        kind_enum = ContractKind.CONCORDATO

                    # -------------------------------------------------------------------------
                    # 2. ENERGY_CLASS (Smart Patch)
                    # Логіка: 
                    #   - YAML == "-" -> None (видалено)
                    #   - YAML == "A" -> "A" (оновлено)
                    #   - YAML == None -> беремо з БД (contract_ctx['immobile']['energy_class'])
                    # -------------------------------------------------------------------------
                    yaml_energy = clean_str(adapter.get("energy_class"))
                    db_energy = contract_ctx.get("immobile", {}).get(
                        "energy_class")  # Тепер це поле доступне завдяки фіксу в db_repo

                    if yaml_energy == "-":
                        final_energy = None
                    elif yaml_energy:
                        final_energy = yaml_energy.upper()
                    else:
                        final_energy = db_energy

                        # -------------------------------------------------------------------------
                    # 3. ISTAT (Smart Patch)
                    # -------------------------------------------------------------------------
                    yaml_istat = adapter.get("istat")
                    db_istat = contract_ctx.get("contract", {}).get("istat_rate")

                    final_istat = None
                    if str(yaml_istat).strip() == "-":
                        final_istat = None
                    elif yaml_istat is not None and str(yaml_istat).strip() != "":
                        final_istat = safe_float(yaml_istat)
                    elif db_istat is not None:
                        final_istat = float(db_istat)

                    # -------------------------------------------------------------------------
                    # 4. DURATA (Smart Patch)
                    # -------------------------------------------------------------------------
                    yaml_durata = adapter.get("durata_anni")
                    db_durata = contract_ctx.get("contract", {}).get("durata_anni")

                    final_durata = 3  # Default 3
                    if str(yaml_durata).strip() == "-":
                        final_durata = 3  # Якщо видалили, повертаємось до дефолту
                    elif yaml_durata is not None and str(yaml_durata).strip() != "":
                        final_durata = int(yaml_durata)
                    elif db_durata is not None:
                        final_durata = int(db_durata)

                    # -------------------------------------------------------------------------
                    # 5. ARREDATO (Smart Patch)
                    # -------------------------------------------------------------------------
                    yaml_arredato = adapter.get("arredato")
                    db_arredato = contract_ctx.get("contract", {}).get("arredato_pct")

                    final_arredato = 0.0
                    if str(yaml_arredato).strip() == "-":
                        final_arredato = 0.0
                    elif yaml_arredato is not None and str(yaml_arredato).strip() != "":
                        final_arredato = safe_float(yaml_arredato) or 0.0
                    elif db_arredato is not None:
                        final_arredato = float(db_arredato)

                    # -------------------------------------------------------------------------
                    # 6. IGNORE_SURCHARGES
                    # -------------------------------------------------------------------------
                    yaml_ignore = adapter.get("ignore_surcharges")
                    db_ignore = contract_ctx.get("contract", {}).get("ignore_surcharges")

                    final_ignore = False
                    if str(yaml_ignore).strip() == "-":
                        final_ignore = False
                    elif yaml_ignore is not None and str(yaml_ignore).strip() != "":
                        final_ignore = str(yaml_ignore).lower() in ("true", "1", "yes", "y")
                    elif db_ignore is not None:
                        final_ignore = bool(db_ignore)

                    # --- Створення об'єкта вхідних даних ---
                    can_in = CanoneInput(
                        superficie_catastale=float(imm.superficie_totale or adapter.get("superficie_totale") or 0),
                        micro_zona=clean_str(imm.micro_zona),
                        foglio=clean_str(imm.foglio),
                        categoria_catasto=clean_str(imm.categoria),
                        classe_catasto=clean_str(imm.classe),
                        count_a=cnt(["a1", "a2"]),
                        count_b=cnt([f"b{i}" for i in range(1, 6)]),
                        count_c=cnt([f"c{i}" for i in range(1, 8)]),
                        count_d=cnt([f"d{i}" for i in range(1, 14)]),

                        arredato=final_arredato,
                        energy_class=final_energy,
                        contract_kind=kind_enum,
                        durata_anni=final_durata,
                        istat=final_istat,
                        ignore_surcharges=final_ignore,
                    )

                    # Логування для відлагодження
                    logger.debug(
                        f"[CALC_INPUT] K={kind_str} En={final_energy} Arr={final_arredato} Dur={final_durata} Ign={final_ignore}")

                    can_res = compute_base_canone(can_in)

                    # Очищення даних для збереження в JSON
                    canone_snapshot = prepare_for_json(can_in.__dict__)
                    canone_result_snapshot = prepare_for_json(can_res.__dict__) if can_res else {}

                    # Збереження результатів розрахунку в БД
                    db_insert_canone_calc(
                        conn, contract_id, "pescara2018_base",
                        inputs={"canone_input": canone_snapshot, "result": canone_result_snapshot},
                        result_mensile=safe_float(getattr(can_res, "canone_finale_mensile", None))
                    )

                    # Перезавантажуємо контекст після розрахунку для генератора
                    contract_ctx = db_load_contract_context(conn, contract_id)

                except Exception as e:
                    spider.logger.warning("[CANONE] Calculation skipped or failed for contract %s: %s", contract_id, e)

                # --- ЕТАП 6: ГЕНЕРАЦІЯ ТА UPLOAD ДОКУМЕНТА ---

                params = build_template_params(adapter, imm, contract_ctx)
                output_path = get_attestazione_path(locatore_cf, contract_id, imm)
                output_path.parent.mkdir(parents=True, exist_ok=True)

                try:
                    logger.debug(f"[DEBUG_ADDR] Contract CTX: {contract_ctx.get('immobile')}")
                    fill_attestazione_template(
                        template_path=str(self.template_path),
                        output_folder=str(output_path.parent),
                        filename=output_path.name,
                        params=params,
                        underscored=underscored,
                    )

                    out_bucket = self.storage.cfg.attestazioni_bucket
                    out_obj = self.storage.attestazione_object_name(locatore_cf, contract_id)

                    self.storage_service.upload_file(
                        out_bucket, out_obj, output_path,
                        content_type="application/vnd.openxmlformats-officedocument.wordprocessingml.document"
                    )

                    # Створюємо детальний знімок даних для аудиту
                    # Тут ми використовуємо immobile_db_row, щоб отримати очищені дані
                    imm_data_snapshot = immobile_db_row(imm)

                    params_snapshot = {
                        "locatore_cf": locatore_cf,
                        "immobile_id": immobile_id,
                        "contract_id": contract_id,
                        "yaml_item": dict(adapter.asdict()),
                        "immobile_master_data": imm_data_snapshot,  # ВИКОРИСТАННЯ ТУТ
                        "contract_ctx": contract_ctx,
                        "template_version": TEMPLATE_VERSION,
                        "canone_result": canone_result_snapshot,
                        "output": {"bucket": out_bucket, "object": out_obj},
                    }

                    clean_params = prepare_for_json(params_snapshot)
                    # Лог успішної генерації
                    db_insert_attestazione_log(
                        conn, contract_id, "generated", out_bucket, out_obj,
                        params_snapshot=clean_params,
                        error=None,
                        author_login_masked=mask_username(AE_USERNAME),
                        author_login_sha256=sha256_text(AE_USERNAME),
                        template_version=TEMPLATE_VERSION
                    )

                except Exception as e:
                    spider.logger.exception("[DOCX] Failed for contract %s", contract_id)
                    db_insert_attestazione_log(
                        conn, contract_id, "failed", "", "",
                        {"error_stage": "generation_or_upload"},
                        error=str(e),
                        author_login_masked=mask_username(AE_USERNAME),
                        author_login_sha256=sha256_text(AE_USERNAME),
                        template_version=TEMPLATE_VERSION
                    )

            conn.commit()

            # Очистка тимчасових файлів
            if DELETE_LOCAL_VISURA_AFTER_UPLOAD and pdf_to_delete:
                safe_unlink(pdf_to_delete)

            return item

        except Exception as e:
            spider.logger.exception("[PIPELINE] Fatal error processing CF %s: %s", locatore_cf, e)
            if conn:
                conn.rollback()
            return item
        finally:
            if conn:
                conn.close()
