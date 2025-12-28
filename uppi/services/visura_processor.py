from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import psycopg2
from itemadapter import ItemAdapter
from decouple import config

from uppi.domain.db import get_pg_connection
from uppi.domain.immobile import Immobile
from uppi.domain.object_storage import ObjectStorage
from uppi.domain.storage import get_attestazione_path, get_client_dir, get_visura_path
from uppi.parsers.visura_pdf_parser import VisuraParser
from uppi.services.attestazione_generator import build_template_params
from uppi.services.db_repo import (
    db_apply_contract_elements,
    db_create_contract,
    db_get_latest_contract_id,
    db_insert_attestazione_log,
    db_insert_canone_calc,
    db_load_contract_context,
    db_load_immobili,
    db_prune_old_immobili_without_contracts,
    db_update_contract_fields,
    db_upsert_contract_overrides,
    db_upsert_contract_parties,
    db_upsert_immobile,
    db_upsert_person,
    db_upsert_visura,
    immobile_db_row,
    immobile_from_parsed_dict,
)
from uppi.services.storage_minio import StorageService
from uppi.utils.audit import mask_username, safe_unlink, sha256_file, sha256_text
from uppi.utils.parse_utils import clean_str, safe_float, to_bool_or_none

from uppi.docs.attestazione_template_filler import fill_attestazione_template, underscored
from uppi.domain.pescara2018_calc import compute_base_canone, CanoneCalculationError
from uppi.domain.canone_models import CanoneInput, ContractKind


logger = logging.getLogger(__name__)

AE_USERNAME = config("AE_USERNAME", default="").strip()
TEMPLATE_VERSION = config("TEMPLATE_VERSION", default="pescara2018_v1").strip()

PRUNE_OLD_IMMOBILI_WITHOUT_CONTRACTS = config("PRUNE_OLD_IMMOBILI_WITHOUT_CONTRACTS", default="True").strip().lower() == "true"
DELETE_LOCAL_VISURA_AFTER_UPLOAD = config("DELETE_LOCAL_VISURA_AFTER_UPLOAD", default="False").strip().lower() == "true"


def find_local_visura_pdf(cf: str, adapter: ItemAdapter) -> Optional[Path]:
    p = clean_str(adapter.get("visura_download_path"))
    if p:
        path = Path(p)
        if path.exists():
            return path

    fallback = get_visura_path(cf)
    if fallback.exists():
        return fallback

    client_dir = get_client_dir(cf)
    candidates = sorted(client_dir.glob("DOC_*.pdf"), key=lambda x: x.stat().st_mtime, reverse=True)
    if candidates:
        return candidates[0]

    any_pdf = sorted(client_dir.glob("*.pdf"), key=lambda x: x.stat().st_mtime, reverse=True)
    if any_pdf:
        return any_pdf[0]

    return None


def filter_immobiles_by_yaml(immobiles: List[Tuple[int, Immobile]], adapter: ItemAdapter) -> List[Tuple[int, Immobile]]:
    foglio_f = clean_str(adapter.get("foglio"))
    numero_f = clean_str(adapter.get("numero"))
    sub_f = clean_str(adapter.get("sub"))
    categoria_f = clean_str(adapter.get("categoria"))
    rendita_f = clean_str(adapter.get("rendita"))
    superficie_f = safe_float(adapter.get("superficie_totale"))

    out: List[Tuple[int, Immobile]] = []
    for imm_id, imm in immobiles:
        if foglio_f and str(getattr(imm, "foglio", "") or "") != foglio_f:
            continue
        if numero_f and str(getattr(imm, "numero", "") or "") != numero_f:
            continue
        if sub_f and str(getattr(imm, "sub", "") or "") != sub_f:
            continue
        if categoria_f and str(getattr(imm, "categoria", "") or "") != categoria_f:
            continue
        if rendita_f and str(getattr(imm, "rendita", "") or "") != rendita_f:
            continue
        if superficie_f is not None:
            st = getattr(imm, "superficie_totale", None)
            if st is not None:
                try:
                    if float(st) != float(superficie_f):
                        continue
                except Exception:
                    pass
        out.append((imm_id, imm))
        logger.info("[PIPELINE] Immobile ID=%s passed YAML filter", imm_id)
    return out


class VisuraProcessor:
    def __init__(
        self,
        storage: Optional[ObjectStorage] = None,
        template_path: Optional[Path] = None,
    ):
        self.storage_service = StorageService(storage)
        self.storage = storage or ObjectStorage()
        self.template_path = template_path or (
            Path(__file__).resolve().parents[2]
            / "attestazione_template"
            / "template_attestazione_pescara.docx"
        )

    def process_item(self, item, spider):
        adapter = ItemAdapter(item)

        locatore_cf = clean_str(adapter.get("locatore_cf") or adapter.get("codice_fiscale"))
        if not locatore_cf:
            spider.logger.error("[PIPELINE] Item без locatore_cf/codice_fiscale: %r", item)
            return item

        cond_cf = clean_str(adapter.get("conduttore_cf"))

        visura_source = clean_str(adapter.get("visura_source")) or "unknown"
        visura_downloaded = bool(adapter.get("visura_downloaded"))

        spider.logger.info(
            "[PIPELINE] CF=%s source=%s downloaded=%s",
            locatore_cf,
            visura_source,
            visura_downloaded,
        )

        conn = get_pg_connection()
        try:
            db_upsert_person(
                conn,
                locatore_cf,
                surname=clean_str(adapter.get("locatore_surname")),
                name=clean_str(adapter.get("locatore_name")),
            )
            if cond_cf:
                db_upsert_person(
                    conn,
                    cond_cf,
                    surname=clean_str(adapter.get("conduttore_surname")),
                    name=clean_str(adapter.get("conduttore_name")),
                )

            pdf_path = None
            fetched_now = False
            checksum = None
            pdf_to_delete: Path | None = None

            if visura_source == "sister" and visura_downloaded:
                pdf_path = find_local_visura_pdf(locatore_cf, adapter)

                if pdf_path is None:
                    raise FileNotFoundError(f"Visura PDF not found for CF={locatore_cf} (downloaded=True)")

                checksum = sha256_file(pdf_path)

                bucket = self.storage.cfg.visure_bucket
                obj_name = self.storage.visura_object_name(locatore_cf)
                self.storage_service.upload_file(bucket, obj_name, pdf_path, content_type="application/pdf")
                fetched_now = True

                db_upsert_visura(conn, locatore_cf, bucket, obj_name, checksum, fetched_now=True)

                pdf_to_delete = pdf_path

            else:
                bucket = self.storage.cfg.visure_bucket
                obj_name = self.storage.visura_object_name(locatore_cf)
                db_upsert_visura(conn, locatore_cf, bucket, obj_name, checksum_sha256=None, fetched_now=False)

                maybe_local = find_local_visura_pdf(locatore_cf, adapter)
                if maybe_local is not None:
                    pdf_to_delete = maybe_local

            keep_ids: List[int] = []
            if fetched_now and pdf_path is not None:
                parser = VisuraParser()
                parsed_dicts = parser.parse(pdf_path)

                if not parsed_dicts:
                    spider.logger.warning("[PIPELINE] No immobili parsed from PDF CF=%s", locatore_cf)
                else:
                    loc_surname = clean_str(parsed_dicts[0].get("locatore_surname"))
                    loc_name = clean_str(parsed_dicts[0].get("locatore_name"))
                    if loc_surname or loc_name:
                        db_upsert_person(conn, locatore_cf, surname=loc_surname, name=loc_name)

                    for d in parsed_dicts:
                        logger.info(
                            "[PIPELINE] PARSED immobile: foglio=%r numero=%r sub=%r categoria=%r sup=%r indirizzo=%r",
                            d.get("foglio"),
                            d.get("numero"),
                            d.get("sub"),
                            d.get("categoria"),
                            d.get("superficie_totale"),
                            d.get("via_name") or d.get("indirizzo_raw"),
                        )
                        imm = immobile_from_parsed_dict(d)

                        imm_id = db_upsert_immobile(conn, locatore_cf, imm)
                        keep_ids.append(imm_id)

                    deleted = db_prune_old_immobili_without_contracts(
                        conn,
                        locatore_cf,
                        keep_ids,
                        enabled=PRUNE_OLD_IMMOBILI_WITHOUT_CONTRACTS,
                    )
                    if deleted:
                        spider.logger.info("[DB] Pruned %d old immobili (no contracts) CF=%s", deleted, locatore_cf)

            immobili_db = db_load_immobili(conn, locatore_cf)
            if not immobili_db:
                spider.logger.error("[PIPELINE] No immobili in DB for CF=%s", locatore_cf)
                conn.commit()
                return item

            selected = filter_immobiles_by_yaml(immobili_db, adapter)
            if not selected:
                spider.logger.warning("[PIPELINE] No immobili matched YAML filter CF=%s", locatore_cf)
                conn.commit()
                return item

            for immobile_id, imm in selected:
                force_new_contract = bool(adapter.get("force_new_contract") or False)
                contract_id = None if force_new_contract else db_get_latest_contract_id(conn, immobile_id)
                if not contract_id:
                    contract_id = db_create_contract(conn, immobile_id)

                db_update_contract_fields(conn, contract_id, adapter)
                db_upsert_contract_parties(conn, contract_id, locatore_cf, cond_cf)
                db_upsert_contract_overrides(conn, contract_id, adapter)
                db_apply_contract_elements(conn, contract_id, adapter)

                contract_ctx = db_load_contract_context(conn, contract_id)

                canone_snapshot: Dict[str, Any] = {}
                canone_result_snapshot: Optional[Dict[str, Any]] = None

                if compute_base_canone is not None and CanoneInput is not None and ContractKind is not None:
                    try:
                        elements = contract_ctx.get("elements") or {}

                        def cnt(keys: List[str]) -> int:
                            return sum(1 for k in keys if str(elements.get(k, "") or "").strip() != "")

                        raw_kind = clean_str(adapter.get("contract_kind")) or "CONCORDATO"
                        raw_kind = raw_kind.upper()
                        try:
                            kind_enum = ContractKind[raw_kind]
                        except Exception:
                            kind_enum = ContractKind.CONCORDATO

                        sup = getattr(imm, "superficie_totale", None)
                        if sup is None:
                            sup = safe_float(adapter.get("superficie_totale"))

                        can_in = CanoneInput(
                            superficie_catastale=float(sup or 0.0),
                            micro_zona=clean_str(getattr(imm, "micro_zona", None)),
                            foglio=clean_str(getattr(imm, "foglio", None)),
                            categoria_catasto=clean_str(getattr(imm, "categoria", None)),
                            classe_catasto=clean_str(getattr(imm, "classe", None)),
                            count_a=cnt(["a1", "a2"]),
                            count_b=cnt([f"b{i}" for i in range(1, 6)]),
                            count_c=cnt([f"c{i}" for i in range(1, 8)]),
                            count_d=cnt([f"d{i}" for i in range(1, 14)]),
                            arredato=bool(to_bool_or_none(adapter.get("arredato"))),
                            energy_class=clean_str(adapter.get("energy_class")),
                            contract_kind=kind_enum,
                            durata_anni=int(adapter.get("durata_anni") or 3),
                        )

                        canone_snapshot = {
                            "superficie_catastale": can_in.superficie_catastale,
                            "micro_zona": can_in.micro_zona,
                            "foglio": can_in.foglio,
                            "categoria_catasto": can_in.categoria_catasto,
                            "classe_catasto": can_in.classe_catasto,
                            "count_a": can_in.count_a,
                            "count_b": can_in.count_b,
                            "count_c": can_in.count_c,
                            "count_d": can_in.count_d,
                            "arredato": can_in.arredato,
                            "energy_class": can_in.energy_class,
                            "contract_kind": str(can_in.contract_kind),
                            "durata_anni": can_in.durata_anni,
                        }

                        can_res = compute_base_canone(can_in)
                        canone_result_snapshot = {
                            "zona": getattr(can_res, "zona", None),
                            "subfascia": getattr(can_res, "subfascia", None),
                            "base_min_euro_mq": getattr(can_res, "base_min_euro_mq", None),
                            "base_max_euro_mq": getattr(can_res, "base_max_euro_mq", None),
                            "base_euro_mq": getattr(can_res, "base_euro_mq", None),
                            "canone_base_annuo": getattr(can_res, "canone_base_annuo", None),
                            "canone_finale_annuo": getattr(can_res, "canone_finale_annuo", None),
                            "canone_finale_mensile": getattr(can_res, "canone_finale_mensile", None),
                        }

                        result_m = None
                        try:
                            result_m = float(getattr(can_res, "canone_finale_mensile", None))
                        except Exception:
                            result_m = None

                        db_insert_canone_calc(
                            conn,
                            contract_id=contract_id,
                            method="pescara2018_base",
                            inputs={"canone_input": canone_snapshot, "result": canone_result_snapshot},
                            result_mensile=result_m,
                        )

                        contract_ctx = db_load_contract_context(conn, contract_id)

                    except CanoneCalculationError as e:
                        spider.logger.warning(
                            "[CANONE] Logical error contract=%s immobile_id=%s: %s",
                            contract_id,
                            immobile_id,
                            e,
                        )
                    except Exception as e:
                        spider.logger.exception(
                            "[CANONE] Unexpected error contract=%s immobile_id=%s: %s",
                            contract_id,
                            immobile_id,
                            e,
                        )

                params = build_template_params(adapter, imm, contract_ctx)

                output_path = get_attestazione_path(locatore_cf, contract_id, imm)
                output_path.parent.mkdir(parents=True, exist_ok=True)

                try:
                    fill_attestazione_template(
                        template_path=str(self.template_path),
                        output_folder=str(output_path.parent),
                        filename=output_path.name,
                        params=params,
                        underscored=underscored,
                    )
                except Exception as e:
                    spider.logger.exception("[PIPELINE] DOCX generation failed contract=%s: %s", contract_id, e)
                    db_insert_attestazione_log(
                        conn,
                        contract_id=contract_id,
                        status="failed",
                        output_bucket=self.storage.cfg.attestazioni_bucket,
                        output_object=self.storage.attestazione_object_name(locatore_cf, contract_id),
                        params_snapshot={
                            "locatore_cf": locatore_cf,
                            "contract_id": contract_id,
                            "error_stage": "docx_generation",
                        },
                        error=str(e)[:5000],
                        author_login_masked=mask_username(AE_USERNAME),
                        author_login_sha256=sha256_text(AE_USERNAME),
                        template_version=TEMPLATE_VERSION,
                    )
                    continue

                try:
                    out_bucket = self.storage.cfg.attestazioni_bucket
                    out_obj = self.storage.attestazione_object_name(locatore_cf, contract_id)
                    self.storage_service.upload_file(
                        out_bucket,
                        out_obj,
                        output_path,
                        content_type="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                    )

                    params_snapshot = {
                        "locatore_cf": locatore_cf,
                        "immobile_id": immobile_id,
                        "contract_id": contract_id,
                        "yaml_item": dict(adapter.asdict()),
                        "immobile_parsed": immobile_db_row(imm),
                        "contract_ctx": contract_ctx,
                        "template_version": TEMPLATE_VERSION,
                        "author_login_masked": mask_username(AE_USERNAME),
                        "canone_input": canone_snapshot,
                        "canone_result": canone_result_snapshot,
                        "output": {"bucket": out_bucket, "object": out_obj},
                    }

                    db_insert_attestazione_log(
                        conn,
                        contract_id=contract_id,
                        status="generated",
                        output_bucket=out_bucket,
                        output_object=out_obj,
                        params_snapshot=params_snapshot,
                        error=None,
                        author_login_masked=mask_username(AE_USERNAME),
                        author_login_sha256=sha256_text(AE_USERNAME),
                        template_version=TEMPLATE_VERSION,
                    )

                    spider.logger.info("[PIPELINE] Attestazione OK contract=%s -> %s/%s", contract_id, out_bucket, out_obj)

                except Exception as e:
                    spider.logger.exception("[PIPELINE] Upload/log attestazione failed contract=%s: %s", contract_id, e)
                    db_insert_attestazione_log(
                        conn,
                        contract_id=contract_id,
                        status="failed",
                        output_bucket=self.storage.cfg.attestazioni_bucket,
                        output_object=self.storage.attestazione_object_name(locatore_cf, contract_id),
                        params_snapshot={
                            "locatore_cf": locatore_cf,
                            "contract_id": contract_id,
                            "error_stage": "upload_or_log",
                        },
                        error=str(e)[:5000],
                        author_login_masked=mask_username(AE_USERNAME),
                        author_login_sha256=sha256_text(AE_USERNAME),
                        template_version=TEMPLATE_VERSION,
                    )

            conn.commit()
            if DELETE_LOCAL_VISURA_AFTER_UPLOAD and pdf_to_delete is not None:
                safe_unlink(pdf_to_delete)
            return item

        except psycopg2.Error as e:
            spider.logger.exception("[PIPELINE] DB error CF=%s: %s", locatore_cf, e)
            try:
                conn.rollback()
            except Exception:
                pass
            return item

        except Exception as e:
            spider.logger.exception("[PIPELINE] Fatal error CF=%s: %s", locatore_cf, e)
            try:
                conn.rollback()
            except Exception:
                pass
            return item

        finally:
            conn.close()
