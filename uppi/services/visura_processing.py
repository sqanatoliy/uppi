from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from uppi.docs.visura_pdf_parser import VisuraParser
from uppi.domain.immobile import Immobile
from uppi.domain.storage import get_attestazione_path, get_client_dir, get_visura_path
from uppi.services.attestazione_generator import AttestazioneGenerator
from uppi.services.db_repo import DatabaseRepository
from uppi.services.storage_minio import StorageService
from uppi.utils.audit import mask_username, safe_unlink, sha256_file
from uppi.utils.parse_utils import clean_str, safe_float

logger = logging.getLogger(__name__)


class VisuraProcessingService:
    def __init__(
        self,
        repo: DatabaseRepository,
        storage: StorageService,
        template_path: Path,
        delete_local_visura_after_upload: bool,
        prune_old_immobili_without_contracts: bool,
    ):
        self.repo = repo
        self.storage = storage
        self.parser = VisuraParser()
        self.generator = AttestazioneGenerator(template_path=template_path)
        self.delete_local_visura_after_upload = delete_local_visura_after_upload
        self.prune_old_immobili_without_contracts = prune_old_immobili_without_contracts

    def _find_local_visura_pdf(self, cf: str, payload: Dict[str, Any]) -> Optional[Path]:
        p = clean_str(payload.get("visura_download_path"))
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

    def _filter_immobiles_by_yaml(
        self,
        immobili: List[Tuple[int, Immobile]],
        payload: Dict[str, Any],
    ) -> List[Tuple[int, Immobile]]:
        foglio_f = clean_str(payload.get("foglio"))
        numero_f = clean_str(payload.get("numero"))
        sub_f = clean_str(payload.get("sub"))
        categoria_f = clean_str(payload.get("categoria"))
        rendita_f = clean_str(payload.get("rendita"))
        superficie_f = safe_float(payload.get("superficie_totale"))

        out: List[Tuple[int, Immobile]] = []
        for imm_id, imm in immobili:
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
            logger.info("[PIPELINE] Immobile ID=%s пройшов фільтр YAML.", imm_id)
        return out

    def process_item(self, payload: Dict[str, Any], conn, spider_logger: logging.Logger) -> None:
        locatore_cf = clean_str(payload.get("locatore_cf") or payload.get("codice_fiscale"))
        if not locatore_cf:
            spider_logger.error("[PIPELINE] Item без locatore_cf/codice_fiscale: %r", payload)
            return

        cond_cf = clean_str(payload.get("conduttore_cf"))

        visura_source = clean_str(payload.get("visura_source")) or "unknown"
        visura_downloaded = bool(payload.get("visura_downloaded"))
        force_update_visura = bool(payload.get("force_update_visura"))

        spider_logger.info(
            "[PIPELINE] CF=%s source=%s downloaded=%s force_update_visura=%s",
            locatore_cf,
            visura_source,
            visura_downloaded,
            force_update_visura,
        )

        self.repo.upsert_person(
            conn,
            locatore_cf,
            surname=clean_str(payload.get("locatore_surname")),
            name=clean_str(payload.get("locatore_name")),
        )
        if cond_cf:
            self.repo.upsert_person(
                conn,
                cond_cf,
                surname=clean_str(payload.get("conduttore_surname")),
                name=clean_str(payload.get("conduttore_name")),
            )

        pdf_path = None
        fetched_now = False
        checksum = None
        pdf_to_delete: Path | None = None

        bucket = self.storage.config.visure_bucket
        obj_name = self.storage.visura_object_name(locatore_cf)

        if visura_source == "sister" and visura_downloaded:
            pdf_path = self._find_local_visura_pdf(locatore_cf, payload)

            if pdf_path is None:
                raise FileNotFoundError(f"Visura PDF not found for CF={locatore_cf} (downloaded=True)")

            checksum = sha256_file(pdf_path)
            self.storage.upload_file(bucket, obj_name, pdf_path, content_type="application/pdf")
            fetched_now = True

            self.repo.upsert_visura(conn, locatore_cf, bucket, obj_name, checksum, fetched_now=True)

            pdf_to_delete = pdf_path
        else:
            self.repo.upsert_visura(conn, locatore_cf, bucket, obj_name, checksum_sha256=None, fetched_now=False)

            maybe_local = self._find_local_visura_pdf(locatore_cf, payload)
            if maybe_local is not None:
                pdf_to_delete = maybe_local

        keep_ids: List[int] = []
        if fetched_now and pdf_path is not None:
            parsed_dicts = self.parser.parse(pdf_path)

            if not parsed_dicts:
                spider_logger.warning("[PIPELINE] No immobili parsed from PDF CF=%s", locatore_cf)
            else:
                loc_surname = clean_str(parsed_dicts[0].get("locatore_surname"))
                loc_name = clean_str(parsed_dicts[0].get("locatore_name"))
                if loc_surname or loc_name:
                    self.repo.upsert_person(conn, locatore_cf, surname=loc_surname, name=loc_name)

                for data in parsed_dicts:
                    logger.info(
                        "[PIPELINE] PARSED immobile: foglio=%r numero=%r sub=%r categoria=%r sup=%r indirizzo=%r",
                        data.get("foglio"),
                        data.get("numero"),
                        data.get("sub"),
                        data.get("categoria"),
                        data.get("superficie_totale"),
                        data.get("via_name") or data.get("indirizzo_raw"),
                    )
                    imm = self.repo.immobile_from_parsed_dict(data)

                    imm_id = self.repo.upsert_immobile(conn, locatore_cf, imm)
                    logger.info("[PIPELINE] UPSERT immobile -> id=%r", imm_id)

                    keep_ids.append(imm_id)

                logger.info("[PIPELINE] keep_ids=%s (count=%d)", keep_ids, len(keep_ids))

                if self.prune_old_immobili_without_contracts:
                    deleted = self.repo.prune_old_immobili_without_contracts(conn, locatore_cf, keep_ids)
                    if deleted:
                        spider_logger.info("[DB] Pruned %d old immobili (no contracts) CF=%s", deleted, locatore_cf)

        immobili_db = self.repo.load_immobili(conn, locatore_cf)
        if not immobili_db:
            spider_logger.error("[PIPELINE] No immobili in DB for CF=%s", locatore_cf)
            return

        selected = self._filter_immobiles_by_yaml(immobili_db, payload)
        if not selected:
            spider_logger.warning("[PIPELINE] No immobili matched YAML filter CF=%s", locatore_cf)
            return

        for immobile_id, imm in selected:
            force_new_contract = bool(payload.get("force_new_contract") or False)
            contract_id = None if force_new_contract else self.repo.get_latest_contract_id(conn, immobile_id)
            if not contract_id:
                contract_id = self.repo.create_contract(conn, immobile_id)

            self.repo.update_contract_fields(conn, contract_id, payload)
            self.repo.upsert_contract_parties(conn, contract_id, locatore_cf, cond_cf)
            self.repo.upsert_contract_overrides(conn, contract_id, payload)
            self.repo.apply_contract_elements(conn, contract_id, payload)

            contract_ctx = self.repo.load_contract_context(conn, contract_id)

            canone_snapshot: Dict[str, Any] = {}
            canone_result_snapshot: Optional[Dict[str, Any]] = None

            try:
                from uppi.domain.pescara2018_calc import compute_base_canone, CanoneCalculationError
                from uppi.domain.canone_models import CanoneInput, ContractKind
            except Exception:  # pragma: no cover
                compute_base_canone = None
                CanoneCalculationError = Exception
                CanoneInput = None
                ContractKind = None

            if compute_base_canone is not None and CanoneInput is not None and ContractKind is not None:
                try:
                    elements = contract_ctx.get("elements") or {}

                    def cnt(keys: List[str]) -> int:
                        return sum(1 for k in keys if str(elements.get(k, "") or "").strip() != "")

                    raw_kind = clean_str(payload.get("contract_kind")) or "CONCORDATO"
                    raw_kind = raw_kind.upper()
                    try:
                        kind_enum = ContractKind[raw_kind]
                    except Exception:
                        kind_enum = ContractKind.CONCORDATO

                    sup = getattr(imm, "superficie_totale", None)
                    if sup is None:
                        sup = safe_float(payload.get("superficie_totale"))

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
                        arredato=bool(payload.get("arredato")),
                        energy_class=clean_str(payload.get("energy_class")),
                        contract_kind=kind_enum,
                        durata_anni=int(payload.get("durata_anni") or 3),
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

                    self.repo.insert_canone_calc(
                        conn,
                        contract_id=contract_id,
                        method="pescara2018_base",
                        inputs={"canone_input": canone_snapshot, "result": canone_result_snapshot},
                        result_mensile=result_m,
                    )

                    contract_ctx = self.repo.load_contract_context(conn, contract_id)

                except CanoneCalculationError as exc:
                    spider_logger.warning("[CANONE] Logical error contract=%s immobile_id=%s: %s", contract_id, immobile_id, exc)
                except Exception as exc:
                    spider_logger.exception("[CANONE] Unexpected error contract=%s immobile_id=%s: %s", contract_id, immobile_id, exc)

            params = self.generator.build_template_params(payload, imm, contract_ctx)

            output_path = get_attestazione_path(locatore_cf, contract_id, imm)

            try:
                self.generator.render_docx(output_path, params)
            except Exception as exc:
                spider_logger.exception("[PIPELINE] DOCX generation failed contract=%s: %s", contract_id, exc)
                self.repo.insert_attestazione_log(
                    conn,
                    contract_id=contract_id,
                    status="failed",
                    output_bucket=self.storage.config.attestazioni_bucket,
                    output_object=self.storage.attestazione_object_name(locatore_cf, contract_id),
                    params_snapshot={
                        "locatore_cf": locatore_cf,
                        "contract_id": contract_id,
                        "error_stage": "docx_generation",
                    },
                    error=str(exc)[:5000],
                )
                continue

            try:
                out_bucket = self.storage.config.attestazioni_bucket
                out_obj = self.storage.attestazione_object_name(locatore_cf, contract_id)
                self.storage.upload_file(
                    out_bucket,
                    out_obj,
                    output_path,
                    content_type="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                )

                params_snapshot = {
                    "locatore_cf": locatore_cf,
                    "immobile_id": immobile_id,
                    "contract_id": contract_id,
                    "yaml_item": payload,
                    "immobile_parsed": self.repo.immobile_db_row(imm),
                    "contract_ctx": contract_ctx,
                    "template_version": self.repo.template_version,
                    "author_login_masked": mask_username(self.repo.ae_username),
                    "canone_input": canone_snapshot,
                    "canone_result": canone_result_snapshot,
                    "output": {"bucket": out_bucket, "object": out_obj},
                }

                self.repo.insert_attestazione_log(
                    conn,
                    contract_id=contract_id,
                    status="generated",
                    output_bucket=out_bucket,
                    output_object=out_obj,
                    params_snapshot=params_snapshot,
                    error=None,
                )

                spider_logger.info("[PIPELINE] Attestazione OK contract=%s -> %s/%s", contract_id, out_bucket, out_obj)

            except Exception as exc:
                spider_logger.exception("[PIPELINE] Upload/log attestazione failed contract=%s: %s", contract_id, exc)
                self.repo.insert_attestazione_log(
                    conn,
                    contract_id=contract_id,
                    status="failed",
                    output_bucket=self.storage.config.attestazioni_bucket,
                    output_object=self.storage.attestazione_object_name(locatore_cf, contract_id),
                    params_snapshot={
                        "locatore_cf": locatore_cf,
                        "contract_id": contract_id,
                        "error_stage": "upload_or_log",
                    },
                    error=str(exc)[:5000],
                )

        if self.delete_local_visura_after_upload and pdf_to_delete is not None:
            safe_unlink(pdf_to_delete)
