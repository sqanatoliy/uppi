from __future__ import annotations

from typing import Any, Dict, List

from uppi.domain.immobile import Immobile
from uppi.utils.audit import format_person_fullname
from uppi.utils.parse_utils import clean_str


ELEMENT_KEYS = (
    ["a1", "a2"]
    + [f"b{i}" for i in range(1, 6)]
    + [f"c{i}" for i in range(1, 8)]
    + [f"d{i}" for i in range(1, 14)]
)


def build_template_params(adapter, imm: Immobile, contract_ctx: Dict[str, Any]) -> Dict[str, str]:
    params: Dict[str, str] = {}

    # Розпаковуємо контекст
    overrides = contract_ctx.get("overrides") or {}
    elements = contract_ctx.get("elements") or {}
    contract = contract_ctx.get("contract") or {}
    parties = contract_ctx.get("parties") or {}
    
    # 1. ДАНІ ОРЕНДОДАВЦЯ (LOCATORE)
    loc = parties.get("LOCATORE") or {}
    loc_addr = loc.get("address") or {} # Нова вкладена структура адреси
    
    loc_cf = clean_str(loc.get("cf") or adapter.get("locatore_cf") or "")
    params["{{LOCATORE_CF}}"] = loc_cf
    params["{{LOCATORE_NOME}}"] = format_person_fullname(loc.get("name"), loc.get("surname"))
    
    # Пріоритет: Override -> Дані з БД (адреса особи) -> Adapter
    params["{{LOCATORE_COMUNE_RES}}"] = str(overrides.get("locatore_comune_res") or loc_addr.get("comune") or adapter.get("locatore_comune_res") or "")
    params["{{LOCATORE_VIA}}"] = str(overrides.get("locatore_via") or loc_addr.get("via_full") or adapter.get("locatore_via") or "")
    params["{{LOCATORE_CIVICO}}"] = str(overrides.get("locatore_civico") or loc_addr.get("civico") or adapter.get("locatore_civico") or "")

    # 2. ДАНІ ОБ'ЄКТА НЕРУХОМОСТІ (IMMOBILE)
    # Використовуємо реальну адресу об'єкта з БД (immobile_address)
    imm_addr = contract_ctx.get("immobile") or {}
    
    params["{{IMMOBILE_COMUNE}}"] = str(overrides.get("immobile_comune_override") or imm_addr.get("comune") or "")
    params["{{IMMOBILE_VIA}}"] = str(overrides.get("immobile_via_override") or imm_addr.get("via") or "")
    params["{{IMMOBILE_CIVICO}}"] = str(overrides.get("immobile_civico_override") or imm_addr.get("civico") or "")
    params["{{IMMOBILE_PIANO}}"] = str(overrides.get("immobile_piano_override") or imm_addr.get("piano") or "")
    params["{{IMMOBILE_INTERNO}}"] = str(overrides.get("immobile_interno_override") or imm_addr.get("interno") or "")

    # Кадастрові дані (з об'єкта Immobile)
    params["{{FOGLIO}}"] = str(getattr(imm, "foglio", "") or "")
    params["{{NUMERO}}"] = str(getattr(imm, "numero", "") or "")
    params["{{SUB}}"] = str(getattr(imm, "sub", "") or "")
    params["{{RENDITA}}"] = str(getattr(imm, "rendita", "") or "")
    params["{{SUPERFICIE_TOTALE}}"] = str(getattr(imm, "superficie_totale", "") or "")
    params["{{CATEGORIA}}"] = str(getattr(imm, "categoria", "") or "")

    # APPARTAMENTO 
    params["{{APP_FOGL}}"] = params["{{FOGLIO}}"]
    params["{{APP_PART}}"] = params["{{NUMERO}}"]
    params["{{APP_SUB}}"] = params["{{SUB}}"]
    params["{{APP_REND}}"] = params["{{RENDITA}}"]
    params["{{APP_SCAT}}"] = params["{{SUPERFICIE_TOTALE}}"]
    params["{{APP_SRIP}}"] = "X"
    params["{{APP_CAT}}"] = params["{{CATEGORIA}}"]

    # GARAGE/BOX/CANTINA
    params["{{GAR_FOGL}}"] = "X"
    params["{{GAR_PART}}"] = "X"
    params["{{GAR_SUB}}"] = "X"
    params["{{GAR_REND}}"] = "X"
    params["{{GAR_SCAT}}"] = "X"
    params["{{GAR_SRIP}}"] = "X"
    params["{{GAR_CAT}}"] = "X"
    
    # POSTO AUTO
    params["{{PST_FOGL}}"] = "X"
    params["{{PST_PART}}"] = "X"
    params["{{PST_SUB}}"] = "X"
    params["{{PST_REND}}"] = "X"
    params["{{PST_SCAT}}"] = "X"
    params["{{PST_SRIP}}"] = "X"
    params["{{PST_CAT}}"] = "X"

    # TOTALE
    params["{{TOT_SCAT}}"] = params["{{SUPERFICIE_TOTALE}}"]
    params["{{TOT_SRIP}}"] = "X"
    params["{{TOT_CAT}}"] = "X"

    for prefix in ("GAR", "PST"):
        for suffix in ("FOGL", "PART", "SUB", "REND", "SCAT", "SRIP", "CAT"):
            params[f"{{{{{prefix}_{suffix}}}}}"] = ""

# 3. ДАНІ КОНТРАКТУ
    # !!! Беремо СУВОРО з адаптера (YAML) для реєстраційних даних, 
    # щоб ігнорувати старі записи в БД, якщо в YAML пусто.

    # --- БЕРЕТЬСЯ Тільки з YAML ---
    params["{{CONTRATTO_DATA}}"] = clean_str(adapter.get("contratto_data")) or ""
    params["{{DECORRENZA_DATA}}"] = clean_str(adapter.get("decorrenza_data")) or ""
    params["{{REGISTRAZIONE_DATA}}"] = clean_str(adapter.get("registrazione_data")) or ""
    params["{{REGISTRAZIONE_NUM}}"] = clean_str(adapter.get("registrazione_num")) or ""
    params["{{AGENZIA_ENTRATE_SEDE}}"] = clean_str(adapter.get("agenzia_entrate_sede")) or ""
    # ---------------------------------

    # 4. ДАНІ ОРЕНДАРЯ (CONDUTTORE)
    # !!! ТУТ ЗМІНИ: Ігноруємо БД (parties.get("CONDUTTORE")), беремо тільки з YAML.
    # Якщо в YAML пусто -> буде пуста строка -> будуть підкреслення.
    
    cond_nome = clean_str(adapter.get("conduttore_nome")) or ""
    cond_cf = clean_str(adapter.get("conduttore_cf")) or ""
    cond_comune = clean_str(adapter.get("conduttore_comune")) or ""
    cond_via = clean_str(adapter.get("conduttore_via")) or ""

    params["{{CONDUTTORE_NOME}}"] = cond_nome
    params["{{CONDUTTORE_CF}}"] = cond_cf
    params["{{CONDUTTORE_COMUNE}}"] = cond_comune
    params["{{CONDUTTORE_VIA}}"] = cond_via

    # ЕЛЕМЕНТИ A-D (Заповнення хрестиків)
    for grp in ['a', 'b', 'c', 'd']:
        for i in range(1, 15):
            key = f"{grp}{i}"  # генерує a1, a2... d14
            val = str(elements.get(key, "") or "")
            # Заповнюємо різні варіанти тегів, які можуть бути у Word
            params[f"{{{{{key.upper()}}}}}"] = val # {{{A1}}} (якщо docxtpl потребує явних дужок)
            params[f"{{{{{key.lower()}}}}}"] = val              # {{a1}}

    def cnt(keys: List[str]) -> int:
        return sum(1 for k in keys if str(elements.get(k, "") or "").strip() != "")

    params["{{A_CNT}}"] = str(cnt(["a1", "a2"]))
    params["{{B_CNT}}"] = str(cnt([f"b{i}" for i in range(1, 6)]))
    params["{{C_CNT}}"] = str(cnt([f"c{i}" for i in range(1, 8)]))
    params["{{D_CNT}}"] = str(cnt([f"d{i}" for i in range(1, 14)]))

    for ph in [
        "CAN_ZONA",
        "CAN_SUBFASCIA",
        "CAN_MQ",
        "CAN_MQ_ANNUO",
        "CAN_ISTAT",
        "CAN_TOTALE_ANNUO",
        "CAN_ARREDATO",
        "CAN_CLASSE_A",
        "CAN_CLASSE_B",
        "CAN_ENERGY",
        "CAN_DURATA",
        "CAN_TRANSITORIO",
        "CAN_STUDENTI",
        "CAN_ANNUO_VAR_MIN",
        "CAN_ANNUO_VAR_MAX",
        "CAN_MENSILE_VAR_MIN",
        "CAN_MENSILE_VAR_MAX",
        "CAN_MENSILE",
    ]:
        params[f"{{{{{ph}}}}}"] = ""

    def _fmt_num(x, decimals=2) -> str:
        """ Форматує число з фіксованою кількістю десяткових знаків."""
        if x is None:
            return ""
        try:
            val_dec = float(x)
        except Exception:
            return str(x)
        return f"{val_dec:.{decimals}f}"

    def _pct_str(p: float) -> str:
        """ Форматує відсоток як рядок з знаком +/-. """
        sign = "+" if p > 0 else ""
        return f"{sign}{int(round(p * 100))}%"

    can = contract_ctx.get("canone_calc") or {}
    cin = can.get("canone_input") or {}
    res = can.get("result") or {}

    if cin:
        istat = cin.get("istat")
        params["{{CAN_ISTAT}}"] = "" if not istat else f"ISTAT (+{_fmt_num(istat, 2)}%)"
        # Для ігнорування надбавок пізніше
        ignore_surcharges = bool(cin.get("ignore_surcharges"))

            

    if res:
        zona = res.get("zona")
        subfascia = res.get("subfascia")
        # # Базова вартість за м² (неформатована) не використовується в розрахунках далі тому що використовується base_euro_mq_istat
        # base_euro_mq = res.get("base_euro_mq")
        base_euro_mq_istat = res.get("base_euro_mq_istat")



        mq = cin.get("superficie_catastale")
        if mq is None:
            mq = getattr(imm, "superficie_totale", None)

        params["{{CAN_ZONA}}"] = "" if zona is None else str(zona)
        params["{{CAN_SUBFASCIA}}"] = "" if subfascia is None else str(subfascia)
        params["{{CAN_MQ}}"] = _fmt_num(mq, 0)
        params["{{CAN_MQ_ANNUO}}"] = _fmt_num(base_euro_mq_istat, 2)


        # Отримуємо чисте число (НЕ форматуємо його поки що)
        raw_canone = res.get("canone_base_annuo") or res.get("canone_finale_annuo")
        # Записуємо в params форматований рядок для шаблону
        params["{{CAN_TOTALE_ANNUO}}"] = _fmt_num(raw_canone, 2)

        delta_pct = 0.0

        arredato = cin.get("arredato")
        if arredato:
            # Використовуємо для розрахунку число raw_canone, а не рядок
            valore_arredato = raw_canone * float(arredato)
            # Форматуємо результат розрахунку перед записом у шаблон
            params["{{CAN_ARREDATO}}"] = f"({_pct_str(arredato)})  +{_fmt_num(valore_arredato, 2)}"
            delta_pct += arredato

        energy = (cin.get("energy_class") or "").strip().upper()
        # Спочатку очищаємо ВСІ теги енергії, щоб не було "сміття"
        params["{{CAN_CLASSE_A}}"] = ""
        params["{{CAN_CLASSE_B}}"] = ""
        params["{{CAN_ENERGY}}"] = ""
        if energy == "A":
            valore_energy_a = raw_canone * 0.08
            params["{{CAN_CLASSE_A}}"] = f"({_pct_str(0.08)}) +{_fmt_num(valore_energy_a, 2)}"
            delta_pct += 0.08
        elif energy == "B":
            valore_energy_b = raw_canone * 0.04
            params["{{CAN_CLASSE_B}}"] = f"({_pct_str(0.04)}) +{_fmt_num(valore_energy_b, 2)}"
            delta_pct += 0.04
        elif energy in ("E", "F", "G"): # Об'єднуємо штрафні класи
            pct = {"E": -0.02, "F": -0.04, "G": -0.06}[energy]
            valore_energy = raw_canone * pct
            params["{{CAN_ENERGY}}"] = f"({_pct_str(pct)}) {_fmt_num(valore_energy, 2)}"
            delta_pct += pct

        durata = cin.get("durata_anni")
        try:
            durata = int(durata) if durata is not None else None
        except Exception:
            durata = None

        if durata == 4:
            valore_durata_4 = raw_canone * 0.05
            params["{{CAN_DURATA}}"] = f"({_pct_str(0.05)}) +{_fmt_num(valore_durata_4, 2)}"
            delta_pct += 0.05
        elif durata == 5:
            valore_durata_5 = raw_canone * 0.06
            params["{{CAN_DURATA}}"] = f"({_pct_str(0.06)}) +{_fmt_num(valore_durata_5, 2)}"
            delta_pct += 0.06
        elif durata is not None and durata >= 6:
            valore_durata_6 = raw_canone * 0.07
            params["{{CAN_DURATA}}"] = f"({_pct_str(0.07)}) +{_fmt_num(valore_durata_6, 2)}"
            delta_pct += 0.07

        # 4. CONTRACT KIND (Transitorio/Studenti) - ТУТ ЛОГІКА IGNORE
        raw_kind = str(cin.get("contract_kind") or "")
        kind = raw_kind.split(".")[-1].upper()
        if kind == "TRANSITORIO":
            valore_transitorio = raw_canone * 0.15
            # Текст в шаблон йде завжди
            params["{{CAN_TRANSITORIO}}"] = f"({_pct_str(0.15)}) +{_fmt_num(valore_transitorio, 2)}"
            
            # Вплив на розрахунок діапазону тільки якщо не ігноруємо
            if not ignore_surcharges:
                delta_pct += 0.15
                
        elif kind == "STUDENTI":
            valore_studenti = raw_canone * 0.20
            params["{{CAN_STUDENTI}}"] = f"({_pct_str(0.20)}) +{_fmt_num(valore_studenti, 2)}"
            
            if not ignore_surcharges:
                delta_pct += 0.20

        min_eur = res.get("base_min_euro_mq")
        max_eur = res.get("base_max_euro_mq")

        try:
            mq_f = float(mq or 0.0)
            min_annuo = float(min_eur or 0.0) * mq_f
            max_annuo = float(max_eur or 0.0) * mq_f
            min_annuo_v = min_annuo * (1.0 + delta_pct)
            max_annuo_v = max_annuo * (1.0 + delta_pct)

            params["{{CAN_ANNUO_VAR_MIN}}"] = _fmt_num(min_annuo_v, 2)
            params["{{CAN_ANNUO_VAR_MAX}}"] = _fmt_num(max_annuo_v, 2)
            params["{{CAN_MENSILE_VAR_MIN}}"] = _fmt_num(min_annuo_v / 12.0, 2)
            params["{{CAN_MENSILE_VAR_MAX}}"] = _fmt_num(max_annuo_v / 12.0, 2)
        except Exception:
            pass

    agreed = adapter.get("canone_contrattuale_mensile") or contract.get("canone_contrattuale_mensile")
    if agreed is not None:
        params["{{CAN_MENSILE}}"] = _fmt_num(agreed, 2)

    return params
