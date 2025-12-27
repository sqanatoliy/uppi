# uppi/domain/pescara2018_calc.py

from __future__ import annotations

import logging
from typing import Optional

from uppi.domain.canone_models import (
    CanoneInput,
    CanoneResult,
    Tipologia,
)
from uppi.domain.pescara2018_data import (
    BASE_RANGES,
    MICROZONA_TO_ZONA,
    FOGLIO_TO_ZONA,
)

logger = logging.getLogger(__name__)


class CanoneCalculationError(Exception):
    """
    Будь-які логічні помилки при розрахунку канону за Accordo Pescara 2018.

    Наприклад:
    - невідома micro_zona / foglio;
    - некоректна площа (<= 0);
    - відсутні комбінації в BASE_RANGES.
    """


# ---------------------------------------------------------------------------
# Допоміжні нормалізатори
# ---------------------------------------------------------------------------

def _parse_cat(categoria: Optional[str]) -> str:
    """
    Нормалізує категорію типу 'A/2 ' -> 'A/2'.

    Якщо categoria порожня/None → повертає "".
    """
    if not categoria:
        return ""
    return categoria.strip().upper().replace(" ", "")


def _normalize_foglio_key(foglio: str) -> list[str]:
    """
    Нормалізує foglio в список можливих ключів для FOGLIO_TO_ZONA.

    - обрізає пробіли;
    - якщо це чисто цифри → додає варіант без лідируючих нулів;
    - результат: список ключів, які можна по черзі спробувати в FOGLIO_TO_ZONA.

    Приклад:
        "02"   -> ["02", "2"]
        "11"   -> ["11", "11"]  (int(11) == 11)
        "27/A" -> ["27/A"]      (не digits — як є)
    """
    raw = str(foglio).strip()
    keys = [raw]

    if raw.isdigit():
        try:
            no_zeros = str(int(raw))
            if no_zeros not in keys:
                keys.append(no_zeros)
        except ValueError:
            # дуже малоймовірно, але якщо int() ляже — ігноруємо
            pass

    return keys


# ---------------------------------------------------------------------------
# Класифікація зони, типології, субфасції
# ---------------------------------------------------------------------------

def classify_zona(micro_zona: Optional[str], foglio: Optional[str]) -> int:
    """
    Визначає зону 1..4 за micro_zona або foglio.

    Порядок:
      1) Якщо micro_zona задана і відома в MICROZONA_TO_ZONA → використовуємо її.
      2) Інакше, якщо foglio заданий → пробуємо знайти його в FOGLIO_TO_ZONA
         (з нормалізацією лідируючих нулів).
      3) Якщо не знайшли ні по micro_zona, ні по foglio → CanoneCalculationError.

    ВАЖЛИВО ПРО FOG-LIO 27:
    - В Accordo foglio 27 розбитий на 27/1 і 27/2 у різних зонах.
    - Ми навмисно НЕ додаємо ключ "27" в FOGLIO_TO_ZONA, щоб не робити
      тупого вгадування.
    - Якщо для такого об'єкта немає micro_zona, ми чесно кидаємо помилку.
    """
    # 1) Спроба по micro_zona
    if micro_zona:
        key = str(micro_zona).strip()
        zona = MICROZONA_TO_ZONA.get(key)
        if zona is not None:
            return zona

        logger.warning(
            "[CANONE] Невідома micro_zona=%r — немає в MICROZONA_TO_ZONA, "
            "пробую fallback по foglio=%r",
            micro_zona,
            foglio,
        )

    # 2) Fallback по foglio
    if foglio:
        for key in _normalize_foglio_key(foglio):
            zona = FOGLIO_TO_ZONA.get(key)
            if zona is not None:
                return zona

        logger.error(
            "[CANONE] Невідомий foglio=%r (варіанти ключів=%r) — немає в FOGLIO_TO_ZONA",
            foglio,
            _normalize_foglio_key(foglio),
        )
        raise CanoneCalculationError(
            f"Неможливо визначити зону: foglio '{foglio}' не знайдено в FOGLIO_TO_ZONA"
        )

    # 3) Взагалі немає даних для визначення зони
    logger.error(
        "[CANONE] Не вказані ні micro_zona, ні foglio — немає як визначити зону"
    )
    raise CanoneCalculationError(
        "Не вказані ні micro_zona, ні foglio — неможливо визначити зону"
    )


def classify_tipologia(
    superficie_catastale: float,
    categoria_catasto: Optional[str],
) -> Tipologia:
    """
    Визначає Tipologia з Accordo (уніфаміліаре / діапазони площі).

    Спрощена, але практична логіка:
      - UNIFAMILIARE: якщо категорія A/7, A/8 або A/9 (villino / villa / castello ecc.);
      - інакше за площею:
          <= 50      → FINO_A_50
          51 – 70    → DA_51_A_70
          71 – 95    → DA_71_A_95
          96 – 110   → DA_96_A_110
          > 110      → OLTRE_111

    Якщо площа некоректна (<= 0) → CanoneCalculationError.
    """
    if superficie_catastale is None or superficie_catastale <= 0:
        logger.error(
            "[CANONE] Некоректна superficie_catastale=%r (має бути > 0)",
            superficie_catastale,
        )
        raise CanoneCalculationError(
            f"superficie_catastale має бути > 0, отримано {superficie_catastale!r}"
        )

    cat = _parse_cat(categoria_catasto)

    # Грубе правило для unifamiliare
    if cat in {"A/7", "A/8", "A/9"}:
        return Tipologia.UNIFAMILIARE

    s = superficie_catastale
    if s <= 50:
        return Tipologia.FINO_A_50
    if s <= 70:
        return Tipologia.DA_51_A_70
    if s <= 95:
        return Tipologia.DA_71_A_95
    if s <= 110:
        return Tipologia.DA_96_A_110
    return Tipologia.OLTRE_111


def classify_subfascia(inp: CanoneInput) -> int:
    """
    Визначає subfascia 1/2/3 за A/B/C + обмеженнями по категорії.

    Робоча інтерпретація п. 6 Accordo:

      1) SUBFASCIA 1, якщо:
         - категорія A/5, або
         - немає ВСІХ елементів A (у нас максимум 2 → count_a < 2), або
         - B < 3.

      2) SUBFASCIA 2, якщо:
         - є всі A (count_a == 2),
         - B >= 3,
         - але C < 3,
         АБО
         - формально тягне на subfascia 3, але категорія заборонена
           для subfascia 3 (A/3 classe 1, A/4, A/6).

      3) SUBFASCIA 3, якщо:
         - всі A,
         - B >= 3,
         - C >= 3,
         - і категорія НЕ A/3 classe 1, A/4, A/6.
    """
    cat = _parse_cat(inp.categoria_catasto)
    classe = (inp.classe_catasto or "").strip()

    a_cnt = max(0, inp.count_a)
    b_cnt = max(0, inp.count_b)
    c_cnt = max(0, inp.count_c)

    # A/5 завжди у найнижчій субфасції
    if cat == "A/5":
        return 1

    # Немає повного набору A → одразу subfascia 1
    if a_cnt < 2:
        return 1

    # Повні A, але мало B → теж 1
    if b_cnt < 3:
        return 1

    # Тут: A повні, B >= 3
    if c_cnt < 3:
        # не добираємо C — середня субфасція
        return 2

    # A повні, B >= 3, C >= 3 → кандидат на subfascia 3
    # але деякі категорії не можуть потрапити в верхню смугу
    if (cat == "A/3" and classe == "1") or cat in {"A/4", "A/6"}:
        return 2

    return 3


# ---------------------------------------------------------------------------
# Діапазон €/mq та базовий розрахунок
# ---------------------------------------------------------------------------

def _get_base_range_euro_mq(
    zona: int,
    tipologia: Tipologia,
    subfascia: int,
) -> tuple[float, float]:
    """
    Беремо (min, max) €/mq з BASE_RANGES.

    Якщо комбінація (zona, tipologia, subfascia) відсутня —
    кидаємо CanoneCalculationError.
    """
    try:
        zona_data = BASE_RANGES[zona]
    except KeyError as exc:
        logger.error(
            "[CANONE] Невідома zona=%r — немає в BASE_RANGES", zona
        )
        raise CanoneCalculationError(
            f"Невідома zona {zona} для BASE_RANGES"
        ) from exc

    try:
        tip_data = zona_data[tipologia]
    except KeyError as exc:
        logger.error(
            "[CANONE] Немає tipologia=%r для zona=%r в BASE_RANGES",
            tipologia,
            zona,
        )
        raise CanoneCalculationError(
            f"Немає tipologia {tipologia} для zona {zona} в BASE_RANGES"
        ) from exc

    try:
        return tip_data[subfascia]
    except KeyError as exc:
        logger.error(
            "[CANONE] Немає subfascia=%r для zona=%r tipologia=%r в BASE_RANGES",
            subfascia,
            zona,
            tipologia,
        )
        raise CanoneCalculationError(
            f"Немає subfascia {subfascia} для zona {zona}, tipologia {tipologia}"
        ) from exc


def compute_base_canone(inp: CanoneInput) -> CanoneResult:
    """
    Базовий розрахунок canone за Accordo Pescara 2018 БЕЗ надбавок
    (меблі, енергоклас, студенти, тривалість тощо ще НЕ враховані).

    Кроки:
      1) zona      ← micro_zona (fallback: foglio через FOGLIO_TO_ZONA);
      2) tipologia ← superficie_catastale + categoria_catasto;
      3) subfascia ← count A/B/C + обмеження A/3 cl.1, A/4, A/6;
      4) (min, max) €/mq з BASE_RANGES;
      5) base_euro_mq визначаємо по кількості D-елементів:
           - D = 0   → min
           - D >= 5  → max
           - 1..4    → min + (max - min) * D / 5  (quinti, як в Accordo);
      6) canone_base_annuo   = base_euro_mq * superficie_catastale;
         canone_base_mensile = canone_base_annuo / 12.

    На цьому етапі:
      - canone_finale == canone_base,
      - всі дельти надбавок = 0.
    """
    # 0) sanity-check площі
    if inp.superficie_catastale is None or inp.superficie_catastale <= 0:
        logger.error(
            "[CANONE] Некоректна superficie_catastale=%r (має бути > 0)",
            inp.superficie_catastale,
        )
        raise CanoneCalculationError(
            f"Некоректна superficie_catastale: {inp.superficie_catastale}"
        )

    # 1) Зона
    zona = classify_zona(inp.micro_zona, inp.foglio)

    # 2) Типологія
    tipologia = classify_tipologia(
        superficie_catastale=inp.superficie_catastale,
        categoria_catasto=inp.categoria_catasto,
    )

    # 3) Subfascia
    subfascia = classify_subfascia(inp)

    # 4) Діапазон €/mq
    min_eur_mq, max_eur_mq = _get_base_range_euro_mq(zona, tipologia, subfascia)

    # 5) D-елементи → позиція в межах [min, max]
    d_cnt = max(0, inp.count_d)
    delta = max_eur_mq - min_eur_mq

    if d_cnt == 0:
        base_euro_mq = min_eur_mq
    elif d_cnt >= 5:
        base_euro_mq = max_eur_mq
    else:
        # Пропорційний рух всередині діапазону згідно з кількістю D
        base_euro_mq = min_eur_mq + delta * (d_cnt / 5.0)

    # 6) Канони
    canone_base_annuo = base_euro_mq * inp.superficie_catastale
    canone_base_mensile = canone_base_annuo / 12.0

    result = CanoneResult(
        zona=zona,
        tipologia=tipologia,
        subfascia=subfascia,
        base_min_euro_mq=min_eur_mq,
        base_max_euro_mq=max_eur_mq,
        base_euro_mq=base_euro_mq,
        canone_base_annuo=canone_base_annuo,
        canone_base_mensile=canone_base_mensile,
        # Надбавки поки що не застосовуємо
        arredamento_delta_pct=0.0,
        energy_delta_pct=0.0,
        studenti_delta_pct=0.0,
        canone_finale_mq=base_euro_mq,
        canone_finale_annuo=canone_base_annuo,
        canone_finale_mensile=canone_base_mensile,
    )

    logger.debug(
        "[CANONE] Базовий canone: zona=%s, tipologia=%s, subfascia=%s, "
        "base_euro_mq=%.2f, base_annuo=%.2f, base_mese=%.2f",
        result.zona,
        result.tipologia,
        result.subfascia,
        result.base_euro_mq,
        result.canone_base_annuo,
        result.canone_base_mensile,
    )

    return result
