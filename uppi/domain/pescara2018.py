"""
Базова логіка розрахунку canone за Accordo Pescara 2018.

ЦЕ ПЕРША ІТЕРАЦІЯ:
- Враховує лише таблицю BASE_RANGES (€/mq по zona/tipologia/subfascia).
- Не враховує ще:
    * A/B/C/D;
    * меблі;
    * енергоклас;
    * студентські надбавки;
    * ліміти варіацій.

Мета: підключити це в pipeline, порахувати для одного об’єкта і
порівняти з твоєю вручну порахованою attestazione.
"""

from __future__ import annotations

import logging

from uppi.domain.canone_models import CanoneInput, CanoneResult

from uppi.domain.pescara2018_calc import (
    compute_base_canone,
)


logger = logging.getLogger(__name__)



def calculate_canone(input_data: CanoneInput) -> CanoneResult:
    """
    Єдина публічна точка входу для розрахунку canone.

    Зараз просто делегує в compute_base_canone з pescara2018_calc.
    Якщо потім захочеш додати сюди логіку надбавок (меблі, енергоклас,
    студенти), можна буде:
      - порахувати базу через compute_base_canone;
      - зверху накрутити %;
      - повернути оновлений CanoneResult.
    """
    return compute_base_canone(input_data)
