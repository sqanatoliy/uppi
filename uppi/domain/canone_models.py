# uppi/domain/canone_models.py

from dataclasses import dataclass
from enum import Enum
from typing import Optional


class Tipologia(Enum):
    """
    Типологія житла з Accordo (табл. 1).

    Обережно: "abitazione unifamiliare" логічно відрізняється
    від "квартири" з певною площею, але на практиці
    для більшості випадків ти будеш використовувати
    квартирні типи за площею.
    """
    UNIFAMILIARE = "unifamiliare"
    OLTRE_111 = ">111"
    DA_96_A_110 = "96-110"
    DA_71_A_95 = "71-95"
    DA_51_A_70 = "51-70"
    FINO_A_50 = "<=50"


class ContractKind(Enum):
    """
    Тип договору для застосування надбавок:
    - CONCORDATO: стандарт 3+2 (art. 2, c.3)
    - TRANSITORIO: тимчасовий
    - STUDENTI: для студентів
    """
    CONCORDATO = "concordato"
    TRANSITORIO = "transitorio"
    STUDENTI = "studenti"


@dataclass
class CanoneInput:
    """
    Вхідні дані для розрахунку канону за Accordo Pescara 2018.

    Це те, що ми збираємо з:
    - Immobile (visura + БД),
    - лічильників A/B/C/D,
    - YAML (меблі / енергоклас / тип договору).
    """
    superficie_catastale: float                # mq (imm.superficie_totale)
    micro_zona: Optional[str]                 #ід з visura (1..10)
    foglio: Optional[str]                     # запасний варіант для зони
    categoria_catasto: Optional[str]          # наприклад "A/2"
    classe_catasto: Optional[str]             # наприклад "3"

    count_a: int                              # скільки A1/A2 ввімкнено
    count_b: int                              # B1..B5
    count_c: int                              # C1..C7
    count_d: int                              # D1..D13

    arredato: bool = False                    # чи мебльована
    energy_class: Optional[str] = None        # "A","B","C","D","E","F","G"
    contract_kind: ContractKind = ContractKind.CONCORDATO
    durata_anni: int = 3                      # для майбутніх надбавок за тривалість


@dataclass
class CanoneResult:
    """
    Вихід розрахунку канону.

    Ми зберігаємо і проміжні значення, щоб було що
    показати в CALCOLO DEL CANONE, і фінальний canone.
    """
    zona: int                                  # 1..4
    tipologia: Tipologia                       # тип за площею / уніфам.
    subfascia: int                             # 1..3

    base_min_euro_mq: float                    # з табл. 1
    base_max_euro_mq: float                    # з табл. 1
    base_euro_mq: float                        # після D-елементів

    canone_base_annuo: float                   # base_euro_mq * superficie
    canone_base_mensile: float                 # / 12

    arredamento_delta_pct: float               # +15, 0, тощо
    energy_delta_pct: float                    # +8, +4, -2*n ...
    studenti_delta_pct: float                  # до +20

    canone_finale_mq: float                    # €/mq після всіх надбавок
    canone_finale_annuo: float                 # фінальний рік
    canone_finale_mensile: float               # фінальний місяць
