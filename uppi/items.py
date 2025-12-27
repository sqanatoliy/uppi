# uppi/items.py
"""
Scrapy items for the Uppi project.

UppiItem — основний контейнер для даних одного "клієнт + візура".

Він використовується і для:
- клієнтів, по яких візура вже є в БД/MinIO (без походу в SISTER),
- клієнтів, для яких візуру качаємо з SISTER у поточному запуску.

Тут зібрані:
- ідентифікаційні дані клієнта (з clients.yml);
- службові прапорці (звідки взята візура, чи вдалася навігація/капча/завантаження);
- базові поля нерухомості / адреси;
- поля для заповнення DOCX-шаблону (A/B/C/D).
"""

import scrapy


class UppiItem(scrapy.Item):
    """
    Основний item для одного клієнта.

    Частина полів приходить з clients.yml (через map_yaml_to_item),
    частина — виставляється в павуку/пайплайні після роботи з SISTER та PDF.
    """

    # -------------------------------------------------------------------------
    # Ідентифікація клієнта / базові параметри запиту
    # -------------------------------------------------------------------------

    # Кодіче фіскале локатора (обов'язкове поле, ключ клієнта)
    locatore_cf = scrapy.Field()

    # Альтернативне імʼя, якщо десь ще використовується
    codice_fiscale = scrapy.Field()

    # Параметри запиту до SISTER з clients.yml
    # (мають дефолти у Client/clients.yml)
    comune = scrapy.Field()          # COMUNE / назва комуни
    tipo_catasto = scrapy.Field()    # TIPO_CATASTO (наприклад, "F")
    ufficio_label = scrapy.Field()   # UFFICIO_PROVINCIALE_LABEL → ufficio_label

    # Прапорець примусового оновлення візури
    force_update_visura = scrapy.Field()  # bool

    # -------------------------------------------------------------------------
    # Джерело та стан візури
    # -------------------------------------------------------------------------

    # Звідки береться візура:
    #   - 'db_cache' — дані вже є в БД/MinIO, SISTER не чіпаємо;
    #   - 'sister'   — візура отримана з SISTER у цьому запуску.
    visura_source = scrapy.Field()

    # Чи потрібно оновлювати візуру (логіка пайплайна / прапорець force_update)
    visura_needs_refresh = scrapy.Field()  # bool

    # Чи вдалося завантажити PDF-візуру в поточному запуску
    visura_downloaded = scrapy.Field()     # bool

    # Локальний шлях до завантаженого PDF (якщо visura_downloaded == True)
    visura_download_path = scrapy.Field()  # str | None

    # -------------------------------------------------------------------------
    # Діагностика автоматизації (навігація, капча)
    # -------------------------------------------------------------------------

    # Чи вдалося дійти до екрана "Visure catastali" і запустити "Visura per soggetto"
    nav_to_visure_catastali = scrapy.Field()  # bool

    # Чи успішно пройшла обробка CAPTCHA (якщо була)
    captcha_ok = scrapy.Field()              # bool

    # -------------------------------------------------------------------------
    # Дані орендодавця (locatore) — з YAML
    # -------------------------------------------------------------------------

    locatore_comune_res = scrapy.Field()
    locatore_via = scrapy.Field()
    locatore_civico = scrapy.Field()

    # -------------------------------------------------------------------------
    # Дані про нерухомість (immobile) — з YAML або як fallback до парсингу PDF
    # -------------------------------------------------------------------------

    immobile_comune = scrapy.Field()
    immobile_via = scrapy.Field()
    immobile_civico = scrapy.Field()
    immobile_piano = scrapy.Field()
    immobile_interno = scrapy.Field()

    # Кадастрові дані — можуть бути як з YAML, так і з парсингу PDF (через Immobile)
    foglio = scrapy.Field()
    numero = scrapy.Field()
    sub = scrapy.Field()
    rendita = scrapy.Field()

    # Загальна площа (в YAML, строкою; у БД/Immobile — NUMERIC/float)
    superficie_totale = scrapy.Field()

    categoria = scrapy.Field()

    # -------------------------------------------------------------------------
    # Дані договору
    # -------------------------------------------------------------------------

    contratto_data = scrapy.Field()

    # -------------------------------------------------------------------------
    # Дані орендаря (conduttore)
    # -------------------------------------------------------------------------

    conduttore_nome = scrapy.Field()
    conduttore_cf = scrapy.Field()
    conduttore_comune = scrapy.Field()
    conduttore_via = scrapy.Field()

    # -------------------------------------------------------------------------
    # Дані реєстрації договору
    # -------------------------------------------------------------------------

    decorrenza_data = scrapy.Field()
    registrazione_data = scrapy.Field()
    registrazione_num = scrapy.Field()
    agenzia_entrate_sede = scrapy.Field()

    # -------------------------------------------------------------------------
    # Поля для заповнення DOCX-шаблону: Тип договору, Мебльованість, Енергоклас, Фактичний canone з договору, Тривалість договору.
    # -------------------------------------------------------------------------
    contract_kind = scrapy.Field()
    arredato = scrapy.Field()
    energy_class = scrapy.Field()
    canone_contrattuale_mensile = scrapy.Field()
    durata_anni = scrapy.Field()

    # -------------------------------------------------------------------------
    # Поля для елементів A/B (табличні "галочки" в шаблоні)
    # -------------------------------------------------------------------------

    a1 = scrapy.Field()
    a2 = scrapy.Field()

    b1 = scrapy.Field()
    b2 = scrapy.Field()
    b3 = scrapy.Field()
    b4 = scrapy.Field()
    b5 = scrapy.Field()

    # -------------------------------------------------------------------------
    # Поля для елементів C
    # -------------------------------------------------------------------------

    c1 = scrapy.Field()
    c2 = scrapy.Field()
    c3 = scrapy.Field()
    c4 = scrapy.Field()
    c5 = scrapy.Field()
    c6 = scrapy.Field()
    c7 = scrapy.Field()

    # -------------------------------------------------------------------------
    # Поля для елементів D
    # -------------------------------------------------------------------------

    d1 = scrapy.Field()
    d2 = scrapy.Field()
    d3 = scrapy.Field()
    d4 = scrapy.Field()
    d5 = scrapy.Field()
    d6 = scrapy.Field()
    d7 = scrapy.Field()
    d8 = scrapy.Field()
    d9 = scrapy.Field()
    d10 = scrapy.Field()
    d11 = scrapy.Field()
    d12 = scrapy.Field()
    d13 = scrapy.Field()

    # -------------------------------------------------------------------------
    # Запасне поле, якщо з YAML прилітає щось нове
    # -------------------------------------------------------------------------
    extra = scrapy.Field()