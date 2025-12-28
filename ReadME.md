# Uppi Scraper & Attestazione Pipeline!

> Док для trainee data engineer, який має самостійно підняти проєкт, зрозуміти потік даних і не зламати БД 🙂

---

## Зміст

1. [Швидкий старт](#швидкий-старт)
2. [Що робить проєкт](#що-робить-проєкт)
3. [Архітектура загалом](#архітектура-загалом)
4. [Розгортання: крок за кроком](#розгортання-крок-за-кроком)  
   4.1. Клонування та Python  
   4.2. Playwright  
   4.3. PostgreSQL: схема БД  
   4.4. MinIO / S3  
   4.5. `.env` конфіг  
5. [Файлова структура та ключові модулі](#файлова-структура-та-ключові-модулі)  
6. [Модель даних: таблиці та `Immobile`](#модель-даних-таблиці-та-immobile)  
7. [Вхідні дані: `clients/clients.yml`](#вхідні-дані-clientsclientsyml)  
8. [Життєвий цикл одного запуску](#життєвий-цикл-одного-запуску)  
   8.1. Робота павука (`UppiSpider`)  
   8.2. Робота pipeline (`UppiPipeline`)  
   8.3. Порядок шарів: хто кого викликає  
9. [Збереження та перезапис даних у БД](#збереження-та-перезапис-даних-у-бд)  
10. [Формування файлу attestazione (DOCX)](#формування-файлу-attestazione-docx)  
11. [Типові сценарії використання](#типові-сценарії-використання)  
12. [Як розширювати систему](#як-розширювати-систему)
13. [CLI утиліта огляду клієнтів](#cli-утиліта-огляду-клієнтів)
14. [Типові проблеми та поради](#типові-проблеми-та-поради)
15. [Accordo Territoriale Pescara 2018 і розрахунок canone](#accordo-territoriale-pescara-2018-і-розрахунок-canone)

---

## Швидкий старт

1. Python 3.11 + локальні PostgreSQL та MinIO з bucket `visure` (ключі з `.env`).
2. `python -m venv venv && source venv/bin/activate` (Windows: `venv\\Scripts\\activate`) → `pip install -r requirements.txt`.
3. `playwright install chromium`.
4. Створи `.env` за зразком із розділу про конфіг (AE/SISTER, TWO_CAPTCHA, Postgres, MinIO).
5. Заповни `clients/clients.yml` хоча б записом з `LOCATORE_CF` (додаткові поля — за потребою).
6. Запусти `scrapy crawl uppi`:  
   - якщо `FORCE_UPDATE_VISURA=false` і в БД вже є візура — будемо працювати з кешем;  
   - інакше піде в SISTER, скачає PDF і оновить БД/MinIO.
7. DOCX-аттестації з’являться у `downloads/<CF>/ATTESTAZIONE_<...>.docx`. PDF-візура після завантаження у MinIO локально видаляється (залишається у `visure/<cf>.pdf` у bucket).
8. Для швидкої перевірки без SISTER використовуй `python -m uppi.cli.inspect_clients --last` або `--cf <CF>`.

---

## Що робить проєкт

Система робить повний цикл для італійської нерухомості:

1. Читає список **контрактів** з `clients/clients.yml`.
2. Для кожного **LOCATORE_CF**:
   - Якщо візура вже є в БД — не йде в SISTER, працює з кешем.
   - Якщо нема або потрібно оновити — за допомогою **Scrapy + Playwright**:
     - логіниться в AE,  
     - заходить у SISTER,  
     - робить **visura catastale**,  
     - качає **PDF**.
3. Парсить PDF в **структури нерухомості** (`Immobile`), зберігає:
   - PDF → **MinIO**  
   - метадані візури → таблиця `visure`  
   - нерухомість (усі об’єкти з візури) → таблиця `immobili`.
4. На основі:
   - даних з **візури** (катастро),
   - даних з **YAML** (реальна адреса, орендар, договір),
   - даних з **БД** (накопичені override-и),

   генерує **DOCX attestazione** для кожного відібраного об’єкта нерухомості.

---

## Архітектура загалом

Умовно є кілька шарів:

- **Вхідні дані**: `clients.yml`
- **Scrapy Spider**: `uppi/spiders/uppi_spider.py` — керує роботою з AE/SISTER.
- **Playwright**: логін, навігація, CAPTCHA, скачування PDF.
- **Parser PDF**: `VisuraParser` → список dict для нерухомості.
- **Domain layer**:
  - `Immobile` — модель одного об’єкта нерухомості.
  - `uppi/domain/db.py` — конекшн і прості хелпери до БД.
  - `uppi/domain/storage.py` — шляхи зберігання файлів.
- **Persistence**:
  - PostgreSQL — таблиці `visure` + `immobili`.
  - MinIO — зберігання PDF.
- **Scrapy Pipeline**: `uppi/pipelines.py` — glue-код між усіма шарами.
- **Документогенерація**:
  - `uppi/docs/attestazione_template_filler.py` — заповнення DOCX-шаблону.

---

## Розгортання: крок за кроком

### 4.1. Клонування та Python

```bash
git clone <url-на-репозиторій> uppi
cd uppi

python -m venv venv
source venv/bin/activate  # Windows: venv\\Scripts\\activate

pip install -r requirements.txt
```

Проєкт орієнтований на **Python 3.11**.

### 4.2. Playwright

```bash
playwright install chromium
```

Scrapy-Playwright повинен мати хоча б Chromium.

### 4.3. PostgreSQL: схема БД

Створити БД та користувача (імена/пароль можна змінити, але мають відповідати `.env`):

```sql
CREATE DATABASE uppi_db;
CREATE USER uppi_user WITH PASSWORD 'uppi_password';
GRANT ALL PRIVILEGES ON DATABASE uppi_db TO uppi_user;
```

Всередині `uppi_db` створити таблиці:

```sql
-- Таблиця з PDF-візурами
CREATE TABLE visure (
    cf          TEXT PRIMARY KEY,
    pdf_bucket  TEXT NOT NULL,
    pdf_object  TEXT NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Таблиця нерухомості
CREATE TABLE immobili (
    id      BIGSERIAL PRIMARY KEY,
    visura_cf TEXT NOT NULL REFERENCES visure(cf) ON DELETE CASCADE,

    -- Табличні дані з візури
    table_num_immobile   TEXT,
    sez_urbana           TEXT,
    foglio               TEXT,
    numero               TEXT,
    sub                  TEXT,
    zona_cens            TEXT,
    micro_zona           TEXT,
    categoria            TEXT,
    classe               TEXT,
    consistenza          TEXT,
    superficie_totale    NUMERIC,
    superficie_escluse   NUMERIC,
    superficie_raw       TEXT,
    rendita              TEXT,

    -- Адреса об'єкта з візури (зберігається "як є", але не використовується для attestazione)
    immobile_comune      TEXT,
    immobile_comune_code TEXT,
    via_type             TEXT,
    via_name             TEXT,
    via_num              TEXT,
    scala                TEXT,
    interno              TEXT,
    piano                TEXT,
    indirizzo_raw        TEXT,
    dati_ulteriori       TEXT,

    -- Дані локатора з візури
    locatore_surname         TEXT,
    locatore_name            TEXT,
    locatore_codice_fiscale  TEXT,

    -- OVERRIDE: реальна адреса об'єкта (з YAML, збережена в БД)
    immobile_comune_override   TEXT,
    immobile_via_override      TEXT,
    immobile_civico_override   TEXT,
    immobile_piano_override    TEXT,
    immobile_interno_override  TEXT,

    -- Адреса орендодавця (з YAML, збережена в БД)
    locatore_comune_res    TEXT,
    locatore_via           TEXT,
    locatore_civico        TEXT
);

CREATE INDEX idx_immobili_visura_cf ON immobili(visura_cf);
CREATE INDEX idx_immobili_foglio_numero_sub ON immobili(visura_cf, foglio, numero, sub);
```

> **Важливо:** цей DDL має узгоджуватись з полями dataclass `Immobile` та SQL у `load_immobiles_from_db()` / `save_visura()`.

### 4.4. MinIO / S3

Для dev-середовища за замовчуванням очікується локальний MinIO:

- endpoint: `localhost:9000`
- access_key: `minioadmin`
- secret_key: `minioadmin`
- bucket: `visure`

Якщо використовуєш Docker — підніми MinIO з цими параметрами, або зміни їх у `.env`.

### 4.5. `.env` конфіг

Створи `.env` у корені проєкту. Мінімальний набір:

```env
# AE / SISTER
AE_LOGIN_URL=https://iampe.agenziaentrate.gov.it/sam/UI/Login?realm=/agenziaentrate
AE_URL_SERVIZI=https://portale.agenziaentrate.gov.it/PortaleWeb/servizi
SISTER_LOGOUT_URL=https://sister.agenziaentrate.gov.it/Servizi/LogoutServlet
AE_USERNAME=...
AE_PASSWORD=...
AE_PIN=...

# TwoCaptcha
TWO_CAPTCHA_API_KEY=...

# PostgreSQL
DB_HOST=localhost
DB_PORT=5432
DB_NAME=uppi_db
DB_USER=uppi_user
DB_PASSWORD=uppi_password

# MinIO
MINIO_ENDPOINT=localhost:9000
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin
MINIO_SECURE=False
MINIO_BUCKET=visure
```

---

## Файлова структура та ключові модулі

Спрощено:

- `uppi/spiders/uppi_spider.py`  
  Головний Scrapy-павук, керує AE/SISTER.

- `uppi/pipelines.py`  
  Scrapy pipeline, клеїть разом YAML → PDF → БД → DOCX.

- `uppi/domain/immobile.py`  
  Dataclass `Immobile` — опис одного об’єкта нерухомості.

- `uppi/domain/db.py`  
  Підключення до PostgreSQL (`_get_pg_connection`, `db_has_visura`).

- `uppi/domain/storage.py`  
  Хелпери для шляхів: де лежать PDF, де зберігати DOCX attestazione.

- `uppi/utils/item_mapper.py`  
  Мапить сирий YAML (`LOCATORE_CF`, `IMMOBILE_VIA`, A1, B2, ...) в нормалізований dict для `UppiItem`.

- `uppi/docs/visura_pdf_parser.py`  
  `VisuraParser` — парсер PDF-візури → список dict.

- `uppi/docs/attestazione_template_filler.py`  
  Логіка заміни `{{KEY}}` в DOCX шаблоні, підтримка підкреслень.

- `clients/clients.yml`  
  Вхідний список контрактів (по ньому запускається вся логіка).

---

## Модель даних: таблиці та `Immobile`

### Таблиця `visure`

Один запис на один CF:

- `cf` — кодіче фіскале орендодавця (локатора).
- `pdf_bucket` / `pdf_object` — де лежить PDF у MinIO.
- `updated_at` — коли востаннє оновлювали.

### Таблиця `immobili`

Кілька записів на один `visura_cf` (якщо у локатора кілька об’єктів).

Грубо поділяється на три блоки:

1. **Сирі дані з візури**  
   `foglio`, `numero`, `sub`, `categoria`, `superficie_totale`, `rendita`, адреса з візури (`immobile_comune`, `via_name`, …), `locatore_surname`, `locatore_name`, `locatore_codice_fiscale`.

2. **Override реальної адреси об’єкта** (з YAML, збережені в БД):  
   `immobile_comune_override`, `immobile_via_override`, `immobile_civico_override`, `immobile_piano_override`, `immobile_interno_override`.

3. **Адреса орендодавця** (також з YAML, збережена в БД):  
   `locatore_comune_res`, `locatore_via`, `locatore_civico`.

### Dataclass `Immobile`

`uppi/domain/immobile.py` — **один в один** відповідає колонкам `immobili` (без `id` та `visura_cf`):

```python
@dataclass
class Immobile:
    # з візури (таблиця нерухомості)
    table_num_immobile: str | None = None
    sez_urbana: str | None = None
    foglio: str | None = None
    numero: str | None = None
    sub: str | None = None
    zona_cens: str | None = None
    micro_zona: str | None = None
    categoria: str | None = None
    classe: str | None = None
    consistenza: str | None = None
    superficie_totale: Optional[float] = None
    superficie_escluse: Optional[float] = None
    superficie_raw: str | None = None
    rendita: str | None = None

    # адреса з візури
    immobile_comune: str | None = None
    immobile_comune_code: str | None = None
    via_type: str | None = None
    via_name: str | None = None
    via_num: str | None = None
    scala: str | None = None
    interno: str | None = None
    piano: str | None = None
    indirizzo_raw: str | None = None
    dati_ulteriori: str | None = None

    # орендодавець з візури
    locatore_surname: str | None = None
    locatore_name: str | None = None
    locatore_codice_fiscale: str | None = None

    # override-адреса об’єкта
    immobile_comune_override: str | None = None
    immobile_via_override: str | None = None
    immobile_civico_override: str | None = None
    immobile_piano_override: str | None = None
    immobile_interno_override: str | None = None

    # адреса орендодавця (override)
    locatore_comune_res: str | None = None
    locatore_via: str | None = None
    locatore_civico: str | None = None
```

---

## Вхідні дані: `clients/clients.yml`

Файл — список записів, кожен описує **один контракт** / **одну комбінацію CF + опціональні уточнення**.

Приклад:

```yaml
- LOCATORE_CF: CCMMRT71S44H501X
  FORCE_UPDATE_VISURA: false

  # Параметри пошуку в SISTER
  COMUNE: PESCARA
  TIPO_CATASTO: F
  UFFICIO_PROVINCIALE_LABEL: PESCARA Territorio

  # Адреса орендодавця
  LOCATORE_COMUNE_RES: Pescara
  LOCATORE_VIA: Predazzo
  LOCATORE_CIVICO: 43

  # Реальна адреса об’єкта
  IMMOBILE_COMUNE: Montesilvano
  IMMOBILE_VIA: C-so Umberto I
  IMMOBILE_CIVICO: 316

  # Ідентифікатор об’єкта в катастро (щоб прив’язати override до конкретного immobile)
  IMMOBILE_PIANO: 4
  IMMOBILE_INTERNO: 55
  FOGLIO: 11
  NUMERO: 138
  SUB:
  RENDITA:
  SUPERFICIE_TOTALE:
  CATEGORIA:

  # Договір
  CONTRATTO_DATA: 15/10/2025

  # Орендар
  CONDUTTORE_NOME: Bianocchi Giovana
  CONDUTTORE_CF: BCCGNN44M45G488W
  CONDUTTORE_COMUNE: Pescara
  CONDUTTORE_VIA: Verdi

  # Реєстрація
  DECORRENZA_DATA: 19/10/2025
  REGISTRAZIONE_DATA: 20/10/2025
  REGISTRAZIONE_NUM: 12345
  AGENZIA_ENTRATE_SEDE: Pescara

  # Флаги A/B/C/D
  A1: X
  A2: X
  B1: X
  B2: X
  B3: X
  B4: X
  B5: X
  ...
```

Швидкі нотатки про `clients.yml`:

- мінімально достатньо `LOCATORE_CF` (інші поля — за потребою, `FORCE_UPDATE_VISURA` за замовчуванням false);
- файл читається послідовно згори вниз, можна мати кілька записів з одним CF (наприклад, щоб проставити різні override-и);
- шлях до файлу можна перевизначити змінною `UPPI_CLIENTS_YAML` (корисно для тестових списків або під час запуску CLI-утиліти);
- щоб override застосувався до конкретного об’єкта, задавай `FOGLIO`/`NUMERO`/`SUB`; якщо їх нема і в CF кілька об’єктів — захистом нічого не буде оновлено.

### Як YAML мапиться у внутрішній item

`uppi/utils/item_mapper.py`:

- приводить ключі до UPPERCASE,
- для відомих ключів використовує мапу `YAML_TO_ITEM_MAP`,
- додає дефолти:
  - `COMUNE = PESCARA`
  - `TIPO_CATASTO = F`
  - `UFFICIO_PROVINCIALE_LABEL = PESCARA Territorio`
- невідомі ключі потрапляють у `item["extra"]`.

У результаті формується dict, з якого будується `UppiItem`.

---

## Життєвий цикл одного запуску

### 8.1. Робота павука (`UppiSpider`)

Запуск:

```bash
scrapy crawl uppi
```

1. **`start()`**:
   - видаляє `state.json` (Playwright сесія) та папку `captcha_images` (старі капчі),
   - читає `clients.yml` через `load_clients()`,
   - для кожного клієнта:
     - дістає `LOCATORE_CF`,
     - викликає `db_has_visura(cf)`:
       - якщо **візура вже є в БД** і `FORCE_UPDATE_VISURA = false` → **не йде в SISTER**, а одразу:
         - будує `mapped = map_yaml_to_item(client)`,
         - ставить `visura_source = "db_cache"` + кілька службових прапорів,
         - `yield UppiItem(**mapped)` → дані йдуть у pipeline.
       - інакше (візури нема або FORCE_UPDATE_VISURA=true) → додає клієнта в `self.clients_to_fetch`.

   - якщо `clients_to_fetch` порожній → SISTER не потрібен, павук закривається.

   - якщо є кого качати → робить `scrapy.Request` на `AE_LOGIN_URL` з Playwright-метою, callback `login_and_fetch_visura`.

2. **`login_and_fetch_visura()`**:
   - отримує Playwright `page`,
   - запускає `authenticate_user(...)` — логін в AE,
   - через `open_sister_service(...)` відкриває SISTER у новій вкладці `sister_page`,
   - далі цикл по `self.clients_to_fetch`:
     - формує `mapped = map_yaml_to_item(client)` + додає `visura_source = "sister"`,
     - викликає `navigate_to_visure_catastali(...)` з параметрами CF + COMUNE + TIPO_CATASTO + UFFICIO_LABEL,
     - якщо навігація ок — викликає `solve_captcha_if_present(...)` (TwoCaptcha),
     - якщо капча ок — викликає `download_document(...)` → отримує шлях до PDF,
     - ставить відповідні прапорці (`nav_to_visure_catastali`, `captcha_ok`, `visura_downloaded`, `visura_download_path`),
     - `yield UppiItem(**mapped)` → далі pipeline.

   - в `finally` завжди пробує зробити logout через `_logout_in_context()`.

### 8.2. Робота pipeline (`UppiPipeline`)

Кожний `UppiItem` проходить через `UppiPipeline.process_item()`.

1. Витягуються:

   - `cf` — з `locatore_cf` / `codice_fiscale`,
   - `visura_source` — `"sister"`, `"db_cache"` або `None`,
   - прапорці: `visura_downloaded`, `visura_download_path`,
   - `force_update` — з item + параметра павука.

2. Гілки:

#### 8.2.1. `visura_source = "sister"`

Кейс: свіжескачаний PDF.

- Якщо PDF відсутній → лог + fallback на БД (якщо `db_has_visura` і `force_update=False`).
- Якщо PDF є:
  - `VisuraParser().parse(pdf_path)` → список dict,
  - `raw_immobiles = [Immobile(**d) for d in imm_dicts]`,
  - `save_visura(cf, raw_immobiles, pdf_path)`:
    - завантажує PDF у MinIO,
    - `INSERT ... ON CONFLICT` у `visure`,
    - `DELETE FROM immobili WHERE visura_cf = cf`,
    - `INSERT INTO immobili (...) VALUES (...)` для кожного `Immobile`,
    - видаляє локальний `pdf_path`.
  - `upsert_overrides_from_yaml(cf, adapter)`:
    - оновлює override-адреси + адресу локатора в `immobili` згідно YAML.
  - `immobiles = load_immobiles_from_db(cf)`:
    - читає canonical-дані з БД у список `Immobile`.

#### 8.2.2. `visura_source = "db_cache"`

Кейс: візура вже є в БД, PDF не качається.

- `upsert_overrides_from_yaml(cf, adapter)` — якщо в поточному запуску є якісь override-и в YAML.
- `immobiles = load_immobiles_from_db(cf)`.

#### 8.2.3. `visura_source is None` (backward-compat)

Кейс: старий формат item’ів.

- Якщо `force_update` або `db_has_visura(cf) == False`:
  - пробує знайти локальний PDF (`get_visura_path(cf)`),
  - парсить його `VisuraParser`,
  - `save_visura(cf, raw_immobiles, pdf_path)`,
  - `upsert_overrides_from_yaml(cf, adapter)`,
  - `immobiles = load_immobiles_from_db(cf)`.
- Інакше (візура вже є):
  - `upsert_overrides_from_yaml(cf, adapter)`,
  - `immobiles = load_immobiles_from_db(cf)`.

#### 8.2.4. Фільтрація та генерація DOCX

Після отримання `immobiles`:

1. `selected_immobiles = filter_immobiles(immobiles, adapter)` — фільтрує по опційних критеріях із YAML (`foglio`, `numero`, `sub`, `categoria`, `rendita`, `superficie_totale`).

2. Для кожного `imm` у `selected_immobiles`:

   - `params = build_params(adapter, imm)` — будує словник `{{KEY}} → value` для шаблону.
   - `output_path = get_attestazione_path(cf, imm)` — формує унікальну назву DOCX (CF + ключові катастро-параметри) і пише в `downloads/<CF>/ATTESTAZIONE_<CF>_<slug>.docx` (каталог створюється автоматично).
   - `fill_attestazione_template(template_path, output_folder, output_path.name, params, underscored)`:
     - заповнює плейсхолдери в DOCX і зберігає файл.

### 8.3. Порядок шарів: коли що викликається

Для одного клієнта логіка виглядає так:

1. `clients.yml` → `map_yaml_to_item()` → `UppiItem`.
2. `UppiSpider.start()` → вирішує:  
   - або **одразу yield item** з `visura_source="db_cache"`,  
   - або спочатку **SISTER** → `visura_source="sister"`.
3. `UppiSpider.login_and_fetch_visura()` (якщо потрібно) → **скачує PDF**.
4. `UppiPipeline.process_item()`:
   - якщо треба — **парсить PDF** (`VisuraParser`),
   - **записує в БД** (`save_visura`),
   - **оновлює override-и** (`upsert_overrides_from_yaml`),
   - **читає canonical `immobiles` з БД** (`load_immobiles_from_db`),
   - **фільтрує** (`filter_immobiles`),
   - **генерує DOCX** (`fill_attestazione_template`).

---

## Збереження та перезапис даних у БД

### Первинний запис (перше звернення з повним YAML)

1. Spider качає PDF → pipeline парсить та викликає `save_visura(cf, raw_immobiles, pdf_path)`:
   - сирі дані з візури (катастро + адреса + locatore_*) йдуть у `immobili`.
   - override-колонки (`immobile_*_override`, `locatore_*`) поки що `NULL`.

2. `upsert_overrides_from_yaml(cf, adapter)`:
   - читає `FOGLIO`, `NUMERO`, `SUB` з YAML:
     - якщо вказано — оновлює **тільки відповідний рядок** в `immobili`,
     - якщо НЕ вказано:
       - якщо у CF **один** immobile → оновлює його,
       - якщо **кілька** — нічого не оновлює (захист від того, щоб однією адресою не залити всі об’єкти).
   - у SET частині:
     - `immobile_*_override` записуються значення з YAML (можуть бути `NULL`),
     - `locatore_comune_res`, `locatore_via`, `locatore_civico` оновлюються через `COALESCE(yaml, old)` — тобто:
       - якщо YAML дав нове значення → воно перезапише старе,
       - якщо в YAML поле пусте → в БД лишається старе значення.

### Повторний запуск ТІЛЬКИ з CF

- Spider бачить, що візура є в БД → `visura_source = "db_cache"`.
- Pipeline:
  - `upsert_overrides_from_yaml(cf, adapter)` — якщо в YAML нічого нема окрім CF, по суті нічого не оновлює.
  - `immobiles = load_immobiles_from_db(cf)`.
  - `build_params()` бере:
    - **катастро** — з полів `imm.*` (foglio, numero, sub, categoria, superficie_totale, rendita),
    - **LOCATORE_NOME** — з `imm.locatore_surname + imm.locatore_name`,
    - **LOCATORE_CF** — з YAML (якщо є) або `imm.locatore_codice_fiscale`,
    - **реальна адреса об’єкта** — з override-колонок `imm.immobile_*_override`,
    - **адреса локатора**:
      - якщо в цьому запуску є поля `LOCATORE_*` у YAML → беруться вони,
      - інакше → береться те, що вже лежить у `imm.locatore_*`,
      - якщо й там пусто → в DOCX буде лише підкреслення.

### Оновлення даних (перезапис)

- **Оновити реальну адресу об’єкта**:
  - у `clients.yml` створити запис з тим же `LOCATORE_CF`,
  - вказати `FOGLIO` / `NUMERO` / `SUB` відповідного об’єкта,
  - задати нові `IMMOBILE_COMUNE`, `IMMOBILE_VIA`, `IMMOBILE_CIVICO`, `IMMOBILE_PIANO`, `IMMOBILE_INTERNO`.
  - запустити `scrapy crawl uppi`:
    - `upsert_overrides_from_yaml` знайде правильний рядок в `immobili` і оновить override-колонки.

- **Оновити адресу орендодавця**:
  - в YAML вказати `LOCATORE_COMUNE_RES`, `LOCATORE_VIA`, `LOCATORE_CIVICO`,
  - при наступному запуску ці значення потраплять в `immobili.locatore_*` і далі в DOCX.

- **Примусово оновити візуру (якщо змінюється катастро-частина)**:
  - поставити `FORCE_UPDATE_VISURA: true` для потрібного CF,
  - при запуску:
    - Spider піде в SISTER, скачає новий PDF,
    - `save_visura` перезапише всі `immobili` для цього CF з нових даних,
    - `upsert_overrides_from_yaml` після цього знову накладе YAML-override-и.

---

## Формування файлу attestazione (DOCX)

Головна функція — `fill_attestazione_template()`.

### 1. Параметри, які в неї заходять

`build_params(adapter, imm)` формує dict, де ключі — це **точні плейсхолдери** з шаблону:

```python
params["{{LOCATORE_NOME}}"]
params["{{LOCATORE_CF}}"]
params["{{LOCATORE_COMUNE_RES}}"]
params["{{LOCATORE_VIA}}"]
params["{{LOCATORE_CIVICO}}"]
params["{{IMMOBILE_COMUNE}}"]
params["{{IMMOBILE_VIA}}"]
params["{{IMMOBILE_CIVICO}}"]
params["{{IMMOBILE_PIANO}}"]
params["{{IMMOBILE_INTERNO}}"]

params["{{FOGLIO}}"]
params["{{NUMERO}}"]
params["{{SUB}}"]
params["{{RENDITA}}"]
params["{{SUPERFICIE_TOTALE}}"]
params["{{CATEGORIA}}"]

params["{{CONTRATTO_DATA}}"]

params["{{CONDUTTORE_NOME}}"]
params["{{CONDUTTORE_CF}}"]
params["{{CONDUTTORE_COMUNE}}"]
params["{{CONDUTTORE_VIA}}"]

params["{{DECORRENZA_DATA}}"]
params["{{REGISTRAZIONE_DATA}}"]
params["{{REGISTRAZIONE_NUM}}"]
params["{{AGENZIA_ENTRATE_SEDE}}"]

# для чекбоксів:
params["{{a1}}"], params["{{A1}}"], ...
```

### 2. Підкреслені поля (`underscored`)

У `attestazione_template_filler.py` є мапа:

```python
underscored = {
    "{{LOCATORE_NOME}}": 40,
    "{{LOCATORE_CF}}": 25,
    ...
}
```

Це означає: **скільки `_` було в шаблоні** для цього поля.

Функція `fill_underscored(text, length)`:

- якщо `text` порожній → повертає `"_" * length`,
- якщо є текст → вставляє текст + добиває пробілами, щоб візуально поле займало ту ж ширину.

Спецкейс: `{{CONDUTTORE_CF}}` — там спеціальна логіка, щоб максимально не ламати верстку.

### 3. Як усе це склеюється

- `fill_attestazione_template()`:
  - копіює `template_attestazione_pescara.docx` у вихідний файл,
  - проходить усі параграфи і таблиці,
  - у кожному `run` шукає `{{KEY}}` і замінює їх:
    - якщо ключ є в `underscored` → використовується `fill_underscored`,
    - якщо ключ не в `underscored`, але є в `params` → проста підстановка,
    - якщо ключ ніде не знайдений → видаляється або замінюється на підкреслення (залежить від того, чи він у `underscored`).

---

## Типові сценарії використання

### 1. Перше звернення з повним YAML

- Запис у `clients.yml` містить:
  - `LOCATORE_CF` + параметри пошуку SISTER,
  - адресу локатора,
  - реальну адресу об’єкта + FOGLIO/NUMERO/SUB,
  - дані договору й орендаря.

Результат:

- SISTER → PDF,
- PDF → `visure` + `immobili`,
- YAML → override-колонки в `immobili` (для потрібного об’єкта),
- DOCX → повністю заповнена attestazione для цього об’єкта.

### 2. Повторне звернення тільки з `LOCATORE_CF`

У `clients.yml`:

```yaml
- LOCATORE_CF: CCMMRT71S44H501X
  FORCE_UPDATE_VISURA: false
```

Результат:

- Spider **не йде в SISTER** (візура вже існує),
- Pipeline:
  - читає `immobili` із БД,
  - для кожного об’єкта, який проходить фільтр (можна не задавати фільтри → всі):
    - бере:
      - катастро (foglio/numero/sub/...) з БД,
      - реальну адресу об’єкта — з override-колонок, якщо вони вже колись були задані,
      - адресу локатора — або з YAML (якщо цього разу задана), або з БД (якщо колись задавали),
    - генерує DOCX.

### 3. Змінилася адреса локатора

У `clients.yml`:

```yaml
- LOCATORE_CF: CCMMRT71S44H501X
  LOCATORE_COMUNE_RES: NuovoComune
  LOCATORE_VIA: NuovaVia
  LOCATORE_CIVICO: 99
```

Результат:

- `upsert_overrides_from_yaml` оновлює `locatore_*` у `immobili` (для конкретного immobile або для єдиного об’єкта),
- усі нові attestazioni будуть з новою адресою.

### 4. Перейменування вулиці об’єкта

У записі для цього CF + FOGLIO/NUMERO/SUB:

```yaml
- LOCATORE_CF: CCMMRT71S44H501X
  FOGLIO: 11
  NUMERO: 138
  SUB:
  IMMOBILE_VIA: NuovaVia
```

Результат:

- override-колонки для конкретного об’єкта оновлюються,
- наступний запуск з одним CF — для цього об’єкта в DOCX буде нова вулиця.

---

## Як розширювати систему

Ці місця найбільш логічні для доповнень:

- **Нові поля з візури**:
  - додати їх у `Immobile`,
  - додати в схему `immobili`,
  - оновити `save_visura()` / `load_immobiles_from_db()`.

- **Нова бізнес-логіка** (наприклад, обрахунки за Accordo Pescara 2018):
  - додати модуль, який приймає `Immobile` + параметри з YAML,
  - повертати значення для таблиці в DOCX,
  - додати нові `{{KEY}}` в `build_params()` + `underscored` (якщо потрібні підкреслення),
  - вставити ці поля в шаблон DOCX.

- **Зміна формату `clients.yml`**:
  - оновити `YAML_TO_ITEM_MAP` у `item_mapper.py`,
  - скоригувати `DEFAULTS` (якщо нові дефолти),
  - при потребі скоригувати `upsert_overrides_from_yaml()` (якщо ідентифікувати об’єкт будемо ще за чимось, окрім FOGLIO/NUMERO/SUB).

---

## CLI утиліта огляду клієнтів

Ця утиліта дає швидкий огляд того, що вже є в системі для конкретного **codice fiscale** локатора.
Вона поєднує дані з `clients.yml` та з БД (`visure` + `immobili`) і показує їх в одному читабельному виводі.

### Призначення

- перевірити, **чи є візура по CF в базі**, без запиту до SISTER;
- подивитися, **скільки об'єктів нерухомості** прив'язано до CF;
- порівняти **реальну адресу** (override з YAML) та **адресу з візури** для кожного об'єкта;
- отримати готові YAML-селектори (`IMMOBILE_COMUNE`, `FOGLIO`, `NUMERO`, `SUB`) для вибору одного об'єкта.

Типовий сценарій: клієнт тільки прийшов, ти вписав його `LOCATORE_CF` у `clients.yml`, запускаєш утиліту і за її виводом вирішуєш,
чи потрібно дописувати ще якісь дані в YAML, чи можна одразу запускати спайдера / формувати attestazione.

### Розташування та залежності

Файл утиліти:

```text
uppi/cli/inspect_clients.py
```

Вона використовує:

- `DB_USER, DB_PASSWORD` — для підключення до PostgreSQL, наприклад:
  ```env
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
  ```
- `UPPI_CLIENTS_YAML` — шлях до `clients/clients.yml` (опційно, за замовчуванням `clients/clients.yml`).

Утиліта **нічого не змінює в БД**, працює тільки на читання.

### Запуск

Запускається як модуль Python з активованим venv у корені проєкту:

```bash
python -m uppi.cli.inspect_clients              # прогнати по всіх записах у clients.yml
python -m uppi.cli.inspect_clients --last       # подивитися тільки останній запис
python -m uppi.cli.inspect_clients --cf CCMMRT71S44H501X  # подивитися конкретний CF
```

Параметри:

- без параметрів — проходить по всім записам `clients.yml`;
- `--last` — бере **останній** запис з файлу (типово: щойно доданий клієнт);
- `--cf <CODICEFISCALE>` — фільтрує записи по `LOCATORE_CF`.

### Що показує утиліта

Для кожного знайденого запису в `clients.yml`:

1. **Блок YAML** — що вже заведено по цьому клієнту в `clients.yml` (CF, ім’я/прізвище, дані орендаря, флаги, FORCE_UPDATE_VISURA тощо).
2. **Блок даних з БД**:
   - чи є запис у `visure` для цього CF;
   - bucket/ключ PDF у MinIO (`pdf_bucket`, `pdf_object`);
   - `updated_at` — коли останній раз оновлювали візуру;
   - кількість об'єктів у таблиці `immobili` для цього CF.
3. **По кожному об'єкту** (`immobili`):
   - катастро-поля: `foglio`, `numero`, `sub`, `categoria`, `superficie_totale`, `rendita`;
   - **реальна адреса** (з override-полів `immobile_*_override` + `locatore_*`, якщо вони були задані з YAML);
   - **адреса в Visura** (сирі дані з візури: `via_type`, `via_name`, `via_num`, `piano`, `interno`, `indirizzo_raw`);
   - готовий YAML-селектор, який можна скопіювати в `clients.yml`, якщо хочеш сформувати attestazione саме по цьому об'єкту:
     ```text
     IMMOBILE_COMUNE:  <comune>
     IMMOBILE_FOGLIO:  <foglio>
     IMMOBILE_NUMERO:  <numero>
     IMMOBILE_SUB:     <sub>
     ```

Наприкінці для кожного клієнта утиліта дає коротку інтерпретацію:
- якщо візури в БД немає — прямо каже, що потрібно запускати спайдера або працювати тільки з даними контракту;
- якщо є один об'єкт — радить, що можна спокійно робити attestazione "по CF";
- якщо об'єктів кілька — підказує, що або робити attestazione по всіх, або звузити через YAML-селектори.

### Робочий флоу з утилітою

1. Додаєш у `clients.yml` запис з мінімумом:
   ```yaml
   - LOCATORE_CF: CCMMRT71S44H501X
     FORCE_UPDATE_VISURA: false
   ```
2. Запускаєш:
   ```bash
   python -m uppi.cli.inspect_clients --last
   ```
3. Дивишся на вивід і вирішуєш:
   - якщо візури немає — спочатку запускаєш спайдера (`scrapy crawl uppi`), щоб її отримати;
   - якщо візура вже є — по кількості об'єктів і адресах вирішуєш, чи треба дописувати селектори/override-и в YAML,
     чи можна одразу формувати attestazione по всіх об'єктах для цього CF.

---

## Типові проблеми та поради

- **Playwright не знаходить браузер**: помилка на старті → виконай `playwright install chromium` у venv.
- **Отримався кеш замість свіжої візури**: якщо у SISTER вже є зміни — в YAML постав `FORCE_UPDATE_VISURA: true`, щоб змусити spider перекачати PDF.
- **Схема БД не збігається**: помилки `column does not exist` у `immobili`/`visure` → звір DDL з розділу про БД та dataclass `Immobile`, накати ALTER/CREATE.
- **MinIO/S3 відмовляє**: якщо `S3Error` при заливці PDF, перевір `MINIO_*`, bucket `visure` і флаг `MINIO_SECURE` (False для локального MinIO).
- **CAPTCHA не вирішується**: перевір `TWO_CAPTCHA_API_KEY` і баланс; на час відладки можна запускатися з `FORCE_UPDATE_VISURA=false`, щоб брати дані з БД.
- **Не бачу артефактів**: DOCX лежать у `downloads/<CF>/`, локальний PDF видаляється після заливки в bucket `visure/<cf>.pdf`; для залипань Playwright можна почистити `state.json` / `captcha_images` і перезапустити.

---

## Accordo Territoriale Pescara 2018 і розрахунок canone

Цей розділ описує, як у проєкті реалізовано розрахунок орендної плати за **Accordo Territoriale Pescara 2018**, як працюють елементи **A/B/C/D**, нові поля контракту та як усе це проходить через `pipelines.py` до DOCX-шаблону.

### 15.1. База тарифів Pescara 2018 (`pescara2018_data.py`)

Модуль:

```text
uppi/domain/pescara2018_data.py
```

містить "статичну" таблицю діапазонів орендної плати по зонам, типологіям і підзонам:

```python
# Схема:
# BASE_RANGES[zona][tipologia][subfascia] = (min_eur_mq, max_eur_mq)

from uppi.domain.canone_models import Tipologia

BASE_RANGES = {
    1: {
        Tipologia.UNIFAMILIARE: {
            1: (48.90, 62.87),
            2: (62.87, 69.86),
            3: (69.86, 76.84),
        },
        Tipologia.OLTRE_111: {
            1: (48.90, 55.89),
            2: (55.89, 62.87),
            3: (62.87, 69.86),
        },
        ...
    },
    2: { ... },
    3: { ... },
    4: { ... },
}
```

Дві додаткові мапи дозволяють визначити **зону** не тільки за microzona, але і за foglio:

```python
MICROZONA_TO_ZONA: dict[str, int] = {
    "1": 1,
    "2": 1,
    "4": 1,
    "5": 2,
    "7": 2,
    ...
}

FOGLIO_TO_ZONA: dict[str, int] = {
    "2": 1,
    "3": 1,
    "4": 1,
    "5": 1,
    "8": 1,
    ...
    "24": 4,
    "30": 4,
    "31": 4,
    ...
}
```

Якщо `micro_zona` вказана коректно — вона має пріоритет. Якщо ні, зона визначається за `FOGLIO`. Суперечливі foglio (що можуть належати до різних зон) у файлі просто закоментовані й розбираються вручну.

### 15.2. Моделі для розрахунку canone (`canone_models.py`)

В модулі:

```text
uppi/domain/canone_models.py
```

оголошені основні моделі та енум:

- `Tipologia` — категорія за площею (як в Accordo Pescara):

  ```python
  class Tipologia(Enum):
      UNIFAMILIARE = "UNIFAMILIARE"
      OLTRE_111 = "OLTRE_111"
      DA_96_A_110 = "DA_96_A_110"
      DA_71_A_95 = "DA_71_A_95"
      DA_51_A_70 = "DA_51_A_70"
      FINO_A_50 = "FINO_A_50"
  ```

- `CanoneInput` — дані, на основі яких рахується орендна плата:

  - катастро: `foglio`, `numero`, `sub`, `superficie_catastale`, `superficie_riparametrata`;
  - зона: `zona` (з `MICROZONA_TO_ZONA` або `FOGLIO_TO_ZONA`);
  - типологія: `tipologia` (`Tipologia`);
  - контракт: `contract_kind`, `arredato`, `energy_class`, `canone_contrattuale_mensile`, `durata_anni`;
  - елементи A/B/C/D: або окремі флаги, або вже пораховані `a_cnt`, `b_cnt`, `c_cnt`, `d_cnt`.

- `CanoneResult` — результат розрахунку:

  - `zona`, `tipologia`,
  - `superficie_catastale`, `superficie_riparametrata`,
  - `canone_min_mq`, `canone_max_mq` — діапазон €/m² згідно Accordo,
  - `canone_min_mese`, `canone_max_mese` — помножено на площу,
  - `canone_base_mese` — обрана точка всередині діапазону,
  - `canone_contrattuale_mensile` — фактична сума з YAML/БД,
  - `canone_ammissibile` — чи входить договірний canone у дозволений коридор,
  - додаткові службові поля, які виводяться в шаблон через плейсхолдери `CAN_*`, `GAR_*`, `PST_*`.

Реальна реалізація може мати трохи інші назви полів, але загальна ідея саме така: **весь складний розрахунок захований у окремий доменний модуль**, а pipeline лише збирає вхідні дані і виводить результат у DOCX.

### 15.3. Оновлена схема таблиці `immobili`

Після додавання елементів A/B/C/D і базових полів контракту схема `immobili` розширена.

Оновлена версія (ключові поля зверху пропущено для стислості, див. повний DDL вище; нижче — лише додатки):

```sql
-- Елементи A/B/C/D (як прапорці / текстові позначки)
a1 TEXT,
a2 TEXT,

b1 TEXT,
b2 TEXT,
b3 TEXT,
b4 TEXT,
b5 TEXT,

c1 TEXT,
c2 TEXT,
c3 TEXT,
c4 TEXT,
c5 TEXT,
c6 TEXT,
c7 TEXT,

d1  TEXT,
d2  TEXT,
d3  TEXT,
d4  TEXT,
d5  TEXT,
d6  TEXT,
d7  TEXT,
d8  TEXT,
d9  TEXT,
d10 TEXT,
d11 TEXT,
d12 TEXT,
d13 TEXT,

-- Автоматично пораховані кількості елементів
a_cnt INTEGER,
b_cnt INTEGER,
c_cnt INTEGER,
d_cnt INTEGER,

-- Параметри контракту/енергоефективності/фактичного canone
contract_kind TEXT,                  -- CONCORDATO | TRANSITORIO | STUDENTI
arredato BOOLEAN,                    -- true/false
energy_class TEXT,                   -- A..G
canone_contrattuale_mensile NUMERIC, -- фактичний canone місяць
durata_anni INTEGER                  -- тривалість договору
```

> Уся ця інформація зберігається **на рівні конкретного immobile** (рядка в `immobili`).

### 15.4. Нові поля у `clients/clients.yml`

Типовий запис у `clients/clients.yml` тепер може містити не лише A/B/C/D, а й параметри контракту:

```yaml
- LOCATORE_CF: CCMMRT71S44H501X
  FORCE_UPDATE_VISURA: false

  # ... інші поля (SISTER, адреси, катастро) ...

  # Contract specifics
  CONTRACT_KIND: CONCORDATO      # CONCORDATO, TRANSITORIO, STUDENTI

  # Furnishing status
  ARREDATO: true                 # true/false

  # Energy class
  ENERGY_CLASS: B                # A, B, C, D, E, F, G

  # Monthly rent (actual)
  CANONE_CONTRATTUALE_MENSILE: 750.0

  # Duration in years
  DURATA_ANNI: 3

  # Elements A/B/C/D
  A1: X
  A2: X
  B1:
  B2:
  B3: X
  B4: X
  B5: X

  C1:
  C2:
  C3: X
  C4:
  C5: X
  C6: X
  C7: X

  D1: X
  D2:
  D3:
  D4: X
  D5:
  D6:
  D7: X
  D8: X
  D9: X
  D10:
  D11: X
  D12: X
  D13: X
```

Правила мапінгу YAML → БД:

- якщо ключ **взагалі відсутній** → поле в БД не чіпається;
- якщо ключ є, але значення **порожній рядок** → вважаємо "немає оновлення", БД не чіпається;
- для A/B/C/D:
  - значення `"-"` → **очищуємо** відповідну колонку (ставимо `NULL`);
  - будь-яке інше непорожнє значення (`"X"`, текст тощо) → пишемо як є в колонку;
  - після оновлення запускається перерахунок `A_CNT`, `B_CNT`, `C_CNT`, `D_CNT` у SQL.
- для контрактних полів (`CONTRACT_KIND`, `ARREDATO`, `ENERGY_CLASS`, `CANONE_CONTRATTUALE_MENSILE`, `DURATA_ANNI`) — значення з YAML мають пріоритет над тим, що вже лежить у БД.

### 15.5. `upsert_elements_from_yaml()` і `upsert_contract_from_yaml()`

У `uppi/pipelines.py` з’явились допоміжні функції для оновлення даних у `immobili` перед розрахунком canone.

#### 15.5.1. `upsert_elements_from_yaml(cf, adapter)`

Відповідає за елементи A/B/C/D:

1. Збирає всі можливі ключі (`a1..a2`, `b1..b5`, `c1..c7`, `d1..d13`) із item’а (YAML → `ItemAdapter`).
2. Для кожного:
   - відсутній / порожній → пропускає;
   - `"-"` → очищує колонку (`SET col = NULL`);
   - інше значення → `SET col = <value>`.
3. Визначає, який рядок(и) оновлювати:
   - завжди фільтрує по `visura_cf = cf`;
   - додатково враховує `FOGLIO`, `NUMERO`, `SUB`, якщо вони задані в YAML;
   - якщо для CF кілька `immobili`, а `FOGLIO`/`NUMERO`/`SUB` **не задані** — нічого не оновлює (захист від масового перезапису).
4. Після оновлення перераховує `A_CNT..D_CNT` SQL-ом для всіх рядків з відповідним `visura_cf`.

Приклад логів (спрощено):

```text
[DB] upsert_elements_from_yaml: для CCMMRT71S44H501X оновлено A/B/C/D (1 рядків)
[DB] upsert_elements_from_yaml: для CCMMRT71S44H501X перераховано A_CNT..D_CNT (3 рядків)
```

#### 15.5.2. `upsert_contract_from_yaml(cf, adapter)`

Оновлює контрактні поля у `immobili`:

- шукає всі рядки по `visura_cf = cf`;
- якщо в YAML задано `FOGLIO`/`NUMERO`/`SUB` — оновлює **конкретний** об’єкт;
- якщо для CF один `immobile` — можна оновлювати без уточнення;
- використовує ті ж правила "YAML має пріоритет, але порожній рядок не чіпає БД", що й `upsert_overrides_from_yaml`;
- зберігає: `contract_kind`, `arredato`, `energy_class`, `canone_contrattuale_mensile`, `durata_anni`.

У логах видно щось на кшталт:

```text
[DB] upsert_contract_from_yaml: для CCMMRT71S44H501X оновлено контрактні поля (1 рядків)
```

### 15.6. Розрахунок canone в `UppiPipeline.process_item()`

Після того як `immobili` завантажено з БД і відфільтровано (`filter_immobiles`), pipeline робить для кожного об’єкта:

1. Оновлює/підтягує в БД:
   - override-адреси (`upsert_overrides_from_yaml`),
   - елементи A/B/C/D (`upsert_elements_from_yaml`),
   - дані контракту (`upsert_contract_from_yaml`).
2. Ще раз читає canonical `immobili` (якщо потрібно) — щоб мати вже "остаточні" дані.
3. По кожному `imm` будує `CanoneInput`:
   - площа → `imm.superficie_totale` (та/або `SUPERFICIE_TOTALE_OVERRIDE`, якщо реалізовано);
   - зона → з `imm.micro_zona` або `FOGLIO_TO_ZONA`;
   - типологія → за площею (`Tipologia.from_superficie(...)` або аналогічна функція);
   - контракт → з полів `contract_kind`, `arredato`, `energy_class`, `canone_contrattuale_mensile`, `durata_anni` (БД вже враховує YAML);
   - елементи → `imm.a_cnt`, `imm.b_cnt`, `imm.c_cnt`, `imm.d_cnt`.
4. Викликає `compute_base_canone(canone_input)` (у модулі типу `canone_pescara2018.py`):
   - якщо повертається `CanoneResult` → використовує його в `build_params`;
   - якщо не вдається порахувати (нема зони, площі, tipologia) → лог `[CANONE]` + шаблон генерується, але всі `CAN_*` / `GAR_*` / `PST_*` лишаються пустими.

У логах це виглядає приблизно так:

```text
[CANONE] Canone per CF=..., imm=1: canone_contrattuale 750.0 в межах діапазону [X, Y]
```

або, якщо щось не так:

```text
[CANONE] Неочікувана помилка при розрахунку canone для CF=..., imm=1: <details>
```

### 15.7. Нові плейсхолдери в DOCX-шаблоні

Окрім вже наявних плейсхолдерів для адрес/катастро/дат, `build_params` тепер заповнює спеціальні поля для таблиць Accordo/Calcolo Canone.

Приклади (фактичний список дивись у коді `build_params`):

- **Підсумок елементів A/B/C/D** (таблиця *CALCOLO NUMERO ELEMENTI*):

  ```python
  params["{{A_CNT}}"] = str(imm.a_cnt)
  params["{{B_CNT}}"] = str(imm.b_cnt)
  params["{{C_CNT}}"] = str(imm.c_cnt)
  params["{{D_CNT}}"] = str(imm.d_cnt)
  ```

- **Скорочений блок CAN*** (паспорт розрахунку canone):

  ```python
  params["{{CAN_SUP_CAT}}"]   = _to_str(canone_result.superficie_catastale)
  params["{{CAN_SUP_RIP}}"]   = _to_str(canone_result.superficie_riparametrata)
  params["{{CAN_ZONE_MIN}}"]  = _to_str(canone_result.canone_min_mq)
  params["{{CAN_ZONE_MAX}}"]  = _to_str(canone_result.canone_max_mq)
  params["{{CAN_CANONE_MIN}}"]   = _to_str(canone_result.canone_min_mese)
  params["{{CAN_CANONE_MAX}}"]   = _to_str(canone_result.canone_max_mese)
  params["{{CAN_CANONE_BASE}}"]  = _to_str(canone_result.canone_base_mese)
  params["{{CAN_CANONE_CONTR}}"] = _to_str(canone_result.canone_contrattuale_mensile)
  params["{{CAN_DURATA}}"]       = _to_str(canone_input.durata_anni)
  params["{{CAN_TIPO_CONTR}}"]   = _to_str(canone_input.contract_kind)
  params["{{CAN_CLASSE_EN}}"]    = _to_str(canone_input.energy_class)
  ```

- **Проміжні таблиці GAR_/PST_** (якщо реалізовано в шаблоні):

  ```python
  params["{{GAR_ABC_MQ}}"]      = _to_str(canone_result.gar_abc_mq)
  params["{{GAR_ABC_EUR_MQ}}"]  = _to_str(canone_result.gar_abc_eur_mq)
  params["{{GAR_D_MQ}}"]        = _to_str(canone_result.gar_d_mq)
  params["{{GAR_D_EUR_MQ}}"]    = _to_str(canone_result.gar_d_eur_mq)
  params["{{GAR_TOT_MQ}}"]      = _to_str(canone_result.gar_tot_mq)
  params["{{GAR_TOT_EUR_MQ}}"]  = _to_str(canone_result.gar_tot_eur_mq)

  params["{{PST_SUP_CAT}}"]         = _to_str(canone_result.superficie_catastale)
  params["{{PST_SUP_RIP}}"]         = _to_str(canone_result.superficie_riparametrata)
  params["{{PST_CANONE_MIN}}"]      = _to_str(canone_result.canone_min_mese)
  params["{{PST_CANONE_MAX}}"]      = _to_str(canone_result.canone_max_mese)
  params["{{PST_CANONE_PATTUITO}}"] = _to_str(canone_result.canone_contrattuale_mensile)
  ```

Головний принцип: **всі складні числа рахує доменний модуль**, pipeline лише ретельно переносить їх у шаблон.

### 15.8. Підсумок робочого флоу з урахуванням canone

Для одного immobile тепер повний цикл виглядає так:

1. **YAML** → `item_mapper` → `UppiItem`.
2. **Spider**:
   - або качає PDF з SISTER (`visura_source="sister"`),
   - або використовує вже наявну візуру в БД (`visura_source="db_cache"`).
3. **Pipeline**:
   - за потреби парсить PDF (`VisuraParser`) і зберігає дані в `visure` + `immobili` (`save_visura`);
   - оновлює override-адреси (`upsert_overrides_from_yaml`);
   - оновлює елементи A/B/C/D (`upsert_elements_from_yaml` → `a_cnt..d_cnt`);
   - оновлює контрактні поля (`upsert_contract_from_yaml`);
   - завантажує canonical `immobili` з БД (`load_immobiles_from_db`);
   - фільтрує потрібний об’єкт (`filter_immobiles`);
   - формує `CanoneInput` та викликає `compute_base_canone` → отримує `CanoneResult`;
   - будує `params` для DOCX (`build_params`), включаючи всі `CAN_*`, `GAR_*`, `PST_*` поля;
   - викликає `fill_attestazione_template` і зберігає `ATTESTAZIONE_*.docx`.

Таким чином, **вся бізнес-логіка Accordo Pescara 2018** тепер прозоро проходить через БД і pipeline, а шаблон DOCX відображає як вихідні дані (візура, YAML), так і фінальний розрахунок canone.
