# uppi

Python pipeline для отримання visura PDF (Scrapy + Playwright), парсингу PDF (PyMuPDF + regex/OCR), збереження метаданих у PostgreSQL, PDF у MinIO та генерації DOCX “Attestazione di Rispondenza”.

## Швидкий старт

```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
playwright install chromium
```

### Конфігурація

Створи `.env` з основними змінними:

```bash
AE_LOGIN_URL=...
AE_URL_SERVIZI=...
SISTER_LOGOUT_URL=...
AE_USERNAME=...
AE_PASSWORD=...
AE_PIN=...
TWO_CAPTCHA_API_KEY=...

DB_HOST=localhost
DB_PORT=5432
DB_NAME=uppi_db
DB_USER=uppi_user
DB_PASSWORD=uppi_password
DB_SSL_MODE=prefer

MINIO_ENDPOINT=localhost:9000
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin
MINIO_SECURE=false
VISURE_BUCKET=uppi-bucket
ATTESTAZIONI_BUCKET=attestazioni

# Опційно: TTL для автоматичного оновлення visura (у днях)
VISURA_TTL_DAYS=
```

### База даних

Схема прикладу — `uppi/utils/db_utils/uppi_schema.sql`.

## Запуск пайплайна

```bash
scrapy crawl uppi
```

## Додавання клієнта в YAML

Файл: `clients/clients.yml`. Мінімальний запис:

```yaml
- LOCATORE_CF: "AAAAAA11A11A111A"
  FORCE_UPDATE_VISURA: false
  COMUNE: "PESCARA"
  TIPO_CATASTO: "F"
  UFFICIO_PROVINCIALE_LABEL: "PESCARA Territorio"
  IMMOBILE_VIA: "Via Roma"
```

## Кешування visura

Логіка рішення централізована в `uppi/services/visura_fetcher.py`:

- `FORCE_UPDATE_VISURA=true` → обовʼязкове скачування.
- Якщо запису в БД немає або обʼєкт відсутній у MinIO → скачування.
- Якщо заданий `VISURA_TTL_DAYS`, і `fetched_at` старший за TTL → повторне скачування.
- Інакше використовуємо кеш (БД + MinIO).

## Структура пакетів

```
uppi/config/      # env + YAML конфіг, валідація
uppi/domain/      # доменні моделі
uppi/services/    # бізнес-логіка: DB repo, MinIO, генерування
uppi/parsers/     # PDF + адресний парсер
uppi/pipelines/   # Scrapy glue
uppi/cli/         # CLI утиліти
```

## Тести

```bash
pytest
```
