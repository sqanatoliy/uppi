# uppi

Python pipeline for Scrapy + Playwright visure download, PDF parsing, PostgreSQL persistence, MinIO storage, and DOCX attestazione generation.

## Quick start

```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
playwright install chromium
```

Create `.env` with DB/MinIO/AE settings (see `ReadME.md` for the full list). Optional cache control:

```
VISURA_TTL_DAYS=30
```

Run the pipeline:

```bash
scrapy crawl uppi
```

## Add a client in YAML

Edit `clients/clients.yml` with entries like:

```yaml
- LOCATORE_CF: "ABCDEF12G34H567I"
  COMUNE: "PESCARA"
  FORCE_UPDATE_VISURA: false
  IMMOBILE_VIA: "Via Roma"
  A1: "X"
```

`LOCATORE_CF` is mandatory. All other fields are optional and validated via `uppi.config.clients.ClientConfig`.

## Visura caching behavior

The decision to fetch from SISTER is centralized in `uppi.services.visura_policy`:

- `FORCE_UPDATE_VISURA=true` always fetches.
- If the visura DB record or MinIO object is missing, it fetches.
- If `VISURA_TTL_DAYS` is set and the last fetch is older than the TTL, it fetches.
- Otherwise it reuses cached visura data.

## Tests

```bash
pytest
```

## Key modules

- `uppi/config/` — env/YAML config schema
- `uppi/domain/` — domain models
- `uppi/services/` — DB/MinIO/visura services and pipeline orchestration
- `uppi/parsers/` — PDF + address parsing
- `uppi/pipelines.py` — Scrapy glue
