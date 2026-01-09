BEGIN;

-- =========================================================
-- 0. EXTENSIONS & TYPES
-- =========================================================
CREATE EXTENSION IF NOT EXISTS pgcrypto;

DO $$ BEGIN
    CREATE TYPE contract_type AS ENUM ('CONCORDATO', 'TRANSITORIO', 'STUDENTI');
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;

CREATE OR REPLACE FUNCTION set_updated_at() RETURNS trigger AS $$
BEGIN
  NEW.updated_at = now();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- =========================================================
-- 1. ADDRESSES (Master Data: Addresses)
-- =========================================================
-- Зберігаємо унікальні адреси. Логіка Python має нормалізувати рядок 
-- перед вставкою, щоб уникнути дублікатів через пробіли/регістр.
CREATE TABLE IF NOT EXISTS public.addresses (
  id              BIGSERIAL PRIMARY KEY,
  comune          TEXT NOT NULL,
  via_full        TEXT NOT NULL, -- Напр. "Via Roma", "Piazza Italia"
  civico          TEXT NOT NULL DEFAULT 'SNC',
  piano           TEXT,
  interno         TEXT,
  scala           TEXT,
  
  -- Хеш для швидкого пошуку існуючої адреси (Case Insensitive + Ignore Whitespace)
  content_hash    TEXT GENERATED ALWAYS AS (
      md5(upper(trim(comune)) || '|' || 
          upper(trim(regexp_replace(via_full, '\s+', ' ', 'g'))) || '|' || 
          upper(trim(civico)))
  ) STORED,

  created_at      TIMESTAMPTZ DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_addresses_hash ON public.addresses(content_hash);

-- =========================================================
-- 2. PERSONS (Master Data: Owners & Tenants)
-- =========================================================
CREATE TABLE IF NOT EXISTS public.persons (
  cf                  TEXT PRIMARY KEY CHECK (cf ~ '^[A-Z0-9]{11,16}$'),
  surname             TEXT,
  name                TEXT,
  residence_address_id BIGINT REFERENCES public.addresses(id) ON DELETE SET NULL,
  
  created_at          TIMESTAMPTZ DEFAULT now(),
  updated_at          TIMESTAMPTZ DEFAULT now()
);

CREATE TRIGGER trg_persons_upd BEFORE UPDATE ON public.persons FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- =========================================================
-- 3. VISURE (Metadata)
-- =========================================================
CREATE TABLE IF NOT EXISTS public.visure (
  id              BIGSERIAL PRIMARY KEY,
  locatore_cf     TEXT NOT NULL REFERENCES public.persons(cf) ON DELETE CASCADE,
  pdf_bucket      TEXT NOT NULL,
  pdf_object      TEXT NOT NULL,
  checksum_sha256 TEXT,
  fetched_at      TIMESTAMPTZ DEFAULT now(),
  updated_at      TIMESTAMPTZ DEFAULT now(),
  
  -- Один CF = одна актуальна візура. Старі перезаписуються або архівуються в S3.
  UNIQUE(locatore_cf)
);

-- =========================================================
-- 4. IMMOBILI (Master Data: Property Units)
-- =========================================================
CREATE TABLE IF NOT EXISTS public.immobili (
  id                    BIGSERIAL PRIMARY KEY,
  owner_cf              TEXT NOT NULL REFERENCES public.persons(cf) ON DELETE CASCADE,
  
  -- Зв'язки
  source_visura_id      BIGINT REFERENCES public.visure(id) ON DELETE SET NULL,
  visura_address_id     BIGINT REFERENCES public.addresses(id) ON DELETE SET NULL, -- Адреса з парсингу PDF
  real_address_id       BIGINT REFERENCES public.addresses(id) ON DELETE SET NULL, -- Адреса з Override (YAML)

  -- Кадастрові ідентифікатори (Composite Key)
  sez_urbana            TEXT DEFAULT '',
  foglio                TEXT NOT NULL,
  numero                TEXT NOT NULL,
  sub                   TEXT NOT NULL DEFAULT '', 
  
  -- Кадастрові параметри (КРИТИЧНО для розрахунку)
  zona_cens             TEXT, -- Додано!
  micro_zona            TEXT, -- Додано! Важливо для Pescara Agreement
  categoria             TEXT, -- A/2, A/3...
  classe                TEXT,
  consistenza           TEXT, -- vani або mq
  rendita               TEXT,
  
  -- Площі
  superficie_totale     DOUBLE PRECISION,
  superficie_escluse    DOUBLE PRECISION,
  superficie_raw        TEXT, -- Сирий рядок з PDF для дебагу

  -- Технічні параметри (можуть бути NULL, якщо не задані в YAML)
  energy_class          TEXT, 

  created_at            TIMESTAMPTZ DEFAULT now(),
  updated_at            TIMESTAMPTZ DEFAULT now(),
  
  UNIQUE (owner_cf, foglio, numero, sub)
);

CREATE INDEX IF NOT EXISTS idx_immobili_owner_cf ON public.immobili(owner_cf);
CREATE TRIGGER trg_immobili_upd BEFORE UPDATE ON public.immobili FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- =========================================================
-- 5. IMMOBILE_ELEMENTS (Details: A/B/C/D)
-- =========================================================
CREATE TABLE IF NOT EXISTS public.immobile_elements (
  immobile_id   BIGINT NOT NULL REFERENCES public.immobili(id) ON DELETE CASCADE,
  grp           CHAR(1) NOT NULL CHECK (grp IN ('A','B','C','D')),
  code          TEXT NOT NULL, -- 'A1', 'B2', 'D10'...
  value         TEXT,          -- Зазвичай 'X' або якесь значення, якщо потрібно
  
  PRIMARY KEY (immobile_id, grp, code)
);

-- =========================================================
-- 6. CONTRACTS (Business Logic Snapshot)
-- =========================================================
-- Ця таблиця акумулює дані з YAML про умови оренди.
-- Вона є "джерелом істини" для генерації Attestazione.
CREATE TABLE IF NOT EXISTS public.contracts (
  id                          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  immobile_id                 BIGINT NOT NULL REFERENCES public.immobili(id) ON DELETE CASCADE,
  conduttore_cf               TEXT REFERENCES public.persons(cf) ON DELETE SET NULL,
  
  -- Параметри договору
  contract_kind               contract_type NOT NULL,
  start_date                  DATE, -- Може бути NULL, якщо ще не визначено (шаблон)
  durata_anni                 INTEGER CHECK (durata_anni > 0 AND durata_anni < 100),
  
  -- Дані реєстрації (ДОДАНО, щоб зберігати state з YAML)
  decorrenza_data             DATE,
  registrazione_data          DATE,
  registrazione_num           TEXT,
  agenzia_entrate_sede        TEXT,

  -- Фінанси
  canone_contrattuale_mensile NUMERIC(12, 2) CHECK (canone_contrattuale_mensile >= 0),
  istat_rate                  NUMERIC(5, 4) DEFAULT 0.0,
  arredato_pct                NUMERIC(5, 4) DEFAULT 0.0,
  
  -- Додаткові налаштування
  ignore_surcharges           BOOLEAN NOT NULL DEFAULT FALSE,
  custom_props                JSONB DEFAULT '{}'::jsonb, -- Для полів, що не увійшли в схему (extra)

  created_at                  TIMESTAMPTZ DEFAULT now(),
  updated_at                  TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_contracts_immobile_id ON public.contracts(immobile_id);
CREATE TRIGGER trg_contracts_upd BEFORE UPDATE ON public.contracts FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- =========================================================
-- 7. ATTESTAZIONI (Immutable Logs)
-- =========================================================
CREATE TABLE IF NOT EXISTS public.attestazioni (
  id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  contract_id         UUID NOT NULL REFERENCES public.contracts(id) ON DELETE CASCADE,
  generated_at        TIMESTAMPTZ DEFAULT now(),
  
  output_bucket       TEXT NOT NULL,
  output_object       TEXT NOT NULL,
  
  -- Повний знімок даних, що пішли в шаблон (JSON).
  -- Це дозволяє відтворити документ навіть якщо дані в contracts/immobili змінилися.
  full_data_snapshot  JSONB NOT NULL,
  
  author_hash         TEXT, -- хеш юзера, що запустив процес
  status              TEXT NOT NULL DEFAULT 'generated'
);

CREATE INDEX IF NOT EXISTS idx_attestazioni_contract_id ON public.attestazioni(contract_id);

-- =========================================================
-- 8. CANONE_CALCOLI (Audit Trail)
-- =========================================================
CREATE TABLE IF NOT EXISTS public.canone_calcoli (
  id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  contract_id    UUID NOT NULL REFERENCES public.contracts(id) ON DELETE CASCADE,
  
  inputs         JSONB, -- Що зайшло в калькулятор (площа, зона, елементи)
  min_val        NUMERIC(12, 2), -- Базова вилка
  max_val        NUMERIC(12, 2),
  result_mensile NUMERIC(12, 2), -- Фінальна цифра
  
  calculated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  
  UNIQUE(contract_id, calculated_at)
);

CREATE INDEX IF NOT EXISTS idx_canone_calcoli_contract_id ON public.canone_calcoli(contract_id);

COMMIT;