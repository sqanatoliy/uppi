-- Після створення таблиць необхідно перевірити чи всі взаємозвязки працюють коректно.
-- Для цього виконати вставки тестових даних у відповідному порядку з файлв test_inserts.sql

BEGIN;

-- =========================================================
-- Extensions
-- =========================================================
CREATE EXTENSION IF NOT EXISTS pgcrypto; -- gen_random_uuid(), digest()

-- =========================================================
-- updated_at trigger helper
-- =========================================================
CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS trigger AS $$
BEGIN
  NEW.updated_at = now();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- =========================================================
-- 1) PERSONS (мінімальний довідник осіб: locatore/conduttore)
-- =========================================================
CREATE TABLE IF NOT EXISTS public.persons (
  cf         TEXT PRIMARY KEY,
  surname    TEXT,
  name       TEXT,

  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'trg_persons_updated_at') THEN
    CREATE TRIGGER trg_persons_updated_at
    BEFORE UPDATE ON public.persons
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();
  END IF;
END$$;

-- =========================================================
-- 2) VISURE (PDF у MinIO + метадані)
--    1 person (locatore) -> 1 visura per CF (актуальна)
-- =========================================================
CREATE TABLE IF NOT EXISTS public.visure (
  cf              TEXT PRIMARY KEY REFERENCES public.persons(cf) ON DELETE CASCADE,
  pdf_bucket      TEXT NOT NULL,
  pdf_object      TEXT NOT NULL,

  checksum_sha256 TEXT,          -- checksum PDF (опційно)
  fetched_at      TIMESTAMPTZ,   -- коли реально стягнули PDF

  created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_visure_fetched_at ON public.visure(fetched_at);

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'trg_visure_updated_at') THEN
    CREATE TRIGGER trg_visure_updated_at
    BEFORE UPDATE ON public.visure
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();
  END IF;
END$$;

-- =========================================================
-- 3) IMMOBILI (тільки parsed fields з visura)
--    1 visura -> N immobili
-- =========================================================
CREATE TABLE IF NOT EXISTS public.immobili (
  id                    BIGSERIAL PRIMARY KEY,
  visura_cf             TEXT NOT NULL REFERENCES public.visure(cf) ON DELETE CASCADE,

  table_num_immobile    TEXT,

  -- Dati catastali
  sez_urbana            TEXT,
  foglio                TEXT,
  numero                TEXT,

  -- IMPORTANT:
  -- sub використовуємо як частину канонічного ключа. Нормалізуємо: NULL -> ''.
  -- На "свіжій" БД створимо одразу NOT NULL DEFAULT '', але для вже існуючої
  -- таблиці нижче є DO-блок з ALTER/UPDATE.
  sub                   TEXT NOT NULL DEFAULT '',

  zona_cens             TEXT,
  micro_zona            TEXT,
  categoria             TEXT,
  classe                TEXT,
  consistenza           TEXT,
  rendita               TEXT,

  -- Superficie
  superficie_totale     DOUBLE PRECISION,
  superficie_escluse    DOUBLE PRECISION,
  superficie_raw        TEXT,

  -- Indirizzo (parsed + raw)
  immobile_comune       TEXT,
  immobile_comune_code  TEXT,
  via_type              TEXT,
  via_name              TEXT,
  via_num               TEXT,
  scala                 TEXT,
  interno               TEXT,
  piano                 TEXT,
  indirizzo_raw         TEXT,
  dati_ulteriori        TEXT,

  created_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at            TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_immobili_visura_cf ON public.immobili(visura_cf);
CREATE INDEX IF NOT EXISTS idx_immobili_catasto   ON public.immobili(foglio, numero, sub);
CREATE INDEX IF NOT EXISTS idx_immobili_comune_code ON public.immobili(immobile_comune_code);

-- --- Ensure sub normalization + uniqueness even if table already existed ---
DO $$
DECLARE
  is_nullable TEXT;
BEGIN
  -- 1) Ensure sub DEFAULT '' and NOT NULL (idempotent + safe for existing DB)
  SELECT c.is_nullable
    INTO is_nullable
  FROM information_schema.columns c
  WHERE c.table_schema = 'public'
    AND c.table_name = 'immobili'
    AND c.column_name = 'sub';

  -- If column exists and is nullable -> normalize old NULLs before NOT NULL
  IF is_nullable = 'YES' THEN
    EXECUTE 'UPDATE public.immobili SET sub = '''' WHERE sub IS NULL;';
    EXECUTE 'ALTER TABLE public.immobili ALTER COLUMN sub SET DEFAULT '''';';
    EXECUTE 'ALTER TABLE public.immobili ALTER COLUMN sub SET NOT NULL;';
  ELSE
    -- Still enforce default (harmless if already set)
    EXECUTE 'ALTER TABLE public.immobili ALTER COLUMN sub SET DEFAULT '''';';
  END IF;

  -- 2) Add UNIQUE constraint on canonical key if missing
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'uq_immobili_visura_fns'
      AND conrelid = 'public.immobili'::regclass
  ) THEN
    -- NOTE: якщо у таблиці вже є дублікати, цей крок впаде.
    -- Це нормально: спочатку почистити дублікати, потім додати constraint.
    EXECUTE 'ALTER TABLE public.immobili
             ADD CONSTRAINT uq_immobili_visura_fns
             UNIQUE (visura_cf, foglio, numero, sub);';
  END IF;
END$$;

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'trg_immobili_updated_at') THEN
    CREATE TRIGGER trg_immobili_updated_at
    BEFORE UPDATE ON public.immobili
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();
  END IF;
END$$;

-- =========================================================
-- 4) CONTRACTS (договір як бізнес-сутність)
--    1 immobile -> N contracts (можливо, з часом)
-- =========================================================
CREATE TABLE IF NOT EXISTS public.contracts (
  contract_id                 UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  immobile_id                 BIGINT NOT NULL REFERENCES public.immobili(id) ON DELETE RESTRICT,

  contract_kind               TEXT,
  start_date                  DATE,
  durata_anni                 INTEGER,
  arredato                    DOUBLE PRECISION,
  energy_class                TEXT,
  canone_contrattuale_mensile NUMERIC,

  created_at                  TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at                  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_contracts_immobile_id ON public.contracts(immobile_id);

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'trg_contracts_updated_at') THEN
    CREATE TRIGGER trg_contracts_updated_at
    BEFORE UPDATE ON public.contracts
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();
  END IF;
END$$;

-- =========================================================
-- 5) CONTRACT_PARTIES (зв’язок контракту з особами)
-- =========================================================
CREATE TABLE IF NOT EXISTS public.contract_parties (
  contract_id UUID NOT NULL REFERENCES public.contracts(contract_id) ON DELETE CASCADE,
  role        TEXT NOT NULL CHECK (role IN ('LOCATORE', 'CONDUTTORE')),
  person_cf   TEXT NOT NULL REFERENCES public.persons(cf) ON DELETE RESTRICT,

  PRIMARY KEY (contract_id, role)
);

CREATE INDEX IF NOT EXISTS idx_contract_parties_person_cf ON public.contract_parties(person_cf);

-- =========================================================
-- 6) CONTRACT_ELEMENTS (A/B/C/D у нормалізованому вигляді)
-- =========================================================
CREATE TABLE IF NOT EXISTS public.contract_elements (
  contract_id UUID NOT NULL REFERENCES public.contracts(contract_id) ON DELETE CASCADE,
  grp         CHAR(1) NOT NULL CHECK (grp IN ('A','B','C','D')),
  code        TEXT NOT NULL,        -- напр. "A1", "B3", "D12" або будь-який внутрішній ключ
  value       TEXT,                 -- зберігай як текст; numeric можна обробляти на рівні коду

  PRIMARY KEY (contract_id, grp, code)
);

-- =========================================================
-- 7) ACCORDI (PDF/версії accordo) + zones (опційно, але корисно)
-- =========================================================
CREATE TABLE IF NOT EXISTS public.accordi (
  accordo_id     UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  comune_code    TEXT,
  title          TEXT,
  pdf_bucket     TEXT,
  pdf_object     TEXT,
  effective_from DATE,

  created_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_accordi_comune_code ON public.accordi(comune_code);

CREATE TABLE IF NOT EXISTS public.accordo_zones (
  zone_id     UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  accordo_id  UUID NOT NULL REFERENCES public.accordi(accordo_id) ON DELETE CASCADE,
  zone_code   TEXT NOT NULL,
  description TEXT,

  UNIQUE (accordo_id, zone_code)
);

CREATE INDEX IF NOT EXISTS idx_accordo_zones_accordo_id ON public.accordo_zones(accordo_id);

-- =========================================================
-- 8) CANONE_CALCOLI (результати розрахунку canone)
-- =========================================================
CREATE TABLE IF NOT EXISTS public.canone_calcoli (
  calc_id        UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  contract_id    UUID NOT NULL REFERENCES public.contracts(contract_id) ON DELETE CASCADE,
  zone_id        UUID REFERENCES public.accordo_zones(zone_id) ON DELETE SET NULL,

  method         TEXT,
  inputs         JSONB,     -- всі вхідні параметри розрахунку
  result_mensile NUMERIC,

  created_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_canone_calcoli_contract_id ON public.canone_calcoli(contract_id);

-- =========================================================
-- 9) ATTESTAZIONI (лог кожної генерації)
-- =========================================================
CREATE TABLE IF NOT EXISTS public.attestazioni (
  attestazione_id     UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  contract_id         UUID NOT NULL REFERENCES public.contracts(contract_id) ON DELETE CASCADE,

  generated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),

  author_login_masked TEXT NOT NULL,
  author_login_sha256 TEXT,

  template_version    TEXT,
  output_bucket       TEXT NOT NULL,
  output_object       TEXT NOT NULL,

  params_snapshot     JSONB NOT NULL,
  params_hash         TEXT GENERATED ALWAYS AS (
    encode(digest(params_snapshot::text, 'sha256'), 'hex')
  ) STORED,

  status              TEXT NOT NULL DEFAULT 'generated'
                      CHECK (status IN ('generated','failed')),
  error               TEXT
);

CREATE INDEX IF NOT EXISTS idx_attestazioni_contract_id   ON public.attestazioni(contract_id);
CREATE INDEX IF NOT EXISTS idx_attestazioni_generated_at  ON public.attestazioni(generated_at);
CREATE INDEX IF NOT EXISTS idx_attestazioni_author_masked ON public.attestazioni(author_login_masked);
CREATE INDEX IF NOT EXISTS idx_attestazioni_params_hash   ON public.attestazioni(params_hash);

-- =========================================================
-- 10) CONTRACT_OVERRIDES (override-адреси)
-- =========================================================
CREATE TABLE IF NOT EXISTS public.contract_overrides (
  contract_id UUID PRIMARY KEY REFERENCES public.contracts(contract_id) ON DELETE CASCADE,

  immobile_comune_override  TEXT,
  immobile_via_override     TEXT,
  immobile_civico_override  TEXT,
  immobile_piano_override   TEXT,
  immobile_interno_override TEXT,

  locatore_comune_res       TEXT,
  locatore_via              TEXT,
  locatore_civico           TEXT,

  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'trg_contract_overrides_updated_at') THEN
    CREATE TRIGGER trg_contract_overrides_updated_at
    BEFORE UPDATE ON public.contract_overrides
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();
  END IF;
END$$;

COMMIT;
