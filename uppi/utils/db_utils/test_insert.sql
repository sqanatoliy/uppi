-- щоб перевірити, що вся схема реально працює зробити мінімальний тест-вставку
-- одна вставка зараз по ланцюжку persons → visure → immobili → contracts → attestazioni

-- 1) person
INSERT INTO public.persons (cf, surname, name)
VALUES ('TESTCF0000000000', 'Rossi', 'Mario')
ON CONFLICT (cf) DO NOTHING;

-- 2) visura
INSERT INTO public.visure (cf, pdf_bucket, pdf_object, fetched_at)
VALUES ('TESTCF0000000000', 'visure', 'TESTCF0000000000/visura.pdf', now())
ON CONFLICT (cf) DO UPDATE SET
  pdf_bucket = EXCLUDED.pdf_bucket,
  pdf_object = EXCLUDED.pdf_object,
  fetched_at = EXCLUDED.fetched_at;

-- 3) immobile
INSERT INTO public.immobili (visura_cf, foglio, numero, sub, immobile_comune, via_type, via_name, via_num, indirizzo_raw)
VALUES ('TESTCF0000000000', '12', '345', '6', 'Pescara', 'VIA', 'ROMA', '10', 'VIA ROMA n. 10')
RETURNING id;

-- 4) contract (підстав id з попереднього RETURNING)
-- припустимо id = 1
INSERT INTO public.contracts (immobile_id, contract_kind, start_date, durata_anni, arredato, canone_contrattuale_mensile)
VALUES (1, 'TRANSITORIO', '2025-01-01', 1, true, 650.00)
RETURNING contract_id;

-- 5) attestazione
-- підстав contract_id з попереднього RETURNING
INSERT INTO public.attestazioni (
  contract_id,
  author_login_masked,
  author_login_sha256,
  template_version,
  output_bucket,
  output_object,
  params_snapshot,
  status
) VALUES (
  'PASTE_CONTRACT_ID_HERE',
  'ma***io',
  encode(digest('mario.rossi@example.com','sha256'),'hex'),
  'attestazione_v1',
  'attestazioni',
  'TESTCF0000000000/attestazione.docx',
  '{"client_yaml":{"foo":"bar"},"computed":{"canone":650},"template_context":{"A":"B"},"versions":{"schema":"v1"}}'::jsonb,
  'generated'
)
RETURNING attestazione_id, params_hash, generated_at;
