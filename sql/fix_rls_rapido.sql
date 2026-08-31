-- Arreglo rápido de RLS para SubaDatos
-- Pegar en: Supabase → SQL Editor → Run
--
-- Qué hace:
-- 1. Activa RLS en todas las tablas públicas (cierra el aviso de seguridad).
-- 2. Permite LECTURA anónima solo de precios (lo que ya es público).
-- 3. BLOQUEA escritura anónima (insert/update/delete). El ETL sigue
--    funcionando porque usa service_role, que se salta RLS.
-- 4. Protege `feedback` (correo/PII): nadie público lee ni escribe.
--    Streamlit con service_role sí puede insertar.

-- ── 1. Activar RLS ──────────────────────────────────────────
alter table if exists public.subastas enable row level security;
alter table if exists public.subastas_casanare enable row level security;
alter table if exists public.subastar_precios_resumen enable row level security;
alter table if exists public.features_externas enable row level security;
alter table if exists public.feedback enable row level security;

-- Por si el advisor también marca el storage interno
-- (el bucket público `subadatos-publicaciones` se regula en Storage → Policies)

-- ── 2. Quitar políticas viejas demasiado abiertas ────────────
do $$
declare
  pol record;
begin
  for pol in
    select schemaname, tablename, policyname
    from pg_policies
    where schemaname = 'public'
      and tablename in (
        'subastas',
        'subastas_casanare',
        'subastar_precios_resumen',
        'features_externas',
        'feedback'
      )
  loop
    execute format('drop policy if exists %I on %I.%I', pol.policyname, pol.schemaname, pol.tablename);
  end loop;
end $$;

-- ── 3. Lectura pública de precios ───────────────────────────
create policy "lectura_publica"
  on public.subastas
  for select
  to anon, authenticated
  using (true);

create policy "lectura_publica"
  on public.subastas_casanare
  for select
  to anon, authenticated
  using (true);

create policy "lectura_publica"
  on public.subastar_precios_resumen
  for select
  to anon, authenticated
  using (true);

create policy "lectura_publica"
  on public.features_externas
  for select
  to anon, authenticated
  using (true);

-- feedback: SIN política de select/insert para anon.
-- Con RLS activo y sin policy, anon queda bloqueado.
-- Streamlit/ETL con service_role no se ven afectados.

-- ── 4. Storage: lectura pública del JSON de la landing, ─────
--    escritura solo autenticada/service (el dashboard de Storage
--    a veces exige policies aunque el bucket sea public).
--
-- Ejecutar solo si el advisor marca el bucket.
-- create policy "lectura_landing"
--   on storage.objects for select
--   to anon, authenticated
--   using (bucket_id = 'subadatos-publicaciones');
