-- =====================================================================
-- Observatorio Estratégico · La Nacional
-- 20260731000001_news_publish_fix_and_archive.sql
--
-- 1. Arregla la publicación del resumen de noticias, rota desde la
--    migración al proyecto Supabase departamental.
--
--    Diagnóstico:
--      a) news_items_status_check solo admitía ('pending','approved','rejected'),
--         pero TODO el front publica con status = 'published' y las políticas
--         RLS "news_items_anon_insert_published" / "_update_published" EXIGEN
--         status = 'published'. Es decir: la política obliga a un valor que el
--         CHECK prohíbe -> cada publicación fallaba con
--         "violates check constraint news_items_status_check".
--      b) news_items.source es NOT NULL y el front nunca lo envía (manda
--         source_name). Aun sin (a), el insert fallaba por not-null.
--      c) Como nada quedaba con status='published', el lector del front
--         (.eq('status','published')) devolvía 0 filas: el resumen se veía
--         solo en el localStorage del navegador que publicó, no en el resto
--         de los dispositivos.
--
-- 2. Agrega news_summary_snapshots: archivo diario del resumen publicado,
--    que alimenta el acápite "Resúmenes pasados" de la portada.
-- =====================================================================

-- DÓNDE CORRERLA
--   Sobre el proyecto que sirve al front hoy, es decir el que aparece en el
--   <meta name="supabase-url"> de index.html: albtuqzdcltcokfagdvy (ORIGEN,
--   "la nacional"). En el proyecto departamental de destino
--   (btccnnreeansagcduutt) hay que correr ANTES el schema base
--   20260730000001_observatorio_public_schema.sql — ver docs/MIGRACION_SUPABASE.md.
--
--   Si sale "relation public.news_items does not exist", el SQL Editor está
--   apuntando a un proyecto sin el schema del Observatorio. El guard de abajo
--   lo dice explícitamente en vez de fallar con el 42P01 pelado.

begin;

-- ---------------------------------------------------------------------
-- 1.0 Guard: esta migración modifica tablas que ya deben existir.
-- ---------------------------------------------------------------------
do $$
begin
  if to_regclass('public.news_items') is null then
    raise exception using
      message = 'Este proyecto no tiene public.news_items.',
      hint    = 'Estás en el proyecto equivocado, o falta correr primero '
                'supabase/migrations/20260730000001_observatorio_public_schema.sql. '
                'El proyecto que sirve al front es el del <meta name="supabase-url"> de index.html.';
  end if;
end $$;

-- ---------------------------------------------------------------------
-- 1.1 Permitir el estado 'published' (el que usan front y políticas RLS)
-- ---------------------------------------------------------------------
alter table public.news_items
  drop constraint if exists news_items_status_check;

alter table public.news_items
  add constraint news_items_status_check
  check (status = any (array['pending','approved','rejected','published']));

-- ---------------------------------------------------------------------
-- 1.2 'source' deja de ser obligatorio: el front envía 'source_name'.
--     Se rellena con source_name para las filas ya existentes.
-- ---------------------------------------------------------------------
alter table public.news_items
  alter column source drop not null;

update public.news_items
   set source = coalesce(source, source_name)
 where source is null
   and source_name is not null;

-- ---------------------------------------------------------------------
-- 2. Archivo diario del resumen de noticias ("Resúmenes pasados")
-- ---------------------------------------------------------------------
create table if not exists public.news_summary_snapshots (
  snapshot_date date        primary key,
  published_at  timestamptz not null default now(),
  item_count    integer     not null default 0,
  items         jsonb       not null default '[]'::jsonb,
  updated_at    timestamptz not null default now()
);

create index if not exists idx_news_summary_snapshots_date
  on public.news_summary_snapshots using btree (snapshot_date desc);

alter table public.news_summary_snapshots enable row level security;

drop policy if exists "public read news snapshots"   on public.news_summary_snapshots;
drop policy if exists "anon insert news snapshots"   on public.news_summary_snapshots;
drop policy if exists "anon update news snapshots"   on public.news_summary_snapshots;

-- Lectura pública: el archivo es contenido publicado.
create policy "public read news snapshots"
  on public.news_summary_snapshots for select to public using (true);

-- Escritura desde el panel del analista. Se mantiene el mismo criterio que
-- news_items (anon + authenticated) porque la sesión del panel es local.
create policy "anon insert news snapshots"
  on public.news_summary_snapshots for insert to anon, authenticated
  with check (jsonb_typeof(items) = 'array');
create policy "anon update news snapshots"
  on public.news_summary_snapshots for update to anon, authenticated
  using (true) with check (jsonb_typeof(items) = 'array');

grant all on public.news_summary_snapshots to anon, authenticated, service_role;

commit;

-- ---------------------------------------------------------------------
-- Verificación rápida (correr aparte):
--   select conname, pg_get_constraintdef(oid)
--     from pg_constraint where conrelid = 'public.news_items'::regclass;
--   select count(*) filter (where status = 'published') as publicadas,
--          count(*) as total
--     from public.news_items;
--   select snapshot_date, item_count from public.news_summary_snapshots
--     order by snapshot_date desc limit 10;
-- ---------------------------------------------------------------------
