-- =====================================================================
-- Observatorio Estratégico La Nacional — schema `public`
-- Migración Ruta B: proyecto origen (personal) -> proyecto destino
-- (departamental). Reproduce el schema `public` completo del origen
-- `albtuqzdcltcokfagdvy` tal como estaba al 2026-07-30.
--
-- Ejecutar contra el proyecto DESTINO:
--   psql "postgresql://postgres:[PASSWORD]@db.<ref>.supabase.co:5432/postgres" \
--        -v ON_ERROR_STOP=1 -f 20260730000001_observatorio_public_schema.sql
--
-- Es idempotente: se puede correr varias veces sin romper nada.
--
-- NO toca los schemas gestionados por Supabase (auth, storage, realtime,
-- vault, extensions) salvo por las FKs a auth.users, que son referencias
-- normales y no modifican ese schema.
-- =====================================================================

begin;

-- ---------------------------------------------------------------------
-- 0. Extensiones (mismas que el origen; Supabase ya trae casi todas)
-- ---------------------------------------------------------------------
create extension if not exists "uuid-ossp" with schema extensions;
create extension if not exists pgcrypto   with schema extensions;

-- ---------------------------------------------------------------------
-- 1. Tablas
-- ---------------------------------------------------------------------

-- 1.1 Bitácora de actualizaciones de datos (también persiste el estado
--     "live" del front en source = 'LIVE_STATE', section = 'front_state').
create table if not exists public.data_update_log (
  id             uuid        primary key default gen_random_uuid(),
  source         text        not null,
  section        text        not null,
  status         text        not null default 'success',
  file_name      text,
  rows_processed integer     default 0,
  message        text,
  metadata       jsonb       not null default '{}'::jsonb,
  updated_at     timestamptz not null default now(),
  updated_by     uuid        references auth.users(id) on delete set null,
  constraint data_update_log_source_check check (
    upper(source) = any (array[
      'SIB','BCRD','NEWS','LIVE','LIVE_STATE','SYSTEM','MACRO',
      'DAILY','WTI','OBSERVATORIO_LIVE','LIVE_SNAPSHOT'
    ])
  ),
  constraint data_update_log_status_check check (
    status = any (array['success','warning','error','pending'])
  ),
  constraint data_update_log_rows_processed_check check (rows_processed >= 0)
);

-- 1.2 Series de la Superintendencia de Bancos en formato largo
create table if not exists public.sib_series_long (
  id           uuid        primary key default gen_random_uuid(),
  periodo      date        not null,
  entidad      text        not null,
  tipo_entidad text,
  indicador    text        not null,
  valor        numeric(20,6),
  unidad       text,
  source_file  text,
  metadata     jsonb       not null default '{}'::jsonb,
  updated_at   timestamptz not null default now(),
  updated_by   uuid        references auth.users(id) on delete set null,
  constraint uq_sib_series_long unique (periodo, entidad, indicador)
);

-- 1.3 Peer group SIB (una fila por entidad y periodo)
create table if not exists public.sib_peer_group (
  id           uuid        primary key default gen_random_uuid(),
  periodo      date        not null,
  entidad      text        not null,
  tipo_entidad text,
  activos      numeric(20,2),
  morosidad    numeric(12,4),
  solvencia    numeric(12,4),
  cti          numeric(12,4),
  roe          numeric(12,4),
  roa          numeric(12,4),
  source_file  text,
  metadata     jsonb       not null default '{}'::jsonb,
  updated_at   timestamptz not null default now(),
  updated_by   uuid        references auth.users(id) on delete set null,
  constraint uq_sib_peer_group unique (periodo, entidad)
);

-- 1.4 Series del Banco Central
create table if not exists public.bcrd_series (
  id          uuid        primary key default gen_random_uuid(),
  serie_id    text        not null,
  nombre      text        not null,
  categoria   text,
  frecuencia  text        default 'mensual',
  unidad      text,
  periodo     date        not null,
  valor       numeric(20,6),
  source_file text,
  metadata    jsonb       not null default '{}'::jsonb,
  updated_at  timestamptz not null default now(),
  updated_by  uuid        references auth.users(id) on delete set null,
  constraint uq_bcrd_series unique (serie_id, periodo),
  constraint bcrd_series_frecuencia_check check (
    frecuencia = any (array['diaria','semanal','mensual','trimestral','anual'])
  )
);

-- 1.5 Lotes de carga BCRD
create table if not exists public.bcrd_upload_batches (
  id             uuid        primary key default gen_random_uuid(),
  batch_month    date        not null,
  file_name      text        not null,
  rows_processed integer     default 0,
  status         text        not null default 'success',
  message        text,
  metadata       jsonb       not null default '{}'::jsonb,
  created_at     timestamptz not null default now(),
  created_by     uuid        references auth.users(id) on delete set null,
  constraint bcrd_upload_batches_status_check check (
    status = any (array['success','warning','error','pending'])
  ),
  constraint bcrd_upload_batches_rows_processed_check check (rows_processed >= 0)
);

-- 1.6 Fuentes de noticias que consume el pipeline
create table if not exists public.news_sources (
  id              uuid        primary key default gen_random_uuid(),
  name            text        not null,
  source_key      text        not null unique,
  url             text        not null,
  category        text        default 'Economía',
  enabled         boolean     not null default true,
  last_checked_at timestamptz,
  created_at      timestamptz not null default now(),
  updated_at      timestamptz not null default now()
);

-- 1.7 Noticias publicadas / en revisión
create table if not exists public.news_items (
  id           uuid        primary key default gen_random_uuid(),
  title        text        not null,
  -- 'source' queda nullable a propósito: el panel publica 'source_name'.
  -- Ver 20260731000001_news_publish_fix_and_archive.sql.
  source       text,
  source_key   text        references public.news_sources(source_key) on delete set null,
  url          text        not null,
  category     text        default 'Economía',
  published_at timestamptz,
  summary      text,
  status       text        not null default 'pending',
  approved_at  timestamptz,
  approved_by  uuid        references auth.users(id) on delete set null,
  rejected_at  timestamptz,
  rejected_by  uuid        references auth.users(id) on delete set null,
  metadata     jsonb       not null default '{}'::jsonb,
  created_at   timestamptz not null default now(),
  updated_at   timestamptz not null default now(),
  source_name  text,
  body         text,
  display_date text,
  sort_order   integer     default 1,
  is_featured  boolean     default false,
  constraint uq_news_items_url unique (url),
  -- 'published' es el estado que exigen las políticas RLS de más abajo
  -- (news_items_anon_insert_published) y el que envía el front al publicar.
  constraint news_items_status_check check (
    status = any (array['pending','approved','rejected','published'])
  )
);

-- 1.8 Propuestas de noticias generadas por el pipeline Python/Gemini
create table if not exists public.news_proposals (
  id           uuid        primary key default gen_random_uuid(),
  source_key   text        not null,
  source_name  text,
  url          text        not null unique,
  title        text,
  summary      text,
  category     text,
  published_at timestamptz,
  fetched_at   timestamptz default now(),
  status       text        default 'pending',
  reviewed_by  text,
  reviewed_at  timestamptz,
  raw_excerpt  text
);

-- 1.9 Cuota diaria de la búsqueda conversacional
create table if not exists public.conversational_search_quota (
  search_date  date        primary key default current_date,
  search_count integer     not null default 0,
  updated_at   timestamptz default now()
);

-- ---------------------------------------------------------------------
-- 2. Índices
-- ---------------------------------------------------------------------
create index if not exists idx_data_update_log_source_updated
  on public.data_update_log using btree (source, updated_at desc);

create index if not exists idx_sib_series_lookup
  on public.sib_series_long using btree (indicador, entidad, periodo desc);

create index if not exists idx_sib_peer_group_periodo
  on public.sib_peer_group using btree (periodo desc);

create index if not exists idx_sib_peer_group_entidad
  on public.sib_peer_group using btree (entidad);

create index if not exists idx_bcrd_series_lookup
  on public.bcrd_series using btree (serie_id, periodo desc);

create unique index if not exists idx_news_items_url_unique
  on public.news_items using btree (url);

create index if not exists idx_news_items_status_published
  on public.news_items using btree (status, published_at desc nulls last, created_at desc);

create index if not exists idx_news_items_status_order
  on public.news_items using btree (status, sort_order, updated_at desc);

-- ---------------------------------------------------------------------
-- 3. Funciones
-- ---------------------------------------------------------------------

-- Incrementa (y limita) la cuota diaria de búsqueda conversacional.
-- Copia fiel del origen: SECURITY INVOKER (el default de Postgres).
-- OJO: `conversational_search_quota` tiene RLS activo y CERO políticas, así
-- que esta función solo funciona si se llama con `service_role`. Llamada con
-- la anon key devuelve un error de RLS. Hoy no la invoca nada en este repo,
-- por eso se replica tal cual en vez de "arreglarla" durante la migración.
-- Si algún día se usa desde el front, cambiarla a:
--   security definer set search_path = public, pg_temp
create or replace function public.increment_search_quota(daily_limit integer default 3)
returns json
language plpgsql
as $function$
declare
  current_count int;
begin
  insert into conversational_search_quota (search_date, search_count)
  values (current_date, 0)
  on conflict (search_date) do nothing;

  select search_count into current_count
  from conversational_search_quota
  where search_date = current_date
  for update;

  if current_count >= daily_limit then
    return json_build_object('allowed', false, 'count', current_count, 'limit', daily_limit);
  end if;

  update conversational_search_quota
  set search_count = search_count + 1, updated_at = now()
  where search_date = current_date;

  return json_build_object('allowed', true, 'count', current_count + 1, 'limit', daily_limit);
end;
$function$;

revoke all on function public.increment_search_quota(integer) from public;
grant execute on function public.increment_search_quota(integer) to anon, authenticated, service_role;

-- ---------------------------------------------------------------------
-- 4. Row Level Security
-- ---------------------------------------------------------------------
alter table public.data_update_log             enable row level security;
alter table public.sib_series_long             enable row level security;
alter table public.sib_peer_group              enable row level security;
alter table public.bcrd_series                 enable row level security;
alter table public.bcrd_upload_batches         enable row level security;
alter table public.news_sources                enable row level security;
alter table public.news_items                  enable row level security;
alter table public.news_proposals              enable row level security;
alter table public.conversational_search_quota enable row level security;

-- 4.1 data_update_log
drop policy if exists "public read update logs"     on public.data_update_log;
drop policy if exists "analysts insert update logs" on public.data_update_log;
drop policy if exists "analysts update update logs" on public.data_update_log;

create policy "public read update logs"
  on public.data_update_log for select to public using (true);
create policy "analysts insert update logs"
  on public.data_update_log for insert to authenticated with check (true);
create policy "analysts update update logs"
  on public.data_update_log for update to authenticated using (true) with check (true);

-- 4.2 sib_series_long
drop policy if exists "public read sib series"     on public.sib_series_long;
drop policy if exists "analysts upsert sib series" on public.sib_series_long;
drop policy if exists "analysts update sib series" on public.sib_series_long;

create policy "public read sib series"
  on public.sib_series_long for select to public using (true);
create policy "analysts upsert sib series"
  on public.sib_series_long for insert to authenticated with check (true);
create policy "analysts update sib series"
  on public.sib_series_long for update to authenticated using (true) with check (true);

-- 4.3 sib_peer_group
drop policy if exists "public read sib peer group"     on public.sib_peer_group;
drop policy if exists "analysts upsert sib peer group" on public.sib_peer_group;
drop policy if exists "analysts update sib peer group" on public.sib_peer_group;

create policy "public read sib peer group"
  on public.sib_peer_group for select to public using (true);
create policy "analysts upsert sib peer group"
  on public.sib_peer_group for insert to authenticated with check (true);
create policy "analysts update sib peer group"
  on public.sib_peer_group for update to authenticated using (true) with check (true);

-- 4.4 bcrd_series
drop policy if exists "public read bcrd series"     on public.bcrd_series;
drop policy if exists "analysts upsert bcrd series" on public.bcrd_series;
drop policy if exists "analysts update bcrd series" on public.bcrd_series;

create policy "public read bcrd series"
  on public.bcrd_series for select to public using (true);
create policy "analysts upsert bcrd series"
  on public.bcrd_series for insert to authenticated with check (true);
create policy "analysts update bcrd series"
  on public.bcrd_series for update to authenticated using (true) with check (true);

-- 4.5 bcrd_upload_batches
drop policy if exists "analysts read bcrd batches"   on public.bcrd_upload_batches;
drop policy if exists "analysts insert bcrd batches" on public.bcrd_upload_batches;

create policy "analysts read bcrd batches"
  on public.bcrd_upload_batches for select to public using (true);
create policy "analysts insert bcrd batches"
  on public.bcrd_upload_batches for insert to authenticated with check (true);

-- 4.6 news_sources
drop policy if exists "public read news sources"     on public.news_sources;
drop policy if exists "analysts manage news sources" on public.news_sources;

create policy "public read news sources"
  on public.news_sources for select to public using (true);
create policy "analysts manage news sources"
  on public.news_sources for all to authenticated using (true) with check (true);

-- 4.7 news_items
drop policy if exists "public read approved news only"     on public.news_items;
drop policy if exists "analysts insert news"               on public.news_items;
drop policy if exists "analysts update news"               on public.news_items;
drop policy if exists "news_items_public_read_published"   on public.news_items;
drop policy if exists "news_items_anon_insert_published"   on public.news_items;
drop policy if exists "news_items_anon_update_published"   on public.news_items;

create policy "public read approved news only"
  on public.news_items for select to public
  using ((status = 'approved') or (auth.role() = 'authenticated'));
create policy "analysts insert news"
  on public.news_items for insert to authenticated with check (true);
create policy "analysts update news"
  on public.news_items for update to authenticated using (true) with check (true);
create policy "news_items_public_read_published"
  on public.news_items for select to anon, authenticated
  using (status = 'published');
create policy "news_items_anon_insert_published"
  on public.news_items for insert to anon, authenticated
  with check ((status = 'published') and (url is not null) and (title is not null));
create policy "news_items_anon_update_published"
  on public.news_items for update to anon, authenticated
  using (status = 'published')
  with check ((status = 'published') and (url is not null) and (title is not null));

-- 4.8 news_proposals
drop policy if exists "anon read"   on public.news_proposals;
drop policy if exists "anon update" on public.news_proposals;

create policy "anon read"
  on public.news_proposals for select to public using (true);
create policy "anon update"
  on public.news_proposals for update to public using (true);

-- 4.9 conversational_search_quota: RLS activo y sin políticas a propósito.
--     Solo se accede vía public.increment_search_quota() (SECURITY DEFINER).

-- ---------------------------------------------------------------------
-- 5. Grants (replican los privilegios por defecto de Supabase del origen)
-- ---------------------------------------------------------------------
grant all on all tables    in schema public to anon, authenticated, service_role;
grant all on all sequences in schema public to anon, authenticated, service_role;

commit;

-- ---------------------------------------------------------------------
-- Verificación rápida (correr aparte):
--   select tablename, rowsecurity from pg_tables where schemaname='public';
--   select tablename, policyname, cmd from pg_policies where schemaname='public'
--     order by tablename, policyname;
-- ---------------------------------------------------------------------
