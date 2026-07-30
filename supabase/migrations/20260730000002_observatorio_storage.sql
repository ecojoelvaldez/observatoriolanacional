-- =====================================================================
-- Observatorio Estratégico La Nacional — Storage
-- Migración Ruta B: recrea el bucket y las políticas del proyecto origen
-- en el proyecto destino.
--
-- Al 2026-07-30 el bucket `analyst-uploads` del origen está VACÍO
-- (0 objetos), así que no hay archivos que copiar: basta con recrear la
-- definición del bucket y sus políticas.
--
-- Ejecutar contra el proyecto DESTINO:
--   psql "postgresql://postgres:[PASSWORD]@db.<ref>.supabase.co:5432/postgres" \
--        -v ON_ERROR_STOP=1 -f 20260730000002_observatorio_storage.sql
--
-- Si prefieres no tocar el schema `storage` por SQL, el equivalente en el
-- dashboard es: Storage -> New bucket -> name `analyst-uploads`, private,
-- sin límite de tamaño ni de mime types; y luego las dos políticas de abajo.
-- =====================================================================

begin;

-- ---------------------------------------------------------------------
-- 1. Bucket privado para cargas de los analistas
-- ---------------------------------------------------------------------
insert into storage.buckets (id, name, public, file_size_limit, allowed_mime_types)
values ('analyst-uploads', 'analyst-uploads', false, null, null)
on conflict (id) do update
  set public             = excluded.public,
      file_size_limit    = excluded.file_size_limit,
      allowed_mime_types = excluded.allowed_mime_types;

-- ---------------------------------------------------------------------
-- 2. Políticas sobre storage.objects
-- ---------------------------------------------------------------------
drop policy if exists "authenticated read analyst files"   on storage.objects;
drop policy if exists "authenticated upload analyst files" on storage.objects;

create policy "authenticated read analyst files"
  on storage.objects for select to authenticated
  using (bucket_id = 'analyst-uploads');

create policy "authenticated upload analyst files"
  on storage.objects for insert to authenticated
  with check (bucket_id = 'analyst-uploads');

commit;

-- ---------------------------------------------------------------------
-- Verificación:
--   select id, public from storage.buckets;
--   select policyname, cmd from pg_policies where schemaname='storage';
-- ---------------------------------------------------------------------
