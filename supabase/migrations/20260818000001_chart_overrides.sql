-- =====================================================================
-- Estudio de gráficas · configuración editable desde el propio sitio
-- =====================================================================
--
-- Motivo. Hasta ahora, cambiar un número, un color o la forma de una
-- gráfica de Cartera exigía tocar index.html y desplegar. Esta tabla
-- guarda la edición que el analista hace dentro del sitio (Estudio de
-- gráficas) para que la vea todo el que entre, sin pasar por el
-- repositorio ni por un desarrollador.
--
-- Una fila = una gráfica en un contexto. `chart_key` combina el id del
-- canvas con la pestaña activa, por ejemplo:
--
--     car-chart-evolucion@creditos:total
--     car-chart-region@creditos:consumo:tarjetas_credito
--     cap-chart-ranking@captacion:ahorros
--
-- `config` guarda etiquetas, series, tipo de gráfica, colores, formato
-- y textos. El sitio sabe reconstruir la gráfica desde ahí; si la fila
-- no existe o `enabled` es false, se dibuja el dato original del
-- pipeline SIB. Restablecer nunca borra: apaga `enabled`, así que el
-- historial de lo que se editó queda disponible.
--
-- RLS. Mismo criterio que news_items: el login del analista vive en el
-- navegador, no en Supabase Auth, así que la clave anónima necesita
-- escribir. Lectura pública para que la edición sea visible para todos.
-- =====================================================================

create table if not exists public.chart_overrides (
  chart_key   text primary key,
  canvas_id   text not null,
  context_key text not null default 'base',
  seccion     text,
  titulo      text,
  config      jsonb not null default '{}'::jsonb,
  enabled     boolean not null default true,
  updated_at  timestamptz not null default now(),
  updated_by  text
);

create index if not exists chart_overrides_enabled_idx
  on public.chart_overrides (enabled)
  where enabled = true;

create index if not exists chart_overrides_canvas_idx
  on public.chart_overrides (canvas_id);

alter table public.chart_overrides enable row level security;

drop policy if exists "chart_overrides_public_read" on public.chart_overrides;
create policy "chart_overrides_public_read"
  on public.chart_overrides for select
  using (true);

drop policy if exists "chart_overrides_anon_insert" on public.chart_overrides;
create policy "chart_overrides_anon_insert"
  on public.chart_overrides for insert
  with check (true);

drop policy if exists "chart_overrides_anon_update" on public.chart_overrides;
create policy "chart_overrides_anon_update"
  on public.chart_overrides for update
  using (true)
  with check (true);

comment on table public.chart_overrides is
  'Ediciones de gráficas hechas desde el Estudio de gráficas del sitio. Una fila por gráfica y contexto (pestaña).';
comment on column public.chart_overrides.chart_key is
  'id del canvas + @ + contexto (pestaña activa). Ej: car-chart-evolucion@creditos:total';
comment on column public.chart_overrides.config is
  'JSON con labels, series, tipo, colores, formato y textos. Ver LN_CHART_STUDIO en index.html.';
comment on column public.chart_overrides.enabled is
  'false = restablecida al dato original del pipeline, conservando lo editado por si se quiere volver.';
