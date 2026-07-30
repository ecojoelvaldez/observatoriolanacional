/* Observatorio Estratégico · La Nacional
   Minimal Supabase data layer for vanilla HTML/Vercel.

   Usage in HTML:
   <script type="module" src="./assets/supabase-data-layer.js"></script>

   El project ref y la anon key salen de los <meta> del <head>:
     <meta name="supabase-url" content="https://<ref>.supabase.co">
     <meta name="supabase-anon-key" content="...">
   o de window.__OBSERVATORIO_SUPABASE__ = { url, anonKey } si hay que
   apuntar a otro proyecto sin editar el HTML.
*/

import { createClient } from 'https://esm.sh/@supabase/supabase-js@2';

function readSupabaseConfig() {
  const meta = name => document.querySelector(`meta[name="${name}"]`)?.content?.trim() || '';
  const override = window.__OBSERVATORIO_SUPABASE__ || {};
  const url = (override.url || meta('supabase-url')).replace(/\/+$/, '');
  const anonKey = override.anonKey || meta('supabase-anon-key');
  if (!url || !anonKey) {
    throw new Error('Falta la configuración de Supabase (meta supabase-url / supabase-anon-key).');
  }
  return { url, anonKey };
}

const { url: SUPABASE_URL, anonKey: SUPABASE_ANON_KEY } = readSupabaseConfig();

export const supabase = createClient(SUPABASE_URL, SUPABASE_ANON_KEY, {
  auth: {
    persistSession: true,
    autoRefreshToken: true,
    detectSessionInUrl: true
  }
});

export function formatUpdateStamp(dateValue = new Date()) {
  const date = new Date(dateValue);
  return new Intl.DateTimeFormat('es-DO', {
    weekday: 'long',
    day: '2-digit',
    month: 'long',
    year: 'numeric',
    hour: '2-digit',
    minute: '2-digit'
  }).format(date);
}

export async function signInAnalyst(email, password) {
  const { data, error } = await supabase.auth.signInWithPassword({ email, password });
  if (error) throw error;
  return data;
}

export async function signOutAnalyst() {
  const { error } = await supabase.auth.signOut();
  if (error) throw error;
}

export async function getCurrentUser() {
  const { data, error } = await supabase.auth.getUser();
  if (error) return null;
  return data?.user ?? null;
}

export async function logUpdate({ source, section, status = 'success', fileName, rowsProcessed = 0, message, metadata = {} }) {
  const user = await getCurrentUser();
  const { data, error } = await supabase
    .from('data_update_log')
    .insert({
      source,
      section,
      status,
      file_name: fileName ?? null,
      rows_processed: rowsProcessed,
      message: message ?? null,
      metadata,
      updated_by: user?.id ?? null
    })
    .select()
    .single();
  if (error) throw error;
  return data;
}

export async function getLatestUpdate(source) {
  const { data, error } = await supabase
    .from('data_update_log')
    .select('*')
    .eq('source', source)
    .order('updated_at', { ascending: false })
    .limit(1)
    .maybeSingle();
  if (error) throw error;
  return data;
}

export async function upsertSibPeerGroup(rows, fileName) {
  const user = await getCurrentUser();
  const payload = rows.map(row => ({
    periodo: row.periodo,
    entidad: row.entidad,
    tipo_entidad: row.tipo_entidad ?? row.tipoEntidad ?? null,
    activos: numericOrNull(row.activos),
    morosidad: numericOrNull(row.morosidad),
    solvencia: numericOrNull(row.solvencia),
    cti: numericOrNull(row.cti),
    roe: numericOrNull(row.roe),
    roa: numericOrNull(row.roa),
    source_file: fileName,
    updated_by: user?.id ?? null,
    updated_at: new Date().toISOString()
  }));

  const { data, error } = await supabase
    .from('sib_peer_group')
    .upsert(payload, { onConflict: 'periodo,entidad' })
    .select();
  if (error) throw error;

  await logUpdate({
    source: 'SIB',
    section: 'Superintendencia · Peer Group',
    fileName,
    rowsProcessed: payload.length,
    message: 'CSV SIB transformado y publicado.'
  });

  return data;
}

export async function upsertSibSeriesLong(rows, fileName) {
  const user = await getCurrentUser();
  const payload = rows.map(row => ({
    periodo: row.periodo,
    entidad: row.entidad,
    tipo_entidad: row.tipo_entidad ?? row.tipoEntidad ?? null,
    indicador: row.indicador,
    valor: numericOrNull(row.valor),
    unidad: row.unidad ?? null,
    source_file: fileName,
    updated_by: user?.id ?? null,
    updated_at: new Date().toISOString(),
    metadata: row.metadata ?? {}
  }));

  const { data, error } = await supabase
    .from('sib_series_long')
    .upsert(payload, { onConflict: 'periodo,entidad,indicador' })
    .select();
  if (error) throw error;
  return data;
}

export async function getLatestSibPeerGroup() {
  const latest = await getLatestPeriod('sib_peer_group');
  if (!latest) return [];

  const { data, error } = await supabase
    .from('sib_peer_group')
    .select('*')
    .eq('periodo', latest)
    .order('activos', { ascending: false, nullsFirst: false });
  if (error) throw error;
  return data ?? [];
}

export async function getSibSeries({ entidad, indicador, months = 24 }) {
  const { data, error } = await supabase
    .from('sib_series_long')
    .select('periodo, entidad, indicador, valor, unidad')
    .eq('entidad', entidad)
    .eq('indicador', indicador)
    .order('periodo', { ascending: false })
    .limit(months);
  if (error) throw error;
  return (data ?? []).reverse();
}

export async function upsertBcrdSeries(rows, fileName) {
  const user = await getCurrentUser();
  const payload = rows.map(row => ({
    serie_id: row.serie_id,
    nombre: row.nombre,
    categoria: row.categoria ?? null,
    frecuencia: row.frecuencia ?? 'mensual',
    unidad: row.unidad ?? null,
    periodo: row.periodo,
    valor: numericOrNull(row.valor),
    source_file: fileName,
    updated_by: user?.id ?? null,
    updated_at: new Date().toISOString(),
    metadata: row.metadata ?? {}
  }));

  const { data, error } = await supabase
    .from('bcrd_series')
    .upsert(payload, { onConflict: 'serie_id,periodo' })
    .select();
  if (error) throw error;

  await logUpdate({
    source: 'BCRD',
    section: 'Macro · Banco Central',
    fileName,
    rowsProcessed: payload.length,
    message: 'Excel/CSV BCRD transformado y publicado.'
  });

  return data;
}

export async function saveNewsSources(sources) {
  const payload = sources.map(source => ({
    name: source.name,
    source_key: source.source_key,
    url: source.url,
    category: source.category ?? 'Economía',
    enabled: source.enabled ?? true,
    updated_at: new Date().toISOString()
  }));

  const { data, error } = await supabase
    .from('news_sources')
    .upsert(payload, { onConflict: 'source_key' })
    .select();
  if (error) throw error;
  return data;
}

export async function upsertNewsItems(items) {
  const { data, error } = await supabase
    .from('news_items')
    .upsert(items, { onConflict: 'url' })
    .select();
  if (error) throw error;

  await logUpdate({
    source: 'NEWS',
    section: 'Resumen de Noticias',
    rowsProcessed: items.length,
    message: 'Noticias consultadas y enviadas a cola editorial.'
  });

  return data;
}

export async function getNewsQueue(status = 'pending') {
  const { data, error } = await supabase
    .from('news_items')
    .select('*')
    .eq('status', status)
    .order('published_at', { ascending: false, nullsFirst: false })
    .order('created_at', { ascending: false });
  if (error) throw error;
  return data ?? [];
}

export async function approveNewsItem(id) {
  const user = await getCurrentUser();
  const { data, error } = await supabase
    .from('news_items')
    .update({
      status: 'approved',
      approved_at: new Date().toISOString(),
      approved_by: user?.id ?? null,
      updated_at: new Date().toISOString()
    })
    .eq('id', id)
    .select()
    .single();
  if (error) throw error;
  return data;
}

export async function rejectNewsItem(id) {
  const user = await getCurrentUser();
  const { data, error } = await supabase
    .from('news_items')
    .update({
      status: 'rejected',
      rejected_at: new Date().toISOString(),
      rejected_by: user?.id ?? null,
      updated_at: new Date().toISOString()
    })
    .eq('id', id)
    .select()
    .single();
  if (error) throw error;
  return data;
}

async function getLatestPeriod(tableName) {
  const { data, error } = await supabase
    .from(tableName)
    .select('periodo')
    .order('periodo', { ascending: false })
    .limit(1)
    .maybeSingle();
  if (error) throw error;
  return data?.periodo ?? null;
}

function numericOrNull(value) {
  if (value === null || value === undefined || value === '') return null;
  if (typeof value === 'number') return Number.isFinite(value) ? value : null;
  const normalized = String(value)
    .replace(/%/g, '')
    .replace(/,/g, '')
    .trim();
  const number = Number(normalized);
  return Number.isFinite(number) ? number : null;
}
