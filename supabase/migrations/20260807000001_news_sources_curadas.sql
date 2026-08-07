-- =====================================================================
-- Fuentes de noticias · depuración de la lista que lee el pipeline
-- =====================================================================
--
-- Motivo. Sobre las últimas 19 corridas del pipeline automático (historia
-- de news_candidates.json) solo cuatro dominios devolvieron titulares:
--
--     hoy.com.do          143 candidatas
--     eldinero.com.do     132
--     www.diariolibre.com  46
--     www.elcaribe.com.do  21
--
-- Las demás fuentes habilitadas —BCRD, Superintendencia, Listín Diario,
-- Acento, Bloomberg Línea, Ministerio de Hacienda, Reserva Federal,
-- Revista Mercado y El Nuevo Diario— nunca devolvieron una sola nota: sus
-- portadas se arman con JavaScript, así que el lector recibe el cascarón
-- del HTML sin enlaces. Mantenerlas habilitadas solo gasta lecturas y
-- llena el log de descartes.
--
-- Qué hace esta migración:
--   1. Apaga todas las fuentes vigentes.
--   2. Vuelve a habilitar únicamente los cuatro dominios productivos, sus
--      feeds RSS (que sí se parsean cuando la portada falla) y dos
--      búsquedas de Google News que recuperan la cobertura institucional
--      del Banco Central y la Superintendencia por un canal legible.
--
-- Nada se borra: las retiradas quedan con enabled = false, así que
-- volver a encender cualquiera es un UPDATE de una línea.
--
-- Esta lista es la misma que el panel del analista trae por defecto
-- (ver RECOMMENDED_SOURCES en index.html), de modo que el flujo manual y
-- el automático leen exactamente lo mismo.
-- =====================================================================

update public.news_sources
   set enabled = false,
       updated_at = now()
 where enabled = true;

insert into public.news_sources (source_key, name, url, category, enabled, updated_at) values
  ('news_eldinero',             'El Dinero · Finanzas',      'https://eldinero.com.do/finanzas/',                    'Economía',  true, now()),
  ('news_eldinero_rss',         'El Dinero · RSS',           'https://eldinero.com.do/feed/',                        'Economía',  true, now()),
  ('news_hoy',                  'Hoy Digital · Economía',    'https://hoy.com.do/economia/',                         'Economía',  true, now()),
  ('news_hoy_rss',              'Hoy Digital · RSS',         'https://hoy.com.do/feed/',                             'Economía',  true, now()),
  ('news_diariolibre',          'Diario Libre · Economía',   'https://www.diariolibre.com/economia',                 'Economía',  true, now()),
  ('news_diariolibre_finanzas', 'Diario Libre · Finanzas',   'https://www.diariolibre.com/economia/finanzas',        'Economía',  true, now()),
  ('news_elcaribe',             'El Caribe · Dinero',        'https://www.elcaribe.com.do/seccion/panorama/dinero/', 'Economía',  true, now()),
  ('news_elcaribe_rss',         'El Caribe · RSS',           'https://www.elcaribe.com.do/feed/',                    'Economía',  true, now()),
  ('news_gnews_bcrd',           'Google News · BCRD y política monetaria',
     'https://news.google.com/rss/search?q=%22Banco+Central%22+Rep%C3%BAblica+Dominicana+inflaci%C3%B3n+OR+tasa+OR+remesas&hl=es-419&gl=DO&ceid=DO:es-419',
     'Monetario', true, now()),
  ('news_gnews_banca',          'Google News · Banca y Superintendencia',
     'https://news.google.com/rss/search?q=%22Superintendencia+de+Bancos%22+OR+%22banca+dominicana%22+OR+%22asociaciones+de+ahorros+y+pr%C3%A9stamos%22&hl=es-419&gl=DO&ceid=DO:es-419',
     'Economía', true, now())
on conflict (source_key) do update
   set name       = excluded.name,
       url        = excluded.url,
       category   = excluded.category,
       enabled    = true,
       updated_at = now();
