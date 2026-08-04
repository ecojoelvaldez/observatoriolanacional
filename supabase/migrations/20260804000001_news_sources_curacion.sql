-- ===========================================================================
-- Curación de las fuentes de noticias
--
-- Motivo: de las 12 fuentes activas, el pipeline solo consiguió sacar noticias
-- de 4 (hoy.com.do, El Dinero, Diario Libre y El Caribe). Las demás llevaban
-- 14 corridas seguidas — todo el histórico registrado en news_candidates.json —
-- sin aportar una sola nota. Una fuente que nunca rinde no es neutral: gasta
-- una lectura, una llamada a Gemini y minutos de la corrida, y encima da la
-- falsa sensación de cobertura amplia.
--
-- Lo que hace esta migración:
--   1. Añade `url_alterna`: segunda ruta que el pipeline prueba cuando el
--      índice principal no se deja leer. Es para las institucionales (BCRD,
--      SIB, Hacienda), que son las que más importan y peor se leen — ahí el
--      problema no es la fuente, es la página.
--   2. Apaga (no borra) las fuentes que nunca rindieron y no valen la pena.
--   3. Da de alta las sustitutas.
--   4. Arregla los nombres: cuatro fuentes se guardaron desde el panel sin
--      nombre y quedaron como "Fuente guardada", así que en el panel el
--      analista no sabía de qué medio venía la mitad de las propuestas.
--
-- Nada se borra: apagar deja el histórico y permite revertir con un update.
-- ===========================================================================

alter table public.news_sources
  add column if not exists url_alterna text;

comment on column public.news_sources.url_alterna is
  'Ruta de respaldo que el pipeline lee si el índice principal no abre (scripts/news_pipeline.py).';

-- ---------------------------------------------------------------------------
-- 1. Nombres reales en lugar de "Fuente guardada"
-- ---------------------------------------------------------------------------
update public.news_sources set name = 'Hoy Digital · Economía'      where source_key = 'news_extra_9';
update public.news_sources set name = 'Ministerio de Hacienda'      where source_key = 'news_extra_10';
update public.news_sources set name = 'El Caribe · Dinero'          where source_key = 'news_extra_12';

-- ---------------------------------------------------------------------------
-- 2. Institucionales: se quedan, con ruta de respaldo
--    Son las fuentes de mayor valor del observatorio (lo que publica el
--    regulador no lo publica nadie más), así que no se sustituyen por no
--    dejarse raspar: se les da una segunda puerta y además el barrido web ya
--    pregunta por ellas en cada corrida.
-- ---------------------------------------------------------------------------
update public.news_sources
   set url_alterna = 'https://www.bancentral.gov.do/'
 where source_key = 'news_bcrd';

update public.news_sources
   set url_alterna = 'https://sb.gob.do/'
 where source_key = 'news_sib';

update public.news_sources
   set url_alterna = 'https://www.hacienda.gob.do/'
 where source_key = 'news_extra_10';

-- ---------------------------------------------------------------------------
-- 3. Sustituciones
-- ---------------------------------------------------------------------------

-- Bloomberg Línea: 0 noticias en 14 corridas. Muro de pago y bloqueo al
-- lector; la nota existe pero el pipeline nunca puede leer el cuerpo.
-- Entra en su lugar Revista Mercado, que cubre banca y finanzas dominicanas
-- con la misma vocación de negocios y sin muro.
update public.news_sources set enabled = false where source_key = 'news_bloomberg';

-- Acento · Economía: 0 noticias en 14 corridas, y su sección de economía es
-- mayormente columna de autor — justo lo que la política editorial nueva veta.
-- Entra El Nuevo Diario, que publica economía como noticia dura.
update public.news_sources set enabled = false where source_key = 'news_acento';

-- FXStreet filtrado por "Oro" + etiqueta Fed: 0 noticias en 14 corridas, y la
-- consulta está fuera del mandato (el precio del oro no mueve el balance de una
-- asociación de ahorros y préstamos). Lo que sí lo mueve es la decisión de la
-- Fed, así que entra la fuente primaria: los comunicados de política monetaria.
update public.news_sources set enabled = false where source_key = 'news_extra_11';

insert into public.news_sources (source_key, name, url, url_alterna, category, enabled)
values
  ('news_mercado',      'Revista Mercado · Finanzas',
   'https://revistamercado.do/market-brief/finanzas/', 'https://revistamercado.do/', 'Economía', true),
  ('news_nuevodiario',  'El Nuevo Diario · Economía',
   'https://elnuevodiario.com.do/economia/', null, 'Economía', true),
  ('news_fed',          'Reserva Federal · Política monetaria',
   'https://www.federalreserve.gov/newsevents/pressreleases/monetary.htm',
   'https://www.federalreserve.gov/newsevents/pressreleases.htm', 'Global', true)
on conflict (source_key) do update
   set name        = excluded.name,
       url         = excluded.url,
       url_alterna = excluded.url_alterna,
       category    = excluded.category,
       enabled     = true,
       updated_at  = now();

-- ---------------------------------------------------------------------------
-- 4. Listín Diario queda EN OBSERVACIÓN, no se sustituye todavía.
--    También lleva 14 corridas en cero, pero es uno de los tres diarios de
--    mayor circulación del país: si el problema es la ruta y no el medio,
--    cambiarlo sería perder cobertura real. Se le da ruta alterna y el
--    histórico data/news_source_health.json dirá en dos semanas si rinde o no,
--    con el mismo criterio que se aplicó aquí.
-- ---------------------------------------------------------------------------
update public.news_sources
   set url_alterna = 'https://listindiario.com/la-republica'
 where source_key = 'news_listin';

-- Duplicados históricos ya apagados: se dejan como están (bcrd, bloomberg_linea,
-- news_elcaribe, news_eldinero_fin, news_diariolibre_fin) para no perder el
-- rastro de qué se probó y cuándo.
