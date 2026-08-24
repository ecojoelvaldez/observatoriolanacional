"""
Observatorio Estratégico La Nacional — Pipeline de Noticias v4 LOCAL/STATIC
============================================================================
Flujo:
  1. Lee fuentes activas desde Supabase (news_sources). Esto es liviano.
  2. Titulares de cada fuente, por este orden:
     a. El RSS del propio medio: título, enlace y fecha real, sin modelo y sin
        lector intermedio (NEWS_RSS_FIRST).
     b. Si no publica feed, la página índice con Jina Reader y Gemini
        extrayendo las URLs, que es como se hacía antes.
  3. Con la fecha del feed por delante, lo que cae fuera del día editorial se
     descarta AQUÍ, sin gastar lectura ni llamada al modelo.
  4. Para cada artículo, lee la URL individual con Jina (en paralelo, 3 hilos).
     Si esa URL no abre, se busca el mismo titular en la web y se lee la
     cobertura de otro medio.
  5. Gemini genera resumen ejecutivo + lead + relevancia financiera (0-10).
  6. Se aceptan candidatos con relevancia >= NEWS_MIN_RELEVANCE, con un PISO
     institucional que el modelo no puede bajar (ver SEÑALES_RELEVANCIA).
  7. Barrido propio en la web sobre los temas que importan, además de las
     fuentes fijas, con el cupo que sobre.
  8. Se colapsan las notas que cuentan la misma noticia desde distintos medios.
  9. Escribe un archivo estático news_candidates.json en la raíz del repo, con
     los candidatos Y todo lo descartado junto con su motivo, para que el panel
     lo muestre y el analista pueda recuperarlo.

La cola es de UN DÍA. Cada corrida reconstruye news_candidates.json para el día
editorial de Santo Domingo (UTC-4) y bota lo que ya no cae en la ventana, tanto
lo que trae esta corrida como lo arrastrado de la anterior. La ventana se mide
en días calendario locales (NEWS_MAX_AGE_DAYS), no en horas rodantes: con horas,
una nota de anteayer seguía contando como "de hace 71 horas" y el 19 de agosto
todavía salían propuestas del 14.

Redes de seguridad, todas por la misma razón: que ninguna noticia importante
obligue a buscarla a mano.
  * Piso de relevancia por señal institucional — el juicio del modelo no es el
    único filtro. Reguladores, política monetaria y bancos dominicanos entran
    aunque el modelo los haya puntuado bajo o marcado como no noticiosos.
  * Mención de una entidad financiera dominicana = entra, punto. Basta que el
    nombre aparezca (ENTIDADES_FINANCIERAS_RD), aunque la nota no sea de
    economía: patrocinios, nombramientos, litigios o actos sociales de un banco
    o una asociación también son seguimiento. Es el único piso que llega a 10.
  * Registro de descartes — nada desaparece en silencio.
  * Rescate por titular — que un medio bloquee al lector no borra la noticia.
  * Búsqueda general — no depender de que la nota salga en un índice concreto.
  * Deduplicación por tema — la misma noticia desde dos medios es una sola.
  * Filtro de páginas inválidas — errores 404 y muros de pago no son noticia.
  * Fecha desde la URL — antes de descartar por falta de fecha se lee la ruta
    del artículo (/2026/08/19/...), que es donde el CMS casi siempre la deja.
  * Purga aunque no haya nada nuevo — si no hay fuentes o ninguna nota pasa el
    filtro, el JSON se reescribe igual: el panel muestra "hoy sin propuestas",
    nunca la cola de ayer.
  * Corta-circuito de cuota — si Gemini se agota, dejar de pedirle llamadas.
  * Ficha sin modelo — agotada la cuota, la nota entra igual: título y fecha del
    feed, cuerpo leído con Jina y relevancia por las mismas reglas
    institucionales. Queda marcada `sin_ia` y el panel la muestra como
    "sin IA · revisar" (NEWS_FALLBACK_SIN_IA). Antes la corrida se cortaba y la
    cola salía a medias: el 24 de agosto, 8 notas de 19 fuentes.
  * Tres buscadores sin API key — gdelt, googlenews y jina, en el orden de
    NEWS_SEARCH_PROVIDERS. El que falla se saca de la corrida y se pasa al
    siguiente, así que ninguno es un punto único de fallo.

El analista carga ese JSON desde el panel, lo guarda en localStorage y aprueba/rechaza localmente.
Solo las noticias publicadas se guardan en Supabase (news_items).

Variables requeridas:
  SUPABASE_URL
  SUPABASE_SERVICE_KEY
  GEMINI_API_KEY

Variables opcionales:
  GEMINI_MODEL=gemini-2.5-flash-lite
  NEWS_CANDIDATES_PATH=news_candidates.json
  MAX_TOTAL_PROPOSALS=30
  NEWS_TZ_OFFSET_HOURS=-4     # Santo Domingo, sin horario de verano
  NEWS_MAX_AGE_DAYS=1         # dias calendario locales: 0=hoy, 1=hoy y ayer
  NEWS_ALLOW_UNDATED=false    # dejar pasar notas sin fecha verificable
  CANDIDATE_TTL_DAYS=1        # dias editoriales que sobrevive la cola
  NEWS_MIN_RELEVANCE=6        # umbral 0-10 de relevancia financiera
  ARTICLE_WORKERS=3           # hilos por fuente para leer articulos
  NEWS_SEARCH_ENABLED=true    # barrido propio en la web ademas de las fuentes
  NEWS_SEARCH_PROVIDERS=gdelt,googlenews,jina   # buscadores, en orden; sin API key
  GDELT_SOURCE_COUNTRY=DR     # pais de publicacion que pide GDELT
  GDELT_SOURCE_LANG=spanish   # idioma que pide GDELT
  NEWS_RSS_FIRST=true         # titulares del RSS del medio antes que del modelo
  NEWS_FALLBACK_SIN_IA=true   # sin cuota de Gemini, ficha por reglas en vez de nada
  RSS_MAX_ITEMS=25            # cuantos items se leen de cada feed
  NEWS_SEARCH_QUERIES=...     # consultas separadas por "|" (ver DEFAULT_SEARCH_QUERIES)
  NEWS_SEARCH_MAX_PER_QUERY=4
  NEWS_RESCUE_ENABLED=true    # buscar el titular en otro medio si la URL no abre
"""

from __future__ import annotations

import os
import re
import json
import time
import hashlib
import html
import logging
import threading
import unicodedata
from pathlib import Path
from datetime import date, datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse, urljoin, parse_qsl, urlencode, urlunparse, quote_plus
from xml.etree import ElementTree

import httpx

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("news_pipeline_static")

SUPABASE_URL = os.environ["SUPABASE_URL"].rstrip("/")
SUPABASE_SERVICE_KEY = os.environ["SUPABASE_SERVICE_KEY"]
GEMINI_API_KEY = os.environ["GEMINI_API_KEY"]
GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-2.5-flash-lite")

NEWS_CANDIDATES_PATH = Path(os.getenv("NEWS_CANDIDATES_PATH", "news_candidates.json"))

JINA_READER_BASE = "https://r.jina.ai/"
GEMINI_ENDPOINT = f"https://generativelanguage.googleapis.com/v1beta/models/{GEMINI_MODEL}:generateContent"

FETCH_TIMEOUT = 35
GEMINI_TIMEOUT = 45
MAX_INDEX_MARKDOWN_CHARS = 9000
MAX_ARTICLE_MARKDOWN_CHARS = 12000
MIN_CONTENT_CHARS = 250
DELAY_BETWEEN_SOURCES = 1.5
MAX_HEADLINES_PER_SOURCE = 6
MAX_TOTAL_PROPOSALS = int(os.getenv("MAX_TOTAL_PROPOSALS", "30"))

# --- El dia editorial manda -------------------------------------------------
# El observatorio es dominicano: el "hoy" del analista es el de Santo Domingo,
# UTC-4 todo el año (el pais no mueve el reloj). Medir la frescura en horas
# rodantes sobre UTC hacia que el corte del dia cayera a las 8:00 p.m. locales
# y que una nota de anteayer siguiera contando como "de hace 71 horas".
NEWS_TZ = timezone(timedelta(hours=int(os.getenv("NEWS_TZ_OFFSET_HOURS", "-4"))))
# Ventana de frescura en DIAS CALENDARIO locales, no en horas.
#   0 -> solo lo publicado hoy
#   1 -> hoy y ayer (default: la corrida de las 9:00 a.m. tiene que traer lo
#        que los medios publicaron anoche)
NEWS_MAX_AGE_DAYS = int(os.getenv("NEWS_MAX_AGE_DAYS", "1"))
# Un articulo sin fecha legible no se puede garantizar fresco. Se descarta
# (queda visible y recuperable en el panel de descartadas) en vez de colarse
# en la cola con fecha desconocida.
NEWS_ALLOW_UNDATED = os.getenv("NEWS_ALLOW_UNDATED", "false").lower() == "true"
# Cuantos dias editoriales sobrevive la cola dentro del JSON.
#   1 -> la cola se vacia sola cada dia (default)
# Estuvo en 7 y por eso el 19 de agosto seguian saliendo propuestas del 14:
# un candidato leido el 17 se quedaba una semana en el archivo.
CANDIDATE_TTL_DAYS = max(1, int(os.getenv("CANDIDATE_TTL_DAYS", "1")))
# Cuantos candidatos se conservan en el JSON. Con la cola de un solo dia
# alcanza para las dos corridas diarias sin desalojar nada del mismo dia.
MAX_STORED_CANDIDATES = int(os.getenv("MAX_STORED_CANDIDATES", "45"))
# Los descartes viven lo mismo que la cola: son el reverso del mismo dia.
DISCARD_TTL_DAYS = max(1, int(os.getenv("DISCARD_TTL_DAYS", "1")))
MAX_STORED_DISCARDS = int(os.getenv("MAX_STORED_DISCARDS", "60"))
NEWS_MIN_RELEVANCE = int(os.getenv("NEWS_MIN_RELEVANCE", "6"))
ARTICLE_WORKERS = max(1, int(os.getenv("ARTICLE_WORKERS", "3")))
# El free tier de Gemini limita las peticiones por minuto; espaciar las
# llamadas evita tormentas de 429 que agotan los reintentos y dejan fuentes
# sin procesar (observado en el run del 2026-07-06).
GEMINI_MIN_INTERVAL = float(os.getenv("GEMINI_MIN_INTERVAL", "6"))

CATEGORY_ALLOWED = {"Monetario", "Financiero", "Regulatorio", "Economia", "Global"}
TRACKING_PARAMS = {"utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content", "fbclid", "gclid", "mc_cid", "mc_eid"}

# --- Busqueda general en la web --------------------------------------------
# Barrido propio ademas de las fuentes fijas, para no depender de que la nota
# aparezca en un indice concreto.
#
# Tres buscadores, todos SIN API key y SIN cuota diaria que administrar, que se
# prueban en el orden de NEWS_SEARCH_PROVIDERS hasta que uno responda:
#
#   gdelt      api.gdeltproject.org — indice academico de prensa mundial, 65
#              idiomas. Devuelve URL directa del medio, dominio y fecha, en
#              JSON. Filtra por pais de publicacion (sourcecountry:DR) y por
#              idioma (sourcelang:spanish). Sin key, sin registro, sin tope
#              documentado: es el reemplazo natural de s.jina.ai.
#   googlenews news.google.com/rss/search — el buscador de Google News como
#              RSS. Tampoco pide key. Cubre medios dominicanos que GDELT tarda
#              en indexar, pero entrega enlaces envueltos en news.google.com
#              que hay que resolver, y a veces no se dejan.
#   jina       s.jina.ai — el que ya estaba. Queda de ultimo porque es el que
#              mas limita por tasa cuando el runner comparte IP.
#
# Ninguno gasta cuota de Gemini: buscar y leer nunca fue el problema de cuota.
GDELT_SEARCH_BASE = "https://api.gdeltproject.org/api/v2/doc/doc"
GOOGLE_NEWS_RSS_BASE = "https://news.google.com/rss/search"
JINA_SEARCH_BASE = "https://s.jina.ai/"
NEWS_SEARCH_PROVIDERS = [
    p.strip().lower()
    for p in os.getenv("NEWS_SEARCH_PROVIDERS", "gdelt,googlenews,jina").split(",")
    if p.strip()
]
# GDELT indexa por pais de publicacion; DR es Republica Dominicana.
GDELT_SOURCE_COUNTRY = os.getenv("GDELT_SOURCE_COUNTRY", "DR").strip()
GDELT_SOURCE_LANG = os.getenv("GDELT_SOURCE_LANG", "spanish").strip()
NEWS_SEARCH_ENABLED = os.getenv("NEWS_SEARCH_ENABLED", "true").strip().lower() not in ("0", "false", "no")
NEWS_SEARCH_MAX_PER_QUERY = int(os.getenv("NEWS_SEARCH_MAX_PER_QUERY", "4"))
DEFAULT_SEARCH_QUERIES = [
    "Superintendencia de Bancos República Dominicana noticias",
    "Banco Central República Dominicana tasa de política monetaria",
    "banca dominicana créditos depósitos noticias",
    "asociaciones de ahorros y préstamos República Dominicana",
    "economía dominicana inflación remesas tipo de cambio",
]
NEWS_SEARCH_QUERIES = [
    q.strip() for q in os.getenv("NEWS_SEARCH_QUERIES", "|".join(DEFAULT_SEARCH_QUERIES)).split("|") if q.strip()
]
# Recuperar por titular cuando no se puede leer el cuerpo en la fuente original.
NEWS_RESCUE_ENABLED = os.getenv("NEWS_RESCUE_ENABLED", "true").strip().lower() not in ("0", "false", "no")

# --- Titulares por RSS antes que por modelo --------------------------------
# Sacar los titulares de una pagina indice era una llamada a Gemini por fuente:
# 19 fuentes = 19 llamadas gastadas en leer una lista de enlaces, antes de
# resumir un solo articulo. En la corrida del 2026-08-24 la cuota se agoto a la
# septima fuente y las 10 restantes se quedaron sin procesar.
#
# El RSS del propio medio da lo mismo gratis y mejor: titulo, enlace y fecha de
# publicacion REAL, sin modelo y sin lector intermedio. Con la fecha por
# delante, ademas, lo que cae fuera del dia editorial se descarta ANTES de
# gastar una lectura y una llamada por articulo (otras 4 llamadas perdidas en
# esa misma corrida).
#
# Gemini queda para lo unico que un parser no sabe hacer: juzgar si la nota le
# importa a la entidad y resumirla.
NEWS_RSS_FIRST = os.getenv("NEWS_RSS_FIRST", "true").strip().lower() not in ("0", "false", "no")
# Rutas convencionales de feed, en orden, cuando el HTML no declara ninguna.
# La lista es corta a proposito: son 19 fuentes y probar diez rutas en cada una
# se come el timeout del job antes de leer una sola noticia.
RSS_PATHS_COMUNES = ("feed/", "rss/", "rss.xml", "index.xml", "atom.xml")
# Presupuesto de descubrimiento por fuente. Pasado esto se abandona el feed y se
# usa el camino de siempre (Jina + Gemini), que al menos avanza.
RSS_DISCOVERY_BUDGET_S = float(os.getenv("RSS_DISCOVERY_BUDGET_S", "20"))
RSS_MAX_ITEMS = int(os.getenv("RSS_MAX_ITEMS", "25"))

# --- Ultimo recurso: candidatos sin Gemini ---------------------------------
# Cuando la cuota se agota a mitad de corrida, la alternativa era entregar la
# cola a medias. Con esto la nota entra igual: titulo y fecha del RSS, cuerpo
# leido con Jina, y relevancia por las mismas reglas institucionales que ya
# mandan sobre el modelo (SEÑALES_RELEVANCIA). Queda marcada `sin_ia` para que
# el analista sepa que ese resumen no paso por modelo.
NEWS_FALLBACK_SIN_IA = os.getenv("NEWS_FALLBACK_SIN_IA", "true").strip().lower() not in ("0", "false", "no")

# --- Piso de relevancia por señal institucional ----------------------------
# El modelo venia descartando noticias que para una entidad financiera son
# obvias (la renuncia del Superintendente de Bancos, una reunion entre
# Banreservas y Popular sobre el sistema financiero). El juicio del modelo no
# puede ser el unico filtro: estas reglas fijan un piso de relevancia que el
# modelo no puede bajar. Si el texto toca una de estas señales, la nota entra.
# ---------------------------------------------------------------------------
# Entidades financieras dominicanas.
# Regla de negocio, dicha por Planeacion Estrategica: una nota que MENCIONE a
# cualquiera de estas entra en la cola, aunque la mencion sea de pasada y la
# nota no sea de economia (un patrocinio, un nombramiento, un litigio, una
# donacion). Para una entidad supervisada, lo que se dice de sus pares es
# seguimiento obligatorio, no color.
#
# Los terminos van sin acentos y en minuscula porque se comparan contra
# norm_texto(); y se comparan por PALABRA COMPLETA (ver _menciona), no por
# subcadena: con subcadena, "sib" hacia match dentro de "posible" y casi
# cualquier nota en español disparaba el piso institucional.
# ---------------------------------------------------------------------------
ENTIDADES_FINANCIERAS_RD = [
    # Asociaciones de ahorros y prestamos
    "asociacion la nacional", "la nacional de ahorros y prestamos", "alnap",
    "asociacion popular de ahorros y prestamos", "asociacion popular", "apap",
    "asociacion cibao", "asociacion duarte", "asociacion peravia",
    "asociacion mocana", "asociacion bonao", "asociacion romana",
    "asociacion higuamo", "asociacion maguana", "asociacion barahona",
    "asociacion noroestana", "asociacion la vega real",
    "asociaciones de ahorros y prestamos", "liga dominicana de asociaciones",
    "lidaapi",
    # Bancos multiples
    "banreservas", "banco de reservas", "banco popular dominicano",
    "banco popular", "popular bank", "banco bhd", "bhd leon", "scotiabank",
    "banesco", "banco santa cruz", "banco bdi", "banco caribe",
    "banco promerica", "promerica", "banco lafise", "lafise", "banco vimenca",
    "vimenca", "banco lopez de haro", "bancamerica", "citibank",
    "banco ademi", "ademi", "qik banco digital", "qik",
    "banco multiple", "bancos multiples", "banca multiple",
    # Bancos de ahorro y credito, corporaciones y banca publica
    "banco adopem", "adopem", "banfondesa", "fondesa", "bancotui",
    "banco confisa", "confisa", "motor credito", "jmmb", "banco atlantico",
    "banco agricola", "banco nacional de las exportaciones", "bandex",
    "bancos de ahorro y credito", "corporaciones de credito",
    # Cooperativas de mayor tamaño
    "cooperativa vega real", "coopvegareal", "coopnama", "coopsano",
    "cooperativa san jose", "cooperativa herrera", "cooperativa maimon",
    "cooperativas de ahorro y credito",
    # Pensiones
    "afp popular", "afp reservas", "afp siembra", "afp crecer",
    "afp romana", "afp atlantico", "administradoras de fondos de pensiones",
    "fondos de pensiones", "sipen",
    # Seguros y ARS
    "seguros universal", "seguros reservas", "seguros banreservas",
    "mapfre bhd", "mapfre", "humano seguros", "seguros humano", "ars humano",
    "seguros la colonial", "la colonial de seguros", "seguros sura",
    "seguros worldwide", "ars palic", "palic", "senasa",
    # Mercado de valores
    "bolsa y mercados de valores", "bolsa de valores", "bvrd", "cevaldom",
    "puesto de bolsa", "puestos de bolsa", "alpha inversiones",
    "parallax valores", "excel puesto de bolsa", "jmmb puesto de bolsa",
    # Gremios del sistema financiero
    "asociacion de bancos comerciales", "asociacion de bancos multiples",
    "abancord", "adocoop", "adobanca",
]

SEÑALES_RELEVANCIA = [
    # (relevancia minima, etiqueta, terminos)
    # Piso 10: mencion de una entidad financiera dominicana. Es el unico grupo
    # que llega al tope, y a proposito: aqui la mencion basta, no hace falta
    # que la nota trate de finanzas.
    (10, "entidad_rd", ENTIDADES_FINANCIERAS_RD),
    (9, "regulador", [
        "superintendencia de bancos", "superintendente de bancos", "superintendente",
        "banco central", "gobernador del banco central", "junta monetaria",
        "superintendencia de valores", "superintendencia de seguros", "superintendencia de pensiones",
        "sib", "bcrd", "ministerio de hacienda", "dgii", "conep",
    ]),
    (9, "politica_monetaria", [
        "tasa de politica monetaria", "politica monetaria", "encaje legal",
        "tasa de interes", "tasas de interes", "inflacion", "indice de precios",
        "tipo de cambio", "devaluacion", "liquidez",
    ]),
    (8, "banca", [
        "banreservas", "banco popular", "banco bhd", "scotiabank", "banesco",
        "banco santa cruz", "banco caribe", "banco promerica", "asociacion popular",
        "apap", "asociacion cibao", "asociacion la nacional", "la nacional",
        "asociaciones de ahorros y prestamos", "banca multiple", "sistema financiero",
        "morosidad", "cartera de credito", "captaciones", "hipotecas", "prestamos",
        "solvencia", "provisiones", "intermediacion financiera",
    ]),
    (8, "macro", [
        "producto interno bruto", "pib", "imae", "remesas", "deuda publica",
        "inversion extranjera", "calificacion crediticia", "riesgo pais",
        "presupuesto complementario", "reservas internacionales",
    ]),
]
# Verbos que convierten una mencion institucional en un hecho noticioso fuerte.
VERBOS_HECHO = [
    "renuncia", "renuncio", "dimite", "dimitio", "designa", "designo", "designado",
    "nombra", "nombro", "nombrado", "juramenta", "destituye", "sustituye", "releva",
    "anuncia", "anuncio", "aprueba", "aprobo", "resolucion", "circular", "reglamento",
    "sanciona", "multa", "interviene", "fusion", "adquisicion", "acuerdo", "reunion",
    "se reunio", "reunieron", "reunen", "reune", "firma", "firmaron", "lanza", "presenta",
    "advierte", "alerta", "eleva", "reduce", "recorta", "sube", "baja",
]

# Paginas que no son noticia aunque el lector devuelva texto: errores, muros de
# pago y avisos de cookies. Sin este filtro se colaban al panel — en el JSON del
# 2026-07-30 entro "Error 404 | Hoy Digital" con relevancia 7, y el propio
# resumen del modelo decia que la pagina no existia.
PATRONES_PAGINA_INVALIDA = [
    "error 404", "404 not found", "pagina no encontrada", "page not found",
    "no se encontro la pagina", "contenido no disponible", "acceso denegado",
    "access denied", "403 forbidden", "suscribete para continuar",
    "solo para suscriptores", "inicia sesion para continuar",
    "habilita javascript", "enable javascript", "verificando que eres humano",
    "are you a robot", "attention required", "service unavailable",
]

# Palabras vacias para comparar titulares (deteccion de duplicados).
STOPWORDS = {
    "a", "al", "ante", "con", "como", "de", "del", "desde", "el", "en", "entre", "es",
    "esta", "este", "hacia", "hasta", "la", "las", "lo", "los", "mas", "no", "para",
    "per", "por", "que", "se", "segun", "ser", "si", "sin", "sobre", "son", "su", "sus",
    "un", "una", "uno", "unos", "unas", "y", "e", "o", "u", "tras", "the", "of", "and",
    "hoy", "ayer", "nueva", "nuevo", "tambien",
}

SUPABASE_HEADERS = {
    "apikey": SUPABASE_SERVICE_KEY,
    "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
    "Content-Type": "application/json",
}


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def stable_id(url: str) -> str:
    return "n_" + hashlib.sha1((url or "").encode("utf-8")).hexdigest()[:16]


# ---------------------------------------------------------------------------
# Normalizacion de texto: comparar titulares y buscar señales sin que acentos
# ni mayusculas cambien el resultado.
# ---------------------------------------------------------------------------
def norm_texto(valor: str) -> str:
    txt = unicodedata.normalize("NFD", str(valor or "").lower())
    txt = "".join(c for c in txt if unicodedata.category(c) != "Mn")
    return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9ñ ]+", " ", txt)).strip()


def limpiar_texto(valor: str) -> str:
    """Texto listo para mostrar: sin entidades HTML ni espacios de sobra.

    Los titulares llegaban con entidades crudas del CMS y se publicaban asi
    ("sector el&eacute;ctrico" en el JSON del 2026-07-30).
    """
    txt = html.unescape(str(valor or ""))
    return re.sub(r"\s+", " ", txt).strip()


def es_pagina_invalida(*textos: str) -> bool:
    """True si el texto es una pagina de error, un muro de pago o un captcha."""
    blob = norm_texto(" ".join(t for t in textos if t)[:1200])
    return any(p in blob for p in PATRONES_PAGINA_INVALIDA)


def tokens_titular(titulo: str) -> set[str]:
    """Tokens significativos de un titular, sin palabras vacias ni ruido corto."""
    return {t for t in norm_texto(titulo).split() if len(t) > 2 and t not in STOPWORDS}


def _menciona(blob_acolchado: str, termino: str) -> bool:
    """True si el termino aparece como palabra completa dentro del texto.

    Se compara contra un blob ya normalizado y rodeado de espacios, asi que
    basta con buscar el termino tambien rodeado de espacios. La variante con
    "s" final cubre el plural sin necesidad de listar las dos formas.

    Antes esto era `term in blob`, una subcadena: "sib" hacia match dentro de
    "posible" y el piso institucional de 9 se disparaba en casi cualquier nota.
    """
    return f" {termino} " in blob_acolchado or f" {termino}s " in blob_acolchado


def señales_detectadas(*textos: str) -> tuple[int, list[str]]:
    """Piso de relevancia y etiquetas segun las señales institucionales.

    Devuelve (piso, etiquetas). piso = 0 si el texto no toca ninguna señal.
    """
    blob = norm_texto(" ".join(t for t in textos if t))
    if not blob:
        return 0, []
    acolchado = f" {blob} "
    piso, etiquetas = 0, []
    for minimo, etiqueta, terminos in SEÑALES_RELEVANCIA:
        if any(_menciona(acolchado, term) for term in terminos):
            etiquetas.append(etiqueta)
            piso = max(piso, minimo)
    if piso and any(_menciona(acolchado, v) for v in VERBOS_HECHO):
        # Mencion institucional + hecho concreto: es justo el caso que se estaba
        # perdiendo (renuncia del superintendente, reunion entre bancos).
        etiquetas.append("hecho")
        piso = max(piso, 9)
    return piso, etiquetas


def mismo_tema(titulo_a: str, titulo_b: str) -> bool:
    """True si dos titulares cuentan la misma noticia.

    Usa contencion ademas de Jaccard: un medio publica el titular corto
    ("Qik Banco Digital, reconocido por Global Finance") y otro el mismo con
    cola ("...por el uso de la IA para transformar la experiencia de sus
    clientes"). Jaccard los separa; la contencion los reconoce.
    """
    a, b = tokens_titular(titulo_a), tokens_titular(titulo_b)
    if not a or not b:
        return False
    comunes = len(a & b)
    if comunes < 3:
        return False
    contencion = comunes / min(len(a), len(b))
    jaccard = comunes / len(a | b)
    return contencion >= 0.7 or jaccard >= 0.55


def sb_get(path: str, params: dict | None = None) -> list[dict]:
    url = f"{SUPABASE_URL}/rest/v1/{path}"
    r = httpx.get(url, headers=SUPABASE_HEADERS, params=params, timeout=20)
    r.raise_for_status()
    return r.json()


def sb_insert_log(entry: dict) -> None:
    # Log mínimo, no metadata pesada.
    url = f"{SUPABASE_URL}/rest/v1/data_update_log"
    headers = {**SUPABASE_HEADERS, "Prefer": "return=minimal"}
    try:
        r = httpx.post(url, headers=headers, json=entry, timeout=10)
        if r.status_code not in (200, 201, 204):
            log.warning("Log insert warning %s: %s", r.status_code, r.text[:300])
    except Exception as exc:
        log.warning("No se pudo insertar log: %s", exc)


def load_sources() -> list[dict]:
    rows = sb_get("news_sources", params={"enabled": "eq.true", "select": "source_key,name,url,enabled"})
    log.info("Fuentes activas: %d", len(rows))
    return rows


# ---------------------------------------------------------------------------
# Registro de descartes: todo lo que el pipeline tira queda anotado con su
# motivo, para que el analista lo vea en el panel y pueda rescatarlo. Antes
# desaparecia en el log del workflow y no habia forma de recuperarlo.
# ---------------------------------------------------------------------------
_descartes: list[dict] = []
_descartes_lock = threading.Lock()

MOTIVO_TEXTO = {
    "relevancia_baja": "El modelo le puso poca relevancia financiera",
    "modelo_irrelevante": "El modelo la marco como no noticiosa",
    "sin_cuerpo": "No se pudo leer el cuerpo del articulo",
    "sin_detalle": "El articulo no devolvio datos utiles",
    "vieja": "Publicada fuera del dia editorial en curso",
    "sin_fecha": "No se pudo verificar la fecha de publicacion",
    "duplicada": "Ya cubierta por otra nota del mismo tema",
}


def registrar_descarte(motivo: str, titulo: str, url: str, fuente: str = "",
                       relevancia=None, señales=None, detalle: str = "") -> None:
    if not (titulo or url):
        return
    with _descartes_lock:
        if any(d.get("url") == url for d in _descartes):
            return
        _descartes.append({
            "id": stable_id(url or titulo),
            "motivo": motivo,
            "motivo_texto": MOTIVO_TEXTO.get(motivo, motivo),
            "title": (titulo or "").strip(),
            "url": url or "",
            "source_name": fuente or "",
            "relevance": relevancia,
            "señales": señales or [],
            "detalle": detalle,
            "fetched_at": now_iso(),
        })


# ---------------------------------------------------------------------------
# Busqueda en la web: tres buscadores sin API key
# ---------------------------------------------------------------------------
# Un buscador que ya fallo en esta corrida no se vuelve a probar: si s.jina.ai
# esta limitando por tasa, lo hara en las cinco consultas y solo sirve para
# gastar cinco timeouts de 35s.
_buscadores_caidos: set[str] = set()
_buscadores_lock = threading.Lock()


def _marcar_buscador_caido(nombre: str, motivo: str) -> None:
    with _buscadores_lock:
        if nombre in _buscadores_caidos:
            return
        _buscadores_caidos.add(nombre)
    log.warning("Buscador '%s' fuera de servicio en esta corrida (%s); se pasa al siguiente.",
                nombre, motivo)


def _rss_items(xml_text: str) -> list[dict]:
    """Titulo, enlace y fecha de cada item de un RSS o Atom. Sin dependencias.

    Devuelve [] ante cualquier XML que no se pueda leer: un feed roto no puede
    tumbar la corrida.
    """
    try:
        raiz = ElementTree.fromstring(xml_text.strip())
    except ElementTree.ParseError:
        return []

    def texto(nodo, *nombres):
        for nombre in nombres:
            hijo = nodo.find(nombre)
            if hijo is None:
                # Atom llega con namespace; se busca por el nombre final.
                for candidato in nodo:
                    if candidato.tag.rsplit("}", 1)[-1] == nombre:
                        hijo = candidato
                        break
            if hijo is not None:
                valor = (hijo.text or "").strip()
                if not valor and nombre == "link":
                    valor = (hijo.get("href") or "").strip()
                if valor:
                    return valor
        return ""

    items: list[dict] = []
    for nodo in raiz.iter():
        if nodo.tag.rsplit("}", 1)[-1] not in ("item", "entry"):
            continue
        titulo = limpiar_texto(html.unescape(texto(nodo, "title")))
        enlace = texto(nodo, "link", "id")
        fecha = texto(nodo, "pubDate", "published", "updated", "date")
        if not titulo or not enlace:
            continue
        items.append({"title": titulo, "link": enlace.strip(), "pub": fecha})
        if len(items) >= RSS_MAX_ITEMS:
            break
    return items


def buscar_gdelt(consulta: str, max_resultados: int) -> list[dict]:
    """GDELT DOC 2.0: prensa mundial indexada, sin API key ni cuota.

    Se acota a prensa dominicana en español y a la misma ventana de frescura
    que usa el resto del pipeline, para no traer archivo.
    """
    filtros = [f"({consulta})"]
    if GDELT_SOURCE_COUNTRY:
        filtros.append(f"sourcecountry:{GDELT_SOURCE_COUNTRY}")
    if GDELT_SOURCE_LANG:
        filtros.append(f"sourcelang:{GDELT_SOURCE_LANG}")
    # timespan en horas: la ventana editorial en dias + el dia en curso.
    horas = max(24, (NEWS_MAX_AGE_DAYS + 1) * 24)
    try:
        r = httpx.get(
            GDELT_SEARCH_BASE,
            params={
                "query": " ".join(filtros),
                "mode": "ArtList",
                "maxrecords": max(10, max_resultados * 3),
                "timespan": f"{horas}h",
                "sort": "datedesc",
                "format": "json",
            },
            headers={"User-Agent": "ObservatorioLaNacional/1.0 (news pipeline)"},
            timeout=FETCH_TIMEOUT,
            follow_redirects=True,
        )
        if r.status_code != 200:
            _marcar_buscador_caido("gdelt", f"HTTP {r.status_code}")
            return []
        # GDELT devuelve HTML de error con status 200 cuando la consulta no le
        # gusta; entonces no hay JSON que leer.
        datos = r.json()
    except json.JSONDecodeError:
        log.warning("GDELT no devolvio JSON para '%s' (consulta rechazada)", consulta[:60])
        return []
    except Exception as exc:
        _marcar_buscador_caido("gdelt", type(exc).__name__)
        return []

    resultados: list[dict] = []
    vistos: set[str] = set()
    for art in datos.get("articles", []) or []:
        limpio = canonicalize_url(art.get("url") or "")
        titulo = limpiar_texto(art.get("title") or "")
        if not limpio or not titulo or limpio in vistos:
            continue
        vistos.add(limpio)
        resultados.append({
            "title": titulo,
            "link": limpio,
            # seendate viene como 20260824T115538Z: se normaliza a ISO para que
            # el filtro de frescura pueda usarla sin abrir el articulo.
            "published_at": _fecha_gdelt(art.get("seendate")),
        })
        if len(resultados) >= max_resultados:
            break
    return resultados


def _fecha_gdelt(valor: str | None) -> str:
    """20260824T115538Z -> 2026-08-24T11:55:38+00:00."""
    crudo = (valor or "").strip()
    if len(crudo) < 15 or "T" not in crudo:
        return ""
    try:
        return datetime.strptime(crudo[:15], "%Y%m%dT%H%M%S").replace(tzinfo=timezone.utc).isoformat()
    except ValueError:
        return ""


def buscar_google_news(consulta: str, max_resultados: int) -> list[dict]:
    """Google News como RSS: sin API key, edicion dominicana en español.

    El enlace del feed apunta a news.google.com y hay que seguirlo hasta el
    medio. Cuando Google no suelta el destino, el resultado se descarta en vez
    de guardar una URL de Google como fuente de la noticia.
    """
    ventana = f" when:{max(1, NEWS_MAX_AGE_DAYS + 1)}d"
    try:
        r = httpx.get(
            GOOGLE_NEWS_RSS_BASE,
            params={"q": consulta + ventana, "hl": "es-419", "gl": "DO", "ceid": "DO:es-419"},
            headers={"User-Agent": "Mozilla/5.0 (compatible; ObservatorioLaNacional/1.0)"},
            timeout=FETCH_TIMEOUT,
            follow_redirects=True,
        )
        if r.status_code != 200:
            _marcar_buscador_caido("googlenews", f"HTTP {r.status_code}")
            return []
        items = _rss_items(r.text)
    except Exception as exc:
        _marcar_buscador_caido("googlenews", type(exc).__name__)
        return []

    resultados: list[dict] = []
    vistos: set[str] = set()
    # Cada enlace de Google cuesta una peticion extra para llegar al medio: se
    # intenta un poco mas de lo que se necesita y no mas.
    intentos_restantes = max_resultados * 3
    for item in items:
        if intentos_restantes <= 0:
            break
        intentos_restantes -= 1
        destino = _resolver_enlace_google(item["link"])
        if not destino or destino in vistos:
            continue
        vistos.add(destino)
        # El titulo del feed trae " - Medio" al final; sobra para el analista.
        titulo = re.sub(r"\s+-\s+[^-]{2,40}$", "", item["title"]).strip() or item["title"]
        resultados.append({
            "title": titulo,
            "link": destino,
            "published_at": _fecha_rss(item.get("pub")),
        })
        if len(resultados) >= max_resultados:
            break
    return resultados


def _resolver_enlace_google(url: str) -> str:
    """news.google.com/rss/articles/... -> URL del medio, o cadena vacia."""
    limpio = (url or "").strip()
    if not limpio:
        return ""
    if "news.google.com" not in limpio:
        return canonicalize_url(limpio)
    try:
        r = httpx.get(
            limpio,
            headers={"User-Agent": "Mozilla/5.0 (compatible; ObservatorioLaNacional/1.0)"},
            timeout=20,
            follow_redirects=True,
        )
        final = str(r.url)
        if "news.google.com" in final:
            # Algunas respuestas traen el destino en un meta refresh o en un
            # data-attribute en vez de un redirect HTTP.
            encontrado = re.search(r'https?://(?!news\.google\.com)[^"\'<>\s]{15,}', r.text or "")
            final = encontrado.group(0) if encontrado else ""
        return canonicalize_url(final) if final and "news.google.com" not in final else ""
    except Exception:
        return ""


def _fecha_rss(valor: str | None) -> str:
    """RFC 822 (Mon, 24 Aug 2026 11:55:38 GMT) o ISO -> ISO. '' si no se lee."""
    crudo = (valor or "").strip()
    if not crudo:
        return ""
    for formato in ("%a, %d %b %Y %H:%M:%S %z", "%a, %d %b %Y %H:%M:%S %Z",
                    "%d %b %Y %H:%M:%S %z", "%a, %d %b %Y %H:%M %z"):
        try:
            fecha = datetime.strptime(crudo, formato)
            if fecha.tzinfo is None:
                fecha = fecha.replace(tzinfo=timezone.utc)
            return fecha.isoformat()
        except ValueError:
            continue
    fecha = parse_iso_date(crudo)
    return fecha.isoformat() if fecha else ""


def buscar_con_jina(consulta: str, max_resultados: int) -> list[dict]:
    """Devuelve [{title, url}] para una consulta. Lista vacia si algo falla."""
    try:
        r = httpx.get(
            JINA_SEARCH_BASE + consulta,
            headers={"Accept": "text/markdown", "X-Return-Format": "markdown"},
            timeout=FETCH_TIMEOUT,
            follow_redirects=True,
        )
        if r.status_code != 200:
            _marcar_buscador_caido("jina", f"HTTP {r.status_code}")
            return []
        texto = r.text
    except Exception as exc:
        _marcar_buscador_caido("jina", type(exc).__name__)
        return []

    # El buscador devuelve markdown; se extraen los pares [titulo](url) y, como
    # respaldo, las lineas "URL Source: ...".
    resultados: list[dict] = []
    vistos: set[str] = set()
    for titulo, url in re.findall(r"\[([^\]]{12,200})\]\((https?://[^\s)]+)\)", texto):
        limpio = canonicalize_url(url)
        if not limpio or limpio in vistos:
            continue
        if "jina.ai" in limpio or "google." in limpio:
            continue
        vistos.add(limpio)
        resultados.append({"title": titulo.strip(), "link": limpio})
        if len(resultados) >= max_resultados:
            break
    return resultados


BUSCADORES = {
    "gdelt": buscar_gdelt,
    "googlenews": buscar_google_news,
    "jina": buscar_con_jina,
}


def buscar_en_web(consulta: str, max_resultados: int) -> list[dict]:
    """Devuelve [{title, link, published_at?}] usando el primer buscador que responda.

    Se prueban en el orden de NEWS_SEARCH_PROVIDERS. Ninguno pide API key, asi
    que caerse a otro no cuesta credenciales ni cuota: solo una peticion mas.
    """
    for nombre in NEWS_SEARCH_PROVIDERS:
        buscador = BUSCADORES.get(nombre)
        if buscador is None:
            log.warning("Buscador desconocido en NEWS_SEARCH_PROVIDERS: %s", nombre)
            continue
        with _buscadores_lock:
            caido = nombre in _buscadores_caidos
        if caido:
            continue
        try:
            resultados = buscador(consulta, max_resultados)
        except Exception as exc:
            _marcar_buscador_caido(nombre, type(exc).__name__)
            continue
        if resultados:
            log.info("  · buscador '%s': %d resultado(s)", nombre, len(resultados))
            return resultados
        log.info("  · buscador '%s' sin resultados para esta consulta", nombre)
    return []


def recuperar_por_titular(titulo: str, url_original: str) -> str | None:
    """Cuando no se puede leer el cuerpo en la fuente original, busca el mismo
    titular en la web y lee la primera cobertura alternativa que si abra.

    Es el caso de los medios que bloquean al lector: la noticia existe, solo
    que no en esa URL.
    """
    if not NEWS_RESCUE_ENABLED or not titulo:
        return None
    dominio_original = urlparse(ensure_scheme(url_original)).netloc.lower()
    for resultado in buscar_en_web(titulo, 4):
        alterno = resultado.get("link") or ""
        if not alterno or urlparse(alterno).netloc.lower() == dominio_original:
            continue
        if not mismo_tema(titulo, resultado.get("title") or ""):
            continue
        markdown = fetch_with_jina(alterno, MAX_ARTICLE_MARKDOWN_CHARS)
        if markdown:
            log.info("  ↻ cuerpo recuperado desde otra fuente: %s", alterno[:90])
            return markdown
    return None


def ensure_scheme(url: str) -> str:
    u = (url or "").strip()
    return u if u.startswith("http") else "https://" + u


def fetch_with_jina(url: str, max_chars: int) -> str | None:
    try:
        r = httpx.get(
            JINA_READER_BASE + url,
            headers={"Accept": "text/markdown", "X-Return-Format": "markdown"},
            timeout=FETCH_TIMEOUT,
            follow_redirects=True,
        )
        if r.status_code != 200:
            log.warning("Jina HTTP %s para %s", r.status_code, url)
            return None
        text = r.text.strip()
        if len(text) < MIN_CONTENT_CHARS:
            log.warning("Jina contenido corto (%d chars) para %s", len(text), url)
            return None
        return text[:max_chars]
    except httpx.TimeoutException:
        log.warning("Jina timeout: %s", url)
        return None
    except Exception as exc:
        log.warning("Jina error (%s): %s", type(exc).__name__, url)
        return None


# ---------------------------------------------------------------------------
# Titulares desde el RSS del medio (sin Gemini, sin Jina)
# ---------------------------------------------------------------------------
_feeds_cache: dict[str, str | None] = {}
_feeds_lock = threading.Lock()


def _pedir(url: str, timeout: int = FETCH_TIMEOUT):
    return httpx.get(
        url,
        headers={
            "User-Agent": "Mozilla/5.0 (compatible; ObservatorioLaNacional/1.0; +https://github.com/ecojoelvaldez)",
            "Accept": "application/rss+xml, application/atom+xml, application/xml, text/xml, text/html;q=0.8",
        },
        timeout=timeout,
        follow_redirects=True,
    )


def _es_feed(texto: str) -> bool:
    cabeza = (texto or "")[:600].lstrip().lower()
    return "<rss" in cabeza or "<feed" in cabeza or "<rdf:rdf" in cabeza


def descubrir_feed(base_url: str) -> str | None:
    """URL del RSS/Atom de una seccion, o None si el medio no publica ninguno.

    Primero se cree lo que el propio HTML declara (<link rel="alternate">);
    despues se prueban las rutas convencionales. El resultado se cachea por
    seccion: dos secciones del mismo medio no vuelven a buscar lo mismo.
    """
    base = ensure_scheme(base_url).rstrip("/") + "/"
    with _feeds_lock:
        if base in _feeds_cache:
            return _feeds_cache[base]

    encontrado: str | None = None
    html_texto = ""
    try:
        r = _pedir(base)
        if r.status_code == 200:
            # La propia URL ya puede ser un feed (fuentes configuradas asi).
            if _es_feed(r.text):
                encontrado = str(r.url)
            else:
                html_texto = r.text
    except Exception as exc:
        log.info("  · no se pudo abrir %s para buscar feed (%s)", base, type(exc).__name__)

    if not encontrado and html_texto:
        for etiqueta in re.findall(r"<link\b[^>]*>", html_texto[:200000], re.I):
            if "alternate" not in etiqueta.lower():
                continue
            if not re.search(r'type=["\']application/(rss|atom)\+xml', etiqueta, re.I):
                continue
            href = re.search(r'href=["\']([^"\']+)["\']', etiqueta, re.I)
            if href:
                encontrado = urljoin(base, html.unescape(href.group(1)).strip())
                break

    if not encontrado:
        limite = time.monotonic() + RSS_DISCOVERY_BUDGET_S
        for ruta in RSS_PATHS_COMUNES:
            if time.monotonic() > limite:
                log.info("  · sin feed tras %.0fs de busqueda; se sigue por el camino de siempre",
                         RSS_DISCOVERY_BUDGET_S)
                break
            candidato = urljoin(base, ruta)
            try:
                r = _pedir(candidato, timeout=8)
            except Exception:
                continue
            if r.status_code == 200 and _es_feed(r.text) and _rss_items(r.text):
                encontrado = str(r.url)
                break

    with _feeds_lock:
        _feeds_cache[base] = encontrado
    if encontrado:
        log.info("  · feed encontrado: %s", encontrado)
    return encontrado


def titulares_desde_rss(base_url: str) -> list[dict]:
    """Titulares de una fuente leidos de su RSS: 0 llamadas a Gemini.

    Devuelve [] si el medio no tiene feed legible; el flujo de siempre
    (Jina + Gemini sobre la pagina indice) sigue detras como respaldo.
    """
    feed = descubrir_feed(base_url)
    if not feed:
        return []
    try:
        r = _pedir(feed)
        if r.status_code != 200:
            return []
        items = _rss_items(r.text)
    except Exception as exc:
        log.info("  · feed ilegible (%s): %s", type(exc).__name__, feed)
        return []

    titulares: list[dict] = []
    for item in items:
        link = canonicalize_url(item["link"], base_url)
        if not link:
            continue
        titulares.append({
            "title": item["title"],
            "link": link,
            "published_at": _fecha_rss(item.get("pub")),
        })
    return titulares


def clean_json_text(raw: str) -> str:
    raw = (raw or "").strip()
    if raw.startswith("```"):
        parts = raw.split("```")
        raw = parts[1] if len(parts) > 1 else raw
        raw = raw.strip()
        if raw.lower().startswith("json"):
            raw = raw[4:].strip()
    return raw.strip()


_gemini_lock = threading.Lock()
_gemini_last_call = 0.0


def _gemini_throttle() -> None:
    """Garantiza un intervalo mínimo entre llamadas a Gemini (todas las hebras)."""
    global _gemini_last_call
    with _gemini_lock:
        wait = _gemini_last_call + GEMINI_MIN_INTERVAL - time.monotonic()
        if wait > 0:
            time.sleep(wait)
        _gemini_last_call = time.monotonic()


def _gemini_quota_detail(response) -> str:
    """Qué cuota dice Google que se excedió (por minuto vs. diaria)."""
    try:
        error = response.json().get("error", {})
        partes = [str(error.get("message", "")).strip()]
        for detail in error.get("details", []):
            for violation in detail.get("violations", []):
                cuota = violation.get("quotaId") or violation.get("quotaMetric")
                if cuota:
                    partes.append(str(cuota))
    except Exception:
        return ""
    return " | ".join(p for p in partes if p)[:300]


def _gemini_retry_delay(response, attempt: int) -> float:
    """Espera sugerida por Google (RetryInfo) o backoff exponencial."""
    try:
        for detail in response.json().get("error", {}).get("details", []):
            delay = str(detail.get("retryDelay", ""))
            if delay.endswith("s"):
                return min(90.0, float(delay[:-1]) + 1)
    except Exception:
        pass
    return min(90.0, 15.0 * attempt)


_quota_agotada = threading.Event()


def gemini_json(prompt: str, max_tokens: int = 1500, retries: int = 4) -> dict | None:
    # Corta-circuito de cuota: cuando una llamada agota sus reintentos contra un
    # 429, la cuota no se recupera dentro de la corrida (visto el 2026-07-31:
    # 4 min de espera y seguia en 429). Sin esto, cada articulo que falta gasta
    # otros 4 reintentos de ~60 s cada uno: el job se va en esperas y machaca
    # una cuota que ya esta cerrada. Mejor terminar con lo que ya se consiguio.
    if _quota_agotada.is_set():
        return None
    payload = {
        "contents": [{"parts": [{"text": prompt}]}],
        # responseMimeType fuerza JSON valido: elimina los fallos de parseo
        # por fences ``` o texto extra alrededor de la respuesta.
        "generationConfig": {
            "temperature": 0.1,
            "maxOutputTokens": max_tokens,
            "responseMimeType": "application/json",
        },
    }
    ultimo_429 = False
    for attempt in range(1, retries + 1):
        if _quota_agotada.is_set():
            return None
        try:
            _gemini_throttle()
            r = httpx.post(GEMINI_ENDPOINT, params={"key": GEMINI_API_KEY}, json=payload, timeout=GEMINI_TIMEOUT)
            if r.status_code in (429, 503):
                ultimo_429 = r.status_code == 429
                delay = _gemini_retry_delay(r, attempt)
                if attempt == 1:
                    detalle = _gemini_quota_detail(r)
                    if detalle:
                        log.warning("Gemini %s · cuota: %s", r.status_code, detalle)
                log.warning("Gemini %s, reintento %d/%d (espera %.0fs)", r.status_code, attempt, retries, delay)
                time.sleep(delay)
                continue
            ultimo_429 = False
            if r.status_code != 200:
                log.warning("Gemini HTTP %s: %s", r.status_code, r.text[:300])
                return None
            data = r.json()
            raw = data["candidates"][0]["content"]["parts"][0]["text"]
            return json.loads(clean_json_text(raw))
        except json.JSONDecodeError as exc:
            log.warning("Gemini devolvio JSON invalido: %s", exc)
            return None
        except Exception as exc:
            log.warning("Gemini error (%s): %s", type(exc).__name__, exc)
            time.sleep(3 * attempt)
    if ultimo_429 and not _quota_agotada.is_set():
        _quota_agotada.set()
        log.warning(
            "Cuota de Gemini agotada tras %d reintentos: se dejan de pedir "
            "llamadas y la corrida termina con los candidatos ya obtenidos.",
            retries,
        )
    return None


HEADLINE_PROMPT = """Eres un analista de inteligencia financiera dominicana que cura noticias para un banco (asociacion de ahorros y prestamos).

Recibes el markdown de una PAGINA INDICE de noticias: lista de titulares, enlaces y fragmentos.

Identifica titulares de NOTICIAS REALES con impacto directo en el sector financiero, monetario, bancario, regulatorio o macroeconomico dominicano.

Prioriza (en este orden):
1. Politica monetaria, tasas de interes, inflacion, tipo de cambio, liquidez
2. Banco Central, Superintendencia de Bancos, Hacienda, DGII, mercado de valores
3. Banca, credito, captaciones, hipotecas, remesas, morosidad
4. Indicadores macroeconomicos de RD (PIB, IMAE, deuda, IED, empleo)
5. Economia global SOLO si tiene canal claro hacia RD (Fed, petroleo, remesas, turismo)

REGLA QUE MANDA SOBRE TODO LO ANTERIOR: si el titular o su fragmento MENCIONA a una entidad
financiera dominicana —Banreservas, Banco Popular, BHD, Scotiabank, Santa Cruz, Caribe, Promerica,
Banesco, Ademi, Adopem, APAP, Asociacion Cibao, Asociacion La Nacional, cualquier otra asociacion de
ahorros y prestamos, banco de ahorro y credito, cooperativa, AFP, aseguradora, ARS o puesto de bolsa
del pais— INCLUYELO, aunque la nota no sea de economia: un patrocinio, un nombramiento, una donacion,
un premio, un litigio o un acto social de una de esas entidades tambien cuenta. Esa mencion pesa mas
que cualquier criterio de descarte.

Descarta sin excepcion (siempre que no mencionen una entidad financiera dominicana): navegacion, relleno, opinion sin dato economico, publirreportajes, tecnologia de consumo, deportes, entretenimiento, sucesos, politica sin componente economico, noticias militares o de seguridad sin impacto financiero cuantificable.

Devuelve SOLO JSON valido con esta estructura exacta:
{"headlines":[{"title":"titular exacto","link":"URL del articulo individual","category":"Economia"}]}

Reglas:
- Devuelve entre 0 y 6 titulares. Menos y mejores es preferible a mas y debiles.
- category debe ser exactamente una de: Monetario, Financiero, Regulatorio, Economia, Global.
- link debe ser la URL del articulo individual. Si no hay enlace claro, omite el titular.
- No inventes enlaces.

Markdown de pagina indice:
"""

ARTICLE_PROMPT = """Eres un analista de inteligencia financiera dominicana que trabaja para una asociacion de ahorros y prestamos.

Recibes el markdown de un ARTICULO individual. Extrae informacion util para que un analista lo apruebe o descarte en un panel editorial.

Devuelve SOLO JSON valido con esta estructura exacta:
{
  "title": "titulo completo del articulo",
  "summary": "resumen ejecutivo en 2 oraciones, en espanol, con impacto para el sector financiero dominicano",
  "body": "lead propio de 1 parrafo, 45-90 palabras, basado en el cuerpo de la noticia, no repitas el titulo y no copies texto largo literalmente",
  "published_at": "fecha ISO 8601 si aparece, o null",
  "category": "Economia",
  "relevance": 7,
  "relevant": true
}

Reglas:
- category debe ser exactamente una de: Monetario, Financiero, Regulatorio, Economia, Global.
- relevance es un entero 0-10 que mide el impacto para el SECTOR FINANCIERO dominicano:
  * 10: la nota MENCIONA a una entidad financiera dominicana (banco, asociacion de ahorros y
          prestamos, banco de ahorro y credito, cooperativa, AFP, aseguradora, ARS, puesto de bolsa
          o gremio del sector), sin importar de que trate la nota. Basta la mencion: un patrocinio
          deportivo de Banreservas, un nombramiento en Adopem o una demanda contra una AFP entran
          con 10 y relevant=true.
  * 9-10: decision de politica monetaria/regulatoria, datos de banca o credito, tasas, tipo de cambio,
          Y TAMBIEN cualquier cambio en la conduccion de los organismos que regulan al sector
          (Superintendencia de Bancos, Banco Central, Junta Monetaria, Hacienda): renuncias,
          designaciones, destituciones, juramentaciones, comparecencias, rendicion de cuentas
  * 7-8: macro RD relevante (PIB, inflacion, deuda, IED, remesas) o global con canal directo a RD;
          movimientos, acuerdos, alianzas, reuniones o declaraciones de bancos y asociaciones
          dominicanas sobre el sistema financiero, aunque no traigan cifras
  * 5-6: economia general RD con impacto indirecto (sectores, comercio, energia)
  * 0-4: sin impacto financiero claro (seguridad, politica, social, curiosidades)

IMPORTANTE — errores a no repetir. Estas SI son relevantes y se estaban descartando:
  * "El Superintendente de Bancos renuncia a su cargo" -> quien dirige al regulador es informacion
    de primer orden para una entidad supervisada, aunque la nota no traiga ni un numero.
  * "Banreservas y Popular se reunen para discutir el sistema financiero" -> lo que hablan los bancos
    mas grandes del pais define el entorno competitivo.
  * Nombramientos y salidas de ejecutivos de bancos, reguladores o gremios financieros.
  * Declaraciones de autoridades monetarias sobre el rumbo de la economia.
Ante la duda entre descartar y proponer una nota que menciona un regulador, un banco dominicano o
la politica monetaria: PROPONLA. El analista filtra despues; lo que no se propone no se ve.
Nunca devuelvas {"relevant": false} para una nota que nombre a una entidad financiera dominicana,
por lateral que sea la mencion.

- Si el articulo no tiene contenido noticioso util, devuelve {"relevant": false}.
- Si no encuentras fecha, usa null; no inventes fechas.
- El body debe ser un parrafo informativo, no una lista ni el titular.

Markdown del articulo:
"""


def extract_headlines(index_markdown: str) -> list[dict]:
    parsed = gemini_json(HEADLINE_PROMPT + index_markdown, max_tokens=1600)
    if not parsed:
        return []
    headlines = parsed.get("headlines", [])
    return headlines[:MAX_HEADLINES_PER_SOURCE] if isinstance(headlines, list) else []


def extract_article_details(article_markdown: str, titular_previo: str = "",
                            url: str = "", fuente: str = "") -> dict | None:
    # Antes de gastar una llamada: si el lector devolvio una pagina de error o
    # un muro de pago, no hay noticia que resumir.
    if es_pagina_invalida(article_markdown[:800]):
        log.info("  -> pagina de error/muro de pago, no es noticia: %s", (titular_previo or url)[:70])
        registrar_descarte("sin_cuerpo", titular_previo, url, fuente,
                           detalle="La pagina es un error o exige suscripcion")
        return None

    parsed = gemini_json(ARTICLE_PROMPT + article_markdown, max_tokens=1800)
    if not parsed:
        registrar_descarte("sin_detalle", titular_previo, url, fuente)
        return None

    titulo = limpiar_texto(parsed.get("title") or titular_previo)
    parsed["title"] = titulo
    parsed["summary"] = limpiar_texto(parsed.get("summary"))
    parsed["body"] = limpiar_texto(parsed.get("body"))

    # Segunda red: a veces el lector trae algo de texto y el modelo igual
    # describe la pagina de error. Paso el 2026-07-30 con "Error 404 | Hoy
    # Digital", propuesto con relevancia 7.
    if es_pagina_invalida(titulo, parsed.get("summary")):
        log.info("  -> el resumen describe una pagina de error: %s", titulo[:70])
        registrar_descarte("sin_cuerpo", titulo, url, fuente,
                           detalle="El modelo resumio una pagina de error")
        return None
    piso, etiquetas = señales_detectadas(titulo, parsed.get("summary"), parsed.get("body"))

    try:
        relevance = int(parsed.get("relevance", 7))
    except (TypeError, ValueError):
        relevance = 7
    relevance = max(0, min(10, relevance))

    # El piso institucional manda sobre el juicio del modelo, incluso sobre un
    # relevant=false: la nota puede no traer cifras y aun asi ser de primer
    # orden para una entidad supervisada.
    if piso > relevance:
        log.info("  ↑ relevancia %d -> %d por señal %s: %s",
                 relevance, piso, "+".join(etiquetas), titulo[:70])
        relevance = piso
    parsed["relevance"] = relevance
    parsed["señales"] = etiquetas
    parsed["rescatado_por_regla"] = bool(piso) and piso >= NEWS_MIN_RELEVANCE and not parsed.get("relevant", True)

    if not parsed.get("relevant", True) and piso < NEWS_MIN_RELEVANCE:
        registrar_descarte("modelo_irrelevante", titulo, url, fuente, relevance, etiquetas)
        return None
    if relevance < NEWS_MIN_RELEVANCE:
        log.info("  -> relevancia %d < %d, descartado: %s", relevance, NEWS_MIN_RELEVANCE, titulo[:70])
        registrar_descarte("relevancia_baja", titulo, url, fuente, relevance, etiquetas)
        return None
    return parsed


def canonicalize_url(link: str, base_url: str | None = None) -> str | None:
    if not link or not isinstance(link, str):
        return None
    link = link.strip().strip("<>()[]{}\"'")
    if base_url and not link.startswith("http"):
        link = urljoin(base_url, link)
    parsed = urlparse(link)
    if not parsed.scheme.startswith("http") or not parsed.netloc:
        return None
    query_pairs = [(k, v) for k, v in parse_qsl(parsed.query, keep_blank_values=True) if k not in TRACKING_PARAMS]
    clean_query = urlencode(query_pairs)
    clean_path = re.sub(r"/{2,}", "/", parsed.path).rstrip("/")
    return urlunparse((parsed.scheme, parsed.netloc.lower(), clean_path, "", clean_query, ""))


def parse_iso_date(value: str | None) -> datetime | None:
    if not value or not isinstance(value, str):
        return None
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Dia editorial: todo el flujo se mide en dias calendario de Santo Domingo.
# ---------------------------------------------------------------------------
def hoy_editorial() -> "date":
    return datetime.now(NEWS_TZ).date()


def dia_editorial(value) -> "date | None":
    """Fecha local (Santo Domingo) de un instante ISO, o None si no se lee."""
    if isinstance(value, datetime):
        return value.astimezone(NEWS_TZ).date()
    dt = parse_iso_date(value)
    return dt.astimezone(NEWS_TZ).date() if dt else None


def fecha_desde_url(url: str | None) -> str | None:
    """Fecha embebida en la ruta (/2026/08/19/...), tipica de los medios locales.

    Es la ultima red antes de descartar por falta de fecha: el CMS la pone en
    la URL aunque el articulo no la muestre en el cuerpo.
    """
    m = re.search(r"/(20\d{2})[/-](0[1-9]|1[0-2])[/-](0[1-9]|[12]\d|3[01])(?:[/-]|$)", str(url or ""))
    if not m:
        return None
    try:
        año, mes, dia = int(m.group(1)), int(m.group(2)), int(m.group(3))
        return datetime(año, mes, dia, 12, 0, tzinfo=NEWS_TZ).isoformat()
    except ValueError:
        return None


def ventana_frescura_texto() -> str:
    hoy = hoy_editorial()
    desde = hoy - timedelta(days=NEWS_MAX_AGE_DAYS)
    return f"{desde} a {hoy}" if desde != hoy else str(hoy)


def is_stale(published_at: str | None) -> bool:
    """True si la nota cae fuera de la ventana de dias editoriales.

    Sin fecha legible no hay frescura demostrable: se trata como vieja salvo
    que NEWS_ALLOW_UNDATED lo permita explicitamente.
    """
    dia = dia_editorial(published_at)
    if not dia:
        return not NEWS_ALLOW_UNDATED
    hoy = hoy_editorial()
    if dia > hoy + timedelta(days=1):
        # Fecha futura absurda: el parser leyo mal, no es garantia de frescura.
        return True
    return dia < hoy - timedelta(days=NEWS_MAX_AGE_DAYS)


def normalize_category(value: str | None) -> str:
    value = (value or "Economia").strip()
    if value in CATEGORY_ALLOWED:
        return value
    low = value.lower()
    if "mon" in low or "tasa" in low or "infl" in low: return "Monetario"
    if "reg" in low or "super" in low: return "Regulatorio"
    if "fin" in low or "banc" in low: return "Financiero"
    if "glob" in low or "intern" in low: return "Global"
    return "Economia"


def load_published_urls() -> set[str]:
    """URLs ya publicadas en el sitio (news_items) en las últimas 2 semanas.

    Sirven para dos cosas distintas: no volver a proponerlas, y sacar del JSON
    las candidatas que el analista ya publicó — si no, la cola se llena de
    notas ya publicadas y las pendientes reales quedan al fondo.
    """
    cutoff = (datetime.now(timezone.utc) - timedelta(days=14)).isoformat()
    try:
        rows = sb_get("news_items", params={"select": "url", "updated_at": f"gte.{cutoff}"})
        urls = {canonicalize_url(r.get("url") or "") for r in rows if r.get("url")}
        urls.discard(None)
        return urls
    except Exception as exc:
        log.info("No se pudo leer news_items para deduplicar: %s", exc)
        return set()


def load_existing_urls(publicados: set[str] | None = None) -> set[str]:
    urls: set[str] = set()
    # 1) Ya publicados en Supabase: pequeño y necesario para no reproponer lo publicado.
    urls.update(publicados if publicados is not None else load_published_urls())

    # 2) Candidatos estáticos previos en el repo.
    try:
        if NEWS_CANDIDATES_PATH.exists():
            data = json.loads(NEWS_CANDIDATES_PATH.read_text(encoding="utf-8"))
            urls.update(canonicalize_url(x.get("url") or "") for x in data.get("candidates", []) if x.get("url"))
    except Exception as exc:
        log.info("No se pudo leer JSON previo para deduplicar: %s", exc)

    urls.discard(None)
    return urls


def detalles_sin_ia(markdown: str, titular: str) -> dict | None:
    """Ficha de la nota sin pasar por el modelo: para cuando la cuota se agoto.

    Resumen y cuerpo salen de los primeros parrafos reales del articulo; la
    relevancia, del mismo piso institucional que ya manda sobre el juicio de
    Gemini. Si la nota no toca ninguna señal, no se propone: sin modelo no hay
    con que defender que le importa a la entidad.
    """
    piso, etiquetas = señales_detectadas(titular, markdown[:2500])
    if piso < NEWS_MIN_RELEVANCE:
        return None

    # El markdown de Jina abre con titulo, creditos y enlaces; los parrafos
    # utiles son las lineas largas de prosa.
    parrafos: list[str] = []
    for linea in markdown.splitlines():
        limpia = limpiar_texto(re.sub(r"[*_`>#\[\]]|\(https?://[^)]+\)", " ", linea))
        if len(limpia) < 120 or limpia.startswith("!"):
            continue
        if norm_texto(limpia) == norm_texto(titular):
            continue
        parrafos.append(limpia)
        if len(parrafos) >= 3:
            break
    if not parrafos:
        return None

    cuerpo = " ".join(parrafos)[:900]
    return {
        "title": titular,
        "summary": parrafos[0][:400],
        "body": cuerpo,
        "category": "Financiero" if "banca" in etiquetas or "entidad_rd" in etiquetas else "Economia",
        "relevance": piso,
        "señales": etiquetas,
        "published_at": None,
        "sin_ia": True,
    }


def build_candidate(source: dict, headline: dict, base_url: str) -> dict | None:
    headline_title = limpiar_texto(headline.get("title"))
    link = canonicalize_url(headline.get("link") or "", base_url)
    if not headline_title or not link:
        return None

    fuente_nombre = source.get("name", source.get("source_key", "desconocida"))

    article_markdown = fetch_with_jina(link, MAX_ARTICLE_MARKDOWN_CHARS)
    recuperado = False
    if not article_markdown:
        # No poder abrir la URL no significa que la noticia no exista. Se busca
        # el mismo titular en la web y se lee la cobertura de otro medio.
        log.info("  -> no pude leer articulo individual, busco el titular: %s", headline_title[:70])
        article_markdown = recuperar_por_titular(headline_title, link)
        recuperado = bool(article_markdown)
    if not article_markdown:
        registrar_descarte("sin_cuerpo", headline_title, link, fuente_nombre,
                           detalle="Ni la URL original ni una cobertura alterna se pudieron leer")
        return None

    # Con la cuota agotada, el modelo ya no responde: la nota entra igual con
    # ficha armada a mano, marcada `sin_ia`, en vez de perderse la corrida.
    sin_ia = False
    if _quota_agotada.is_set() and NEWS_FALLBACK_SIN_IA:
        details = detalles_sin_ia(article_markdown, headline_title)
        sin_ia = bool(details)
        if not details:
            registrar_descarte("sin_detalle", headline_title, link, fuente_nombre,
                               detalle="Sin cuota de Gemini y sin señal institucional que la sostenga")
            return None
        log.info("  ✓ candidato SIN IA (rel %s): %s", details.get("relevance"), headline_title[:70])
    else:
        details = extract_article_details(article_markdown, headline_title, link, fuente_nombre)
    if not details:
        log.info("  -> articulo sin detalle util: %s", headline_title[:70])
        return None

    # La fecha decide si la nota entra: sin fecha no hay frescura demostrable.
    # El feed o el buscador suelen traerla ya; si no, se busca en la ruta del
    # articulo (/2026/08/19/...) antes de rendirse.
    published_at = details.get("published_at") or headline.get("published_at")
    fecha_inferida = False
    if not parse_iso_date(published_at):
        desde_url = fecha_desde_url(link)
        if desde_url:
            published_at = desde_url
            fecha_inferida = True

    if not parse_iso_date(published_at):
        if not NEWS_ALLOW_UNDATED:
            log.info("  -> articulo sin fecha verificable, se omite: %s", headline_title[:70])
            registrar_descarte("sin_fecha", details.get("title") or headline_title, link, fuente_nombre,
                               details.get("relevance"), details.get("señales"),
                               detalle="Ni el cuerpo ni la URL traen fecha; recupérala a mano si es de hoy")
            return None
    elif is_stale(published_at):
        log.info("  -> fuera del dia editorial, se omite: %s", headline_title[:70])
        registrar_descarte("vieja", details.get("title") or headline_title, link, fuente_nombre,
                           details.get("relevance"), details.get("señales"),
                           detalle=f"publicada {(dia_editorial(published_at) or '?')}, "
                                   f"ventana {ventana_frescura_texto()}")
        return None

    title = limpiar_texto(details.get("title") or headline_title)
    return {
        "id": stable_id(link),
        "source_key": source.get("source_key", ""),
        "source_name": fuente_nombre,
        "url": link,
        "title": title,
        "summary": limpiar_texto(details.get("summary")),
        "body": limpiar_texto(details.get("body")),
        "category": normalize_category(details.get("category") or headline.get("category")),
        "relevance": details.get("relevance", NEWS_MIN_RELEVANCE),
        "señales": details.get("señales") or [],
        "rescatado_por_regla": bool(details.get("rescatado_por_regla")),
        "cuerpo_recuperado": recuperado,
        # El panel lo muestra: un resumen sin modelo no se lee igual que uno
        # curado, y el analista tiene que saber cual esta viendo.
        "sin_ia": sin_ia,
        "published_at": published_at,
        "fecha_inferida_de_url": fecha_inferida,
        "dia_editorial": str(dia_editorial(published_at) or hoy_editorial()),
        "fetched_at": now_iso(),
        "dia_lectura": str(hoy_editorial()),
        "status": "pending",
    }


def process_source(source: dict, existing_urls: set[str]) -> list[dict]:
    name = source.get("name", source.get("source_key", "desconocida"))
    base_url = (source.get("url") or "").strip()
    if not base_url:
        log.warning("Fuente '%s' sin URL, se omite", name)
        return []

    log.info("Procesando fuente: %s (%s)", name, base_url)

    # 1. El RSS del medio, que da titulo + enlace + fecha sin gastar modelo.
    headlines: list[dict] = []
    via_rss = False
    if NEWS_RSS_FIRST:
        headlines = titulares_desde_rss(base_url)
        via_rss = bool(headlines)
        if via_rss:
            log.info("  · %d titulares del RSS (0 llamadas a Gemini)", len(headlines))

    # 2. Respaldo: la pagina indice leida con Jina e interpretada por Gemini.
    if not headlines:
        if _quota_agotada.is_set():
            log.info("  -> sin feed y sin cuota de Gemini: no hay de donde sacar titulares")
            return []
        index_markdown = fetch_with_jina(base_url, MAX_INDEX_MARKDOWN_CHARS)
        if not index_markdown:
            log.info("  -> Jina no devolvio indice util")
            return []
        headlines = extract_headlines(index_markdown)
        if not headlines:
            log.info("  -> Gemini no extrajo titulares utiles")
            return []

    # Filtrar titulares nuevos ANTES de lanzar trabajo pesado (Jina + Gemini).
    pending: list[dict] = []
    seen_in_batch: set[str] = set()
    for headline in headlines:
        link = canonicalize_url(headline.get("link") or "", base_url)
        title = (headline.get("title") or "").strip()
        if not link or not title:
            continue
        if link in existing_urls or link in seen_in_batch:
            log.info("  -> ya vista: %s", title[:70])
            continue
        # El RSS ya trae la fecha de publicacion: lo que cae fuera del dia
        # editorial se descarta AQUI, antes de gastar una lectura y una llamada
        # al modelo. En la corrida del 2026-08-24 se resumieron 4 notas viejas
        # solo para botarlas despues.
        fecha_previa = headline.get("published_at") or ""
        if fecha_previa and is_stale(fecha_previa):
            log.info("  -> fuera del dia editorial segun el feed, no se lee: %s", title[:70])
            registrar_descarte("vieja", title, link, name,
                               detalle=f"el feed la fecha {(dia_editorial(fecha_previa) or '?')}, "
                                       f"ventana {ventana_frescura_texto()}")
            continue
        seen_in_batch.add(link)
        pending.append(headline)
        if len(pending) >= MAX_HEADLINES_PER_SOURCE:
            break

    # Cada articulo requiere 1 lectura Jina + 1 llamada Gemini; se procesan
    # en paralelo con pocos hilos para no disparar los limites de tasa.
    candidates: list[dict] = []
    if pending:
        with ThreadPoolExecutor(max_workers=ARTICLE_WORKERS) as pool:
            futures = {pool.submit(build_candidate, source, h, base_url): h for h in pending}
            for future in as_completed(futures):
                try:
                    candidate = future.result()
                except Exception as exc:
                    log.warning("  -> error procesando articulo: %s", exc)
                    continue
                if not candidate or candidate["url"] in existing_urls:
                    continue
                candidates.append(candidate)
                existing_urls.add(candidate["url"])
                log.info("  ✓ candidato (rel %s): %s", candidate.get("relevance", "?"), candidate["title"][:90])
    candidates = candidates[:MAX_HEADLINES_PER_SOURCE]
    log.info("  -> %d candidatos nuevos de %s", len(candidates), name)
    return candidates


def dedupe_por_tema(candidatos: list[dict]) -> list[dict]:
    """Colapsa las notas que cuentan la misma noticia desde distintos medios.

    La deduplicacion por URL no alcanza: la misma nota (el reconocimiento a Qik,
    la tasa del dolar del dia) llega redactada distinto desde dos medios y se
    proponia dos veces. Se conserva la version con mas relevancia — a igualdad,
    la que tenga cuerpo mas completo — y las otras quedan anotadas como
    cobertura adicional y registradas como descarte por duplicado.
    """
    ordenados = sorted(
        candidatos,
        key=lambda c: (c.get("relevance") or 0, len(c.get("body") or "")),
        reverse=True,
    )
    elegidos: list[dict] = []
    for cand in ordenados:
        gemelo = next((e for e in elegidos if mismo_tema(e.get("title", ""), cand.get("title", ""))), None)
        if gemelo is None:
            elegidos.append(cand)
            continue
        cobertura = gemelo.setdefault("tambien_en", [])
        cobertura.append({
            "source_name": cand.get("source_name", ""),
            "url": cand.get("url", ""),
            "title": cand.get("title", ""),
        })
        log.info("  ⇉ duplicada de '%s': %s (%s)",
                 gemelo.get("title", "")[:50], cand.get("title", "")[:50], cand.get("source_name", ""))
        registrar_descarte("duplicada", cand.get("title", ""), cand.get("url", ""),
                           cand.get("source_name", ""), cand.get("relevance"), cand.get("señales"),
                           detalle=f"Misma noticia que: {gemelo.get('title','')[:120]}")
    # Se devuelve en el orden original de llegada para no alterar el criterio
    # de recencia que aplica write_candidates.
    ids = {id(c) for c in elegidos}
    return [c for c in candidatos if id(c) in ids]


def procesar_busqueda_general(existing_urls: set[str], cupo: int) -> list[dict]:
    """Barrido propio de la web, ademas de las fuentes configuradas.

    Las fuentes fijas fallan de dos maneras: el indice no lista la nota, o el
    medio no la publica. Este paso pregunta directo por los temas que importan,
    asi una noticia relevante no depende de aparecer en un indice concreto.
    """
    if not NEWS_SEARCH_ENABLED or cupo <= 0:
        return []
    if _quota_agotada.is_set() and not NEWS_FALLBACK_SIN_IA:
        return []

    fuente_busqueda = {"source_key": "busqueda_web", "name": "Búsqueda web"}
    encontrados: list[dict] = []
    for consulta in NEWS_SEARCH_QUERIES:
        if len(encontrados) >= cupo:
            break
        if _quota_agotada.is_set() and not NEWS_FALLBACK_SIN_IA:
            break
        log.info("Búsqueda general: %s", consulta)
        resultados = buscar_en_web(consulta, NEWS_SEARCH_MAX_PER_QUERY)
        if not resultados:
            log.info("  -> el buscador no devolvio resultados utiles")
            continue
        for resultado in resultados:
            if len(encontrados) >= cupo:
                break
            link = resultado.get("link")
            titulo = resultado.get("title") or ""
            if not link or link in existing_urls:
                continue
            # No re-procesar lo que ya entro por una fuente fija.
            if any(mismo_tema(titulo, c.get("title", "")) for c in encontrados):
                continue
            # GDELT y Google News devuelven la fecha con el resultado: lo viejo
            # se bota antes de leerlo, igual que con el RSS de las fuentes.
            fecha_previa = resultado.get("published_at") or ""
            if fecha_previa and is_stale(fecha_previa):
                log.info("  -> resultado fuera del dia editorial, no se lee: %s", titulo[:70])
                continue
            existing_urls.add(link)
            try:
                cand = build_candidate(
                    fuente_busqueda,
                    {"title": titulo, "link": link, "published_at": fecha_previa},
                    link,
                )
            except Exception as exc:
                log.warning("  -> error procesando resultado de busqueda: %s", exc)
                continue
            if cand:
                encontrados.append(cand)
                log.info("  ✓ candidato de búsqueda (rel %s): %s", cand.get("relevance"), cand["title"][:80])
    log.info("Búsqueda general: %d candidatos nuevos", len(encontrados))
    return encontrados


def load_previous_candidates() -> list[dict]:
    """Candidatos de corridas anteriores que siguen siendo de hoy.

    Dos cortes, no uno: cuando se leyeron (dia editorial de la lectura) y
    cuando se publicaron. El segundo importa porque un candidato arrastrado
    envejece mientras espera en la cola: sin el, la nota leida ayer a las
    11:59 p.m. seguia viva hoy aunque ya estuviera fuera de la ventana.
    """
    try:
        if not NEWS_CANDIDATES_PATH.exists():
            return []
        data = json.loads(NEWS_CANDIDATES_PATH.read_text(encoding="utf-8"))
        arr = data.get("candidates", [])
        if not isinstance(arr, list):
            return []
        corte_lectura = hoy_editorial() - timedelta(days=CANDIDATE_TTL_DAYS - 1)
        kept, caducados = [], 0
        for x in arr:
            leido = dia_editorial(x.get("fetched_at"))
            # Sin marca de lectura no se sabe de que dia es: fuera.
            if not leido or leido < corte_lectura:
                caducados += 1
                continue
            if is_stale(x.get("published_at")):
                caducados += 1
                continue
            kept.append(x)
        if caducados:
            log.info("Cola: %d candidatos de dias anteriores salieron del JSON.", caducados)
        return kept
    except Exception as exc:
        log.info("No se pudieron cargar candidatos previos: %s", exc)
        return []


def load_previous_discards() -> list[dict]:
    """Descartes de corridas anteriores que siguen dentro del TTL."""
    try:
        if not NEWS_CANDIDATES_PATH.exists():
            return []
        data = json.loads(NEWS_CANDIDATES_PATH.read_text(encoding="utf-8"))
        arr = data.get("discarded", [])
        if not isinstance(arr, list):
            return []
        corte = hoy_editorial() - timedelta(days=DISCARD_TTL_DAYS - 1)
        kept = []
        for x in arr:
            leido = dia_editorial(x.get("fetched_at"))
            if not leido or leido < corte:
                continue
            kept.append(x)
        return kept
    except Exception as exc:
        log.info("No se pudieron cargar descartes previos: %s", exc)
        return []


def write_candidates(candidates: list[dict], errors: int, sources_count: int, elapsed: float,
                     publicados: set[str] | None = None) -> None:
    NEWS_CANDIDATES_PATH.parent.mkdir(parents=True, exist_ok=True)
    publicados = publicados or set()

    merged = []
    seen = set()
    ya_publicados = 0
    fuera_de_dia = 0
    for item in candidates + load_previous_candidates():
        url = canonicalize_url(item.get("url") or "")
        if not url or url in seen:
            continue
        # Lo que el analista ya publicó sale de la cola: cumplió su ciclo.
        if url in publicados:
            ya_publicados += 1
            continue
        # Ultima verificacion antes de escribir: nada fuera de la ventana
        # llega al JSON, venga de esta corrida o arrastrado de la anterior.
        if is_stale(item.get("published_at")):
            fuera_de_dia += 1
            continue
        item["url"] = url
        item["id"] = item.get("id") or stable_id(url)
        item["status"] = "pending"
        item["dia_editorial"] = str(dia_editorial(item.get("published_at")) or hoy_editorial())
        item.setdefault("dia_lectura", str(dia_editorial(item.get("fetched_at")) or hoy_editorial()))
        merged.append(item)
        seen.add(url)

    # La deduplicacion por tema va sobre el conjunto ya fusionado (nuevos +
    # previos): si la misma noticia entro ayer por un medio y hoy por otro,
    # tambien se colapsa.
    merged = dedupe_por_tema(merged)

    # El cupo del archivo es MAX_STORED_CANDIDATES, no el cupo por corrida:
    # si se recortara con MAX_TOTAL_PROPOSALS, cada corrida nueva desalojaria
    # a los pendientes de los dias anteriores aunque nadie los hubiera visto.
    merged = sorted(merged, key=lambda x: x.get("fetched_at") or "", reverse=True)[:MAX_STORED_CANDIDATES]

    with _descartes_lock:
        nuevos_descartes = list(_descartes)
    descartes = []
    vistos_descarte = set()
    for d in nuevos_descartes + load_previous_discards():
        url_descarte = canonicalize_url(d.get("url") or "") if d.get("url") else None
        clave = url_descarte or d.get("id") or d.get("title")
        if not clave or clave in vistos_descarte:
            continue
        # Si la misma nota entro despues por otra fuente, ya no es un descarte.
        if url_descarte and url_descarte in seen:
            continue
        vistos_descarte.add(clave)
        descartes.append(d)
    descartes = sorted(descartes, key=lambda d: (d.get("relevance") or 0), reverse=True)[:MAX_STORED_DISCARDS]

    # Conteo por fecha de publicacion: es la que el analista ve en la ficha y
    # la que hace evidente si se coló algo viejo.
    por_dia: dict[str, int] = {}
    for item in merged:
        dia = item.get("dia_editorial") or (item.get("published_at") or "")[:10]
        if dia:
            por_dia[dia] = por_dia.get(dia, 0) + 1

    hoy = hoy_editorial()
    payload = {
        "generated_at": now_iso(),
        # El panel usa estos tres campos para saber si lo que tiene delante es
        # de hoy o el sobrante de una corrida vieja.
        "dia_editorial": str(hoy),
        "zona_horaria": f"UTC{NEWS_TZ.utcoffset(None).total_seconds() / 3600:+.0f} (Santo Domingo)",
        "ventana_frescura": ventana_frescura_texto(),
        "gemini_model": GEMINI_MODEL,
        "min_relevance": NEWS_MIN_RELEVANCE,
        "count": len(merged),
        "count_by_day": dict(sorted(por_dia.items(), reverse=True)),
        "new_this_run": len(candidates),
        "already_published": ya_publicados,
        "out_of_window": fuera_de_dia,
        "ttl_days": CANDIDATE_TTL_DAYS,
        "max_age_days": NEWS_MAX_AGE_DAYS,
        "errors": errors,
        "sources": sources_count,
        "elapsed_s": elapsed,
        "candidates": merged,
        "discarded_count": len(descartes),
        "discarded": descartes,
    }
    NEWS_CANDIDATES_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    log.info("JSON escrito: %s (dia %s; %d candidatos; nuevos %d, publicados fuera %d, "
             "fuera de ventana %d, por fecha %s)",
             NEWS_CANDIDATES_PATH, hoy, len(merged), len(candidates), ya_publicados,
             fuera_de_dia, payload["count_by_day"])


def run() -> None:
    log.info("=== Pipeline de noticias static/local iniciado ===")
    log.info("Dia editorial %s (%s). Solo entra lo publicado entre %s.",
             hoy_editorial(), f"UTC{NEWS_TZ.utcoffset(None).total_seconds() / 3600:+.0f}",
             ventana_frescura_texto())
    start = time.time()
    sources = load_sources()
    if not sources:
        # Sin fuentes no hay nada nuevo, pero la cola del dia anterior tampoco
        # puede quedarse: se reescribe el JSON con la purga aplicada para que
        # el panel muestre "hoy sin propuestas" y no lo de la semana pasada.
        log.warning("No hay fuentes activas en Supabase; se purga la cola del dia anterior.")
        write_candidates([], 1, 0, round(time.time() - start, 1), load_published_urls())
        return

    publicados = load_published_urls()
    existing = load_existing_urls(publicados)
    log.info("Articulos ya vistos/publicados: %d (publicados: %d)", len(existing), len(publicados))

    all_candidates: list[dict] = []
    errors = 0
    _aviso_sin_ia_dado = False
    for i, source in enumerate(sources):
        if len(all_candidates) >= MAX_TOTAL_PROPOSALS:
            break
        if _quota_agotada.is_set() and not NEWS_FALLBACK_SIN_IA:
            log.warning("Sin cuota de Gemini: se omiten las %d fuentes restantes.", len(sources) - i)
            break
        if _quota_agotada.is_set() and not _aviso_sin_ia_dado:
            _aviso_sin_ia_dado = True
            log.warning("Sin cuota de Gemini: las %d fuentes restantes siguen por RSS, "
                        "con fichas armadas sin modelo (marcadas `sin_ia`).", len(sources) - i)
        try:
            remaining = MAX_TOTAL_PROPOSALS - len(all_candidates)
            all_candidates.extend(process_source(source, existing)[:remaining])
        except Exception as exc:
            log.error("Error inesperado en '%s': %s", source.get("name", "?"), exc)
            errors += 1
        if i < len(sources) - 1:
            time.sleep(DELAY_BETWEEN_SOURCES)

    # Barrido propio despues de las fuentes: complementa, no reemplaza, y solo
    # con el cupo que sobre.
    try:
        all_candidates.extend(
            procesar_busqueda_general(existing, MAX_TOTAL_PROPOSALS - len(all_candidates))
        )
    except Exception as exc:
        log.error("Error en la busqueda general: %s", exc)
        errors += 1

    elapsed = round(time.time() - start, 1)
    write_candidates(all_candidates, errors, len(sources), elapsed, publicados)

    sb_insert_log({
        "source": "NEWS",
        "section": "Pipeline candidatos",
        "status": "success" if errors == 0 else "warning",
        "rows_processed": len(all_candidates),
        "message": (f"Pipeline JSON {hoy_editorial()}: {len(all_candidates)} candidatos nuevos, "
                    f"{len(_descartes)} descartados, {errors} errores, {elapsed}s"),
        "metadata": {"mode": "static_json", "path": str(NEWS_CANDIDATES_PATH),
                     "gemini_model": GEMINI_MODEL, "dia_editorial": str(hoy_editorial()),
                     "ventana_frescura": ventana_frescura_texto()},
        "updated_at": now_iso(),
    })
    if not all_candidates:
        log.warning("Ninguna nota nueva paso el filtro. Revisa la lista de descartadas "
                    "en el panel: ahi queda el motivo de cada una.")
    log.info("=== Finalizado: %d candidatos nuevos en %.1fs ===", len(all_candidates), elapsed)


if __name__ == "__main__":
    run()
