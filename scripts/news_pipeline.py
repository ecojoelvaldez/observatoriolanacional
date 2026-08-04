"""
Observatorio Estratégico La Nacional — Pipeline de Noticias v4 LOCAL/STATIC
============================================================================
Flujo:
  1. Calcula la VENTANA EDITORIAL del día: desde las 2:00 PM del día hábil
     anterior hasta ahora (hora de Santo Domingo). El lunes eso arrastra desde
     el viernes 2:00 PM, o sea viernes tarde + todo el fin de semana.
  2. Lee fuentes activas desde Supabase (news_sources). Esto es liviano.
  3. Lee cada página índice con Jina Reader.
  4. Gemini extrae URLs reales de artículos candidatos.
  5. Para cada artículo, lee la URL individual con Jina (en paralelo, 3 hilos).
     Si esa URL no abre, se busca el mismo titular en la web y se lee la
     cobertura de otro medio.
  6. Gemini genera resumen ejecutivo + lead + "por qué importa" + tipo de pieza
     + relevancia financiera (0-10).
  7. Se descartan de plano los temas que el analista no quiere ver: sector
     eléctrico, opinión/editorial/columna, variedades y notas de servicio
     (ver TEMAS_EXCLUIDOS y TIPOS_EXCLUIDOS).
  8. Se aceptan candidatos con relevancia >= NEWS_MIN_RELEVANCE, con un PISO
     institucional que el modelo no puede bajar (ver SEÑALES_RELEVANCIA).
  9. Barrido propio en la web sobre los temas que importan, además de las
     fuentes fijas, con el cupo que sobre.
 10. Se colapsan las notas que cuentan la misma noticia desde distintos medios.
 11. GARANTÍA DE PISO: si tras todo eso no hay al menos NEWS_MIN_CANDIDATES,
     se relaja en escalera (ver completar_minimo) hasta llenar la cuota.
 12. Gemini redacta el BRIEF del día sobre los candidatos ya elegidos: qué pasó,
     por qué importa y qué vigilar. Es lo primero que ve el analista.
 13. Escribe un archivo estático news_candidates.json en la raíz del repo, con
     los candidatos, el brief, la salud de cada fuente Y todo lo descartado
     junto con su motivo, para que el panel lo muestre y el analista pueda
     recuperarlo.

Redes de seguridad, todas por la misma razón: que ninguna noticia importante
obligue a buscarla a mano.
  * Piso de relevancia por señal institucional — el juicio del modelo no es el
    único filtro. Reguladores, política monetaria y bancos dominicanos entran
    aunque el modelo los haya puntuado bajo o marcado como no noticiosos.
  * Registro de descartes — nada desaparece en silencio.
  * Rescate por titular — que un medio bloquee al lector no borra la noticia.
  * Búsqueda general — no depender de que la nota salga en un índice concreto.
  * Deduplicación por tema — la misma noticia desde dos medios es una sola.
  * Filtro de páginas inválidas — errores 404 y muros de pago no son noticia.
  * Corta-circuito de cuota — si Gemini se agota, terminar limpio y rápido.
  * Piso de candidatos — un día flojo no puede dejar al analista sin material.
  * Salud de fuentes — se anota qué rindió cada fuente, corrida a corrida, para
    que sustituir una fuente muerta sea una decisión con datos y no una
    corazonada (data/news_source_health.json).

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
  NEWS_MIN_RELEVANCE=6        # umbral 0-10 de relevancia financiera
  NEWS_MIN_CANDIDATES=6       # piso de candidatos por corrida (garantia)
  NEWS_WINDOW_CUTOFF_HOUR=14  # "la tarde de ayer" empieza a esta hora (RD)
  NEWS_WINDOW_MAX_DAYS=5      # tope de arrastre de la ventana (feriados largos)
  ARTICLE_WORKERS=3           # hilos por fuente para leer articulos
  NEWS_SEARCH_ENABLED=true    # barrido propio en la web ademas de las fuentes
  NEWS_SEARCH_QUERIES=...     # consultas separadas por "|" (ver DEFAULT_SEARCH_QUERIES)
  NEWS_SEARCH_MAX_PER_QUERY=4
  NEWS_RESCUE_ENABLED=true    # buscar el titular en otro medio si la URL no abre
  NEWS_BRIEF_ENABLED=true     # brief del dia redactado por Gemini
  NEWS_HEALTH_PATH=data/news_source_health.json
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
from datetime import datetime, timezone, timedelta, time as _time
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse, urljoin, parse_qsl, urlencode, urlunparse

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
NEWS_HEALTH_PATH = Path(os.getenv("NEWS_HEALTH_PATH", "data/news_source_health.json"))

JINA_READER_BASE = "https://r.jina.ai/"
GEMINI_ENDPOINT = f"https://generativelanguage.googleapis.com/v1beta/models/{GEMINI_MODEL}:generateContent"

FETCH_TIMEOUT = 35
GEMINI_TIMEOUT = 45
MAX_INDEX_MARKDOWN_CHARS = 9000
MAX_ARTICLE_MARKDOWN_CHARS = 12000
MIN_CONTENT_CHARS = 250
DELAY_BETWEEN_SOURCES = 1.5
MAX_TOTAL_PROPOSALS = int(os.getenv("MAX_TOTAL_PROPOSALS", "30"))
NEWS_MIN_RELEVANCE = int(os.getenv("NEWS_MIN_RELEVANCE", "6"))
# Piso de candidatos por corrida. El analista abre el panel esperando material
# de trabajo; un dia flojo no puede dejarlo con dos notas.
NEWS_MIN_CANDIDATES = int(os.getenv("NEWS_MIN_CANDIDATES", "6"))
ARTICLE_WORKERS = max(1, int(os.getenv("ARTICLE_WORKERS", "3")))

# --- Ventana editorial -----------------------------------------------------
# El resumen se arma con lo de la tarde de ayer en adelante: lo de la mañana de
# ayer ya salio en el resumen de ayer. Antes esto era una edad maxima en horas
# (72h), que es otra cosa: dejaba entrar notas de anteayer y, peor, no
# distinguia el lunes — y el lunes es justo el dia que necesita mas arrastre,
# porque debe cubrir la tarde del viernes y todo el fin de semana.
TZ_RD = timezone(timedelta(hours=-4))  # Santo Domingo, sin horario de verano
NEWS_WINDOW_CUTOFF_HOUR = int(os.getenv("NEWS_WINDOW_CUTOFF_HOUR", "14"))
NEWS_WINDOW_MAX_DAYS = int(os.getenv("NEWS_WINDOW_MAX_DAYS", "5"))
# Cuanto se ensancha la ventana en cada peldaño de la escalera de rescate.
NEWS_WINDOW_RELAX_HOURS = int(os.getenv("NEWS_WINDOW_RELAX_HOURS", "24"))
CANDIDATE_TTL_DAYS = int(os.getenv("CANDIDATE_TTL_DAYS", "3"))
# Titulares que se piden por fuente. El lunes se sube el cupo porque la ventana
# cubre tres dias de publicaciones en vez de uno.
MAX_HEADLINES_PER_SOURCE = int(os.getenv("MAX_HEADLINES_PER_SOURCE", "6"))
MAX_HEADLINES_LUNES = int(os.getenv("MAX_HEADLINES_LUNES", "9"))
NEWS_BRIEF_ENABLED = os.getenv("NEWS_BRIEF_ENABLED", "true").strip().lower() not in ("0", "false", "no")
# El free tier de Gemini limita las peticiones por minuto; espaciar las
# llamadas evita tormentas de 429 que agotan los reintentos y dejan fuentes
# sin procesar (observado en el run del 2026-07-06).
GEMINI_MIN_INTERVAL = float(os.getenv("GEMINI_MIN_INTERVAL", "6"))

CATEGORY_ALLOWED = {"Monetario", "Financiero", "Regulatorio", "Economia", "Global"}
TRACKING_PARAMS = {"utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content", "fbclid", "gclid", "mc_cid", "mc_eid"}

# --- Busqueda general en la web (s.jina.ai) --------------------------------
# Barrido propio ademas de las fuentes fijas, para no depender de que la nota
# aparezca en un indice concreto. Usa el mismo servicio que ya lee articulos,
# asi que no agrega credenciales ni consume cuota de Gemini para buscar.
JINA_SEARCH_BASE = "https://s.jina.ai/"
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
# Segunda tanda: solo se usa cuando la corrida se queda corta del piso de
# candidatos. Abre el abanico sin ensuciar el dia normal.
DEFAULT_SEARCH_QUERIES_EXTRA = [
    "Junta Monetaria resolución República Dominicana",
    "crédito hipotecario vivienda República Dominicana",
    "Ministerio de Hacienda deuda pública República Dominicana",
    "calificación crediticia República Dominicana Fitch Moody's S&P",
    "reservas internacionales dólar peso dominicano mercado cambiario",
    "Reserva Federal tasas de interés impacto América Latina",
    "fintech pagos digitales inclusión financiera República Dominicana",
]
NEWS_SEARCH_QUERIES_EXTRA = [
    q.strip() for q in os.getenv("NEWS_SEARCH_QUERIES_EXTRA", "|".join(DEFAULT_SEARCH_QUERIES_EXTRA)).split("|") if q.strip()
]
# Recuperar por titular cuando no se puede leer el cuerpo en la fuente original.
NEWS_RESCUE_ENABLED = os.getenv("NEWS_RESCUE_ENABLED", "true").strip().lower() not in ("0", "false", "no")

# --- Piso de relevancia por señal institucional ----------------------------
# El modelo venia descartando noticias que para una entidad financiera son
# obvias (la renuncia del Superintendente de Bancos, una reunion entre
# Banreservas y Popular sobre el sistema financiero). El juicio del modelo no
# puede ser el unico filtro: estas reglas fijan un piso de relevancia que el
# modelo no puede bajar. Si el texto toca una de estas señales, la nota entra.
SEÑALES_RELEVANCIA = [
    # (relevancia minima, etiqueta, terminos)
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

# --- Temas que el analista no quiere ver -----------------------------------
# Tres familias distintas, y conviene no mezclarlas:
#   * TEMAS_EXCLUIDOS  -> el asunto no interesa (sector electrico, variedades).
#     Manda incluso sobre el piso institucional: "el Superintendente de
#     Electricidad anuncia..." dispara la señal "regulador" por la palabra
#     "superintendente", y no es una noticia de este observatorio.
#   * SECCIONES_EXCLUIDAS -> la URL delata la seccion (/opinion/, /deportes/).
#     Es el filtro mas barato y el mas fiable: no gasta ni una llamada.
#   * TIPOS_EXCLUIDOS  -> el modelo clasifica la pieza y decimos que no es
#     noticia (columna de opinion, publirreportaje, nota de servicio).
TEMAS_EXCLUIDOS = [
    ("electricidad", [
        "apagon", "apagones", "edeeste", "edenorte", "edesur", "cdeee", "eted",
        "punta catalina", "generacion electrica", "energia electrica", "sector electrico",
        "tarifa electrica", "pacto electrico", "subsidio electrico", "distribuidoras electricas",
        "kilovatio", "megavatio", "kwh", "superintendencia de electricidad",
        "superintendente de electricidad", "sistema electrico nacional",
        "generadoras electricas", "electricidad",
    ]),
    ("variedades", [
        "farandula", "espectaculo", "espectaculos", "entretenimiento", "horoscopo",
        "concurso de belleza", "reina de belleza", "telenovela", "celebridad",
        "beisbol", "futbol", "baloncesto", "olimpicos", "loteria", "quiniela",
    ]),
    ("sucesos", [
        "homicidio", "asesinato", "tiroteo", "narcotrafico", "incautacion de droga",
        "accidente de transito", "feminicidio", "reo", "carcel preventiva",
    ]),
]

SECCIONES_EXCLUIDAS = [
    "/opinion", "/opiniones", "/editorial", "/editoriales", "/columna", "/columnas",
    "/columnistas", "/punto-de-vista", "/blogs", "/blog/",
    "/variedades", "/gente", "/farandula", "/entretenimiento", "/espectaculos",
    "/deportes", "/estilo", "/vida-y-estilo", "/revista", "/sociales", "/tendencias",
    "/sucesos", "/policiales", "/judicial",
    "/electricidad", "/energia",
    "/horoscopo", "/loteria", "/loterias", "/servicios/loterias",
]

# Piezas que no son noticia dura. "servicio" es la nota rutinaria que se repite
# igual cada dia o cada semana (la tasa del dolar de hoy, el precio semanal de
# los combustibles): no es falsa ni irrelevante, pero el analista ya sabe que
# existe y no necesita que se la propongan como hallazgo del dia. Queda en la
# reserva, por si un dia flojo hace falta completar la cuota.
TIPOS_EXCLUIDOS = {"opinion", "publirreportaje", "servicio"}
TIPOS_VALIDOS = {"noticia", "analisis", "opinion", "publirreportaje", "servicio"}

# Titulares de nota rutinaria: el modelo a veces las etiqueta "noticia" porque
# traen cifras. Se reconocen por el patron del titular.
PATRONES_RUTINA = [
    r"\bprecio del dolar\b.*\bhoy\b", r"\bdolar hoy\b", r"\btasa del dolar\b",
    r"\bprecios? de (los )?combustibles?\b", r"\bcombustibles\b.*\b(semana|congel|suben|bajan)\b",
    # El aviso semanal de combustibles: "Gobierno congela precios de gasolinas,
    # gasoil y GLP". Sale igual todos los viernes y no es un hallazgo del dia.
    r"\b(congel|mantien|sube|baja|varia)\w*\b.{0,40}\bprecios?\b.{0,40}\b(gasolina|gasoil|glp|gas natural)",
    r"\bprecios?\b.{0,30}\b(gasolinas?|gasoil|glp)\b.{0,40}\b(semana|congel)",
    r"\bhoroscopo\b", r"\bloteri", r"\bresultados de las loterias\b",
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
# Ventana editorial
# ---------------------------------------------------------------------------
DIAS_ES = ["lunes", "martes", "miercoles", "jueves", "viernes", "sabado", "domingo"]


def _ultima_corrida() -> datetime | None:
    """Cuando corrio el pipeline por ultima vez, segun el JSON en el repo.

    Sirve para cubrir el hueco de un feriado: si el lunes fue dia libre y no
    hubo corrida, el martes la ventana arrastra desde el viernes sola, sin
    tener que mantener un calendario de feriados dominicanos.
    """
    try:
        if not NEWS_CANDIDATES_PATH.exists():
            return None
        data = json.loads(NEWS_CANDIDATES_PATH.read_text(encoding="utf-8"))
        return parse_iso_date(data.get("generated_at"))
    except Exception:
        return None


def calcular_ventana(ahora: datetime | None = None) -> dict:
    """Desde las 2:00 PM del dia habil anterior hasta ahora, hora de RD.

    Martes a viernes: desde ayer 2:00 PM (la mañana de ayer ya se resumio).
    Lunes: el dia habil anterior es el viernes, asi que la ventana arrastra
    viernes 2:00 PM + sabado + domingo. Ese es justo el dia que venia flojo.
    Si la corrida anterior es mas vieja que ese corte (feriado, workflow caido),
    la ventana se estira hasta ella para no perder nada en el hueco.
    """
    ahora_utc = (ahora or datetime.now(timezone.utc)).astimezone(timezone.utc)
    ahora_rd = ahora_utc.astimezone(TZ_RD)

    dia_anterior = ahora_rd.date() - timedelta(days=1)
    while dia_anterior.weekday() >= 5:  # 5 sabado, 6 domingo
        dia_anterior -= timedelta(days=1)
    inicio = datetime.combine(dia_anterior, _time(NEWS_WINDOW_CUTOFF_HOUR), tzinfo=TZ_RD)

    motivo = "tarde de ayer en adelante"
    if ahora_rd.weekday() == 0:
        motivo = "lunes: desde la tarde del viernes, con el fin de semana completo"

    previa = _ultima_corrida()
    if previa and previa < inicio.astimezone(timezone.utc):
        inicio = previa.astimezone(TZ_RD)
        motivo = "estirada hasta la corrida anterior (hubo un hueco sin pipeline)"

    tope = ahora_rd - timedelta(days=NEWS_WINDOW_MAX_DAYS)
    if inicio < tope:
        inicio = tope
        motivo = f"recortada al tope de {NEWS_WINDOW_MAX_DAYS} dias"

    return {
        "inicio": inicio.astimezone(timezone.utc),
        "fin": ahora_utc,
        "es_lunes": ahora_rd.weekday() == 0,
        "dia": DIAS_ES[ahora_rd.weekday()],
        "motivo": motivo,
        "horas": round((ahora_utc - inicio.astimezone(timezone.utc)).total_seconds() / 3600, 1),
        "inicio_local": inicio.strftime("%Y-%m-%d %H:%M"),
        "fin_local": ahora_rd.strftime("%Y-%m-%d %H:%M"),
    }


# La ventana se calcula una vez por corrida y se relaja, si hace falta, en la
# escalera de rescate. Vive en un dict para que ensancharla no obligue a pasarla
# por parametro por todo el pipeline.
VENTANA: dict = {}


def fuera_de_ventana(published_at: str | None) -> bool:
    """True si la nota quedo fuera de la ventana editorial.

    Sin fecha NO es motivo de descarte: muchos medios dominicanos no la
    publican en el markdown y tirarlas por eso perderia noticias buenas. Se
    marcan con `sin_fecha` para que el analista lo sepa.
    """
    dt = parse_iso_date(published_at)
    if not dt or not VENTANA:
        return False
    if dt > VENTANA["fin"] + timedelta(hours=6):
        # Fecha futura: casi siempre es basura del CMS, no se castiga.
        return False
    return dt < VENTANA["inicio"]


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


def tema_excluido(*textos: str) -> str | None:
    """Etiqueta del tema vetado que aparece en el texto, o None.

    Se mira sobre todo el titular y el resumen: una mencion de pasada al sector
    electrico dentro de una nota macro no deberia tumbarla, pero una nota cuyo
    titular habla de apagones no es materia de este observatorio.
    """
    blob = norm_texto(" ".join(t for t in textos if t))
    if not blob:
        return None
    for etiqueta, terminos in TEMAS_EXCLUIDOS:
        if any(re.search(rf"\b{re.escape(term)}", blob) for term in terminos):
            return etiqueta
    return None


def seccion_excluida(url: str) -> str | None:
    """Seccion vetada segun la ruta de la URL, o None.

    El filtro mas barato del pipeline: se aplica antes de leer el articulo, asi
    que una columna de opinion no gasta ni una lectura ni una llamada a Gemini.
    """
    try:
        ruta = urlparse(ensure_scheme(url)).path.lower()
    except Exception:
        return None
    for seccion in SECCIONES_EXCLUIDAS:
        if seccion in ruta:
            return seccion.strip("/")
    return None


def es_nota_rutinaria(titulo: str) -> bool:
    """True si el titular es de nota de servicio que se repite cada dia/semana."""
    blob = norm_texto(titulo)
    return any(re.search(p, blob) for p in PATRONES_RUTINA)


def tokens_titular(titulo: str) -> set[str]:
    """Tokens significativos de un titular, sin palabras vacias ni ruido corto."""
    return {t for t in norm_texto(titulo).split() if len(t) > 2 and t not in STOPWORDS}


def señales_detectadas(*textos: str) -> tuple[int, list[str]]:
    """Piso de relevancia y etiquetas segun las señales institucionales.

    Devuelve (piso, etiquetas). piso = 0 si el texto no toca ninguna señal.
    """
    blob = norm_texto(" ".join(t for t in textos if t))
    if not blob:
        return 0, []
    piso, etiquetas = 0, []
    for minimo, etiqueta, terminos in SEÑALES_RELEVANCIA:
        if any(term in blob for term in terminos):
            etiquetas.append(etiqueta)
            piso = max(piso, minimo)
    if piso and any(v in blob for v in VERBOS_HECHO):
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
    # url_alterna es opcional: si la columna no existe todavia (migracion sin
    # aplicar), se vuelve a pedir sin ella en vez de tumbar la corrida.
    try:
        rows = sb_get("news_sources", params={
            "enabled": "eq.true",
            "select": "source_key,name,url,url_alterna,enabled",
        })
    except Exception:
        log.info("news_sources sin columna url_alterna; se lee el esquema anterior")
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
# Candidatos completos apartados por un criterio de calidad (ventana, umbral,
# rutina). Son la reserva de la que tira completar_minimo si el dia viene flojo.
_reserva: list[dict] = []

MOTIVO_TEXTO = {
    "relevancia_baja": "El modelo le puso poca relevancia financiera",
    "modelo_irrelevante": "El modelo la marco como no noticiosa",
    "sin_cuerpo": "No se pudo leer el cuerpo del articulo",
    "sin_detalle": "El articulo no devolvio datos utiles",
    "vieja": "Publicada fuera de la ventana editorial del dia",
    "duplicada": "Ya cubierta por otra nota del mismo tema",
    "tema_vetado": "Tema que el analista pidio no ver",
    "seccion_vetada": "Viene de una seccion que no se cubre",
    "opinion": "Es opinion, columna o editorial, no noticia",
    "publirreportaje": "Es contenido patrocinado, no noticia",
    "rutinaria": "Nota de servicio que se repite cada dia o cada semana",
}

# Motivos que la escalera de rescate puede revertir cuando faltan candidatos:
# son notas reales y ya procesadas, apartadas por un criterio de calidad, no
# por ser basura. Se listan en el orden en que se prefiere recuperarlas.
MOTIVOS_RESCATABLES = ["vieja", "relevancia_baja", "rutinaria", "modelo_irrelevante"]


def registrar_descarte(motivo: str, titulo: str, url: str, fuente: str = "",
                       relevancia=None, señales=None, detalle: str = "",
                       candidato: dict | None = None) -> None:
    if not (titulo or url):
        return
    with _descartes_lock:
        if any(d.get("url") == url for d in _descartes):
            return
        if candidato and motivo in MOTIVOS_RESCATABLES:
            # Se guarda el candidato completo, no solo el titular: si al final
            # falta cupo, recuperarlo no cuesta ni una lectura ni una llamada.
            _reserva.append({**candidato, "motivo_original": motivo})
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
# Busqueda en la web (s.jina.ai)
# ---------------------------------------------------------------------------
def buscar_en_web(consulta: str, max_resultados: int) -> list[dict]:
    """Devuelve [{title, url}] para una consulta. Lista vacia si algo falla."""
    try:
        r = httpx.get(
            JINA_SEARCH_BASE + consulta,
            headers={"Accept": "text/markdown", "X-Return-Format": "markdown"},
            timeout=FETCH_TIMEOUT,
            follow_redirects=True,
        )
        if r.status_code != 200:
            log.warning("Jina search HTTP %s para '%s'", r.status_code, consulta[:60])
            return []
        texto = r.text
    except Exception as exc:
        log.warning("Jina search error (%s) para '%s'", type(exc).__name__, consulta[:60])
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

DESCARTA SIN EXCEPCION (el analista pidio expresamente no verlos):
- OPINION en cualquier forma: columnas, editoriales, analisis de autor, cartas, "punto de vista".
  Si el texto es la postura de alguien y no un hecho ocurrido, no lo devuelvas.
- SECTOR ELECTRICO: apagones, EDEs, generacion, tarifa electrica, Punta Catalina, pacto electrico.
- VARIEDADES: farandula, entretenimiento, deportes, sociales, estilo de vida, loterias, horoscopos.
- NOTAS DE SERVICIO que se repiten igual cada dia o cada semana: "precio del dolar hoy",
  precios semanales de combustibles, resultados de loterias.
- Publirreportajes y contenido patrocinado, navegacion, relleno, tecnologia de consumo,
  sucesos, politica sin componente economico, seguridad sin impacto financiero cuantificable.

Devuelve SOLO JSON valido con esta estructura exacta:
{"headlines":[{"title":"titular exacto","link":"URL del articulo individual","category":"Economia"}]}

Reglas:
- Devuelve entre 0 y {max_titulares} titulares. Menos y mejores es preferible a mas y debiles.
- category debe ser exactamente una de: Monetario, Financiero, Regulatorio, Economia, Global.
- link debe ser la URL del articulo individual. Si no hay enlace claro, omite el titular.
- No inventes enlaces.

VENTANA: solo interesan hechos ocurridos o publicados en este periodo: {ventana}.
Si el indice muestra fecha y la nota es claramente anterior, omitela.

Markdown de pagina indice:
"""

ARTICLE_PROMPT = """Eres un analista de inteligencia financiera dominicana que trabaja para una asociacion de ahorros y prestamos.

Recibes el markdown de un ARTICULO individual. Extrae informacion util para que un analista lo apruebe o descarte en un panel editorial.

Devuelve SOLO JSON valido con esta estructura exacta:
{
  "title": "titulo completo del articulo",
  "summary": "resumen ejecutivo en 2 oraciones, en espanol, con impacto para el sector financiero dominicano",
  "body": "lead propio de 1 parrafo, 45-90 palabras, basado en el cuerpo de la noticia, no repitas el titulo y no copies texto largo literalmente",
  "por_que_importa": "1 oracion (max 25 palabras) sobre el efecto concreto para una asociacion de ahorros y prestamos dominicana",
  "tipo": "noticia",
  "published_at": "fecha ISO 8601 si aparece, o null",
  "category": "Economia",
  "relevance": 7,
  "relevant": true
}

Reglas:
- tipo debe ser exactamente uno de: noticia, analisis, opinion, publirreportaje, servicio.
  * "opinion": columna, editorial, carta o analisis firmado donde lo que se cuenta es la
    postura del autor y no un hecho ocurrido. El analista NO quiere opinion: clasificala bien.
  * "publirreportaje": contenido pagado o promocional de una empresa.
  * "servicio": nota rutinaria que se repite igual cada dia o cada semana (precio del dolar
    de hoy, precios semanales de combustibles, resultados de loterias).
  * "analisis": reportaje con datos y contexto propio, escrito por la redaccion, no por un columnista.
  * "noticia": hecho ocurrido, reportado. Ante la duda entre noticia y analisis, usa "noticia".
- category debe ser exactamente una de: Monetario, Financiero, Regulatorio, Economia, Global.
- relevance es un entero 0-10 que mide el impacto para el SECTOR FINANCIERO dominicano:
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

TEMAS QUE NO SE CUBREN (aunque traigan cifras): sector electrico (apagones, EDEs, generacion,
tarifa, Punta Catalina), variedades, deportes, farandula, sociales, loterias y horoscopos.
Si el articulo trata principalmente de eso, devuelve {"relevant": false}.

- Si el articulo no tiene contenido noticioso util, devuelve {"relevant": false}.
- Si no encuentras fecha, usa null; no inventes fechas.
- El body debe ser un parrafo informativo, no una lista ni el titular.
- por_que_importa se escribe para La Nacional (asociacion de ahorros y prestamos): fondeo,
  costo del dinero, demanda de credito hipotecario, cumplimiento, competencia. Nunca lo dejes vacio.

Markdown del articulo:
"""


def cupo_titulares() -> int:
    """Titulares por fuente. El lunes se pide mas: la ventana cubre 3 dias."""
    return MAX_HEADLINES_LUNES if VENTANA.get("es_lunes") else MAX_HEADLINES_PER_SOURCE


def texto_ventana() -> str:
    if not VENTANA:
        return "las ultimas 24 horas"
    return (f"desde {VENTANA['inicio_local']} hasta {VENTANA['fin_local']} "
            f"(hora de Republica Dominicana)")


def extract_headlines(index_markdown: str) -> list[dict]:
    cupo = cupo_titulares()
    # replace y no format: el prompt lleva JSON con llaves literales.
    prompt = (HEADLINE_PROMPT
              .replace("{max_titulares}", str(cupo))
              .replace("{ventana}", texto_ventana()))
    parsed = gemini_json(prompt + index_markdown, max_tokens=1600 + 100 * cupo)
    if not parsed:
        return []
    headlines = parsed.get("headlines", [])
    return headlines[:cupo] if isinstance(headlines, list) else []


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
    parsed["por_que_importa"] = limpiar_texto(parsed.get("por_que_importa"))

    tipo = norm_texto(parsed.get("tipo") or "noticia").replace(" ", "")
    if tipo not in TIPOS_VALIDOS:
        tipo = "noticia"
    # El modelo etiqueta "noticia" la tasa del dolar del dia porque trae cifras;
    # el patron del titular la reconoce como lo que es.
    if tipo == "noticia" and es_nota_rutinaria(titulo):
        tipo = "servicio"
    parsed["tipo"] = tipo

    # Segunda red: a veces el lector trae algo de texto y el modelo igual
    # describe la pagina de error. Paso el 2026-07-30 con "Error 404 | Hoy
    # Digital", propuesto con relevancia 7.
    if es_pagina_invalida(titulo, parsed.get("summary")):
        log.info("  -> el resumen describe una pagina de error: %s", titulo[:70])
        registrar_descarte("sin_cuerpo", titulo, url, fuente,
                           detalle="El modelo resumio una pagina de error")
        return None

    # Temas vetados: van ANTES del piso institucional a proposito. "El
    # Superintendente de Electricidad anuncia..." dispara la señal "regulador"
    # por la palabra superintendente, y aun asi no es materia del observatorio.
    veto = tema_excluido(titulo, parsed.get("summary"))
    if veto:
        log.info("  -> tema vetado (%s): %s", veto, titulo[:70])
        registrar_descarte("tema_vetado", titulo, url, fuente,
                           detalle=f"Tema apartado por politica editorial: {veto}")
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

    # Opinion y publirreportaje se van sin apelacion: no son hechos, y el piso
    # institucional no deberia colar una columna sobre el Banco Central.
    if tipo in ("opinion", "publirreportaje"):
        log.info("  -> %s, no es noticia: %s", tipo, titulo[:70])
        registrar_descarte(tipo, titulo, url, fuente, relevance, etiquetas)
        return None

    # Los motivos que siguen NO tiran la nota aqui: la marcan. build_candidate
    # termina de armarla y la manda a la reserva, para que la escalera de
    # rescate pueda recuperarla sin volver a leer ni a llamar a Gemini.
    if not parsed.get("relevant", True) and piso < NEWS_MIN_RELEVANCE:
        parsed["_motivo_reserva"] = "modelo_irrelevante"
    elif relevance < NEWS_MIN_RELEVANCE:
        log.info("  -> relevancia %d < %d, a reserva: %s", relevance, NEWS_MIN_RELEVANCE, titulo[:70])
        parsed["_motivo_reserva"] = "relevancia_baja"
    elif tipo == "servicio":
        log.info("  -> nota de servicio, a reserva: %s", titulo[:70])
        parsed["_motivo_reserva"] = "rutinaria"
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


def etiqueta_ventana(published_at: str | None) -> str:
    """"hoy" / "ayer tarde" / "fin de semana"... para el panel.

    El analista prioriza distinto lo de hoy y lo del viernes por la tarde; sin
    esta etiqueta tendria que leer cada fecha ISO a mano.
    """
    dt = parse_iso_date(published_at)
    if not dt:
        return "sin fecha"
    local = dt.astimezone(TZ_RD)
    hoy = datetime.now(TZ_RD).date()
    dias = (hoy - local.date()).days
    if dias <= 0:
        return "hoy"
    if dias == 1:
        return "ayer tarde" if local.hour >= NEWS_WINDOW_CUTOFF_HOUR else "ayer"
    if local.weekday() >= 5:
        return "fin de semana"
    return f"{DIAS_ES[local.weekday()]} {local.strftime('%d/%m')}"


# Muchas fuentes se guardaron desde el panel sin nombre y quedaron todas como
# "Fuente guardada": en el JSON del 2026-08-04, 13 de 18 candidatos decian eso,
# asi que el analista no sabia de que medio venia ninguno.
NOMBRES_POR_DOMINIO = {
    "hoy.com.do": "Hoy Digital",
    "eldinero.com.do": "El Dinero",
    "diariolibre.com": "Diario Libre",
    "elcaribe.com.do": "El Caribe",
    "listindiario.com": "Listín Diario",
    "acento.com.do": "Acento",
    "elnuevodiario.com.do": "El Nuevo Diario",
    "bancentral.gov.do": "Banco Central (BCRD)",
    "sb.gob.do": "Superintendencia de Bancos",
    "hacienda.gob.do": "Ministerio de Hacienda",
    "bloomberglinea.com": "Bloomberg Línea",
    "forbes.com.do": "Forbes RD",
    "revistamercado.do": "Revista Mercado",
    "federalreserve.gov": "Reserva Federal (EE. UU.)",
}


def nombre_fuente(source: dict, url: str = "") -> str:
    nombre = (source.get("name") or "").strip()
    if nombre and nombre.lower() not in ("fuente guardada", "desconocida", ""):
        return nombre
    dominio = urlparse(ensure_scheme(url or source.get("url") or "")).netloc.lower().replace("www.", "")
    if dominio in NOMBRES_POR_DOMINIO:
        return NOMBRES_POR_DOMINIO[dominio]
    return dominio or nombre or source.get("source_key") or "desconocida"


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


def load_existing_urls() -> set[str]:
    urls: set[str] = set()
    # 1) Ya publicados en Supabase: pequeño y necesario para no reproponer lo publicado.
    cutoff = (datetime.now(timezone.utc) - timedelta(days=14)).isoformat()
    try:
        rows = sb_get("news_items", params={"select": "url", "updated_at": f"gte.{cutoff}"})
        urls.update(canonicalize_url(r.get("url") or "") for r in rows if r.get("url"))
    except Exception as exc:
        log.info("No se pudo leer news_items para deduplicar: %s", exc)

    # 2) Candidatos estáticos previos en el repo.
    try:
        if NEWS_CANDIDATES_PATH.exists():
            data = json.loads(NEWS_CANDIDATES_PATH.read_text(encoding="utf-8"))
            urls.update(canonicalize_url(x.get("url") or "") for x in data.get("candidates", []) if x.get("url"))
    except Exception as exc:
        log.info("No se pudo leer JSON previo para deduplicar: %s", exc)

    urls.discard(None)
    return urls


def build_candidate(source: dict, headline: dict, base_url: str) -> dict | None:
    headline_title = limpiar_texto(headline.get("title"))
    link = canonicalize_url(headline.get("link") or "", base_url)
    if not headline_title or not link:
        return None

    fuente_nombre = nombre_fuente(source, link)

    # Filtros baratos primero: descartar por seccion o por titular no cuesta ni
    # una lectura ni una llamada a Gemini.
    seccion = seccion_excluida(link)
    if seccion:
        log.info("  -> seccion vetada (%s): %s", seccion, headline_title[:70])
        registrar_descarte("seccion_vetada", headline_title, link, fuente_nombre,
                           detalle=f"La URL cae en la seccion /{seccion}")
        return None
    veto_titular = tema_excluido(headline_title)
    if veto_titular:
        log.info("  -> tema vetado en el titular (%s): %s", veto_titular, headline_title[:70])
        registrar_descarte("tema_vetado", headline_title, link, fuente_nombre,
                           detalle=f"Tema apartado por politica editorial: {veto_titular}")
        return None

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

    details = extract_article_details(article_markdown, headline_title, link, fuente_nombre)
    if not details:
        log.info("  -> articulo sin detalle util: %s", headline_title[:70])
        return None

    published_at = details.get("published_at")
    title = limpiar_texto(details.get("title") or headline_title)
    candidato = {
        "id": stable_id(link),
        "source_key": source.get("source_key", ""),
        "source_name": fuente_nombre,
        "url": link,
        "title": title,
        "summary": limpiar_texto(details.get("summary")),
        "body": limpiar_texto(details.get("body")),
        "por_que_importa": limpiar_texto(details.get("por_que_importa")),
        "tipo": details.get("tipo") or "noticia",
        "category": normalize_category(details.get("category") or headline.get("category")),
        "relevance": details.get("relevance", NEWS_MIN_RELEVANCE),
        "señales": details.get("señales") or [],
        "rescatado_por_regla": bool(details.get("rescatado_por_regla")),
        "cuerpo_recuperado": recuperado,
        "published_at": published_at,
        "sin_fecha": not bool(parse_iso_date(published_at)),
        "ventana": etiqueta_ventana(published_at),
        "fetched_at": now_iso(),
        "status": "pending",
    }

    motivo = details.get("_motivo_reserva")
    if fuera_de_ventana(published_at) and not motivo:
        motivo = "vieja"
    if motivo:
        log.info("  -> a reserva (%s): %s", motivo, title[:70])
        registrar_descarte(motivo, title, link, fuente_nombre,
                           candidato["relevance"], candidato["señales"],
                           detalle=(f"published_at={published_at}" if motivo == "vieja"
                                    else MOTIVO_TEXTO.get(motivo, motivo)),
                           candidato=candidato)
        return None
    return candidato


def process_source(source: dict, existing_urls: set[str]) -> list[dict]:
    name = nombre_fuente(source)
    base_url = (source.get("url") or "").strip()
    telemetria = _telemetria_fuente(source, name)
    if not base_url:
        log.warning("Fuente '%s' sin URL, se omite", name)
        telemetria["fallo"] = "sin_url"
        return []

    log.info("Procesando fuente: %s (%s)", name, base_url)
    index_markdown = fetch_with_jina(base_url, MAX_INDEX_MARKDOWN_CHARS)
    # Segunda puerta para las fuentes institucionales: el BCRD y la SIB son las
    # que mas importan y las que peor se dejan leer. Si su indice no abre, se
    # prueba la portada u otra ruta antes de darlas por perdidas.
    alterna = (source.get("url_alterna") or "").strip()
    if not index_markdown and alterna:
        log.info("  -> indice principal ilegible, pruebo la alterna: %s", alterna)
        index_markdown = fetch_with_jina(alterna, MAX_INDEX_MARKDOWN_CHARS)
        if index_markdown:
            base_url = alterna
            telemetria["via_alterna"] = True
    if not index_markdown:
        log.info("  -> Jina no devolvio indice util")
        telemetria["fallo"] = "indice_ilegible"
        return []
    telemetria["indice_leido"] = True

    headlines = extract_headlines(index_markdown)
    telemetria["titulares"] = len(headlines)
    if not headlines:
        log.info("  -> Gemini no extrajo titulares utiles")
        telemetria["fallo"] = "sin_titulares"
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
        seen_in_batch.add(link)
        pending.append(headline)

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
    candidates = candidates[:cupo_titulares()]
    telemetria["candidatos"] = len(candidates)
    if not candidates and not telemetria.get("fallo"):
        telemetria["fallo"] = "titulares_sin_candidato"
    log.info("  -> %d candidatos nuevos de %s", len(candidates), name)
    return candidates


# ---------------------------------------------------------------------------
# Salud de fuentes
# Sustituir una fuente era una corazonada: nadie sabia cuales llevaban semanas
# sin aportar una sola nota. Aqui se anota, corrida a corrida, en que peldaño
# falla cada una — no se pudo leer el indice / se leyo pero no hubo titulares /
# hubo titulares pero ninguno paso el filtro — y se acumula en un historico.
# Con eso, decidir que fuente se cambia deja de ser opinion.
# ---------------------------------------------------------------------------
_telemetria: list[dict] = []
_telemetria_lock = threading.Lock()

FALLO_TEXTO = {
    "sin_url": "La fuente no tiene URL configurada",
    "indice_ilegible": "No se pudo leer la pagina indice (bloqueo, JS o caida)",
    "sin_titulares": "El indice se leyo pero no traia titulares del sector",
    "titulares_sin_candidato": "Hubo titulares pero ninguno paso el filtro",
}


def _telemetria_fuente(source: dict, nombre: str) -> dict:
    entrada = {
        "source_key": source.get("source_key", ""),
        "name": nombre,
        "url": source.get("url", ""),
        "indice_leido": False,
        "titulares": 0,
        "candidatos": 0,
        "fallo": "",
    }
    with _telemetria_lock:
        _telemetria.append(entrada)
    return entrada


def actualizar_salud_fuentes() -> dict:
    """Acumula la telemetria de esta corrida en el historico y señala las
    fuentes candidatas a sustitucion (las que llevan muchas corridas en cero).
    """
    historico = {"corridas": 0, "actualizado_en": "", "fuentes": {}}
    try:
        if NEWS_HEALTH_PATH.exists():
            historico = json.loads(NEWS_HEALTH_PATH.read_text(encoding="utf-8"))
    except Exception as exc:
        log.info("No se pudo leer el historico de fuentes: %s", exc)

    fuentes = historico.setdefault("fuentes", {})
    historico["corridas"] = int(historico.get("corridas") or 0) + 1
    historico["actualizado_en"] = now_iso()

    with _telemetria_lock:
        actual = list(_telemetria)

    for t in actual:
        clave = t["source_key"] or t["url"]
        if not clave:
            continue
        f = fuentes.setdefault(clave, {
            "name": t["name"], "url": t["url"], "corridas": 0, "indices_ok": 0,
            "titulares": 0, "candidatos": 0, "corridas_sin_candidato": 0,
            "ultimo_candidato_en": None, "ultimo_fallo": "",
        })
        f["name"], f["url"] = t["name"], t["url"]
        f["corridas"] += 1
        f["indices_ok"] += 1 if t["indice_leido"] else 0
        f["titulares"] += t["titulares"]
        f["candidatos"] += t["candidatos"]
        f["ultimo_fallo"] = t["fallo"]
        if t["candidatos"]:
            f["corridas_sin_candidato"] = 0
            f["ultimo_candidato_en"] = now_iso()
        else:
            f["corridas_sin_candidato"] = int(f.get("corridas_sin_candidato") or 0) + 1

    # Veredicto legible, para que el analista no tenga que interpretar contadores.
    for clave, f in fuentes.items():
        seguidas = f.get("corridas_sin_candidato") or 0
        if f.get("corridas", 0) >= 8 and seguidas >= 8:
            f["veredicto"] = "sustituir"
            f["veredicto_texto"] = (
                f"{seguidas} corridas seguidas sin aportar una sola noticia. "
                + (FALLO_TEXTO.get(f.get("ultimo_fallo", ""), "") or "")
            ).strip()
        elif seguidas >= 4:
            f["veredicto"] = "vigilar"
            f["veredicto_texto"] = f"{seguidas} corridas seguidas sin aportar noticias."
        else:
            f["veredicto"] = "ok"
            f["veredicto_texto"] = ""

    try:
        NEWS_HEALTH_PATH.parent.mkdir(parents=True, exist_ok=True)
        NEWS_HEALTH_PATH.write_text(json.dumps(historico, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception as exc:
        log.warning("No se pudo escribir la salud de fuentes: %s", exc)

    sustituir = [f["name"] for f in fuentes.values() if f.get("veredicto") == "sustituir"]
    if sustituir:
        log.warning("Fuentes candidatas a sustitucion (nunca aportan): %s", ", ".join(sustituir))
    return historico


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


def procesar_busqueda_general(existing_urls: set[str], cupo: int,
                              consultas: list[str] | None = None) -> list[dict]:
    """Barrido propio de la web, ademas de las fuentes configuradas.

    Las fuentes fijas fallan de dos maneras: el indice no lista la nota, o el
    medio no la publica. Este paso pregunta directo por los temas que importan,
    asi una noticia relevante no depende de aparecer en un indice concreto.
    """
    if not NEWS_SEARCH_ENABLED or cupo <= 0 or _quota_agotada.is_set():
        return []

    fuente_busqueda = {"source_key": "busqueda_web", "name": "Búsqueda web"}
    encontrados: list[dict] = []
    for consulta in (consultas if consultas is not None else NEWS_SEARCH_QUERIES):
        if len(encontrados) >= cupo or _quota_agotada.is_set():
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
            existing_urls.add(link)
            try:
                cand = build_candidate(fuente_busqueda, {"title": titulo, "link": link}, link)
            except Exception as exc:
                log.warning("  -> error procesando resultado de busqueda: %s", exc)
                continue
            if cand:
                encontrados.append(cand)
                log.info("  ✓ candidato de búsqueda (rel %s): %s", cand.get("relevance"), cand["title"][:80])
    log.info("Búsqueda general: %d candidatos nuevos", len(encontrados))
    return encontrados


# ---------------------------------------------------------------------------
# Garantia de piso: minimo NEWS_MIN_CANDIDATES por corrida
# El panel tiene que abrir con material de trabajo. Los lunes flojos venian con
# dos o tres notas y el analista terminaba buscando a mano, que es exactamente
# lo que este pipeline existe para evitar.
# La escalera va de lo mas barato y menos invasivo a lo mas caro:
#   1. Reserva de la propia corrida — notas ya leidas y puntuadas que quedaron
#      fuera por la ventana, por el umbral o por ser rutinarias. Coste cero.
#   2. Busquedas extra en la web — abre el abanico de temas. Cuesta Gemini.
#   3. Se acepta quedarse corto, pero se dice en el JSON, no en silencio.
# Nunca se relajan los temas vetados ni la opinion: completar cuota con lo que
# el analista pidio no ver seria peor que quedarse corto.
# ---------------------------------------------------------------------------
PRIORIDAD_RESERVA = {"vieja": 0, "relevancia_baja": 1, "rutinaria": 2, "modelo_irrelevante": 3}


def completar_minimo(candidatos: list[dict], existing_urls: set[str]) -> dict:
    """Rellena hasta NEWS_MIN_CANDIDATES. Devuelve el acta de lo que se relajo."""
    acta = {"piso": NEWS_MIN_CANDIDATES, "al_inicio": len(candidatos), "pasos": [], "cumplido": True}
    if len(candidatos) >= NEWS_MIN_CANDIDATES:
        return acta

    urls = {c.get("url") for c in candidatos}

    # Paso 1 — reserva de la corrida, de la mas recuperable a la menos.
    with _descartes_lock:
        reserva = [r for r in _reserva if r.get("url") not in urls]
    reserva.sort(key=lambda r: (PRIORIDAD_RESERVA.get(r.get("motivo_original", ""), 9),
                                -(r.get("relevance") or 0)))
    recuperados = 0
    for item in reserva:
        if len(candidatos) >= NEWS_MIN_CANDIDATES:
            break
        if any(mismo_tema(item.get("title", ""), c.get("title", "")) for c in candidatos):
            continue
        item = {**item, "recuperado_por_piso": True,
                "motivo_recuperacion": MOTIVO_TEXTO.get(item.get("motivo_original", ""), "")}
        candidatos.append(item)
        urls.add(item.get("url"))
        existing_urls.add(item.get("url"))
        recuperados += 1
        log.info("  ↑ recuperado por piso (%s, rel %s): %s",
                 item.get("motivo_original"), item.get("relevance"), item.get("title", "")[:70])
    if recuperados:
        acta["pasos"].append({
            "paso": "reserva",
            "sumados": recuperados,
            "detalle": "Notas ya leidas que estaban fuera de ventana, bajo el umbral o eran rutinarias",
        })

    # Paso 2 — busquedas extra, con la ventana ensanchada un dia.
    if len(candidatos) < NEWS_MIN_CANDIDATES and NEWS_SEARCH_ENABLED and not _quota_agotada.is_set():
        faltan = NEWS_MIN_CANDIDATES - len(candidatos)
        log.info("Piso sin cubrir: faltan %d. Se abren busquedas extra.", faltan)
        VENTANA["inicio"] = VENTANA["inicio"] - timedelta(hours=NEWS_WINDOW_RELAX_HOURS)
        VENTANA["relajada_horas"] = NEWS_WINDOW_RELAX_HOURS
        extra = procesar_busqueda_general(existing_urls, faltan, NEWS_SEARCH_QUERIES_EXTRA)
        if extra:
            candidatos.extend(extra)
            acta["pasos"].append({
                "paso": "busquedas_extra",
                "sumados": len(extra),
                "detalle": f"Consultas adicionales con la ventana ensanchada {NEWS_WINDOW_RELAX_HOURS}h",
            })

    acta["al_final"] = len(candidatos)
    acta["cumplido"] = len(candidatos) >= NEWS_MIN_CANDIDATES
    if not acta["cumplido"]:
        acta["nota"] = (
            f"Solo se consiguieron {len(candidatos)} de {NEWS_MIN_CANDIDATES}. "
            "No se rebajaron los temas vetados ni se coló opinión para llenar el cupo; "
            "revisa la salud de fuentes."
        )
        log.warning(acta["nota"])
    return acta


# ---------------------------------------------------------------------------
# Brief del dia
# Lo primero que ve el analista: que paso desde ayer por la tarde, agrupado por
# tema y con el angulo para La Nacional. Antes tenia que leerse 18 tarjetas para
# hacerse esa idea; ahora las tarjetas son el respaldo del brief, no el punto
# de partida.
# ---------------------------------------------------------------------------
BRIEF_PROMPT = """Eres el analista senior del Observatorio Estratégico de La Nacional (asociacion de
ahorros y prestamos dominicana). Recibes las noticias candidatas de la ventana editorial de hoy.

Redacta el brief con el que un directivo entiende el dia en 30 segundos. Sobrio, sin adjetivos de
mas, sin recomendaciones de inversion, sin inventar cifras: solo puedes usar lo que esta abajo.

Devuelve SOLO JSON valido con esta forma exacta:
{
  "titular": "una linea con lo mas importante del periodo (max 90 caracteres)",
  "resumen": "2 o 3 oraciones con el panorama de la ventana",
  "temas": [{"tema":"etiqueta corta","que_paso":"1 oracion","por_que_importa":"1 oracion para una AAyP","ids":["id de las notas"]}],
  "vigilar": ["1 a 3 viñetas de lo que conviene seguir en los proximos dias"],
  "destacadas": ["ids de las 3 notas que el analista deberia publicar si solo pudiera elegir tres"]
}

Reglas:
- Entre 2 y 4 temas. Agrupa las notas que cuentan lo mismo bajo un solo tema.
- Usa exactamente los ids que se te dan; no inventes ninguno.
- Si el material del dia es flojo, dilo en el resumen en vez de inflarlo.

NOTAS DE LA VENTANA (%s):
"""


def generar_brief(candidatos: list[dict]) -> dict | None:
    """Brief del dia sobre los candidatos ya elegidos. Una sola llamada."""
    if not NEWS_BRIEF_ENABLED or not candidatos or _quota_agotada.is_set():
        return None
    lineas = []
    for c in sorted(candidatos, key=lambda x: -(x.get("relevance") or 0))[:14]:
        lineas.append(
            f"- id={c.get('id')} | {c.get('ventana', '')} | rel {c.get('relevance')} | "
            f"{c.get('category')} | {c.get('source_name')}\n"
            f"  {c.get('title')}\n  {c.get('summary') or ''}"
        )
    parsed = gemini_json((BRIEF_PROMPT % texto_ventana()) + "\n".join(lineas), max_tokens=1800)
    if not isinstance(parsed, dict):
        log.warning("No se pudo generar el brief del dia")
        return None

    ids_validos = {c.get("id") for c in candidatos}
    temas = []
    for t in (parsed.get("temas") or [])[:4]:
        if not isinstance(t, dict):
            continue
        temas.append({
            "tema": limpiar_texto(t.get("tema"))[:60],
            "que_paso": limpiar_texto(t.get("que_paso"))[:400],
            "por_que_importa": limpiar_texto(t.get("por_que_importa"))[:400],
            "ids": [i for i in (t.get("ids") or []) if i in ids_validos],
        })
    brief = {
        "titular": limpiar_texto(parsed.get("titular"))[:120],
        "resumen": limpiar_texto(parsed.get("resumen"))[:900],
        "temas": temas,
        "vigilar": [limpiar_texto(v)[:280] for v in (parsed.get("vigilar") or [])[:3] if str(v).strip()],
        "destacadas": [i for i in (parsed.get("destacadas") or [])[:3] if i in ids_validos],
        "generado_en": now_iso(),
    }
    if not brief["resumen"]:
        return None
    log.info("Brief del dia: %s", brief["titular"][:90])
    return brief


def load_previous_candidates() -> list[dict]:
    try:
        if not NEWS_CANDIDATES_PATH.exists():
            return []
        data = json.loads(NEWS_CANDIDATES_PATH.read_text(encoding="utf-8"))
        arr = data.get("candidates", [])
        if not isinstance(arr, list):
            return []
        cutoff = datetime.now(timezone.utc) - timedelta(days=CANDIDATE_TTL_DAYS)
        kept = []
        for x in arr:
            dt = parse_iso_date(x.get("fetched_at"))
            if dt and dt < cutoff:
                continue
            kept.append(x)
        return kept
    except Exception as exc:
        log.info("No se pudieron cargar candidatos previos: %s", exc)
        return []


def write_candidates(candidates: list[dict], errors: int, sources_count: int, elapsed: float,
                     acta: dict | None = None, brief: dict | None = None,
                     salud: dict | None = None) -> None:
    NEWS_CANDIDATES_PATH.parent.mkdir(parents=True, exist_ok=True)

    merged = []
    seen = set()
    arrastradas = 0
    for i, item in enumerate(candidates + load_previous_candidates()):
        url = canonicalize_url(item.get("url") or "")
        if not url or url in seen:
            continue
        es_previa = i >= len(candidates)
        # Las de corridas anteriores solo siguen si aun caen en la ventana: el
        # panel es el resumen de HOY, no un archivo de tres dias. Antes se
        # arrastraban por TTL y el lunes convivian notas del miercoles.
        if es_previa and fuera_de_ventana(item.get("published_at")):
            continue
        if es_previa:
            arrastradas += 1
        item["url"] = url
        item["id"] = item.get("id") or stable_id(url)
        item["status"] = "pending"
        item.setdefault("ventana", etiqueta_ventana(item.get("published_at")))
        merged.append(item)
        seen.add(url)

    # La deduplicacion por tema va sobre el conjunto ya fusionado (nuevos +
    # previos): si la misma noticia entro ayer por un medio y hoy por otro,
    # tambien se colapsa.
    merged = dedupe_por_tema(merged)

    # Orden de trabajo, no de llegada: primero lo mas relevante y, a igual
    # relevancia, lo mas reciente. El analista lee de arriba hacia abajo.
    merged = sorted(
        merged,
        key=lambda x: ((x.get("relevance") or 0),
                       x.get("published_at") or x.get("fetched_at") or ""),
        reverse=True,
    )[:MAX_TOTAL_PROPOSALS]

    with _descartes_lock:
        recuperadas = {c.get("url") for c in merged}
        descartes = [d for d in _descartes if d.get("url") not in recuperadas]
        descartes = sorted(descartes, key=lambda d: (d.get("relevance") or 0), reverse=True)[:40]
    with _telemetria_lock:
        telemetria = list(_telemetria)

    payload = {
        "generated_at": now_iso(),
        "gemini_model": GEMINI_MODEL,
        "min_relevance": NEWS_MIN_RELEVANCE,
        "min_candidates": NEWS_MIN_CANDIDATES,
        "ventana": {
            "desde": VENTANA.get("inicio_local"),
            "hasta": VENTANA.get("fin_local"),
            "desde_utc": VENTANA["inicio"].isoformat() if VENTANA.get("inicio") else None,
            "dia": VENTANA.get("dia"),
            "es_lunes": VENTANA.get("es_lunes"),
            "horas": VENTANA.get("horas"),
            "motivo": VENTANA.get("motivo"),
            "relajada_horas": VENTANA.get("relajada_horas", 0),
        },
        "count": len(merged),
        "arrastradas": arrastradas,
        "errors": errors,
        "sources": sources_count,
        "elapsed_s": elapsed,
        "brief": brief,
        "garantia": acta,
        "candidates": merged,
        "discarded_count": len(descartes),
        "discarded": descartes,
        "fuentes": telemetria,
        "fuentes_a_sustituir": [
            {"name": f.get("name"), "url": f.get("url"), "motivo": f.get("veredicto_texto")}
            for f in (salud or {}).get("fuentes", {}).values()
            if f.get("veredicto") == "sustituir"
        ],
    }
    NEWS_CANDIDATES_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    log.info("JSON escrito: %s (%d candidatos)", NEWS_CANDIDATES_PATH, len(merged))


def run() -> None:
    log.info("=== Pipeline de noticias static/local iniciado ===")
    start = time.time()
    global VENTANA
    VENTANA = calcular_ventana()
    log.info("Ventana editorial (%s): %s → %s · %.1f h · %s",
             VENTANA["dia"], VENTANA["inicio_local"], VENTANA["fin_local"],
             VENTANA["horas"], VENTANA["motivo"])
    if VENTANA["es_lunes"]:
        log.info("Lunes: cupo de titulares por fuente elevado a %d", MAX_HEADLINES_LUNES)
    sources = load_sources()
    if not sources:
        log.warning("No hay fuentes activas en Supabase.")
        return

    existing = load_existing_urls()
    log.info("Articulos ya vistos/publicados: %d", len(existing))

    all_candidates: list[dict] = []
    errors = 0
    for i, source in enumerate(sources):
        if len(all_candidates) >= MAX_TOTAL_PROPOSALS:
            break
        if _quota_agotada.is_set():
            log.warning("Sin cuota de Gemini: se omiten las %d fuentes restantes.", len(sources) - i)
            break
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

    # Garantia de piso antes de escribir: si el dia vino flojo, la escalera
    # recupera de la reserva y, si hace falta, abre busquedas extra.
    try:
        acta = completar_minimo(all_candidates, existing)
    except Exception as exc:
        log.error("Error completando el piso de candidatos: %s", exc)
        acta = {"error": str(exc)}
        errors += 1

    salud = actualizar_salud_fuentes()

    brief = None
    try:
        brief = generar_brief(all_candidates)
    except Exception as exc:
        log.warning("No se pudo generar el brief: %s", exc)

    elapsed = round(time.time() - start, 1)
    write_candidates(all_candidates, errors, len(sources), elapsed, acta, brief, salud)

    sb_insert_log({
        "source": "NEWS",
        "section": "Pipeline candidatos",
        "status": "success" if errors == 0 and acta.get("cumplido", True) else "warning",
        "rows_processed": len(all_candidates),
        "message": (f"Pipeline JSON: {len(all_candidates)} candidatos "
                    f"(piso {NEWS_MIN_CANDIDATES}), ventana {VENTANA['horas']}h desde "
                    f"{VENTANA['inicio_local']}, {len(_descartes)} descartados, "
                    f"{errors} errores, {elapsed}s"),
        "metadata": {"mode": "static_json", "path": str(NEWS_CANDIDATES_PATH),
                     "gemini_model": GEMINI_MODEL, "ventana": VENTANA.get("motivo"),
                     "garantia": acta},
        "updated_at": now_iso(),
    })
    log.info("=== Finalizado: %d candidatos nuevos en %.1fs ===", len(all_candidates), elapsed)


if __name__ == "__main__":
    run()
