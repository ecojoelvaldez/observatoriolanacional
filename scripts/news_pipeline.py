"""
Observatorio Estratégico La Nacional — Pipeline de Noticias (v2)
================================================================
Flujo:
  1. Lee fuentes activas desde Supabase (tabla: news_sources)
  2. Para cada fuente (página índice), obtiene el markdown via Jina Reader
  3. Gemini Flash-Lite extrae MÚLTIPLES titulares con sus enlaces
  4. Escribe cada titular como candidato en Supabase (tabla: news_proposals)
     con status='pending'. Analista decide cuáles publicar.

Diseño Opción A: el sistema PROPONE varios candidatos, el humano DISPONE.
Deduplicación por URL del artículo individual (no de la página índice).

Variables de entorno requeridas:
  SUPABASE_URL         — URL del proyecto Supabase
  SUPABASE_SERVICE_KEY — service_role key (NO la anon key)
  GEMINI_API_KEY       — key de Google AI Studio
"""

import os
import json
import time
import logging
from datetime import datetime, timezone, timedelta
from urllib.parse import urlparse, urljoin

import httpx

# -- Logging -------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("news_pipeline")

# -- Configuracion -------------------------------------------------------------
SUPABASE_URL         = os.environ["SUPABASE_URL"].rstrip("/")
SUPABASE_SERVICE_KEY = os.environ["SUPABASE_SERVICE_KEY"]
GEMINI_API_KEY       = os.environ["GEMINI_API_KEY"]

JINA_BASE       = "https://r.jina.ai/"
GEMINI_ENDPOINT = (
    "https://generativelanguage.googleapis.com/v1beta/models/"
    "gemini-2.5-flash-lite:generateContent"
)

FETCH_TIMEOUT            = 30
GEMINI_TIMEOUT           = 25
MAX_MARKDOWN_CHARS       = 8000
MIN_CONTENT_CHARS        = 300
DELAY_BETWEEN_SOURCES    = 5
MAX_HEADLINES_PER_SOURCE = 6

SUPABASE_HEADERS = {
    "apikey":        SUPABASE_SERVICE_KEY,
    "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
    "Content-Type":  "application/json",
    "Prefer":        "return=minimal",
}


def sb_get(path, params=None):
    url = f"{SUPABASE_URL}/rest/v1/{path}"
    r = httpx.get(url, headers=SUPABASE_HEADERS, params=params, timeout=15)
    r.raise_for_status()
    return r.json()


def sb_upsert(table, rows):
    url = f"{SUPABASE_URL}/rest/v1/{table}"
    headers = {**SUPABASE_HEADERS, "Prefer": "resolution=ignore-duplicates,return=minimal"}
    r = httpx.post(url, headers=headers, json=rows, timeout=20)
    if r.status_code not in (200, 201, 204):
        log.error("Supabase upsert error %s: %s", r.status_code, r.text[:300])
    r.raise_for_status()


def sb_insert_log(entry):
    url = f"{SUPABASE_URL}/rest/v1/data_update_log"
    headers = {**SUPABASE_HEADERS, "Prefer": "return=minimal"}
    r = httpx.post(url, headers=headers, json=entry, timeout=10)
    if r.status_code not in (200, 201, 204):
        log.warning("Log insert warning %s: %s", r.status_code, r.text[:200])


def load_sources():
    rows = sb_get("news_sources", params={"enabled": "eq.true", "select": "*"})
    log.info("Fuentes activas: %d", len(rows))
    return rows


def fetch_with_jina(url):
    jina_url = JINA_BASE + url
    try:
        r = httpx.get(
            jina_url,
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
        return text[:MAX_MARKDOWN_CHARS]
    except httpx.TimeoutException:
        log.warning("Jina timeout: %s", url)
        return None
    except Exception as e:
        log.warning("Jina error (%s): %s", type(e).__name__, url)
        return None


EXTRACTION_PROMPT = """Eres un analista de inteligencia financiera dominicana que cura noticias para un banco.

Recibes el markdown de una PAGINA INDICE de noticias (lista de titulares con enlaces).

Tu tarea: identificar los titulares de NOTICIAS REALES mas relevantes para el sector financiero, monetario, bancario o macroeconomico dominicano. Prioriza:
- Politica monetaria, tasas, inflacion, tipo de cambio
- Decisiones del Banco Central, Superintendencia, Hacienda
- Banca, credito, mercado de valores, remesas
- Indicadores macroeconomicos, crecimiento, fiscal

Descarta: notas de relleno, publirreportajes, tecnologia de consumo, entretenimiento, deportes, secciones de navegacion.

Devuelve SOLO un objeto JSON valido (sin texto extra, sin fences markdown) con esta estructura exacta:

{"headlines": [{"title": "titular exacto", "link": "URL del articulo tal como aparece en el markdown", "summary": "resumen de maximo 35 palabras", "category": "Monetario"}]}

Reglas estrictas:
- Devuelve entre 1 y 6 titulares, ordenados de MAS a MENOS relevante.
- "category" es exactamente una de: Monetario, Financiero, Regulatorio, Economia, Global
- "summary" NUNCA excede 35 palabras. Escapa comillas internas correctamente.
- "link" debe ser la URL del articulo individual que aparece junto al titular en el markdown. Si no hay enlace claro, omite ese titular.
- Si la pagina no tiene ningun titular economico/financiero util, devuelve: {"headlines": []}

Contenido:
"""


def extract_headlines(markdown, source_name):
    time.sleep(7)
    payload = {
        "contents": [{"parts": [{"text": EXTRACTION_PROMPT + markdown}]}],
        "generationConfig": {"temperature": 0.1, "maxOutputTokens": 1500},
    }
    for intento in range(3):
        try:
            r = httpx.post(
                GEMINI_ENDPOINT,
                params={"key": GEMINI_API_KEY},
                json=payload,
                timeout=GEMINI_TIMEOUT,
            )
            if r.status_code == 503:
                log.info("Gemini 503, reintento %d para '%s'", intento + 1, source_name)
                time.sleep(8)
                continue
            if r.status_code == 429:
                log.warning("Gemini 429 (quota) para '%s' — se detiene", source_name)
                return []
            if r.status_code != 200:
                log.warning("Gemini HTTP %s para '%s': %s", r.status_code, source_name, r.text[:200])
                return []
            data = r.json()
            raw = data["candidates"][0]["content"]["parts"][0]["text"].strip()
            if raw.startswith("```"):
                raw = raw.split("```")[1]
                if raw.startswith("json"):
                    raw = raw[4:]
            raw = raw.strip()
            parsed = json.loads(raw)
            headlines = parsed.get("headlines", [])
            if not isinstance(headlines, list):
                return []
            return headlines[:MAX_HEADLINES_PER_SOURCE]
        except json.JSONDecodeError as e:
            log.warning("Gemini JSON invalido para '%s': %s", source_name, e)
            return []
        except Exception as e:
            log.warning("Gemini error (%s) para '%s': %s", type(e).__name__, source_name, e)
            return []
    return []


def resolve_link(link, base_url):
    if not link or not isinstance(link, str):
        return None
    link = link.strip()
    if link.startswith("(") and link.endswith(")"):
        link = link[1:-1]
    if not link.startswith("http"):
        link = urljoin(base_url, link)
    parsed = urlparse(link)
    if not parsed.scheme.startswith("http") or not parsed.netloc:
        return None
    clean = f"{parsed.scheme}://{parsed.netloc}{parsed.path}".rstrip("/")
    return clean


def load_existing_urls():
    cutoff = (datetime.now(timezone.utc) - timedelta(days=5)).isoformat()
    rows = sb_get("news_proposals", params={"select": "url", "fetched_at": f"gte.{cutoff}"})
    return {r["url"] for r in rows}


def process_source(source, existing_urls):
    name     = source.get("name", source.get("source_key", "desconocida"))
    base_url = source.get("url", "").strip()

    if not base_url:
        log.warning("Fuente '%s' sin URL, se omite", name)
        return []

    log.info("Procesando: %s (%s)", name, base_url)

    markdown = fetch_with_jina(base_url)
    if not markdown:
        log.info("  -> Jina no devolvio contenido util")
        return []

    headlines = extract_headlines(markdown, name)
    if not headlines:
        log.info("  -> Gemini no extrajo titulares utiles")
        return []

    proposals = []
    for h in headlines:
        title    = (h.get("title") or "").strip()
        summary  = (h.get("summary") or "").strip()
        category = (h.get("category") or "Economia").strip()
        link     = resolve_link(h.get("link", ""), base_url)

        if not title or not link:
            continue
        if link in existing_urls:
            log.info("  -> ya propuesta: %s", title[:60])
            continue

        proposals.append({
            "source_key":   source.get("source_key", ""),
            "source_name":  name,
            "url":          link,
            "title":        title,
            "summary":      summary,
            "category":     category,
            "published_at": None,
            "fetched_at":   datetime.now(timezone.utc).isoformat(),
            "status":       "pending",
        })
        existing_urls.add(link)
        log.info("  v %s", title[:80])

    log.info("  -> %d candidatos nuevos de %s", len(proposals), name)
    return proposals


def run():
    log.info("=== Pipeline de noticias iniciado ===")
    start = time.time()

    sources = load_sources()
    if not sources:
        log.warning("No hay fuentes activas en Supabase.")
        return

    existing = load_existing_urls()
    log.info("Articulos ya propuestos (ultimos 5 dias): %d", len(existing))

    all_proposals = []
    errors = 0

    for i, source in enumerate(sources):
        try:
            all_proposals.extend(process_source(source, existing))
        except Exception as e:
            log.error("Error inesperado en '%s': %s", source.get("name", "?"), e)
            errors += 1

        if i < len(sources) - 1:
            time.sleep(DELAY_BETWEEN_SOURCES)

    if all_proposals:
        log.info("Insertando %d candidatos en Supabase...", len(all_proposals))
        sb_upsert("news_proposals", all_proposals)
    else:
        log.info("No se generaron candidatos nuevos.")

    elapsed = round(time.time() - start, 1)

    sb_insert_log({
        "source":         "NEWS_PIPELINE",
        "section":        "Resumen de Noticias",
        "rows_processed": len(all_proposals),
        "message":        f"Pipeline: {len(all_proposals)} candidatos, {errors} errores, {elapsed}s",
        "metadata":       json.dumps({
            "candidatos": len(all_proposals),
            "errores":    errors,
            "fuentes":    len(sources),
            "elapsed_s":  elapsed,
        }),
        "updated_at":     datetime.now(timezone.utc).isoformat(),
    })

    log.info("=== Finalizado: %d candidatos en %.1fs ===", len(all_proposals), elapsed)


if __name__ == "__main__":
    run()
