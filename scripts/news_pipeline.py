"""
Observatorio Estratégico La Nacional — Pipeline de Noticias
============================================================
Flujo:
  1. Lee fuentes activas desde Supabase (tabla: news_sources)
  2. Para cada fuente, obtiene el contenido via Jina Reader
  3. Gemini Flash extrae: título, resumen, fecha, categoría
  4. Escribe candidatos en Supabase (tabla: news_proposals)
     con status='pending' para revisión de Nathali

Variables de entorno requeridas:
  SUPABASE_URL        — URL del proyecto Supabase
  SUPABASE_SERVICE_KEY — service_role key (NO la anon key)
  GEMINI_API_KEY      — key de Google AI Studio
"""

import os
import json
import time
import hashlib
import logging
from datetime import datetime, timezone, timedelta
from urllib.parse import urlparse, quote

import httpx

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("news_pipeline")

# ── Configuración ─────────────────────────────────────────────────────────────
SUPABASE_URL        = os.environ["SUPABASE_URL"].rstrip("/")
SUPABASE_SERVICE_KEY = os.environ["SUPABASE_SERVICE_KEY"]
GEMINI_API_KEY      = os.environ["GEMINI_API_KEY"]

JINA_BASE           = "https://r.jina.ai/"
GEMINI_ENDPOINT     = (
    "https://generativelanguage.googleapis.com/v1beta/models/"
    "gemini-2.5-flash-lite:generateContent"
)

FETCH_TIMEOUT       = 30        # segundos por request a Jina
GEMINI_TIMEOUT      = 20        # segundos por request a Gemini
MAX_MARKDOWN_CHARS  = 6000      # recorte para no exceder contexto
MIN_CONTENT_CHARS   = 300       # mínimo para considerar que Jina devolvió algo útil
STALENESS_HOURS     = 36        # ignorar artículos más viejos que esto
DELAY_BETWEEN_SOURCES = 5     # segundos entre fuentes (rate limit Jina)

SUPABASE_HEADERS = {
    "apikey":        SUPABASE_SERVICE_KEY,
    "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
    "Content-Type":  "application/json",
    "Prefer":        "return=minimal",
}


# ── Supabase helpers ──────────────────────────────────────────────────────────

def sb_get(path: str, params: dict = None) -> list:
    url = f"{SUPABASE_URL}/rest/v1/{path}"
    r = httpx.get(url, headers=SUPABASE_HEADERS, params=params, timeout=15)
    r.raise_for_status()
    return r.json()


def sb_upsert(table: str, rows: list, on_conflict: str = "url") -> None:
    url = f"{SUPABASE_URL}/rest/v1/{table}"
    headers = {**SUPABASE_HEADERS, "Prefer": f"resolution=ignore-duplicates,return=minimal"}
    r = httpx.post(url, headers=headers, json=rows, timeout=15)
    if r.status_code not in (200, 201, 204):
        log.error("Supabase upsert error %s: %s", r.status_code, r.text[:300])
    r.raise_for_status()


def sb_insert_log(entry: dict) -> None:
    url = f"{SUPABASE_URL}/rest/v1/data_update_log"
    headers = {**SUPABASE_HEADERS, "Prefer": "return=minimal"}
    r = httpx.post(url, headers=headers, json=entry, timeout=10)
    if r.status_code not in (200, 201, 204):
        log.warning("Log insert warning %s: %s", r.status_code, r.text[:200])


# ── Carga fuentes ─────────────────────────────────────────────────────────────

def load_sources() -> list[dict]:
    rows = sb_get("news_sources", params={"enabled": "eq.true", "select": "*"})
    log.info("Fuentes activas: %d", len(rows))
    return rows


# ── Jina Reader ───────────────────────────────────────────────────────────────

def fetch_with_jina(url: str) -> str | None:
    """Devuelve markdown limpio del artículo, o None si falla."""
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
            log.warning("Jina devolvió contenido muy corto (%d chars) para %s", len(text), url)
            return None
        return text[:MAX_MARKDOWN_CHARS]
    except httpx.TimeoutException:
        log.warning("Jina timeout: %s", url)
        return None
    except Exception as e:
        log.warning("Jina error (%s): %s", type(e).__name__, url)
        return None


# ── Gemini extractor ──────────────────────────────────────────────────────────

EXTRACTION_PROMPT = """Eres un analista de inteligencia financiera dominicana.
Recibes el contenido markdown de una PÁGINA ÍNDICE de noticias (lista de titulares con enlaces).

Tu tarea: identificar el titular MÁS RECIENTE Y RELEVANTE para el sector financiero/económico dominicano, y extraer sus datos.

Devuelve SOLO un objeto JSON válido, sin texto adicional, sin fences markdown. Estructura exacta:

{"title": "titular exacto", "summary": "resumen de 1-2 oraciones máximo 40 palabras", "published_at": null, "category": "Monetario", "relevant": true}

Reglas estrictas:
- "summary" NUNCA debe exceder 40 palabras. Sé conciso.
- "category" debe ser exactamente una de: Monetario, Financiero, Regulatorio, Economia, Global
- "published_at" usa formato ISO 8601 solo si la fecha es visible y explícita; si no, null
- Si la página no tiene ningún titular económico/financiero útil, devuelve exactamente: {"relevant": false}
- Escapa correctamente las comillas dentro de los textos.

Contenido:
"""


def extract_with_gemini(markdown: str, source_name: str) -> dict | None:
    prompt = EXTRACTION_PROMPT + markdown
    time.sleep(7)  # respeta 15 RPM del tier free de Gemini (max 1 req/4s)
    payload = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {
            "temperature": 0.1,
            "maxOutputTokens": 800,
        },
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
                log.info("Gemini 503 (sobrecarga), reintento %d para '%s'", intento + 1, source_name)
                time.sleep(8)
                continue
            if r.status_code != 200:
                log.warning("Gemini HTTP %s para fuente '%s': %s", r.status_code, source_name, r.text[:200])
                return None
            data = r.json()
            raw = data["candidates"][0]["content"]["parts"][0]["text"].strip()
            if raw.startswith("```"):
                raw = raw.split("```")[1]
                if raw.startswith("json"):
                    raw = raw[4:]
            raw = raw.strip()
            extracted = json.loads(raw)
            if not extracted.get("relevant", True):
                log.info("Gemini: sin artículo relevante en '%s'", source_name)
                return None
            return extracted
        except json.JSONDecodeError as e:
            log.warning("Gemini JSON inválido para '%s': %s", source_name, e)
            return None
        except Exception as e:
            log.warning("Gemini error (%s) para '%s': %s", type(e).__name__, source_name, e)
            return None
    return None


# ── Deduplicación ─────────────────────────────────────────────────────────────

def url_fingerprint(url: str) -> str:
    """Normaliza y hashea la URL para deduplicar."""
    parsed = urlparse(url)
    canonical = parsed.netloc + parsed.path.rstrip("/")
    return hashlib.md5(canonical.lower().encode()).hexdigest()


def load_existing_urls() -> set[str]:
    """Carga URLs ya propuestas en las últimas 72h para evitar duplicados."""
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=72)).isoformat()
    rows = sb_get(
        "news_proposals",
        params={
            "select": "url",
            "fetched_at": f"gte.{cutoff}",
        },
    )
    return {r["url"] for r in rows}


# ── Pipeline principal ────────────────────────────────────────────────────────

def process_source(source: dict, existing_urls: set[str]) -> dict | None:
    """
    Procesa una fuente y devuelve la propuesta lista para insertar, o None.
    """
    name = source.get("name", source.get("source_key", "desconocida"))
    url  = source.get("url", "").strip()

    if not url:
        log.warning("Fuente '%s' sin URL, se omite", name)
        return None

    log.info("Procesando: %s (%s)", name, url)

    # 1. Obtener contenido con Jina
    markdown = fetch_with_jina(url)
    if not markdown:
        log.info("  → Jina no devolvió contenido útil")
        return None

    # 2. Extraer con Gemini
    extracted = extract_with_gemini(markdown, name)
    if not extracted:
        log.info("  → Gemini no encontró artículo relevante")
        return None

    title    = extracted.get("title", "").strip()
    summary  = extracted.get("summary", "").strip()
    category = extracted.get("category", "Economía")
    pub_date = extracted.get("published_at")

    if not title:
        log.info("  → Título vacío, se descarta")
        return None

    # 3. Validar frescura si hay fecha
    if pub_date:
        try:
            dt = datetime.fromisoformat(pub_date.replace("Z", "+00:00"))
            age_h = (datetime.now(timezone.utc) - dt).total_seconds() / 3600
            if age_h > STALENESS_HOURS:
                log.info("  → Artículo demasiado viejo (%.0fh), se descarta", age_h)
                return None
        except Exception:
            pass  # si no parsea la fecha, no descartamos

    # 4. Deduplicar por URL de la fuente (no del artículo — Jina nos da la página index)
    if url in existing_urls:
        log.info("  → URL ya registrada, se omite")
        return None

    proposal = {
        "source_key":   source.get("source_key", ""),
        "source_name":  name,
        "url":          url,
        "title":        title,
        "summary":      summary,
        "category":     category,
        "published_at": pub_date,
        "fetched_at":   datetime.now(timezone.utc).isoformat(),
        "status":       "pending",
    }

    log.info("  ✓ Propuesta: %s", title[:80])
    return proposal


def run():
    log.info("=== Pipeline de noticias iniciado ===")
    start = time.time()

    sources   = load_sources()
    if not sources:
        log.warning("No hay fuentes activas en Supabase. Verifica la tabla news_sources.")
        return

    existing  = load_existing_urls()
    log.info("URLs ya en Supabase (últimas 72h): %d", len(existing))

    proposals = []
    errors    = 0

    for i, source in enumerate(sources):
        try:
            proposal = process_source(source, existing)
            if proposal:
                proposals.append(proposal)
                existing.add(proposal["url"])  # evitar duplicados dentro de la misma ejecución
        except Exception as e:
            log.error("Error inesperado en fuente '%s': %s", source.get("name", "?"), e)
            errors += 1

        if i < len(sources) - 1:
            time.sleep(DELAY_BETWEEN_SOURCES)

    # Insertar en Supabase
    if proposals:
        log.info("Insertando %d propuestas en Supabase...", len(proposals))
        sb_upsert("news_proposals", proposals, on_conflict="url")
    else:
        log.info("No se generaron propuestas nuevas en esta ejecución.")

    elapsed = round(time.time() - start, 1)

    # Log de auditoría
    sb_insert_log({
        "source":        "NEWS_PIPELINE",
        "section":       "Resumen de Noticias",
        "rows_processed": len(proposals),
        "message":       f"Pipeline completado: {len(proposals)} propuestas, {errors} errores, {elapsed}s",
        "metadata":      json.dumps({
            "proposals": len(proposals),
            "errors":    errors,
            "sources":   len(sources),
            "elapsed_s": elapsed,
        }),
        "updated_at":    datetime.now(timezone.utc).isoformat(),
    })

    log.info("=== Pipeline finalizado: %d propuestas en %.1fs ===", len(proposals), elapsed)


if __name__ == "__main__":
    run()
