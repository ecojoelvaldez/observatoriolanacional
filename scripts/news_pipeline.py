"""
Observatorio Estratégico La Nacional — Pipeline de Noticias v3 LOCAL/STATIC
============================================================================
Flujo:
  1. Lee fuentes activas desde Supabase (news_sources). Esto es liviano.
  2. Lee cada página índice con Jina Reader.
  3. Gemini extrae URLs reales de artículos candidatos.
  4. Para cada artículo, lee la URL individual con Jina.
  5. Gemini genera resumen ejecutivo + lead/cuerpo breve + fecha/categoría.
  6. NO guarda candidatos en news_proposals.
  7. Escribe un archivo estático news_candidates.json en la raíz del repo.

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
"""

from __future__ import annotations

import os
import re
import json
import time
import hashlib
import logging
from pathlib import Path
from datetime import datetime, timezone, timedelta
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

JINA_READER_BASE = "https://r.jina.ai/"
GEMINI_ENDPOINT = f"https://generativelanguage.googleapis.com/v1beta/models/{GEMINI_MODEL}:generateContent"

FETCH_TIMEOUT = 35
GEMINI_TIMEOUT = 30
MAX_INDEX_MARKDOWN_CHARS = 9000
MAX_ARTICLE_MARKDOWN_CHARS = 12000
MIN_CONTENT_CHARS = 250
DELAY_BETWEEN_SOURCES = 4
DELAY_BETWEEN_ARTICLES = 1.5
MAX_HEADLINES_PER_SOURCE = 6
MAX_TOTAL_PROPOSALS = int(os.getenv("MAX_TOTAL_PROPOSALS", "30"))
NEWS_MAX_AGE_HOURS = int(os.getenv("NEWS_MAX_AGE_HOURS", "72"))
CANDIDATE_TTL_DAYS = int(os.getenv("CANDIDATE_TTL_DAYS", "3"))

CATEGORY_ALLOWED = {"Monetario", "Financiero", "Regulatorio", "Economia", "Global"}
TRACKING_PARAMS = {"utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content", "fbclid", "gclid", "mc_cid", "mc_eid"}

SUPABASE_HEADERS = {
    "apikey": SUPABASE_SERVICE_KEY,
    "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
    "Content-Type": "application/json",
}


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def stable_id(url: str) -> str:
    return "n_" + hashlib.sha1((url or "").encode("utf-8")).hexdigest()[:16]


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


def gemini_json(prompt: str, max_tokens: int = 1500, retries: int = 3) -> dict | None:
    payload = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {"temperature": 0.1, "maxOutputTokens": max_tokens},
    }
    for attempt in range(1, retries + 1):
        try:
            r = httpx.post(GEMINI_ENDPOINT, params={"key": GEMINI_API_KEY}, json=payload, timeout=GEMINI_TIMEOUT)
            if r.status_code in (429, 503):
                log.warning("Gemini %s, reintento %d/%d", r.status_code, attempt, retries)
                time.sleep(6 * attempt)
                continue
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
    return None


HEADLINE_PROMPT = """Eres un analista de inteligencia financiera dominicana que cura noticias para un banco.

Recibes el markdown de una PAGINA INDICE de noticias: lista de titulares, enlaces y fragmentos.

Identifica titulares de NOTICIAS REALES relevantes para el sector financiero, monetario, bancario, regulatorio o macroeconomico dominicano.

Prioriza:
- Politica monetaria, tasas, inflacion, tipo de cambio
- Banco Central, Superintendencia, Hacienda, DGII, mercado de valores
- Banca, credito, remesas, turismo, energia, comercio exterior
- Indicadores macroeconomicos locales o globales con impacto para RD

Descarta: navegacion, relleno, opinion sin dato economico, publirreportajes, tecnologia de consumo, deportes, entretenimiento.

Devuelve SOLO JSON valido con esta estructura exacta:
{"headlines":[{"title":"titular exacto","link":"URL del articulo individual","category":"Economia"}]}

Reglas:
- Devuelve entre 0 y 6 titulares.
- category debe ser exactamente una de: Monetario, Financiero, Regulatorio, Economia, Global.
- link debe ser la URL del articulo individual. Si no hay enlace claro, omite el titular.
- No inventes enlaces.

Markdown de pagina indice:
"""

ARTICLE_PROMPT = """Eres un analista de inteligencia financiera dominicana.

Recibes el markdown de un ARTICULO individual. Extrae informacion util para que un analista lo apruebe o descarte en un panel editorial.

Devuelve SOLO JSON valido con esta estructura exacta:
{
  "title": "titulo completo del articulo",
  "summary": "resumen ejecutivo en 2 oraciones, en espanol, con impacto para el sector financiero dominicano",
  "body": "lead propio de 1 parrafo, 45-90 palabras, basado en el cuerpo de la noticia, no repitas el titulo y no copies texto largo literalmente",
  "published_at": "fecha ISO 8601 si aparece, o null",
  "category": "Economia",
  "relevant": true
}

Reglas:
- category debe ser exactamente una de: Monetario, Financiero, Regulatorio, Economia, Global.
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


def extract_article_details(article_markdown: str) -> dict | None:
    parsed = gemini_json(ARTICLE_PROMPT + article_markdown, max_tokens=1800)
    if not parsed or not parsed.get("relevant", True):
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


def is_stale(published_at: str | None) -> bool:
    dt = parse_iso_date(published_at)
    if not dt:
        return False
    return (datetime.now(timezone.utc) - dt).total_seconds() / 3600 > NEWS_MAX_AGE_HOURS


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
    headline_title = (headline.get("title") or "").strip()
    link = canonicalize_url(headline.get("link") or "", base_url)
    if not headline_title or not link:
        return None

    article_markdown = fetch_with_jina(link, MAX_ARTICLE_MARKDOWN_CHARS)
    if not article_markdown:
        log.info("  -> no pude leer articulo individual: %s", link)
        return None

    details = extract_article_details(article_markdown)
    if not details:
        log.info("  -> articulo sin detalle util: %s", headline_title[:70])
        return None

    published_at = details.get("published_at")
    if is_stale(published_at):
        log.info("  -> articulo viejo, se omite: %s", headline_title[:70])
        return None

    title = (details.get("title") or headline_title).strip()
    return {
        "id": stable_id(link),
        "source_key": source.get("source_key", ""),
        "source_name": source.get("name", source.get("source_key", "desconocida")),
        "url": link,
        "title": title,
        "summary": (details.get("summary") or "").strip(),
        "body": (details.get("body") or "").strip(),
        "category": normalize_category(details.get("category") or headline.get("category")),
        "published_at": published_at,
        "fetched_at": now_iso(),
        "status": "pending",
    }


def process_source(source: dict, existing_urls: set[str]) -> list[dict]:
    name = source.get("name", source.get("source_key", "desconocida"))
    base_url = (source.get("url") or "").strip()
    if not base_url:
        log.warning("Fuente '%s' sin URL, se omite", name)
        return []

    log.info("Procesando fuente: %s (%s)", name, base_url)
    index_markdown = fetch_with_jina(base_url, MAX_INDEX_MARKDOWN_CHARS)
    if not index_markdown:
        log.info("  -> Jina no devolvio indice util")
        return []

    headlines = extract_headlines(index_markdown)
    if not headlines:
        log.info("  -> Gemini no extrajo titulares utiles")
        return []

    candidates: list[dict] = []
    for headline in headlines:
        link = canonicalize_url(headline.get("link") or "", base_url)
        title = (headline.get("title") or "").strip()
        if not link or not title:
            continue
        if link in existing_urls:
            log.info("  -> ya vista: %s", title[:70])
            continue
        candidate = build_candidate(source, headline, base_url)
        if not candidate:
            continue
        candidates.append(candidate)
        existing_urls.add(candidate["url"])
        log.info("  ✓ candidato: %s", candidate["title"][:90])
        time.sleep(DELAY_BETWEEN_ARTICLES)
        if len(candidates) >= MAX_HEADLINES_PER_SOURCE:
            break
    log.info("  -> %d candidatos nuevos de %s", len(candidates), name)
    return candidates


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


def write_candidates(candidates: list[dict], errors: int, sources_count: int, elapsed: float) -> None:
    NEWS_CANDIDATES_PATH.parent.mkdir(parents=True, exist_ok=True)

    merged = []
    seen = set()
    for item in candidates + load_previous_candidates():
        url = canonicalize_url(item.get("url") or "")
        if not url or url in seen:
            continue
        item["url"] = url
        item["id"] = item.get("id") or stable_id(url)
        item["status"] = "pending"
        merged.append(item)
        seen.add(url)

    merged = sorted(merged, key=lambda x: x.get("fetched_at") or "", reverse=True)[:MAX_TOTAL_PROPOSALS]
    payload = {
        "generated_at": now_iso(),
        "gemini_model": GEMINI_MODEL,
        "count": len(merged),
        "errors": errors,
        "sources": sources_count,
        "elapsed_s": elapsed,
        "candidates": merged,
    }
    NEWS_CANDIDATES_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    log.info("JSON escrito: %s (%d candidatos)", NEWS_CANDIDATES_PATH, len(merged))


def run() -> None:
    log.info("=== Pipeline de noticias static/local iniciado ===")
    start = time.time()
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
        try:
            remaining = MAX_TOTAL_PROPOSALS - len(all_candidates)
            all_candidates.extend(process_source(source, existing)[:remaining])
        except Exception as exc:
            log.error("Error inesperado en '%s': %s", source.get("name", "?"), exc)
            errors += 1
        if i < len(sources) - 1:
            time.sleep(DELAY_BETWEEN_SOURCES)

    elapsed = round(time.time() - start, 1)
    write_candidates(all_candidates, errors, len(sources), elapsed)

    sb_insert_log({
        "source": "NEWS",
        "section": "Pipeline candidatos",
        "status": "success" if errors == 0 else "warning",
        "rows_processed": len(all_candidates),
        "message": f"Pipeline JSON: {len(all_candidates)} candidatos nuevos, {errors} errores, {elapsed}s",
        "metadata": {"mode": "static_json", "path": str(NEWS_CANDIDATES_PATH), "gemini_model": GEMINI_MODEL},
        "updated_at": now_iso(),
    })
    log.info("=== Finalizado: %d candidatos nuevos en %.1fs ===", len(all_candidates), elapsed)


if __name__ == "__main__":
    run()
