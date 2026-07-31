"""
Observatorio Estratégico La Nacional — Pipeline de Noticias v4 LOCAL/STATIC
============================================================================
Flujo:
  1. Lee fuentes activas desde Supabase (news_sources). Esto es liviano.
  2. Lee cada página índice con Jina Reader.
  3. Gemini extrae URLs reales de artículos candidatos.
  4. Para cada artículo, lee la URL individual con Jina (en paralelo, 3 hilos).
  5. Gemini genera resumen ejecutivo + lead + relevancia financiera (0-10).
  6. Solo se aceptan candidatos con relevancia >= NEWS_MIN_RELEVANCE.
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
  NEWS_MIN_RELEVANCE=6        # umbral 0-10 de relevancia financiera
  ARTICLE_WORKERS=3           # hilos por fuente para leer articulos
"""

from __future__ import annotations

import os
import re
import json
import time
import hashlib
import logging
import threading
from pathlib import Path
from datetime import datetime, timezone, timedelta
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
NEWS_MAX_AGE_HOURS = int(os.getenv("NEWS_MAX_AGE_HOURS", "72"))
CANDIDATE_TTL_DAYS = int(os.getenv("CANDIDATE_TTL_DAYS", "3"))
NEWS_MIN_RELEVANCE = int(os.getenv("NEWS_MIN_RELEVANCE", "6"))
ARTICLE_WORKERS = max(1, int(os.getenv("ARTICLE_WORKERS", "3")))
# El free tier de Gemini limita las peticiones por minuto; espaciar las
# llamadas evita tormentas de 429 que agotan los reintentos y dejan fuentes
# sin procesar (observado en el run del 2026-07-06).
GEMINI_MIN_INTERVAL = float(os.getenv("GEMINI_MIN_INTERVAL", "6"))

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

Descarta sin excepcion: navegacion, relleno, opinion sin dato economico, publirreportajes, tecnologia de consumo, deportes, entretenimiento, sucesos, politica sin componente economico, noticias militares o de seguridad sin impacto financiero cuantificable.

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
  * 9-10: decision de politica monetaria/regulatoria, datos de banca o credito, tasas, tipo de cambio
  * 7-8: macro RD relevante (PIB, inflacion, deuda, IED, remesas) o global con canal directo a RD
  * 5-6: economia general RD con impacto indirecto (sectores, comercio, energia)
  * 0-4: sin impacto financiero claro (seguridad, politica, social, curiosidades)
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
    try:
        relevance = int(parsed.get("relevance", 7))
    except (TypeError, ValueError):
        relevance = 7
    parsed["relevance"] = max(0, min(10, relevance))
    if parsed["relevance"] < NEWS_MIN_RELEVANCE:
        log.info("  -> relevancia %d < %d, descartado: %s",
                 parsed["relevance"], NEWS_MIN_RELEVANCE, (parsed.get("title") or "")[:70])
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
        "relevance": details.get("relevance", NEWS_MIN_RELEVANCE),
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
    candidates = candidates[:MAX_HEADLINES_PER_SOURCE]
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
        "min_relevance": NEWS_MIN_RELEVANCE,
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
