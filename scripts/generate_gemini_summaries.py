#!/usr/bin/env python3
# ==============================================================================
# generate_gemini_summaries.py
# Genera resúmenes ejecutivos con Gemini para las vistas "Estadísticas Macro"
# y "Estadísticas SIB" del Observatorio. Produce data/gemini_summaries.json,
# que el front (index.html) carga y pinta debajo del título de cada vista.
#
# Contexto que se le pasa a Gemini (todo opcional salvo el snapshot SIB para
# la sección SIB):
#   - SIB:   data/sib_snapshot.json (pipeline fetch_sib_indicadores.py)
#   - Macro: series bcrd_series publicadas por el analista en Supabase
#            (lectura REST con anon key; si falla, se omite) +
#            titulares recientes de news_candidates.json
#
# Diseño anti-rotura:
#   - Si Gemini falla en una sección, se conserva el resumen previo de esa
#     sección (no se pisa el archivo con vacío).
#   - Si ninguna sección queda disponible (ni nueva ni previa), sale con
#     código 1 y NO escribe el archivo.
#
# Variables de entorno:
#   GEMINI_API_KEY          (requerida, salvo --dry-run)
#   GEMINI_MODEL            (default: gemini-2.5-flash-lite)
#   GEMINI_SUMMARY_PATH     (default: data/gemini_summaries.json)
#   SIB_SNAPSHOT_PATH       (default: data/sib_snapshot.json)
#   NEWS_CANDIDATES_PATH    (default: news_candidates.json)
#   SUPABASE_URL            (requerida — sin default, ver MIGRACION_SUPABASE.md)
#   SUPABASE_KEY            (requerida; acepta anon o service key)
#
# Uso:
#   python scripts/generate_gemini_summaries.py            # corrida normal
#   python scripts/generate_gemini_summaries.py --dry-run  # sin llamar Gemini
# ==============================================================================

import os
import re
import sys
import json
import time
from collections import defaultdict
from datetime import datetime, timezone

import httpx

GEMINI_MODEL = os.environ.get("GEMINI_MODEL", "gemini-2.5-flash-lite")
GEMINI_ENDPOINT = (
    f"https://generativelanguage.googleapis.com/v1beta/models/{GEMINI_MODEL}:generateContent"
)
GEMINI_TIMEOUT = 60
GEMINI_MIN_INTERVAL = float(os.environ.get("GEMINI_MIN_INTERVAL", "6"))

OUTPUT_PATH = os.environ.get("GEMINI_SUMMARY_PATH", "data/gemini_summaries.json")
SIB_SNAPSHOT_PATH = os.environ.get("SIB_SNAPSHOT_PATH", "data/sib_snapshot.json")
NEWS_CANDIDATES_PATH = os.environ.get("NEWS_CANDIDATES_PATH", "news_candidates.json")

# Proyecto Supabase del Observatorio. Sin default a propósito: durante la
# migración al proyecto departamental, un fallback silencioso haría que el
# pipeline siguiera escribiendo en el proyecto viejo sin que nadie lo note.
SUPABASE_URL = (os.environ.get("SUPABASE_URL") or "").strip().rstrip("/")
SUPABASE_KEY = (
    os.environ.get("SUPABASE_KEY")
    or os.environ.get("SUPABASE_SERVICE_KEY")
    or os.environ.get("SUPABASE_ANON_KEY")
    or ""
).strip()

if not SUPABASE_URL or not SUPABASE_KEY:
    sys.exit(
        "Faltan SUPABASE_URL y/o SUPABASE_KEY. Defínelas apuntando al proyecto "
        "correcto (ver docs/MIGRACION_SUPABASE.md)."
    )

# Meses de historia que se resumen en el contexto de cada serie.
MESES_CONTEXTO = 13
MAX_NOTICIAS = 10
# Antigüedad máxima de un titular para entrar al contexto macro. Sin este tope,
# el resumen del día 21 podía citar como "reciente" una nota de hace dos semanas
# que seguía en news_candidates.json, y nadie lo notaba porque el texto no dice
# de cuándo es cada cosa.
NOTICIAS_MAX_DIAS = int(os.environ.get("NOTICIAS_MAX_DIAS", "12"))
# Tipos de pieza que el pipeline de noticias etiqueta y que no deben alimentar
# un resumen ejecutivo: la opinión de un columnista no es un hecho, y la tasa
# del dólar del martes no es contexto macro.
TIPOS_NO_CONTEXTO = {"opinion", "publirreportaje", "servicio"}
# Respaldo por titular para los candidatos generados antes de que el pipeline
# empezara a etiquetar `tipo`: sin esto, el resumen de esta semana seguiría
# abriendo con dos notas del precio del dólar.
PATRONES_RUTINA = re.compile(
    r"(precio del d[oó]lar|d[oó]lar hoy|tasas? de compra y venta|"
    r"precios? de (los )?combustibles|loter[ií]a|hor[oó]scopo)",
    re.IGNORECASE,
)

INDICADOR_LABEL = {
    "morosidad": "Morosidad (%)",
    "roe": "ROE (%)",
    "roa": "ROA (%)",
    "cti": "Eficiencia / Cost-to-income (%)",
    "activos": "Activos netos totales (RD$)",
}
TIPO_LABEL = {
    "BM": "Bancos Múltiples",
    "AAyP": "Asociaciones de Ahorros y Préstamos",
    "BAyC": "Bancos de Ahorro y Crédito",
}


def log(msg):
    print(msg, flush=True)


def _fmt_valor(indicador, valor):
    if valor is None:
        return "s/d"
    if indicador == "activos":
        return f"{valor / 1e9:,.1f} MMM"  # miles de millones RD$
    return f"{valor:,.2f}"


# ------------------------------------------------------------------
# Contexto SIB (data/sib_snapshot.json)
# ------------------------------------------------------------------
def build_sib_context():
    try:
        with open(SIB_SNAPSHOT_PATH, encoding="utf-8") as f:
            snap = json.load(f)
    except (OSError, json.JSONDecodeError) as exc:
        log(f"!! No se pudo leer snapshot SIB ({SIB_SNAPSHOT_PATH}): {exc}")
        return None, None

    rows = snap.get("rows") or []
    if not rows:
        return None, None

    periodos = sorted({r["periodo"] for r in rows if r.get("periodo")})
    if not periodos:
        return None, None
    ventana = periodos[-MESES_CONTEXTO:]
    latest = periodos[-1]

    # series del sistema: entidad "TODOS" por tipo de entidad
    sistema = defaultdict(dict)  # (tipo, indicador) -> {periodo: valor}
    entidades = defaultdict(dict)  # (entidad, indicador) -> {periodo: valor}
    for r in rows:
        p = r.get("periodo")
        if p not in ventana or r.get("valor") is None:
            continue
        ent = str(r.get("entidad") or "").strip().upper()
        if ent == "TODOS":
            sistema[(r["tipo_entidad"], r["indicador"])][p] = r["valor"]
        elif r.get("tipo_entidad") == "AAyP":
            entidades[(ent, r["indicador"])][p] = r["valor"]

    lines = [f"Último período con datos: {latest}", ""]

    lines.append("== SERIES DEL SISTEMA (agregado 'TODOS' por tipo de entidad) ==")
    for tipo in ("AAyP", "BM", "BAyC"):
        bloque = []
        for ind in ("morosidad", "roe", "roa", "cti", "activos"):
            serie = sistema.get((tipo, ind))
            if not serie:
                continue
            pares = [f"{p}: {_fmt_valor(ind, serie.get(p))}" for p in ventana if p in serie]
            bloque.append(f"  {INDICADOR_LABEL[ind]}: " + " | ".join(pares))
        if bloque:
            lines.append(f"- {TIPO_LABEL.get(tipo, tipo)} ({tipo}):")
            lines.extend(bloque)
    lines.append("")

    # tabla del último período para las AAyP (peer group de La Nacional)
    lines.append(f"== ASOCIACIONES (AAyP) · corte {latest} ==")
    ents_aayp = sorted({e for (e, _i) in entidades})
    tabla = []
    for ent in ents_aayp:
        vals = {}
        for ind in ("activos", "morosidad", "roe", "roa", "cti"):
            serie = entidades.get((ent, ind)) or {}
            vals[ind] = serie.get(latest)
        if all(v is None for v in vals.values()):
            continue
        tabla.append((vals.get("activos") or 0, ent, vals))
    for _act, ent, vals in sorted(tabla, reverse=True):
        lines.append(
            f"- {ent}: activos {_fmt_valor('activos', vals['activos'])} RD$ | "
            f"morosidad {_fmt_valor('morosidad', vals['morosidad'])} | "
            f"ROE {_fmt_valor('roe', vals['roe'])} | "
            f"ROA {_fmt_valor('roa', vals['roa'])} | "
            f"CTI {_fmt_valor('cti', vals['cti'])}"
        )
    lines.append("")

    # trayectoria de LA NACIONAL
    ln_bloque = []
    for ind in ("morosidad", "roe", "roa", "cti", "activos"):
        serie = entidades.get(("LA NACIONAL", ind))
        if not serie:
            continue
        pares = [f"{p}: {_fmt_valor(ind, serie.get(p))}" for p in ventana if p in serie]
        ln_bloque.append(f"  {INDICADOR_LABEL[ind]}: " + " | ".join(pares))
    if ln_bloque:
        lines.append("== TRAYECTORIA DE LA NACIONAL (últimos meses) ==")
        lines.extend(ln_bloque)

    return "\n".join(lines), latest


# ------------------------------------------------------------------
# Contexto Macro (Supabase bcrd_series + titulares recientes)
# ------------------------------------------------------------------
def fetch_bcrd_series():
    """Lee las series BCRD publicadas por el analista. Devuelve texto o None."""
    url = f"{SUPABASE_URL}/rest/v1/bcrd_series"
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
    }
    params = {
        "select": "serie_id,nombre,periodo,valor,unidad",
        "order": "periodo.desc",
        "limit": "1200",
    }
    try:
        resp = httpx.get(url, headers=headers, params=params, timeout=45)
        if resp.status_code != 200:
            log(f"!! bcrd_series HTTP {resp.status_code}: {resp.text[:200]}")
            return None
        data = resp.json()
    except Exception as exc:
        log(f"!! bcrd_series no disponible: {type(exc).__name__}: {exc}")
        return None
    if not data:
        return None

    series = defaultdict(list)  # serie_id -> [(periodo, valor, nombre, unidad)]
    for row in data:
        if row.get("periodo") is None or row.get("valor") is None:
            continue
        series[row["serie_id"]].append(
            (row["periodo"], row["valor"], row.get("nombre"), row.get("unidad"))
        )

    lines = []
    for sid, obs in sorted(series.items()):
        obs.sort()  # cronológico
        obs = obs[-MESES_CONTEXTO:]
        nombre = obs[-1][2] or sid
        unidad = obs[-1][3]
        etiqueta = f"{nombre}" + (f" ({unidad})" if unidad else "")
        pares = [f"{p[:7]}: {v:,.2f}" for p, v, _n, _u in obs]
        lines.append(f"- {etiqueta}: " + " | ".join(pares))
    return "\n".join(lines) if lines else None


def _dias_desde(iso):
    if not iso:
        return None
    try:
        dt = datetime.fromisoformat(str(iso).replace("Z", "+00:00"))
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return (datetime.now(timezone.utc) - dt).days


def load_news_headlines():
    """Titulares frescos y noticiosos para el contexto macro.

    Se filtran tres cosas que antes entraban sin más: piezas de opinión o de
    servicio (el pipeline ya las etiqueta con `tipo`), notas viejas que seguían
    en el archivo de candidatos, y las que el propio pipeline apartó por tema.
    Devuelve (texto, usadas) para poder dejar constancia de qué se citó.
    """
    try:
        with open(NEWS_CANDIDATES_PATH, encoding="utf-8") as f:
            data = json.load(f)
    except (OSError, json.JSONDecodeError):
        return None, []

    cands = []
    for c in (data.get("candidates") or []):
        if (c.get("tipo") or "noticia") in TIPOS_NO_CONTEXTO:
            continue
        dias = _dias_desde(c.get("published_at") or c.get("fetched_at"))
        if dias is not None and dias > NOTICIAS_MAX_DIAS:
            continue
        titulo = (c.get("title") or "").strip()
        if not titulo or PATRONES_RUTINA.search(titulo):
            continue
        cands.append(c)

    # Primero lo más relevante y, a igual relevancia, lo más reciente.
    cands.sort(key=lambda c: ((c.get("relevance") or 0),
                              c.get("published_at") or c.get("fetched_at") or ""),
               reverse=True)

    lines, usadas = [], []
    for c in cands[:MAX_NOTICIAS]:
        fecha = str(c.get("published_at") or c.get("fetched_at") or "")[:10]
        titulo = (c.get("title") or "").strip()
        resumen = (c.get("summary") or "").strip()
        angulo = (c.get("por_que_importa") or "").strip()
        linea = f"- [{fecha}] {titulo}. {resumen}"
        if angulo:
            linea += f" (Para una AAyP: {angulo})"
        lines.append(linea)
        usadas.append({"fecha": fecha, "titulo": titulo, "url": c.get("url") or ""})

    # El brief del día que redacta el pipeline de noticias resume la ventana
    # editorial completa; darle ese encabezado al modelo evita que reconstruya
    # a mano un panorama que ya está escrito.
    brief = data.get("brief") or {}
    if brief.get("resumen"):
        cabecera = f"Brief del día ({brief.get('titular','')}): {brief['resumen']}"
        lines.insert(0, cabecera)

    return ("\n".join(lines) if lines else None), usadas


def build_macro_context():
    partes = []
    series = fetch_bcrd_series()
    if series:
        partes.append("== SERIES MACRO BCRD (publicadas en el Observatorio) ==\n" + series)
    else:
        partes.append(
            "== SERIES MACRO BCRD ==\n(No disponibles en esta corrida; "
            "apóyate en los titulares y en el panorama general dominicano.)"
        )
    noticias, usadas = load_news_headlines()
    if noticias:
        partes.append(
            f"== TITULARES RECIENTES (curados por el Observatorio, últimos {NOTICIAS_MAX_DIAS} días) ==\n"
            + noticias
        )
    else:
        partes.append(
            "== TITULARES RECIENTES ==\n(Sin titulares frescos en esta corrida; "
            "no cites hechos noticiosos que no estén en las series de arriba.)"
        )
    return "\n\n".join(partes), usadas


# ------------------------------------------------------------------
# Gemini
# ------------------------------------------------------------------
BASE_PROMPT = """Eres el analista económico senior del Observatorio Estratégico de La Nacional \
(Asociación La Nacional de Ahorros y Préstamos, República Dominicana). Escribes resúmenes \
ejecutivos breves y sobrios para directivos: nada sensacionalista, sin recomendaciones de \
inversión, sin inventar cifras. SOLO puedes citar números que aparezcan en el CONTEXTO; si un \
dato no está, no lo menciones. Escribe en español dominicano formal, claro y directo.

Devuelve EXCLUSIVAMENTE un objeto JSON con esta forma exacta:
{
  "titulo": "titular corto del mes (máx. 80 caracteres, sin punto final)",
  "resumen": "2 o 3 oraciones con el panorama general",
  "puntos": ["3 a 4 viñetas cortas sobre qué está pasando"],
  "vigilar": ["2 a 3 viñetas cortas sobre qué conviene tener pendiente"]
}
"""

MACRO_PROMPT = BASE_PROMPT + """
SECCIÓN: "Estadísticas Macro" del Observatorio (tablero macro-financiero dominicano: IMAE, \
IPC/inflación, remesas, tipo de cambio y entorno monetario).

TAREA: redacta el resumen ejecutivo mensual del panorama macroeconómico dominicano con base en \
el contexto. Prioriza tendencias (aceleración/desaceleración, cambios de nivel) sobre cifras \
sueltas, y en "vigilar" señala publicaciones o riesgos concretos a seguir (BCRD, Fed, precios \
del petróleo, remesas, tipo de cambio), sin alarmismo.

CONTEXTO:
"""

SIB_PROMPT = BASE_PROMPT + """
SECCIÓN: "Estadísticas SIB" del Observatorio (indicadores prudenciales del sistema financiero \
dominicano publicados por la Superintendencia de Bancos: morosidad, ROE, ROA, eficiencia y \
activos, por tipo de entidad y para el peer group de asociaciones).

TAREA: redacta el resumen ejecutivo del sistema financiero con base en el contexto. Describe la \
salud general del sistema (calidad de cartera, rentabilidad, eficiencia), el bloque de \
Asociaciones de Ahorros y Préstamos y, con discreción y sin autoelogios, dónde se ubica \
LA NACIONAL frente a sus pares. En "vigilar" indica señales concretas de los propios datos \
(tendencias de morosidad, presión en eficiencia, próximos cortes de la SIB).

CONTEXTO:
"""

_last_call_end = 0.0


def _throttle():
    """Espacia las llamadas midiendo desde que TERMINÓ la anterior.

    Medir desde el inicio no espaciaba nada: la llamada de macro tarda ~6 s,
    o sea lo mismo que el intervalo, así que la de SIB salía 130 ms después de
    que la primera respondiera y se comía un 429 (visto el 2026-07-21).
    """
    wait = _last_call_end + GEMINI_MIN_INTERVAL - time.monotonic()
    if wait > 0:
        time.sleep(wait)


def _mark_call_done():
    global _last_call_end
    _last_call_end = time.monotonic()


def _quota_detail(response):
    """Qué cuota dice Google que se excedió: sirve para distinguir el límite
    por minuto (se resuelve esperando) del límite diario (no se resuelve hoy).
    """
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


def _retry_delay(response, attempt):
    try:
        for detail in response.json().get("error", {}).get("details", []):
            delay = str(detail.get("retryDelay", ""))
            if delay.endswith("s"):
                return min(120.0, float(delay[:-1]) + 1)
    except Exception:
        pass
    return min(120.0, 20.0 * attempt)


def gemini_json(prompt, api_key, max_tokens=1200, retries=6):
    payload = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {
            "temperature": 0.25,
            "maxOutputTokens": max_tokens,
            "responseMimeType": "application/json",
        },
    }
    for attempt in range(1, retries + 1):
        try:
            _throttle()
            try:
                r = httpx.post(
                    GEMINI_ENDPOINT,
                    params={"key": api_key},
                    json=payload,
                    timeout=GEMINI_TIMEOUT,
                )
            finally:
                _mark_call_done()
            if r.status_code in (429, 503):
                delay = _retry_delay(r, attempt)
                # El cuerpo del 429 dice qué cuota se pasó. Sin esto, un límite
                # por minuto y uno diario se ven exactamente igual en el log.
                if attempt == 1:
                    detalle = _quota_detail(r)
                    if detalle:
                        log(f"  Gemini {r.status_code} · cuota: {detalle}")
                log(f"  Gemini {r.status_code}, reintento {attempt}/{retries} (espera {delay:.0f}s)")
                time.sleep(delay)
                continue
            if r.status_code != 200:
                log(f"  Gemini HTTP {r.status_code}: {r.text[:300]}")
                return None
            data = r.json()
            raw = data["candidates"][0]["content"]["parts"][0]["text"]
            return json.loads(raw)
        except json.JSONDecodeError as exc:
            log(f"  Gemini devolvió JSON inválido: {exc}")
            return None
        except Exception as exc:
            log(f"  Gemini error ({type(exc).__name__}): {exc}")
            time.sleep(3 * attempt)
    return None


def _clean_lista(value, maximo):
    if not isinstance(value, list):
        return []
    out = []
    for item in value:
        s = str(item).strip()
        if s:
            out.append(s[:280])
        if len(out) >= maximo:
            break
    return out


def validar_seccion(parsed, periodo):
    """Normaliza y valida la respuesta de Gemini; None si no sirve."""
    if not isinstance(parsed, dict):
        return None
    titulo = str(parsed.get("titulo") or "").strip()[:120]
    resumen = str(parsed.get("resumen") or "").strip()[:900]
    puntos = _clean_lista(parsed.get("puntos"), 4)
    vigilar = _clean_lista(parsed.get("vigilar"), 3)
    if not resumen or not puntos:
        return None
    return {
        "periodo": periodo,
        "titulo": titulo,
        "resumen": resumen,
        "puntos": puntos,
        "vigilar": vigilar,
        "generado_en": datetime.now(timezone.utc).isoformat(),
    }


# ------------------------------------------------------------------
# Main
# ------------------------------------------------------------------
def main():
    dry_run = "--dry-run" in sys.argv

    api_key = os.environ.get("GEMINI_API_KEY", "")
    if not api_key and not dry_run:
        sys.exit("ERROR: GEMINI_API_KEY no está definida en el entorno.")

    hoy = datetime.now(timezone.utc)
    periodo_actual = hoy.strftime("%Y-%m")

    log(">> Construyendo contexto SIB...")
    sib_ctx, sib_periodo = build_sib_context()
    log(f"   SIB: {'OK · corte ' + str(sib_periodo) if sib_ctx else 'sin datos'}")

    log(">> Construyendo contexto Macro...")
    macro_ctx, noticias_usadas = build_macro_context()
    log(f"   Macro: OK · {len(noticias_usadas)} titulares frescos en el contexto")

    if dry_run:
        log("\n===== CONTEXTO MACRO =====\n" + macro_ctx)
        log("\n===== CONTEXTO SIB =====\n" + (sib_ctx or "(vacío)"))
        log("\n[dry-run] No se llamó a Gemini ni se escribió salida.")
        return

    # resúmenes previos, para no perder secciones si Gemini falla hoy
    previas = {}
    try:
        with open(OUTPUT_PATH, encoding="utf-8") as f:
            previas = (json.load(f).get("secciones")) or {}
    except (OSError, json.JSONDecodeError):
        pass

    secciones = {}

    log(">> Generando resumen MACRO con Gemini...")
    parsed = gemini_json(MACRO_PROMPT + macro_ctx, api_key)
    nueva = validar_seccion(parsed, periodo_actual)
    if nueva:
        # Trazabilidad: qué noticias vio el modelo. Sin esto, verificar una
        # afirmación del resumen obligaba a adivinar de dónde salió.
        nueva["noticias_usadas"] = noticias_usadas
        secciones["macro"] = nueva
        log("   Macro: OK")
    elif previas.get("macro"):
        secciones["macro"] = previas["macro"]
        log("   Macro: FALLÓ · se conserva el resumen previo")
    else:
        log("   Macro: FALLÓ · sin resumen previo")

    if sib_ctx:
        log(">> Generando resumen SIB con Gemini...")
        parsed = gemini_json(SIB_PROMPT + sib_ctx, api_key)
        nueva = validar_seccion(parsed, sib_periodo or periodo_actual)
        if nueva:
            secciones["sib"] = nueva
            log("   SIB: OK")
        elif previas.get("sib"):
            secciones["sib"] = previas["sib"]
            log("   SIB: FALLÓ · se conserva el resumen previo")
        else:
            log("   SIB: FALLÓ · sin resumen previo")
    elif previas.get("sib"):
        secciones["sib"] = previas["sib"]
        log("   SIB: sin snapshot · se conserva el resumen previo")

    if not secciones:
        log("\n!! Ninguna sección disponible. No se escribe salida.")
        sys.exit(1)

    salida = {
        "generado_en": hoy.isoformat(),
        "modelo": GEMINI_MODEL,
        "secciones": secciones,
    }
    os.makedirs(os.path.dirname(OUTPUT_PATH) or ".", exist_ok=True)
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(salida, f, ensure_ascii=False, indent=2)

    log("\n=== OK ===")
    log(f"  secciones: {', '.join(secciones)}")
    log(f"  salida: {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
