#!/usr/bin/env python3
# ==============================================================================
# fetch_sib_indicadores.py
# Descarga indicadores/financieros del API SIB v2 y genera un snapshot
# estatico (data/sib_snapshot.json) en formato "long" que el index.html
# del Observatorio ya sabe leer (buildSibDataFromLong).
#
# Reemplaza el paso manual de subir CSV. Cero egress de Supabase.
# Usa httpx (misma dependencia que news_pipeline.py).
#
# Espejo de la logica de consultar_sib_paginado() del pipeline R:
#   - Base: https://apis.sb.gob.do/estadisticas/v2/
#   - Auth: header Ocp-Apim-Subscription-Key
#   - Paginacion: header x-pagination con HasNext / TotalPages / TotalRecords
#   - Retries con backoff en 429/500/502/503/504
# ==============================================================================

import os
import sys
import json
import time
import random
from datetime import date, datetime, timezone

import httpx

SIB_BASE_URL = "https://apis.sb.gob.do/estadisticas/v2/"
ENDPOINT = "indicadores/financieros"

# Tipos de entidad (espejo del pipeline R).
# Se puede acotar con la variable de entorno SIB_TIPOS (lista separada por
# comas, ej. "AAyP" para descargar solo asociaciones de ahorros y prestamos:
# APAP, La Nacional, Cibao, etc.). Reduce el tiempo de descarga.
TIPOS_ENTIDAD = ["BM", "AAyP", "BAyC"]
_tipos_env = os.environ.get("SIB_TIPOS", "").strip()
if _tipos_env:
    TIPOS_ENTIDAD = [t.strip() for t in _tipos_env.split(",") if t.strip()]

SIB_REGISTROS = 300
TIMEOUT_SEC = 90
MAX_ATTEMPTS = 6
SLEEP_BETWEEN = (0.6, 1.8)
RETRYABLE = {429, 500, 502, 503, 504}

# Cuantos meses hacia atras descargar en cada corrida (ventana movil)
MESES_VENTANA = 26

OUTPUT_PATH = os.environ.get("SIB_SNAPSHOT_PATH", "data/sib_snapshot.json")

# ------------------------------------------------------------------
# Mapa de campos del API -> etiqueta de indicador que el front reconoce.
#
# El front (sibMetricId) reconoce por substring, asi que basta con que
# la etiqueta contenga la palabra clave. Las claves de este dict se
# comparan de forma flexible (normalizadas: minuscula, sin acentos,
# espacios/puntos -> _). Si el API devuelve un nombre distinto, se
# agrega aqui. El modo --diagnostico imprime los campos crudos.
# ------------------------------------------------------------------
# El endpoint indicadores/financieros devuelve un catálogo de ~80 ratios con
# nombre exacto (verificado con --diagnostico el 2026-07-06). El mapeo es por
# IGUALDAD EXACTA: el matching por substring colisionaba ("cti" está contenido
# en "aCTIvos_productivos_...", "cartera" en ratios de cobertura) y contaminaba
# el snapshot con valores de ratios equivocados.
#
# Nota: este endpoint NO trae el balance de cartera de créditos en RD$; solo
# "activos_netos_totales" es un monto absoluto. La serie "cartera" del front
# sigue alimentándose del CSV del analista u otro endpoint futuro.
CAMPO_A_INDICADOR = {
    "indice_de_morosidad": "morosidad",
    "roe_rentabilidad_del_patrimonio": "roe",
    "roa_rentabilidad_de_los_activos": "roa",
    "indicador_de_eficiencia": "cti",
    "activos_netos_totales": "activos",
}


def mapear_indicador(nombre_normalizado):
    """Mapea el nombre normalizado de un indicador del API al id del front."""
    if not nombre_normalizado:
        return None
    return CAMPO_A_INDICADOR.get(nombre_normalizado)

# Campos que identifican entidad / periodo / tipo en la respuesta del API.
CAMPOS_ENTIDAD = ["entidad", "nombreEntidad", "nombre_entidad", "institucion",
                  "razonSocial", "razon_social"]
CAMPOS_PERIODO = ["periodo", "fechaCorte", "fecha_corte", "fecha", "mes",
                  "periodoStr", "periodo_str"]
CAMPOS_TIPO = ["tipoEntidad", "tipo_entidad", "tipoEntidadNombre",
               "tipo_entidad_nombre"]


def _normalizar(texto):
    if texto is None:
        return ""
    t = str(texto).strip().lower()
    for a, b in {"á": "a", "é": "e", "í": "i", "ó": "o", "ú": "u",
                 "ñ": "n", "ü": "u"}.items():
        t = t.replace(a, b)
    # colapsar separadores a _
    for ch in (" ", ".", "-", "/", "(", ")", "%"):
        t = t.replace(ch, "_")
    while "__" in t:
        t = t.replace("__", "_")
    return t.strip("_")


def _headers():
    api_key = os.environ.get("SIB_SUBSCRIPTION_KEY", "")
    if not api_key:
        sys.exit("ERROR: SIB_SUBSCRIPTION_KEY no esta definida en el entorno.")
    return {
        "Ocp-Apim-Subscription-Key": api_key,
        "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                       "AppleWebKit/537.36 (KHTML, like Gecko) "
                       "Chrome/111.0.0.0 Safari/537.36"),
        "accept": "application/json",
    }


def _backoff(attempt, retry_after=None):
    if retry_after:
        try:
            time.sleep(float(retry_after))
            return
        except (ValueError, TypeError):
            pass
    time.sleep(min(30, 2 ** (attempt - 1)) + random.uniform(0, 1.5))


def _flatten(obj, prefix=""):
    """Aplana un dict anidado con notacion de punto, como fromJSON(flatten=TRUE)."""
    items = {}
    if isinstance(obj, dict):
        for k, v in obj.items():
            nk = f"{prefix}.{k}" if prefix else k
            if isinstance(v, (dict, list)):
                items.update(_flatten(v, nk))
            else:
                items[nk] = v
    elif isinstance(obj, list):
        # listas de escalares: unir; listas de dicts: indexar
        for i, v in enumerate(obj):
            nk = f"{prefix}.{i}" if prefix else str(i)
            if isinstance(v, (dict, list)):
                items.update(_flatten(v, nk))
            else:
                items[nk] = v
    else:
        items[prefix] = obj
    return items


def consultar(client, tipo, periodo, verbose=True):
    """Descarga todas las paginas de indicadores/financieros para tipo+periodo."""
    params = {
        "periodoInicial": periodo,
        "periodoFinal": periodo,
        "tipoEntidad": tipo,
        "registros": SIB_REGISTROS,
        "paginas": 1,
    }
    registros = []
    next_page = True
    url = SIB_BASE_URL + ENDPOINT

    while next_page:
        attempt = 1
        while True:
            try:
                resp = client.get(url, params=params, timeout=TIMEOUT_SEC)
            except httpx.RequestError as e:
                if verbose:
                    print(f"  !! {tipo} {periodo} pag={params['paginas']} red: {e}")
                if attempt >= MAX_ATTEMPTS:
                    return registros
                _backoff(attempt)
                attempt += 1
                continue

            status = resp.status_code
            if 200 <= status < 300:
                try:
                    dat = resp.json()
                except json.JSONDecodeError:
                    if verbose:
                        print(f"  XX {tipo} {periodo}: JSON parse error")
                    return registros
                if isinstance(dat, dict) and "data" in dat:
                    dat = dat["data"]
                if isinstance(dat, dict):
                    dat = [dat]
                if dat:
                    registros.extend(dat)

                pag = resp.headers.get("x-pagination")
                if not pag:
                    next_page = False
                else:
                    try:
                        meta = json.loads(pag)
                        next_page = bool(meta.get("HasNext", False))
                    except json.JSONDecodeError:
                        next_page = False

                if verbose:
                    print(f"  OK {tipo} {periodo} pag {params['paginas']} "
                          f"| acum={len(registros)}")
                params["paginas"] += 1
                time.sleep(random.uniform(*SLEEP_BETWEEN))
                break

            body = resp.text[:200].strip()
            if verbose:
                print(f"  XX {tipo} {periodo} pag={params['paginas']} "
                      f"HTTP {status} | {body}")
            if status not in RETRYABLE or attempt >= MAX_ATTEMPTS:
                return registros
            _backoff(attempt, resp.headers.get("retry-after"))
            attempt += 1

    return registros


def _buscar_campo(reg_norm, candidatos):
    """Busca el primer candidato presente en el registro normalizado."""
    for c in candidatos:
        nc = _normalizar(c)
        if nc in reg_norm and reg_norm[nc] not in (None, ""):
            return reg_norm[nc]
    return None


def _num(v):
    if v is None or v == "":
        return None
    if isinstance(v, (int, float)):
        return float(v)
    s = str(v).replace("%", "").replace(",", "").strip()
    try:
        return float(s)
    except ValueError:
        return None


def transformar_a_long(registros, tipo, periodo_fallback, diagnostico=False):
    """Convierte registros crudos del API a filas long:
       { periodo, entidad, tipo_entidad, indicador, valor }

    El API v2 ya devuelve formato LARGO: cada registro trae los campos
    entidad / indicador / periodo / tipoEntidad / valor, con el nombre del
    indicador como VALOR del campo `indicador` (no como nombre de columna).
    Se mantiene el modo ancho como fallback por si el API cambiara.
    """
    filas = []
    indicadores_vistos = {}
    vistos = set()  # (periodo, entidad, indicador) para no duplicar variantes

    for reg in registros:
        plano = _flatten(reg)
        reg_norm = {_normalizar(k): v for k, v in plano.items()}

        entidad = _buscar_campo(reg_norm, CAMPOS_ENTIDAD)
        periodo = _buscar_campo(reg_norm, CAMPOS_PERIODO) or periodo_fallback
        # normalizar periodo a YYYY-MM
        if periodo:
            ps = str(periodo).strip()
            if len(ps) >= 7 and ps[4] == "-":
                periodo = ps[:7]
            elif len(ps) == 6 and ps.isdigit():
                periodo = f"{ps[:4]}-{ps[4:6]}"

        ind_raw = reg_norm.get("indicador")
        if ind_raw not in (None, ""):
            # ---- FORMATO LARGO (caso real del API v2) ----
            ind_norm = _normalizar(ind_raw)
            indicador = mapear_indicador(ind_norm)
            if diagnostico:
                indicadores_vistos[ind_norm] = indicador
            if not indicador:
                continue
            valnum = _num(reg_norm.get("valor"))
            if valnum is None:
                continue
            clave = (periodo, _normalizar(entidad), indicador)
            if clave in vistos:
                continue
            vistos.add(clave)
            filas.append({
                "periodo": periodo,
                "entidad": entidad,
                "tipo_entidad": tipo,
                "indicador": indicador,
                "valor": valnum,
            })
            continue

        # ---- FORMATO ANCHO (fallback): columnas = indicadores ----
        for campo_norm, valor in reg_norm.items():
            indicador = mapear_indicador(campo_norm)
            if not indicador:
                continue
            valnum = _num(valor)
            if valnum is None:
                continue
            filas.append({
                "periodo": periodo,
                "entidad": entidad,
                "tipo_entidad": tipo,
                "indicador": indicador,
                "valor": valnum,
            })

    if diagnostico and indicadores_vistos:
        print("\n=== DIAGNOSTICO: indicadores devueltos por el API ===")
        for nombre in sorted(indicadores_vistos):
            destino = indicadores_vistos[nombre]
            marca = f"  -> {destino}" if destino else "  (sin mapear)"
            print(f"  {nombre}{marca}")
        print("=== fin diagnostico ===\n")

    return filas


def meses_recientes(n):
    hoy = date.today()
    y, m = hoy.year, hoy.month
    out = []
    for _ in range(n):
        out.append(f"{y:04d}-{m:02d}")
        m -= 1
        if m == 0:
            m = 12
            y -= 1
    return list(reversed(out))


def _parse_periodo(txt, fallback=None):
    """Convierte 'YYYY-MM' o 'YYYYMM' a (anio, mes). Devuelve fallback si vacio."""
    if not txt:
        return fallback
    s = str(txt).strip()
    if len(s) == 6 and s.isdigit():
        s = f"{s[:4]}-{s[4:6]}"
    try:
        y, m = s.split("-")[:2]
        return int(y), int(m)
    except (ValueError, IndexError):
        sys.exit(f"ERROR: periodo invalido '{txt}' (usa YYYY-MM).")


def meses_rango(inicio, fin):
    """Lista de meses 'YYYY-MM' desde inicio (anio, mes) hasta fin (anio, mes), inclusive."""
    (yi, mi), (yf, mf) = inicio, fin
    out = []
    y, m = yi, mi
    while (y, m) <= (yf, mf):
        out.append(f"{y:04d}-{m:02d}")
        m += 1
        if m == 13:
            m = 1
            y += 1
    return out


def main():
    diagnostico = "--diagnostico" in sys.argv
    verbose = "--quiet" not in sys.argv

    # Rango de periodos:
    #   - Por defecto: ventana movil de MESES_VENTANA meses (comportamiento
    #     original del pipeline programado).
    #   - Si se define SIB_PERIODO_INICIAL (YYYY-MM), se descarga el rango
    #     completo desde ese mes hasta SIB_PERIODO_FINAL (o el mes actual).
    #     Sirve para reconstruir la historia (ej. 2021-01 -> hoy).
    periodo_inicial = os.environ.get("SIB_PERIODO_INICIAL", "").strip()
    if periodo_inicial:
        hoy = date.today()
        inicio = _parse_periodo(periodo_inicial)
        fin = _parse_periodo(os.environ.get("SIB_PERIODO_FINAL"),
                             fallback=(hoy.year, hoy.month))
        meses = meses_rango(inicio, fin)
    else:
        meses = meses_recientes(MESES_VENTANA)
    print(f">> Ventana: {meses[0]} -> {meses[-1]} ({len(meses)} meses)")
    print(f">> Tipos: {', '.join(TIPOS_ENTIDAD)}")

    todas = []
    with httpx.Client(headers=_headers()) as client:
        for tipo in TIPOS_ENTIDAD:
            print(f"\n--- {tipo} ---")
            for periodo in meses:
                regs = consultar(client, tipo, periodo, verbose=verbose)
                if not regs:
                    continue
                filas = transformar_a_long(
                    regs, tipo, periodo,
                    diagnostico=(diagnostico and not todas)  # solo 1ra vez
                )
                todas.extend(filas)

    if not todas:
        print("\n!! Sin datos. No se escribe snapshot "
              "(evita borrar el existente).")
        sys.exit(1)

    # periodo mas reciente con datos
    periodos = sorted({f["periodo"] for f in todas if f["periodo"]})
    latest = periodos[-1] if periodos else None

    snapshot = {
        "generado_en": datetime.now(timezone.utc).isoformat(),
        "fuente": "SIB API v2 - indicadores/financieros",
        "endpoint": ENDPOINT,
        "ultimo_periodo": latest,
        "formato": "long",
        "rows": todas,
    }

    os.makedirs(os.path.dirname(OUTPUT_PATH) or ".", exist_ok=True)
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(snapshot, f, ensure_ascii=False, separators=(",", ":"))

    print(f"\n=== OK ===")
    print(f"  filas: {len(todas)}")
    print(f"  periodos: {len(periodos)} | ultimo: {latest}")
    print(f"  salida: {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
