#!/usr/bin/env python3
# ==============================================================================
# fetch_sib_cartera.py
# Descarga la CARTERA DE CREDITOS (balance/deuda) del API SIB v2 y genera un
# snapshot estatico (data/sib_cartera_snapshot.json) en formato "long" que el
# index.html del Observatorio ya sabe leer (buildSibDataFromLong).
#
# Complementa a fetch_sib_indicadores.py: el endpoint indicadores/financieros
# NO trae el balance de cartera en RD$; este script usa el endpoint dedicado
#   carteras/creditos/moneda
# (documentado en el portal de APIs SB y en el paquete R supeRbancos), que
# devuelve deuda por entidad / periodo / tipo de moneda. Sumando las monedas
# se obtiene el balance total por entidad, que es lo que grafica el front en
# la serie "cartera" (RD$ MM) del benchmark SIB.
#
# Misma infraestructura que fetch_sib_indicadores.py:
#   - Base: https://apis.sb.gob.do/estadisticas/v2/
#   - Auth: header Ocp-Apim-Subscription-Key (env SIB_SUBSCRIPTION_KEY)
#   - Paginacion: header x-pagination con HasNext / TotalPages / TotalRecords
#   - Retries con backoff en 429/500/502/503/504
#
# Modo --diagnostico: ademas de la descarga, sondea una lista de endpoints
# candidatos de la familia carteras/* (region, provincia, sector economico,
# etc.) e imprime HTTP status, total de registros y campos devueltos. Sirve
# para verificar con la key real que rebanadas ofrece el API para el drilldown
# regional/segmento del observatorio (hoy alimentado por CSV de SIMBAD).
# ==============================================================================

import os
import sys
import json
import time
import random
from datetime import date, datetime, timezone

import httpx

SIB_BASE_URL = "https://apis.sb.gob.do/estadisticas/v2/"
ENDPOINT = "carteras/creditos/moneda"

# Tipos de entidad (espejo del pipeline de indicadores)
TIPOS_ENTIDAD = ["BM", "AAyP", "BAyC"]

SIB_REGISTROS = 300
TIMEOUT_SEC = 90
MAX_ATTEMPTS = 6
SLEEP_BETWEEN = (0.6, 1.8)
RETRYABLE = {429, 500, 502, 503, 504}

# Ventana movil de descarga; alineada con fetch_sib_indicadores.py para que
# el front pueda cruzar cartera contra morosidad/roe/roa en el mismo rango.
MESES_VENTANA = int(os.environ.get("SIB_CARTERA_MESES", "").strip() or "26")

OUTPUT_PATH = os.environ.get("SIB_CARTERA_SNAPSHOT_PATH",
                             "data/sib_cartera_snapshot.json")

# Endpoints candidatos para el sondeo --diagnostico. Se pueden ampliar sin
# tocar codigo via env SIB_PROBE_ENDPOINTS (lista separada por comas).
PROBE_ENDPOINTS = [
    "carteras/creditos/moneda",
    "carteras/creditos/region",
    "carteras/creditos/provincia",
    "carteras/creditos/sector-economico",
    "carteras/creditos/sectores-economicos",
    "carteras/creditos/tipo-cartera",
    "carteras/creditos/tipo-cliente",
    "carteras/creditos/genero",
    "carteras/creditos/facilidades",
    "carteras/creditos/clasificacion-riesgo",
    "carteras/creditos",
    "estados/situacion/eif",
    "captaciones/sector-depositante",
]

# Campos que identifican entidad / periodo / tipo / moneda / monto en la
# respuesta del API (deteccion flexible, igual que el pipeline de indicadores).
CAMPOS_ENTIDAD = ["entidad", "nombreEntidad", "nombre_entidad", "institucion",
                  "razonSocial", "razon_social"]
CAMPOS_PERIODO = ["periodo", "fechaCorte", "fecha_corte", "fecha", "mes",
                  "periodoStr", "periodo_str"]
CAMPOS_TIPO = ["tipoEntidad", "tipo_entidad", "tipoEntidadNombre",
               "tipo_entidad_nombre"]
CAMPOS_MONEDA = ["tipoMoneda", "tipo_moneda", "moneda", "monedaNombre"]
CAMPOS_MONTO = ["deuda", "deudaTotal", "deuda_total", "balance", "monto",
                "valor", "saldo"]


def _normalizar(texto):
    if texto is None:
        return ""
    t = str(texto).strip().lower()
    for a, b in {"á": "a", "é": "e", "í": "i", "ó": "o", "ú": "u",
                 "ñ": "n", "ü": "u"}.items():
        t = t.replace(a, b)
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
    """Aplana un dict anidado con notacion de punto."""
    items = {}
    if isinstance(obj, dict):
        for k, v in obj.items():
            nk = f"{prefix}.{k}" if prefix else k
            if isinstance(v, (dict, list)):
                items.update(_flatten(v, nk))
            else:
                items[nk] = v
    elif isinstance(obj, list):
        for i, v in enumerate(obj):
            nk = f"{prefix}.{i}" if prefix else str(i)
            if isinstance(v, (dict, list)):
                items.update(_flatten(v, nk))
            else:
                items[nk] = v
    else:
        items[prefix] = obj
    return items


def consultar(client, endpoint, tipo, periodo, registros=SIB_REGISTROS,
              max_paginas=None, verbose=True):
    """Descarga todas las paginas de un endpoint para tipo+periodo.

    Devuelve (registros, ultimo_status). ultimo_status permite al modo
    diagnostico distinguir 404 (no existe) de 200 vacio (existe sin datos).
    """
    params = {
        "periodoInicial": periodo,
        "periodoFinal": periodo,
        "tipoEntidad": tipo,
        "registros": registros,
        "paginas": 1,
    }
    filas = []
    next_page = True
    url = SIB_BASE_URL + endpoint
    ultimo_status = None

    while next_page:
        attempt = 1
        while True:
            try:
                resp = client.get(url, params=params, timeout=TIMEOUT_SEC)
            except httpx.RequestError as e:
                if verbose:
                    print(f"  !! {endpoint} {tipo} {periodo} "
                          f"pag={params['paginas']} red: {e}")
                if attempt >= MAX_ATTEMPTS:
                    return filas, ultimo_status
                _backoff(attempt)
                attempt += 1
                continue

            ultimo_status = resp.status_code
            if 200 <= resp.status_code < 300:
                try:
                    dat = resp.json()
                except json.JSONDecodeError:
                    if verbose:
                        print(f"  XX {endpoint} {tipo} {periodo}: "
                              "JSON parse error")
                    return filas, ultimo_status
                if isinstance(dat, dict) and "data" in dat:
                    dat = dat["data"]
                if isinstance(dat, dict):
                    dat = [dat]
                if dat:
                    filas.extend(dat)

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
                    print(f"  OK {endpoint} {tipo} {periodo} "
                          f"pag {params['paginas']} | acum={len(filas)}")
                params["paginas"] += 1
                if max_paginas and params["paginas"] > max_paginas:
                    next_page = False
                if next_page:
                    time.sleep(random.uniform(*SLEEP_BETWEEN))
                break

            body = resp.text[:200].strip()
            if verbose:
                print(f"  XX {endpoint} {tipo} {periodo} "
                      f"pag={params['paginas']} HTTP {resp.status_code} | {body}")
            if resp.status_code not in RETRYABLE or attempt >= MAX_ATTEMPTS:
                return filas, ultimo_status
            _backoff(attempt, resp.headers.get("retry-after"))
            attempt += 1

    return filas, ultimo_status


def _buscar_campo(reg_norm, candidatos):
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


def _periodo_ym(valor, fallback):
    periodo = valor or fallback
    if not periodo:
        return None
    ps = str(periodo).strip()
    if len(ps) >= 7 and ps[4] == "-":
        return ps[:7]
    if len(ps) == 6 and ps.isdigit():
        return f"{ps[:4]}-{ps[4:6]}"
    return ps


def transformar(registros, tipo, periodo_fallback):
    """Convierte registros crudos de carteras/creditos/moneda en:
       - detalle: { periodo, entidad, tipo_entidad, moneda, valor }
       - los totales por entidad se agregan luego sumando monedas.
    """
    detalle = []
    for reg in registros:
        plano = _flatten(reg)
        reg_norm = {_normalizar(k): v for k, v in plano.items()}

        entidad = _buscar_campo(reg_norm, CAMPOS_ENTIDAD)
        periodo = _periodo_ym(_buscar_campo(reg_norm, CAMPOS_PERIODO),
                              periodo_fallback)
        moneda = _buscar_campo(reg_norm, CAMPOS_MONEDA)
        valor = _num(_buscar_campo(reg_norm, CAMPOS_MONTO))
        if not entidad or not periodo or valor is None:
            continue
        detalle.append({
            "periodo": periodo,
            "entidad": entidad,
            "tipo_entidad": tipo,
            "moneda": str(moneda).strip() if moneda else "N/D",
            "valor": valor,
        })
    return detalle


def agregar_totales(detalle):
    """Suma la deuda de todas las monedas por (periodo, entidad) y produce
    filas long con indicador='cartera', que es lo que el front mapea via
    sibMetricId a la serie de balance de cartera (RD$ MM)."""
    acc = {}
    for fila in detalle:
        clave = (fila["periodo"], _normalizar(fila["entidad"]))
        if clave not in acc:
            acc[clave] = {
                "periodo": fila["periodo"],
                "entidad": fila["entidad"],
                "tipo_entidad": fila["tipo_entidad"],
                "indicador": "cartera",
                "valor": 0.0,
            }
        acc[clave]["valor"] += fila["valor"]
    filas = list(acc.values())
    for f in filas:
        f["valor"] = round(f["valor"], 2)
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


def diagnostico_endpoints(client):
    """Sondea endpoints candidatos con la key real e imprime que existe.

    Usa un periodo con rezago (3 meses atras) para maximizar la probabilidad
    de datos publicados, 1 sola pagina y pocos registros: barato y suficiente
    para ver status + campos.
    """
    extra = os.environ.get("SIB_PROBE_ENDPOINTS", "")
    candidatos = PROBE_ENDPOINTS + [e.strip() for e in extra.split(",")
                                    if e.strip()]
    periodo = meses_recientes(4)[0]  # hace ~3 meses

    print("\n=== DIAGNOSTICO: sondeo de endpoints con la key real ===")
    print(f"    periodo de prueba: {periodo} | tipoEntidad=BM | 1 pagina\n")
    resumen = []
    for ep in candidatos:
        regs, status = consultar(client, ep, "BM", periodo,
                                 registros=5, max_paginas=1, verbose=False)
        if regs:
            campos = sorted(_flatten(regs[0]).keys())
            resumen.append((ep, status, len(regs), campos))
            print(f"  [OK ] {ep} -> HTTP {status} | {len(regs)} registro(s)")
            print(f"        campos: {', '.join(campos)}")
            # Muestra registros crudos para ver las etiquetas de categoria
            # (tipo de cartera, facilidad, etc.), no solo los nombres de campo.
            for i, r in enumerate(regs[:5]):
                print(f"        muestra[{i}]: {json.dumps(r, ensure_ascii=False)}")
        else:
            resumen.append((ep, status, 0, []))
            print(f"  [ -- ] {ep} -> HTTP {status} | sin registros")
        time.sleep(random.uniform(*SLEEP_BETWEEN))
    print("\n=== fin diagnostico ===\n")
    return resumen


def main():
    diagnostico = "--diagnostico" in sys.argv
    verbose = "--quiet" not in sys.argv

    meses = meses_recientes(MESES_VENTANA)
    print(f">> Endpoint: {ENDPOINT}")
    print(f">> Ventana: {meses[0]} -> {meses[-1]} ({len(meses)} meses)")
    print(f">> Tipos: {', '.join(TIPOS_ENTIDAD)}")

    detalle_total = []
    with httpx.Client(headers=_headers()) as client:
        if diagnostico:
            diagnostico_endpoints(client)

        for tipo in TIPOS_ENTIDAD:
            print(f"\n--- {tipo} ---")
            for periodo in meses:
                regs, _status = consultar(client, ENDPOINT, tipo, periodo,
                                          verbose=verbose)
                if not regs:
                    continue
                detalle_total.extend(transformar(regs, tipo, periodo))

    if not detalle_total:
        print("\n!! Sin datos de cartera. No se escribe snapshot "
              "(evita borrar el existente).")
        sys.exit(1)

    filas = agregar_totales(detalle_total)
    periodos = sorted({f["periodo"] for f in filas if f["periodo"]})
    latest = periodos[-1] if periodos else None

    snapshot = {
        "generado_en": datetime.now(timezone.utc).isoformat(),
        "fuente": "SIB API v2 - carteras/creditos/moneda",
        "endpoint": ENDPOINT,
        "ultimo_periodo": latest,
        "formato": "long",
        "rows": filas,
        # Desglose por moneda: no lo usa el front hoy, pero deja listo el
        # detalle para una vista futura sin re-descargar del API.
        "por_moneda": detalle_total,
    }

    os.makedirs(os.path.dirname(OUTPUT_PATH) or ".", exist_ok=True)
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(snapshot, f, ensure_ascii=False, separators=(",", ":"))

    print("\n=== OK ===")
    print(f"  filas cartera (totales entidad/mes): {len(filas)}")
    print(f"  filas detalle por moneda: {len(detalle_total)}")
    print(f"  periodos: {len(periodos)} | ultimo: {latest}")
    print(f"  salida: {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
