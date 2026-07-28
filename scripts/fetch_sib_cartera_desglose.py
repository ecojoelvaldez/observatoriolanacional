#!/usr/bin/env python3
# ==============================================================================
# fetch_sib_cartera_desglose.py
# Descarga el DESGLOSE de cartera de creditos y de captacion del API SIB v2 y
# genera un snapshot agregado que permite construir las series por producto:
#   Cartera : total, hipotecario, consumo (total / sin tarjeta), tarjetas de
#             credito, comercial (total / pymes / mayores deudores-interinos).
#   Captacion: total, ahorros, depositos.
#
# Fuente:
#   - Cartera  : endpoint granular  carteras/creditos  (trae tipoCartera,
#                tipoCredito, moneda, region, provincia, facilidad,
#                tipoCliente, genero por fila micro). Se agrega AL VUELO
#                sumando `deuda` por
#                (periodo, entidad, tipoCartera, tipoCredito, moneda, region),
#                colapsando el resto de dimensiones. Asi el snapshot es chico
#                aunque el endpoint devuelva millones de filas micro.
#   - Captacion: endpoint  captaciones/moneda  (trae instrumentoCaptacion y
#                balance). Se agrega sumando `balance` por
#                (periodo, entidad, partidaNivel2, instrumentoCaptacion).
#
# Misma infraestructura que fetch_sib_cartera.py (auth, paginacion, backoff).
#
# Variables de entorno:
#   SIB_SUBSCRIPTION_KEY        (obligatoria)
#   SIB_TIPOS                   tipos de entidad, coma-sep (def "AAyP")
#   SIB_PERIODO_INICIAL         YYYY-MM (def "2021-01")
#   SIB_PERIODO_FINAL           YYYY-MM (def mes actual)
#   SIB_DESGLOSE_SOLO           "cartera" | "captacion" | "both" (def "both")
#   SIB_DESGLOSE_SNAPSHOT_PATH  salida (def data/sib_cartera_desglose_snapshot.json)
# ==============================================================================

import os
import sys
import json
import time
import random
from datetime import date, datetime, timezone

import httpx

SIB_BASE_URL = "https://apis.sb.gob.do/estadisticas/v2/"
EP_CARTERA = "carteras/creditos"
EP_CAPTACION = "captaciones/moneda"

SIB_REGISTROS = 1000
TIMEOUT_SEC = 120
MAX_ATTEMPTS = 6
SLEEP_BETWEEN = (0.4, 1.1)
RETRYABLE = {429, 500, 502, 503, 504}

OUTPUT_PATH = os.environ.get("SIB_DESGLOSE_SNAPSHOT_PATH",
                             "data/sib_cartera_desglose_snapshot.json")


def _tipos():
    env = os.environ.get("SIB_TIPOS", "").strip()
    if env:
        return [t.strip() for t in env.split(",") if t.strip()]
    return ["AAyP"]


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


def _parse_periodo(txt, fallback=None):
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


def consultar_agrega(client, endpoint, tipo, periodo, acumular, verbose=True):
    """Descarga todas las paginas de `endpoint` para tipo+periodo y llama
    acumular(fila_cruda) por cada registro. Devuelve (n_filas, status)."""
    params = {
        "periodoInicial": periodo,
        "periodoFinal": periodo,
        "tipoEntidad": tipo,
        "registros": SIB_REGISTROS,
        "paginas": 1,
    }
    n = 0
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
                    return n, ultimo_status
                _backoff(attempt)
                attempt += 1
                continue

            ultimo_status = resp.status_code
            if 200 <= resp.status_code < 300:
                try:
                    dat = resp.json()
                except json.JSONDecodeError:
                    if verbose:
                        print(f"  XX {endpoint} {tipo} {periodo}: JSON error")
                    return n, ultimo_status
                if isinstance(dat, dict) and "data" in dat:
                    dat = dat["data"]
                if isinstance(dat, dict):
                    dat = [dat]
                for reg in dat or []:
                    acumular(reg)
                    n += 1

                pag = resp.headers.get("x-pagination")
                total = None
                if not pag:
                    next_page = False
                else:
                    try:
                        meta = json.loads(pag)
                        next_page = bool(meta.get("HasNext", False))
                        total = meta.get("TotalRecords")
                    except json.JSONDecodeError:
                        next_page = False

                if verbose:
                    extra = f" / total={total}" if total is not None else ""
                    print(f"  OK {endpoint} {tipo} {periodo} "
                          f"pag {params['paginas']} | acum={n}{extra}")
                params["paginas"] += 1
                if next_page:
                    time.sleep(random.uniform(*SLEEP_BETWEEN))
                break

            body = resp.text[:160].strip()
            if verbose:
                print(f"  XX {endpoint} {tipo} {periodo} "
                      f"pag={params['paginas']} HTTP {resp.status_code} | {body}")
            if resp.status_code not in RETRYABLE or attempt >= MAX_ATTEMPTS:
                return n, ultimo_status
            _backoff(attempt, resp.headers.get("retry-after"))
            attempt += 1

    return n, ultimo_status


def main():
    verbose = "--quiet" not in sys.argv
    tipos = _tipos()
    solo = os.environ.get("SIB_DESGLOSE_SOLO", "both").strip().lower()

    hoy = date.today()
    # SIB_PERIODOS: lista explicita de cortes "YYYY-MM" separados por coma.
    # Tiene prioridad sobre el rango; permite bajar solo ciertos cortes
    # (ej. cierres de diciembre + ultimo mes) y hace el script replicable a
    # meses nuevos con solo editar la lista.
    periodos_env = os.environ.get("SIB_PERIODOS", "").strip()
    if periodos_env.lower() == "auto":
        # Modo estandar del Observatorio: cierres de diciembre de cada anio
        # desde SIB_ANIO_INICIAL, mas los ultimos meses para capturar el corte
        # mas reciente que la SIB haya publicado (los meses aun sin publicar
        # devuelven 0 filas y se descartan solos). Asi la corrida programada
        # se mantiene al dia sin editar la lista cada mes.
        anio_ini = int(os.environ.get("SIB_ANIO_INICIAL", "2021"))
        recientes = int(os.environ.get("SIB_MESES_RECIENTES", "4"))
        meses = [f"{y:04d}-12" for y in range(anio_ini, hoy.year)]
        y, m = hoy.year, hoy.month
        for _ in range(recientes):
            meses.append(f"{y:04d}-{m:02d}")
            m -= 1
            if m == 0:
                m, y = 12, y - 1
        meses = sorted(set(meses))
    elif periodos_env:
        meses = []
        for p in periodos_env.split(","):
            p = p.strip()
            if not p:
                continue
            y, m = _parse_periodo(p)
            meses.append(f"{y:04d}-{m:02d}")
        meses = sorted(set(meses))
    else:
        inicio = _parse_periodo(os.environ.get("SIB_PERIODO_INICIAL"),
                                fallback=(2021, 1))
        fin = _parse_periodo(os.environ.get("SIB_PERIODO_FINAL"),
                             fallback=(hoy.year, hoy.month))
        meses = meses_rango(inicio, fin)

    print(f">> Cartera EP: {EP_CARTERA} | Captacion EP: {EP_CAPTACION}")
    print(f">> Ventana: {meses[0]} -> {meses[-1]} ({len(meses)} meses)")
    print(f">> Tipos: {', '.join(tipos)} | solo={solo}")

    # Agregadores
    cartera = {}   # (periodo, entidad, tipoCartera, tipoCredito, moneda, region) -> suma deuda
    cart_meta = {}  # clave -> tipo de entidad
    captacion = {}  # (periodo, entidad, partida, instrumento) -> suma balance
    capt_meta = {}

    def acc_cartera(tipo):
        def _f(reg):
            rn = {_normalizar(k): v for k, v in reg.items()}
            periodo = _periodo_ym(rn.get("periodo"), None)
            entidad = rn.get("entidad")
            deuda = _num(rn.get("deuda"))
            if not periodo or not entidad or deuda is None:
                return
            tc = (rn.get("tipocartera") or "N/D")
            tcred = (rn.get("tipocredito") or "N/D")
            # La moneda se conserva en la clave: el front del Observatorio
            # grafica solo moneda nacional, asi que sin esta dimension no se
            # puede reproducir su corte (la diferencia aparece en consumo).
            mon = (rn.get("moneda") or "N/D")
            # La region alimenta el drilldown regional y el market share por
            # producto/region del reporte. Se agrega solo region (4 valores);
            # provincia multiplicaria el snapshot por ~32 sin usarse hoy.
            # A cambio se sueltan facilidad y tipoCliente: ningun producto se
            # clasifica con ellos (el clasificador usa tipoCartera/tipoCredito).
            reg_geo = (rn.get("region") or "N/D")
            clave = (periodo, str(entidad), str(tc), str(tcred),
                     str(mon), str(reg_geo))
            cartera[clave] = cartera.get(clave, 0.0) + deuda
            if clave not in cart_meta:
                cart_meta[clave] = tipo
        return _f

    def acc_captacion(tipo):
        def _f(reg):
            rn = {_normalizar(k): v for k, v in reg.items()}
            periodo = _periodo_ym(rn.get("periodo"), None)
            entidad = rn.get("entidad")
            balance = _num(rn.get("balance"))
            if not periodo or not entidad or balance is None:
                return
            partida = (rn.get("partidanivel2") or "N/D")
            instr = (rn.get("instrumentocaptacion") or "N/D")
            clave = (periodo, str(entidad), str(partida), str(instr))
            captacion[clave] = captacion.get(clave, 0.0) + balance
            if clave not in capt_meta:
                capt_meta[clave] = tipo
        return _f

    with httpx.Client(headers=_headers()) as client:
        if solo in ("both", "cartera"):
            for tipo in tipos:
                print(f"\n--- CARTERA {tipo} ---")
                for periodo in meses:
                    n, st = consultar_agrega(client, EP_CARTERA, tipo, periodo,
                                             acc_cartera(tipo), verbose=verbose)
                    if verbose and n == 0:
                        print(f"  .. {tipo} {periodo}: sin filas (HTTP {st})")
        if solo in ("both", "captacion"):
            for tipo in tipos:
                print(f"\n--- CAPTACION {tipo} ---")
                for periodo in meses:
                    n, st = consultar_agrega(client, EP_CAPTACION, tipo, periodo,
                                             acc_captacion(tipo), verbose=verbose)
                    if verbose and n == 0:
                        print(f"  .. {tipo} {periodo}: sin filas (HTTP {st})")

    cartera_rows = []
    for clave, val in cartera.items():
        periodo, entidad, tc, tcred, mon, reg_geo = clave
        cartera_rows.append({
            "periodo": periodo, "entidad": entidad,
            "tipo_entidad": cart_meta[clave],
            "tipoCartera": tc, "tipoCredito": tcred,
            "moneda": mon, "region": reg_geo,
            "deuda": round(val, 2),
        })
    captacion_rows = []
    for (periodo, entidad, partida, instr), val in captacion.items():
        captacion_rows.append({
            "periodo": periodo, "entidad": entidad,
            "tipo_entidad": capt_meta[(periodo, entidad, partida, instr)],
            "partidaNivel2": partida, "instrumentoCaptacion": instr,
            "balance": round(val, 2),
        })

    if not cartera_rows and not captacion_rows:
        print("\n!! Sin datos. No se escribe snapshot.")
        sys.exit(1)

    periodos = sorted({r["periodo"] for r in cartera_rows + captacion_rows})
    snapshot = {
        "generado_en": datetime.now(timezone.utc).isoformat(),
        "fuente": "SIB API v2 - carteras/creditos (granular) + captaciones/moneda",
        "endpoints": {"cartera": EP_CARTERA, "captacion": EP_CAPTACION},
        "ultimo_periodo": periodos[-1] if periodos else None,
        "tipos": tipos,
        # Catalogos de valores distintos (para mapear productos):
        "catalogos": {
            "tipoCartera": sorted({r["tipoCartera"] for r in cartera_rows}),
            "tipoCredito": sorted({r["tipoCredito"] for r in cartera_rows}),
            "moneda": sorted({r["moneda"] for r in cartera_rows}),
            "region": sorted({r["region"] for r in cartera_rows}),
            "instrumentoCaptacion": sorted({r["instrumentoCaptacion"]
                                            for r in captacion_rows}),
            "partidaNivel2": sorted({r["partidaNivel2"] for r in captacion_rows}),
        },
        "cartera_agg": cartera_rows,
        "captacion_agg": captacion_rows,
    }

    os.makedirs(os.path.dirname(OUTPUT_PATH) or ".", exist_ok=True)
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(snapshot, f, ensure_ascii=False, separators=(",", ":"))

    print("\n=== OK ===")
    print(f"  filas cartera agregadas: {len(cartera_rows)}")
    print(f"  filas captacion agregadas: {len(captacion_rows)}")
    print(f"  periodos: {len(periodos)} | ultimo: {periodos[-1] if periodos else None}")
    print(f"  catalogos tipoCartera: {snapshot['catalogos']['tipoCartera']}")
    print(f"  salida: {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
