#!/usr/bin/env python3
# ==============================================================================
# verificar_frescura.py
# Revisa el corte mas reciente de cada archivo de datos que consume el sitio y
# falla la corrida cuando uno se queda atras.
#
# Por que existe: los pipelines terminaban en verde aunque el API devolviera
# cero filas para los meses recientes. El 5 de septiembre de 2026 la corrida de
# cartera reporto exito con carteras/creditos vacio desde 2025-12, y el sitio
# siguio graficando ese cierre durante meses sin que nadie lo notara. Un run en
# rojo avisa por correo al dueno del repositorio sin configurar nada mas.
#
# Comprueba dos cosas distintas:
#   1. REGRESION: el corte publicado nunca puede retroceder respecto al que ya
#      estaba en el repositorio. Detecta descargas truncadas y borrados.
#      Siempre es error, sin importar el umbral.
#   2. REZAGO: el corte esta mas atras de lo que corresponde a esa fuente. El
#      umbral es por archivo porque la SIB publica cada endpoint a su ritmo:
#      los indicadores salen con dos meses de rezago y el desglose granular de
#      cartera con mas de un ano.
#
# Uso:
#   python scripts/verificar_frescura.py                # revisa todo
#   python scripts/verificar_frescura.py --solo cartera # subconjunto por nombre
#   python scripts/verificar_frescura.py --aviso        # reporta sin fallar
#
# Para ajustar un umbral se edita CHEQUEOS; no hace falta tocar la logica.
# ==============================================================================

import argparse
import json
import os
import sys
from datetime import date

# Linea base contra la que se mide la regresion. Se actualiza sola en cada
# corrida que pasa, de modo que el corte solo puede avanzar.
BASELINE_PATH = "data/_frescura_baseline.json"

# clave -> (ruta, extractor del corte, rezago maximo en meses, nota)
#
# El extractor recibe el JSON ya cargado y devuelve el corte "YYYY-MM" o None.
# Los rezagos salen del comportamiento observado de cada endpoint de la SIB,
# con un mes de margen sobre lo normal para no fallar por un retraso puntual.
def _ultimo_periodo(d):
    return d.get("ultimo_periodo")


def _max_periodo_filas(campo):
    def _f(d):
        filas = d.get(campo) or []
        ps = [r.get("periodo") for r in filas if isinstance(r, dict) and r.get("periodo")]
        return max(ps) if ps else None
    return _f


def _max_corte(d):
    cortes = d.get("cortes") or []
    return max(cortes) if cortes else None


CHEQUEOS = [
    {
        "clave": "indicadores",
        "ruta": "data/sib_snapshot.json",
        "corte": _ultimo_periodo,
        "rezago_max": 3,
        "nota": "indicadores/financieros · morosidad, ROE, ROA, CTI, activos",
    },
    {
        "clave": "cartera_mensual",
        "ruta": "data/sib_cartera_snapshot.json",
        "corte": _ultimo_periodo,
        "rezago_max": 4,
        "nota": "carteras/creditos/moneda · balance mensual que grafica el front",
    },
    {
        "clave": "captacion",
        "ruta": "data/captacion_sistema.json",
        "corte": _max_corte,
        "rezago_max": 4,
        "nota": "captaciones/moneda · al dia junto con los indicadores",
    },
    {
        "clave": "cartera_granular",
        "ruta": "data/cartera_sistema.json",
        "corte": _max_corte,
        # La SIB publica carteras/creditos (con region, moneda y producto) solo
        # en cierres anuales y con mucho rezago: en septiembre de 2026 el ultimo
        # disponible seguia siendo 2025-12. El umbral cubre ese ciclo; si se
        # pasa de 15 meses es que el cierre anual dejo de publicarse.
        "rezago_max": 15,
        "nota": "carteras/creditos · desglose por producto y region, cierres anuales",
    },
    {
        "clave": "desglose_snapshot",
        "ruta": "data/sib_desglose_snapshot.json",
        "corte": _max_periodo_filas("captacion_agg"),
        "rezago_max": 4,
        "nota": "snapshot crudo del desglose · se mide por captacion, que va al dia",
    },
]


def rezago_meses(corte, hoy):
    """Meses entre el corte YYYY-MM y el mes de `hoy`."""
    y, m = int(corte[:4]), int(corte[5:7])
    return (hoy.year - y) * 12 + (hoy.month - m)


def cargar(ruta):
    with open(ruta, encoding="utf-8") as f:
        return json.load(f)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--solo", default="", help="subcadena para filtrar por clave")
    ap.add_argument("--aviso", action="store_true",
                    help="reportar sin devolver codigo de error")
    args = ap.parse_args()

    hoy = date.today()
    baseline = {}
    if os.path.exists(BASELINE_PATH):
        try:
            baseline = cargar(BASELINE_PATH).get("cortes") or {}
        except (OSError, json.JSONDecodeError):
            baseline = {}

    chequeos = [c for c in CHEQUEOS if args.solo.lower() in c["clave"].lower()]
    if not chequeos:
        print(f"!! Ningun chequeo coincide con --solo {args.solo!r}")
        return 2

    problemas = []
    nuevos = dict(baseline)

    print(f">> Frescura de datos · mes actual {hoy:%Y-%m}")
    print(f"{'archivo':<34} {'corte':<9} {'rezago':<8} {'max':<5} estado")
    print("-" * 78)

    for c in chequeos:
        ruta, clave = c["ruta"], c["clave"]
        if not os.path.exists(ruta):
            problemas.append(f"{clave}: falta el archivo {ruta}")
            print(f"{ruta:<34} {'-':<9} {'-':<8} {c['rezago_max']:<5} FALTA")
            continue
        try:
            corte = c["corte"](cargar(ruta))
        except (OSError, json.JSONDecodeError, ValueError, TypeError) as exc:
            problemas.append(f"{clave}: no se pudo leer {ruta}: {exc}")
            print(f"{ruta:<34} {'-':<9} {'-':<8} {c['rezago_max']:<5} ILEGIBLE")
            continue

        if not corte:
            problemas.append(f"{clave}: {ruta} no declara ningun corte")
            print(f"{ruta:<34} {'-':<9} {'-':<8} {c['rezago_max']:<5} SIN CORTE")
            continue

        rez = rezago_meses(corte, hoy)
        previo = baseline.get(clave)
        estado = "ok"

        if previo and corte < previo:
            estado = "REGRESION"
            problemas.append(
                f"{clave}: el corte retrocedio de {previo} a {corte} "
                f"({ruta}). La descarga entrego menos datos que la anterior."
            )
        elif rez > c["rezago_max"]:
            estado = "REZAGADO"
            problemas.append(
                f"{clave}: corte {corte}, {rez} meses de rezago "
                f"(maximo {c['rezago_max']}) en {ruta} · {c['nota']}"
            )

        print(f"{ruta:<34} {corte:<9} {str(rez) + ' m':<8} {c['rezago_max']:<5} {estado}")
        # La linea base solo avanza: un corte rezagado sigue siendo el techo a
        # respetar, para que la proxima corrida no lo pueda deshacer.
        if not previo or corte > previo:
            nuevos[clave] = corte

    if nuevos != baseline:
        os.makedirs(os.path.dirname(BASELINE_PATH) or ".", exist_ok=True)
        with open(BASELINE_PATH, "w", encoding="utf-8") as f:
            json.dump({"actualizado_en": hoy.isoformat(), "cortes": nuevos},
                      f, ensure_ascii=False, indent=2)
            f.write("\n")

    print()
    if not problemas:
        print("=== OK === todos los cortes dentro de lo esperado")
        return 0

    print("=== PROBLEMAS DE FRESCURA ===")
    for p in problemas:
        print(f"  - {p}")
    print()
    print("Que revisar: si el API devolvio 'sin filas (HTTP 200)' para los meses")
    print("recientes, la SIB aun no publico ese corte y no hay nada que corregir")
    print("en el codigo; si devolvio errores HTTP, revisar la key y los reintentos.")

    if args.aviso:
        print("\n(--aviso: se reporta sin fallar la corrida)")
        return 0
    return 1


if __name__ == "__main__":
    sys.exit(main())
