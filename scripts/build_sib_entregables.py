#!/usr/bin/env python3
# ==============================================================================
# build_sib_entregables.py
# Convierte los snapshots crudos de fetch_sib_cartera_desglose.py en los dos
# JSON entregables del Observatorio:
#
#   1) Cartera de creditos desglosada por producto
#      total, hipotecario, consumo_total, consumo_sin_tarjeta,
#      tarjetas_credito, comercial_total, pymes, mayores_deudores_interinos
#
#   2) Captacion desglosada por instrumento
#      total, ahorros, corrientes, depositos
#
# Ambos con TODAS las entidades (para ranking top-10 y filtrado en el front),
# separando la fila agregada "TODOS" que publica la SIB para que no compita
# en los rankings.
#
# Uso:
#   python scripts/build_sib_entregables.py \
#       --cartera data/_desg_cartera_cortes.json data/_desg_cartera_bm.json ... \
#       --captacion data/_desg_capta_all.json \
#       --out-cartera data/cartera_sistema.json \
#       --out-captacion data/captacion_sistema.json
#
# Los snapshots de entrada se pueden pasar en cualquier cantidad; se fusionan
# (util cuando la descarga se partio por tipo de entidad).
# ==============================================================================

import argparse
import json
import os
from datetime import datetime, timezone

AGREGADO = "TODOS"   # fila de sistema que publica la SIB

# ---------------------------------------------------------------- cartera ----
PRODUCTOS_CARTERA = [
    "total",
    "hipotecario",
    "consumo_total",
    "consumo_sin_tarjeta",
    "tarjetas_credito",
    "comercial_total",
    "pymes",
    "mayores_deudores_interinos",
]

DEFINICIONES_CARTERA = {
    "total": "Toda la cartera de creditos (suma de todos los tipos)",
    "hipotecario": "tipoCartera = Creditos Hipotecarios",
    "consumo_total": "tipoCartera = Creditos de consumo (incluye tarjetas personales)",
    "consumo_sin_tarjeta": "tipoCredito = Creditos de Consumo (excluye tarjetas)",
    "tarjetas_credito": "tipoCredito = Tarjetas de Creditos Personales + Comerciales",
    "comercial_total": "tipoCartera = Creditos comerciales",
    "pymes": "tipoCredito = Comerciales a Medianos + Menores Deudores + Microcredito",
    "mayores_deudores_interinos": "tipoCredito = Creditos Comerciales a Mayores Deudores",
}


def _n(s):
    return (s or "").strip().lower()


def clasifica_cartera(fila):
    """Devuelve el conjunto de productos a los que aporta la fila."""
    tc = _n(fila.get("tipoCartera"))
    tcred = _n(fila.get("tipoCredito"))
    dest = {"total"}

    if "hipotecari" in tc:
        dest.add("hipotecario")
    if "consumo" in tc:
        dest.add("consumo_total")
    if "comercial" in tc:
        dest.add("comercial_total")

    es_tarjeta = "tarjeta" in tcred
    if es_tarjeta:
        dest.add("tarjetas_credito")
    elif "consumo" in tcred:
        dest.add("consumo_sin_tarjeta")

    if "mayores deudores" in tcred:
        dest.add("mayores_deudores_interinos")
    if ("medianos deudores" in tcred or "menores deudores" in tcred
            or "microcredito" in tcred or "microcrédito" in tcred):
        dest.add("pymes")
    return dest


# -------------------------------------------------------------- captacion ----
PRODUCTOS_CAPTACION = ["total", "ahorros", "corrientes", "depositos"]

DEFINICIONES_CAPTACION = {
    "total": "Captacion total del publico (todos los instrumentos)",
    "ahorros": "Cuentas de ahorro, basicas de ahorro, ahorro programado y basicas de nomina",
    "corrientes": "Cuentas corrientes remuneradas y no remuneradas",
    "depositos": "Depositos a plazo, certificados (financieros/inversion/participacion), bonos y cedulas",
}


def clasifica_captacion(fila):
    instr = _n(fila.get("instrumentoCaptacion"))
    dest = {"total"}
    if "corriente" in instr:
        dest.add("corrientes")
    elif "ahorro" in instr or "nomina" in instr or "nómina" in instr:
        dest.add("ahorros")
    else:
        # plazo, certificados, bonos, cedulas
        dest.add("depositos")
    return dest


# ------------------------------------------------------------------ core -----
def cargar(paths, key):
    """Fusiona la lista `key` de varios snapshots."""
    filas = []
    for p in paths or []:
        if not os.path.exists(p):
            print(f"  !! no existe, se omite: {p}")
            continue
        with open(p, encoding="utf-8") as f:
            d = json.load(f)
        n = len(d.get(key, []))
        filas.extend(d.get(key, []))
        print(f"  + {p}: {n} filas ({key})")
    return filas


def construir(filas, productos, clasificador, campo_valor):
    """Arma {entidad: {producto: [{periodo, valor}]}} + metadatos de tipo."""
    periodos = sorted({f["periodo"] for f in filas})
    acc = {}       # entidad -> producto -> periodo -> suma
    tipo_de = {}   # entidad -> tipo_entidad

    for f in filas:
        ent = f["entidad"]
        tipo_de.setdefault(ent, f.get("tipo_entidad"))
        valor = f.get(campo_valor)
        if valor is None:
            continue
        destinos = clasificador(f)
        pe = acc.setdefault(ent, {p: {} for p in productos})
        for prod in destinos:
            if prod in pe:
                pe[prod][f["periodo"]] = pe[prod].get(f["periodo"], 0.0) + valor

    entidades = {}
    for ent, prods in acc.items():
        entidades[ent] = {
            "tipo_entidad": tipo_de.get(ent),
            "series": {
                prod: [{"periodo": p, "valor": round(prods[prod].get(p, 0.0), 2)}
                       for p in periodos]
                for prod in productos
            },
        }
    return periodos, entidades


def ranking(entidades, periodos, producto="total"):
    """Ranking descendente por producto y periodo, excluyendo el agregado."""
    out = {}
    for i, p in enumerate(periodos):
        fila = []
        for ent, d in entidades.items():
            if ent == AGREGADO:
                continue
            fila.append({"entidad": ent, "tipo_entidad": d["tipo_entidad"],
                         "valor": d["series"][producto][i]["valor"]})
        fila.sort(key=lambda x: -x["valor"])
        out[p] = [dict(x, posicion=j + 1) for j, x in enumerate(fila)]
    return out


def escribir(path, payload):
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    print(f"  -> {path} ({os.path.getsize(path):,} bytes)")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--cartera", nargs="*", default=[])
    ap.add_argument("--captacion", nargs="*", default=[])
    ap.add_argument("--out-cartera", default="data/cartera_sistema.json")
    ap.add_argument("--out-captacion", default="data/captacion_sistema.json")
    args = ap.parse_args()

    base = {
        "fuente": "Superintendencia de Bancos de la Republica Dominicana (SIB) - API v2",
        "unidad": "RD$ (pesos dominicanos)",
        "generado_en": datetime.now(timezone.utc).isoformat(),
        "nota_agregado": (f"La entidad '{AGREGADO}' es el agregado que publica la "
                          "SIB; se excluye del ranking."),
    }

    if args.cartera:
        print("Cartera:")
        filas = cargar(args.cartera, "cartera_agg")
        periodos, entidades = construir(filas, PRODUCTOS_CARTERA,
                                        clasifica_cartera, "deuda")
        escribir(args.out_cartera, dict(
            base,
            endpoint="estadisticas/v2/carteras/creditos (granular, agregado por producto)",
            cortes=periodos,
            productos=DEFINICIONES_CARTERA,
            n_entidades=len([e for e in entidades if e != AGREGADO]),
            ranking_total=ranking(entidades, periodos, "total"),
            entidades=entidades,
        ))

    if args.captacion:
        print("Captacion:")
        filas = cargar(args.captacion, "captacion_agg")
        periodos, entidades = construir(filas, PRODUCTOS_CAPTACION,
                                        clasifica_captacion, "balance")
        escribir(args.out_captacion, dict(
            base,
            endpoint="estadisticas/v2/captaciones/moneda",
            cortes=periodos,
            productos=DEFINICIONES_CAPTACION,
            n_entidades=len([e for e in entidades if e != AGREGADO]),
            ranking_total=ranking(entidades, periodos, "total"),
            entidades=entidades,
        ))


if __name__ == "__main__":
    main()
