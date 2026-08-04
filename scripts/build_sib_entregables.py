#!/usr/bin/env python3
# ==============================================================================
# build_sib_entregables.py
# Convierte los snapshots crudos de fetch_sib_cartera_desglose.py en los JSON
# entregables del Observatorio:
#
#   1) Cartera de creditos: por producto y por producto/region, con balance y
#      market share, para TODAS las entidades (top-10 y filtrado en el front).
#      Productos: total, hipotecario, consumo_total, consumo_sin_tarjeta,
#      tarjetas_credito, comercial_total, pymes, mayores_deudores_interinos.
#
#   2) Captacion: total, ahorros, corrientes, depositos (el endpoint de
#      captacion no expone region; ver nota en el JSON de salida).
#
# El market share se calcula contra el sistema = suma de entidades reales del
# mismo corte (se excluye la fila agregada "TODOS" que publica la SIB para no
# contarla dos veces), y se calcula tanto a nivel nacional como por region.
#
# Uso:
#   python scripts/build_sib_entregables.py \
#       --cartera data/_reg_cartera_bm.json data/_reg_cartera_aayp.json ... \
#       --captacion data/_desg_capta_all.json \
#       --out-cartera data/cartera_sistema.json \
#       --out-captacion data/captacion_sistema.json \
#       [--moneda nacional|extranjera|todas]
#
# Los snapshots de entrada se fusionan (la descarga se parte por tipo de
# entidad para que ninguna corrida se alargue de mas).
# ==============================================================================

import argparse
import json
import os
from datetime import datetime, timezone

AGREGADO = "TODOS"   # fila de sistema que publica la SIB
SIN_REGION = "N/D"

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
    """Productos a los que aporta la fila (una fila puede alimentar varios)."""
    tc = _n(fila.get("tipoCartera"))
    tcred = _n(fila.get("tipoCredito"))
    dest = {"total"}

    if "hipotecari" in tc:
        dest.add("hipotecario")
    if "consumo" in tc:
        dest.add("consumo_total")
    if "comercial" in tc:
        dest.add("comercial_total")

    if "tarjeta" in tcred:
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
        dest.add("depositos")
    return dest


# ------------------------------------------------------------------ core -----
def cargar(paths, key):
    filas = []
    for p in paths or []:
        if not os.path.exists(p):
            print(f"  !! no existe, se omite: {p}")
            continue
        with open(p, encoding="utf-8") as f:
            d = json.load(f)
        n = len(d.get(key, []))
        filas.extend(d.get(key, []))
        print(f"  + {p}: {n:,} filas ({key})")
    return filas


def filtrar_moneda(filas, modo):
    """modo: todas | nacional | extranjera. Si el snapshot no trae moneda se
    deja pasar todo (compatibilidad con snapshots viejos)."""
    if modo == "todas":
        return filas
    quiere_nac = modo == "nacional"
    out = []
    for f in filas:
        mon = _n(f.get("moneda"))
        if not mon or mon == "n/d":
            out.append(f)
            continue
        es_nac = "nacional" in mon
        if es_nac == quiere_nac:
            out.append(f)
    print(f"  filtro moneda={modo}: {len(out):,} de {len(filas):,} filas")
    return out


def agregar(filas, productos, clasificador, campo_valor, con_region):
    """Devuelve (periodos, regiones, datos) donde
    datos[entidad][region][producto][periodo] = [total, mn, me].
    La region 'TOTAL' agrega todas las regiones (vista nacional); mn/me
    permiten filtrar por moneda sin re-descargar ni pedir otro archivo."""
    periodos = sorted({f["periodo"] for f in filas})
    regiones = sorted({f.get("region") or SIN_REGION for f in filas}) if con_region else []
    datos = {}
    tipo_de = {}

    for f in filas:
        ent = f["entidad"]
        tipo_de.setdefault(ent, f.get("tipo_entidad"))
        val = f.get(campo_valor)
        if val is None:
            continue
        destinos = clasificador(f)
        reg = (f.get("region") or SIN_REGION) if con_region else None
        mon = _n(f.get("moneda"))
        # indice 1 = moneda nacional, 2 = extranjera; sin moneda solo suma total
        idx = 1 if "nacional" in mon else (2 if "extranjera" in mon else None)

        e = datos.setdefault(ent, {})
        for zona in (["TOTAL", reg] if con_region else ["TOTAL"]):
            z = e.setdefault(zona, {})
            for prod in destinos:
                if prod not in productos:
                    continue
                serie = z.setdefault(prod, {})
                acc = serie.setdefault(f["periodo"], [0.0, 0.0, 0.0])
                acc[0] += val
                if idx:
                    acc[idx] += val
    return periodos, regiones, datos, tipo_de


def sistema_totales(datos, productos, periodos, zonas):
    """Suma de entidades reales (excluye el agregado TODOS)."""
    tot = {z: {p: {per: 0.0 for per in periodos} for p in productos} for z in zonas}
    for ent, zonas_e in datos.items():
        if ent == AGREGADO:
            continue
        for z, prods in zonas_e.items():
            if z not in tot:
                continue
            for p, serie_p in prods.items():
                for per, acc in serie_p.items():
                    tot[z][p][per] += acc[0]
    return tot


def serie(prods, producto, periodos, sistema=None, con_moneda=False):
    """Lista [{periodo, valor, share_pct}] para un producto.
    Con con_moneda agrega valor_mn / valor_me para poder filtrar por moneda
    sin pedir otro archivo."""
    d = prods.get(producto, {})
    out = []
    for per in periodos:
        acc = d.get(per) or [0.0, 0.0, 0.0]
        v = round(acc[0], 2)
        item = {"periodo": per, "valor": v}
        if con_moneda:
            item["valor_mn"] = round(acc[1], 2)
            item["valor_me"] = round(acc[2], 2)
        if sistema is not None:
            base = sistema.get(producto, {}).get(per, 0.0)
            item["share_pct"] = round(v / base * 100, 4) if base else None
        out.append(item)
    return out


def ranking(entidades, periodos, producto="total", zona="TOTAL"):
    out = {}
    for i, per in enumerate(periodos):
        fila = []
        for ent, d in entidades.items():
            if ent == AGREGADO:
                continue
            bloque = d["series"] if zona == "TOTAL" else d.get("regiones", {}).get(zona)
            if not bloque:
                continue
            s = bloque[producto][i]
            fila.append({"entidad": ent, "tipo_entidad": d["tipo_entidad"],
                         "valor": s["valor"], "share_pct": s.get("share_pct")})
        fila.sort(key=lambda x: -x["valor"])
        out[per] = [dict(x, posicion=j + 1) for j, x in enumerate(fila)]
    return out


def escribir(path, payload):
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    print(f"  -> {path} ({os.path.getsize(path):,} bytes)")


def construir_bloque(filas, productos, clasificador, campo_valor, con_region,
                     con_moneda=False):
    periodos, regiones, datos, tipo_de = agregar(
        filas, productos, clasificador, campo_valor, con_region)
    zonas = ["TOTAL"] + [r for r in regiones]
    sist = sistema_totales(datos, productos, periodos, zonas)

    entidades = {}
    for ent, zonas_e in datos.items():
        bloque = {
            "tipo_entidad": tipo_de.get(ent),
            "series": {p: serie(zonas_e.get("TOTAL", {}), p, periodos,
                                sist["TOTAL"], con_moneda) for p in productos},
        }
        if con_region:
            bloque["regiones"] = {
                r: {p: serie(zonas_e.get(r, {}), p, periodos, sist[r], con_moneda)
                    for p in productos}
                for r in regiones
            }
        entidades[ent] = bloque

    sistema_out = {
        "nacional": {p: [{"periodo": per, "valor": round(sist["TOTAL"][p][per], 2)}
                         for per in periodos] for p in productos},
    }
    if con_region:
        sistema_out["por_region"] = {
            r: {p: [{"periodo": per, "valor": round(sist[r][p][per], 2)}
                    for per in periodos] for p in productos}
            for r in regiones
        }
    return periodos, regiones, entidades, sistema_out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--cartera", nargs="*", default=[])
    ap.add_argument("--captacion", nargs="*", default=[])
    ap.add_argument("--out-cartera", default="data/cartera_sistema.json")
    ap.add_argument("--out-captacion", default="data/captacion_sistema.json")
    ap.add_argument("--moneda", choices=["todas", "nacional", "extranjera"],
                    default="todas",
                    help="Filtra la cartera por moneda; el front del "
                         "Observatorio grafica solo moneda nacional.")
    args = ap.parse_args()

    base = {
        "fuente": "Superintendencia de Bancos de la Republica Dominicana (SIB) - API v2",
        "unidad": "RD$ (pesos dominicanos)",
        "generado_en": datetime.now(timezone.utc).isoformat(),
        "nota_agregado": (f"La entidad '{AGREGADO}' es el agregado que publica "
                          "la SIB; se excluye del ranking y del sistema."),
        "nota_share": ("share_pct = balance de la entidad / balance del sistema "
                       "(suma de entidades) para el mismo producto, periodo y "
                       "region."),
        "nota_moneda": ("En la vista de todas las monedas cada punto trae "
                        "valor_mn y valor_me (nacional / extranjera) para "
                        "filtrar sin pedir otro archivo; valor = valor_mn + "
                        "valor_me."),
    }

    if args.cartera:
        print("Cartera:")
        filas = filtrar_moneda(cargar(args.cartera, "cartera_agg"), args.moneda)
        con_region = any("region" in f for f in filas[:50])
        # El desglose por moneda solo tiene sentido en la vista completa; en
        # las variantes ya filtradas el total ES la moneda pedida.
        con_moneda = (args.moneda == "todas") and any("moneda" in f for f in filas[:50])
        periodos, regiones, entidades, sistema = construir_bloque(
            filas, PRODUCTOS_CARTERA, clasifica_cartera, "deuda", con_region,
            con_moneda)
        payload = dict(
            base,
            endpoint="estadisticas/v2/carteras/creditos (granular)",
            moneda=args.moneda,
            cortes=periodos,
            regiones=regiones,
            productos=DEFINICIONES_CARTERA,
            n_entidades=len([e for e in entidades if e != AGREGADO]),
            sistema=sistema,
            ranking_total=ranking(entidades, periodos, "total"),
            entidades=entidades,
        )
        if con_region:
            payload["ranking_total_por_region"] = {
                r: ranking(entidades, periodos, "total", zona=r) for r in regiones
            }
        escribir(args.out_cartera, payload)

    if args.captacion:
        print("Captacion:")
        filas = cargar(args.captacion, "captacion_agg")
        periodos, _, entidades, sistema = construir_bloque(
            filas, PRODUCTOS_CAPTACION, clasifica_captacion, "balance", False)
        escribir(args.out_captacion, dict(
            base,
            endpoint="estadisticas/v2/captaciones/moneda",
            nota_region=("El endpoint de captacion no expone la dimension "
                         "regional; solo la cartera trae region."),
            cortes=periodos,
            productos=DEFINICIONES_CAPTACION,
            n_entidades=len([e for e in entidades if e != AGREGADO]),
            sistema=sistema,
            ranking_total=ranking(entidades, periodos, "total"),
            entidades=entidades,
        ))


if __name__ == "__main__":
    main()
