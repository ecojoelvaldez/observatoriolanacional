#!/usr/bin/env python3
# ==============================================================================
# recalcular_payload_editor.py
# Recalcula el PAYLOAD del "Editor de indicadores" a partir del snapshot crudo
# de la SIB, en MONEDA NACIONAL y con las definiciones de producto que usa el
# Observatorio:
#
#   pymes             = Creditos Comerciales a Menores Deudores  (solo esos)
#   tarjetas_credito  = Tarjetas de Creditos PERSONALES          (las
#                       comerciales quedan dentro de comercial_total)
#   consumo_total     = tipoCartera "Creditos de consumo"
#   consumo_sin_tarjeta = tipoCredito "Creditos de Consumo"
#   comercial_total   = tipoCartera "Creditos comerciales" (incluye tarjetas
#                       comerciales)
#   hipotecario       = tipoCartera "Creditos Hipotecarios"
#   mayores_deudores  = Creditos Comerciales a Mayores Deudores
#   total             = suma de todo
#
# Con estas definiciones cierra la identidad
#   consumo_sin_tarjeta + tarjetas_credito = consumo_total
#
# Reescribe solo los arrays `data` y los textos `insights` de los indicadores
# de cartera; deja intactos estilos, offsets, textos editados y el resto del
# archivo.
#
# Uso:
#   python scripts/recalcular_payload_editor.py \
#       --html editor.html --snapshot data/sib_desglose_snapshot.json \
#       [--snapshot-extra data/_mensual_aayp_2026.json] \
#       --out editor_mn.html [--dump-payload payload_mn.json]
# ==============================================================================

import argparse
import json
import os
import re
import unicodedata

# indicador del editor -> clave interna de producto
INDICADOR_A_PRODUCTO = {
    "cartera-total": "total",
    "hipotecario": "hipotecario",
    "consumo-total": "consumo_total",
    "consumo-sin-tarjeta": "consumo_sin_tarjeta",
    "tarjeta-credito": "tarjetas_credito",
    "comercial-total": "comercial_total",
    "pymes": "pymes",
    "mayores-deudores": "mayores_deudores",
}

# etiqueta del editor -> corte YYYY-MM
MESES = {"ENE": 1, "FEB": 2, "MAR": 3, "ABR": 4, "MAY": 5, "JUN": 6,
         "JUL": 7, "AGO": 8, "SEP": 9, "OCT": 10, "NOV": 11, "DIC": 12}


def periodo_a_corte(p):
    """'2024' -> '2024-12' (cierre anual); 'MAY-26' -> '2026-05'."""
    p = str(p).strip().upper()
    if re.fullmatch(r"\d{4}", p):
        return f"{p}-12"
    m = re.fullmatch(r"([A-ZÁÉÍÓÚ]{3})-(\d{2})", p)
    if m and m.group(1) in MESES:
        return f"20{m.group(2)}-{MESES[m.group(1)]:02d}"
    return None


def _n(s):
    s = unicodedata.normalize("NFD", str(s or ""))
    s = "".join(c for c in s if unicodedata.category(c) != "Mn")
    return s.strip().lower()


def productos_de(fila):
    """Productos a los que aporta la fila, con las definiciones del Observatorio."""
    tc = _n(fila.get("tipoCartera"))
    tcred = _n(fila.get("tipoCredito"))
    out = {"total"}
    if "hipotecari" in tc:
        out.add("hipotecario")
    if "consumo" in tc:
        out.add("consumo_total")
    if "comercial" in tc:
        out.add("comercial_total")
    # Tarjetas: solo las personales cuentan como "tarjeta de credito"; las
    # comerciales ya quedaron sumadas en comercial_total.
    if "tarjeta" in tcred and "personal" in tcred:
        out.add("tarjetas_credito")
    elif "consumo" in tcred and "tarjeta" not in tcred:
        out.add("consumo_sin_tarjeta")
    if "mayores deudores" in tcred:
        out.add("mayores_deudores")
    if "menores deudores" in tcred:
        out.add("pymes")
    return out


def agregar_mn(paths):
    """{(entidad, corte, producto): millones} en moneda nacional."""
    agg = {}
    vistos = set()
    for p in paths:
        if not p or not os.path.exists(p):
            continue
        with open(p, encoding="utf-8") as f:
            filas = json.load(f).get("cartera_agg", [])
        n = 0
        for r in filas:
            if "nacional" not in _n(r.get("moneda")):
                continue
            clave_fila = (r["entidad"], r["periodo"], r.get("tipoCartera"),
                          r.get("tipoCredito"), r.get("region"))
            if clave_fila in vistos:      # evita doble conteo al fusionar
                continue
            vistos.add(clave_fila)
            n += 1
            for prod in productos_de(r):
                k = (r["entidad"], r["periodo"], prod)
                agg[k] = agg.get(k, 0.0) + r["deuda"] / 1e6
        print(f"  + {p}: {n:,} filas MN")
    return agg


def fmt_mm(v, dec=1):
    return f"{v:,.{dec}f}"


def insight(ent, serie, periodos, corte_label):
    """Reproduce el texto del editor: variacion contra el cierre anterior."""
    i_ult = len(serie) - 1
    # ultimo cierre de diciembre disponible antes del corte
    i_base = max((i for i, p in enumerate(periodos)
                  if re.fullmatch(r"\d{4}", str(p)) and i < i_ult), default=None)
    if i_base is None or serie[i_ult] is None or serie[i_base] is None:
        return None
    ult, base = serie[i_ult], serie[i_base]
    delta = ult - base
    pct = (delta / base * 100) if base else 0.0
    verbo = "Crece" if delta >= 0 else "Cae"
    return (f"{verbo} RD${fmt_mm(abs(delta))} MM vs. {corte_label};\n"
            f"cierra {MES_LARGO} en RD${fmt_mm(ult)} MM,\n"
            f"una variación de {pct:+.1f}%.")


MES_LARGO = "mayo"   # se ajusta en main() segun el ultimo corte


def main():
    global MES_LARGO
    ap = argparse.ArgumentParser()
    ap.add_argument("--html", required=True)
    ap.add_argument("--snapshot", required=True)
    ap.add_argument("--snapshot-extra", nargs="*", default=[])
    ap.add_argument("--out", required=True)
    ap.add_argument("--dump-payload")
    args = ap.parse_args()

    print("Agregando snapshot(s) en moneda nacional:")
    agg = agregar_mn([args.snapshot] + list(args.snapshot_extra))

    html = open(args.html, encoding="utf-8").read()
    i = html.index("const PAYLOAD={")
    ini = html.index("{", i)
    d = 0
    j = ini
    en_str = False
    q = ""
    esc = False
    while j < len(html):
        c = html[j]
        if en_str:
            if esc:
                esc = False
            elif c == "\\":
                esc = True
            elif c == q:
                en_str = False
        elif c in "\"'":
            en_str, q = True, c
        elif c == "{":
            d += 1
        elif c == "}":
            d -= 1
            if d == 0:
                j += 1
                break
        j += 1
    crudo = html[ini:j]
    payload = json.loads(crudo)

    cambios = 0
    faltantes = []
    for ind in payload.get("indicators", []):
        prod = INDICADOR_A_PRODUCTO.get(ind.get("slug"))
        if not prod:
            continue
        periodos = ind.get("periods") or []
        cortes = [periodo_a_corte(p) for p in periodos]
        ult = [c for c in cortes if c]
        if ult:
            MES_LARGO = {1: "enero", 2: "febrero", 3: "marzo", 4: "abril",
                         5: "mayo", 6: "junio", 7: "julio", 8: "agosto",
                         9: "septiembre", 10: "octubre", 11: "noviembre",
                         12: "diciembre"}[int(ult[-1][5:7])]
        data = ind.get("data") or {}
        for ent in list(data.keys()):
            nueva = []
            for k, corte in enumerate(cortes):
                if corte is None:
                    nueva.append(data[ent][k] if k < len(data[ent]) else None)
                    continue
                v = agg.get((ent, corte, prod))
                if v is None:
                    faltantes.append((ind["slug"], ent, periodos[k]))
                    nueva.append(data[ent][k] if k < len(data[ent]) else None)
                else:
                    if k >= len(data[ent]) or abs(v - (data[ent][k] or 0)) > 1e-6:
                        cambios += 1
                    nueva.append(round(v, 6))
            data[ent] = nueva
        ind["data"] = data
        # recalcular insights con la misma redaccion
        if ind.get("insights"):
            base_lbl = next((str(p) for p in reversed(periodos)
                             if re.fullmatch(r"\d{4}", str(p))), None)
            etiqueta = f"dic-{base_lbl[-2:]}" if base_lbl else "el cierre"
            for ent in list(ind["insights"].keys()):
                if ent in data:
                    txt = insight(ent, data[ent], periodos, etiqueta)
                    if txt:
                        ind["insights"][ent] = txt

    nuevo = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
    open(args.out, "w", encoding="utf-8").write(html[:ini] + nuevo + html[j:])
    if args.dump_payload:
        with open(args.dump_payload, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)

    print(f"\nvalores recalculados: {cambios:,}")
    if faltantes:
        print(f"sin dato en el snapshot: {len(faltantes)} "
              f"(se conservo el valor original)")
        for x in faltantes[:10]:
            print("   ", x)
    print(f"salida: {args.out}")


if __name__ == "__main__":
    main()
