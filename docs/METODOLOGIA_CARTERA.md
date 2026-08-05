# Metodología · Cartera de créditos del Observatorio

Definiciones que el Observatorio usa para publicar cartera. Los rankings de la
presentación ("SECTOR FINANCIERO MONEDA NACIONAL – TOP 10") se calculan con
estas reglas; el pipeline debe entregarlas siempre en este formato.

Fuente: API SIB v2, endpoint granular `estadisticas/v2/carteras/creditos`.

---

## 1. Moneda

**Todo se publica en moneda nacional.** Se filtra `moneda = "Moneda Nacional"`
y se descarta la extranjera.

No es un detalle menor: en el sistema completo el 22% de la cartera está en
divisas, concentrado en comercial (34.7%) y sobre todo en mayores deudores
(46.1%). En las asociaciones el efecto es chico (0.1%–1.0%) y vive casi todo en
tarjetas de crédito.

## 2. Universo de entidades

Todo el sector financiero, sin excluir tipos. Al día de hoy:

| Tipo | Descripción |
|---|---|
| `BM` | Bancos múltiples |
| `AAyP` | Asociaciones de ahorros y préstamos |
| `BAyC` | Bancos de ahorro y crédito |
| *(pendiente)* | Banca pública de desarrollo — **BANDEX** |

> **Importante**: descargar solo BM+AAyP+BAyC deja fuera a BANDEX, que sí
> aparece en los rankings de la presentación (8º en mayores deudores, 10º en
> comercial total al corte 2026-05). Omitirlo corre un puesto hacia arriba a
> todas las entidades por debajo — La Nacional aparecía 12ª en vez de 13ª.

La fila `TODOS` que publica la SIB es el agregado del sistema: **se excluye de
los rankings** para no contarla dos veces, y se usa solo como control (la suma
de entidades debe reproducirla exactamente).

## 3. Productos

Se clasifica por `tipoCartera` (nivel grueso) y `tipoCredito` (detalle).

| Producto | Regla |
|---|---|
| `total` | Toda la cartera |
| `hipotecario` | `tipoCartera` = Créditos Hipotecarios |
| `consumo_total` | `tipoCartera` = Créditos de consumo |
| `consumo_sin_tarjeta` | `tipoCredito` = Créditos de Consumo |
| `tarjetas_credito` | `tipoCredito` = Tarjetas de Créditos **Personales** |
| `comercial_total` | `tipoCartera` = Créditos comerciales |
| `pymes` | `tipoCredito` = Comerciales a **Menores Deudores** + **Microcrédito** |
| `mayores_deudores` | `tipoCredito` = Comerciales a Mayores Deudores |

### Precisiones que cambian resultados

**Tarjetas de crédito = solo personales.** Las tarjetas comerciales *no* entran
aquí: quedan dentro de `comercial_total`, que es donde vive su `tipoCartera`.
Incluirlas inflaba el producto y rompía la identidad de consumo.

**Pymes = menores deudores + microcrédito.** Los **medianos deudores quedan
fuera**. El microcrédito es imprescindible: es lo que hace aparecer a ADOPEM,
ADEMI y BANFONDESA en el top 10, cuyo negocio comercial es casi todo
microcrédito.

**Mayores deudores** es lo que la presentación llama "interinos".

### Identidades que deben cumplirse

```
hipotecario + consumo_total + comercial_total = total
consumo_sin_tarjeta + tarjetas_credito        = consumo_total
pymes + mayores_deudores                      ≤ comercial_total
```

La segunda solo cierra con tarjetas = personales. La tercera es desigualdad
porque comercial incluye además medianos deudores y tarjetas comerciales.

## 4. Cortes

Cierres de diciembre de cada año más el último mes publicado. En las series
mensuales, **todos los puntos van en moneda nacional**: mezclar bases produce
lecturas falsas — con abril en todas las monedas y mayo en nacional, APAP
aparecía cayendo −0.03% cuando en realidad creció +1.00%.

La SIB publica con rezago; los meses aún no disponibles devuelven cero filas y
se descartan solos.

## 5. Market share

```
share = balance de la entidad / balance del sistema
```

para el mismo producto, periodo y región. El denominador es la suma de
entidades reales (sin `TODOS`), y coincide exactamente con el agregado que
publica la SIB.

## 6. Región

Solo **cartera** tiene desagregación regional: Metropolitana, Norte, Este, Sur,
más una bolsa `N/D` de créditos sin geolocalizar (marginal, ~0.02%). Las cuatro
regiones **no suman el total** por esa bolsa; conviene mostrarla o advertirlo.

**Captación no tiene región en el API.** Los endpoints `captaciones/region`,
`/provincia`, `/geografia`, `/oficina`, `/sucursal`, `/genero` y
`/tipo-persona` responden 404. Solo existen `captaciones/moneda` (instrumento y
divisa) y `captaciones/sector-depositante`. Cualquier vista regional de
captación tiene que venir de SIMBAD, no de esta API.

## 7. Dónde vive esto en el código

| Archivo | Rol |
|---|---|
| `scripts/fetch_sib_cartera_desglose.py` | Descarga y agrega el granular |
| `scripts/build_sib_entregables.py` | Aplica estas definiciones y calcula share |
| `scripts/recalcular_payload_editor.py` | Reescribe el editor de indicadores |
| `.github/workflows/sib_cartera_pipeline.yml` | Corre todo y publica los JSON |
