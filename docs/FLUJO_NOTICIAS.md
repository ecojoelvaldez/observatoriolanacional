# Flujo de noticias y resúmenes de IA

Cómo se arma la cola editorial que ve el analista, qué se filtra y por qué.
Para tocar parámetros no hace falta editar código: casi todo son variables de
entorno del workflow `.github/workflows/news_pipeline_v2.yml`.

---

## 1. Ventana editorial

El resumen de hoy cubre **desde las 2:00 PM del día hábil anterior hasta ahora**
(hora de Santo Domingo). Lo de la mañana de ayer ya salió en el resumen de ayer.

| Día que corre | La ventana empieza en | Cubre |
|---|---|---|
| Martes a viernes | ayer 2:00 PM | tarde de ayer + hoy |
| **Lunes** | **viernes 2:00 PM** | **tarde del viernes + sábado + domingo + hoy** |

El lunes no necesita configuración aparte: "día hábil anterior" ya es el viernes.
Ese era el día flojo — la ventana vieja era una edad máxima de 72 horas que no
distinguía el fin de semana.

Si el pipeline no corrió (feriado, workflow caído), la ventana se estira sola
hasta la corrida anterior, con tope de `NEWS_WINDOW_MAX_DAYS` (5 días).

Una nota **sin fecha no se descarta**: muchos medios dominicanos no la publican
en el markdown. Se marca `sin_fecha: true` y en el panel sale como "sin fecha".

- `NEWS_WINDOW_CUTOFF_HOUR` (14) — la hora en que empieza "la tarde de ayer".
- `MAX_HEADLINES_LUNES` (9 frente a 6) — el lunes se piden más titulares por
  fuente, porque la ventana cubre tres días de publicaciones.

## 2. Qué no entra

Tres filtros distintos, del más barato al más caro:

1. **Sección de la URL** (`SECCIONES_EXCLUIDAS`) — `/opinion`, `/editorial`,
   `/columna`, `/deportes`, `/variedades`, `/farandula`, `/sucesos`, `/energia`…
   Se aplica antes de leer el artículo: una columna no gasta ni una llamada.
2. **Tema** (`TEMAS_EXCLUIDOS`) — sector eléctrico (apagones, EDEs, generación,
   tarifa, Punta Catalina), variedades, sucesos. Manda **por encima** del piso
   institucional: "el Superintendente de Electricidad anuncia…" dispara la señal
   `regulador` por la palabra *superintendente* y aun así no es materia nuestra.
3. **Tipo de pieza** — Gemini clasifica cada nota como `noticia`, `analisis`,
   `opinion`, `publirreportaje` o `servicio`.
   - `opinion` y `publirreportaje` se descartan sin apelación.
   - `servicio` es la nota rutinaria que se repite igual cada día o cada semana
     (el precio del dólar de hoy, el aviso semanal de combustibles). **No se
     tira: va a la reserva**, y solo aparece si hace falta para llegar al piso.

Todo lo descartado queda en `news_candidates.json` → `discarded`, con su motivo,
y el panel lo muestra abajo con un botón **Recuperar**.

## 3. Piso de 6 candidatos

`NEWS_MIN_CANDIDATES` (6). Si tras las fuentes y el barrido web no se llega,
la escalera de rescate va de lo más barato a lo más caro:

1. **Reserva de la corrida** — notas ya leídas y puntuadas que quedaron fuera
   por la ventana, por el umbral de relevancia o por ser rutinarias. Coste cero:
   no vuelve a leer ni a llamar a Gemini. Se recuperan en ese orden de prioridad.
2. **Búsquedas extra** (`NEWS_SEARCH_QUERIES_EXTRA`) — siete consultas
   adicionales (Junta Monetaria, hipotecario, calificación crediticia, Fed…) con
   la ventana ensanchada 24 h.
3. **Quedarse corto, pero diciéndolo** — el JSON trae `garantia.cumplido: false`
   y el panel lo muestra en rojo.

**Nunca** se relajan los temas vetados ni la opinión para llenar cupo: completar
la cuota con lo que el analista pidió no ver es peor que quedarse corto.

Lo que se relajó queda en `garantia.pasos`, así que siempre se sabe si un día
llegó a 6 por mérito propio o a fuerza de rescate.

## 4. Brief del día

Una sola llamada a Gemini sobre los candidatos **ya elegidos** produce
`news_candidates.json` → `brief`: titular, resumen, 2-4 temas agrupados con su
"por qué importa" para una AAyP, qué vigilar y las 3 notas destacadas.

El panel lo pinta arriba de la cola. Cada tarjeta además trae:

- `por_que_importa` — el ángulo concreto para La Nacional (fondeo, costo del
  dinero, demanda de crédito hipotecario, cumplimiento, competencia).
- `ventana` — "hoy", "ayer tarde", "fin de semana"…

## 5. Salud de fuentes

Cada corrida anota en `data/news_source_health.json`, por fuente, en qué peldaño
falla: no se pudo leer el índice / se leyó pero no traía titulares del sector /
hubo titulares pero ninguno pasó el filtro.

Cuando una fuente acumula 8 corridas seguidas sin aportar una sola nota, queda
marcada `veredicto: "sustituir"` y aparece en el panel. **Sustituir una fuente
deja de ser una corazonada.**

Fuentes institucionales (BCRD, SIB, Hacienda): no se sustituyen por no dejarse
leer — lo que publica el regulador no lo publica nadie más. Tienen `url_alterna`
(segunda ruta que se prueba si el índice principal no abre) y además el barrido
web pregunta por ellas en cada corrida.

## 6. Resumen de IA (Macro / SIB)

`scripts/generate_gemini_summaries.py`, días 6 y 21 de cada mes.

Del lado de noticias ahora solo entran al contexto:
- piezas cuyo `tipo` no sea `opinion`, `publirreportaje` ni `servicio`;
- publicadas en los últimos `NOTICIAS_MAX_DIAS` (12) días;
- ordenadas por relevancia y, a igual relevancia, por recencia;
- encabezadas por el brief del día, para no reconstruir a mano un panorama que
  el pipeline de noticias ya escribió.

El resumen macro guarda `noticias_usadas` (fecha, titular, URL): verificar una
afirmación del resumen ya no obliga a adivinar de dónde salió.
