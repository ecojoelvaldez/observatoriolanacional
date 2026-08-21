# Estudio de gráficas · editar el observatorio desde el observatorio

Este documento es para el analista, no para un desarrollador. Explica cómo
cambiar los números y la forma de las gráficas de **Cartera** —y de
cualquier otra gráfica del sitio— sin tocar código, sin abrir el
repositorio y sin esperar un despliegue.

Lo que se guarda aquí lo ve **todo el que entre al sitio**, no solo quien
lo editó.

---

## 1. Entrar

1. Abre el observatorio y ve a **Acceso analista** (abajo, en la barra de
   navegación).
2. Inicia sesión con el usuario de Estrategia e Innovación.
3. A partir de ese momento aparecen dos cosas que antes no estaban:
   - un botón **✎ Editar** en la esquina de cada gráfica;
   - un botón **✎ Editar números** junto a las pestañas Créditos /
     Captación.

Si cierras sesión o entras desde otro navegador sin iniciar sesión, esos
botones desaparecen: el visitante normal nunca los ve, pero **sí ve los
cambios publicados**.

---

## 2. Cambiar los números de una gráfica

Pulsa **✎ Editar** sobre la gráfica. Se abre el estudio en la pestaña
**Datos**, que funciona como una hoja de cálculo:

| Qué quieres hacer | Cómo |
|---|---|
| Corregir un dato | Haz clic en la celda y escribe encima |
| Renombrar una serie | Escribe sobre el nombre en el encabezado de la columna |
| Cambiar el color de una serie | Cuadrito de color junto al nombre de la columna |
| Agregar un año, una región, una entidad | **+ Fila** |
| Agregar otra serie de comparación | **+ Serie** |
| Quitar una fila o una serie | La **×** de esa fila o columna |
| Traer una tabla entera desde Excel | Cópiala en Excel y pega (Ctrl+V) sobre cualquier celda |
| Trabajar el archivo en Excel | **Exportar Excel** → editas → **Importar Excel** |
| Deshacer solo los números | **Recargar números del pipeline** (conserva el formato que elegiste) |

Sobre el pegado: se acepta lo que Excel pone en el portapapeles
(columnas separadas por tabulador). La hoja crece sola si pegas más filas
o más columnas de las que había. También entiende `1,234.56`, `1.234,56`
y valores con `RD$` o `%` adentro.

En dona y pastel el color se elige **por fila**, porque cada fila es una
porción.

---

## 3. Cambiar la forma de la gráfica

Pestaña **Tipo y forma**:

- **Tipo de gráfica**: barras agrupadas, barras apiladas, barras
  horizontales, líneas, dona, pastel o área polar.
- **Cada serie por separado** (como el gráfico combinado de Excel): cada
  serie puede dibujarse como barra, línea o área, con su color, punteada o
  no, y oculta o visible.
- **Eje vertical**: izquierdo o derecho. El eje derecho existe para cuando
  una serie es mucho más grande que otra —el sistema financiero completo
  frente a La Nacional, por ejemplo— y en un solo eje la pequeña quedaría
  aplastada contra el piso.

Pestaña **Formato**:

- Posición de la leyenda, o sin leyenda.
- Mostrar u ocultar el valor sobre cada dato.
- Cuadrícula de fondo, eje empezando en cero, puntos en las líneas.
- **Decimales, prefijo y sufijo**: con prefijo `RD$` y sufijo ` MM`, el
  número 39082 se lee `RD$39,082 MM`. Esto aplica a la etiqueta del dato,
  al tooltip y a la escala.
- Títulos de los ejes.

Pestaña **Textos**: título del bloque, subtítulo y nota de fuente. Si
dejas un campo vacío, ese texto lo sigue calculando el pipeline. Hay
también una **nota interna** para dejar dicho por qué se editó; esa nota
no se muestra en el sitio.

---

## 4. Publicar, previsualizar y deshacer

Al pie del estudio:

- **Vista previa** — aplica los cambios a la gráfica de atrás sin
  publicarlos. Si cierras el estudio sin guardar, se deshace.
- **Guardar y publicar** — lo guarda y desde ese momento lo ve todo el
  mundo.
- **Restablecer al original** — la gráfica vuelve al dato del pipeline
  SIB. Lo que habías editado **no se borra**: queda apagado, así que
  siempre se puede volver a encender.

Cuando una gráfica muestra números escritos a mano, debajo aparece el
sello **«Números editados por el analista»**. Es a propósito: quien lea el
observatorio tiene que poder distinguir el dato del pipeline del dato
corregido a mano.

---

## 5. Cada pestaña se edita por separado

Las gráficas de Cartera cambian de contenido según la pestaña activa
(Total, Hipotecario, Consumo general, Tarjetas de crédito, Comercial,
Pymes, Mayores deudores; y en Captación: Total, Ahorros, Corrientes,
Depósitos). La edición se guarda **por pestaña**: corregir el dato de
Hipotecario no toca el de Total.

Si lo que quieres es que *todas* las pestañas se vean igual —por ejemplo,
pasar el market share de línea a barras en todos los segmentos— usa
**Formato → Guardar y aplicar esta forma a todas las pestañas**. Copia el
tipo, los colores, la leyenda y el formato; **los números de cada pestaña
se quedan como estaban**, cada una con los suyos.

---

## 6. Editar los números de las tarjetas KPI y los textos

El botón **✎ Editar números**, junto a las pestañas Créditos / Captación,
enciende el modo de edición directa. Todo lo que se puede escribir queda
marcado con un borde punteado:

- las cuatro tarjetas KPI (valor, título y nota) de Créditos y de
  Captación;
- la frase de contexto de cada panel;
- la nota de corte («SIB · Abril 2026»);
- el encabezado del módulo.

Haz clic encima, escribe, y pulsa **Guardar y publicar** en la barra
inferior. **Cancelar** descarta lo escrito. **Restablecer todo** devuelve
todos los textos a lo que calcula el pipeline.

Estos valores también se guardan por pestaña, y se vuelven a escribir
automáticamente cada vez que el pipeline recalcula, así que la corrección
no se pierde al cambiar de pestaña y volver.

---

## 7. Editar las tarjetas de mercado (USD, EUR, WTI y TPM)

Las tarjetas de la parte de arriba de **Hoy** solo se movían cargando un Excel.
Cuando el dato del día llega por otra vía —una llamada, el cierre publicado
antes que el archivo— ahora se puede escribir a mano.

El dólar y el euro están partidos en dos: **Compra** arriba y **Venta** abajo,
dentro del mismo hueco de la franja. Cada mitad es independiente — su propio
número, su propia variación y su propia mini-gráfica de 7 días— así que hay
seis valores editables en total: compra y venta de USD, compra y venta de EUR,
WTI y TPM. Los dos lados salen de las columnas Compra/Venta que ya trae el PDF
TAC4009 del BCRD y el Excel de euro sondeo; antes solo se usaba la de venta.

Con sesión de analista, debajo de las tarjetas aparece **✎ Editar tarjetas**.
Pulsa, escribe el valor encima del número y pulsa **Guardar y publicar**
(o Enter; Esc cancela).

Lo importante es lo que pasa con la mini-gráfica de 7 días que hay detrás de
cada tarjeta. El número visible **es el último punto de esa serie**, así que al
guardar:

1. el valor que escribiste entra como **último dato**;
2. el que estaba pasa a ser el **penúltimo**;
3. la serie corre una posición y suelta el más viejo, para seguir siendo de 7;
4. la variación «vs ayer» se recalcula contra ese penúltimo;
5. la mini-gráfica se redibuja y el ticker de arriba toma el valor nuevo.

Ejemplo. WTI viene en `63.40 · 63.05 · 62.88 · 62.70 · 62.55 · 62.40 · 62.18`
y escribes **64.75**. La serie queda
`63.05 · 62.88 · 62.70 · 62.55 · 62.40 · 62.18 · 64.75`, y la tarjeta muestra
«▲ +2.57 (+4.13%) vs ayer», calculado contra 62.18. Es exactamente la misma
operación que hace el pipeline cuando llega un cierre nuevo; aquí la hace el
analista.

La TPM no tiene serie: ahí solo cambia el número. Editar la compra no toca la
venta, ni al revés.

La tarjeta editada no lleva sello a la vista: en portada el dato es el dato. El
rastro de quién lo puso queda en `chart_overrides`, y **Restablecer** devuelve
las cuatro al dato del pipeline sin borrar lo editado.

---

## 8. Dónde vive todo esto

- **Tabla:** `public.chart_overrides` en Supabase (migración
  `supabase/migrations/20260818000001_chart_overrides.sql`).
- **Una fila = una gráfica en una pestaña.** La llave `chart_key` es el id
  del lienzo más el contexto, por ejemplo
  `car-chart-evolucion@creditos:total`. Los textos usan el prefijo
  `texto:` y las tarjetas de mercado el prefijo `tarjeta:`
  (`tarjeta:wti@hoy:mercado`), con su serie de 7 días dentro del `config`.
- **Restablecer no borra:** pone `enabled = false` y conserva el `config`.
- **Copia local:** lo publicado se guarda además en `localStorage`
  (`ln-chart-overrides-cache-v1`) para que la página no espere a la red
  en la primera pintada. Si Supabase no responde, el sitio dibuja el dato
  del pipeline y la edición guardada en ese navegador sigue funcionando.

### Cómo está hecho, en dos párrafos

El sitio dibuja las mismas gráficas desde cinco controladores distintos
que se van pisando entre sí (herencia de parches sucesivos). En vez de
tocar los cinco, el estudio envuelve el constructor `Chart` en un `Proxy`
instalado en el `<head>`, antes de que nada dibuje. Así **toda** gráfica
pasa por un único punto: ahí se guarda la receta original —que es lo que
permite restablecer— y se aplica la edición vigente si existe. Da igual
qué controlador dibuje: la edición manda.

El mismo `Proxy` suelta el lienzo antes de construir. Chart.js se niega a
reutilizar un lienzo ocupado, y con varios controladores redibujando la
misma gráfica sin saber unos de otros esa colisión ya ocurría; ahora no
puede cortar un redibujado a la mitad.

### Nota sobre permisos

La escritura va con la clave anónima de Supabase, igual que la cola de
noticias (`news_items`). La protección real es la puerta de Entra ID del
`middleware.js`: al sitio solo entra gente del tenant de La Nacional. El
login del analista de la página es una segunda capa dentro del navegador,
no una identidad de Supabase.

Si en algún momento se quiere endurecer esto, el camino es hacer que el
login del analista emita la cookie firmada `oe_internal_session` que ya
sabe verificar `api/_lib/analyst-session.js`, mover la escritura a un
endpoint en `/api` y cerrar las políticas `chart_overrides_anon_*` a
`service_role`. Nada de la interfaz cambiaría.
