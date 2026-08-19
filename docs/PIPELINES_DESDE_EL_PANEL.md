# Lanzar los pipelines desde el panel del analista

El panel del analista tiene una tarjeta **Automatizaciones GitHub** con cuatro
botones que disparan, a mano, los workflows que normalmente corren por
calendario:

| Botón | Workflow | Horario | Qué hace |
|---|---|---|---|
| Resumen de noticias | `news_pipeline_v2.yml` | 9:00 AM y 12:30 PM, L-V | Lee las fuentes autorizadas y deja candidatas en `news_candidates.json`. |
| Resumen IA Gemini | `gemini_summaries.yml` | — | Regenera los resúmenes ejecutivos con el corte SIB más reciente. |
| Indicadores pipeline | `sib_pipeline.yml` | — | Descarga ROA, ROE, morosidad y cobertura del API de la SIB. |
| Cartera pipeline | `sib_cartera_pipeline.yml` | — | Reconstruye cartera y captación por producto, región y moneda. |

## La cola de noticias es de un día

`news_candidates.json` guarda **un solo día editorial**, el de Santo Domingo
(UTC-4). Cada corrida lo reconstruye: entra lo publicado hoy o anoche
(`NEWS_MAX_AGE_DAYS=1`) y sale todo lo demás, incluso lo que había dejado la
corrida anterior. Antes la cola guardaba una semana y por eso el 19 de agosto
seguían apareciendo propuestas del 14.

El panel aplica la misma regla del lado del navegador, porque las propuestas
también viven en `localStorage` y sobrevivían a cualquier arreglo del pipeline.
Sobre la cola hay una barra con el día editorial, cuántas propuestas quedan y
qué se purgó. Si dice que **la última corrida no es de hoy**, hay dos salidas:

1. **Resumen de noticias** en esta misma tarjeta, que lanza el workflow; o
2. **Leer fuentes** en la tarjeta de noticias, que lee las portadas desde el
   navegador y llena la cola sin esperar a GitHub. Es el respaldo cuando el
   pipeline falla o cuando hace falta la cola antes de las 9:00 AM.

El workflow falla a propósito si termina y el JSON no quedó fechado hoy o
quedó con candidatas fuera de la ventana: es preferible una corrida en rojo a
una cola vieja que parezca nueva.

## Cómo funciona

El navegador **no** habla con GitHub. Disparar un workflow exige un token con
permiso de escritura sobre Actions, y ese token no puede vivir en el HTML. El
botón llama a `POST /api/github-actions` (Vercel), que:

1. Verifica la cookie de sesión del analista (`oe_internal_session`).
2. Comprueba que el pipeline pedido esté en la lista blanca del endpoint.
   Un cliente no puede pedir un archivo de workflow arbitrario.
3. Llama al `workflow_dispatch` de GitHub con el token del servidor.
4. Busca la corrida recién creada y devuelve su enlace, para que el analista
   pueda seguirla sin abrir GitHub a mano.

`GET /api/github-actions` devuelve el estado de la última corrida de cada
workflow; es lo que pinta las etiquetas "OK · 07 ago 09:12" de cada tarjeta.

## Variables de entorno en Vercel

Obligatoria para que los botones funcionen:

| Variable | Valor |
|---|---|
| `GITHUB_ACTIONS_TOKEN` | Personal Access Token *fine-grained* sobre este repositorio, con permiso **Actions: Read and write**. También se acepta el nombre `GITHUB_DISPATCH_TOKEN`. |

Opcionales:

| Variable | Default |
|---|---|
| `GITHUB_ACTIONS_REPO` | `ecojoelvaldez/observatoriolanacional` |
| `GITHUB_ACTIONS_REF` | `main` |

Ya existente y también requerida (es la que valida la sesión del analista):
`INTERNAL_SESSION_SECRET`.

Sin `GITHUB_ACTIONS_TOKEN` el endpoint responde 503 y la tarjeta muestra el
mensaje exacto de qué falta; no falla en silencio.

## Errores que verás y qué significan

| Mensaje | Causa |
|---|---|
| "Sesión de analista requerida" | La cookie expiró (dura 8 horas). Vuelve a iniciar sesión en el panel. |
| "El token de GitHub no es válido o expiró" | Renueva el PAT en Vercel. |
| "GitHub rechazó la solicitud por permisos" | Al token le falta *Actions: Read and write*. |
| "GitHub no encontró el workflow o el repositorio" | `GITHUB_ACTIONS_REPO` apunta a otro repo, o el archivo no existe en la rama de `GITHUB_ACTIONS_REF`. |

## Nota sobre tiempos

Una corrida tarda entre dos y veinte minutos según el pipeline. El panel
refresca el estado solo a los 20 s, 1 min y 2.5 min de haber lanzado; el botón
**Actualizar estado** lo consulta cuando quieras. El dato queda commiteado por
el propio workflow, así que el sitio lo toma en la siguiente carga.
