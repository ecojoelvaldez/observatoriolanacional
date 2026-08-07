# Lanzar los pipelines desde el panel del analista

El panel del analista tiene una tarjeta **Automatizaciones GitHub** con cuatro
botones que disparan, a mano, los workflows que normalmente corren por
calendario:

| Botón | Workflow | Qué hace |
|---|---|---|
| Resumen de noticias | `news_pipeline_v2.yml` | Lee las fuentes autorizadas y deja candidatas en `news_candidates.json`. |
| Resumen IA Gemini | `gemini_summaries.yml` | Regenera los resúmenes ejecutivos con el corte SIB más reciente. |
| Indicadores pipeline | `sib_pipeline.yml` | Descarga ROA, ROE, morosidad y cobertura del API de la SIB. |
| Cartera pipeline | `sib_cartera_pipeline.yml` | Reconstruye cartera y captación por producto, región y moneda. |

## Cómo funciona

El navegador **no** habla con GitHub. Disparar un workflow exige un token con
permiso de escritura sobre Actions, y ese token no puede vivir en el HTML. El
botón llama a `POST /api/github-actions` (Vercel), que:

1. Verifica que quien llama tenga sesión válida del sitio.
2. Comprueba que el pipeline pedido esté en la lista blanca del endpoint.
   Un cliente no puede pedir un archivo de workflow arbitrario.
3. Llama al `workflow_dispatch` de GitHub con el token del servidor.
4. Busca la corrida recién creada y devuelve su enlace, para que el analista
   pueda seguirla sin abrir GitHub a mano.

`GET /api/github-actions` devuelve el estado de la última corrida de cada
workflow; es lo que pinta las etiquetas "OK · 07 ago 09:12" de cada tarjeta.

## Qué sesión hace falta (y por qué no es la del panel)

En el sitio hay dos cookies de sesión y es fácil confundirlas:

| Cookie | Quién la emite | ¿Existe? |
|---|---|---|
| `ln_gate_session` | `/api/session`, después de validar el token de Microsoft contra Supabase. `middleware.js` la exige para servir cualquier página. | Sí. Si estás viendo el sitio, la tienes. |
| `oe_internal_session` | `/api/internal-auth` | Normalmente **no**. |

El login del panel del analista (usuario y clave) se valida **en el navegador**
—ver el bloque `ln-hardcoded-bypass-final` en `index.html`—: guarda un flag en
`sessionStorage` y nunca llama a `/api/internal-auth`, así que esa cookie no se
emite. La primera versión de este endpoint exigía justo esa cookie, y por eso
los cuatro botones respondían "no se lanzó" sin importar qué se hiciera.

Ahora el endpoint acepta cualquiera de las dos. Exigir solo la del panel no
añadía seguridad real: su clave viaja en el HTML y cualquiera puede leerla,
mientras que la del portal la firma el servidor con `LN_GATE_SECRET` y no se
puede fabricar. La barrera efectiva sigue siendo el login institucional de
Microsoft, que es la misma que protege todo el resto del sitio.

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

Ya existente y también requerida, porque es la que valida la sesión:
`LN_GATE_SECRET`. (`INTERNAL_SESSION_SECRET` solo hace falta si algún día el
panel vuelve a autenticarse por `/api/internal-auth`.)

Sin `GITHUB_ACTIONS_TOKEN` el endpoint responde 503 y la tarjeta muestra el
mensaje exacto de qué falta; no falla en silencio.

## Errores que verás y qué significan

Antes de nada: abre la consola del navegador y ejecuta
`lnDiagnosticoPipelines()`. Dice si llegó la cookie, si el token de GitHub está
configurado y contra qué repo y rama se apunta — sin exponer ningún secreto.
Equivale a abrir `/api/github-actions?diagnostico=1`.

| Mensaje | Causa |
|---|---|
| "El endpoint /api/github-actions no existe en este despliegue" | Falta desplegar la rama que lo añade. |
| "No llegó ninguna cookie de sesión" | Entraste sin pasar por el acceso institucional. |
| "La sesión existe pero venció" | La cookie del portal dura 8 horas. Recarga la página. |
| "El token de GitHub no es válido o expiró" | Renueva el PAT en Vercel. |
| "GitHub rechazó la solicitud por permisos" | Al token le falta *Actions: Read and write*. |
| "GitHub no encontró el workflow o el repositorio" | `GITHUB_ACTIONS_REPO` apunta a otro repo, o el archivo no existe en la rama de `GITHUB_ACTIONS_REF`. |

## Cómo commitean los workflows (y por qué ya no fallan)

Los cinco workflows terminaban así:

```bash
git add archivo && git commit -m "..." && git pull --rebase origin main && git push
```

El `--rebase` reproduce el commit del job encima del remoto. Si entre el
checkout y el push alguien más tocó el mismo archivo —otra corrida del mismo
pipeline, el pipeline de Gemini, o un push a mano— el replay chocaba y el job
moría con:

```
error: could not apply <sha>... Actualizar candidatos de noticias
```

Lanzar los pipelines a mano desde el panel hace esa colisión mucho más
probable, porque ya no están espaciados por el calendario.

Un conflicto ahí no tiene sentido: estos archivos no se editan a mano, se
regeneran completos en cada corrida, así que la versión recién generada
siempre es la correcta. Ahora el commit va por `scripts/commit_generated.sh`,
que en vez de rebasar:

1. Guarda los archivos generados fuera del árbol de trabajo.
2. Hace `fetch` + `reset --hard` al remoto.
3. Vuelve a escribir los archivos generados encima.
4. Commitea y empuja. Si pierde la carrera contra otro job, repite el ciclo
   hasta cinco veces con espera creciente.

El push nunca conflictúa, no se pierde el commit del otro pipeline y, si dos
corridas coinciden, gana la que termina de último — la que trae el dato más
fresco. El script deja `pushed=true|false` en los outputs del paso, y no crea
commits vacíos cuando el dato regenerado es idéntico al que ya está en el repo.

## Nota sobre tiempos

Una corrida tarda entre dos y veinte minutos según el pipeline. El panel
refresca el estado solo a los 20 s, 1 min y 2.5 min de haber lanzado; el botón
**Actualizar estado** lo consulta cuando quieras. El dato queda commiteado por
el propio workflow, así que el sitio lo toma en la siguiente carga.
