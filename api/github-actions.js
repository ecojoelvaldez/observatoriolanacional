/* =====================================================================
   Observatorio Estratégico La Nacional — disparador de GitHub Actions
   =====================================================================

   Permite que el analista lance, desde su panel en el sitio, los cuatro
   pipelines que hoy solo corren por calendario:

     noticias     → .github/workflows/news_pipeline_v2.yml
     gemini       → .github/workflows/gemini_summaries.yml
     indicadores  → .github/workflows/sib_pipeline.yml
     cartera      → .github/workflows/sib_cartera_pipeline.yml

   Por qué existe este endpoint y no se llama a GitHub desde el navegador:
   disparar un workflow exige un token con permiso de escritura sobre
   Actions. Ese token NUNCA puede vivir en el HTML. Aquí queda del lado
   servidor y solo se usa después de comprobar la cookie del analista.

   Métodos:
     GET   → estado de las últimas corridas de cada workflow (para pintar
             el panel sin tener que abrir GitHub).
     GET ?diagnostico=1
           → qué está configurado y qué cookie llegó, sin exponer valores.
             Es lo primero que hay que mirar cuando el panel dice que no
             pudo lanzar nada.
     POST  → { workflow: "noticias" | "gemini" | "indicadores" | "cartera",
               inputs?: {...} }  dispara la corrida.

   Quién puede llamar: cualquiera con sesión válida del portal (la cookie
   que emite el login de Microsoft) o del panel. Ver api/_lib/analyst-session.js
   para el detalle de por qué son dos cookies y no una.

   Variables de entorno requeridas en Vercel:
     LN_GATE_SECRET            ya existente (sesión del portal). También
                               sirve INTERNAL_SESSION_SECRET si algún día el
                               panel vuelve a usar /api/internal-auth.
     GITHUB_ACTIONS_TOKEN      PAT fine-grained con permiso
                               "Actions: read and write" sobre este repo.
                               (También se acepta GITHUB_DISPATCH_TOKEN.)

   Opcionales:
     GITHUB_ACTIONS_REPO       owner/repo. Default: el repo del observatorio.
     GITHUB_ACTIONS_REF        rama sobre la que corre. Default: main.
   ===================================================================== */

const {
  normalizeEnvValue,
  getAuthorizedSession,
  describeAuthContext
} = require('./_lib/analyst-session');

const DEFAULT_REPO = 'ecojoelvaldez/observatoriolanacional';
const DEFAULT_REF = 'main';
const GITHUB_API = 'https://api.github.com';

/* Lista blanca. Un cliente solo puede pedir una de estas claves: nunca
   un nombre de archivo arbitrario. Cada entrada declara qué inputs se
   aceptan, así el navegador tampoco puede inyectar parámetros libres. */
const WORKFLOWS = {
  noticias: {
    file: 'news_pipeline_v2.yml',
    label: 'Resumen de noticias',
    allowedInputs: ['debug']
  },
  gemini: {
    file: 'gemini_summaries.yml',
    label: 'Resumen IA Gemini',
    allowedInputs: ['dry_run']
  },
  indicadores: {
    file: 'sib_pipeline.yml',
    label: 'Indicadores SIB',
    allowedInputs: ['diagnostico', 'periodo_inicial', 'periodo_final', 'salida']
  },
  cartera: {
    file: 'sib_cartera_pipeline.yml',
    label: 'Cartera SIB',
    allowedInputs: ['periodos', 'tipos', 'diagnostico']
  }
};

function readToken() {
  return (
    normalizeEnvValue(process.env.GITHUB_ACTIONS_TOKEN, 'GITHUB_ACTIONS_TOKEN') ||
    normalizeEnvValue(process.env.GITHUB_DISPATCH_TOKEN, 'GITHUB_DISPATCH_TOKEN')
  );
}

function readRepo() {
  return (
    normalizeEnvValue(process.env.GITHUB_ACTIONS_REPO, 'GITHUB_ACTIONS_REPO') ||
    DEFAULT_REPO
  );
}

function readRef() {
  return (
    normalizeEnvValue(process.env.GITHUB_ACTIONS_REF, 'GITHUB_ACTIONS_REF') ||
    DEFAULT_REF
  );
}

async function githubFetch(path, token, options = {}) {
  const response = await fetch(`${GITHUB_API}${path}`, {
    ...options,
    headers: {
      Accept: 'application/vnd.github+json',
      Authorization: `Bearer ${token}`,
      'X-GitHub-Api-Version': '2022-11-28',
      'User-Agent': 'observatorio-la-nacional',
      ...(options.body ? { 'Content-Type': 'application/json' } : {}),
      ...(options.headers || {})
    }
  });

  const raw = await response.text();
  let json = null;
  if (raw) {
    try { json = JSON.parse(raw); } catch { json = null; }
  }

  return { ok: response.ok, status: response.status, json, raw };
}

/* Traduce el error crudo de GitHub a algo que el analista pueda accionar
   sin abrir la consola del navegador. */
function explainGithubError(status, json) {
  const detail = (json && json.message) || '';

  if (status === 401) return 'El token de GitHub no es válido o expiró. Renueva GITHUB_ACTIONS_TOKEN en Vercel.';
  if (status === 403) return `GitHub rechazó la solicitud por permisos: ${detail || 'el token necesita "Actions: read and write".'}`;
  if (status === 404) return 'GitHub no encontró el workflow o el repositorio. Verifica GITHUB_ACTIONS_REPO y que el archivo exista en la rama configurada.';
  if (status === 422) return `GitHub rechazó los parámetros: ${detail || 'revisa que el workflow tenga workflow_dispatch y que la rama exista.'}`;

  return detail || `GitHub respondió HTTP ${status}.`;
}

function summarizeRun(run) {
  if (!run) return null;

  return {
    id: run.id,
    status: run.status,             // queued | in_progress | completed
    conclusion: run.conclusion,     // success | failure | cancelled | null
    event: run.event,
    branch: run.head_branch,
    createdAt: run.created_at,
    updatedAt: run.updated_at,
    url: run.html_url
  };
}

/* Estado de las últimas corridas: una llamada por workflow, en paralelo.
   Si una falla, se reporta solo esa y el resto sigue sirviendo. */
async function collectStatus(token, repo) {
  const entries = await Promise.all(
    Object.entries(WORKFLOWS).map(async ([key, config]) => {
      const path = `/repos/${repo}/actions/workflows/${config.file}/runs?per_page=1`;
      const result = await githubFetch(path, token);

      if (!result.ok) {
        return [key, {
          label: config.label,
          file: config.file,
          error: explainGithubError(result.status, result.json)
        }];
      }

      const run = summarizeRun((result.json && result.json.workflow_runs || [])[0]);
      return [key, { label: config.label, file: config.file, lastRun: run }];
    })
  );

  return Object.fromEntries(entries);
}

/* Tras el dispatch, GitHub responde 204 sin decir qué corrida creó. Se
   consulta la corrida más reciente disparada a mano para devolver su
   enlace: sin esto el analista no tiene a dónde ir a mirar. */
async function findDispatchedRun(token, repo, file) {
  const path = `/repos/${repo}/actions/workflows/${file}/runs?event=workflow_dispatch&per_page=1`;

  for (let attempt = 0; attempt < 3; attempt += 1) {
    await new Promise(resolve => setTimeout(resolve, attempt === 0 ? 1200 : 2000));

    const result = await githubFetch(path, token);
    if (!result.ok) return null;

    const run = summarizeRun((result.json && result.json.workflow_runs || [])[0]);
    if (run) return run;
  }

  return null;
}

function parseBody(req) {
  let body = req.body || {};

  if (typeof body === 'string') {
    try { body = JSON.parse(body); } catch { body = {}; }
  }

  return body && typeof body === 'object' ? body : {};
}

/* Solo pasan los inputs declarados por el workflow, y siempre como texto:
   workflow_dispatch rechaza cualquier otra cosa. */
function sanitizeInputs(config, rawInputs) {
  const out = {};
  if (!rawInputs || typeof rawInputs !== 'object') return out;

  for (const key of config.allowedInputs) {
    if (rawInputs[key] === undefined || rawInputs[key] === null) continue;
    const value = String(rawInputs[key]).trim();
    if (value) out[key] = value;
  }

  return out;
}

module.exports = async function handler(req, res) {
  res.setHeader('Cache-Control', 'no-store, max-age=0');
  res.setHeader('Content-Type', 'application/json; charset=utf-8');

  if (!['GET', 'POST'].includes(req.method)) {
    res.setHeader('Allow', 'GET, POST');
    return res.status(405).json({ ok: false, error: 'Método no permitido.' });
  }

  const auth = getAuthorizedSession(req);
  const context = describeAuthContext(req);
  const token = readToken();
  const repo = readRepo();
  const ref = readRef();

  // El diagnóstico se responde antes del control de sesión, a propósito: si
  // devolviera 401 no serviría justamente cuando hay que usarlo. Solo dice
  // qué está configurado y qué cookie llegó — nunca un valor.
  if (req.method === 'GET' && String(req.query?.diagnostico || '') === '1') {
    return res.status(200).json({
      ok: true,
      diagnostico: {
        autorizado: Boolean(auth),
        via: auth ? auth.via : null,
        ...context,
        githubTokenConfigurado: Boolean(token),
        repo,
        ref,
        pipelines: Object.keys(WORKFLOWS)
      }
    });
  }

  if (!auth) {
    const pista = context.cookies.gate || context.cookies.analyst
      ? 'La sesión existe pero venció o no se pudo verificar; recarga la página para renovarla.'
      : 'No llegó ninguna cookie de sesión. Entra al sitio por el acceso institucional y vuelve a intentar.';

    return res.status(401).json({
      ok: false,
      error: `Sesión requerida para lanzar pipelines. ${pista}`,
      diagnostico: context
    });
  }

  if (!token) {
    return res.status(503).json({
      ok: false,
      error: 'Falta GITHUB_ACTIONS_TOKEN en las variables de entorno de Vercel. Sin ese token el sitio no puede lanzar los pipelines.'
    });
  }

  if (req.method === 'GET') {
    const workflows = await collectStatus(token, repo);
    return res.status(200).json({ ok: true, repo, ref, workflows });
  }

  const body = parseBody(req);
  const key = String(body.workflow || '').trim().toLowerCase();
  const config = WORKFLOWS[key];

  if (!config) {
    return res.status(400).json({
      ok: false,
      error: `Pipeline desconocido: "${key}". Válidos: ${Object.keys(WORKFLOWS).join(', ')}.`
    });
  }

  const inputs = sanitizeInputs(config, body.inputs);

  const dispatch = await githubFetch(
    `/repos/${repo}/actions/workflows/${config.file}/dispatches`,
    token,
    {
      method: 'POST',
      body: JSON.stringify({ ref, ...(Object.keys(inputs).length ? { inputs } : {}) })
    }
  );

  if (!dispatch.ok) {
    return res.status(dispatch.status === 401 ? 502 : dispatch.status).json({
      ok: false,
      workflow: key,
      label: config.label,
      error: explainGithubError(dispatch.status, dispatch.json)
    });
  }

  const run = await findDispatchedRun(token, repo, config.file);

  return res.status(202).json({
    ok: true,
    workflow: key,
    label: config.label,
    repo,
    ref,
    inputs,
    run,
    // Si la corrida aún no aparece en el API, el analista igual tiene a
    // dónde ir: la vista del workflow en GitHub.
    runsUrl: `https://github.com/${repo}/actions/workflows/${config.file}`,
    triggeredBy: auth.user,
    triggeredVia: auth.via
  });
};
