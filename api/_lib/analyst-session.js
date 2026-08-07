/* =====================================================================
   Observatorio Estratégico La Nacional — verificación de sesión analista
   =====================================================================

   Extraído de api/internal-auth.js para poder reutilizar la verificación
   desde otros endpoints (por ejemplo api/github-actions.js) sin duplicar
   la lógica de firma.

   Hay DOS sesiones distintas en este sitio y conviene no confundirlas:

     ln_gate_session      La puerta del sitio. La emite /api/session después
                          de validar el token de Microsoft contra Supabase, y
                          middleware.js la exige para servir cualquier página.
                          Si alguien está viendo el sitio, tiene esta cookie:
                          es identidad real, verificada en servidor.

     oe_internal_session  El login del analista vía /api/internal-auth. Hoy el
                          panel NO usa esta ruta: valida usuario y clave en el
                          navegador (ver ln-hardcoded-bypass-final en
                          index.html) y guarda un flag en sessionStorage, así
                          que esta cookie normalmente no existe.

   Por eso getAuthorizedSession() acepta cualquiera de las dos. Exigir solo
   la del analista dejaba los endpoints respondiendo 401 siempre, y no habría
   añadido seguridad real: la clave del panel viaja en el HTML, mientras que
   la del portal la firma el servidor y no se puede fabricar.

   Los archivos y carpetas que empiezan con "_" dentro de /api NO se
   publican como endpoints en Vercel: esto es una librería interna.
   ===================================================================== */

const crypto = require('node:crypto');

const COOKIE_NAME = 'oe_internal_session';
const GATE_COOKIE_NAME = 'ln_gate_session';

/* Corrige los dos errores clásicos al pegar variables en Vercel:
   dejar el "NOMBRE=" adelante, o envolver el valor en comillas. */
function normalizeEnvValue(value, variableName) {
  let normalized = String(value || '').trim();

  const prefix = `${variableName}=`;
  if (normalized.startsWith(prefix)) {
    normalized = normalized.slice(prefix.length).trim();
  }

  if (
    normalized.length >= 2 &&
    (
      (normalized.startsWith('"') && normalized.endsWith('"')) ||
      (normalized.startsWith("'") && normalized.endsWith("'"))
    )
  ) {
    normalized = normalized.slice(1, -1);
  }

  return normalized.trim();
}

function safeEqual(valueA = '', valueB = '') {
  const a = Buffer.from(String(valueA));
  const b = Buffer.from(String(valueB));

  if (a.length !== b.length) return false;
  return crypto.timingSafeEqual(a, b);
}

function sign(payload, secret) {
  return crypto.createHmac('sha256', secret).update(payload).digest('base64url');
}

/* requiredRole: 'analyst' para la cookie del panel; null para la del portal,
   cuyo payload no lleva rol (ver createToken en api/session.js). */
function verifySignedToken(token, secret, requiredRole = null) {
  if (!token || !secret) return null;

  const [payload, suppliedSignature] = String(token).split('.');
  if (!payload || !suppliedSignature) return null;

  if (!safeEqual(suppliedSignature, sign(payload, secret))) return null;

  try {
    const session = JSON.parse(Buffer.from(payload, 'base64url').toString('utf8'));

    if (!session.exp || session.exp <= Date.now()) return null;
    if (requiredRole && session.role !== requiredRole) return null;

    return session;
  } catch {
    return null;
  }
}

function verifyToken(token, secret) {
  return verifySignedToken(token, secret, 'analyst');
}

function readCookie(req, name) {
  if (req.cookies && req.cookies[name]) return req.cookies[name];

  const cookieHeader = (req.headers && req.headers.cookie) || '';

  for (const item of cookieHeader.split(';')) {
    const separator = item.indexOf('=');
    if (separator < 0) continue;

    if (item.slice(0, separator).trim() === name) {
      return decodeURIComponent(item.slice(separator + 1).trim());
    }
  }

  return null;
}

/* Devuelve la sesión del analista o null. No responde nada: el endpoint
   que llama decide qué código de error emitir. */
function getAnalystSession(req) {
  const secret = normalizeEnvValue(
    process.env.INTERNAL_SESSION_SECRET,
    'INTERNAL_SESSION_SECRET'
  );

  if (!secret) return null;
  return verifyToken(readCookie(req, COOKIE_NAME), secret);
}

/* Sesión de la puerta del sitio: el usuario ya pasó por Microsoft. */
function getGateSession(req) {
  const secret = normalizeEnvValue(process.env.LN_GATE_SECRET, 'LN_GATE_SECRET');

  if (!secret) return null;
  return verifySignedToken(readCookie(req, GATE_COOKIE_NAME), secret, null);
}

/* Cualquiera de las dos sesiones sirve. Devuelve
   { via, user } o null, para poder registrar quién disparó qué. */
function getAuthorizedSession(req) {
  const analyst = getAnalystSession(req);
  if (analyst) {
    return { via: 'analyst', user: analyst.username || 'analista', session: analyst };
  }

  const gate = getGateSession(req);
  if (gate) {
    return { via: 'gate', user: gate.email || gate.name || 'usuario del portal', session: gate };
  }

  return null;
}

/* Para diagnóstico: qué cookies llegaron y qué secretos están configurados.
   Solo booleanos y nombres — nunca valores de cookie ni de token. */
function describeAuthContext(req) {
  return {
    cookies: {
      analyst: Boolean(readCookie(req, COOKIE_NAME)),
      gate: Boolean(readCookie(req, GATE_COOKIE_NAME))
    },
    secrets: {
      INTERNAL_SESSION_SECRET: Boolean(normalizeEnvValue(process.env.INTERNAL_SESSION_SECRET, 'INTERNAL_SESSION_SECRET')),
      LN_GATE_SECRET: Boolean(normalizeEnvValue(process.env.LN_GATE_SECRET, 'LN_GATE_SECRET'))
    }
  };
}

module.exports = {
  COOKIE_NAME,
  GATE_COOKIE_NAME,
  normalizeEnvValue,
  getAnalystSession,
  getGateSession,
  getAuthorizedSession,
  describeAuthContext,
  verifyToken,
  readCookie
};
