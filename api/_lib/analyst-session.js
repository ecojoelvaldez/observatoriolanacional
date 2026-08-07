/* =====================================================================
   Observatorio Estratégico La Nacional — verificación de sesión analista
   =====================================================================

   Extraído de api/internal-auth.js para poder reutilizar la verificación
   desde otros endpoints (por ejemplo api/github-actions.js) sin duplicar
   la lógica de firma.

   Los archivos y carpetas que empiezan con "_" dentro de /api NO se
   publican como endpoints en Vercel: esto es una librería interna.
   ===================================================================== */

const crypto = require('node:crypto');

const COOKIE_NAME = 'oe_internal_session';

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

function verifyToken(token, secret) {
  if (!token || !secret) return null;

  const [payload, suppliedSignature] = String(token).split('.');
  if (!payload || !suppliedSignature) return null;

  if (!safeEqual(suppliedSignature, sign(payload, secret))) return null;

  try {
    const session = JSON.parse(Buffer.from(payload, 'base64url').toString('utf8'));

    if (session.role !== 'analyst' || !session.exp || session.exp <= Date.now()) {
      return null;
    }

    return session;
  } catch {
    return null;
  }
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

module.exports = {
  COOKIE_NAME,
  normalizeEnvValue,
  getAnalystSession,
  verifyToken,
  readCookie
};
