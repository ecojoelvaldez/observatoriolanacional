const crypto = require('node:crypto');

const COOKIE_NAME = 'oe_internal_session';
const SESSION_SECONDS = 8 * 60 * 60;

function normalizeEnvValue(value, variableName) {
  let normalized = String(value || '').trim();

  // Corrige el error común de pegar "NOMBRE_VARIABLE=valor"
  const prefix = `${variableName}=`;
  if (normalized.startsWith(prefix)) {
    normalized = normalized.slice(prefix.length).trim();
  }

  // Corrige comillas externas añadidas al pegar el valor en Vercel.
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
  return crypto
    .createHmac('sha256', secret)
    .update(payload)
    .digest('base64url');
}

function createToken(secret, username) {
  const payload = Buffer.from(
    JSON.stringify({
      role: 'analyst',
      username,
      source: 'internal-vercel',
      exp: Date.now() + SESSION_SECONDS * 1000
    })
  ).toString('base64url');

  return `${payload}.${sign(payload, secret)}`;
}

function verifyToken(token, secret) {
  if (!token || !secret) return null;

  const [payload, suppliedSignature] = String(token).split('.');
  if (!payload || !suppliedSignature) return null;

  const expectedSignature = sign(payload, secret);
  if (!safeEqual(suppliedSignature, expectedSignature)) return null;

  try {
    const session = JSON.parse(
      Buffer.from(payload, 'base64url').toString('utf8')
    );

    if (
      session.role !== 'analyst' ||
      !session.exp ||
      session.exp <= Date.now()
    ) {
      return null;
    }

    return session;
  } catch {
    return null;
  }
}

function readCookie(req, name) {
  if (req.cookies && req.cookies[name]) return req.cookies[name];

  const cookieHeader = req.headers.cookie || '';

  for (const item of cookieHeader.split(';')) {
    const separator = item.indexOf('=');
    if (separator < 0) continue;

    const key = item.slice(0, separator).trim();

    if (key === name) {
      return decodeURIComponent(
        item.slice(separator + 1).trim()
      );
    }
  }

  return null;
}

function createSessionCookie(token) {
  return [
    `${COOKIE_NAME}=${encodeURIComponent(token)}`,
    'HttpOnly',
    'Secure',
    'SameSite=Strict',
    'Path=/',
    `Max-Age=${SESSION_SECONDS}`
  ].join('; ');
}

function expireSessionCookie() {
  return [
    `${COOKIE_NAME}=`,
    'HttpOnly',
    'Secure',
    'SameSite=Strict',
    'Path=/',
    'Max-Age=0'
  ].join('; ');
}

module.exports = async function handler(req, res) {
  res.setHeader('Cache-Control', 'no-store, max-age=0');
  res.setHeader('Content-Type', 'application/json; charset=utf-8');

  const configuredUser = normalizeEnvValue(
    process.env.INTERNAL_BYPASS_USER,
    'INTERNAL_BYPASS_USER'
  );

  const configuredPassword = normalizeEnvValue(
    process.env.INTERNAL_BYPASS_PASSWORD,
    'INTERNAL_BYPASS_PASSWORD'
  );

  const sessionSecret = normalizeEnvValue(
    process.env.INTERNAL_SESSION_SECRET,
    'INTERNAL_SESSION_SECRET'
  );

  if (!configuredUser || !configuredPassword || !sessionSecret) {
    console.error(
      'Faltan INTERNAL_BYPASS_USER, INTERNAL_BYPASS_PASSWORD o INTERNAL_SESSION_SECRET.'
    );

    return res.status(503).json({
      ok: false,
      authenticated: false,
      error: 'Acceso interno no configurado en este entorno de Vercel.'
    });
  }

  if (req.method === 'POST') {
    let body = req.body || {};

    if (typeof body === 'string') {
      try {
        body = JSON.parse(body);
      } catch {
        body = {};
      }
    }

    const username = String(body.username || '')
      .trim()
      .toLowerCase();

    const password = String(body.password || '').trim();

    const expectedUser = configuredUser
      .trim()
      .toLowerCase();

    const validUser = safeEqual(username, expectedUser);
    const validPassword = safeEqual(password, configuredPassword);

    if (!validUser || !validPassword) {
      return res.status(401).json({
        ok: false,
        authenticated: false,
        error: 'Usuario o contraseña incorrectos.',
        diagnostic: {
          userMatches: validUser,
          passwordMatches: validPassword,
          receivedUsername: username,
          configuredUsername: expectedUser,
          receivedPasswordLength: password.length,
          configuredPasswordLength: configuredPassword.length,
          deployment: process.env.VERCEL_ENV || 'unknown'
        }
      });
    }

    const token = createToken(sessionSecret, configuredUser);

    res.setHeader('Set-Cookie', createSessionCookie(token));

    return res.status(200).json({
      ok: true,
      authenticated: true,
      user: {
        username: configuredUser,
        role: 'analyst',
        mode: 'internal'
      }
    });
  }

  if (req.method === 'GET') {
    const session = verifyToken(
      readCookie(req, COOKIE_NAME),
      sessionSecret
    );

    if (!session) {
      return res.status(401).json({
        ok: false,
        authenticated: false
      });
    }

    return res.status(200).json({
      ok: true,
      authenticated: true,
      user: {
        username: session.username || configuredUser,
        role: session.role,
        mode: session.source
      }
    });
  }

  if (req.method === 'DELETE') {
    res.setHeader('Set-Cookie', expireSessionCookie());

    return res.status(200).json({
      ok: true,
      authenticated: false
    });
  }

  res.setHeader('Allow', 'GET, POST, DELETE');

  return res.status(405).json({
    ok: false,
    error: 'Método no permitido.'
  });
};
