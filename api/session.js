/* =====================================================================
   Observatorio Estratégico La Nacional — emisión de la sesión del gate
   =====================================================================

   La página /acceso hace el login con Microsoft contra Supabase y manda
   aquí el access_token resultante. Este endpoint:

     1. Verifica el token CONTRA SUPABASE (no confía en el cliente).
     2. Comprueba que el usuario sea del tenant / dominio de La Nacional.
     3. Emite una cookie HttpOnly firmada que middleware.js sabe leer.

   El paso 1 es lo que hace que esto no sea decorativo: el navegador no
   puede fabricarse una sesión, porque el token lo valida Supabase.

   Variables de entorno requeridas en Vercel:
     LN_GATE_SECRET        secreto para firmar la cookie (aleatorio, largo)
     SUPABASE_URL          https://<ref>.supabase.co  (proyecto destino)
     SUPABASE_ANON_KEY     anon key del mismo proyecto

   Opcionales (control de quién entra):
     LN_ALLOWED_DOMAINS    dominios de correo permitidos, separados por coma
                           (default: asociacionlanacional.com.do)
     LN_ALLOWED_TENANT_ID  si se define, el claim `tid` de Entra debe coincidir
   ===================================================================== */

const crypto = require('node:crypto');

const COOKIE_NAME = 'ln_gate_session';
const SESSION_SECONDS = 8 * 60 * 60;
const DEFAULT_DOMAINS = 'asociacionlanacional.com.do';

function env(name, fallback = '') {
  return String(process.env[name] || fallback).trim();
}

function sign(payload, secret) {
  return crypto.createHmac('sha256', secret).update(payload).digest('base64url');
}

function createToken(secret, user) {
  const payload = Buffer.from(
    JSON.stringify({
      sub: user.id,
      email: user.email,
      name: user.name || null,
      tid: user.tid || null,
      exp: Date.now() + SESSION_SECONDS * 1000,
    }),
  ).toString('base64url');

  return `${payload}.${sign(payload, secret)}`;
}

function sessionCookie(token) {
  return [
    `${COOKIE_NAME}=${encodeURIComponent(token)}`,
    'HttpOnly',
    'Secure',
    'SameSite=Lax', // Lax y no Strict: hay que sobrevivir al retorno desde Microsoft
    'Path=/',
    `Max-Age=${SESSION_SECONDS}`,
  ].join('; ');
}

function expiredCookie() {
  return [
    `${COOKIE_NAME}=`,
    'HttpOnly',
    'Secure',
    'SameSite=Lax',
    'Path=/',
    'Max-Age=0',
  ].join('; ');
}

/* Valida el access_token contra Supabase y devuelve el usuario real. */
async function resolveUser(accessToken, supabaseUrl, anonKey) {
  const response = await fetch(`${supabaseUrl}/auth/v1/user`, {
    headers: {
      apikey: anonKey,
      Authorization: `Bearer ${accessToken}`,
    },
  });

  if (!response.ok) return null;

  const user = await response.json();
  if (!user || !user.id) return null;

  const meta = user.user_metadata || {};
  return {
    id: user.id,
    email: String(user.email || meta.email || '').toLowerCase(),
    name: meta.full_name || meta.name || null,
    tid: meta.tid || meta.tenant_id || null,
  };
}

function isAuthorized(user) {
  const allowedTenant = env('LN_ALLOWED_TENANT_ID');
  if (allowedTenant && user.tid && user.tid !== allowedTenant) return false;

  const domains = env('LN_ALLOWED_DOMAINS', DEFAULT_DOMAINS)
    .split(',')
    .map(d => d.trim().toLowerCase())
    .filter(Boolean);

  if (!domains.length) return true;

  const domain = user.email.split('@')[1] || '';
  return domains.includes(domain);
}

module.exports = async function handler(req, res) {
  res.setHeader('Cache-Control', 'no-store, max-age=0');
  res.setHeader('Content-Type', 'application/json; charset=utf-8');

  const secret = env('LN_GATE_SECRET');
  const supabaseUrl = env('SUPABASE_URL').replace(/\/+$/, '');
  const anonKey = env('SUPABASE_ANON_KEY');

  if (!secret || !supabaseUrl || !anonKey) {
    console.error('Faltan LN_GATE_SECRET, SUPABASE_URL o SUPABASE_ANON_KEY.');
    return res.status(503).json({
      ok: false,
      error: 'La puerta de acceso no está configurada en este entorno.',
    });
  }

  // La página /acceso pregunta aquí contra qué proyecto debe autenticar, en vez
  // de traerlo en un <meta>. Así el proyecto se configura en UN solo lugar (las
  // variables de Vercel) y no puede quedar desincronizado con la validación de
  // más abajo. Ambos valores son públicos por diseño: la anon key viaja al
  // navegador en cualquier app de Supabase, y RLS es lo que protege los datos.
  if (req.method === 'GET') {
    return res.status(200).json({
      ok: true,
      supabaseUrl,
      supabaseAnonKey: anonKey,
    });
  }

  if (req.method === 'DELETE') {
    res.setHeader('Set-Cookie', expiredCookie());
    return res.status(200).json({ ok: true, authenticated: false });
  }

  if (req.method !== 'POST') {
    res.setHeader('Allow', 'GET, POST, DELETE');
    return res.status(405).json({ ok: false, error: 'Método no permitido.' });
  }

  let body = req.body || {};
  if (typeof body === 'string') {
    try { body = JSON.parse(body); } catch { body = {}; }
  }

  const accessToken = String(body.access_token || '').trim();
  if (!accessToken) {
    return res.status(400).json({ ok: false, error: 'Falta access_token.' });
  }

  let user;
  try {
    user = await resolveUser(accessToken, supabaseUrl, anonKey);
  } catch (error) {
    console.error('Error validando el token con Supabase:', error);
    return res.status(502).json({ ok: false, error: 'No se pudo validar la sesión.' });
  }

  if (!user) {
    return res.status(401).json({ ok: false, error: 'Sesión de Microsoft no válida.' });
  }

  if (!isAuthorized(user)) {
    console.warn('Acceso rechazado para %s (tid=%s)', user.email, user.tid);
    return res.status(403).json({
      ok: false,
      error: 'Tu cuenta no pertenece a La Nacional. Si crees que es un error, escribe a Planeación Estratégica.',
    });
  }

  res.setHeader('Set-Cookie', sessionCookie(createToken(secret, user)));

  return res.status(200).json({
    ok: true,
    authenticated: true,
    user: { email: user.email, name: user.name },
  });
};
