/* =====================================================================
   Observatorio Estratégico La Nacional — puerta de acceso institucional
   =====================================================================

   Edge Middleware de Vercel. Corre ANTES de servir cualquier cosa: el
   HTML, los JSON de /data y /news_candidates.json. Si no hay una sesión
   válida de La Nacional, nadie ve nada — se redirige a /acceso.

   Esto es el equivalente al gate de SharePoint: el link lo puede tener
   cualquiera, pero solo entra gente del tenant de La Nacional.

   OJO: esto NO es el login del analista. Ese es una segunda capa, aparte,
   dentro del sitio, y sigue funcionando como siempre.

   La cookie la emite /api/session después de validar el token de
   Microsoft (ver api/session.js). Aquí solo se verifica la firma.
   ===================================================================== */

const COOKIE_NAME = 'ln_gate_session';

// Todo lo que no requiere sesión. `assets/` entra aquí porque son el logo
// y la marca que necesita la propia página de acceso — no hay datos ahí.
export const config = {
  matcher: ['/((?!api/|acceso|assets/|_vercel/|favicon\\.ico|robots\\.txt).*)'],
};

function base64urlToBytes(value) {
  const padded = value.replace(/-/g, '+').replace(/_/g, '/');
  const binary = atob(padded + '='.repeat((4 - (padded.length % 4)) % 4));
  return Uint8Array.from(binary, char => char.charCodeAt(0));
}

function timingSafeEqual(a, b) {
  if (a.length !== b.length) return false;
  let diff = 0;
  for (let i = 0; i < a.length; i += 1) diff |= a[i] ^ b[i];
  return diff === 0;
}

async function verifyToken(token, secret) {
  if (!token || !secret) return null;

  const [payload, signature] = String(token).split('.');
  if (!payload || !signature) return null;

  const key = await crypto.subtle.importKey(
    'raw',
    new TextEncoder().encode(secret),
    { name: 'HMAC', hash: 'SHA-256' },
    false,
    ['sign'],
  );

  const expected = new Uint8Array(
    await crypto.subtle.sign('HMAC', key, new TextEncoder().encode(payload)),
  );

  let supplied;
  try {
    supplied = base64urlToBytes(signature);
  } catch {
    return null;
  }

  if (!timingSafeEqual(expected, supplied)) return null;

  try {
    const session = JSON.parse(new TextDecoder().decode(base64urlToBytes(payload)));
    if (!session.exp || session.exp <= Date.now()) return null;
    return session;
  } catch {
    return null;
  }
}

function readCookie(request, name) {
  const header = request.headers.get('cookie') || '';
  for (const item of header.split(';')) {
    const separator = item.indexOf('=');
    if (separator < 0) continue;
    if (item.slice(0, separator).trim() === name) {
      return decodeURIComponent(item.slice(separator + 1).trim());
    }
  }
  return null;
}

export default async function middleware(request) {
  const secret = (process.env.LN_GATE_SECRET || '').trim();

  // Sin secreto configurado no se puede verificar nada. Se cierra el sitio
  // en vez de dejarlo abierto: fallar cerrado, no abierto.
  if (!secret) {
    return new Response(
      'Acceso no configurado: falta LN_GATE_SECRET en las variables de entorno.',
      { status: 503, headers: { 'content-type': 'text/plain; charset=utf-8' } },
    );
  }

  const session = await verifyToken(readCookie(request, COOKIE_NAME), secret);
  if (session) return undefined; // sesión válida: seguir a la página pedida

  const url = new URL(request.url);
  const destination = new URL('/acceso', url.origin);

  // Se recuerda a dónde iba para devolverlo ahí después del login.
  const wanted = url.pathname + url.search;
  if (wanted && wanted !== '/') destination.searchParams.set('next', wanted);

  return Response.redirect(destination, 302);
}
