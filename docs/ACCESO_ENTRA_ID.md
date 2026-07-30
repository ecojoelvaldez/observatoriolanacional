# Puerta de acceso institucional (Entra ID)

Objetivo: que el Observatorio se comporte como SharePoint. **El link lo puede
tener cualquiera, pero solo entra gente de La Nacional.** Antes de mostrar nada
—ni el dashboard, ni los JSON de datos— se verifica la sesión de Entra ID.

> Esto **no** es el login del analista. Ese es una segunda capa, adentro del
> sitio, y sigue funcionando igual que siempre. Son independientes: la puerta
> decide *quién ve el Observatorio*; el login del analista decide *quién puede
> publicar*.

## Cómo funciona

```
Usuario abre el link
   │
   ├─ middleware.js  ¿cookie ln_gate_session válida?
   │     └─ no ──► redirige a /acceso
   │                  │
   │                  ├─ acceso.html ──► login Microsoft (Supabase provider azure)
   │                  │                     │
   │                  │                     ▼
   │                  │              Entra ID autentica
   │                  │                     │
   │                  │                     ▼
   │                  └─ POST /api/session con el access_token
   │                          │
   │                          ├─ valida el token CONTRA Supabase
   │                          ├─ comprueba tenant + dominio de correo
   │                          └─ emite cookie HttpOnly firmada (8 h)
   │
   └─ sí ──► se sirve el Observatorio
```

La cookie se firma con HMAC-SHA256 (`LN_GATE_SECRET`) y el middleware verifica
la firma en el edge. El navegador no puede fabricarse una sesión: el token de
Microsoft lo valida Supabase del lado del servidor.

### Por qué middleware y no solo JavaScript

El sitio sirve `/data/*.json` y `/news_candidates.json` como **archivos
estáticos**. Una puerta hecha solo en el front escondería la interfaz, pero
cualquiera podría bajarse los datos escribiendo la URL del JSON. El middleware
corre antes de servir *cualquier* ruta, así que tapa también esos archivos.

Rutas que quedan fuera de la puerta a propósito: `/acceso`, `/api/*` y
`/assets/*` (logo y marca que necesita la propia página de acceso; ahí no hay
datos).

## Archivos

| Archivo | Rol |
|---|---|
| `middleware.js` | Edge Middleware. Verifica la cookie y redirige a `/acceso`. |
| `acceso.html` | Página de login con Microsoft. Redirige sola, como SharePoint. |
| `api/session.js` | Valida el token contra Supabase y emite la cookie. |

## Configuración

### 1. Entra ID — App Registration

**Authentication → Redirect URIs, plataforma "Web":**

```
https://btccnnreeansagcduutt.supabase.co/auth/v1/callback
```

> **Ojo con esto**, es el error clásico y cuesta una tarde de depuración: en
> Entra ID va la URL de **Supabase**, no la del Observatorio. Entra le responde
> a Supabase, y Supabase es quien después devuelve al usuario al Observatorio.
> La URL del Observatorio se registra en Supabase (paso 2), no aquí.

**Supported account types:** *Accounts in this organizational directory only
(single tenant)*. Esta es la primera barrera real: con esto, una cuenta
`@gmail.com` ni siquiera llega a autenticarse.

**API permissions:** `openid`, `profile`, `email` (delegados). Si el tenant
exige consentimiento de administrador, hay que darle **Grant admin consent**
o el login falla aunque todo lo demás esté bien.

**Certificates & secrets:** el client secret que se cargue en Supabase. Anotar
la fecha de expiración — los secrets de Entra caducan y el día que caduque, la
puerta deja de abrir para todo el mundo.

### 2. Supabase (proyecto destino)

**Authentication → Providers → Azure:** habilitar y cargar

| Campo | Valor |
|---|---|
| Client ID | el del App Registration |
| Secret | el client secret |
| Azure Tenant URL | `https://login.microsoftonline.com/<TENANT_ID>/v2.0` |

**Authentication → URL Configuration → Redirect URLs:** aquí sí van las URLs
del Observatorio, una por línea:

```
https://<dominio-de-produccion>/acceso
https://<preview-de-vercel>/acceso
```

Si falta esta lista, el login termina con un error de *redirect not allowed*.

### 3. Vercel — Environment Variables

| Variable | Valor |
|---|---|
| `LN_GATE_SECRET` | cadena aleatoria larga: `openssl rand -base64 48` |
| `SUPABASE_URL` | `https://btccnnreeansagcduutt.supabase.co` |
| `SUPABASE_ANON_KEY` | anon key del proyecto destino |
| `LN_ALLOWED_TENANT_ID` | el Tenant ID de La Nacional (opcional pero recomendado) |
| `LN_ALLOWED_DOMAINS` | opcional; default `asociacionlanacional.com.do` |

Si falta `LN_GATE_SECRET`, el sitio responde **503 y no deja entrar a nadie**.
Falla cerrado a propósito: es preferible un sitio caído a un sitio abierto.

### 4. Frontend

`acceso.html` **no** trae el proyecto escrito: se lo pregunta a
`GET /api/session`, que lo lee de `SUPABASE_URL` y `SUPABASE_ANON_KEY`. Así el
proyecto contra el que se autentica y el proyecto contra el que se valida el
token no pueden quedar desincronizados. No hay nada que editar en el HTML de la
puerta: se cambian las variables de Vercel y ya.

(`index.html` sí tiene sus `<meta>` propios, porque eso apunta a de dónde salen
los *datos* del dashboard, que es una decisión distinta y se cambia en el
cutover de datos.)

---

## Qué es `LN_GATE_SECRET`

No te lo da Microsoft ni Supabase. **Lo inventas tú**, una sola vez.

Piénsalo como la pulsera de un evento. En la puerta, el de seguridad
(Microsoft) te revisa la cédula una vez. No te la va a revisar otra vez cada
vez que caminas de un salón a otro — te ponen una pulsera. La pulsera lleva un
estampado que solo el organizador sabe imprimir: tú puedes mirarla, pero no
puedes fabricarte una en tu casa.

`LN_GATE_SECRET` es el molde de ese estampado.

Sin él, cualquiera podría fabricarse una cookie que diga "yo soy de La
Nacional" y entrar sin pasar por Microsoft. Con él, el servidor firma la cookie
al emitirla y verifica la firma en cada visita. La firma se puede leer, pero no
se puede falsificar sin conocer el secreto.

Por eso:

- Se genera **al azar**, una sola vez: `openssl rand -base64 48`
- Se guarda **solo** en las variables de entorno de Vercel. En ningún archivo.
- Si lo cambias, todo el mundo tiene que volver a entrar por Microsoft. Es
  inofensivo — de hecho es la forma de echar a todos si hiciera falta.
- Si falta, el sitio responde 503 y no entra nadie. Falla cerrado a propósito.

## Quién puede entrar

Tres filtros, en orden:

1. **Entra ID** rechaza cuentas fuera del directorio (single tenant).
2. **`LN_ALLOWED_TENANT_ID`** compara el claim `tid` del token.
3. **`LN_ALLOWED_DOMAINS`** compara el dominio del correo.

Para dejar entrar a un dominio adicional (una filial, un consultor) se agrega a
`LN_ALLOWED_DOMAINS` separado por coma. No hace falta tocar código.

## Verificación

Probado en local con tokens reales de prueba: la cookie que firma
`api/session.js` con Node la verifica el Web Crypto del middleware; se rechazan
cookies expiradas, firmadas con otro secreto y con el payload manipulado
conservando la firma vieja. Se confirmó que `/data/*.json` y
`/news_candidates.json` quedan detrás de la puerta, y que `/acceso`, `/api/*` y
`/assets/*` quedan fuera. En autorización: pasa el usuario del tenant, se
rechaza con 403 la cuenta personal y la de otro tenant, y con 401 el token que
Supabase no reconoce.

**Falta probar contra Entra ID real.** El entorno donde se escribió esto no
alcanza ni a `*.supabase.co` ni a `login.microsoftonline.com`, así que el
viaje completo del login hay que verificarlo en un preview de Vercel.

Checklist para esa prueba:

- [ ] Abrir el preview en ventana privada → debe mandar a Microsoft sin pedir nada.
- [ ] Entrar con una cuenta de La Nacional → vuelve al Observatorio.
- [ ] Pedir `/data/sib_snapshot.json` directo sin sesión → debe redirigir, no descargar.
- [ ] Entrar con una cuenta personal → mensaje de "no perteneces a La Nacional".
- [ ] Borrar la cookie y recargar → vuelve a pedir login.
- [ ] Verificar que el panel del analista sigue abriendo con su contraseña.

## Pendientes conocidos

- **Los datos de Supabase siguen siendo legibles con la anon key.** Las
  políticas RLS actuales permiten `select` a `public`, así que alguien con la
  anon key (que es pública por diseño) puede leer las tablas por la API de
  Supabase, sin pasar por la puerta. Cerrar eso significa cambiar los `select`
  de `to public` a `to authenticated` — pero eso solo tiene sentido cuando el
  front esté autenticando contra Supabase con la identidad de Entra, porque hoy
  el dashboard lee como `anon`. Es el siguiente paso lógico después de que la
  puerta esté estable.
- **La sesión dura 8 horas** y no se renueva sola: pasado ese tiempo el usuario
  vuelve a pasar por Microsoft (normalmente sin escribir nada, porque la sesión
  de Microsoft sigue viva).
