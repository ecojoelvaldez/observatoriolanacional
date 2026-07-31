# Migración Supabase — Observatorio Estratégico La Nacional

Handoff institucional: mover el backend del Observatorio del proyecto Supabase
personal al proyecto de la cuenta departamental, sin cortar la operación diaria
de Nathali Orozco.

**Ruta B: migración manual de datos entre dos proyectos que ya existen.** No es
una transferencia de organización — el proyecto destino ya tiene trabajo de Auth
(Azure/Entra ID) encima que se perdería al transferir el origen.

| | Origen | Destino |
|---|---|---|
| Nombre | la nacional | (departamental) |
| Project ref | `albtuqzdcltcokfagdvy` | `btccnnreeansagcduutt` |
| Host DB | `db.albtuqzdcltcokfagdvy.supabase.co` | `db.btccnnreeansagcduutt.supabase.co` |
| Región | us-east-1 | confirmar |
| Postgres | 17.6.1.127 | confirmar que sea 17.x |
| Cuenta | Personal (Joel) | Departamental |

---

## 1. Qué hay realmente en el origen

Inventario levantado el 2026-07-30 contra `albtuqzdcltcokfagdvy`:

| Objeto | Cantidad | Nota |
|---|---|---|
| Schemas custom | 0 | solo `public`; el resto son los gestionados por Supabase |
| Tablas | 9 | todas con RLS activo |
| Políticas RLS | 23 | ver `supabase/migrations/…_public_schema.sql` |
| Funciones | 1 | `increment_search_quota(integer)` |
| Triggers / vistas / secuencias / enums | 0 | nada que migrar |
| Edge Functions | 0 | nada que migrar |
| Buckets de Storage | 1 | `analyst-uploads`, privado, **0 objetos** |
| Extensiones | `uuid-ossp`, `pgcrypto`, `pg_stat_statements`, `supabase_vault` | las últimas dos ya vienen en cualquier proyecto Supabase |

Filas por tabla:

| Tabla | Filas | Observación |
|---|---|---|
| `data_update_log` | 734 | **661 son snapshots `LIVE_STATE` que pesan ~915 MB** |
| `news_proposals` | 87 | |
| `news_sources` | 17 | |
| `news_items` | 0 | |
| `sib_series_long`, `sib_peer_group`, `bcrd_series`, `bcrd_upload_batches`, `conversational_search_quota` | 0 | vacías |

### Hallazgos que cambian el plan del `.md` original

1. **`pg_dump` completo no aplica.** El grueso del origen son snapshots del
   estado del front (`data_update_log` con `source = 'LIVE_STATE'`,
   `section = 'front_state'`): ~661 filas, hasta 3.5 MB de JSONB cada una,
   ~915 MB en total. El front **solo lee la fila más reciente** de esa
   combinación (`index.html`, `LIVE_SOURCE` / `LIVE_SECTION`). Migrar el
   histórico completo es mover casi un giga que nadie consulta. Por eso el
   script copia solo el último snapshot por defecto.
2. **No hay usuarios de Auth que migrar**, tal como decía el plan — pero sí hay
   consecuencia en los datos: 696 filas de `data_update_log` tienen
   `updated_by = 8c951d50-d763-4554-b28c-d08d8bbbdc68`, un UUID que **no va a
   existir en el destino**. Como la columna es FK a `auth.users(id)`, copiarla
   tal cual reventaría el restore. El script la pone en `NULL` y guarda el UUID
   original en `metadata.legacy_updated_by`.
3. **Storage no requiere copiar objetos**: el bucket está vacío. Solo hay que
   recrear el bucket y sus dos políticas.
4. **`increment_search_quota` está muerta hoy**: `conversational_search_quota`
   tiene RLS activo y **cero políticas**, y la función es `SECURITY INVOKER`, así
   que solo funcionaría llamándola con `service_role`. Nada en este repo la
   invoca. Se replica tal cual (no se "arregla" durante una migración); si
   algún día se usa desde el front, hay que pasarla a `SECURITY DEFINER` — el
   SQL lo deja anotado.
5. **Tres políticas de `news_items` son código muerto**
   (`news_items_public_read_published` y sus dos hermanas): filtran por
   `status = 'published'`, pero el CHECK de la tabla solo admite
   `pending | approved | rejected`. Se replican igual para no cambiar
   comportamiento en medio del corte.

---

## 2. Qué hay en este repo para ejecutar la migración

```
supabase/migrations/20260730000001_observatorio_public_schema.sql   schema + RLS + grants
supabase/migrations/20260730000002_observatorio_storage.sql         bucket + políticas de storage
scripts/migrate_supabase_data.py                                    copia de datos por REST
```

Los dos `.sql` son idempotentes: se pueden correr más de una vez.

`migrate_supabase_data.py` usa la API REST (PostgREST) en vez de `pg_dump`, así
que **no necesita el puerto 5432 abierto** — solo HTTPS y las service keys. Es
idempotente (upsert sobre la clave natural de cada tabla).

---

## 3. Ejecución

### 3.0 Antes de empezar

- Confirmar que el destino corre **Postgres 17.x** (Project Settings → Database).
  Si no coincide la major version, avisar antes de seguir.
- Tener a mano, de cada proyecto: la **service_role key** (Project Settings →
  API) y la **anon key** del destino.
- El origen queda **vivo y en producción** durante todos estos pasos.

### 3.1 Crear el schema en el destino

```bash
export DST_DB="postgresql://postgres:[PASSWORD]@db.btccnnreeansagcduutt.supabase.co:5432/postgres"

psql "$DST_DB" -v ON_ERROR_STOP=1 -f supabase/migrations/20260730000001_observatorio_public_schema.sql
psql "$DST_DB" -v ON_ERROR_STOP=1 -f supabase/migrations/20260730000002_observatorio_storage.sql
```

Si no tienes acceso al 5432 desde donde estás, pega el contenido de cada archivo
en el **SQL Editor** del dashboard del destino y ejecútalos en ese orden.

Verificación:

```sql
select tablename, rowsecurity from pg_tables where schemaname = 'public' order by 1;
-- 9 tablas, rowsecurity = true en todas
select count(*) from pg_policies where schemaname = 'public';  -- 23
select id, public from storage.buckets;                        -- analyst-uploads, false
```

### 3.2 Copiar los datos

```bash
pip install -r requirements.txt

export SRC_SUPABASE_URL="https://albtuqzdcltcokfagdvy.supabase.co"
export SRC_SERVICE_KEY="<service_role key del ORIGEN>"
export DST_SUPABASE_URL="https://btccnnreeansagcduutt.supabase.co"
export DST_SERVICE_KEY="<service_role key del DESTINO>"

python scripts/migrate_supabase_data.py --dry-run   # cuenta filas, no escribe
python scripts/migrate_supabase_data.py             # copia
```

Debe reportar aproximadamente: `news_sources` 17, `news_proposals` 87,
`data_update_log` 73 + 1 (el último `LIVE_STATE`), el resto en 0.

Opciones:

- `--live-state none` — no copiar ningún snapshot del front. Nathali republica
  desde el panel después del corte y listo.
- `--live-state all` — copiar los ~661 snapshots (~915 MB). No recomendado.
- `--tables news_sources,news_items` — copiar solo ciertas tablas.

### 3.3 Reconfigurar secrets y variables

Ninguno de estos se copia solo.

**GitHub Actions** (Settings → Secrets and variables → Actions). Los workflows
`news_pipeline_v2.yml` y `gemini_summaries.yml` los consumen:

| Secret | Valor nuevo |
|---|---|
| `SUPABASE_URL` | `https://btccnnreeansagcduutt.supabase.co` |
| `SUPABASE_SERVICE_KEY` | service_role key del **destino** |
| `GEMINI_API_KEY` | sin cambios |
| `SIB_SUBSCRIPTION_KEY` | sin cambios |

**Vercel** (Project → Settings → Environment Variables): revisar si hay
`SUPABASE_*` definidas y apuntarlas al destino. Las de acceso interno
(`INTERNAL_BYPASS_USER`, `INTERNAL_BYPASS_PASSWORD`, `INTERNAL_SESSION_SECRET`,
usadas por `api/internal-auth.js`) no tienen que ver con Supabase y quedan igual.

**Supabase destino** (Project Settings → Edge Functions → Secrets): el origen no
tiene Edge Functions ni secrets de Vault, así que no hay nada que replicar hoy.

### 3.4 Frontend

El project ref y la anon key ya no están regados por el monolito: viven en dos
`<meta>` del `<head>` de `index.html`.

```html
<meta name="supabase-url" content="https://albtuqzdcltcokfagdvy.supabase.co">
<meta name="supabase-anon-key" content="...">
```

El cutover del front es cambiar esas dos líneas al destino. Para **probar sin
tocar el archivo** (paso 4.1), define antes de que cargue la página:

```html
<script>
  window.__OBSERVATORIO_SUPABASE__ = {
    url: 'https://btccnnreeansagcduutt.supabase.co',
    anonKey: '<anon key del destino>'
  };
</script>
```

`assets/supabase-data-layer.js` lee la misma configuración (hoy `index.html` no
lo importa — es código legado — pero queda consistente).

---

## 4. Plan de corte

1. Pasos 3.1 y 3.2 con el origen **todavía en producción**. Nada cambia para
   Nathali.
2. Levantar un **preview de Vercel** apuntado al destino (con el override
   `window.__OBSERVATORIO_SUPABASE__`, o cambiando los `<meta>` en una rama) y
   validar:
   - Login por Microsoft (Azure/Entra ID) entra y crea la identidad en el destino.
   - Las políticas RLS bloquean/permiten lo esperado. Ojo con lo que dependa de
     `auth.uid()`: **el mismo usuario tiene un UUID distinto en cada proyecto**,
     porque es una identidad nueva, no una fila copiada. Hoy ninguna política
     del Observatorio usa `auth.uid()` (solo `auth.role() = 'authenticated'`),
     así que no debería haber sorpresas — pero hay que verificarlo.
   - El panel del analista puede subir un CSV SIB y escribir en
     `sib_series_long` / `sib_peer_group`.
   - Publicar el estado del front (`LIVE_STATE`) y verlo desde otro dispositivo.
3. Correr el pipeline de noticias **una vez a mano** contra el destino
   (Actions → `news_pipeline_v2` → Run workflow) con los secrets ya cambiados, y
   revisar que escriba en `news_proposals` / `data_update_log` del destino.
4. Solo después de eso: cambiar los `<meta>` de `index.html` en `main` y
   desplegar a producción.
5. Dejar el origen **vivo pero sin tráfico** unos días como fallback. Rollback =
   revertir los dos `<meta>` y los secrets de GitHub. No se borra nada del
   origen en ningún paso de este plan.
6. Cuando esté estable: pausar o archivar el proyecto origen.

---

## 5. Azure / Entra ID

El App Registration ya tiene agregado (sin reemplazar) el Redirect URI del
proyecto destino bajo la plataforma "Web":

```
https://btccnnreeansagcduutt.supabase.co/auth/v1/callback
```

En el **destino**, Authentication → Providers → Azure hay que cargar Client ID,
Client Secret y la URL del tenant:

```
https://login.microsoftonline.com/<TENANT_ID>
```

Sin `/v2.0` al final: Supabase agrega esa parte sola, y si se pone queda
duplicada y Microsoft responde 404. Ver `docs/ACCESO_ENTRA_ID.md`.

**Estas credenciales van en un solo lugar: la configuración del provider Azure
del proyecto destino.** No van como secrets de GitHub — los pipelines de Actions
no hacen SSO y nunca tocan Entra, así que ahí solo serían una copia de más de un
secreto. El Tenant ID se repite en Vercel como `LN_ALLOWED_TENANT_ID` (no es
secreto: es el filtro de quién puede entrar).

**Este repositorio es público.** Ni el client secret ni ninguna otra credencial
pueden escribirse en un archivo versionado, ni siquiera como valor por defecto.
Ver `docs/ACCESO_ENTRA_ID.md` para el detalle de la configuración.

> **Rotar el client secret antes del handoff definitivo.** El secret actual se
> compartió por chat durante la preparación de esta migración, así que hay que
> darlo por quemado: crear uno nuevo en el App Registration, actualizarlo en el
> proyecto destino y en los secrets, y borrar el viejo. Los client secrets de
> Entra ID además expiran — anotar la fecha de expiración del nuevo en algún
> lugar que sobreviva a la salida de Joel.

---

## 6. Lo que este plan deliberadamente NO hace

- No toca los schemas `auth`, `storage`, `realtime`, `vault` ni `extensions` del
  destino (más allá de crear el bucket y sus políticas).
- No migra `auth.users`: el login es 100% SSO y cada persona se crea sola en el
  destino la primera vez que entra por Microsoft.
- No borra ni modifica nada en el proyecto origen.
- No cambia comportamiento de la app: las rarezas encontradas (§1) se replican
  tal cual y quedan documentadas para arreglarlas después del corte, no durante.
