#!/usr/bin/env python3
"""
Observatorio Estratégico La Nacional — copia de datos entre proyectos Supabase
=============================================================================

Migración "Ruta B": el schema se crea en el destino con los archivos de
`supabase/migrations/`, y este script copia los DATOS del schema `public` del
proyecto origen al destino usando la API REST (PostgREST). No necesita acceso
directo al puerto 5432, solo HTTPS y las service keys de ambos proyectos.

Uso
---
    export SRC_SUPABASE_URL="https://albtuqzdcltcokfagdvy.supabase.co"
    export SRC_SERVICE_KEY="<service_role key del ORIGEN>"
    export DST_SUPABASE_URL="https://btccnnreeansagcduutt.supabase.co"
    export DST_SERVICE_KEY="<service_role key del DESTINO>"

    python scripts/migrate_supabase_data.py --dry-run   # inspecciona, no escribe
    python scripts/migrate_supabase_data.py             # copia de verdad
    python scripts/migrate_supabase_data.py --tables news_sources,news_items

Decisiones de migración (ver docs/MIGRACION_SUPABASE.md)
-------------------------------------------------------
* Las columnas que son FK a `auth.users` (`updated_by`, `created_by`,
  `approved_by`, `rejected_by`) se copian como NULL. Los usuarios de Auth NO se
  migran: el login es 100% SSO por Azure/Entra ID y cada persona obtiene un
  `auth.uid()` nuevo la primera vez que entra al proyecto destino, así que el
  UUID viejo no existiría en el destino y rompería la FK. El UUID original se
  preserva dentro de `metadata.legacy_updated_by` para no perder la trazabilidad.
* `data_update_log` con `source = 'LIVE_STATE'` es el mecanismo de publicación
  del estado del front: el front solo lee la fila MÁS RECIENTE de
  (LIVE_STATE, front_state). En el origen hay ~660 snapshots históricos que
  suman ~915 MB y no los lee nadie. Por defecto se copia solo la última fila
  (`--live-state latest`). Alternativas: `--live-state none` / `--live-state all`.
* La copia es idempotente: usa upsert sobre la clave natural de cada tabla.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from typing import Any, Iterable

import httpx

# ---------------------------------------------------------------------------
# Configuración por tabla
# ---------------------------------------------------------------------------
# on_conflict: clave natural usada para el upsert en el destino.
# auth_fk:     columnas que apuntan a auth.users y hay que neutralizar.
# order_by:    orden estable para paginar.
TABLES: dict[str, dict[str, Any]] = {
    "news_sources": {
        "on_conflict": "source_key",
        "auth_fk": [],
        "order_by": "created_at",
    },
    "news_items": {
        "on_conflict": "url",
        "auth_fk": ["approved_by", "rejected_by"],
        "order_by": "created_at",
    },
    "news_proposals": {
        "on_conflict": "url",
        "auth_fk": [],
        "order_by": "fetched_at",
    },
    "sib_series_long": {
        "on_conflict": "periodo,entidad,indicador",
        "auth_fk": ["updated_by"],
        "order_by": "periodo",
    },
    "sib_peer_group": {
        "on_conflict": "periodo,entidad",
        "auth_fk": ["updated_by"],
        "order_by": "periodo",
    },
    "bcrd_series": {
        "on_conflict": "serie_id,periodo",
        "auth_fk": ["updated_by"],
        "order_by": "periodo",
    },
    "bcrd_upload_batches": {
        "on_conflict": "id",
        "auth_fk": ["created_by"],
        "order_by": "created_at",
    },
    "data_update_log": {
        "on_conflict": "id",
        "auth_fk": ["updated_by"],
        "order_by": "updated_at",
    },
    "conversational_search_quota": {
        "on_conflict": "search_date",
        "auth_fk": [],
        "order_by": "search_date",
    },
}

# El orden importa: news_items tiene FK a news_sources(source_key).
TABLE_ORDER = [
    "news_sources",
    "news_items",
    "news_proposals",
    "sib_series_long",
    "sib_peer_group",
    "bcrd_series",
    "bcrd_upload_batches",
    "conversational_search_quota",
    "data_update_log",
]

LIVE_SOURCE = "LIVE_STATE"
PAGE_SIZE = 500
WRITE_CHUNK = 200
TIMEOUT = httpx.Timeout(120.0, connect=30.0)


def env(name: str) -> str:
    value = (os.environ.get(name) or "").strip()
    if not value:
        sys.exit(f"Falta la variable de entorno {name}.")
    return value.rstrip("/") if name.endswith("_URL") else value


def headers(key: str, extra: dict[str, str] | None = None) -> dict[str, str]:
    base = {
        "apikey": key,
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
    }
    if extra:
        base.update(extra)
    return base


# ---------------------------------------------------------------------------
# Lectura del origen
# ---------------------------------------------------------------------------
def fetch_rows(
    client: httpx.Client,
    url: str,
    key: str,
    table: str,
    order_by: str,
    extra_params: dict[str, str] | None = None,
    limit: int | None = None,
) -> Iterable[list[dict]]:
    """Pagina la tabla del origen y va entregando lotes."""
    offset = 0
    while True:
        page = PAGE_SIZE if limit is None else min(PAGE_SIZE, limit - offset)
        if page <= 0:
            return
        params = {"select": "*", "order": f"{order_by}.desc"}
        if extra_params:
            params.update(extra_params)
        response = client.get(
            f"{url}/rest/v1/{table}",
            headers=headers(key, {"Range-Unit": "items", "Range": f"{offset}-{offset + page - 1}"}),
            params=params,
        )
        response.raise_for_status()
        rows = response.json()
        if not rows:
            return
        yield rows
        if len(rows) < page:
            return
        offset += len(rows)


def count_rows(client: httpx.Client, url: str, key: str, table: str,
               extra_params: dict[str, str] | None = None) -> int:
    params = {"select": "id"} if table != "conversational_search_quota" else {"select": "search_date"}
    if extra_params:
        params.update(extra_params)
    response = client.get(
        f"{url}/rest/v1/{table}",
        headers=headers(key, {"Prefer": "count=exact", "Range": "0-0"}),
        params=params,
    )
    response.raise_for_status()
    content_range = response.headers.get("content-range", "*/0")
    try:
        return int(content_range.split("/")[-1])
    except ValueError:
        return 0


# ---------------------------------------------------------------------------
# Transformación
# ---------------------------------------------------------------------------
def scrub(row: dict, auth_fk: list[str]) -> dict:
    """Neutraliza las FKs a auth.users, dejando rastro en metadata."""
    out = dict(row)
    legacy: dict[str, str] = {}
    for column in auth_fk:
        value = out.get(column)
        if value:
            legacy[f"legacy_{column}"] = value
        out[column] = None
    if legacy:
        metadata = out.get("metadata")
        if not isinstance(metadata, dict):
            metadata = {} if metadata is None else {"_original": metadata}
        metadata.update(legacy)
        out["metadata"] = metadata
    return out


# ---------------------------------------------------------------------------
# Escritura en el destino
# ---------------------------------------------------------------------------
def upsert(client: httpx.Client, url: str, key: str, table: str,
           on_conflict: str, rows: list[dict]) -> int:
    written = 0
    for start in range(0, len(rows), WRITE_CHUNK):
        chunk = rows[start:start + WRITE_CHUNK]
        response = client.post(
            f"{url}/rest/v1/{table}",
            headers=headers(key, {
                "Prefer": "resolution=merge-duplicates,return=minimal",
                "Content-Profile": "public",
            }),
            params={"on_conflict": on_conflict},
            content=json.dumps(chunk, default=str),
        )
        if response.status_code not in (200, 201, 204):
            raise RuntimeError(
                f"Fallo al escribir en {table} ({response.status_code}): {response.text[:600]}"
            )
        written += len(chunk)
    return written


# ---------------------------------------------------------------------------
# Migración de una tabla
# ---------------------------------------------------------------------------
def migrate_table(src: httpx.Client, dst: httpx.Client, args, table: str,
                  src_url: str, src_key: str, dst_url: str, dst_key: str) -> None:
    config = TABLES[table]
    extra_params: dict[str, str] | None = None
    limit: int | None = None
    note = ""

    if table == "data_update_log":
        if args.live_state == "none":
            extra_params = {"source": f"neq.{LIVE_SOURCE}"}
            note = " (sin filas LIVE_STATE)"
        elif args.live_state == "latest":
            # Primero todo lo que no es LIVE_STATE, luego solo el último snapshot.
            migrate_slice(src, dst, args, table, src_url, src_key, dst_url, dst_key,
                          {"source": f"neq.{LIVE_SOURCE}"}, None, " (histórico no-LIVE)")
            migrate_slice(src, dst, args, table, src_url, src_key, dst_url, dst_key,
                          {"source": f"eq.{LIVE_SOURCE}"}, 1, " (último snapshot LIVE_STATE)")
            return
        else:
            note = " (TODOS los snapshots LIVE_STATE — puede ser ~1 GB)"

    migrate_slice(src, dst, args, table, src_url, src_key, dst_url, dst_key,
                  extra_params, limit, note)


def migrate_slice(src: httpx.Client, dst: httpx.Client, args, table: str,
                  src_url: str, src_key: str, dst_url: str, dst_key: str,
                  extra_params: dict[str, str] | None, limit: int | None,
                  note: str) -> None:
    config = TABLES[table]
    total = count_rows(src, src_url, src_key, table, extra_params)
    if limit is not None:
        total = min(total, limit)

    if args.dry_run:
        print(f"  [dry-run] {table}{note}: {total} filas en el origen")
        return

    copied = 0
    for batch in fetch_rows(src, src_url, src_key, table, config["order_by"],
                            extra_params, limit):
        cleaned = [scrub(row, config["auth_fk"]) for row in batch]
        copied += upsert(dst, dst_url, dst_key, table, config["on_conflict"], cleaned)
        print(f"  {table}{note}: {copied}/{total}", end="\r", flush=True)
    print(f"  {table}{note}: {copied}/{total} filas copiadas" + " " * 20)


def main() -> int:
    parser = argparse.ArgumentParser(description="Copia datos de un proyecto Supabase a otro.")
    parser.add_argument("--tables", help="Lista separada por comas. Por defecto, todas.")
    parser.add_argument("--dry-run", action="store_true",
                        help="Solo cuenta filas, no escribe nada en el destino.")
    parser.add_argument("--live-state", choices=["latest", "none", "all"], default="latest",
                        help="Qué hacer con data_update_log source=LIVE_STATE (default: latest).")
    args = parser.parse_args()

    src_url = env("SRC_SUPABASE_URL")
    src_key = env("SRC_SERVICE_KEY")
    dst_url = env("DST_SUPABASE_URL")
    dst_key = env("DST_SERVICE_KEY")

    if src_url == dst_url:
        sys.exit("SRC_SUPABASE_URL y DST_SUPABASE_URL son iguales. Abortando.")

    selected = TABLE_ORDER
    if args.tables:
        requested = [t.strip() for t in args.tables.split(",") if t.strip()]
        unknown = [t for t in requested if t not in TABLES]
        if unknown:
            sys.exit(f"Tablas desconocidas: {', '.join(unknown)}")
        selected = [t for t in TABLE_ORDER if t in requested]

    print(f"Origen  : {src_url}")
    print(f"Destino : {dst_url}")
    print(f"Modo    : {'DRY-RUN' if args.dry_run else 'ESCRITURA'} | live-state={args.live_state}")
    print()

    with httpx.Client(timeout=TIMEOUT) as src, httpx.Client(timeout=TIMEOUT) as dst:
        for table in selected:
            migrate_table(src, dst, args, table, src_url, src_key, dst_url, dst_key)

    print("\nListo.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
