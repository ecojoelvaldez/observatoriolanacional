#!/usr/bin/env bash
# =====================================================================
# Commit y push de archivos GENERADOS, sin rebase y sin conflictos
# =====================================================================
#
# Problema que resuelve. Los workflows terminaban así:
#
#     git add archivo && git commit -m "..." && git pull --rebase && git push
#
# El rebase reproduce el commit local encima del remoto. Si entre el
# checkout y el push alguien más tocó el mismo archivo —otra corrida del
# mismo pipeline, el pipeline de Gemini, o un push a mano— el replay
# choca y el job muere con:
#
#     error: could not apply <sha>... Actualizar candidatos de noticias
#
# Un conflicto ahí no tiene sentido: estos archivos no se editan a mano,
# se regeneran completos en cada corrida. La versión recién generada
# siempre es la buena, no hay nada que fusionar.
#
# Estrategia. En vez de rebasar, se sincroniza con el remoto y se vuelve
# a escribir el archivo generado encima:
#
#     1. Se guardan los archivos generados fuera del árbol de trabajo.
#     2. fetch + reset --hard al remoto (el árbol queda limpio).
#     3. Se restauran los archivos generados.
#     4. commit + push. Si el push pierde la carrera contra otro job,
#        se repite el ciclo con espera creciente.
#
# Así el push nunca conflictúa y, si dos corridas coinciden, gana la que
# termina de último, que es la que trae el dato más fresco.
#
# Uso:
#     scripts/commit_generated.sh "Mensaje del commit" archivo [archivo...]
#
# Variables usadas: GITHUB_REF_NAME (rama destino). Si no está definida,
# se toma la rama actual.
#
# Salida para el workflow: escribe "pushed=true|false" en $GITHUB_OUTPUT,
# de modo que un paso posterior pueda condicionar sobre si hubo cambios.
# =====================================================================

set -euo pipefail

if [ "$#" -lt 2 ]; then
  echo "Uso: $0 \"mensaje\" archivo [archivo...]" >&2
  exit 2
fi

MENSAJE="$1"
shift
ARCHIVOS=("$@")

BRANCH="${GITHUB_REF_NAME:-$(git rev-parse --abbrev-ref HEAD)}"
INTENTOS="${COMMIT_PUSH_RETRIES:-5}"

reportar() {
  if [ -n "${GITHUB_OUTPUT:-}" ]; then
    echo "pushed=$1" >> "$GITHUB_OUTPUT"
  fi
}

# ¿Hay algo que commitear? Si el pipeline regeneró un archivo idéntico al
# que ya está en el repo, no se toca la historia.
if [ -z "$(git status --porcelain -- "${ARCHIVOS[@]}")" ]; then
  echo "Sin cambios en: ${ARCHIVOS[*]}"
  reportar false
  exit 0
fi

git config user.name "github-actions[bot]"
git config user.email "41898282+github-actions[bot]@users.noreply.github.com"

# Copia de resguardo fuera del árbol: el reset --hard del paso 2 borraría
# los archivos generados si no se guardan antes.
RESPALDO="$(mktemp -d)"
trap 'rm -rf "$RESPALDO"' EXIT

for archivo in "${ARCHIVOS[@]}"; do
  if [ -f "$archivo" ]; then
    mkdir -p "$RESPALDO/$(dirname "$archivo")"
    cp "$archivo" "$RESPALDO/$archivo"
  fi
done

for intento in $(seq 1 "$INTENTOS"); do
  echo "--- Intento $intento de $INTENTOS (rama $BRANCH)"

  git fetch origin "$BRANCH"
  git reset --hard "origin/$BRANCH"

  # Se vuelve a poner el dato generado encima del remoto ya actualizado.
  for archivo in "${ARCHIVOS[@]}"; do
    if [ -f "$RESPALDO/$archivo" ]; then
      mkdir -p "$(dirname "$archivo")"
      cp "$RESPALDO/$archivo" "$archivo"
    fi
  done

  # Otra corrida pudo haber subido exactamente el mismo contenido.
  if [ -z "$(git status --porcelain -- "${ARCHIVOS[@]}")" ]; then
    echo "El remoto ya tiene este contenido. Nada que subir."
    reportar false
    exit 0
  fi

  git add -- "${ARCHIVOS[@]}"
  git commit -m "$MENSAJE"

  if git push origin "HEAD:$BRANCH"; then
    echo "Push completado en el intento $intento."
    reportar true
    exit 0
  fi

  # Perdió la carrera: alguien más empujó entre el fetch y el push.
  espera=$((intento * 5))
  echo "El push fue rechazado. Reintentando en ${espera}s…" >&2
  sleep "$espera"
done

echo "No se pudo empujar ${ARCHIVOS[*]} tras $INTENTOS intentos." >&2
exit 1
