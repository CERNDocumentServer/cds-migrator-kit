#!/usr/bin/env bash
# Dump/restore the Postgres database to/from a local SQL file.
# Auto-detects the running Postgres container - override with DB_CONTAINER
# if you have more than one running.
# Usage:
#   scripts/postgres-init.sh dump [dump_file]
#   scripts/postgres-init.sh restore <dump_file>
set -euo pipefail

DB_USER="${POSTGRES_USER:-cds-rdm}"
DB_NAME="${POSTGRES_DB:-cds-rdm}"
ACTION="${1:-}"
DUMP_FILE="${2:-}"

usage() {
  echo "Usage: $0 {dump|restore} <dump_file>" >&2
  exit 1
}

[[ "$ACTION" == "dump" || "$ACTION" == "restore" ]] || usage

if [[ "$ACTION" == "dump" && -z "$DUMP_FILE" ]]; then
  DUMP_FILE="dump_$(date +%Y%m%d_%H%M%S).sql"
fi
[[ -n "$DUMP_FILE" ]] || usage

detect_container() {
  local matches
  # Match by image name prefix (e.g. "postgres:14", "postgres:16-alpine"),
  # not docker's --filter ancestor=, which requires an exact repo:tag match.
  matches="$(docker ps --format '{{.ID}}\t{{.Image}}\t{{.Names}}' | awk -F'\t' '$2 ~ /^postgres(:|$)/')"
  if [[ -z "$matches" ]]; then
    echo "No running Postgres container found (looked for image 'postgres*')." >&2
    echo "Start it, or set DB_CONTAINER explicitly." >&2
    exit 1
  fi
  if [[ "$(echo "$matches" | wc -l)" -gt 1 ]]; then
    echo "Multiple Postgres containers found, using the first one:" >&2
    echo "$matches" >&2
    echo "Set DB_CONTAINER to pick a different one." >&2
  fi
  echo "$matches" | head -n1 | cut -f1
}

CONTAINER_ID="${DB_CONTAINER:-$(detect_container)}"
CONTAINER_NAME="$(docker inspect --format '{{.Name}}' "$CONTAINER_ID" 2>/dev/null | sed 's|^/||')"
echo "Using Postgres container: $CONTAINER_ID (${CONTAINER_NAME:-unknown})"

case "$ACTION" in
  dump)
    echo "Dumping database '$DB_NAME' to $DUMP_FILE ..."
    docker exec "$CONTAINER_ID" pg_dump -U "$DB_USER" -d "$DB_NAME" >"$DUMP_FILE"
    ;;
  restore)
    [[ -f "$DUMP_FILE" ]] || { echo "Dump file not found: $DUMP_FILE" >&2; exit 1; }

    if [[ "${FORCE:-}" != "1" ]]; then
      read -r -p "This will DESTROY the current database '$DB_NAME' and restore it from $DUMP_FILE. Continue? [y/N] " reply
      [[ "$reply" =~ ^[Yy]$ ]] || { echo "Aborted."; exit 1; }
    fi

    echo "Recreating database schema ..."
    invenio db destroy --yes-i-know
    invenio db init

    echo "Restoring $DUMP_FILE into database '$DB_NAME' ..."
    docker exec -i "$CONTAINER_ID" psql -U "$DB_USER" -d "$DB_NAME" <"$DUMP_FILE"
    ;;
esac

echo "Done."
