#!/usr/bin/env bash
# Dump/restore Postgres + OpenSearch together as one named snapshot, so both
# stay tied to the same point in time instead of drifting apart.
#
# Usage:
#   scripts/snapshot.sh dump <name>
#   scripts/snapshot.sh restore <name>
#
# Layout for a snapshot named "before-ep-approval-fix":
#   snapshots/before-ep-approval-fix/db.sql
#   snapshots/before-ep-approval-fix/opensearch/*.json
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SNAPSHOTS_ROOT="${SNAPSHOTS_ROOT:-snapshots}"

# opensearch-init.sh's restore path runs `invenio index destroy`/`invenio
# index init`, which need the actual app (cds-rdm) virtualenv, not whatever
# happens to be active in the caller's shell (e.g. cds-migrator-kit's own).
CDS_RDM_VENV_ACTIVATE="${CDS_RDM_VENV_ACTIVATE:-$SCRIPT_DIR/../../cds-rdm/.venv/bin/activate}"

ACTION="${1:-}"
NAME="${2:-}"

usage() {
  echo "Usage: $0 {dump|restore} <name>" >&2
  exit 1
}

[[ "$ACTION" == "dump" || "$ACTION" == "restore" ]] || usage
[[ -n "$NAME" ]] || usage

[[ -f "$CDS_RDM_VENV_ACTIVATE" ]] || {
  echo "cds-rdm virtualenv not found at $CDS_RDM_VENV_ACTIVATE" >&2
  echo "Set CDS_RDM_VENV_ACTIVATE to its activate script if cds-rdm lives elsewhere." >&2
  exit 1
}

run_opensearch_init() {
  # Subshell: activate cds-rdm's venv only for this call, without leaking it
  # into the rest of snapshot.sh (postgres-init.sh needs no app venv at all).
  (
    # shellcheck disable=SC1090
    source "$CDS_RDM_VENV_ACTIVATE"
    "$SCRIPT_DIR/opensearch-init.sh" "$@"
  )
}

SNAPSHOT_DIR="$SNAPSHOTS_ROOT/$NAME"
DB_FILE="$SNAPSHOT_DIR/db.sql"
OS_DIR="$SNAPSHOT_DIR/opensearch"

case "$ACTION" in
  dump)
    mkdir -p "$SNAPSHOT_DIR"
    echo "=== Snapshot '$NAME': dumping Postgres ==="
    "$SCRIPT_DIR/postgres-init.sh" dump "$DB_FILE"
    echo "=== Snapshot '$NAME': dumping OpenSearch ==="
    run_opensearch_init dump "$OS_DIR"
    echo "Snapshot '$NAME' written to $SNAPSHOT_DIR"
    ;;
  restore)
    [[ -f "$DB_FILE" ]] || { echo "Snapshot db dump not found: $DB_FILE" >&2; exit 1; }
    [[ -d "$OS_DIR" ]] || { echo "Snapshot OpenSearch dump not found: $OS_DIR" >&2; exit 1; }

    if [[ "${FORCE:-}" != "1" ]]; then
      read -r -p "This will DESTROY the current database and OpenSearch indices and restore snapshot '$NAME'. Continue? [y/N] " reply
      [[ "$reply" =~ ^[Yy]$ ]] || { echo "Aborted."; exit 1; }
    fi

    echo "=== Snapshot '$NAME': restoring Postgres + OpenSearch in parallel ==="
    (FORCE=1 "$SCRIPT_DIR/postgres-init.sh" restore "$DB_FILE" 2>&1 | sed -u 's/^/[postgres]  /') &
    pg_pid=$!
    (run_opensearch_init restore "$OS_DIR" 2>&1 | sed -u 's/^/[opensearch] /') &
    os_pid=$!

    pg_status=0
    os_status=0
    wait "$pg_pid" || pg_status=$?
    wait "$os_pid" || os_status=$?

    if [[ "$pg_status" -ne 0 || "$os_status" -ne 0 ]]; then
      echo "Restore failed (postgres exit=$pg_status, opensearch exit=$os_status)" >&2
      exit 1
    fi
    echo "Snapshot '$NAME' restored."
    ;;
esac

echo "Done."
