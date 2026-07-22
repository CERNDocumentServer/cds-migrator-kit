#!/usr/bin/env bash
# Dump/restore OpenSearch index *data* to/from local JSON files.
#
# Mappings/settings/aliases are NOT dumped or restored from file: they are
# defined by the Invenio code (index templates) and are recreated with
# `invenio index init`, which is the source of truth - like a DB schema
# migration. Restoring a stale dumped mapping instead has caused field-type
# mismatches (e.g. a keyword field coming back as text), breaking
# aggregations/facets with a "Misconfigured search" error.
#
# Usage:
#   scripts/opensearch-init.sh dump [output_dir]
#   scripts/opensearch-init.sh restore [input_dir]
set -euo pipefail

HOST="${OPENSEARCH_HOST:-http://127.0.0.1:9200}"
ACTION="${1:-}"
DIR="${2:-dumps}"

# Dump filenames look like "<index-name-at-dump-time>-<run-suffix>.json",
# where <run-suffix> is a long numeric id multielasticdump appends per run
# (not part of the real index/alias name). Physical index names get a new
# numeric suffix each time `invenio index init` (re)creates them, so on
# restore we resolve each dump file back to whatever alias it belongs to,
# then to whichever real index that alias currently points to.
SKIP_PATTERNS='-percolators\.json$|^top_queries-'

usage() {
  echo "Usage: $0 {dump|restore} [dir]" >&2
  exit 1
}

[[ "$ACTION" == "dump" || "$ACTION" == "restore" ]] || usage

if ! command -v elasticdump &>/dev/null; then
  echo "elasticdump not found. Install it with: npm install -g elasticdump" >&2
  exit 1
fi

resolve_real_index() {
  local alias_guess="$1"
  curl -s "$HOST/_alias/$alias_guess" | python3 -c "
import json, sys
try:
    d = json.load(sys.stdin)
    print(list(d.keys())[0])
except Exception:
    print('')
"
}

case "$ACTION" in
  dump)
    mkdir -p "$DIR"
    echo "Dumping index data (no mapping/settings/alias) from $HOST to $DIR ..."
    # Not using multielasticdump here: it discovers indices via GET
    # /_aliases, then checks `'error' in response` to detect an ES-style
    # error payload. If the cluster happens to have a real index literally
    # named "error" (e.g. invenio-logging's error log index), that check
    # false-positives on the alias listing itself and multielasticdump exits
    # before dumping anything. /_cat/indices isn't keyed by index name, so
    # it's immune to that collision - dump each real index individually
    # with plain elasticdump instead, which produces the same per-index
    # files multielasticdump would have.
    indices="$(curl -sf "$HOST/_cat/indices?h=index" | sort)"
    if [[ -z "$indices" ]]; then
      echo "No indices found at $HOST" >&2
      exit 1
    fi
    while IFS= read -r index; do
      [[ -n "$index" ]] || continue
      echo "=== Dumping $index ==="
      elasticdump --input="$HOST/$index" --output="$DIR/$index.json" --type=data
    done <<<"$indices"
    ;;
  restore)
    [[ -d "$DIR" ]] || { echo "Dump directory not found: $DIR" >&2; exit 1; }

    echo "Rebuilding indices from current Invenio mappings ..."
    invenio index destroy --force --yes-i-know
    invenio index init

    echo "Restoring data from $DIR into the freshly-mapped indices ..."
    cd "$DIR"
    for f in *.json; do
      [[ -e "$f" ]] || continue
      grep -qE "$SKIP_PATTERNS" <<<"$f" && { echo "Skipping $f (not alias-managed)"; continue; }
      [[ "$(stat -f%z "$f" 2>/dev/null || stat -c%s "$f")" -eq 0 ]] && continue

      base="${f%.json}"
      alias_guess="$(sed -E 's/-[0-9]{6,}$//' <<<"$base")"
      real_index="$(resolve_real_index "$alias_guess")"

      if [[ -z "$real_index" ]]; then
        # Not an aliased index (e.g. stats/events indices use a literal
        # name) - fall back to the same name if it already exists.
        if curl -sf -o /dev/null "$HOST/$alias_guess"; then
          real_index="$alias_guess"
        else
          echo "Skipping $f (no matching alias or index for $alias_guess)"
          continue
        fi
      fi

      echo "=== Restoring $f -> $real_index ==="
      elasticdump --input="$f" --output="$HOST/$real_index" --type=data
    done
    ;;
esac

echo "Done."
