#!/usr/bin/env bash
#
# One-shot comments migration for a collection (run on the migration pod).
#
# Consolidates, in this order:
#   1. pre-flight checks (files from export_collection_comments.py are there,
#      the collection is configured in streams.yaml)
#   2. invenio migration comments commenters-run   (dry run, then real run)
#   3. invenio migration comments run              (dry run, then real run)
#
# Usage:
#   ./scripts/migrate_collection_comments.sh --collection antares_restr
#   ./scripts/migrate_collection_comments.sh --collection antares_restr --yes
#   ./scripts/migrate_collection_comments.sh --collection antares_restr --dry-run-only
#
# The input paths are NOT passed on the command line: the CLI reads them from the
# `comments:` section of streams.yaml (`dir_path`, plus `<dir_path>/users`).
#
# Commenters are indexed by the loader right when they are created, so no manual
# re-indexing is needed. Both runners can be re-run: already created users and
# comments are skipped.

set -euo pipefail

COLLECTION=""
ASSUME_YES=false
DRY_RUN_ONLY=false
STREAMS_FILE="${CDS_MIGRATOR_KIT_STREAM_CONFIG:-}"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --collection) COLLECTION="$2"; shift 2 ;;
        --yes|-y) ASSUME_YES=true; shift ;;
        --dry-run-only) DRY_RUN_ONLY=true; shift ;;
        --streams-file) STREAMS_FILE="$2"; shift 2 ;;
        -h|--help) sed -n '2,19p' "$0"; exit 0 ;;
        *) echo "Unknown argument: $1" >&2; exit 2 ;;
    esac
done

[[ -n "$COLLECTION" ]] || { echo "--collection is required" >&2; exit 2; }

# --- 1. pre-flight checks ---------------------------------------------------
# dir_path of the collection, from the same streams.yaml the runners use.
DIR_PATH="$(python - "$COLLECTION" "$STREAMS_FILE" <<'EOF'
import sys
import yaml
from flask import current_app
from invenio_app.factory import create_app

collection, streams_file = sys.argv[1], sys.argv[2]
if not streams_file:
    with create_app().app_context():
        streams_file = current_app.config["CDS_MIGRATOR_KIT_STREAM_CONFIG"]
with open(streams_file) as fp:
    config = yaml.safe_load(fp)
try:
    print(config["comments"][collection]["dir_path"])
except KeyError:
    sys.exit(f"collection '{collection}' has no entry under `comments:` in {streams_file}")
EOF
)"

echo "Comments directory: $DIR_PATH"
missing=0
for f in "comments_metadata.json" "users/missing_commentors_from_ldap.json"; do
    if [[ ! -f "$DIR_PATH/$f" ]]; then
        echo "MISSING: $DIR_PATH/$f  (run scripts/export_collection_comments.py first)" >&2
        missing=1
    fi
done
[[ -f "$DIR_PATH/users/people.csv" ]] || echo "WARNING: $DIR_PATH/users/people.csv not found"
[[ $missing -eq 0 ]] || exit 1

confirm() {
    $ASSUME_YES && return 0
    read -r -p "$1 [y/N] " answer
    [[ "$answer" =~ ^[Yy]$ ]]
}

# The runners catch and log their own errors, so a dry run exit code can't be
# trusted to tell success from failure: check the logs before confirming.
run_step() {
    local name="$1" cmd="$2"
    echo; echo "=== $name: dry run ==="
    invenio migration comments "$cmd" --collection "$COLLECTION" --dry-run
    $DRY_RUN_ONLY && return 0
    confirm "Dry run of '$name' finished. Check the logs, then run it for real?" \
        || { echo "Stopped before '$name'."; exit 1; }
    echo "=== $name: real run ==="
    invenio migration comments "$cmd" --collection "$COLLECTION"
}

# --- 2. commenters, 3. comments --------------------------------------------
run_step "commenters" commenters-run
run_step "comments" run

echo; echo "Done. Re-run is safe: existing users and comments are skipped."
