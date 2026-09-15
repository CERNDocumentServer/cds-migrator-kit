"""Regenerate ``rdm_records_state.json`` straight from the CDS-RDM database.

Root cause - previously, the code had a bug:

    for version in versions.keys():
        draft = self._pre_publish(...)
        published_record = current_rdm_records_service.publish(...)
        self._after_publish(...)
    records.append(published_record._record)   # left outside the loop

It ran exactly once per record, for only the latest version. Now fixed in code already.

``invenio migration stats run`` uses only the ``versions[]`` in that state
file to attribute downloads, so any multi-version record migrated in that
window lost every download event belonging to any version except the latest.

This script rebuilds the state file from scratch, entirely from CDS-RDM,
one stream/collection at a time - the same collection name used everywhere
else (``invenio migration run --collection <name>``, and the folder name
under ``cds_migrator_kit/rdm/data/`` and ``<CDS_MIGRATOR_KIT_LOGS_PATH>/``):

1. Resolve the collection's community id(s) from ``streams.yaml`` /
   ``streams_done.yaml`` / ``streams_shelved.yaml`` (same as
   cds_migrator_kit/runner/runner.py).
2. Resolve those community ids to every parent record in them using ``RDMParentCommunity``.
3. Resolve each parent to its legacy recid via the ``lrecid`` PID minted at migration time.
4. Fetch every published version for that parent.
5. Rebuild the state entry field-for-field like ``_load_record_state`` does,
   reading current file metadata straight off each version.

Output is written next to the original, as ``<collection>/rdm_records_state.fixed.json``.

On restart the script reads its own log file for DONE lines and skips any
legacy recid already completed.

Usage:
    invenio shell scripts/rebuild_records_state.py

    main(collection="it", dry_run=True)
    main(collection="it", dry_run=False)
"""

import json
import traceback
from pathlib import Path

import yaml
from flask import current_app
from invenio_pidstore.models import PersistentIdentifier
from invenio_rdm_records.records.api import RDMRecord
from invenio_rdm_records.records.models import RDMParentCommunity, RDMRecordMetadata

STREAM_CONFIG_FILES = (
    "cds_migrator_kit/rdm/streams_done.yaml",
    "cds_migrator_kit/rdm/streams.yaml",
    "cds_migrator_kit/rdm/streams_shelved.yaml",
)

log_fp = None


def log(msg):
    print(msg)
    if log_fp is not None:
        log_fp.write(msg + "\n")
        log_fp.flush()


def load_completed_recids(log_path):
    """Return the set of legacy recids already marked DONE in a previous run."""
    completed = set()
    path = Path(log_path)
    if not path.exists():
        return completed
    with open(log_path, "r") as f:
        for line in f:
            if line.startswith("DONE: legacy_recid="):
                completed.add(line.strip().split("=")[1])
    return completed


def mark_done(legacy_recid):
    log(f"DONE: legacy_recid={legacy_recid}")


def load_communities_ids(collection, config_files=STREAM_CONFIG_FILES):
    """Look up a collection's ``transform.communities_ids`` in streams.yaml.

    Reads the same shape ``Runner._read_config()`` does
    (cds_migrator_kit/runner/runner.py:28-31): a top-level ``records`` key,
    one entry per collection name.
    """
    for path in config_files:
        if not Path(path).exists():
            continue
        with open(path) as f:
            config = yaml.safe_load(f) or {}
        collection_config = config.get("records", {}).get(collection)
        if collection_config:
            return collection_config["transform"]["communities_ids"]
    raise ValueError(
        f"collection {collection!r} not found in any of {config_files} - "
        "pass its community id(s) directly via community_ids= instead"
    )


def default_output_path(collection):
    """Same folder ``RecordStateLogger`` writes to (cds_migrator_kit/reports/log.py),
    a different filename - this never overwrites ``rdm_records_state.json``."""
    base_path = current_app.config["CDS_MIGRATOR_KIT_LOGS_PATH"]
    return str(Path(base_path) / collection / "rdm_records_state.fixed.json")


def find_legacy_recids(community_ids):
    """Return {legacy_recid: parent_object_uuid} for every migrated record in
    any of the given communities.

    Two plain queries, nothing read from disk: community ids -> parent
    uuids (``RDMParentCommunity``), then parent uuids -> legacy recid via
    the ``lrecid`` PID (mirrors the lookup
    ``CDSMigrationEntryLoad._have_migrated_recid`` does one recid at a time,
    cds_migrator_kit/rdm/records/load/load.py).
    """
    parent_uuids = [
        row.record_id
        for row in RDMParentCommunity.query.filter(
            RDMParentCommunity.community_id.in_(community_ids)
        )
    ]
    pids = PersistentIdentifier.query.filter(
        PersistentIdentifier.pid_type == "lrecid",
        PersistentIdentifier.object_uuid.in_(parent_uuids),
    )
    return {pid.pid_value: str(pid.object_uuid) for pid in pids}


def get_rdm_versions(parent_object_uuid):
    """Return {version_index: RDMRecord} for every published version of a parent.

    Soft-deleted records are excluded by ``RDMRecord.get_record()`` default.
    """
    version_models = (
        RDMRecordMetadata.query.filter_by(parent_id=parent_object_uuid)
        .order_by(RDMRecordMetadata.created)
        .all()
    )
    records = {}
    for m in version_models:
        try:
            rdm_record = RDMRecord.get_record(str(m.id))
            records[rdm_record.versions.index] = rdm_record
        except Exception as exc:
            log(f"could not load record {m.id}: {exc}")
    return records


def convert_file_format(file_entries, bucket_id):
    """Mirror ``RecordLoad._load_record_state.convert_file_format``."""
    return [
        {
            "legacy_file_id": entry["metadata"]["legacy_file_id"],
            "bucket_id": bucket_id,
            "file_key": entry["key"],
            "file_id": entry["file_id"],
            "size": str(entry["size"]),
        }
        for entry in file_entries.values()
    ]


def extract_record_version(record):
    """Mirror ``RecordLoad._load_record_state.extract_record_version``."""
    bucket_id = str(record.files.bucket_id)
    files = record.__class__.files.dump(
        record, record.files, include_entries=True
    ).get("entries", {})
    return {
        "new_recid": record.pid.pid_value,
        "version": record.versions.index,
        "files": convert_file_format(files, bucket_id),
    }


def build_state_entry(legacy_recid, rdm_versions):
    """Rebuild one ``rdm_records_state.json`` entry from live RDM versions.

    Mirrors ``RecordLoad._load_record_state``
    (cds_migrator_kit/rdm/records/load/entities/record.py) field-for-field,
    reading current per-version file metadata straight off each version
    instead of relying on the state the buggy loop bookkeeping produced.
    """
    recid_state = {"legacy_recid": str(legacy_recid), "versions": []}
    parent_recid = None

    for version_index in sorted(rdm_versions):
        record = rdm_versions[version_index]

        if parent_recid is None:
            parent_recid = record.parent.pid.pid_value
            recid_state["parent_recid"] = parent_recid
            recid_state["parent_object_uuid"] = str(record.parent.id)

        recid_state["versions"].append(extract_record_version(record))

        if "latest_version" not in recid_state:
            latest = record.get_latest_by_parent(record.parent)
            recid_state["latest_version"] = latest["id"]
            recid_state["latest_version_object_uuid"] = str(latest.id)

    return recid_state


def write_state_file(filepath, entries):
    """Write entries in the same JSON-list-of-compact-objects format
    ``RecordStateLogger.finalise()`` uses (cds_migrator_kit/reports/log.py),
    which is what ``invenio migration stats run --filepath`` expects."""
    Path(filepath).parent.mkdir(parents=True, exist_ok=True)
    with open(filepath, "w", encoding="utf-8") as f:
        f.write("[\n")
        for i, entry in enumerate(entries):
            json_str = json.dumps(entry, ensure_ascii=False, separators=(",", ":"))
            comma = "," if i < len(entries) - 1 else ""
            f.write(f"{json_str}{comma}\n")
        f.write("]")


def main(collection, dry_run=True, output_path=None):
    """Rebuild ``rdm_records_state.json`` for one stream/collection.

    :param collection: the stream name, as used by
        ``invenio migration run --collection`` - looked up in streams.yaml
        to find the community id(s) and to build the default output path.
    :param output_path: defaults to
        ``<CDS_MIGRATOR_KIT_LOGS_PATH>/<collection>/rdm_records_state.fixed.json``,
        next to (never over) the original state file.
    """
    global log_fp

    community_ids = load_communities_ids(collection)
    output_path = output_path or default_output_path(collection)
    log_file = f"{output_path}.log"

    completed_recids = load_completed_recids(log_file)
    log_fp = open(log_file, "a")

    if completed_recids:
        log(f"resuming — {len(completed_recids)} recid(s) already completed, skipping them")

    try:
        legacy_recids = find_legacy_recids(community_ids)
        log(
            f"starting [collection={collection}, community_ids={community_ids}, "
            f"dry_run={dry_run}, legacy_recids={len(legacy_recids)}]"
        )

        stats = {"checked": 0, "skipped_done": 0, "no_versions": 0, "fixed": 0, "errors": 0}
        entries = []

        for i, (legacy_recid, parent_object_uuid) in enumerate(legacy_recids.items(), start=1):
            stats["checked"] += 1

            if legacy_recid in completed_recids:
                stats["skipped_done"] += 1
                continue

            log(f"[{i}/{len(legacy_recids)}] legacy_recid={legacy_recid}")

            try:
                rdm_versions = get_rdm_versions(parent_object_uuid)
                if not rdm_versions:
                    log(f"legacy_recid={legacy_recid} - no published versions found, skipping")
                    stats["no_versions"] += 1
                    continue

                entries.append(build_state_entry(legacy_recid, rdm_versions))
                stats["fixed"] += 1

                if not dry_run:
                    mark_done(legacy_recid)

            except Exception as exc:
                log(f"unexpected error for legacy_recid={legacy_recid}: {exc}")
                log(traceback.format_exc())
                stats["errors"] += 1

        if not dry_run:
            write_state_file(output_path, entries)
            log(f"wrote {output_path}")
        else:
            log(f"dry run — would write {output_path}")

        log(
            f"\nsummary:\nchecked={stats['checked']}\nalready_done={stats['skipped_done']}\n"
            f"no_versions={stats['no_versions']}\nfixed={stats['fixed']}\n"
            f"errors={stats['errors']}"
        )
    finally:
        log_fp.close()
        log_fp = None
