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

This script rebuilds the state file from scratch, entirely from CDS-RDM, for
a given collection's community id(s) (from that collection's
``transform.communities_ids`` in ``streams.yaml`` / ``streams_done.yaml`` /
``streams_shelved.yaml``):

1. Resolve the given community id(s) to every parent record in them using
   ``RDMParentCommunity``.
2. Resolve each parent to its legacy recid and its parent recid in one PID
   query (``pid_type`` ``lrecid`` and ``recid``).
3. Load every published version of those parents and the file rows. Each
   query is limited to ``CHUNK_SIZE`` ids so one result set is not the whole
   community. ``rdm_records_metadata.parent_id`` is not indexed, so this is a
   handful of table scans instead of one scan per record.
4. Resolve each record uuid to its recid through the ``recid`` PID. The
   version query does not read the record JSON.
5. Build the state list in memory. The latest version is the highest
   ``index`` among those rows. File entries are not stored on the record
   JSON (``FilesField(store=False)``); they come from ``rdm_records_files``.

Output is written once, next to the original, as
``<collection>/rdm_records_state.fixed.json``.

Usage:
    invenio shell

    main(community_ids=["<community-id>"],
         output_path="/migration/tmp/<name>/rdm_records_state.fixed.json",
         log_file="/migration/tmp/<name>/records_state.log",
         dry_run=True)
"""

import json
import traceback
from pathlib import Path

from invenio_db import db
from invenio_files_rest.models import FileInstance, ObjectVersion
from invenio_pidstore.models import PersistentIdentifier, PIDStatus
from invenio_rdm_records.records.models import (
    RDMFileRecordMetadata,
    RDMParentCommunity,
    RDMRecordMetadata,
)

log_fp = None


def log(msg):
    print(msg)
    if log_fp is not None:
        log_fp.write(msg + "\n")
        log_fp.flush()


CHUNK_SIZE = 10000


def chunked(items, size=CHUNK_SIZE):
    """Yield ``items`` in slices of ``size``."""
    items = list(items)
    for start in range(0, len(items), size):
        yield items[start : start + size]


def find_parent_pids(community_ids):
    """Return legacy recids and parent recids for migrated records.

    Community ids map to parent uuids via ``RDMParentCommunity``. One PID
    query per chunk then splits ``lrecid`` and ``recid`` for those parents.

    :returns: ``({legacy_recid: parent_uuid}, {parent_uuid: parent_recid})``
    """
    parent_uuids = list(
        {
            row.record_id
            for row in RDMParentCommunity.query.filter(
                RDMParentCommunity.community_id.in_(community_ids)
            )
        }
    )
    legacy_recids = {}
    parent_recids = {}
    for batch in chunked(parent_uuids):
        pids = PersistentIdentifier.query.filter(
            PersistentIdentifier.pid_type.in_(("lrecid", "recid")),
            PersistentIdentifier.object_type == "rec",
            PersistentIdentifier.status == PIDStatus.REGISTERED,
            PersistentIdentifier.object_uuid.in_(batch),
        )
        for pid in pids:
            object_uuid = str(pid.object_uuid)
            if pid.pid_type == "lrecid":
                legacy_recids[pid.pid_value] = object_uuid
            else:
                parent_recids[object_uuid] = pid.pid_value
    db.session.expunge_all()
    return legacy_recids, parent_recids


def load_pids(object_uuids, pid_type):
    """Return ``{object_uuid: pid_value}`` for registered PIDs.

    ``object_type`` is included so Postgres can use ``idx_object``
    ``(object_type, object_uuid)`` instead of scanning ``pidstore_pid``.
    """
    found = {}
    for batch in chunked(object_uuids):
        pids = PersistentIdentifier.query.filter(
            PersistentIdentifier.pid_type == pid_type,
            PersistentIdentifier.object_type == "rec",
            PersistentIdentifier.status == PIDStatus.REGISTERED,
            PersistentIdentifier.object_uuid.in_(batch),
        )
        for pid in pids:
            found[str(pid.object_uuid)] = pid.pid_value
    db.session.expunge_all()
    return found


def load_versions(legacy_recids):
    """Return ``({legacy_recid: {version_index: version dict}}, failed_recids)``.

    Version rows are loaded without the JSON column. The recid is the
    ``recid`` PID for that row's primary key. Soft-deleted rows (``json``
    is NULL) are excluded, matching ``RDMRecord.get_record()``.
    """
    parent_of = {parent: recid for recid, parent in legacy_recids.items()}
    versions = {}
    parents = list(parent_of)
    for n, batch in enumerate(chunked(parents), start=1):
        rows = (
            db.session.query(
                RDMRecordMetadata.id,
                RDMRecordMetadata.parent_id,
                RDMRecordMetadata.index,
                RDMRecordMetadata.bucket_id,
            )
            .filter(RDMRecordMetadata.parent_id.in_(batch))
            .filter(RDMRecordMetadata.json.isnot(None))
            .all()
        )
        for rec_id, parent_id, index, bucket_id in rows:
            legacy_recid = parent_of[str(parent_id)]
            versions.setdefault(legacy_recid, {})[index] = {
                "record_uuid": str(rec_id),
                "bucket_id": str(bucket_id) if bucket_id else None,
                "parent_object_uuid": str(parent_id),
                "files": [],
            }
        log(f"loaded version chunk {n}, legacy_recids with versions={len(versions)}")
    db.session.expunge_all()

    record_uuids = [
        version["record_uuid"]
        for by_index in versions.values()
        for version in by_index.values()
    ]
    record_recids = load_pids(record_uuids, "recid")
    failed = set()
    for legacy_recid, by_index in versions.items():
        for version in by_index.values():
            new_recid = record_recids.get(version["record_uuid"])
            if not new_recid:
                log(
                    f"record {version['record_uuid']} has no recid PID, "
                    f"legacy_recid={legacy_recid}"
                )
                failed.add(legacy_recid)
                continue
            version["new_recid"] = new_recid
    return versions, failed


def load_files(record_uuids):
    """Return {record_uuid: [file dict, ...]} from ``rdm_records_files``.

    ``record_id`` is indexed. ``file_id`` and ``size`` live on the object
    version and its file instance, not on the record JSON.
    """
    files = {}
    uuids = list(record_uuids)
    for n, batch in enumerate(chunked(uuids), start=1):
        rows = (
            db.session.query(
                RDMFileRecordMetadata.record_id,
                RDMFileRecordMetadata.key,
                RDMFileRecordMetadata.json,
                ObjectVersion.file_id,
                FileInstance.size,
            )
            .join(
                ObjectVersion,
                ObjectVersion.version_id == RDMFileRecordMetadata.object_version_id,
            )
            .join(FileInstance, FileInstance.id == ObjectVersion.file_id)
            .filter(RDMFileRecordMetadata.record_id.in_(batch))
            .filter(RDMFileRecordMetadata.json.isnot(None))
            .all()
        )
        for record_id, key, file_json, file_id, size in rows:
            metadata = (file_json or {}).get("metadata") or {}
            files.setdefault(str(record_id), []).append(
                {
                    "legacy_file_id": metadata.get("legacy_file_id"),
                    "file_key": key,
                    "file_id": str(file_id),
                    "size": str(size),
                }
            )
        del rows
        log(f"loaded file chunk {n}, records with files={len(files)}")
    db.session.expunge_all()
    return files


def _file_for_state(bucket_id, file_entry):
    """One file entry in the shape ``_load_record_state`` writes."""
    legacy_file_id = file_entry["legacy_file_id"]
    if legacy_file_id is None:
        raise KeyError("legacy_file_id")
    return {
        "legacy_file_id": legacy_file_id,
        "bucket_id": bucket_id,
        "file_key": file_entry["file_key"],
        "file_id": file_entry["file_id"],
        "size": file_entry["size"],
    }


def build_state_entry(legacy_recid, rdm_versions, parent_recid):
    """Rebuild one ``rdm_records_state.json`` entry.

    Same fields as ``RecordLoad._load_record_state``
    (cds_migrator_kit/rdm/records/load/entities/record.py). The latest
    version is the highest ``index`` still published.
    """
    recid_state = {
        "legacy_recid": str(legacy_recid),
        "parent_recid": parent_recid,
        "versions": [],
    }
    for version_index in sorted(rdm_versions):
        version = rdm_versions[version_index]
        if "parent_object_uuid" not in recid_state:
            recid_state["parent_object_uuid"] = version["parent_object_uuid"]
        recid_state["versions"].append(
            {
                "new_recid": version["new_recid"],
                "version": version_index,
                "files": [
                    _file_for_state(version["bucket_id"], f) for f in version["files"]
                ],
            }
        )
    latest_version = rdm_versions[max(rdm_versions)]
    recid_state["latest_version"] = latest_version["new_recid"]
    recid_state["latest_version_object_uuid"] = latest_version["record_uuid"]
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


def main(community_ids, output_path, log_file, dry_run=True):
    """Rebuild ``rdm_records_state.json`` for one stream/collection.

    :param community_ids: the community UUIDs for a collection, as set in
        its ``transform.communities_ids`` in streams.yaml.
    :param output_path: should be `<CDS_MIGRATOR_KIT_LOGS_PATH>/<collection>/rdm_records_state.fixed.json`
        next to the original state file (no overwriting).
    :param log_file: progress log for this run.
    :param dry_run: pass dry_run=False to actually write entries to disk.
    """
    global log_fp

    log_fp = open(log_file, "a")
    try:
        legacy_recids, parent_recids = find_parent_pids(community_ids)
        log(
            f"starting, community_ids={str(community_ids)}, "
            f"dry_run={dry_run}, found legacy_recids={len(legacy_recids)}"
        )
        versions, failed = load_versions(legacy_recids)
        record_uuids = [
            version["record_uuid"]
            for by_index in versions.values()
            for version in by_index.values()
        ]
        files = load_files(record_uuids)
        for by_index in versions.values():
            for version in by_index.values():
                version["files"] = files.get(version["record_uuid"], [])

        stats = {"checked": 0, "no_versions": 0, "fixed": 0, "errors": 0}
        entries = []
        for legacy_recid, parent_uuid in legacy_recids.items():
            stats["checked"] += 1
            if legacy_recid in failed:
                stats["errors"] += 1
                continue
            rdm_versions = versions.get(legacy_recid)
            if not rdm_versions:
                log(
                    f"legacy_recid={legacy_recid} - no published versions found, skipping"
                )
                stats["no_versions"] += 1
                continue
            try:
                entries.append(
                    build_state_entry(
                        legacy_recid,
                        rdm_versions,
                        parent_recids[parent_uuid],
                    )
                )
                stats["fixed"] += 1
            except Exception as exc:
                log(f"unexpected error for legacy_recid={legacy_recid}: {exc}")
                log(traceback.format_exc())
                stats["errors"] += 1

        if not dry_run:
            write_state_file(output_path, entries)
            log(f"wrote {output_path} ({len(entries)} entries)")
        else:
            log(f"dry run — would write {output_path} ({len(entries)} entries)")

        log(
            f"\nsummary:\nchecked={stats['checked']}\n"
            f"no_versions={stats['no_versions']}\nfixed={stats['fixed']}\n"
            f"errors={stats['errors']}"
        )
    finally:
        log_fp.close()
        log_fp = None
