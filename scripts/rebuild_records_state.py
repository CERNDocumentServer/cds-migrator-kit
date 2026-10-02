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
2. Resolve each parent to its legacy recid via the ``lrecid`` PID minted at
   migration time.
3. Rebuild the state entry via the shared
   ``cds_migrator_kit.rdm.records.load.entities.record.build_record_state_from_db``
   - the same function ``CDSMigrationEntryLoad._should_skip_recid`` uses to
   backfill a single record's state when it's found missing for an
   already-migrated record (cds_migrator_kit/rdm/records/load/load.py).

Output is written next to the original, as ``<collection>/rdm_records_state.fixed.json``,
in batches of ``BATCH_SIZE`` entries at a time rather than all at once - a
batch is merged into whatever's already on disk and the complete file is
rewritten, so a crash loses at most one in-progress batch, and the file
never has to be held in memory in full.

On restart the script reads its own log file for DONE lines and skips any
legacy recid already completed.

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

from invenio_pidstore.models import PersistentIdentifier
from invenio_rdm_records.records.models import RDMParentCommunity

from cds_migrator_kit.rdm.records.load.entities.record import (
    build_record_state_from_db,
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


BATCH_SIZE = 500


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


def flush_batch(output_path, batch):
    """Merge a batch of new entries into whatever is already on disk and
    rewrite the complete file. Called every ``BATCH_SIZE`` entries instead
    of once per entry (too slow, file never readable) or once for the whole
    run (loses everything on a crash) - entries already flushed by a
    previous batch are read back and re-written, not held in memory for the
    rest of the run."""
    existing = []
    if Path(output_path).exists():
        with open(output_path, encoding="utf-8") as f:
            existing = json.load(f)
    write_state_file(output_path, existing + batch)


def main(community_ids, output_path, log_file, dry_run=True):
    """Rebuild ``rdm_records_state.json`` for one stream/collection.

    :param community_ids: the community UUIDs for a collection, as set in
        its ``transform.communities_ids`` in streams.yaml.
    :param output_path: should be `<CDS_MIGRATOR_KIT_LOGS_PATH>/<collection>/rdm_records_state.fixed.json`
        next to the original state file (no overwriting).
    :param log_file: resumable progress log - DONE lines mark completed recids.
    :param dry_run: pass dry_run=False to actually write entries to disk.
    """
    global log_fp

    completed_recids = load_completed_recids(log_file)
    log_fp = open(log_file, "a")

    if completed_recids:
        log(f"resuming — {len(completed_recids)} recid(s) already completed, skipping them")

    batch = []
    # legacy_recids pending in `batch` - only marked DONE once their batch
    # is actually flushed, so a crash before that flush leaves them absent
    # from completed_recids on resume, and state generation re-runs for
    # them instead of being silently skipped as already done.
    batch_recids = []

    def flush_pending():
        if batch:
            flush_batch(output_path, batch)
            for r in batch_recids:
                log(f"DONE: legacy_recid={r}")
            batch.clear()
            batch_recids.clear()

    try:
        legacy_recids = find_legacy_recids(community_ids)
        log(
            f"starting, community_ids={str(community_ids)}, "
            f"dry_run={dry_run}, found legacy_recids={len(legacy_recids)}]"
        )

        stats = {"checked": 0, "skipped_done": 0, "no_versions": 0, "fixed": 0, "errors": 0}

        for i, (legacy_recid, parent_object_uuid) in enumerate(legacy_recids.items(), start=1):
            stats["checked"] += 1

            if legacy_recid in completed_recids:
                stats["skipped_done"] += 1
                continue

            log(f"[{i}/{len(legacy_recids)}] legacy_recid={legacy_recid}")

            try:
                rebuilt_state = build_record_state_from_db(legacy_recid, parent_object_uuid)
                if not rebuilt_state:
                    log(f"legacy_recid={legacy_recid} - no published versions found, skipping")
                    stats["no_versions"] += 1
                    continue

                batch.append(rebuilt_state)
                stats["fixed"] += 1

                if not dry_run:
                    batch_recids.append(legacy_recid)
                    if len(batch) >= BATCH_SIZE:
                        flush_pending()

            except Exception as exc:
                log(f"unexpected error for legacy_recid={legacy_recid}: {exc}")
                log(traceback.format_exc())
                stats["errors"] += 1

        if not dry_run:
            flush_pending()
            log(f"wrote {output_path}")
        else:
            log(f"dry run — would write {output_path}")

        log(
            f"\nsummary:\nchecked={stats['checked']}\nalready_done={stats['skipped_done']}\n"
            f"no_versions={stats['no_versions']}\nfixed={stats['fixed']}\n"
            f"errors={stats['errors']}"
        )
    finally:
        if not dry_run:
            flush_pending()
        log_fp.close()
        log_fp = None
