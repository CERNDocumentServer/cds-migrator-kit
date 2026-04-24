import json
from datetime import datetime

from cds_rdm.legacy.resolver import get_pid_by_legacy_recid
from invenio_access.permissions import system_identity
from invenio_db import db
from invenio_rdm_records.proxies import current_rdm_records_service


def log(message):
    """Write a success message to the success log file."""
    if not LOG_PATH:
        raise RuntimeError("LOG_PATH is not initialized.")
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(LOG_PATH, "a") as log_file:
        log_file.write(f"[{timestamp}] {message}\n")
    print(message)


def read_created_dates_json(path):
    """Read JSON mapping legacy_recid: 'YYYY-MM-DD HH:MM:SS'."""
    with open(path, "r", encoding="utf-8") as fp:
        return json.load(fp)


def fix_created_dates_from_json(mapping, dry_run=True):
    """Fix CDS-RDM record created timestamps. Updates only the first version of the record."""
    for legacy_recid, created_str in mapping.items():
        created_dt = datetime.strptime(created_str, "%Y-%m-%d %H:%M:%S").replace(
            tzinfo=None
        )
        date_fixed = False

        try:
            parent_pid = get_pid_by_legacy_recid(str(legacy_recid))
        except Exception as exc:
            log(f"[skip] legacy_recid={legacy_recid} pid not found ({exc})")
            continue
        if not parent_pid:
            log(f"[skip] legacy_recid={legacy_recid} pid not found")
            continue
        latest_record = current_rdm_records_service.read_latest(
            identity=system_identity, id_=parent_pid.pid_value
        )
        versions_scan = current_rdm_records_service.scan_versions(
            system_identity,
            latest_record["id"],
        )

        version_hits = [
            (hit["versions"]["index"], hit["id"]) for hit in versions_scan.hits
        ]
        if not version_hits:
            log(f"[skip] legacy_recid={legacy_recid} no versions found")
            continue

        targets = [min(version_hits, key=lambda x: x[0])]

        if not (len(targets) == 1 and int(targets[0][0]) == 1):
            log(
                f"[skip] legacy_recid={legacy_recid} expected one target with version=1, "
                f"got targets={targets}"
            )
            continue

        for version_index, record_uuid in targets:
            record_item = current_rdm_records_service.read(
                identity=system_identity,
                id_=record_uuid,
            )
            record = record_item._record

            if record.model.created == created_dt:
                log(f"[skip] already correct date for record {record_uuid}")
                continue

            if dry_run:
                log(
                    f"[dry-run] legacy_recid={legacy_recid} version={version_index} "
                    f"record={record_uuid} created {record.model.created} -> {created_dt}"
                )
            else:
                record.model.created = created_dt
                record.commit()
                current_rdm_records_service.indexer.index(record)
                date_fixed = True
                log(
                    f"[applied] legacy_recid={legacy_recid} version={version_index} "
                    f"record={record_uuid}"
                )

        if not dry_run and date_fixed:
            db.session.commit()

    log(
        f"Done (mode={'dry-run' if dry_run else 'apply'}; " "scope=first-version-only)."
    )


LOG_PATH = "/tmp/fix_created_dates_log.txt"
with open(LOG_PATH, "w") as log_file:
    log_file.write(f"Log - Started at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    log_file.write("=" * 80 + "\n\n")


json_path = "/tmp/migrated_rdm_records_with_correct_dates.json"
mapping = read_created_dates_json(json_path)

# test with first 10 records and dry-run
record_mapping = dict(list(mapping.items())[10:15])
fix_created_dates_from_json(record_mapping, dry_run=True)
