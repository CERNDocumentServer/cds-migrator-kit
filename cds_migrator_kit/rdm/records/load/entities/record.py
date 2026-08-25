# -*- coding: utf-8 -*-
#
# Copyright (C) 2026 CERN.
#
# CDS-RDM is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""Creates, versions, and publishes a single RDM record from a ``RecordEntry``."""
import os
from typing import Dict

import arrow
from cds_rdm.minters import legacy_recid_minter
from flask import current_app
from invenio_access.permissions import system_identity
from invenio_db import db
from invenio_pidstore.errors import PIDAlreadyExists
from invenio_pidstore.models import PersistentIdentifier, PIDStatus
from invenio_rdm_records.proxies import current_rdm_records_service
from psycopg2.errors import UniqueViolation
from sqlalchemy.exc import IntegrityError

from cds_migrator_kit.errors import ManualImportRequired
from cds_migrator_kit.rdm.records.transform.entities.parent import RecordParent
from cds_migrator_kit.rdm.records.transform.entities.record import RecordEntry
from cds_migrator_kit.rdm.records.transform.entities.version import (
    VersionAccess,
    VersionFileEntry,
)

from .parent import ParentLoad


def import_legacy_files(filepath):
    """Download file from legacy."""
    if current_app.config["CDS_MIGRATOR_KIT_ENV"] == "local":
        import cds_migrator_kit

        base_path = os.path.dirname(os.path.realpath(cds_migrator_kit.__file__))
        filepath = os.path.join(base_path, "rdm/data/files/dummy.pdf")
    filestream = open(filepath, "rb")
    return filestream


class RecordLoad:
    """Creates, versions, and publishes a single RDM record from a ``RecordEntry``.

    The load-side counterpart of ``RecordEntry`` - takes the already-built
    transform-side entity (plus its sibling ``RecordParent``, needed when
    creating the very first version) and drives the record-service calls
    that materialize/version/publish it - mirrors how ``ParentLoad`` is the
    counterpart of ``RecordParent``.
    """

    def __init__(
        self,
        record_entry: RecordEntry,
        record_parent: RecordParent,
        migration_logger,
        is_final_record=True,
        update_new_version_publication_date=False,
        record_state_logger=None,
    ):
        """Constructor.

        :param record_entry: the built ``RecordEntry`` for this entry
            (``MigrationEntry["record"]``).
        :param record_parent: the built ``RecordParent`` for this entry -
            only used on the first version, to set up the new draft's
            parent access/communities (see ``pre_publish()``).
        :param is_final_record: whether this is the "real"/final record for
            its legacy recid - False for the restricted half of an EP
            approval split, which exists only as an internal source record
            and must skip legacy-recid-facing side effects (rep-number PID
            assignment, DOI updates, recid minting) - see
            ``CDSMigrationEntryLoad._is_final_record``.
        :param record_state_logger: used by ``build_record_state()`` to
            dump the computed record state, for later stats migration.
        """
        self.record_entry = record_entry
        self.record_parent = record_parent
        self.migration_logger = migration_logger
        self.is_final_record = is_final_record
        self.update_new_version_publication_date = update_new_version_publication_date
        self.record_state_logger = record_state_logger

    def load_files(
        self,
        draft,
        version_files: Dict[str, VersionFileEntry],
        uow=None,
    ):
        """Load files to draft."""
        recid = self.record_entry.recid
        identity = system_identity  # Should we create an identity for the migration?

        for filename, file_data in version_files.items():

            file_data = version_files[filename]

            try:
                current_rdm_records_service.draft_files.init_files(
                    identity,
                    draft.id,
                    data=[
                        {
                            "key": file_data["key"],
                            "metadata": {
                                **file_data["metadata"],
                                "legacy_file_id": file_data["id_bibdoc"],
                                "legacy_recid": recid,
                            },
                            "access": {"hidden": False},
                        }
                    ],
                    uow=uow,
                )
                current_rdm_records_service.draft_files.set_file_content(
                    identity,
                    draft.id,
                    file_data["key"],
                    import_legacy_files(file_data["eos_tmp_path"]),
                    uow=uow,
                )
                result = current_rdm_records_service.draft_files.commit_file(
                    identity, draft.id, file_data["key"], uow=uow
                )
                legacy_checksum = f"md5:{file_data['checksum']}"
                new_checksum = result.to_dict()["checksum"]
                if current_app.config["CDS_MIGRATOR_KIT_ENV"] != "local":
                    try:
                        assert legacy_checksum == new_checksum
                    except AssertionError:
                        raise ManualImportRequired(
                            message=f"Files checksum failed legacy:{legacy_checksum} calculated new: {new_checksum}",
                            field="checksum",
                            stage="load",
                            recid=recid,
                            priority="critical",
                            value=file_data["key"],
                            subfield=None,
                        )

            except Exception as e:
                exc = ManualImportRequired(
                    recid=recid,
                    message=str(e),
                    field="filename",
                    value=file_data["key"],
                    stage="file load",
                    priority="critical",
                )
                self.migration_logger.add_log(exc, record={"record": self.record_entry})
                raise e

    def load_access(self, draft, access_dict: VersionAccess):
        """Set this version's access on the published record."""
        record = draft._record
        record.access = access_dict["access_obj"]
        record.commit()

    def assign_rep_numbers(self, draft):
        """Mint ``cdsrn`` PIDs for this draft's report-number identifiers."""
        if not self.is_final_record:
            return
        draft_report_nums = {}
        for index, id in enumerate(draft.data["metadata"].get("identifiers", [])):
            if id["scheme"] == "cdsrn":
                draft_report_nums[id["identifier"]] = index

        if not draft_report_nums:
            # If no mintable identifiers, return early
            return

        for report_number, index in draft_report_nums.items():
            try:
                PersistentIdentifier.create(
                    pid_type="cdsrn",
                    pid_value=report_number,
                    object_type="rec",
                    object_uuid=draft._record.parent.id,
                    status=PIDStatus.REGISTERED,
                )
            except PIDAlreadyExists as e:
                pid = PersistentIdentifier.get(
                    pid_type="cdsrn", pid_value=report_number
                )
                if pid.object_uuid != draft._record.parent.id:
                    # raise only if different parent uuid found, meaning they are 2
                    # different records and the repnum is duplicated
                    raise ManualImportRequired(
                        f"Report number {report_number} already exists."
                    )

    def pre_publish(self, identity, versions, version, draft, uow):
        """Create (or version) and process a draft before publish."""
        files = versions[version]["files"]
        access = versions[version]["access"]

        if version == 1 or (version > 1 and draft is None):
            # when draft is None, it means the initial version one was hard deleted
            # and we don't have index 1
            # we decided to skip it and act normal
            try:
                draft = current_rdm_records_service.create(
                    identity, data=self.record_entry.body, uow=uow
                )
                self.assign_rep_numbers(draft)
            except (UniqueViolation, IntegrityError) as e:
                raise ManualImportRequired(message=str(e))
            except Exception as e:
                raise ManualImportRequired(message=str(e))
            if draft.errors:
                raise ManualImportRequired(
                    message=f"{str(draft.errors)}: {str(self.record_entry.body)}",
                    field="validation",
                    stage="load",
                    recid=self.record_entry.recid,
                    priority="warning",
                    value=draft._record.pid.pid_value,
                    subfield=None,
                )
        else:
            draft = current_rdm_records_service.new_version(
                identity, draft["id"], uow=uow
            )
            draft_dict = draft.to_dict()
            if not self.update_new_version_publication_date:
                publication_date = arrow.get(
                    self.record_entry.body["metadata"]["publication_date"]
                )
            else:
                publication_date = versions[version]["publication_date"]
            missing_data = {
                **draft_dict,
                "metadata": {
                    # copy over the previous draft metadata
                    **draft_dict["metadata"],
                    # add missing publication date based
                    # on the time of creation of the new file version
                    "publication_date": publication_date.date().isoformat(),
                },
            }
            draft = current_rdm_records_service.update_draft(
                identity, draft["id"], data=missing_data, uow=uow
            )

        self.load_access(draft, access)
        self.load_files(draft, files, uow=uow)

        return draft

    def after_publish_update_dois(self, identity, record, uow):
        """Update migrated DOIs post publish."""
        if not self.is_final_record:
            return
        migrated_pids = self.record_entry.body["pids"]
        for pid_type, identifier in migrated_pids.items():
            if pid_type == "doi":
                # If a DOI was already minted from legacy then on publish the datacite
                # will return a warning that "This DOI has already been taken"
                # In that case, we edit and republish to force an update of the doi with
                # the new published metadata as in the new system we have more information available
                _draft = current_rdm_records_service.edit(
                    identity, record["id"], uow=uow
                )
                record = current_rdm_records_service.publish(
                    identity, _draft["id"], uow=uow
                )
                return record

    def after_publish_update_created(self, record, version_data, version):
        """Update created timestamp post publish.

        Ensures that the `created` timestamp is correctly set, preferring:
        1. The original legacy system value for the version.
        2. The record's creation date if there are no files.
        3. Today's date if the original value and file creation date is missing.
        """
        creation_date = arrow.get(self.record_entry.created).datetime.replace(
            tzinfo=None
        )

        if version_data.get("files") and version != 1:
            # Subsequent versions should use the file creation date, instead of the record creation date,
            # which is stored as the publication date in the version data
            creation_date = version_data["publication_date"].datetime.replace(
                tzinfo=None
            )

        record._record.model.created = creation_date
        db.session.add(record._record.model)

    def after_publish_mint_recid(self, record):
        """Mint legacy ids for redirections assigned to the parent."""
        if not self.is_final_record:
            return
        legacy_recid = self.record_entry.recid
        if record._record.versions.index == 1:
            # it seems more intuitive if we mint the lrecid for parent
            # but then we get a double redirection
            legacy_recid_minter(legacy_recid, record._record.parent.model.id)

    def after_publish_update_files_created(self, record, version_data):
        """Update the created date of the files post publish."""
        # Fix the `created` timestamp forcing the one from the legacy system
        # Force the created date. This can be done after publish as the service
        # overrides the `created` date otherwise.
        files = version_data.get("files", {})
        for _, file_data in files.items():
            file = record._record.files.entries[file_data["key"]]
            file.model.created = arrow.get(file_data["creation_date"]).datetime.replace(
                tzinfo=None
            )
            db.session.add(file.model)

    def _after_publish(self, identity, published_record, entry, version, uow):
        """Run fixes after record publish."""
        record = self.after_publish_update_dois(identity, published_record, uow)
        if record:
            published_record = record
        version_data = entry.get("versions", {}).get(version, {})
        self.after_publish_update_created(published_record, version_data, version)
        self.after_publish_mint_recid(published_record)
        self.after_publish_update_files_created(published_record, version_data)

    def _load_versions(self, entry, uow):
        """Create, publish, and run after-publish fixes for every version."""
        identity = system_identity
        records = []
        # initial value of draft. If different file versions identified then the first
        # created draft is used to populate all newer versions
        draft = None
        for version in entry["versions"].keys():
            # Create and prepare draft
            draft = self.pre_publish(identity, entry["versions"], version, draft, uow)

            # Publish draft
            published_record = current_rdm_records_service.publish(
                identity, draft["id"], uow=uow
            )
            # Run after publish fixes
            self._after_publish(identity, published_record, entry, version, uow)
            records.append(published_record)

        return records

    def load(self, entry, uow=None):
        """Load all versions of the record; return the list of published records."""
        return self._load_versions(entry, uow)

    def dry_load(self):
        """Validate the record body via the service schema without persisting it."""
        current_rdm_records_service.schema.load(
            self.record_entry.body,
            context=dict(
                identity=system_identity,
            ),
            raise_errors=True,
        )

    def build_record_state(self, legacy_recid, records):
        """Compute the record state for newly published records.

        Deliberately has no side effects - the caller must not treat this as
        "the load succeeded", since parent/request loading can still fail
        and roll back the surrounding unit of work. Call ``log_record_state``
        only once the whole unit of work has actually committed, otherwise
        the record-state log ends up with entries for records that were
        rolled back (see ``log_record_state``).
        """
        if not records:
            return None
        return self._load_record_state(legacy_recid, records)

    def log_record_state(self, record_state_context):
        """Persist the computed record state, for later stats migration.

        Must only be called after the unit of work that produced
        ``record_state_context`` has committed - this writes straight to
        disk, so calling it before commit (e.g. right after
        ``build_record_state``) risks logging a record that later gets
        rolled back by a failure elsewhere in the same load (e.g. a grant
        creation failure).
        """
        if self.is_final_record:
            self.record_state_logger.add_record_state(record_state_context)

    def _load_record_state(self, legacy_recid, records):
        """Compute state for legacy recid.

        Returns
        {
            "legacy_recid": "2884810",
            "parent_recid": "zts3q-6ef46",
            "parent_object_uuid": "435be22f-3038-49e0-9f17-9518eaac783a",
            "latest_version": "1mae4-skq89"
            "latest_version_object_uuid": "895be22f-3038-49e0-9f17-9518eaac783a",
            "versions": [
                {
                    "new_recid": "1mae4-skq89",
                    "version": 2,
                    "files": [
                        {
                            "legacy_file_id": 1568736,
                            "bucket_id": "155be22f-3038-49e0-9f17-9518eaac783a",
                            "file_key": "Summer student program report.pdf",
                            "file_id": "06cdb9d2-635f-4dbe-89fe-4b27afddeaa2",
                            "size": "1690854"
                        }
                    ]
                }
            ]
        }
        """

        def convert_file_format(file_entries, bucket_id):
            """Convert the file metadata into the required format."""
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
            """Extract relevant details from a single record."""
            bucket_id = str(record.files.bucket_id)
            files = record.__class__.files.dump(
                record, record.files, include_entries=True
            ).get("entries", {})
            return {
                "new_recid": record.pid.pid_value,
                "version": record.versions.index,
                "files": convert_file_format(files, bucket_id),
            }

        recid_state = {"legacy_recid": legacy_recid, "versions": []}
        parent_recid = None

        for record in records:
            if parent_recid is None:
                parent_id = str(record._record.parent.id)
                parent_recid = record._record.parent.pid.pid_value
                recid_state["parent_recid"] = parent_recid
                recid_state["parent_object_uuid"] = parent_id

            recid_version = extract_record_version(record._record)
            # Save the record versions for legacy recid
            recid_state["versions"].append(recid_version)

            if "latest_version" not in recid_state:
                rec = record._record.get_latest_by_parent(record._record.parent)
                recid_state["latest_version"] = rec["id"]
                recid_state["latest_version_object_uuid"] = str(rec.id)
        return recid_state
