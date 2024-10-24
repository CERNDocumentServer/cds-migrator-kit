# -*- coding: utf-8 -*-
#
# Copyright (C) 2022 CERN.
#
# CDS-RDM is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""CDS-RDM migration load module."""
import logging
import os
import json

import arrow
from invenio_access.permissions import system_identity
from invenio_db import db
from invenio_pidstore.errors import PIDAlreadyExists
from invenio_rdm_migrator.load.base import Load
from invenio_rdm_records.proxies import current_rdm_records_service
from invenio_records.systemfields.relations import InvalidRelationValue
from marshmallow import ValidationError
from psycopg.errors import UniqueViolation

from cds_migrator_kit.rdm.migration.transform.xml_processing.errors import (
    ManualImportRequired,
    RecordStatsNotImported, CDSMigrationException,
)

from cds_migrator_kit.records.log import RDMJsonLogger
from cds_rdm.minters import legacy_recid_minter
from cds_rdm.models import CDSMigrationLegacyRecord

cli_logger = logging.getLogger("migrator")


def import_legacy_files(filepath):
    """Download file from legacy."""
    filestream = open(filepath, "rb")
    return filestream


class CDSRecordServiceLoad(Load):
    """CDSRecordServiceLoad."""

    def __init__(
        self,
        db_uri,
        data_dir,
        tmp_dir,
        existing_data=False,
        entries=None,
        dry_run=False,
    ):
        """Constructor."""
        self.db_uri = db_uri
        self.data_dir = data_dir
        self.tmp_dir = tmp_dir
        self.existing_data = existing_data
        self.entries = entries
        self.dry_run = dry_run

    def _prepare(self, entry):
        """Prepare the record."""
        pass

    def _load_files(self, draft, entry, version_files):
        """Load files to draft."""
        recid = entry.get("record", {}).get("recid", {})
        migration_logger = RDMJsonLogger()
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
                            },
                            "access": {"hidden": False},
                        }
                    ],
                )
                # TODO change to eos move or xrootd command instead of going through the app
                # TODO leave the init part to pre-create the destination folder
                # TODO update checksum, size, commit (to be checked on how these methods work)
                # if current_app.config["XROOTD_ENABLED"]:
                #     storage = current_files_rest.storage_factory
                #     current_rdm_records_service.draft_files.set_file_content(
                #         identity,
                #         draft.id,
                #         file["key"],
                #         BytesIO(b"Placeholder file"),
                #     )
                #     obj = None
                #     for object in draft._record.files.objects:
                #         if object.key == file["key"]:
                #             obj = object
                #     path = obj.file.uri
                # else:
                # for local development
                current_rdm_records_service.draft_files.set_file_content(
                    identity,
                    draft.id,
                    file_data["key"],
                    import_legacy_files(file_data["eos_tmp_path"]),
                )
                result = current_rdm_records_service.draft_files.commit_file(
                    identity, draft.id, file_data["key"]
                )
                legacy_checksum = f"md5:{file_data['checksum']}"
                new_checksum = result.to_dict()["checksum"]
                assert legacy_checksum == new_checksum

            except Exception as e:
                exc = ManualImportRequired(
                    recid=recid,
                    message=str(e),
                    field="filename",
                    value=file_data["key"],
                    stage="file load",
                    priority="critical",
                )
                migration_logger.add_log(exc, record=entry)
                raise e

    def _load_parent_access(self, draft, entry):
        """Load access rights."""
        parent = draft._record.parent
        access = entry["parent"]["json"]["access"]
        parent.access = access
        parent.commit()

    def _load_record_access(self, draft, access_dict):
        record = draft._record
        # TODO hook in here if we want to share with groups

        record.access = access_dict["access_obj"]
        record.commit()

    def _load_communities(self, draft, entry):
        parent = draft._record.parent
        communities = entry["parent"]["json"]["communities"]["ids"]
        for community in communities:
            parent.communities.add(community)
        parent.commit()

    def _load_versions(self, entry, logger):
        """Load other versions of the record."""
        versions = entry["versions"]
        legacy_recid = entry["record"]["recid"]

        def publish_and_mint_recid(draft, version):
            record_item = current_rdm_records_service.publish(system_identity, draft["id"])
            # mint legacy ids for redirections
            if version == 1:
                record_item._record.model.created = arrow.get(
                    entry["record"]["created"]
                ).datetime
                record_item._record.commit()
                # it seems more intuitive if we mint the lrecid for parent
                # but then we get a double redirection
                legacy_recid_minter(legacy_recid, record_item._record.parent.model.id)
            return record_item

        identity = system_identity  # TODO: load users instead ?

        records = []
        for version in versions.keys():
            files = versions[version]["files"]
            publication_date = versions[version]["publication_date"]
            access = versions[version]["access"]

            if version == 1:
                draft = current_rdm_records_service.create(
                    identity, data=entry["record"]["json"]
                )
                # TODO we can use unit of work when it is moved to invenio-db module
                self._load_parent_access(draft, entry)
                self._load_communities(draft, entry)
                db.session.commit()
            else:
                draft = current_rdm_records_service.new_version(
                    system_identity, draft["id"]
                )
                missing_data = {
                    "metadata": {
                        # copy over the previous draft metadata
                        **draft.to_dict()["metadata"],
                        # add missing publication date based
                        # on the time of creation of the new file version
                        "publication_date": publication_date.date().isoformat(),
                    }
                }
                draft = current_rdm_records_service.update_draft(
                    system_identity, draft["id"], data=missing_data
                )

            self._load_record_access(draft, access)
            self._load_files(draft, entry, files)

            record = publish_and_mint_recid(draft, version)
            # Force the created date. This can be done after publish as the service
            # overrides the `created` date otherwise.
            self._load_model_fields(record, entry)
            records.append(record._record)
        if records:
            record_state_context = self._load_record_state(legacy_recid, records)
            # Dump the computed record state. This is useful to migrate then the record stats
            if record_state_context:
                logger.add_record_state(record_state_context)
                return record_state_context

    def _load_model_fields(self, record, entry):
        """Load model fields of the record."""
        record._record.model.created = arrow.get(entry["record"]["created"]).datetime
        record._record.commit()
        db.session.commit()

    def _dry_load(self, entry):
        current_rdm_records_service.schema.load(
            entry["record"]["json"],
            context=dict(
                identity=system_identity,
            ),
            raise_errors=True,
        )

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
                parent_id = str(record.parent.id)
                parent_recid = record.parent.pid.pid_value
                recid_state["parent_recid"] = parent_recid
                recid_state["parent_object_uuid"] = parent_id

            recid_version = extract_record_version(record)
            # Save the record versions for legacy recid
            recid_state["versions"].append(recid_version)

            if "latest_version" not in recid_state:
                rec = record.get_latest_by_parent(record.parent)
                recid_state["latest_version"] = rec["id"]
                recid_state["latest_version_object_uuid"] = str(rec.id)
        return recid_state

    def _save_original_dumped_record(self, entry, recid_state, logger):
        """Save the original dumped record.

        This is the originally extracted record before any transformation.
        """
        _original_dump = entry["_original_dump"]

        _original_dump_model = CDSMigrationLegacyRecord(
            json=_original_dump,
            parent_object_uuid=recid_state["parent_object_uuid"],
            migrated_record_object_uuid=recid_state["latest_version_object_uuid"],
            legacy_recid=entry["record"]["recid"],
        )
        db.session.add(_original_dump_model)
        db.session.commit()

    def _load(self, entry):
        """Use the services to load the entries."""
        if entry:
            recid = entry.get("record", {}).get("recid", {})
            migration_logger = RDMJsonLogger()
            try:
                if self.dry_run:
                    self._dry_load(entry)
                else:
                    recid_state_after_load = self._load_versions(
                        entry, migration_logger
                    )
                    if recid_state_after_load:
                        self._save_original_dumped_record(
                            entry, recid_state_after_load, migration_logger
                        )
                migration_logger.add_success(recid)
            except (PIDAlreadyExists, UniqueViolation) as e:
                # TODO remove when there is a way of cleaning local environment from
                # previous run of migration
                exc = ManualImportRequired(
                    message=str(e),
                    field="validation",
                    stage="load",
                    description="RECORD Already exists.",
                    recid=recid,
                    priority="warning",
                    value=e.pid_value,
                    subfield="PID"
                )
                migration_logger.add_log(exc, record=entry)
            except (CDSMigrationException, ValidationError, InvalidRelationValue) as e:
                exc = ManualImportRequired(
                    message=str(e),
                    field="validation",
                    stage="load",
                    recid=recid,
                    priority="warning",
                )
                migration_logger.add_log(exc, record=entry)

    def _cleanup(self, *args, **kwargs):
        """Cleanup the entries."""
        pass
