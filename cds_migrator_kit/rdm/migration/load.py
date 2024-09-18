# -*- coding: utf-8 -*-
#
# Copyright (C) 2022 CERN.
#
# CDS-RDM is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""CDS-RDM migration load module."""
import logging

import arrow
from invenio_access.permissions import system_identity
from invenio_db import db
from invenio_rdm_migrator.load.base import Load
from invenio_rdm_records.proxies import current_rdm_records_service

from cds_migrator_kit.rdm.migration.transform.xml_processing.errors import (
    ManualImportRequired,
)
from cds_migrator_kit.records.log import RDMJsonLogger

cli_logger = logging.getLogger("migrator")


def import_legacy_files(filepath):
    """Download file from legacy."""
    filestream = open(filepath, "rb")
    return filestream


class CDSRecordServiceLoad(Load):
    """CDSRecordServiceLoad."""

    def __init__(self, db_uri, data_dir, tmp_dir, existing_data=False, entries=None):
        """Constructor."""
        self.db_uri = db_uri
        self.data_dir = data_dir
        self.tmp_dir = tmp_dir
        self.existing_data = existing_data
        self.entries = entries

    def _prepare(self, entry):
        """Prepare the record."""
        pass

    def _load_files(self, draft, entry, version_files):
        """Load files to draft."""
        recid = entry.get("record", {}).get("recid", {})
        migration_logger = RDMJsonLogger()
        migration_logger.add_recid_to_stats(recid)

        identity = system_identity  # Should we create an identity for the migration?

        # take first file for the fist version
        filename = next(iter(version_files))

        file = version_files[filename]

        try:
            current_rdm_records_service.draft_files.init_files(
                identity,
                draft.id,
                data=[
                    {
                        "key": file["key"],
                        "metadata": file["metadata"],
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
                file["key"],
                import_legacy_files(file["eos_tmp_path"]),
            )
            result = current_rdm_records_service.draft_files.commit_file(
                identity, draft.id, file["key"]
            )
            legacy_checksum = f"md5:{file['checksum']}"
            new_checksum = result.to_dict()["checksum"]
            assert legacy_checksum == new_checksum
        except Exception as e:
            exc = ManualImportRequired(
                message=str(e), field="filename", value=file["key"]
            )
            migration_logger.add_log(exc, output=entry)

    def _load_access(self, draft, entry):
        """Load access rights."""
        parent = draft._record.parent
        access = entry["parent"]["json"]["access"]
        parent.access = access
        parent.commit()

    def _load_versions(self, draft, entry):
        """Load other versions of the record."""
        draft_files = entry["draft_files"]
        for version in draft_files.keys():
            file_dict = draft_files.get(version)
            if version == 1:
                self._load_files(draft, entry, file_dict)
            else:
                draft = current_rdm_records_service.new_version(
                    system_identity, draft["id"]
                )

                self._load_files(draft, entry, file_dict)
                filename = next(iter(file_dict))
                file = file_dict[filename]
                missing_data = {
                    "metadata": {
                        **draft.to_dict()["metadata"],
                        "publication_date": file["creation_date"],
                    }
                }

                draft = current_rdm_records_service.update_draft(
                    system_identity, draft["id"], data=missing_data
                )
            record = current_rdm_records_service.publish(system_identity, draft["id"])

    def _load_model_fields(self, draft, entry):
        """Load model fields of the record."""

        draft._record.model.created = arrow.get(entry["record"]["created"]).datetime
        # TODO we can use unit of work when it is moved to invenio-db module
        self._load_access(draft, entry)
        db.session.commit()


    def _load(self, entry):
        """Use the services to load the entries."""
        recid = entry.get("record", {}).get("recid", {})
        migration_logger = RDMJsonLogger()
        migration_logger.add_recid_to_stats(recid)
        identity = system_identity  # Should we create an identity for the migration?
        draft = current_rdm_records_service.create(
            identity, data=entry["record"]["json"]
        )
        try:
            self._load_model_fields(draft, entry)

            self._load_versions(draft, entry)
        except Exception as e:
            exc = ManualImportRequired(message=str(e), field="validation")
            migration_logger.add_log(exc, output=entry)

    def _cleanup(self, *args, **kwargs):
        """Cleanup the entries."""
        pass
