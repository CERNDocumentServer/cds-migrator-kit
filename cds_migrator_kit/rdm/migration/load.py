# -*- coding: utf-8 -*-
#
# Copyright (C) 2022 CERN.
#
# CDS-RDM is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""CDS-RDM migration load module."""
import logging
import os

from invenio_access.permissions import system_identity
from invenio_db import db
from invenio_rdm_migrator.load.base import Load
from invenio_rdm_records.proxies import current_rdm_records_service

from cds_migrator_kit.rdm.migration.transform.xml_processing.errors import \
    ManualImportRequired

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

    def _load(self, entry):
        """Use the services to load the entries."""
        from cds_migrator_kit.rdm.migration.cli import migration_logger, cli_logger
        recid = entry.get("record", {}).get("json", {}).get("id")
        migration_logger.add_recid_to_stats(recid)
        identity = system_identity  # Should we create an identity for the migration?
        draft = current_rdm_records_service.create(identity, entry["record"]["json"])
        parent = draft._record.parent
        access = entry["parent"]["json"]["access"]
        parent.access = access
        parent.commit()
        db.session.commit()
        draft_files = entry["draft_files"]

        for file in draft_files:
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
                exc = ManualImportRequired(message=str(e))
                migration_logger.add_log(exc, output=entry)
        current_rdm_records_service.publish(system_identity, draft["id"])

    def _cleanup(self, *args, **kwargs):
        """Cleanup the entries."""
        pass
