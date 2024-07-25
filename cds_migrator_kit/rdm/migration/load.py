# -*- coding: utf-8 -*-
#
# Copyright (C) 2022 CERN.
#
# CDS-RDM is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""CDS-RDM migration load module."""

import os

from invenio_access.permissions import system_identity
from invenio_rdm_migrator.load.base import Load
from invenio_rdm_records.proxies import current_rdm_records_service


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
        identity = system_identity  # Should we create an identity for the migration?
        draft = current_rdm_records_service.create(identity, entry["record"]["json"])
        draft_files = entry["draft_files"]

        for file in draft_files:
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
        current_rdm_records_service.publish(system_identity, draft["id"])

    def _cleanup(self, *args, **kwargs):
        """Cleanup the entries."""
        pass
