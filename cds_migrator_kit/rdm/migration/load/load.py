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
from invenio_pidstore.errors import PIDAlreadyExists
from invenio_rdm_migrator.load.base import Load
from invenio_rdm_records.proxies import current_rdm_records_service

from cds_migrator_kit.rdm.migration.transform.xml_processing.errors import (
    ManualImportRequired,
)
from cds_migrator_kit.records.log import RDMJsonLogger
from cds_rdm.minters import legacy_recid_minter

cli_logger = logging.getLogger("migrator")


def import_legacy_files(filepath):
    """Download file from legacy."""
    filestream = open(filepath, "rb")
    return filestream


class CDSRecordServiceLoad(Load):
    """CDSRecordServiceLoad."""

    def __init__(self, db_uri, data_dir, tmp_dir, existing_data=False, entries=None,
                 dry_run=False):
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
                            "metadata": file_data["metadata"],
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
                    priority="critical"
                )
                migration_logger.add_log(exc, record=entry)
                raise e

    def _load_access(self, draft, entry):
        """Load access rights."""
        parent = draft._record.parent
        access = entry["parent"]["json"]["access"]
        parent.access = access
        parent.commit()

    def _load_communities(self, draft, entry):
        parent = draft._record.parent
        communities = entry["parent"]["json"]["communities"]["ids"]
        for community in communities:
            parent.communities.add(community)
        parent.commit()

    def _load_versions(self, draft, entry):
        """Load other versions of the record."""
        draft_files = entry["draft_files"]

        def publish_and_mint_recid(draft, version):
            record = current_rdm_records_service.publish(system_identity, draft["id"])
            # mint legacy ids for redirections
            if version == 1:
                record._record.model.created = arrow.get(
                    entry["record"]["created"]).datetime
                record._record.commit()
                # it seems more intuitive if we mint the lrecid for parent
                # but then we get a double redirection
                legacy_recid_minter(entry["record"]["recid"],
                                    record._record.parent.model.id)

        if not draft_files:
            # when missing files, just publish
            publish_and_mint_recid(draft, 1)

        for version in draft_files.keys():
            version_files_dict = draft_files.get(version)
            if version == 1:
                self._load_files(draft, entry, version_files_dict)
            else:
                draft = current_rdm_records_service.new_version(
                    system_identity, draft["id"]
                )

                self._load_files(draft, entry, version_files_dict)

                # attention! the metadata of new version
                # will be taken from the first file
                # on the list TODO can we improve this for publication accuracy?
                # maybe sorting by creation date would be better?
                filename = next(iter(version_files_dict))
                file = version_files_dict[filename]
                # add missing metadata for new version
                missing_data = {
                    "metadata": {
                        # copy over the previous draft metadata
                        **draft.to_dict()["metadata"],
                        # add missing publication date based on the time of creation of the new file version
                        "publication_date": file["creation_date"],
                    }
                }

                draft = current_rdm_records_service.update_draft(
                    system_identity, draft["id"], data=missing_data
                )

            publish_and_mint_recid(draft, version)

    def _load_model_fields(self, draft, entry):
        """Load model fields of the record."""

        draft._record.model.created = arrow.get(entry["record"]["created"]).datetime
        draft._record.commit()
        # TODO we can use unit of work when it is moved to invenio-db module
        self._load_access(draft, entry)
        self._load_communities(draft, entry)
        db.session.commit()

    def _dry_load(self, entry):
        current_rdm_records_service.schema.load(
            entry["record"]["json"],
            context=dict(
                identity=system_identity,
            ),
            raise_errors=True,
        )


    def _load(self, entry):
        """Use the services to load the entries."""
        if entry:
            recid = entry.get("record", {}).get("recid", {})
            migration_logger = RDMJsonLogger()
            identity = system_identity  # TODO: load users instead
            try:
                if self.dry_run:
                    self._dry_load(entry)
                else:
                    draft = current_rdm_records_service.create(
                        identity, data=entry["record"]["json"]
                    )

                    self._load_model_fields(draft, entry)

                    self._load_versions(draft, entry)
                migration_logger.add_success(recid)
            except Exception as e:
                exc = ManualImportRequired(message=str(e),
                                           field="validation",
                                           stage="load",
                                           recid=recid,
                                           priority="warning"
                                           )
                migration_logger.add_log(exc, record=entry)
                # raise e

    def _cleanup(self, *args, **kwargs):
        """Cleanup the entries."""
        pass
