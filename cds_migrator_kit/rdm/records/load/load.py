# -*- coding: utf-8 -*-
#
# Copyright (C) 2022 CERN.
#
# CDS-RDM is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""CDS-RDM migration load module."""
import datetime
import json
import re
from copy import deepcopy

import arrow
from cds_rdm.clc_sync.models import CDSToCLCSyncModel
from cds_rdm.legacy.models import CDSMigrationLegacyRecord
from cds_rdm.legacy.resolver import get_pid_by_legacy_recid
from cds_rdm.minters import legacy_recid_minter
from invenio_access.permissions import system_identity
from invenio_accounts.models import User
from invenio_db import db
from invenio_pidstore.errors import PIDAlreadyExists
from invenio_pidstore.models import PersistentIdentifier
from invenio_rdm_migrator.load.base import Load
from invenio_rdm_records.proxies import current_rdm_records_service
from invenio_records.systemfields.relations import InvalidRelationValue
from marshmallow import ValidationError

from cds_migrator_kit.errors import CDSMigrationException, ManualImportRequired
from cds_migrator_kit.reports.log import RDMJsonLogger


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
        entries=None,
        dry_run=False,
        legacy_pids_to_redirect=None,
    ):
        """Constructor."""
        self.dry_run = dry_run
        self.legacy_pids_to_redirect = {}
        self.clc_sync = False

        if legacy_pids_to_redirect is not None:
            with open(legacy_pids_to_redirect, "r") as fp:
                self.legacy_pids_to_redirect = json.load(fp)

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
                                "legacy_recid": recid,
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
                migration_logger.add_log(exc, record=entry)
                raise e

    def _load_parent_access(self, draft, entry):
        """Load access rights."""
        parent = draft._record.parent

        # Set parent access from entry data
        access = entry["parent"]["json"]["access"]
        parent.access = access

        parent.commit()

    def _load_parent_access_grants(self, draft, access_dict):
        """Load access grants from metadata."""
        record = draft._record
        parent = record.parent
        identity = system_identity
        migration_logger = RDMJsonLogger()

        metadata = access_dict.get("meta", "")
        permission = "view"  # Default permission for grants

        if metadata == "SSO":
            groups = ["cern-personnel"]
            emails = []
        else:
            # Warn about unexpected formats
            if not any(
                kw in metadata for kw in ("firerole: allow group", "allow email")
            ):
                migration_logger.add_log(
                    f"Unexpected access grant format: {metadata}", record=record
                )

            groups, emails = CDSRecordServiceLoad._parse_access_metadata(metadata)

        # Helper function for creating and validating grants
        def _create_grant(subject_type, subject_id):
            try:
                grant_data = {
                    "grants": [
                        {
                            "subject": {"type": subject_type, "id": str(subject_id)},
                            "permission": permission,
                        }
                    ]
                }
                # Validate the grant data against the schema
                current_rdm_records_service.access.schema_grants.load(
                    grant_data,
                    context={"identity": identity},
                    raise_errors=True,
                )

                # Create the grant
                grant = parent.access.grants.create(
                    subject_type=subject_type,
                    subject_id=subject_id,
                    permission=permission,
                    origin="migrated",
                )
                # Validate subject existence
                if not current_rdm_records_service.access._validate_grant_subject(
                    identity, grant
                ):
                    raise ValidationError("Could not find the specified subject.")

            except ValidationError as e:
                exc = ManualImportRequired(
                    message=str(e),
                    field="access",
                    subfield="subject.id",
                    stage="load",
                    recid=record.get("id"),
                    priority="warning",
                )
                migration_logger.add_log(exc, record=record)
            except Exception as e:
                migration_logger.add_log(f"Grant creation failed: {e}", record=record)

        # Apply group-based grants
        for group in groups:
            _create_grant(subject_type="role", subject_id=group)

        # Apply user-based grants
        for email in emails:
            user = User.query.filter_by(email=email).one_or_none()
            if user:
                _create_grant(subject_type="user", subject_id=user.id)
            else:
                migration_logger.add_log(
                    f"User not found for email: {email}", record=record
                )

        parent.commit()

    def _load_record_access(self, draft, access_dict):
        record = draft._record

        record.access = access_dict["access_obj"]
        record.commit()

    def _load_communities(self, draft, entry):
        parent = draft._record.parent
        communities = entry["parent"]["json"]["communities"]["ids"]
        for community in communities:
            parent.communities.add(community)
        parent.communities.default = entry["parent"]["json"]["communities"]["default"]
        parent.commit()

    def _after_publish_update_dois(self, identity, record, entry):
        """Update migrated DOIs post publish."""
        migrated_pids = entry["record"]["json"]["pids"]
        for pid_type, identifier in migrated_pids.items():
            if pid_type == "doi":
                # If a DOI was already minted from legacy then on publish the datacite
                # will return a warning that "This DOI has already been taken"
                # In that case, we edit and republish to force an update of the doi with
                # the new published metadata as in the new system we have more information available
                _draft = current_rdm_records_service.edit(identity, record["id"])
                current_rdm_records_service.publish(identity, _draft["id"])

    def _after_publish_update_created(self, record, entry, version):
        """Update created timestamp post publish.

        Ensures that the `created` timestamp is correctly set, preferring:
        1. The original legacy system value for the version.
        2. The record's creation date if there are no files.
        3. Today's date if the original value and file creation date is missing.
        """
        creation_date = entry["record"]["created"].datetime.replace(tzinfo=None)

        versions = entry.get("versions", {})
        version_data = versions.get(version, {})
        # Use records creation date for the first version, unless it was created today
        # which means it was missing so we rely on the files creation date
        creation_date_is_today = creation_date.date() == datetime.date.today()
        if version_data.get("files") and not version == 1 or creation_date_is_today:
            # Subsequent versions should use the file creation date, instead of the record creation date,
            # which is stored as the publication date in the version data
            creation_date = version_data["publication_date"].datetime.replace(
                tzinfo=None
            )

        record._record.model.created = creation_date
        record._record.commit()

    def _after_publish_mint_recid(self, record, entry, version):
        """Mint legacy ids for redirections assigned to the parent."""
        legacy_recid = entry["record"]["recid"]
        if version == 1:
            # it seems more intuitive if we mint the lrecid for parent
            # but then we get a double redirection
            legacy_recid_minter(legacy_recid, record._record.parent.model.id)

    def _after_publish_update_files_created(self, record, entry, version):
        """Update the created date of the files post publish."""
        # Fix the `created` timestamp forcing the one from the legacy system
        # Force the created date. This can be done after publish as the service
        # overrides the `created` date otherwise.
        versions = entry.get("versions", {})
        version_data = versions.get(version, {})
        files = version_data.get("files", {})
        for _, file_data in files.items():
            file = record._record.files.entries[file_data["key"]]
            file.model.created = arrow.get(file_data["creation_date"]).datetime.replace(
                tzinfo=None
            )
            file.commit()

    def _after_publish(self, identity, published_record, entry, version):
        """Run fixes after record publish."""
        self._after_publish_update_dois(identity, published_record, entry)
        self._after_publish_update_created(published_record, entry, version)
        self._after_publish_mint_recid(published_record, entry, version)
        self._after_publish_update_files_created(published_record, entry, version)
        db.session.commit()

    def _pre_publish(self, identity, entry, version, draft):
        """Create and process draft before publish."""
        versions = entry["versions"]
        files = versions[version]["files"]
        publication_date = versions[version]["publication_date"]
        access = versions[version]["access"]

        if version == 1 or (version > 1 and draft is None):
            # when draft is None, it means the initial version one was hard deleted
            # and we don't have index 1
            # we decided to skip it and act normal
            draft = current_rdm_records_service.create(
                identity, data=entry["record"]["json"]
            )
            if draft.errors:
                raise ManualImportRequired(
                    message=f"{str(draft.errors)}: {str(entry['record']['json'])}",
                    field="validation",
                    stage="load",
                    recid=entry["record"]["recid"],
                    priority="warning",
                    value=draft._record.pid.pid_value,
                    subfield=None,
                )
            # TODO we can use unit of work when it is moved to invenio-db module
            self._load_parent_access(draft, entry)
            self._load_parent_access_grants(draft, access)
            self._load_communities(draft, entry)
            db.session.commit()
        else:
            draft = current_rdm_records_service.new_version(identity, draft["id"])
            draft_dict = draft.to_dict()
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
                identity, draft["id"], data=missing_data
            )
        self._load_record_access(draft, access)
        self._load_files(draft, entry, files)

        return draft

    def _load_versions(self, entry, logger):
        """Load other versions of the record."""
        versions = entry["versions"]
        legacy_recid = entry["record"]["recid"]

        identity = system_identity

        records = []
        # initial value of draft. If different file versions identified then the first
        # created draft is used to populate all newer versions
        draft = None
        for version in versions.keys():
            # Create and prepare draft
            draft = self._pre_publish(identity, entry, version, draft)

            # Publish draft
            published_record = current_rdm_records_service.publish(
                identity, draft["id"]
            )
            # Run after publish fixes
            self._after_publish(identity, published_record, entry, version)
            records.append(published_record._record)

        if records:
            record_state_context = self._load_record_state(legacy_recid, records)
            # Dump the computed record state. This is useful to migrate then the record stats
            if record_state_context:
                logger.add_record_state(record_state_context)
                return record_state_context

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

    def _have_migrated_recid(self, recid):
        """Check if we have minted `lrecid` pid."""
        pid = PersistentIdentifier.query.filter_by(
            pid_type="lrecid",
            pid_value=recid,
        ).one_or_none()
        return pid is not None

    def _should_skip_recid(self, recid):
        """Check if recid should be skipped."""
        if recid in self.legacy_pids_to_redirect or self._have_migrated_recid(recid):
            return True
        return False

    def _after_load_clc_sync(self, record_state):
        if self.clc_sync:
            sync = CDSToCLCSyncModel(
                parent_record_pid=record_state["parent_recid"],
                status="P",
                auto_sync=False,
            )
            db.session.add(sync)
            db.session.commit()

    def _load(self, entry):
        """Use the services to load the entries."""
        if entry:

            self.clc_sync = deepcopy(entry.get("_clc_sync", False))
            if "_clc_sync" in entry:
                del entry["_clc_sync"]

            recid = entry.get("record", {}).get("recid", {})

            if self._should_skip_recid(recid):
                return

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
                        self._after_load_clc_sync(recid_state_after_load)
                migration_logger.add_success(recid)
            except ManualImportRequired as e:
                migration_logger.add_log(e, record=entry)
            except PIDAlreadyExists as e:
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
                    subfield="PID",
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
        """Post migration process."""
        migration_logger = RDMJsonLogger()
        for legacy_src_pid, legacy_dest_pid in self.legacy_pids_to_redirect.items():
            if self._have_migrated_recid(legacy_src_pid):
                continue
            try:
                parent_dest_pid = get_pid_by_legacy_recid(str(legacy_dest_pid))
                assert str(parent_dest_pid.status) == "R"
                legacy_recid_minter(legacy_src_pid, parent_dest_pid.object_uuid)
                db.session.commit()
                migration_logger.add_success(legacy_src_pid)
            except Exception as exc:
                db.session.rollback()
                migration_logger.add_log(
                    f"Failed to redirect {legacy_src_pid} to {legacy_dest_pid}: {str(exc)}",
                    record={"recid": legacy_src_pid},
                )

    def _parse_access_metadata(metadata):
        groups = []
        emails = []

        # Normalize newlines
        metadata = metadata.replace("\r\n", "\n")

        # Match group entries: all quoted values after 'allow group'
        group_block = re.search(r'allow group\s+((?:"[^"]+",?\s*)+)', metadata)
        if group_block:
            group_values = re.findall(r'"([^"]+)"', group_block.group(1))
            for g in group_values:
                # Remove legacy '[CERN]' suffix if present
                if g.endswith(" [CERN]"):
                    g = g.rsplit(" [CERN]", 1)[0]
                groups.append(g.strip())

        # Match email entries: all quoted values after 'allow email'
        email_block = re.search(r'allow email\s+((?:"[^"]+",?\s*)+)', metadata)
        if email_block:
            email_values = re.findall(r'"([^"]+)"', email_block.group(1))
            emails.extend(email_values)

        return groups, emails
