# -*- coding: utf-8 -*-
#
# Copyright (C) 2022 CERN.
#
# CDS-RDM is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""CDS-RDM transform step module."""
import datetime
import logging
import re
from collections import OrderedDict
from copy import deepcopy
from pathlib import Path
from typing import Any, Dict, NamedTuple, Optional

import arrow
from cds_dojson.marc21.utils import create_record
from cds_rdm.legacy.models import CDSMigrationAffiliationMapping
from cds_rdm.legacy.resolver import get_pid_by_legacy_recid
from dateutil.parser import ParserError
from flask import current_app
from idutils.validators import is_doi
from invenio_access.permissions import system_identity
from invenio_accounts.models import User
from invenio_pidstore.models import PersistentIdentifier, PIDStatus
from invenio_rdm_migrator.logging import Logger
from invenio_rdm_records.proxies import current_rdm_records_service, current_record_communities_service
from sqlalchemy.exc import NoResultFound

from cds_migrator_kit.errors import (
    ManualImportRequired,
    MissingRequiredField,
    MultipleModelsMatched,
    RecordFlaggedCuration,
    RestrictedFileDetected,
    UnexpectedValue,
)
from cds_migrator_kit.rdm.records.transform.config import (
    FILE_SUBFORMATS_TO_DROP,
    PIDS_SCHEMES_ALLOWED,
    PIDS_SCHEMES_TO_DROP,
)
from cds_migrator_kit.rdm.records.transform.entry_types import (
    MigrationEntry,
    ParentAccess,
    ParentEntry,
    ParentJson,
    RecordEntry,
    VersionEntry,
)
from cds_migrator_kit.rdm.records.transform.mappers.base import RecordTransformContext
from cds_migrator_kit.rdm.records.transform.mappers.record import AccessGrantsMapper
from cds_migrator_kit.rdm.records.transform.mappers.registry import (
    CUSTOM_FIELD_MAPPERS,
    METADATA_MAPPERS,
)
from cds_migrator_kit.transform.dumper import CDSRecordDump
from cds_migrator_kit.transform.errors import LossyConversion

cli_logger = logging.getLogger("migrator")


class CDSToRDMRecordEntry:
    """Transform CDS record to RDM record.

    Builds the ``record`` content dict consumed by
    ``CDSToRDMRecordTransform`` - not the invenio_rdm_migrator "generic RDM
    record" envelope (this class deliberately does not use that framework's
    ``RDMRecordEntry.transform()``/``_load_partial`` orchestration, since the
    CDS legacy shape and the CDS loader's needs don't match it).
    """

    def __init__(
        self,
        partial=False,
        missing_users_dir=None,
        missing_users_filename="people.csv",
        affiliations_mapping=None,
        dry_run=False,
        collection=None,
        restricted=False,
        migration_logger=None,
        record_state_logger=None,
        access_grants_view=None,
        preferred_model=None,
    ):
        """Constructor."""
        self.partial = partial
        self.missing_users_dir = missing_users_dir
        self.missing_users_filename = missing_users_filename
        self.affiliations_mapping = affiliations_mapping
        self.dry_run = dry_run
        self.collection = collection
        self.restricted = restricted
        self.access_grants_view = access_grants_view
        self.migration_logger = migration_logger
        self.record_state_logger = record_state_logger
        # populated by transform(); an ETL-envelope concern (does the
        # parent need a CLC sync after load), not record content, so it
        # isn't part of the dict transform() returns - the caller
        # (CDSToRDMRecordTransform._record()) reads it off this instance
        # instead. See CDSToRDMRecordTransform._transform()'s docstring.
        self.clc_sync = None
        self.preferred_model = preferred_model
        self.ep_approval_request = None
        super().__init__(partial)

    def _created(self, entry):
        return entry["created"]

    def _updated(self, record_dump):
        """Returns the creation date of the record."""
        return record_dump.data["record"][0]["modification_datetime"]

    def _version_id(self, entry):
        """Returns the version id of the record."""
        return 1

    def _access(self, entry, record_dump):
        record_restriction = (
            r[0] if isinstance(r := entry.get("record_restriction"), list) else r
        )
        restrictions = "restricted" if self.restricted else record_restriction
        if not restrictions:
            raise RecordFlaggedCuration(
                message="record restriction not found make sure the record should be public",
                stage="transform",
                field="record_restriction",
            )
        return restrictions

    def _index(self, record_dump):
        """Returns the version index of the record."""
        return 1  # in legacy we start at 0

    def _recid(self, record_dump):
        """Returns the recid of the record."""
        return str(record_dump.data["recid"])

    def _pids(self, json_entry):
        DATACITE_PREFIX = current_app.config["DATACITE_PREFIX"]

        pids = json_entry.get("_pids", {})
        output_pids = deepcopy(pids)
        for key, identifier in pids.items():
            # ignoring some pids
            if key.upper() in PIDS_SCHEMES_TO_DROP:
                del output_pids[key]

            elif key and key.upper() not in PIDS_SCHEMES_ALLOWED:
                raise UnexpectedValue(
                    field=key,
                    subfield="2",
                    message="Unexpected PID scheme (should be DOI)",
                    priority="warning",
                    stage="transform",
                    value=identifier,
                )
            elif not key and is_doi(identifier):
                # assume it is DOI
                key = "DOI"
            if key.upper() == "DOI":
                doi_identifier = deepcopy(identifier)
                doi = identifier["identifier"]

                if doi.startswith(DATACITE_PREFIX):
                    doi_identifier["provider"] = "datacite"
                else:
                    doi_identifier["provider"] = "external"

                if doi.startswith(DATACITE_PREFIX) or doi.startswith("10.5170"):
                    if not json_entry.get("publisher"):
                        json_entry["publisher"] = "CERN"
                output_pids["doi"] = doi_identifier
        if output_pids:
            return output_pids
        else:
            return {}

    def _files(self, record_dump):
        """Transform the files of a record."""
        record_dump.prepare_files()
        files = record_dump.files
        return {"enabled": bool(files)}

    def _communities(self, json_entry):
        return json_entry.get("communities", [])

    def _owner(self, json_entry):
        email = json_entry.get("submitter")
        return email

    def _metadata(self, json_entry, entry):
        """Build the metadata dict by running the composed field mappers."""
        ctx = RecordTransformContext(
            json_entry=json_entry,
            entry=entry,
            migration_logger=self.migration_logger,
            affiliations_mapping=self.affiliations_mapping,
        )
        metadata = ctx.metadata
        # Order matters: ResourceTypeMapper must run before TitleMapper reads
        # metadata["resource_type"]; see mappers/registry.py.
        for mapper in METADATA_MAPPERS:
            metadata[mapper.id] = mapper.map_value(ctx)

        # filter empty keys
        helper_keys = [
            "recid",
            "legacy_recid",
            "agency_code",
            "submitter",
            "status_week_date",
            "record_restriction",
            "access_grants",
            "custom_fields",
            "_pids",
            "internal_notes",
            "ep_approval",
        ]
        keys = deepcopy(list(json_entry.keys()))
        for item in helper_keys:
            if item in keys:
                keys.remove(item)

        forgotten_keys = [key for key in keys if key not in list(metadata.keys())]
        if forgotten_keys:
            raise ManualImportRequired("Unassigned metadata key", value=forgotten_keys)
        return {k: v for k, v in metadata.items() if v}

    def _custom_fields(self, json_entry, json_output):
        """Build the custom_fields dict by running the composed field mappers."""
        ctx = RecordTransformContext(
            json_entry=json_entry,
            entry=json_entry,
            migration_logger=self.migration_logger,
            json_output=json_output,
        )
        for mapper in CUSTOM_FIELD_MAPPERS:
            mapper.apply(ctx)
        custom_fields = ctx.custom_fields

        forgotten_keys = [
            key
            for key in json_entry["custom_fields"].keys()
            if key not in custom_fields.keys()
        ]
        if forgotten_keys:
            raise ManualImportRequired(
                "Unassigned custom field key", value=forgotten_keys
            )
        # filter out null values
        return {k: v for k, v in custom_fields.items() if v}

    def _verify_publication_date(self, entry, json_data):
        """Verify creation date.

        If the record has no files (file creation date will be used as record
        creation date) and no creation date, raise an exception.
        """
        if not entry.get("files") and not (
            json_data.get("status_week_date") or json_data.get("publication_date")
        ):
            raise ManualImportRequired(
                message="Record missing publication date",
                field="validation",
                stage="transform",
                description="Record has no files and no publication date",
                recid=entry["recid"],
                priority="warning",
                value=None,
                subfield=None,
            )

    def transform(self, entry) -> RecordEntry:
        """Transform a record single entry."""
        record_dump = CDSRecordDump(
            entry,
            preferred_model=self.preferred_model,
        )

        record_dump.prepare_revisions()

        if record_dump.multiple_models_warning:
            w = record_dump.multiple_models_warning
            recid = entry.get("recid") or entry.get("record", {}).get("recid")
            matched = re.findall(r"\['(\w+)',", w.message or "")
            self.migration_logger.add_information(
                str(recid),
                {
                    "type": w.type,
                    "error": w.description,
                    "message": w.message,
                    "value": ", ".join(matched),
                    "priority": "warning",
                },
            )

        timestamp, json_data = record_dump.latest_revision

        self._verify_publication_date(entry, json_data)

        self.record_state_logger.add_record(json_data)

        self.clc_sync = deepcopy(json_data.get("_clc_sync", False))
        if "_clc_sync" in json_data:
            del json_data["_clc_sync"]

        request_data = json_data.pop("request_data", None)
        if request_data:
            reviewer_errors = request_data.pop("_reviewer_errors", [])
            for error in reviewer_errors:
                self.migration_logger.add_information(
                    entry["recid"],
                    error,
                )

        record_ctx = RecordTransformContext(
            json_entry=json_data,
            entry=entry,
            access_grants_view=self.access_grants_view,
        )
        record_json_output = {
            "files": self._files(record_dump),
            "pids": self._pids(json_data),
            "metadata": self._metadata(json_data, entry),
            "access_grants": AccessGrantsMapper().map_value(record_ctx),
        }

        custom_fields = self._custom_fields(json_data, record_json_output)
        internal_notes = json_data.get("internal_notes")

        if custom_fields:
            record_json_output.update({"custom_fields": custom_fields})
        if internal_notes:
            record_json_output.update(
                {"internal_notes": json_data.get("internal_notes")}
            )

        access = None
        try:
            access = self._access(json_data, record_dump)
        except RecordFlaggedCuration as exc:
            self.migration_logger.add_information(
                entry["recid"],
                {"message": exc.message, "value": exc.value},
            )
        return {
            "created": record_dump.first_created,
            "updated": self._updated(record_dump),
            "version_id": self._version_id(record_dump),
            "index": self._index(record_dump),
            "recid": self._recid(record_dump),
            "communities": self._communities(json_data),
            "json": record_json_output,
            "access_status": access,
            "owned_by": self._owner(json_data),
            # record-scoped extras, read by load.py nested under "record"
            "_request_data": request_data,
            "ep_approval": entry.get("ep_approval", []),
        }


class RecordBuildResult(NamedTuple):
    """Private return type of ``CDSToRDMRecordTransform._record()``.

    Pairs the record content with the one ETL-envelope extra
    (``clc_sync``) that can only be computed as a side effect of building
    it - see ``CDSToRDMRecordEntry.clc_sync``.
    """

    record: RecordEntry
    clc_sync: Any


class CDSToRDMRecordTransform:
    """Assembles the ETL entry consumed by ``CDSRecordServiceLoad``.

    Wraps the ``record`` content built by ``CDSToRDMRecordEntry`` together
    with ``versions``/``parent`` (computed here) and the ETL-envelope extras
    that aren't record content (currently just ``_original_dump`` and
    ``_clc_sync`` - see ``_transform()``).
    """

    def __init__(
        self,
        workers=None,
        throw=True,
        files_dump_dir=None,
        missing_users=None,
        communities_ids=None,
        dry_run=False,
        collection=None,
        restricted=False,
        plots=False,
        migration_logger=None,
        record_state_logger=None,
        access_grants_view=None,
        preferred_model=None,
    ):
        """Constructor."""
        self._workers = workers
        self._throw = throw
        self._logger = None
        self.files_dump_dir = Path(files_dump_dir).absolute().as_posix()
        self.missing_users_dir = Path(missing_users).absolute().as_posix()
        self.communities_ids = communities_ids
        self.dry_run = dry_run
        self.collection = collection
        self.restricted = restricted
        self.access_grants_view = access_grants_view
        self.plots = plots
        self.migration_logger = migration_logger
        self.record_state_logger = record_state_logger
        self.preferred_model = preferred_model
        self.db_state = {"affiliations": CDSMigrationAffiliationMapping}

    @property
    def logger(self):
        """Return the base logger."""
        if self._logger is None:
            self._logger = Logger.get_logger()
        return self._logger

    def _communities_ids(self, entry, record):
        communities = record.get("communities", [])
        communities = self.communities_ids + [slug for slug in communities]
        if communities:
            return {"ids": communities, "default": self.communities_ids[0]}
        return {}

    def _parent(self, entry, record: RecordEntry) -> ParentEntry:

        email = record["owned_by"]
        if not email:
            owner = "system"
        else:
            try:
                user = User.query.filter_by(email=email).one()
                owner = user.id
            except NoResultFound:
                raise UnexpectedValue(
                    message=f"{email} not found - did you run user migration?",
                    stage="transform",
                    recid=entry["legacy_recid"],
                    value=email,
                    priority="critical",
                )

        return ParentEntry(
            created=record["created"],  # same as the record
            updated=record["updated"],  # same as the record
            version_id=record["version_id"],
            json=ParentJson(
                # loader is responsible for creating/updating if the PID exists.
                # this part will be simply omitted
                id=f'{record["recid"]}-parent',
                access=ParentAccess(owned_by={"user": owner}),
                communities=self._communities_ids(entry, record),
            ),
        )

    def _transform(self, entry) -> Optional[MigrationEntry]:
        """Transform a single entry."""
        # creates the output structure for load step
        migration_logger = self.migration_logger
        try:
            built = self._record(entry)
            record = built.record

            if record:
                return {
                    "record": record,
                    "versions": self._versions(entry, record),
                    "parent": self._parent(entry, record),
                    "_original_dump": entry,
                    "_clc_sync": built.clc_sync,
                }
        except (
                LossyConversion,
                RestrictedFileDetected,
                UnexpectedValue,
                ManualImportRequired,
                MissingRequiredField,
                MultipleModelsMatched,
        ) as e:
            migration_logger.add_log(e, record=entry)

    def _record(self, entry) -> RecordBuildResult:
        # could be in draft as well, depends on how we decide to publish
        entry_builder = CDSToRDMRecordEntry(
            missing_users_dir=self.missing_users_dir,
            affiliations_mapping=self.db_state["affiliations"],
            dry_run=self.dry_run,
            collection=self.collection,
            restricted=self.restricted,
            access_grants_view=self.access_grants_view,
            migration_logger=self.migration_logger,
            record_state_logger=self.record_state_logger,
            preferred_model=self.preferred_model,
        )
        record = entry_builder.transform(entry)
        return RecordBuildResult(record=record, clc_sync=entry_builder.clc_sync)

    def _draft(self, entry):
        return None

    def _parse_file_status(self, file_status):
        pass

    def _versions(self, entry, record: RecordEntry) -> Dict[int, VersionEntry]:

        def compute_access(file, record_access):

            if file is None:
                return {
                    "access_obj": {
                        "record": record_access,
                        "files": record_access,
                    }
                }

            if not file["status"]:
                return {
                    "access_obj": {
                        "record": record_access,
                        "files": record_access,
                    }
                }

            if file["status"]:
                # if we have anything in the status string,
                # it means the file is restricted
                # we pass this information to parse later in load step
                self.migration_logger.add_information(
                    str(file["recid"]),
                    {
                        "message": "Record has individual file restrictions",
                        "value": file["status"],
                    },
                )
                return {
                    "access_obj": {"record": record_access, "files": "restricted"},
                    "meta": file["status"],
                }

        def should_skip_file(file_dump):
            if file_dump["subformat"] in FILE_SUBFORMATS_TO_DROP:
                self.migration_logger.add_information(
                    str(file_dump["recid"]),
                    {
                        "message": f"File subformat {file_dump['subformat']} dropped.",
                        "value": file_dump["full_name"],
                    },
                )
                return True

            if not self.plots and file_dump["type"] == "Plot":
                # skip figures if configuration says so
                self.migration_logger.add_information(
                    str(file_dump["recid"]),
                    {
                        "message": f"Plot file dropped.",
                        "value": file_dump["full_name"],
                    },
                )
                return True
            if file_dump["hidden"]:
                # skip hidden files
                self.migration_logger.add_information(
                    str(file_dump["recid"]),
                    {
                        "message": f"Hidden file dropped.",
                        "value": file_dump["full_name"],
                    },
                )
                return True
            return False

        def compute_files(file_dump, versions_dict):
            legacy_path_root = Path("/opt/cdsweb/var/data/files/")
            tmp_eos_root = Path(self.files_dump_dir)
            full_path = Path(file_dump["full_path"])

            versions_dict[file_dump["version"]]["files"].update(
                {
                    file_dump["full_name"]: {
                        "eos_tmp_path": tmp_eos_root
                                        / full_path.relative_to(legacy_path_root),
                        "id_bibdoc": file_dump["bibdocid"],
                        "key": file_dump["full_name"],
                        "metadata": {
                            "description": file_dump["description"],
                            "name": file_dump["name"],
                            "status": file_dump["status"],
                            "original_path": file_dump["path"],
                            "comment": file_dump["comment"],
                        },
                        "mimetype": file_dump["mime"],
                        "checksum": file_dump["checksum"],
                        "version": file_dump["version"],
                        "access": file_dump["status"],
                        "type": file_dump["type"],
                        "creation_date": arrow.get(file_dump["creation_date"])
                        .replace(tzinfo=None)
                        .date()
                        .isoformat(),
                    }
                }
            )

        # grouping draft attributes by version
        # we build temporary representation of each version
        # {"1": {"access": {...}, "files": [], "publication_date": None}
        # {"2": {"access": {...}, "files": [], "publication_date: "2021-04-21"}
        versions = OrderedDict()
        # we start versions from files (because this is the only way of
        # mapping version of files to version of records from legacy)
        _files = entry["files"]
        record_access = record["access_status"]
        for file in _files:
            if should_skip_file(file):
                continue
            if file["version"] not in versions:
                versions[file["version"]] = {
                    "files": {},
                    "publication_date": arrow.get(file["creation_date"]).replace(
                        tzinfo=None
                    ),
                    "access": compute_access(file, record_access),
                }

            compute_files(file, versions)

        versioned_files = {}
        # creates a collection of files per each version
        # lets say record has 2 files: A & B
        # if for file A new version was uploaded (version 2),
        # we need to preserve the file B for version 2 of the record
        for version in versions.keys():
            versioned_files |= versions.get(version, {}).get("files")
            versions[version]["files"] = deepcopy(versioned_files)
        publication_date = record["json"]["metadata"]["publication_date"]

        if not versioned_files:
            # Record has no files. Add metadata-only record as single version
            versions[1] = {
                "files": {},
                "publication_date": publication_date,
                "access": compute_access(
                    None, record_access
                ),  # public metadata and files
            }

        return versions

    def _record_files(self, entry, record):
        """Record files entries transform."""
        # TO implement if we decide not to go via draft publish
        return []

    def _load_migrated_recids(self):
        """Load all already-migrated legacy record IDs into a set once."""
        return {
            pid.pid_value
            for pid in PersistentIdentifier.query.filter_by(
                pid_type="lrecid",
                status=PIDStatus.REGISTERED,
            ).all()
        }

    def should_skip(self, entry):
        return str(entry["recid"]) in self._migrated_recids

    def _existing_record_is_restricted(self, record_id):
        """Check the current access state of an already-migrated RDM record.

        Reads live from the record service (DB) rather than the legacy
        MARCXML, since access restrictions may have been changed in RDM
        after migration and the legacy record data would be stale.
        """
        record = current_rdm_records_service.read_latest(
            system_identity, id_=record_id
        )
        access = record.data.get("access", {})
        return access.get("record") != "public" or access.get("files") != "public"

    def run(self, entries):
        """Run transformation step."""
        self._migrated_recids = self._load_migrated_recids()
        for entry in entries:
            if self.should_skip(entry):
                recid = entry["recid"]
                try:
                    parent_pid = get_pid_by_legacy_recid(str(recid))
                    # we don't check here if the record has 980:MIGRATED
                    # because this does not add anything and it should not be deciding factor.
                    # if the legacy recid has been minted - we know already the record has been migrated
                    if self._existing_record_is_restricted(parent_pid.pid_value):
                        # checking if we need to be more careful while assigning access
                        # to various communities
                        raise ManualImportRequired(
                            message=(
                                "Existing record is restricted or has "
                                "restricted files; not adding to "
                                "communities automatically"
                            ),
                            field="access",
                            stage="transform",
                            recid=recid,
                            priority="warning",
                        )
                    # bulk_add resolves record_ids via RDMRecord.pid.resolve(),
                    # which -- unlike read_latest -- has no fallback for a
                    # parent-level recid (what get_pid_by_legacy_recid returns).
                    # We must pass the record's own recid instead.
                    record_item = current_rdm_records_service.read_latest(
                        system_identity, id_=parent_pid.pid_value
                    )
                    for community_id in self.communities_ids:
                        current_record_communities_service.bulk_add(
                            system_identity,
                            community_id,
                            [record_item.id],
                        )
                except NoResultFound:
                    self.migration_logger.add_information(
                        recid,
                        {
                            "message": (
                                "Problem with PIDs - minted legacy recid but"
                                "no corresponding parent record found."
                            ),
                            "value": recid,
                        },
                    )
                except ManualImportRequired as exc:
                    self.migration_logger.add_log(exc, record=entry)

                self.migration_logger.add_information(
                    recid,
                    {
                        "message": "Record already migrated, skipping,"
                                   " added existing {} to communities {}".format(
                            recid, self.communities_ids),
                        "value": recid,
                    },
                )
                self.migration_logger.finalise_record(recid)
                continue
            try:
                yield self._transform(entry)
            except Exception:
                self.logger.exception(entry, exc_info=True)
                if self._throw:
                    raise
                continue

    #
    #
    # "files": [
    #   {
    #     "comment": null,
    #     "status": "firerole: allow group \"council-full [CERN]\"\ndeny until \"1996-02-01\"\nallow all",
    #     "version": 1,
    #     "encoding": null,
    #     "creation_date": "2009-11-03T12:29:06+00:00",
    #     "bibdocid": 502379,
    #     "mime": "application/pdf",
    #     "full_name": "CM-P00080632-e.pdf",
    #     "superformat": ".pdf",
    #     "recids_doctype": [[32097, "Main", "CM-P00080632-e.pdf"]],
    #     "path": "/opt/cdsweb/var/data/files/g50/502379/CM-P00080632-e.pdf;1",
    #     "size": 5033532,
    #     "license": {},
    #     "modification_date": "2009-11-03T12:29:06+00:00",
    #     "copyright": {},
    #     "url": "http://cds.cern.ch/record/32097/files/CM-P00080632-e.pdf",
    #     "checksum": "ed797ce5d024dcff0040db79c3396da9",
    #     "description": "English",
    #     "format": ".pdf",
    #     "name": "CM-P00080632-e",
    #     "subformat": "",
    #     "etag": "\"502379.pdf1\"",
    #     "recid": 32097,
    #     "flags": [],
    #     "hidden": false,
    #     "type": "Main",
    #     "full_path": "/opt/cdsweb/var/data/files/g50/502379/CM-P00080632-e.pdf;1"
    #   },]
