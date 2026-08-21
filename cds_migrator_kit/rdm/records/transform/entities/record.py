# -*- coding: utf-8 -*-
#
# Copyright (C) 2022-2026 CERN.
#
# CDS-RDM is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""The RDM record's own content - ``MigrationEntry["record"]``."""
from copy import deepcopy
from typing import Any, List, Optional, TypedDict, Union

from flask import current_app
from idutils.validators import is_doi

from cds_migrator_kit.errors import (
    ManualImportRequired,
    RecordFlaggedCuration,
    UnexpectedValue,
)
from cds_migrator_kit.rdm.records.transform.config import (
    PIDS_SCHEMES_ALLOWED,
    PIDS_SCHEMES_TO_DROP,
)
from cds_migrator_kit.rdm.records.transform.mappers.base import RecordTransformContext
from cds_migrator_kit.rdm.records.transform.mappers.registry import (
    CUSTOM_FIELD_MAPPERS,
    METADATA_MAPPERS,
)


class RecordBodyRequired(TypedDict):
    """Required keys of ``RecordEntryData["body"]``."""

    files: dict
    pids: dict
    metadata: dict


class RecordBody(RecordBodyRequired, total=False):
    """The RDM record body: ``RecordEntryData["body"]``.

    ``current_rdm_records_service.create()``'s ``data`` argument - built by
    ``RecordEntry.transform()``. Deliberately excludes:

    - ``access``: set per-version, after creation, via
      ``VersionEntry["access"]`` (see ``load.py::_load_record_access``)
      rather than at record-create time.
    - ``access_grants``: RDM parent-level, not record-level - see
      ``RecordParent.access_grants`` in ``entities/parent.py``.
    """

    custom_fields: dict
    internal_notes: Any


class RecordEntryData(TypedDict):
    """A single record's content - ``MigrationEntry["record"]``.

    Built by ``RecordEntry.transform()``. Everything here is
    record-scoped (as opposed to ``MigrationEntry``'s other top-level keys,
    which are ETL-envelope-scoped - see that type's docstring).
    """

    created: str
    updated: str
    version_id: int
    index: int
    recid: str
    communities: List[str]
    # The record's actual content, handed wholesale to
    # current_rdm_records_service.create()/schema.load() - see RecordBody.
    body: RecordBody
    # None when RecordFlaggedCuration was raised and caught - see
    # RecordEntry._access()/.transform().
    access_status: Optional[str]
    owned_by: Union[str, int]


class RecordEntry:
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
    ):
        """Constructor."""
        self.partial = partial
        self.missing_users_dir = missing_users_dir
        self.missing_users_filename = missing_users_filename
        self.affiliations_mapping = affiliations_mapping
        self.dry_run = dry_run
        self.collection = collection
        self.restricted = restricted
        self.migration_logger = migration_logger
        self.record_state_logger = record_state_logger

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

    def _custom_fields(self, json_entry):
        """Build the custom_fields dict by running the composed field mappers.

        Must run before ``_metadata()``: a couple of these mappers add a
        fallback ``json_entry["subjects"]`` entry when a vocabulary lookup
        fails, which metadata's own SubjectsMapper then picks up like any
        other subject - see DepartmentsMapper.
        """
        ctx = RecordTransformContext(
            json_entry=json_entry,
            entry=json_entry,
            migration_logger=self.migration_logger,
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

    def transform(self, entry, record_dump, json_data) -> RecordEntryData:
        """Transform a record single entry.

        :param entry: the original harvested legacy entry.
        :param record_dump: the ``CDSRecordDump`` for ``entry`` - produced
            by ``CDSToRDMRecordTransform.transform_xml_to_json()``.
        :param json_data: the DOJSON-processed record data
            (``record_dump.latest_revision``'s content) - also produced by
            ``transform_xml_to_json()``, passed in separately since this
            method mutates it in place as it builds the record.
        """
        self._verify_publication_date(entry, json_data)

        # custom_fields runs before metadata: see _custom_fields()'s docstring.
        custom_fields = self._custom_fields(json_data)

        record_json_output = {
            "files": self._files(record_dump),
            "pids": self._pids(json_data),
            "metadata": self._metadata(json_data, entry),
            "internal_notes": json_data.get("internal_notes"),
            "custom_fields": custom_fields,
        }
        # drop empty optional keys rather than sending them to the RDM
        # record schema as null/{}
        for key in ("internal_notes", "custom_fields"):
            if not record_json_output[key]:
                del record_json_output[key]

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
            "body": record_json_output,
            "access_status": access,
            "owned_by": self._owner(json_data),
            # _request_data/ep_approval are no longer record content here -
            # CDSToRDMRecordTransform._transform() assigns them directly on
            # MigrationEntry, since neither needs anything only this method
            # has: ep_approval reads the raw entry (already available to
            # the caller), and _request_data's pop off json_data happens
            # in CDSToRDMRecordTransform._transform(), before this method
            # even runs.
        }
