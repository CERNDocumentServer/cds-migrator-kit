# -*- coding: utf-8 -*-
#
# Copyright (C) 2022-2026 CERN.
#
# CDS-RDM is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""The RDM record's own content - ``MigrationEntry["record"]``."""
from copy import deepcopy
from typing import Any, TypedDict

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
    """Required keys of ``RecordEntry.body``."""

    files: dict
    pids: dict
    metadata: dict


class RecordBody(RecordBodyRequired, total=False):
    """The RDM record body: ``RecordEntry.body``, returned by ``build()``.

    ``current_rdm_records_service.create()``'s ``data`` argument. Deliberately
    excludes:

    - ``access``: set per-version, after creation, via
      ``VersionEntry["access"]`` (see ``load.py::_load_record_access``)
      rather than at record-create time.
    - ``access_grants``: RDM parent-level, not record-level - see
      ``RecordParent.access_grants`` in ``entities/parent.py``.
    """

    custom_fields: dict
    internal_notes: Any


class RecordEntry:
    """A single record's content - ``MigrationEntry["record"]``.

    Builds the RDM record's own content - not the invenio_rdm_migrator
    "generic RDM record" envelope (this class deliberately does not use
    that framework's ``RDMRecordEntry.transform()``/``_load_partial``
    orchestration, since the CDS legacy shape and the CDS loader's needs
    don't match it). The constructor eagerly computes the cheap,
    single-value supporting data (``created``/``recid``/``access_status``);
    ``build()`` does the more involved work of assembling ``body`` - the
    dict handed wholesale to
    ``current_rdm_records_service.create()``/``schema.load()``.
    """

    def __init__(
        self,
        dump,
        dojson_entry,
        affiliations_mapping=None,
        restricted=False,
        migration_logger=None,
    ):
        """Constructor.

        :param dump: the ``CDSRecordDump`` for this entry - produced by
            ``CDSToRDMRecordTransform._transform_xml_to_json()``. Its
            ``.data`` is the raw harvested entry (``raw_dump_entry`` below).
        :param dojson_entry: the DOJSON-processed record data
            (``dump.latest_revision``'s content) - passed in separately
            since ``build()`` mutates it in place as it builds the record.
        """
        self.dump = dump
        self.raw_dump_entry = dump.data
        self.dojson_entry = dojson_entry
        self.affiliations_mapping = affiliations_mapping
        self.restricted = restricted
        self.migration_logger = migration_logger
        self.body = None

        self.created = dump.first_created
        self.recid = self._recid(dump)
        # None when RecordFlaggedCuration was raised and caught.
        self.access_status = None
        try:
            self.access_status = self._access(dojson_entry)
        except RecordFlaggedCuration as exc:
            self.migration_logger.add_information(
                self.raw_dump_entry["recid"],
                {"message": exc.message, "value": exc.value},
            )

    def _access(self, dojson_entry):
        record_restriction = dojson_entry.pop("record_restriction", None)
        if isinstance(record_restriction, list):
            record_restriction = record_restriction[0]
        restrictions = "restricted" if self.restricted else record_restriction
        if not restrictions:
            raise RecordFlaggedCuration(
                message="record restriction not found make sure the record should be public",
                stage="transform",
                field="record_restriction",
            )
        return restrictions

    def _recid(self, dump):
        """Returns the recid of the record."""
        return str(dump.data["recid"])

    def _pids(self, dojson_entry):
        DATACITE_PREFIX = current_app.config["DATACITE_PREFIX"]

        pids = dojson_entry.pop("_pids", {})
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
                    if not dojson_entry.get("publisher"):
                        dojson_entry["publisher"] = "CERN"
                output_pids["doi"] = doi_identifier
        if output_pids:
            return output_pids
        else:
            return {}

    def _files(self, dump):
        """Transform the files of a record."""
        dump.prepare_files()
        files = dump.files
        return {"enabled": bool(files)}

    def _metadata(self, dojson_entry, raw_dump_entry):
        """Build the metadata dict by running the composed field mappers.

        Whether every ``dojson_entry`` key ended up consumed *somewhere* in
        the pipeline is no longer this method's concern - see
        ``CDSToRDMRecordTransform._check_forgotten_keys()``, which runs
        once all entities (this one, ``RecordParent``, ...) have built and
        popped their own keys, for a check across the whole entry rather
        than just this record's metadata.
        """
        ctx = RecordTransformContext(
            dojson_entry=dojson_entry,
            raw_dump_entry=raw_dump_entry,
            migration_logger=self.migration_logger,
            affiliations_mapping=self.affiliations_mapping,
        )
        metadata = ctx.metadata
        # Order matters: ResourceTypeMapper must run before TitleMapper reads
        # metadata["resource_type"]; see mappers/registry.py.
        for mapper in METADATA_MAPPERS:
            metadata[mapper.id] = mapper.map_value(ctx)
        return {k: v for k, v in metadata.items() if v}

    def _custom_fields(self, dojson_entry, raw_dump_entry):
        """Build the custom_fields dict by running the composed field mappers.

        Must run before ``_metadata()``: a couple of these mappers add a
        fallback ``dojson_entry["subjects"]`` entry when a vocabulary lookup
        fails, which metadata's own SubjectsMapper then picks up like any
        other subject - see DepartmentsMapper.
        """
        ctx = RecordTransformContext(
            dojson_entry=dojson_entry,
            raw_dump_entry=raw_dump_entry,
            migration_logger=self.migration_logger,
        )
        for mapper in CUSTOM_FIELD_MAPPERS:
            mapper.apply(ctx)
        custom_fields = ctx.custom_fields

        forgotten_keys = [
            key
            for key in dojson_entry["custom_fields"].keys()
            if key not in custom_fields.keys()
        ]
        if forgotten_keys:
            raise ManualImportRequired(
                "Unassigned custom field key", value=forgotten_keys
            )
        # filter out null values
        return {k: v for k, v in custom_fields.items() if v}

    def _verify_publication_date(self, raw_dump_entry, dojson_entry):
        """Verify creation date.

        If the record has no files (file creation date will be used as record
        creation date) and no creation date, raise an exception.
        """
        if not raw_dump_entry.get("files") and not (
            dojson_entry.get("status_week_date")
            or dojson_entry.get("publication_date")
            or dojson_entry.get("preprint_date")
        ):
            raise ManualImportRequired(
                message="Record missing publication date",
                field="validation",
                stage="transform",
                description="Record has no files and no publication date",
                recid=raw_dump_entry["recid"],
                priority="warning",
                value=None,
                subfield=None,
            )

    def build(self) -> RecordBody:
        """Build and return this record's content - the RDM record body."""
        raw_dump_entry, dump, dojson_entry = (
            self.raw_dump_entry,
            self.dump,
            self.dojson_entry,
        )
        self._verify_publication_date(raw_dump_entry, dojson_entry)

        # custom_fields runs before metadata: see _custom_fields()'s docstring.
        custom_fields = self._custom_fields(dojson_entry, raw_dump_entry)
        # popped ahead of CDSToRDMRecordTransform._check_forgotten_keys(),
        # same reason as _pids()/_access()'s record_restriction pop.
        internal_notes = dojson_entry.pop("internal_notes", None)

        record_json_output = {
            "files": self._files(dump),
            "pids": self._pids(dojson_entry),
            "metadata": self._metadata(dojson_entry, raw_dump_entry),
            "internal_notes": internal_notes,
            "custom_fields": custom_fields,
        }
        # drop empty optional keys rather than sending them to the RDM
        # record schema as null/{}
        for key in ("internal_notes", "custom_fields"):
            if not record_json_output[key]:
                del record_json_output[key]

        self.body = record_json_output
        return self.body
