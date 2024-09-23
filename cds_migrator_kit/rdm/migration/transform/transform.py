# -*- coding: utf-8 -*-
#
# Copyright (C) 2022 CERN.
#
# CDS-RDM is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""CDS-RDM transform step module."""

import datetime
import logging
from collections import OrderedDict
from pathlib import Path

import arrow
from invenio_rdm_migrator.streams.records.transform import (
    RDMRecordEntry,
    RDMRecordTransform,
)

from cds_migrator_kit.rdm.migration.transform.xml_processing.dumper import CDSRecordDump
from cds_migrator_kit.rdm.migration.transform.xml_processing.errors import (
    LossyConversion, RestrictedFileDetected,
)
from cds_migrator_kit.records.log import RDMJsonLogger

cli_logger = logging.getLogger("migrator")


class CDSToRDMRecordEntry(RDMRecordEntry):
    """Transform Zenodo record to RDM record."""

    def _created(self, json_entry):
        try:
            return arrow.get(json_entry["_created"])
        except KeyError:
            return datetime.date.today().isoformat()

    def _updated(self, record_dump):
        """Returns the creation date of the record."""
        return record_dump.data["record"][0]["modification_datetime"]

    def _version_id(self, entry):
        """Returns the version id of the record."""
        return 1

    def _access(self, entry, record_dump):
        is_file_public = True

        for key, value in record_dump.files.items():
            if value[0]["hidden"]:
                is_file_public = False
        return {
            "record": "public",
            "files": "public" if is_file_public else "restricted",
        }

    def _index(self, record_dump):
        """Returns the version index of the record."""
        return 1  # in legacy we start at 0

    def _recid(self, record_dump):
        """Returns the recid of the record."""
        return str(record_dump.data["recid"])

    def _pids(self, json_entry):
        return {}

    def _bucket_id(self, json_entry):
        return

    def _custom_fields(self, json_entry):
        return {}

    def _id(self, entry):
        return

    def _media_bucket_id(self, entry):
        return

    def _media_files(self, entry):
        return {}

    def _pids(self, json_entry):
        return []

    def _files(self, record_dump):
        """Transform the files of a record."""
        record_dump.prepare_files()
        files = record_dump.files
        return {"enabled": True if files else False}

    def _communities(self, json_entry):
        return json_entry["communities"]

    def _metadata(self, json_entry):
        def creators(json):
            try:
                return json["creators"]
            except KeyError:
                return [
                    {
                        "person_or_org": {
                            "given_name": "unknown",
                            "name": "unknown",
                            "family_name": "unknown",
                            "type": "personal",
                        }
                    }
                ]

        def _resource_type(data):
            t = "publication-technicalnote"
            st = None
            return {"id": f"{t}-{st}"} if st else {"id": t}
        return {
            "creators": creators(json_entry),
            "title": json_entry["title"],
            "resource_type": _resource_type(json_entry),
            "description": json_entry.get("description"),
            "publication_date": json_entry.get("publication_date"),
        }

    def transform(self, entry):
        """Transform a record single entry."""
        record_dump = CDSRecordDump(
            entry,
        )
        try:
            migration_logger = RDMJsonLogger()
            migration_logger.add_recid_to_stats(entry["recid"])
            record_dump.prepare_revisions()
            timestamp, json_data = record_dump.revisions[-1]
            migration_logger.add_record(json_data)
            return {
                "created": self._created(json_data),
                "updated": self._updated(record_dump),
                "version_id": self._version_id(record_dump),
                "index": self._index(record_dump),
                "recid": self._recid(record_dump),
                # "communities": self._communities(json_data),
                "json": {
                    "created": self._created(json_data),
                    "updated": self._updated(record_dump),
                    "pids": self._pids(json_data),
                    "files": self._files(record_dump),
                    "metadata": self._metadata(json_data),
                    "access": self._access(json_data, record_dump),
                },
            }
        except LossyConversion as e:
            cli_logger.error("[DATA ERROR]: {0}".format(e.message))
            migration_logger.add_log(e, output=entry)
        except Exception as e:
            migration_logger.add_log(e, output=entry)
            raise e
        # TODO take only the last


class CDSToRDMRecordTransform(RDMRecordTransform):
    """CDSToRDMRecordTransform."""

    def __init__(self, workers=None, throw=True, files_dump_dir=None):
        """Constructor."""
        self.files_dump_dir = Path(files_dump_dir).absolute().as_posix()
        super().__init__(workers, throw)

    def _community_id(self, entry, record):
        communities = record.get("communities")
        if communities:
            # TODO: handle all slugs
            slug = communities[0]
            if slug:
                return {"ids": [slug], "default": slug}
        return {}

    def _parent(self, entry, record):
        parent = {
            "created": record["created"],  # same as the record
            "updated": record["updated"],  # same as the record
            "version_id": record["version_id"],
            "json": {
                # loader is responsible for creating/updating if the PID exists.
                "id": f'{record["recid"]}-parent',
                "access": {
                    "owned_by": {"user": "1"},
                },
                # "communities": self._community_id(entry, record),
            },
        }

        return parent

    def _transform(self, entry):
        """Transform a single entry."""
        # the functions receive the full record/data entry
        # while in most cases the full view is not needed
        # since this is a low level tool used only by users
        # with deep system knowledge providing the flexibility
        # is future proofing and simplifying the interface
        migration_logger = RDMJsonLogger()
        try:
            record = self._record(entry)
            if record:
                return {
                    "record": record,
                    "draft": self._draft(entry),
                    "parent": self._parent(entry, record),
                    "record_files": self._record_files(entry, record),
                    "draft_files": self._draft_files(entry),
                }
        except Exception as e:
            migration_logger.add_log(e, output=entry)

    def _record(self, entry):
        # could be in draft as well, depends on how we decide to publish
        return CDSToRDMRecordEntry().transform(entry)

    def _draft(self, entry):
        return None

    def _draft_files(self, entry):
        """Point to temporary eos storage to import files from."""
        _files = entry["files"]
        draft_files = OrderedDict()
        legacy_path_root = Path("/opt/cdsweb/var/data/files/")
        tmp_eos_root = Path(self.files_dump_dir)

        for file in _files:
            full_path = Path(file["full_path"])

            if file["version"] not in draft_files:
                draft_files[file["version"]] = {}

            # TODO other access types to be dealt later, for now we make sure
            # TODO that no restricted file goes through
            if file["status"]:
                raise RestrictedFileDetected(value=file["full_name"])
            # group files by version
            # {"1": {"filename": {...}}
            draft_files[file["version"]].update(
                {
                    file["full_name"]: {
                        "eos_tmp_path": tmp_eos_root
                        / full_path.relative_to(legacy_path_root),
                        "key": file["full_name"],
                        "metadata": {},
                        "mimetype": file["mime"],
                        "checksum": file["checksum"],
                        "version": file["version"],
                        "access": file["status"],
                        "type": file["type"],
                        "creation_date": arrow.get(file["creation_date"])
                        .date()
                        .isoformat(),
                    }
                }
            )
        versioned_files = {}

        # creates a collection of files per each version
        for version in draft_files.keys():
            versioned_files |= draft_files.get(version)
            draft_files[version] = versioned_files

        return draft_files

    def _record_files(self, entry, record):
        """Record files entries transform."""
        # TO implement if we decide not to go via draft publish
        return []

    def run(self, entries):
        return super().run(entries)

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
