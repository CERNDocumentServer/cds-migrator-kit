# -*- coding: utf-8 -*-
#
# Copyright (C) 2022-2026 CERN.
#
# CDS-RDM is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""One record version - a value in ``MigrationEntry["versions"]``."""
from pathlib import Path
from typing import Dict, Optional, TypedDict, Union

import arrow
from arrow import Arrow

LEGACY_FILES_PATH_ROOT = Path("/opt/cdsweb/var/data/files/")


class VersionAccessObj(TypedDict):
    """``VersionAccess["access_obj"]`` - mirrors the RDM record access schema."""

    record: Optional[str]
    files: Optional[str]


class VersionAccess(TypedDict, total=False):
    """A version's access - ``VersionEntry["access"]``.

    Set directly on the record post-create via
    ``load.py::_load_record_access`` (``record.access = access_dict["access_obj"]``).
    """

    access_obj: VersionAccessObj
    # Raw legacy file-restriction status string, present only when an
    # individual file carried its own restriction - see
    # RecordVersion.compute_access().
    meta: str


class VersionFileMetadata(TypedDict):
    """``VersionFileEntry["metadata"]``."""

    description: Optional[str]
    name: str
    status: str
    original_path: str
    comment: Optional[str]


class VersionFileEntry(TypedDict):
    """One file within ``VersionEntry["files"]``, keyed by its ``full_name``."""

    eos_tmp_path: Path
    id_bibdoc: int
    key: str
    metadata: VersionFileMetadata
    mimetype: str
    checksum: str
    version: int
    access: str
    type: str
    creation_date: str


class VersionEntry(TypedDict):
    """One record version - a value in ``MigrationEntry["versions"]``.

    Built by ``RecordVersionsTransform``, keyed there by legacy file
    version number (int), starting at 1.
    """

    files: Dict[str, VersionFileEntry]
    # Arrow instance when derived from a file's creation date; a plain ISO
    # date string in the no-files fallback branch (copied straight from
    # RecordEntryData["body"]["metadata"]["publication_date"]) - see
    # RecordVersionsTransform.build().
    publication_date: Union[Arrow, str]
    access: VersionAccess


class RecordVersion:
    """One record version - a value in ``MigrationEntry["versions"]``.

    Built by ``RecordVersionsTransform``, which resolves the cross-version
    file carry-forward (a version includes every earlier version's files
    too - see that class) after each ``RecordVersion`` computes its own
    files/access from its own raw legacy file dumps.
    """

    def __init__(
        self,
        record_access,
        files_dump_dir,
        migration_logger,
        representative_file=None,
        own_file_dumps=None,
        publication_date=None,
    ):
        """Constructor.

        :param record_access: the record's overall access status
            (``RecordEntryData["access_status"]``).
        :param files_dump_dir: local EOS mirror root for file content.
        :param migration_logger: for individual-file-restriction logging.
        :param representative_file: the raw legacy file dump this version's
            access is derived from (the first raw file dump encountered
            for this version - see ``RecordVersionsTransform``), or
            ``None`` for the metadata-only fallback version (no files at
            all for the record).
        :param own_file_dumps: this version's own raw legacy file dumps -
            NOT including files carried forward from earlier versions,
            that's ``RecordVersionsTransform``'s job.
        :param publication_date: used as-is when ``representative_file``
            is ``None`` (the metadata-only fallback - a plain ISO date
            string copied from the record's own publication date, rather
            than an Arrow instance derived from a file's creation date).
        """
        self.record_access = record_access
        self.files_dump_dir = files_dump_dir
        self.migration_logger = migration_logger
        self.representative_file = representative_file
        self.own_file_dumps = own_file_dumps or []
        self.publication_date = publication_date
        self.files = None
        self.access = None

    def build(self):
        """Populate ``files``/``access``/``publication_date``; return this version's dict."""
        self.files = self.compute_files()
        self.access = self.compute_access()
        if self.representative_file is not None:
            self.publication_date = arrow.get(
                self.representative_file["creation_date"]
            ).replace(tzinfo=None)
        return {
            "files": self.files,
            "publication_date": self.publication_date,
            "access": self.access,
        }

    def compute_access(self):
        """Return this version's access dict, from its representative file."""
        file = self.representative_file
        record_access = self.record_access
        if file is None or not file["status"]:
            return {
                "access_obj": {
                    "record": record_access,
                    "files": record_access,
                }
            }
        # if we have anything in the status string, it means the file is
        # restricted; we pass this information to parse later in load step
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

    def compute_files(self):
        """Transform this version's own raw file dumps into RDM file entries."""
        files = {}
        for file_dump in self.own_file_dumps:
            files[file_dump["full_name"]] = self._compute_file(file_dump)
        return files

    def _compute_file(self, file_dump):
        tmp_eos_root = Path(self.files_dump_dir)
        full_path = Path(file_dump["full_path"])
        return {
            "eos_tmp_path": tmp_eos_root
            / full_path.relative_to(LEGACY_FILES_PATH_ROOT),
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


# ATTENTION -leave this comment as it describes an example file dump
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
