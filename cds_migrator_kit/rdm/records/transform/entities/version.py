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
from typing_extensions import Required

from cds_migrator_kit.errors import ManualImportRequired

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

    access_obj: Required[VersionAccessObj]
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
        raw_file_dumps=None,
        publication_date=None,
    ):
        """Constructor.

        :param record_access: the record's overall access status
            (``RecordEntryData["access_status"]``).
        :param files_dump_dir: local EOS mirror root for file content.
        :param migration_logger: for individual-file-restriction logging.
        :param raw_file_dumps: this version's own raw legacy file dumps -
            NOT including files carried forward from earlier versions,
            that's ``RecordVersionsTransform``'s job.
        :param publication_date: Arrow instance (when derived from a file's
            creation date by the caller) or plain ISO date string (metadata-
            only fallback copied from the record's publication date).
        """
        self.record_access = record_access
        self.files_dump_dir = files_dump_dir
        self.migration_logger = migration_logger
        self.raw_file_dumps = raw_file_dumps or []
        self.publication_date = publication_date
        self.files = None
        self.access = None

    @property
    def representative_file(self):
        """First file dump for this version, or None when there are no files."""
        return self.raw_file_dumps[0] if self.raw_file_dumps else None

    def build(self):
        """Populate ``files``/``access``; return this version's dict."""
        self.files = self.compute_files()
        self.access = self.compute_access()
        return {
            "files": self.files,
            "publication_date": self.publication_date,
            "access": self.access,
        }

    def compute_access(self):
        """Return this version's access dict, from this version's files."""
        file = self.representative_file
        record_access = self.record_access
        if file is None:
            return {
                "access_obj": {
                    "record": record_access,
                    "files": record_access,
                }
            }

        # A version carries a single access state + a single `meta`, so every
        # file in it must share the same status. Any non-uniform mix — some
        # public and some restricted, and/or several distinct restriction
        # statuses — can't be represented, so hard-stop for manual review.
        recid = str(file["recid"])
        distinct_statuses = {f["status"] for f in self.raw_file_dumps}
        if len(distinct_statuses) > 1:
            raise ManualImportRequired(
                message=(
                    "Mixed file restrictions in one version. "
                    "Cannot auto-assign access — manual review required."
                ),
                field="files",
                subfield="status",
                stage="transform",
                recid=recid,
                priority="critical",
                value=", ".join(
                    f"{f['full_name']}: {f['status']}" for f in self.raw_file_dumps
                ),
            )

        # All files share one status: public (empty) or a single restriction.
        # Note: we do NOT validate the status against CDS_ACCESS_GROUP_MAPPINGS
        # here. Recognising/resolving the restriction string (mapping keyword,
        # firerole, bare [CERN] e-group, ...) is RecordParent.resolve_grants()'s
        # job at load time; it hard-raises on anything it can't resolve. Here we
        # only decide public-vs-restricted and carry the raw status as `meta`.
        status = distinct_statuses.pop()
        if not status:
            return {
                "access_obj": {
                    "record": record_access,
                    "files": record_access,
                }
            }

        self.migration_logger.add_information(
            recid,
            {
                "message": "Record has individual file restrictions",
                "value": status,
            },
        )
        return {
            "access_obj": {"record": record_access, "files": "restricted"},
            "meta": status,
        }

    def compute_files(self):
        """Transform this version's own raw file dumps into RDM file entries."""
        files = {}
        for file_dump in self.raw_file_dumps:
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
