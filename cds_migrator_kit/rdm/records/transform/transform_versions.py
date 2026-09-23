# -*- coding: utf-8 -*-
#
# Copyright (C) 2022-2026 CERN.
#
# CDS-RDM is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""Builds all of a record's versions - ``MigrationEntry["versions"]``."""

from collections import OrderedDict
from copy import deepcopy

from cds_migrator_kit.rdm.records.transform.config import FILE_SUBFORMATS_TO_DROP
from cds_migrator_kit.rdm.records.transform.entities.version import RecordVersion


class RecordVersionsTransform:
    """Builds all of a record's versions - ``MigrationEntry["versions"]``.

    Groups the legacy file dumps by version, builds each version's own
    files/access via ``RecordVersion``, then carries files forward across
    versions: lets say a record has 2 files, A & B - if a new version of
    file A gets uploaded, the later record version still needs to include
    file B too, so each version's file list is a cumulative snapshot, not
    just its own delta.

    Exception: when the record's DOI is external (not minted by our
    DataCite prefix - see ``RecordEntry._pids()``), we don't own/manage
    that DOI, so its legacy per-file version history is collapsed into a
    single RDM version holding every file, instead of one RDM version per
    legacy file revision - see ``_is_external_doi()``.
    """

    def __init__(self, raw_dump_entry, record, files_dump_dir, plots, migration_logger):
        """Constructor.

        :param raw_dump_entry: the original harvested legacy entry (needs
            "files").
        :param record: the already-built ``RecordEntry`` (needs
            ``access_status`` and ``body["metadata"]["publication_date"]``).
        :param files_dump_dir: local EOS mirror root for file content.
        :param plots: whether to keep Plot-type files.
        :param migration_logger: for skip/restriction logging.
        """
        self.raw_dump_entry = raw_dump_entry
        self.record = record
        self.files_dump_dir = files_dump_dir
        self.plots = plots
        self.migration_logger = migration_logger

    def build(self):
        """Group legacy files by version, build + carry files forward, return."""
        record_access = self.record.access_status

        # group non-skipped raw file dumps by legacy version number, in
        # first-seen order - own_file_dumps[v] is version v's own files
        # (not yet carrying anything forward from earlier versions).
        own_file_dumps = OrderedDict()
        representative_file = {}
        for file_dump in self.raw_dump_entry["files"]:
            if self._should_skip_file(file_dump):
                continue
            version_number = file_dump["version"]
            own_file_dumps.setdefault(version_number, []).append(file_dump)
            representative_file.setdefault(version_number, file_dump)

        if own_file_dumps and self._is_external_doi():
            # collapse every legacy file revision into a single version -
            # its files (see the carry-forward below, still a no-op for one
            # version) and its access/publication_date (from the latest
            # legacy version's own representative file, i.e. the current
            # state) instead of one RDM version per legacy revision.
            latest_version_number = max(own_file_dumps)
            own_file_dumps = OrderedDict(
                [
                    (
                        latest_version_number,
                        [fd for fds in own_file_dumps.values() for fd in fds],
                    )
                ]
            )
            representative_file = {
                latest_version_number: representative_file[latest_version_number]
            }

        versions = OrderedDict(
            (
                version_number,
                RecordVersion(
                    record_access=record_access,
                    files_dump_dir=self.files_dump_dir,
                    migration_logger=self.migration_logger,
                    representative_file=representative_file[version_number],
                    own_file_dumps=own_file_dumps[version_number],
                ).build(),
            )
            for version_number in own_file_dumps
        )

        # carry files forward across versions (see class docstring)
        versioned_files = {}
        for version_number in versions:
            versioned_files |= versions[version_number]["files"]
            versions[version_number]["files"] = deepcopy(versioned_files)

        if not versioned_files:
            # Record has no files. Add metadata-only record as single version
            versions[1] = RecordVersion(
                record_access=record_access,
                files_dump_dir=self.files_dump_dir,
                migration_logger=self.migration_logger,
                publication_date=self.record.body["metadata"]["publication_date"],
            ).build()

        return versions

    def _is_external_doi(self):
        """Return True if this record's DOI isn't minted through our prefix.

        Mirrors the ``provider`` set in ``RecordEntry._pids()``
        (``"external"`` when the DOI doesn't start with
        ``current_app.config["DATACITE_PREFIX"]``); ``False`` (no
        collapsing) when the record has no DOI at all.
        """
        doi = self.record.body.get("pids", {}).get("doi", {})
        return doi.get("provider") == "external"

    def _should_skip_file(self, file_dump):
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
                    "message": "Plot file dropped.",
                    "value": file_dump["full_name"],
                },
            )
            return True
        if file_dump["hidden"]:
            # skip hidden files
            self.migration_logger.add_information(
                str(file_dump["recid"]),
                {
                    "message": "Hidden file dropped.",
                    "value": file_dump["full_name"],
                },
            )
            return True
        return False
