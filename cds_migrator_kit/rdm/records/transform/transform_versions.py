# -*- coding: utf-8 -*-
#
# Copyright (C) 2022-2026 CERN.
#
# CDS-RDM is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""Builds all of a record's versions - ``MigrationEntry["versions"]``."""

from collections import OrderedDict

import arrow

from cds_migrator_kit.rdm.records.transform.config import FILE_SUBFORMATS_TO_DROP
from cds_migrator_kit.rdm.records.transform.entities.version import RecordVersion


class RecordVersionsTransform:
    """Builds all of a record's versions - ``MigrationEntry["versions"]``.

    Groups the legacy file dumps by version, then builds each version via
    ``RecordVersion`` from its cumulative file set: lets say a record has 2
    files, A & B - if a new version of file A gets uploaded, the later record
    version still needs to include file B too, so each version's file list is
    a cumulative snapshot (latest revision per file), not just its own delta.
    Access is computed over that same cumulative set, so a restricted file
    carried into a later version still counts (see ``RecordVersion``).

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
        # first-seen order - raw_file_dumps_by_version[v] is version v's own
        # files (not yet carrying anything forward from earlier versions).
        raw_file_dumps_by_version = OrderedDict()
        representative_file = {}
        for file_dump in self.raw_dump_entry["files"]:
            if self._should_skip_file(file_dump):
                continue
            version_number = file_dump["version"]
            raw_file_dumps_by_version.setdefault(version_number, []).append(file_dump)
            representative_file.setdefault(version_number, file_dump)

        # Derive publication dates now, before any external-DOI collapse
        # reorders raw_file_dumps_by_version. Each version's date comes from
        # its first (representative) file — the current/latest state per version.
        publication_dates = {
            v: arrow.get(fd["creation_date"]).replace(tzinfo=None)
            for v, fd in representative_file.items()
        }

        if raw_file_dumps_by_version and self._is_external_doi():
            # collapse every legacy file revision into a single version -
            # its files (see the carry-forward below, still a no-op for one
            # version) and its access/publication_date (from the latest
            # legacy version's representative file, i.e. the current state)
            # instead of one RDM version per legacy revision.
            latest_version_number = max(raw_file_dumps_by_version)
            raw_file_dumps_by_version = OrderedDict(
                [
                    (
                        latest_version_number,
                        [
                            fd
                            for fds in raw_file_dumps_by_version.values()
                            for fd in fds
                        ],
                    )
                ]
            )
            publication_dates = {
                latest_version_number: publication_dates[latest_version_number]
            }

        # Build each version from its CUMULATIVE file set (its own files plus
        # every earlier version's latest file revision). Computing access over
        # the cumulative set - not just the version's new files - means a
        # restricted file carried into a later version is still seen by
        # RecordVersion.compute_access(), so a version that mixes it with a
        # public file hard-fails instead of silently going public.
        cumulative_raw_dumps = {}  # full_name -> latest raw dump seen so far
        versions = OrderedDict()
        for version_number, own_dumps in raw_file_dumps_by_version.items():
            for file_dump in own_dumps:
                cumulative_raw_dumps[file_dump["full_name"]] = file_dump
            versions[version_number] = RecordVersion(
                record_access=record_access,
                files_dump_dir=self.files_dump_dir,
                migration_logger=self.migration_logger,
                publication_date=publication_dates[version_number],
                raw_file_dumps=list(cumulative_raw_dumps.values()),
            ).build()

        if not versions:
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
