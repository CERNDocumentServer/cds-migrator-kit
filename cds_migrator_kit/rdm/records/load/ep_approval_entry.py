# -*- coding: utf-8 -*-
#
# Copyright (C) 2026 CERN.
#
# CDS-RDM is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""Build public and restricted load entries for EP approval records."""
import re
from collections import OrderedDict
from copy import deepcopy

from flask import current_app

from cds_migrator_kit.errors import UnexpectedValue

EPPHAPP_FILE_TYPE = "EPPHAPP_FILE"
EP_APPROVAL_REPORT_NUMBER_PREFIX = "CERN-EP"
EP_APPROVAL_REPORT_NUMBER_RE = re.compile(r"^CERN-EP-\d{4}-\d{3}$")


def _cern_scientific_community_id():
    """Return the CERN Scientific community id from the app config.

    Read via ``current_app.config`` (not a direct import of
    ``migration_config``) so environment-specific overrides (e.g. an
    ``INVENIO_CDS_CERN_SCIENTIFIC_COMMUNITY_ID`` env var on a given instance)
    actually take effect - a plain module-level import would be frozen to
    whatever migration_config.py hardcodes, invisible to Flask's config
    loading/env var override mechanism entirely.
    """
    return current_app.config["CDS_CERN_SCIENTIFIC_COMMUNITY_ID"]


class MetadataEntry:
    """Build a load entry for the public or restricted EP approval split."""

    def __init__(self, entry, approval_request, migration_logger):
        self.entry = entry
        self.approval_request = approval_request
        self.migration_logger = migration_logger

    def identifiers(self, identifiers):
        """Return identifiers for this split."""
        raise NotImplementedError

    def build(self):
        """Return a load entry with split files and modified metadata."""
        split = deepcopy(self.entry)
        split["record"].pop("ep_approval", None)
        split["versions"] = self._build_versions(split)
        self._apply_metadata(split)
        self._apply_entry_modifications(split)
        return split

    def _apply_metadata(self, split):
        metadata = split["record"]["json"]["metadata"]
        metadata["identifiers"] = self.identifiers(metadata.get("identifiers", []))
        self._remove_doi_pid(split)

    def _apply_entry_modifications(self, split):
        """Apply record/parent level modifications."""

    @staticmethod
    def _is_restricted_file(file_data):
        """Return whether a file belongs to the restricted split.

        A file is restricted either because it is an EPPHAPP draft file, or
        because it carries its own file-level access restriction independent
        of the EPPHAPP workflow (e.g. a record that was never restricted as
        a whole, but ships a mix of public and individually-restricted
        files).
        """
        return bool(
            file_data.get("type") == EPPHAPP_FILE_TYPE or file_data.get("access")
        )

    def _log_removed_identifiers(self, removed, split_type):
        recid = self.entry.get("record", {}).get("recid")
        self.migration_logger.add_information(
            recid,
            {
                "message": (
                    f"Removed EP approval report number(s) from {split_type} " "record."
                ),
                "value": removed,
            },
        )

    def _remove_doi_pid(self, split):
        """Remove DOI PID from record."""
        pass

    def _build_versions(self, split):
        """Return versioned files for this split; override in subclasses."""
        raise NotImplementedError

    @staticmethod
    def _version_signature(versioned_files):
        return tuple(
            sorted(
                (
                    key,
                    file_data.get("checksum"),
                    file_data.get("id_bibdoc"),
                    file_data.get("version"),
                    file_data.get("type"),
                    file_data.get("access"),
                )
                for key, file_data in versioned_files.items()
            )
        )


class PublicEntry(MetadataEntry):
    """Build the public EP approval split entry."""

    def _build_versions(self, split):
        new_versions = OrderedDict()
        versioned_files = OrderedDict()
        previous_signature = None

        for _, version_data in split.get("versions", {}).items():
            current_version_files = OrderedDict()

            for key, file_data in version_data.get("files", {}).items():
                if self._is_restricted_file(file_data):
                    continue

                current_version_files[key] = deepcopy(file_data)

            if not current_version_files:
                continue

            versioned_files.update(current_version_files)

            signature = self._version_signature(versioned_files)
            # If the signature is the same, skip the version.
            if signature == previous_signature:
                continue

            previous_signature = signature

            version_access = deepcopy(version_data.get("access", {}))
            access_obj = deepcopy(version_access.get("access_obj", {}))
            access_obj["record"] = "public"
            access_obj["files"] = "public"
            version_access.pop("meta", None)
            version_access["access_obj"] = access_obj

            new_version_data = deepcopy(version_data)
            new_version_data["files"] = deepcopy(versioned_files)
            new_version_data["access"] = version_access

            new_versions[len(new_versions) + 1] = new_version_data

        if not new_versions:
            raise UnexpectedValue(
                message="No public files found to load for EP approval public split",
                stage="load",
                recid=split["record"]["recid"],
                priority="critical",
            )

        return new_versions

    def identifiers(self, identifiers):
        kept = []
        removed = []
        for id_entry in identifiers:
            if id_entry.get("scheme") != "cdsrn":
                kept.append(id_entry)
                continue
            identifier = id_entry.get("identifier", "")
            if identifier.startswith(EP_APPROVAL_REPORT_NUMBER_PREFIX):
                removed.append(identifier)
            else:
                kept.append(id_entry)

        kept.append(
            {
                "identifier": self.approval_request.report_number,
                "scheme": "apprn",
            }
        )

        if removed:
            self._log_removed_identifiers(removed, "public")

        return kept

    def _apply_entry_modifications(self, split):
        split["record"].pop("_request_data", None)
        split["record"]["owned_by"] = "system"
        split["parent"]["json"]["access"]["owned_by"] = {"user": "system"}
        self._add_cern_scientific_community(split)

    def _add_cern_scientific_community(self, entry):
        community_id = _cern_scientific_community_id()
        communities = entry.get("parent", {}).get("json", {}).get("communities", {})
        ids = list(communities.get("ids", []))
        if community_id not in ids:
            ids.append(community_id)
        communities["ids"] = ids
        entry.setdefault("parent", {}).setdefault("json", {})[
            "communities"
        ] = communities


class RestrictedEntry(MetadataEntry):
    """Build the restricted EP approval split entry."""

    def _apply_entry_modifications(self, split):
        self._remove_cern_scientific_community(split)

    def _remove_cern_scientific_community(self, entry):
        """Drop the CERN Scientific community from the restricted split.

        The restricted record holds the internal-only EPPHAPP draft and must
        not be discoverable via the broader community; only PublicEntry adds
        it (see _add_cern_scientific_community).
        """
        community_id = _cern_scientific_community_id()
        communities = entry.get("parent", {}).get("json", {}).get("communities", {})
        ids = [
            cid for cid in communities.get("ids", []) if cid != community_id
        ]
        communities["ids"] = ids
        if communities.get("default") == community_id:
            communities["default"] = ids[0] if ids else None
        entry.setdefault("parent", {}).setdefault("json", {})[
            "communities"
        ] = communities

    def _has_restricted_files(self, split):
        return any(
            self._is_restricted_file(file_data)
            for version_data in split.get("versions", {}).values()
            for file_data in version_data.get("files", {}).values()
        )

    def _build_versions(self, split):
        new_versions = OrderedDict()
        versioned_files = OrderedDict()
        previous_signature = None
        has_restricted_files = self._has_restricted_files(split)

        if not has_restricted_files:
            self.migration_logger.add_information(
                split["record"]["recid"],
                {
                    "message": (
                        "No restricted files found; public files used for the "
                        "restricted record."
                    ),
                    "value": "public files",
                },
            )

        for _, version_data in split.get("versions", {}).items():
            current_version_files = OrderedDict()

            for key, file_data in version_data.get("files", {}).items():
                is_restricted = self._is_restricted_file(file_data)

                # If restricted files exist, use only those; otherwise fall
                # back to using all (public) files for the restricted record.
                if not is_restricted and has_restricted_files:
                    continue

                current_version_files[key] = deepcopy(file_data)

            if not current_version_files:
                continue

            versioned_files.update(current_version_files)

            signature = self._version_signature(versioned_files)
            if signature == previous_signature:
                continue

            previous_signature = signature

            version_access = deepcopy(version_data.get("access", {}))
            access_obj = deepcopy(version_access.get("access_obj", {}))
            access_obj["record"] = "restricted"
            access_obj["files"] = "restricted"
            version_access["access_obj"] = access_obj

            new_version_data = deepcopy(version_data)
            new_version_data["files"] = deepcopy(versioned_files)
            new_version_data["access"] = version_access

            new_versions[len(new_versions) + 1] = new_version_data

        if not new_versions:
            raise UnexpectedValue(
                message=("No files found to load for EP approval restricted split"),
                stage="load",
                recid=split["record"]["recid"],
                priority="critical",
            )

        return new_versions

    def identifiers(self, identifiers):
        kept = []
        removed = []
        for id_entry in identifiers:
            if id_entry.get("scheme") != "cdsrn":
                kept.append(id_entry)
                continue
            identifier = id_entry.get("identifier", "")
            if not identifier.startswith(EP_APPROVAL_REPORT_NUMBER_PREFIX):
                kept.append(id_entry)
                continue
            # Remove CERN-EP-YYYY-NNN but keep CERN-EP-DRAFT report number
            if EP_APPROVAL_REPORT_NUMBER_RE.match(identifier):
                if identifier != self.approval_request.report_number:
                    raise UnexpectedValue(
                        message=(
                            "EP report number is not the same as the approved entry"
                        ),
                        stage="load",
                        priority="critical",
                    )
                removed.append(identifier)
            else:
                kept.append(id_entry)

        if removed:
            self._log_removed_identifiers(removed, "restricted")

        return kept

    def _remove_doi_pid(self, split):
        """Remove DOI PID from restricted record."""
        recid = split.get("record", {}).get("recid")
        record_json = split.get("record", {}).get("json", {})
        pids = record_json.get("pids")

        if not pids or "doi" not in pids:
            return

        removed = pids.pop("doi")

        self.migration_logger.add_information(
            recid,
            {
                "message": "Removed DOI PID from restricted record.",
                "value": removed,
            },
        )
