# -*- coding: utf-8 -*-
#
# Copyright (C) 2022-2026 CERN.
#
# CDS-RDM is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""A community-inclusion request for one migrated CDS record."""
from invenio_accounts.models import User
from invenio_db import db

from cds_migrator_kit.errors import ManualImportRequired, RecordFlaggedCuration


class RecordRequest:
    """A community-inclusion request for one migrated CDS record.

    Built by ``CDSToRDMRecordTransform._transform()`` - pops the raw
    request data off ``json_entry`` (before any field mapper can see it,
    since it isn't part of the RDM record schema), resolves raw reviewer
    name/email strings to actual user accounts (a DB lookup, which is why
    this lives here and not in a dojson rule - rules must stay DB-free),
    and logs any resolution failures. Consumed by ``load.py`` after
    publish, which owns actually creating the RDM request.
    """

    def __init__(self, json_entry, recid, migration_logger):
        """Constructor.

        :param json_entry: the DOJSON-processed record data - request_data
            is popped off it.
        :param recid: this record's legacy recid, for error reporting.
        :param migration_logger: for reviewer-error/validation logging.
        """
        self.json_entry = json_entry
        self.recid = recid
        self.migration_logger = migration_logger
        self.data = None

    def build(self):
        """Pop request_data off ``json_entry``, resolve reviewers; return self."""
        request_data = self.json_entry.pop("request_data", None)
        if request_data:
            reviewer_names = request_data.pop("reviewer_names", [])
            # merge into whatever's already there rather than overwriting -
            # some rules (e.g. faser_publication.py's status rule) add
            # already-resolved reviewer entries directly (no DB lookup
            # needed, e.g. a group reviewer), and those must be kept.
            reviewers = request_data.setdefault("reviewers", [])
            for reviewer_entry in self._resolve_reviewers(reviewer_names):
                if reviewer_entry not in reviewers:
                    reviewers.append(reviewer_entry)
        self.data = request_data
        return self

    def __bool__(self):
        """True when there's request data to act on."""
        return bool(self.data)

    def ensure_enabled(self, create_inclusion_request):
        """Raise if this record has request data but requests aren't enabled.

        :param create_inclusion_request: whether the current load run is
            configured to create community-inclusion requests - see
            ``CDSRecordServiceLoad.create_inclusion_request``.
        """
        if self.data and not create_inclusion_request:
            raise ManualImportRequired(
                message="Detected request data, enable the requests",
                field="validation",
                stage="load",
                recid=self.recid,
                priority="warning",
                subfield=None,
            )

    def _resolve_reviewers(self, reviewer_names):
        """Resolve raw reviewer name/email strings to RDM reviewer entries.

        A reviewer that can't be matched to a user account is flagged for
        curation (logged) and represented by a "-1" placeholder user id,
        rather than aborting the whole record.
        """
        resolved = []
        for reviewer_name in reviewer_names:
            try:
                user = self._find_reviewer(reviewer_name)
                reviewer_entry = {"user": str(user.id)}
            except RecordFlaggedCuration as exc:
                self.migration_logger.add_information(
                    self.recid, {"message": exc.message, "value": exc.value}
                )
                reviewer_entry = {"user": "-1"}
            if reviewer_entry not in resolved:
                resolved.append(reviewer_entry)
        return resolved

    @staticmethod
    def _find_reviewer(reviewer):
        """Resolve a reviewer string (email or name) to a User.

        :param reviewer: email address, or a "Family, Given"/"Given Family" name.
        :raises RecordFlaggedCuration: if no matching user is found, so the
            record is flagged for manual curation instead of failing outright.
        """
        reviewer = reviewer.strip()
        if RecordRequest._is_email(reviewer):
            user = User.query.filter_by(email=reviewer).one_or_none()
        else:
            family_name, given_name = RecordRequest._parse_reviewer_name(reviewer)
            query = User.query.filter(
                db.func.lower(User._user_profile["family_name"].as_string())
                == family_name.lower()
            )
            if given_name:
                query = query.filter(
                    db.func.lower(User._user_profile["given_name"].as_string())
                    == given_name.lower()
                )
            user = query.one_or_none()

        if user is None:
            raise RecordFlaggedCuration(
                message=f"Reviewer '{reviewer}' could not be matched to an account.",
                field="request_reviewers",
                stage="transform",
                value=reviewer,
            )
        return user

    @staticmethod
    def _is_email(value):
        """Return True if the reviewer value looks like an email address."""
        return "@" in value

    @staticmethod
    def _parse_reviewer_name(name):
        """Split a 'Family, Given' or 'Given Family' string into (family, given).

        ``request_reviewers`` (906__p) stores names as "Given Family" (comma
        already resolved), but legacy data can also arrive as "Family, Given".
        """
        name = name.strip()
        if "," in name:
            family, _, given = name.partition(",")
            return family.strip(), given.strip()
        parts = name.split()
        if len(parts) > 1:
            return parts[-1], " ".join(parts[:-1])
        return name, ""
