# -*- coding: utf-8 -*-
#
# Copyright (C) 2022 CERN.
#
# CDS-RDM is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""Reviewer resolution utilities."""

from invenio_accounts.models import User
from invenio_db import db

from cds_migrator_kit.errors import RecordFlaggedCuration


def _is_email(value):
    """Return True if the reviewer value looks like an email address."""
    return "@" in value


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


def find_reviewer(reviewer):
    """Resolve a reviewer string (email or name) to a User.

    :param reviewer: email address, or a "Family, Given"/"Given Family" name.
    :raises RecordFlaggedCuration: if no matching user is found, so the
        record is flagged for manual curation instead of failing outright.
    """
    reviewer = reviewer.strip()
    if _is_email(reviewer):
        user = User.query.filter_by(email=reviewer).one_or_none()
    else:
        family_name, given_name = _parse_reviewer_name(reviewer)
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
