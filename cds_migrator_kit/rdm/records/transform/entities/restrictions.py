# -*- coding: utf-8 -*-
#
# Copyright (C) 2026 CERN.
#
# CDS-RDM is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""A typed restriction dictionary for dumped records."""

import re
from typing import TypedDict


class RoleUser(TypedDict):
    """A user."""

    email: str
    """The user's email address."""


class LegacyRole(TypedDict):
    """A role on the legacy system."""

    id: str
    """The role's ID."""
    name: str
    """The role's name."""
    description: str
    """A description of the role."""
    firerole_def_src: str
    """The firerole definition of the role."""
    users: list[RoleUser]
    """A list of users manually added to the role (usually the users are defined by an e-group in the firerole)."""


class RecordRestrictionsFile(TypedDict):
    """Permissions of a restricted file of the record."""

    bibdocid: int
    """The ID of the file."""
    status: str
    """The raw `status` string (e.g. `SSO`)."""
    roles: list[LegacyRole]
    """Who has ``viewrestdoc` for the given status."""


class RecordRestrictionsPermissions(TypedDict):
    """MARC-derived restrictions of the record itself."""

    owners: dict[str, list[str]]
    """The submitters/owners of this records (MARC tag -> list of role names)."""
    viewers: dict[str, list[str]]
    """The allowed viewers of this records (MARC tag -> list of role names)."""


class RecordRestrictions(TypedDict):
    """The restrictions object as dumped by invenio-migrator."""

    collections: dict[str, list[LegacyRole]]
    """
    The restricted collections the record is in and the corresponding roles that have `viewrestrcoll` on that collection.

    If this dictionary is empty, the record is not in any restricted collections.
    """
    files: list[RecordRestrictionsFile]
    """
    A list of the record's restricted files.

    Note: ONLY restricted files are included in this list. Files without a `status` are not listed. So records that are fully public will have an empty list here.
    """
    record: RecordRestrictionsPermissions
    """The owners and viewers granted access for this specific record via MARC tags."""
    edit: dict[str, list[LegacyRole]]
    """Roles with the `runbibedit` permission on this record's collections."""


_FIREROLE_ALLOW_RE = re.compile(r'^allow\s+(group|email)\s+((?:"[^"]+"\s*,?\s*)+)$')
# Lines that add nothing on top of the default (deny) behaviour
_FIREROLE_NOOP_LINES = {"deny any", "deny all"}


def normalize_group_name(subject: str) -> str:
    """Strip the legacy ``[CERN]`` suffix from an e-group name."""
    if subject.endswith(" [CERN]"):
        subject = subject.replace(" [CERN]", "")
    return subject.strip()


def parse_firerole(src: str) -> tuple[set[str], set[str]]:
    """Return the ``(groups, emails)`` a firerole definition allows.

    Only plain ``allow group "..."`` / ``allow email "..."`` rules can be
    represented on RDM, anything else (``allow any``, ``deny group``, ``allow
    remote_ip``, ...) raises ``ValueError``.
    """
    groups = set()
    emails = set()
    for line in src.replace("\r\n", "\n").split("\n"):
        line = line.strip()
        if line.startswith("firerole:"):
            line = line[len("firerole:") :].strip()
        if not line or line.startswith("#") or line in _FIREROLE_NOOP_LINES:
            continue
        match = _FIREROLE_ALLOW_RE.match(line)
        if not match:
            raise ValueError(f"Unsupported firerole rule: {line}")
        values = re.findall(r'"([^"]+)"', match.group(2))
        if match.group(1) == "group":
            groups.update(normalize_group_name(value) for value in values)
        else:
            emails.update(value.lower() for value in values)
    return groups, emails


def legacy_role_subjects(roles: list[LegacyRole]) -> tuple[set[str], set[str]]:
    """Return all the ``(groups, emails)`` covered by a list of legacy roles.

    Combines the groups/emails of each role's firerole definition with the
    users directly assigned to the role.
    """
    groups = set()
    emails = set()
    for role in roles:
        role_groups, role_emails = parse_firerole(role["firerole_def_src"] or "")
        groups.update(role_groups)
        emails.update(role_emails)
        emails.update(user["email"].lower() for user in role["users"])
    return groups, emails
