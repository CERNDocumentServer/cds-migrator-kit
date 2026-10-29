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
    email: str


class LegacyRole(TypedDict):
    id: str
    name: str
    description: str
    firerole_def_src: str
    users: list[RoleUser]


class RecordRestrictionsFile(TypedDict):
    bibdocid: int
    status: str
    roles: list[LegacyRole]


class RecordRestrictionsPermissions(TypedDict):
    owners: dict[str, list[str]]
    viewers: dict[str, list[str]]


class RecordRestrictionsEdit(TypedDict):
    collections: dict[str, list[LegacyRole]]
    any_collection: list[LegacyRole]


class RecordRestrictionsCommentsRoles(TypedDict):
    roles: list[LegacyRole]
    any_collection: list[LegacyRole]


class RecordRestrictionsCommentsRestrictedComment(TypedDict):
    restriction: str
    comment_ids: list[int]
    roles: list[LegacyRole]


class RecordRestrictionsComments(TypedDict):
    primary_collection: str
    view: RecordRestrictionsCommentsRoles
    send: RecordRestrictionsCommentsRoles
    record_restriction: str
    restricted_comments: list[RecordRestrictionsCommentsRestrictedComment]


class RecordRestrictions(TypedDict):
    collections: dict[str, list[LegacyRole]]
    files: list[RecordRestrictionsFile]
    record: RecordRestrictionsPermissions
    edit: RecordRestrictionsEdit
    comments: RecordRestrictionsComments


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
