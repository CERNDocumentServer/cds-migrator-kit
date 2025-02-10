# -*- coding: utf-8 -*-
#
# Copyright (C) 2022 CERN.
#
# CDS-Videos is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""CDS-Videos contributors migration module."""

from cds_migrator_kit.errors import (
    UnexpectedValue,
)

# TODO role will be changed to optional, update the `get_contributor_role` method
def get_contributor_role(subfield, role, raise_unexpected=False):
    """Clean up roles."""
    translations = {
        "speaker": "Speaker",
        "author.": "Speaker",
        "ed.": "Editor",
        "editor": "Editor",
        "editor.": "Editor",
        "ed": "Editor",
        "ed. et al.": "Editor",
    }
    clean_role = None
    if role is None:
        return "Speaker"  # TODO ?
    if isinstance(role, str):
        clean_role = role.lower()
    elif isinstance(role, list) and role and role[0]:
        clean_role = role[0].lower()
    elif raise_unexpected:
        raise UnexpectedValue(subfield=subfield, message="unknown author role")

    if clean_role not in translations or clean_role is None:
        return "Speaker"

    return translations[clean_role]


# TODO contributor affiliation will be implemented
