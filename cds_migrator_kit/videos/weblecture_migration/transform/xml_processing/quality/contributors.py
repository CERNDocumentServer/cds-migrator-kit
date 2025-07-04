# -*- coding: utf-8 -*-
#
# Copyright (C) 2025 CERN.
#
# CDS-Videos is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""CDS-Videos contributors migration module."""

from cds_migrator_kit.errors import UnexpectedValue


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
        return "Speaker"  # TODO default?
    if isinstance(role, str):
        clean_role = role.lower()
    elif isinstance(role, list) and role and role[0]:
        clean_role = role[0].lower()
    elif raise_unexpected:
        raise UnexpectedValue(subfield=subfield, message="unknown author role")

    if clean_role not in translations or clean_role is None:
        return "Speaker"

    return translations[clean_role]


def get_contributor(key, value, contributor_role="", name=""):
    """Create contributor json."""
    beard = value.get("9")
    if beard is not None and beard != "#BEARD#":
        # checking if anything else stored in this field
        # historically it was some kind of automatic script tagging
        # and it should be ignored if value == #BEARD#
        raise UnexpectedValue(field=key, subfield="9", value=beard)
    if not name:
        name = value.get("a", "").strip()
    affiliation = value.get("u", "")
    contributor = {"name": name}
    if affiliation:
        contributor.update({"affiliations": [affiliation]})
    if contributor_role:
        contributor.update({"role": contributor_role})
    else:
        # Role is mandatory from UI but not for the datamodel?
        role = get_contributor_role("e", value.get("e", ""))
        contributor.update({"role": role})
    if not name:
        raise UnexpectedValue(
            field=key, subfield="a", message="Contributor name missing!"
        )
    return contributor
