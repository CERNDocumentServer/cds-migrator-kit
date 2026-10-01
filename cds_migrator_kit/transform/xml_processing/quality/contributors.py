# -*- coding: utf-8 -*-
#
# Copyright (C) 2022 CERN.
#
# CDS-RDM is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""CDS-RDM contributors migration module."""

import re

import idutils
from dojson.utils import force_list

from cds_migrator_kit.errors import UnexpectedValue
from cds_migrator_kit.transform.xml_processing.quality.parsers import StringValue
from cds_migrator_kit.transform.xml_processing.quality.regex import ALPHANUMERIC_ONLY

# RDM:
# "contributors": {
#   "description": "Contributors in order of importance.",
#   "type": "array",
#   "items": {
#     "type": "object",
#     "additionalProperties": false,
#     "properties": {
#       "person_or_org": {
#          "type": "object",
#           "additionalProperties": false,
#           "properties": {
#           "name": {
#           "type": "string"
#         },
#          "type": {
#               "description": "Type of name.",
#               "type": "string",
#               "enum": ["personal", "organizational"]
#           },
#           "given_name": {
#           "type": "string"
#           },
#           "family_name": {
#               "type": "string"
#           },
#           "identifiers": {
#               "type": "array",
#               "items": {
#                   "description": "Identifiers object with identifier
#                                   value and scheme in separate keys.",
#                   "type": "object",
#                   "additionalProperties": false,
#                   "properties": {
#                       "identifier": {
#                       "description": "An identifier.",
#                       "type": "string"
#                   },
#                   "scheme": {
#                       "description": "A scheme.",
#                       "type": "string" # VOCABULARY ?
#                   }
#           }
#         },
#         "uniqueItems": true
#         }
#       }
#     },
#     "role": {
#       "description": "Role of creator/contributor.",
#       "type": "object"
#       "additionalProperties": false,
#       "properties": {
#       "id": { # ROLES VOCABULARY }
#       }
#     },
#     "affiliations": {
#       "type": "array",
#       "uniqueItems": true,
#       "items": {
#           type": "object",
#           "additionalProperties": false,
#           "properties": {
#           "id": {
#               # AFFILIATIONS VOCABULARY
#           },
#         }
#       },
#       "required": [
#         "name"
#         ]
#       }
#     }
#   }
#  }
# }


def get_contributor_role(subfield, role, raise_unexpected=False):
    """Clean up roles."""
    translations = {
        "author": "OTHER",
        "author.": "OTHER",
        "dir.": "SUPERVISOR",
        "dir": "SUPERVISOR",
        "supervisor": "SUPERVISOR",
        "ed.": "EDITOR",
        "editor": "EDITOR",
        "editor.": "EDITOR",
        "ed": "EDITOR",
        "ill.": "other",
        "ill": "other",
        "ed. et al.": "EDITOR",
    }
    clean_role = None
    if role is None:
        return "other"
    if isinstance(role, str):
        clean_role = role.lower()
    elif isinstance(role, list) and role and role[0]:
        clean_role = role[0].lower()
    elif raise_unexpected:
        raise UnexpectedValue(subfield=subfield, message="unknown author role")

    if clean_role not in translations or clean_role is None:
        return "other"

    return translations[clean_role].lower()


def get_contributor_affiliations(info):
    """Get affiliations of a contributor/creator."""
    u = info.get("u")
    v = info.get("v")
    if not u and not v:
        return
    if not v:
        affiliations = force_list(u)
    else:
        affiliations = force_list(v)
    parsed_affiliations = [
        StringValue(aff).parse(filter_regex=ALPHANUMERIC_ONLY) for aff in affiliations
    ]
    return parsed_affiliations


def format_author_orcid(author_orcid):
    """Format a single ORCiD value for a single author."""
    author_orcid = author_orcid.replace("ORCID:", "")
    if not author_orcid.lower().startswith("jacow-"):
        if idutils.is_orcid(author_orcid):
            new_id = {"identifier": author_orcid, "scheme": "orcid"}
            return new_id
        else:
            raise UnexpectedValue(
                message="Author has invalid orcid",
                value=author_orcid,
                stage="transform",
            )
    return None


def extract_json_contributor_ids(info, orcid_subfield="k"):
    """Extract author IDs from MARC tags."""
    SOURCES = {
        "AUTHOR|(INSPIRE)": "inspire_author",
        "AUTHOR|(CDS)": "cds",
        "AUTHOR|(SzGeCERN)": "cern",
    }
    regex = re.compile(r"(AUTHOR\|\((INSPIRE|CDS|SzGeCERN)\))(.*)")
    ids = []
    author_ids = force_list(info.get("0", ""))
    for author_id in author_ids:
        match = regex.match(author_id)
        if match:
            identifier = {
                "identifier": match.group(3),
                "scheme": SOURCES[match.group(1)],
            }
            if identifier not in ids:
                ids.append(identifier)

    author_orcid = info.get(orcid_subfield)
    if author_orcid:
        # If there are multiple ORCiDs (or ORCiD-like identifiers) they will be in a tuple
        # We need to make sure the tuple only has one actual ORCiD. It can have other non-ORCiD
        # identifiers, which we will skip/ignore if they are JACoW identifiers and error otherwise.
        if isinstance(author_orcid, tuple):
            orcid_seen = False
            for val in author_orcid:
                new_id = format_author_orcid(val)
                if new_id is None:
                    # ID was a skippable non-ORCiD value
                    continue

                if new_id not in ids:
                    if orcid_seen:
                        raise UnexpectedValue(
                            message="Multiple ORCID values found for a single author",
                            value=author_orcid,
                            stage="transform",
                        )

                    orcid_seen = True
                    ids.append(new_id)
        else:
            new_id = format_author_orcid(author_orcid)
            # new_id is None if it is a skippable non-ORCiD value
            if new_id is not None and new_id not in ids:
                ids.append(new_id)

    inspire = info.get("i", "")
    if inspire and inspire.startswith("INSPIRE-"):
        new_id = {"identifier": inspire, "scheme": "inspire_author"}
        if new_id not in ids:
            ids.append(new_id)

    return ids
