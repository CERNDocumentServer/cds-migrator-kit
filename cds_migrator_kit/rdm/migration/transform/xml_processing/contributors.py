# -*- coding: utf-8 -*-
#
# Copyright (C) 2022 CERN.
#
# CDS-RDM is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""CDS-RDM contributors migration module."""

import re

from dojson.utils import force_list
from dojson.errors import IgnoreKey
from cds_migrator_kit.rdm.migration.transform.xml_processing.errors import (
    UnexpectedValue,
)
from invenio_records_resources.proxies import current_service_registry
from invenio_access.permissions import system_identity



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
    aff_results = []
    affiliations = force_list(info.get("u", None))

    service = current_service_registry.get("affiliations")

    if affiliations:
        for affiliation_name in affiliations:
            vocabulary_result = service.search(system_identity, params={"q": affiliation_name}).to_dict()
            affiliation_hits = vocabulary_result["hits"]
            if affiliation_hits["total"] == 0:
                raise UnexpectedValue(field="creators", subfield="affiliations", message=f"No affiliation found by the name: {affiliation_name}")
            else:
                aff_results.append({"name": affiliation_name, "id": affiliation_hits["hits"][0]["id"]})

        return aff_results
    else:
        raise IgnoreKey("affiliations")


def extract_json_contributor_ids(info):
    """Extract author IDs from MARC tags."""
    SOURCES = {
        "AUTHOR|(INSPIRE)": "inspire",
        "AUTHOR|(CDS)": "cds",
        # "AUTHOR|(SzGeCERN)": "CERN", --> ?
    }
    regex = re.compile(r"(AUTHOR\|\((INSPIRE|CDS)\))(.*)")
    ids = []
    author_ids = force_list(info.get("0", ""))
    for author_id in author_ids:
        match = regex.match(author_id)
        if match:
            # ids.append(
            #     {"identifier": match.group(3), "scheme": SOURCES[match.group(1)]}
            # )
            pass
    # try:
    #     ids.append({"identifier": info["inspireid"], "scheme": "inspire"})
    # except KeyError:
    #     pass

    author_orcid = info.get("k")
    if author_orcid:
        ids.append({"identifier": author_orcid, "scheme": "orcid"})

    return ids
