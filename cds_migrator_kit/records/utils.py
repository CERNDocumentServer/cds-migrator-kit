# -*- coding: utf-8 -*-
#
# This file is part of Invenio.
# Copyright (C) 2015-2018 CERN.
#
# cds-migrator-kit is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.

"""CDS Migrator Records utils."""
import re

from fuzzywuzzy import fuzz


def same_issn(obj1, obj2):
    """Check if two objects have the same ISSN."""
    return (
        obj1["issn"] is not None
        and obj2["issn"] is not None
        and obj1["issn"] == obj2["issn"]
    )


def compare_titles(title1, title2):
    """Return the ratio of the fuzzy comparison between two titles."""
    return fuzz.ratio(title1, title2)


def clean_exception_message(message):
    """Cleanup exception message."""
    match = re.match(r"^(\[[^\]]*\])?(.*)$", message)
    if match:
        return match.group(2).strip().capitalize()
    return message
