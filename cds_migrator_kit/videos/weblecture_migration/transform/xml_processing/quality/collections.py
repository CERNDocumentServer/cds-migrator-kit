# -*- coding: utf-8 -*-
#
# Copyright (C) 2025 CERN.
#
# CDS-Videos is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""CDS-Videos collections migration module."""


def append_collection_hierarchy(collection_list, tag_string):
    """Appends hierarchical tag levels to self['collections']."""
    parts = tag_string.split("::")
    for i in range(1, len(parts) + 1):
        hierarchical_tag = "::".join(parts[:i])
        if hierarchical_tag not in collection_list:
            collection_list.append(hierarchical_tag)
    return collection_list
