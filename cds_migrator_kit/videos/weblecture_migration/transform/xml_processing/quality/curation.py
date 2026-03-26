# -*- coding: utf-8 -*-
#
# Copyright (C) 2025 CERN.
#
# CDS-Videos is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""CDS-Videos helper migration module."""


def transform_subfields(key, value):
    """Helper to transform MARC subfields into key-prefixed strings."""
    value = dict(value)

    priority = {
        "9": 0,
        "8": 1,
    }

    output = []
    for subfield, subvalue in sorted(
        value.items(),
        key=lambda item: (priority.get(item[0], 99), item[0]),
    ):
        if isinstance(subvalue, (list, tuple)):
            for item in subvalue:
                output.append(f"{key}{subfield}:{item}")
        else:
            output.append(f"{key}{subfield}:{subvalue}")
    return output
