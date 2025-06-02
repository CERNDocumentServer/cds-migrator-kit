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
    output = []
    for subfield, subvalue in value.items():
        if isinstance(subvalue, (list, tuple)):
            for item in subvalue:
                output.append(f"{key}{subfield}:{item}")
        else:
            output.append(f"{key}{subfield}:{subvalue}")
    return output
