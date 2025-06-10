# -*- coding: utf-8 -*-
#
# This file is part of Invenio.
# Copyright (C) 2015-2018 CERN.
#
# cds-migrator-kit is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.

"""Helper functions for usage in tests."""

import json
import xml.etree.ElementTree as ET
from os.path import join


def load_json(datadir, filename):
    """Load file in json format."""
    filepath = join(datadir, filename)
    data = None
    with open(filepath, "r") as file_:
        data = json.load(file_)
    return data


def remove_tag_from_marcxml(marcxml, tag):
    """
    Removes a specific MARCXML datafield tag to manipulate the record.

    :param marcxml: The MARCXML string.
    :param tag: The MARC tag (e.g., "520") to remove.
    :return: Modified MARCXML string with the specified tag removed.
    """
    root = ET.fromstring(marcxml)

    # Find and remove all <datafield> elements with the specified tag
    for datafield in root.findall(f".//datafield[@tag='{tag}']"):
        root.remove(datafield)

    return ET.tostring(root, encoding="unicode")


def add_tag_to_marcxml(marcxml, tag, subfields, ind1=" "):
    """
    Adds a MARCXML datafield tag to manipulate the record.

    :param marcxml: The MARCXML string.
    :param tag: The MARC tag (e.g., tag="999", ind1=" ", ind2=" ") to add.
    :param subfields: Dictionary of subfields (e.g., {"a": "New Description"}).
    :return: Modified MARCXML string with the new tag added.
    """
    root = ET.fromstring(marcxml)

    # Create new datafield element
    new_datafield = ET.Element("datafield", tag=tag, ind1=ind1, ind2=" ")

    for code, values in subfields.items():
        if not isinstance(values, list):
            values = [values]  # Normalize to list
        for value in values:
            subfield = ET.SubElement(new_datafield, "subfield", code=code)
            subfield.text = value

    # Append the new datafield
    root.append(new_datafield)

    return ET.tostring(root, encoding="unicode")
