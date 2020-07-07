# -*- coding: utf-8 -*-
#
# This file is part of Invenio.
# Copyright (C) 2020 CERN.
#
# cds-migrator-kit is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.

"""CDS Migrator Records validators."""


from cds_migrator_kit.records.errors import RequiredFieldMissing


def document_validator(record_json):
    """Validates required fields."""
    required = ["$schema", "title", "authors", "publication_year",
                "document_type"]
    for field in required:
        if field not in record_json:
            raise RequiredFieldMissing(missing=field, subfield=field)


def record_validator(record_json, rectype):
    """Select validator type based on record type."""
    if rectype == 'document':
        return document_validator(record_json)
