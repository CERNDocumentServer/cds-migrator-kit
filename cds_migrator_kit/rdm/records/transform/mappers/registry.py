# -*- coding: utf-8 -*-
#
# Copyright (C) 2022-2026 CERN.
#
# CDS-RDM is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""Composed lists of field mappers used by RecordEntry."""
from cds_migrator_kit.rdm.records.transform.mappers.base import (
    PassthroughCustomFieldMapper,
    PassthroughMapper,
)
from cds_migrator_kit.rdm.records.transform.mappers.contributors import (
    ContributorsMapper,
    CreatorsMapper,
)
from cds_migrator_kit.rdm.records.transform.mappers.custom_fields import (
    AcceleratorsMapper,
    BeamsMapper,
    DepartmentsMapper,
    ExperimentsMapper,
    JournalMapper,
    ProgrammesMapper,
)
from cds_migrator_kit.rdm.records.transform.mappers.metadata import (
    PASSTHROUGH_METADATA_FIELDS,
    IdentifiersMapper,
    PublicationDateMapper,
    ResourceTypeMapper,
    SubjectsMapper,
    TableOfContentsMapper,
    TitleMapper,
)

# Order matters: TableOfContentsMapper must run before "additional_descriptions"
# is read elsewhere, and ResourceTypeMapper must run before TitleMapper (which
# reads the already-resolved metadata["resource_type"]).
METADATA_MAPPERS = (
    TableOfContentsMapper(),
    ResourceTypeMapper(),
    TitleMapper(),
    CreatorsMapper(),
    ContributorsMapper(),
    PublicationDateMapper(),
    SubjectsMapper(),
    IdentifiersMapper(),
    *(PassthroughMapper(field_name) for field_name in PASSTHROUGH_METADATA_FIELDS),
)

# custom_fields keys copied straight from the legacy custom_fields dict,
# each with its own default when absent.
CUSTOM_FIELDS_PASSTHROUGH_DEFAULTS = {
    "cern:administrative_unit": [],
    "cern:projects": [],
    "cern:facilities": [],
    "cern:studies": [],
    "cern:committees": None,
    "cern:oa_funding_model": None,
    "thesis:thesis": {},
    "imprint:imprint": {},
    "meeting:meeting": {},
}

# Every custom_fields mapper writes its own key(s) directly into
# ctx.custom_fields and handles its own curation flagging internally, so
# this list is run as a single uninterrupted loop - see
# CustomFieldMapper in mappers/base.py. Order matters only for
# "cern:administrative_unit": DepartmentsMapper overrides the passthrough
# default when a department can't be matched, so it must run after it.
CUSTOM_FIELD_MAPPERS = (
    *(
        PassthroughCustomFieldMapper(field_name, default)
        for field_name, default in CUSTOM_FIELDS_PASSTHROUGH_DEFAULTS.items()
    ),
    JournalMapper(),
    ProgrammesMapper(),
    ExperimentsMapper(),
    DepartmentsMapper(),
    AcceleratorsMapper(),
    BeamsMapper(),
)
