# -*- coding: utf-8 -*-
#
# Copyright (C) 2022-2025 CERN.
#
# CDS-RDM is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""CDS-RDM migration errors module."""

class CDSMigrationException(Exception):
    """CDSDoJSONException class."""

    description = None

    def __init__(
        self,
        message=None,
        field=None,
        subfield=None,
        value=None,
        stage=None,
        recid=None,
        exc=None,
        priority=None,
        *args,
        **kwargs
    ):
        """Constructor."""
        self.subfield = subfield
        self.field = field
        self.value = value
        self.stage = stage
        self.recid = recid
        self.type = str(self.__class__.__name__)
        self.exc = exc
        self.message = message
        self.priority = priority
        super(CDSMigrationException, self).__init__(*args)


class RecordModelMissing(CDSMigrationException):
    """Missing record model exception."""

    description = "[Record did not match any available model]"


class UnexpectedValue(CDSMigrationException):
    """The corresponding value is unexpected."""

    description = "[UNEXPECTED INPUT VALUE]"


class MissingRequiredField(CDSMigrationException):
    """The corresponding value is required."""

    description = "[MISSING REQUIRED FIELD]"


class ManualImportRequired(CDSMigrationException):
    """The corresponding field should be manually migrated."""

    description = "[MANUAL IMPORT REQUIRED]"


class RestrictedFileDetected(CDSMigrationException):
    """Record has restricted files record."""

    description = "[Restricted file detected]"


class RecordFlaggedCuration(CDSMigrationException):
    """Record statistics error."""

    description = "[Record needs to be curated]"
