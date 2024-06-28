# -*- coding: utf-8 -*-
#
# This file is part of Invenio.
# Copyright (C) 2024 CERN.
#
# cds-migrator-kit is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.

"""Exceptions."""

from dojson.errors import DoJSONException

#################################################################
# CDS-ILS Migrator Exceptions
#################################################################

class LossyConversion(DoJSONException):
    """Data lost during migration."""

    def __init__(self, *args, **kwargs):
        """Exception custom initialisation."""
        self.missing = kwargs.pop("missing", None)
        self.message = self.description = "Lossy conversion: {0}".format(
            self.missing or ""
        )
        super().__init__(*args, **kwargs)


class RecordNotDeletable(DoJSONException):
    """Record is not marked as deletable."""

    def __init__(self, *args, **kwargs):
        """Exception custom initialisation."""
        self.message = self.description = "Record is not marked as deletable"
        super().__init__(*args, **kwargs)


class ProviderNotAllowedDeletion(DoJSONException):
    """Provider is not allowed to delete records."""

    def __init__(self, *args, **kwargs):
        """Exception custom initialisation."""
        self.provider = kwargs.pop("provider", None)
        self.message = self.description = (
            "This provider {0} is not allowed to delete records".format(self.provider)
        )
        super().__init__(*args, **kwargs)


class CDSImporterException(DoJSONException):
    """CDSDoJSONException class."""

    def __init__(self, *args, **kwargs):
        """Constructor."""
        self.subfield = kwargs.get("subfield", "")
        message = kwargs.get("message", None)
        if message:
            self.message = message

        # because of ILSRestException class attributes
        self.description = self.message

        super(CDSImporterException, self).__init__(*args)


class RecordModelMissing(CDSImporterException):
    """Missing record model exception."""

    message = "[Record did not match any available model]"


class UnexpectedValue(CDSImporterException):
    """The corresponding value is unexpected."""

    message = "[UNEXPECTED INPUT VALUE]"


class MissingRequiredField(CDSImporterException):
    """The corresponding value is required."""

    message = "[MISSING REQUIRED FIELD]"


class ManualImportRequired(CDSImporterException):
    """The corresponding field should be manually migrated."""

    message = "[MANUAL IMPORT REQUIRED]"


class DocumentImportError(CDSImporterException):
    """Document import exception."""

    message = "[DOCUMENT IMPORT ERROR]"


class SeriesImportError(CDSImporterException):
    """Document import exception."""

    message = "[SERIES IMPORT ERROR]"


class UnknownProvider(CDSImporterException):
    """Unknown provider exception."""

    message = "Unknown record provider."


class InvalidProvider(CDSImporterException):
    """Invalid provider exception."""

    message = "Invalid record provider."


class SimilarityMatchUnavailable(CDSImporterException):
    """Similarity match unavailable exception."""

    message = (
        "Title similarity matching cannot be performed for "
        "this record. Please import it manually."
    )


###############################################################################
# Migration exceptions
###############################################################################


class DumpRevisionException(Exception):
    """Exception for dump revision."""


class JSONConversionException(Exception):
    """JSON Conversion Exception in migration."""


class MigrationException(Exception):
    """Base exception for CDS-ILS migration errors."""


class DocumentMigrationError(MigrationException):
    """Raised for multipart migration errors."""


class SeriesMigrationError(MigrationException):
    """Raised for multipart migration errors."""


class MultipartMigrationError(MigrationException):
    """Raised for multipart migration errors."""


class UserMigrationError(MigrationException):
    """Raised for user migration errors."""


class SerialMigrationError(MigrationException):
    """Raised for serial migration errors."""


class ItemMigrationError(MigrationException):
    """Raised for item migration errors."""


class LoanMigrationError(MigrationException):
    """Raised for loan migration errors."""


class EItemMigrationError(MigrationException):
    """Raised for EItem migration errors."""


class FileMigrationError(MigrationException):
    """Raised for File migration errors."""


class BorrowingRequestError(MigrationException):
    """Raised for borrowing request migration errors."""


class AcqOrderError(MigrationException):
    """Raised for acquisition order migration errors."""


class ProviderError(MigrationException):
    """Raised for provider migration errors."""


class RelationMigrationError(MigrationException):
    """Raised for exceptions when migrating relations."""
