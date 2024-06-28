# -*- coding: utf-8 -*-
#
# This file is part of Invenio.
# Copyright (C) 2015-2018 CERN.
#
# cds-migrator-kit is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.

"""CDS Migrator Records loader."""

from __future__ import absolute_import, print_function

import logging

import arrow
from cds_dojson.marc21.utils import create_record
from cds_dojson.overdo import OverdoBase
from flask import current_app

from cds_migrator_kit.errors import ManualImportRequired, MissingRequiredField, UnexpectedValue
from cds_migrator_kit.records.errors import LossyConversion
from cds_migrator_kit.records.handlers import migration_exception_handler
from cds_migrator_kit.xml_to_json_dump import CDSRecordDump

cli_logger = logging.getLogger('migrator')

marc21 = OverdoBase(entry_point_models="cds.importer.models")

class CDSMigKitDump(CDSRecordDump):
    """CDS record dump class."""

    def __init__(self,
                 data,
                 source_type='marcxml',
                 latest_only=False,
                 dojson_model=marc21,
                 logger=None):
        """Initialize."""
        super().__init__(data, source_type, latest_only, dojson_model)
        self.logger = logger
        cli_logger.info('\n=====#RECID# {0} INIT=====\n'.format(data['recid']))

    def _prepare_intermediate_revision(self, data):
        """Convert intermediate versions to marc into JSON."""
        # TODO
        dt = arrow.get(data['modification_datetime']).datetime

        if self.source_type == 'marcxml':
            marc_record = create_record(data['marcxml'])
            return dt, marc_record
        else:
            val = data['json']

        return dt, val

    def _prepare_final_revision(self, data):
        dt = arrow.get(data['modification_datetime']).datetime

        exception_handlers = {
            UnexpectedValue: migration_exception_handler(self.logger),
            MissingRequiredField: migration_exception_handler(self.logger),
            ManualImportRequired: migration_exception_handler(self.logger),
        }
        if self.source_type == 'marcxml':
            marc_record = create_record(data['marcxml'])
            try:
                val = self.dojson_model.do(
                    marc_record, exception_handlers=exception_handlers)
                missing = self.dojson_model.missing(marc_record)
                if missing:
                    raise LossyConversion(missing=missing)
                return dt, val
            except LossyConversion as e:
                raise e
            except Exception as e:
                current_app.logger.error(
                    'Impossible to convert to JSON {0} - {1}'.format(
                        e, marc_record))
                raise e
        else:
            val = data['json']

            # Calculate the _access key
            return dt, val

    def prepare_revisions(self):
        """Prepare data.

        We don't convert intermediate versions to JSON to avoid conversion
        errors and get a lossless version migration.

        If the revisions is the last one, an error will be generated if the
        final translation is not complete.
        """
        # Prepare revisions
        self.revisions = []
        it = [self.data['record'][0]] if self.latest_only \
            else self.data['record']

        for i in it[:-1]:
            self.revisions.append(self._prepare_intermediate_revision(i))

        self.revisions.append(self._prepare_final_revision(it[-1]))
