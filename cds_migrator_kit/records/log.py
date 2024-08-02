# -*- coding: utf-8 -*-
#
# This file is part of Invenio.
# Copyright (C) 2015-2018 CERN.
#
# cds-migrator-kit is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.

"""CDS Migrator Records loggers."""

import copy
import json
import logging
import os

from flask import current_app
from marshmallow import ValidationError

from cds_migrator_kit.rdm.migration.transform.xml_processing.errors import (
    LossyConversion,
    ManualImportRequired,
    MissingRequiredField,
    UnexpectedValue,
)
from cds_migrator_kit.records.utils import (
    clean_exception_message,
    compare_titles,
    same_issn,
)


class Singleton(type):
    """Temporary solution for this logger."""

    _instances = {}

    def __call__(cls, *args, **kwargs):
        """Call."""
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class JsonLogger(metaclass=Singleton):
    """Log migration statistic to file controller."""

    logger = None

    @classmethod
    def initialize(cls, log_dir):
        """Constructor."""
        logger_migrator = logging.getLogger("migrator-rules")
        logger_migrator.setLevel(logging.DEBUG)
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - "
            "%(message)s - \n "
            "[in %(pathname)s:%(lineno)d]"
        )
        # errors to file
        fh = logging.FileHandler(log_dir / "error.log")
        fh.setLevel(logging.ERROR)
        fh.setFormatter(formatter)
        logger_migrator.addHandler(fh)
        # info to stream/stdout
        sh = logging.StreamHandler()
        sh.setFormatter(formatter)
        sh.setLevel(logging.INFO)
        logger_migrator.addHandler(sh)

        logger_matcher = logging.getLogger("cds_dojson.matcher.dojson_matcher")
        logger_matcher.setLevel(logging.DEBUG)
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - "
            "%(message)s - \n "
            "[in %(pathname)s:%(lineno)d]"
        )
        mh = logging.FileHandler("matcher.log")
        mh.setFormatter(formatter)
        mh.setLevel(logging.DEBUG)
        logger_matcher.addHandler(mh)

    @classmethod
    def get_logger(cls):
        """Get migration logger."""
        return logging.getLogger("migrator-rules")

    @classmethod
    def get_json_logger(cls):
        """Get JsonLogger instance based on the rectype."""
        return RDMJsonLogger

    def __init__(self, stats_filename, records_filename):
        """Constructor."""
        self._logs_path = current_app.config["CDS_MIGRATOR_KIT_LOGS_PATH"]
        self.stats = {}
        self.records = {}
        self.STAT_FILEPATH = os.path.join(self._logs_path, stats_filename)
        self.RECORD_FILEPATH = os.path.join(self._logs_path, records_filename)

        if not os.path.exists(self._logs_path):
            os.makedirs(self._logs_path)

    def load(self):
        """Load stats from file as json."""
        logger = logging.getLogger("migrator-rules")
        logger.warning(self.STAT_FILEPATH)
        with open(self.STAT_FILEPATH, "r") as f:
            self.stats = json.load(f)
        with open(self.RECORD_FILEPATH, "r") as f:
            self.records = json.load(f)

    def save(self):
        """Save stats from file as json."""
        logger = logging.getLogger("migrator-rules")
        logger.warning(self.STAT_FILEPATH)
        with open(self.STAT_FILEPATH, "w") as f:
            json.dump(self.stats, f)
        with open(self.RECORD_FILEPATH, "w") as f:
            json.dump(self.records, f)

    def add_recid_to_stats(self, recid, **kwargs):
        """Add recid to stats."""
        pass

    def add_record(self, record, **kwargs):
        """Add record to list of collected records."""
        pass

    def add_log(self, exc, key=None, value=None, output=None):
        """Add exception log."""
        self.resolve_error_type(exc, output, key, value)

    def resolve_error_type(self, exc, output, key, value):
        """Check the type of exception and log to dict."""
        recid = output.get("recid", None) or output.get("record", {}).get(
            "recid", {}
        )
        rec_stats = self.stats[recid]
        rec_stats["clean"] = False
        if isinstance(exc, ManualImportRequired):
            rec_stats["manual_migration"].append(
                dict(
                    key=key,
                    value=value,
                    subfield=exc.subfield,
                    message=clean_exception_message(exc.message),
                )
            )
        elif isinstance(exc, UnexpectedValue):
            rec_stats["unexpected_value"].append(
                dict(
                    key=key,
                    value=value,
                    subfield=exc.subfield,
                    message=clean_exception_message(exc.message),
                )
            )
        elif isinstance(exc, MissingRequiredField):
            rec_stats["missing_required_field"].append(
                dict(
                    key=key,
                    value=value,
                    subfield=exc.subfield,
                    message=clean_exception_message(exc.message),
                )
            )
        elif isinstance(exc, LossyConversion):
            rec_stats["lost_data"].append(
                dict(
                    key=key, value=value, missing=list(exc.missing), message=exc.message
                )
            )
        elif isinstance(exc, ValidationError):
            rec_stats["missing_required_field"].append(
                dict(
                    value=exc.value,
                    subfield=exc.subfield,
                    missing=list(exc.missing),
                    message=exc.message,
                )
            )
        elif isinstance(exc, KeyError):
            rec_stats["unexpected_value"].append(str(exc))
        elif isinstance(exc, TypeError) or isinstance(exc, AttributeError):
            rec_stats["unexpected_value"].append(
                "Model definition missing for this record."
                " Contact CDS team to tune the query"
            )
        else:
            raise exc


class RDMJsonLogger(JsonLogger):
    """Log rdm record migration statistic to file controller."""

    def __init__(self):
        """Constructor."""
        super().__init__("rdm_stats.json", "rdm_records.json")

    def add_recid_to_stats(self, recid):
        """Add empty log item."""
        if recid not in self.stats:
            self.stats[recid] = {
                "recid": recid,
                "manual_migration": [],
                "unexpected_value": [],
                "missing_required_field": [],
                "lost_data": [],
                "clean": True,
            }

    def add_record(self, record):
        """Add record to collected records."""
        self.records[record["legacy_recid"]] = record
