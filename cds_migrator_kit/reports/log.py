# -*- coding: utf-8 -*-
#
# This file is part of Invenio.
# Copyright (C) 2015-2018 CERN.
#
# cds-migrator-kit is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.

"""CDS Migrator Records loggers."""

import csv
import json
import logging
import os
from copy import deepcopy

from flask import current_app


class StandardLogger:
    logger = None

    @classmethod
    def initialize(cls, log_dir):
        """Constructor."""
        logger_migrator = logging.getLogger("migrator-rules")
        logger_users = logging.getLogger("users")
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
        fh = logging.FileHandler(log_dir / "users.log")
        fh.setFormatter(formatter)
        logger_users.addHandler(fh)
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


class MigrationProgressLogger:

    def __init__(
        self,
        collection,
        log_progress_filename="rdm_migration_errors.csv",
    ):
        """Constructor."""
        self._logs_path = os.path.join(
            current_app.config["CDS_MIGRATOR_KIT_LOGS_PATH"], collection
        )
        self.PROGRESS_LOG_FILEPATH = os.path.join(
            self._logs_path, log_progress_filename
        )
        self.collection = collection

        if not os.path.exists(self._logs_path):
            os.makedirs(self._logs_path)

        self.error_file = open(self.PROGRESS_LOG_FILEPATH, "a")
        columns = [
            "recid",
            "stage",
            "type",
            "error",
            "field",
            "value",
            "message",
            "clean",
            "priority",
        ]
        self.log_writer = csv.DictWriter(self.error_file, fieldnames=columns)
        self._temp_state_cache = {}

    def start_log(self):
        """Initialize logging file descriptors."""
        # init log files

        self.error_file.truncate(0)
        self.log_writer.writeheader()
        self.error_file.flush()

    def read_log(self):
        """Read error log file."""
        self.error_file = open(self.PROGRESS_LOG_FILEPATH, "r")
        reader = csv.DictReader(self.error_file)
        for row in reader:
            yield row

    def finalise(self):
        """Finalise logging files."""
        self.error_file.close()

    def add_log(self, exc, record=None, key=None, value=None):
        """Add exception log."""
        logger_migrator = logging.getLogger("migrator-rules")

        recid = getattr(exc, "recid", None)
        if not recid and record:
            recid = record.get("recid", None) or record.get("record", {}).get("recid")

        subfield = f"subfield: {exc.subfield}" if getattr(exc, "subfield", None) else ""
        error_format = {
            "recid": recid,
            "type": getattr(exc, "type", None),
            "error": getattr(exc, "description", None),
            "field": f"{getattr(exc, 'field', key)} {subfield}",
            "value": getattr(exc, "value", value),
            "stage": getattr(exc, "stage", None),
            "message": getattr(exc, "message", str(exc)),
            "priority": getattr(exc, "priority", None),
            "clean": False,
        }
        self.log_writer.writerow(error_format)
        logger_migrator.error(exc)
        self.error_file.flush()

    def add_information(self, recid, state):
        """Save a temporary success state for recid.

        For example, we store affiliation warnings when we don't match.
        """
        if recid in self._temp_state_cache:
            new_state = deepcopy(self._temp_state_cache[recid])
            new_state["message"] = f"{new_state['message']}\n{state['message']}"
            new_state["value"] = f"{new_state['value']}\n{state['value']}"
            state = new_state
        self._temp_state_cache[recid] = state

    def finalise_record(self, recid):
        """Log recid as success."""
        _state = self._temp_state_cache.pop(recid, {})
        self.log_writer.writerow({"recid": recid, "clean": True, **_state})


class RecordStateLogger:

    def __init__(
        self,
        collection,
        records_dump_filename="rdm_records_dump.json",
        records_state_filename="rdm_records_state.json",
    ):
        """Constructor."""
        self._logs_path = os.path.join(
            current_app.config["CDS_MIGRATOR_KIT_LOGS_PATH"], collection
        )
        self.RECORD_DUMP_FILEPATH = os.path.join(self._logs_path, records_dump_filename)
        self.RECORD_STATE_FILEPATH = os.path.join(
            self._logs_path, records_state_filename
        )
        self.collection = collection

        if not os.path.exists(self._logs_path):
            os.makedirs(self._logs_path)

        self.record_dump_file = open(self.RECORD_DUMP_FILEPATH, "a")
        self.record_state_file = open(self.RECORD_STATE_FILEPATH, "a")

    def start_log(self):
        """Initialize logging file descriptors."""
        # init log files
        with open(self.RECORD_DUMP_FILEPATH, "w") as temp_dump_file:
            temp_dump_file.truncate(0)
            temp_dump_file.write("{\n")
        with open(self.RECORD_STATE_FILEPATH, "w") as temp_state_file:
            temp_state_file.truncate(0)
            temp_state_file.write("[\n")

    def add_record(self, record, **kwargs):
        """Add record to list of collected records."""
        recid = record["legacy_recid"]
        self.record_dump_file.write(f'"{recid}": {json.dumps(record)},\n')
        self.record_dump_file.flush()

    def add_record_state(self, record_state, **kwargs):
        """Add record state."""
        self.record_state_file.write(f"{json.dumps(record_state)},\n")
        self.record_state_file.flush()

    def load_record_dumps(self):
        """Load stats from file as json."""
        record_dump_file = open(self.RECORD_DUMP_FILEPATH, "r")
        yield json.load(record_dump_file)
        record_dump_file.close()

    def finalise(self):
        """Finalise logging files."""
        # remove last comma and newline in the json dump
        self.record_dump_file.close()
        self.record_state_file.close()

        with open(self.RECORD_DUMP_FILEPATH, "r+") as temp_dump_file:
            temp_dump_file.seek(0, 2)
            temp_dump_file.truncate(temp_dump_file.tell() - 2)
            temp_dump_file.seek(0, 2)
            temp_dump_file.write("}")

        with open(self.RECORD_STATE_FILEPATH, "r+") as temp_state_file:
            temp_state_file.seek(0, 2)
            temp_state_file.truncate(temp_state_file.tell() - 2)
            temp_state_file.seek(0, 2)
            temp_state_file.write("]")
