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
        keep_logs=False,
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
        self.keep_logs = keep_logs
        if not os.path.exists(self._logs_path):
            os.makedirs(self._logs_path)

        self.error_file = open(self.PROGRESS_LOG_FILEPATH, "a", newline="")
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
        if not self.keep_logs:
            # clear existing file content
            self.error_file.close()
            self.error_file = open(self.PROGRESS_LOG_FILEPATH, "w", newline="")
            self.log_writer = csv.DictWriter(
                self.error_file, fieldnames=self.log_writer.fieldnames
            )
            self.log_writer.writeheader()
        else:
            # if appending, check if the file is empty and needs a header
            self.error_file.seek(0, os.SEEK_END)
            if self.error_file.tell() == 0:
                self.log_writer.writeheader()
        self.error_file.flush()

    def read_log(self):
        """Read error log file."""
        with open(self.PROGRESS_LOG_FILEPATH, "r", newline="") as f:
            reader = csv.DictReader(f)
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
        self.error_file.flush()


class RecordStateLogger:

    def __init__(
        self,
        collection,
        keep_logs=False,
        records_dump_filename="rdm_records_dump.json",
        records_state_filename="rdm_records_state.json",
    ):
        """Constructor."""
        base_path = current_app.config["CDS_MIGRATOR_KIT_LOGS_PATH"]
        self._logs_path = os.path.join(base_path, collection)
        os.makedirs(self._logs_path, exist_ok=True)

        self.RECORD_DUMP_FILEPATH = os.path.join(self._logs_path, records_dump_filename)
        self.RECORD_STATE_FILEPATH = os.path.join(
            self._logs_path, records_state_filename
        )
        self.keep_logs = keep_logs

        self._records = {}
        self._record_states = []
        self._existing_recids = set()

    def _load_existing_logs(self):
        """Load existing JSON data if keeping logs."""
        if not self.keep_logs:
            return

        # Load records
        if os.path.exists(self.RECORD_DUMP_FILEPATH):
            try:
                with open(self.RECORD_DUMP_FILEPATH, encoding="utf-8") as f:
                    self._records = json.load(f)
                self._existing_recids = set(self._records.keys())
            except Exception:
                self._records = {}
                self._existing_recids = set()

        # Load record states
        if os.path.exists(self.RECORD_STATE_FILEPATH):
            try:
                with open(self.RECORD_STATE_FILEPATH, encoding="utf-8") as f:
                    self._record_states = json.load(f)
            except Exception:
                self._record_states = []

    def start_log(self):
        """Initialize logger."""
        self._load_existing_logs()

    def add_record(self, record, **kwargs):
        """Add record to list of collected records."""
        recid = str(record["legacy_recid"])
        if recid not in self._existing_recids:
            self._records[recid] = record
            self._existing_recids.add(recid)

    def add_record_state(self, record_state, **kwargs):
        """Add record state."""
        self._record_states.append(record_state)

    def finalise(self):
        """Finalise logging files."""
        # Write records
        with open(self.RECORD_DUMP_FILEPATH, "w", encoding="utf-8") as f:
            f.write("{\n")
            items = list(self._records.items())
            for i, (recid, record) in enumerate(items):
                json_str = json.dumps(record, ensure_ascii=False, separators=(",", ":"))
                comma = "," if i < len(items) - 1 else ""
                f.write(f'"{recid}":{json_str}{comma}\n')
            f.write("}")

        # Write record states
        with open(self.RECORD_STATE_FILEPATH, "w", encoding="utf-8") as f:
            f.write("[\n")
            for i, state in enumerate(self._record_states):
                json_str = json.dumps(state, ensure_ascii=False, separators=(",", ":"))
                comma = "," if i < len(self._record_states) - 1 else ""
                f.write(f"{json_str}{comma}\n")
            f.write("]")
