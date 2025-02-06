# -*- coding: utf-8 -*-
#
# Copyright (C) 2025 CERN.
#
# CDS-Videos is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""CDS-Videos transform step module."""
import datetime
import logging
from pathlib import Path

import arrow
from invenio_rdm_migrator.streams.records.transform import (
    RDMRecordEntry,
    RDMRecordTransform,
)

from cds_migrator_kit.errors import (
    ManualImportRequired,
    MissingRequiredField,
    RestrictedFileDetected,
    UnexpectedValue,
)
from cds_migrator_kit.reports.log import RDMJsonLogger
from cds_migrator_kit.transform.dumper import CDSRecordDump
from cds_migrator_kit.transform.errors import LossyConversion
from cds_migrator_kit.videos.weblecture_migration.transform import (
    videos_migrator_marc21,
)

cli_logger = logging.getLogger("migrator")


class CDSToVideosRecordEntry(RDMRecordEntry):
    """Transform CDS record to CDS Videos record."""

    def __init__(
        self,
        partial=False,
        dry_run=False,
    ):
        """Constructor."""
        self.dry_run = dry_run
        super().__init__(partial)

    def _schema(self, entry):
        """Return JSONSchema of the record."""
        return

    def _created(self, json_entry):
        try:
            return arrow.get(json_entry["_created"])
        except KeyError:
            return datetime.date.today().isoformat()

    def _updated(self, record_dump):
        """Returns the creation date of the record."""
        return record_dump.data["record"][0]["modification_datetime"]

    def _access(self, entry, record_dump):
        return

    def _recid(self, record_dump):
        """Returns the recid of the record."""
        return str(record_dump.data["recid"])

    def _bucket_id(self, json_entry):
        return

    def _id(self, entry):
        return

    def _version_id(self, entry):
        return

    def _index(self, entry):
        return

    def _pids(self, entry):
        return

    def _bucket_id(self, json_entry):
        return

    def _media_bucket_id(self, entry):
        return

    def _files(self, record_dump):
        """Transform the files of a record."""
        raise NotImplementedError("_files are not implemented for this class.")

    def _media_files(self, entry):
        return

    def _metadata(self, entry):
        """Transform the metadata of a record."""

        def extract_dates(json_data, key, subkey=None):
            """Extracts date values from a given key in json_data."""
            items = json_data.get(key, [])
            if subkey:
                return {
                    item[subkey]["date"]
                    for item in items
                    if isinstance(item, dict)
                    and subkey in item
                    and isinstance(item[subkey], dict)
                    and "date" in item[subkey]
                }

            return {
                item["date"]
                for item in items
                if isinstance(item, dict) and "date" in item
            }

        def reformat_date(json_data):
            """Reformat the date for the cds-videos data model."""
            # 1. Check primary date field
            dates_set = {date for date in json_data.get("date", []) if date}

            # 2. If no date found, check `indico_links`
            if not dates_set:
                dates_set = extract_dates(json_data, "url_files", subkey="indico")

            # 3. If still no date found, check `internal_notes`
            if not dates_set:
                dates_set = extract_dates(json_data, "internal_notes")

            # 4. Return the valid date if only one is found
            if len(dates_set) == 1:
                return next(iter(dates_set))

            # 5. Multiple dates (Must have different indico event videos?)
            if len(dates_set) > 1:
                raise UnexpectedValue(
                    f"More than one date found in record: {json_data.get('recid')} dates: {dates_set}.",
                    stage="transform",
                )

            raise MissingRequiredField(
                f"No valid date found in record: {json_data.get('recid')}.",
                stage="transform",
            )

        def description(json_data):
            """Reformat the description for the cds-videos data model."""
            if not json_data.get("description"):
                return json_data.get("title").get("title")
            return json_data.get("description")

        def format_contributors(json_data):
            """
            Same contributors could be both in tag 700 and 906.

            TODO: Should we keep them both? https://cds.cern.ch/record/2233152/export/xm?ln=en
            Removes duplicate contributors based on name, role, and affiliations.
            """
            contributors = json_data.get("contributors")
            if not contributors:
                raise MissingRequiredField(
                    f"No valid contributor found in record: {json_data.get('recid')}.",
                    stage="transform",
                )

            unique_contributors = []
            seen = set()

            for contributor in contributors:
                # Create a tuple to identify contributors
                identifier = (
                    contributor["name"],
                    contributor.get("role"),
                    tuple(contributor.get("affiliations", [])),
                )
                if identifier not in seen:
                    seen.add(identifier)
                    unique_contributors.append(contributor)

            return unique_contributors

        metadata = {
            "title": entry["title"],
            "description": description(entry),
            "contributors": format_contributors(entry),
            "language": entry.get("language"),
            "date": reformat_date(entry),
        }
        # filter empty keys
        return {k: v for k, v in metadata.items() if v}

    def _custom_fields(self, entry):
        """Transform the custom fields of a record."""
        return

    def transform(self, entry):
        """Transform a record single entry."""
        record_dump = CDSRecordDump(data=entry, dojson_model=videos_migrator_marc21)
        migration_logger = RDMJsonLogger()

        record_dump.prepare_revisions()
        timestamp, json_data = record_dump.latest_revision

        migration_logger.add_record(json_data)
        record_json_output = {
            "created": self._created(json_data),
            "updated": self._updated(record_dump),
            "metadata": self._metadata(json_data),
        }

        return {
            "created": self._created(json_data),
            "updated": self._updated(record_dump),
            "recid": self._recid(record_dump),
            "json": record_json_output,
            # keep the original extracted entry for storing it
            "_original_dump": entry,
        }


class CDSToVideosRecordTransform(RDMRecordTransform):
    """CDSToVideosRecordTransform."""

    def __init__(
        self,
        workers=None,
        throw=True,
        files_dump_dir=None,
        dry_run=False,
    ):
        """Constructor."""
        self.files_dump_dir = Path(files_dump_dir).absolute().as_posix()
        self.dry_run = dry_run
        super().__init__(workers, throw)

    def _parent(self, entry, record):
        return

    def _record(self, entry):
        return CDSToVideosRecordEntry(
            dry_run=self.dry_run,
        ).transform(entry)

    def _draft(self, entry):
        return None

    def _transform(self, entry):
        """Transform a single entry."""
        # creates the output structure for load step
        migration_logger = RDMJsonLogger()

        try:
            record = self._record(entry)
            original_dump = record.pop("_original_dump", {})
            if record:
                return {
                    "record": record,
                    "_original_dump": original_dump,
                }
        except (
            LossyConversion,
            RestrictedFileDetected,
            UnexpectedValue,
            ManualImportRequired,
            MissingRequiredField,
        ) as e:
            migration_logger.add_log(e, record=entry)

    def run(self, entries):
        """Run transformation step."""
        return super().run(entries)
