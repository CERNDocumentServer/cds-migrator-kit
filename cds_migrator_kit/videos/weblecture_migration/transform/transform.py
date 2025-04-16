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
import os

import arrow
from invenio_rdm_migrator.streams.records.transform import (
    RDMRecordEntry,
    RDMRecordTransform,
)
from invenio_accounts.models import User
from sqlalchemy.exc import NoResultFound
from flask import current_app


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
from cds_migrator_kit.videos.weblecture_migration.transform.transform_files import (
    TransformFiles,
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
        """Returns the creation date of the record."""
        # Try to get '_created' tag: 916, if not found then 'lecture_created' tag: 961
        created_date = json_entry.get("_created") or json_entry.get("lecture_created")
        if created_date:
            return arrow.get(created_date)
        return datetime.date.today().isoformat()

    def _updated(self, record_dump):
        """Returns the modification date of the record."""
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

    def _owner(self, json_entry):
        email = json_entry.get("submitter")
        error_message = f"{email} not found - did you run user migration?"
        if not email:
            email = current_app.config["WEBLECTURES_MIGRATION_SYSTEM_USER"] 
            error_message = f"{email} not found - did you created system user?"     
        try:
            user = User.query.filter_by(email=email).one()
            return {"id": user.id, "email": email}
        except NoResultFound:
            raise UnexpectedValue(
                message=error_message,
                stage="transform",
                recid=json_entry["legacy_recid"],
                value=email,
                priority="critical",
            )

    def _media_files(self, entry):
        """Transform the media files (lecturemedia files) of a record."""
        # Check if record has one master folder, or more
        master_paths = [
            item["master_path"] for item in entry.get("files") if "master_path" in item
        ]
        if len(master_paths) == 1:
            transform_files = TransformFiles(
                recid=entry["legacy_recid"], entry_files=entry.get("files")
            )
            file_info_json = transform_files.transform()
            return file_info_json
        elif len(master_paths) > 1:
            # TODO group them to have different file transform to create multiple records
            return UnexpectedValue(
                message="Multiple master folders! Multiple records should be created.",
                stage="transform",
                recid=self.recid,
                value="master_path",
                priority="critical",
            )
        else:
            return UnexpectedValue(
                message="Master folder does not exists!",
                stage="transform",
                recid=self.recid,
                value="master_path",
                priority="critical",
            )

    def _metadata(self, entry):
        """Transform the metadata of a record."""

        def get_values_in_json(json_data, field):
            """Get the not none values in json as a set"""
            return {d for d in json_data.get(field, []) if d}
        
        def guess_dates(json_data, key, subkey=None):
            """Try to get `date` from other fields.

            ### Examples:
            1. **8564 tag may include digitized file information, indico information (link, date) or any url file
                json_data = {"url_files": [{"indico": {"url": "http://agenda.cern.ch/..", "date": "2002-03-18"}}], ...}
                Calling the method with `key="url_files", subkey="indico"`
                Returns all the possible:
                json_data["url_files"]["indico"]["date]

            2. **500__ tag: internal notes that may contain date information
                json_data = {"internal_notes": [{"note": "note, 1 Jun 2025", "date": "2025-06-01"}], ...}
                Calling the method with `key="internal_notes"
                Returns all the possible:
                json_data["internal_notes"]["date"]

            ### Returns:
            - `set[str]`: A set of date strings.
            """
            items = json_data.get(key, [])
            if subkey:
                return {
                    item[subkey]["date"]
                    for item in items
                    if subkey in item and "date" in item[subkey]
                }

            return {item["date"] for item in items if "date" in item}

        def reformat_date(json_data):
            """Reformat the date for the cds-videos data model."""

            # Priority: date > publication_date > guessed (indico, notes)
            dates_set = (
                get_values_in_json(json_data, "date") or
                get_values_in_json(json_data, "publication_date") or
                guess_dates(json_data, "url_files", subkey="indico") | 
                guess_dates(json_data, "internal_notes")
            )

            # Return the valid date if only one is found
            if len(dates_set) == 1:
                return next(iter(dates_set))

            # Multiple dates (Must have different indico event videos?)
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
            Format the contributors.

            - If there are contributors, don't use 906 (event speakers).
            - If there are no contributors, use 906 (event speakers).
            - If no valid contributors are found, use "Unknown, Unknown" and log it.
            """

            contributors = (
                json_data.get("contributors") or json_data.get("event_speakers") or None
            )

            if not contributors:
                # TODO do we need another logger?
                logger_migrator = logging.getLogger("users")
                logger_migrator.warning(
                    f"Missing contributors in record:{json_data['recid']}! Using:`Unknown`"
                )

                contributors = [{"name": "Unknown, Unknown"}]

            return contributors

        def publication_date(json_data):
            """Get the publication date"""
            dates = get_values_in_json(json_data, "publication_date")
            if len(dates) == 1:
                return next(iter(dates))
            
        record_date = reformat_date(entry)
        metadata = {
            "title": entry["title"],
            "description": description(entry),
            "contributors": format_contributors(entry),
            "language": entry.get("language"),
            "date": record_date,
            "publication_date": publication_date(entry) or record_date,
        }
        # filter empty keys
        return {k: v for k, v in metadata.items() if v}

    def _custom_fields(self, entry):
        """Transform the custom fields of a record."""
        return

    def transform(self, entry):
        """Transform a record single entry."""
        record_dump = CDSRecordDump(data=entry, dojson_model=videos_migrator_marc21)
        migration_logger = RDMJsonLogger(collection="weblectures")

        record_dump.prepare_revisions()
        timestamp, json_data = record_dump.latest_revision

        migration_logger.add_record(json_data)
        record_json_output = {
            "metadata": self._metadata(json_data),
            "created": self._created(json_data),
            "updated": self._updated(record_dump),
            "media_files": self._media_files(json_data),
        }

        return {
            "created": self._created(json_data),
            "updated": self._updated(record_dump),
            "recid": self._recid(record_dump),
            "json": record_json_output,
            "owned_by": self._owner(json_data),
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
        migration_logger = RDMJsonLogger(collection="weblectures")

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
