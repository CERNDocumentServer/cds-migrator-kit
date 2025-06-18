# -*- coding: utf-8 -*-
#
# Copyright (C) 2025 CERN.
#
# CDS-Videos is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""CDS-Videos transform step module."""
import datetime
import logging
import os
from pathlib import Path

import arrow
from flask import current_app
from invenio_accounts.models import User
from invenio_pidstore.models import PersistentIdentifier
from invenio_rdm_migrator.streams.records.transform import (
    RDMRecordEntry,
    RDMRecordTransform,
)
from sqlalchemy.exc import NoResultFound

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
        files_dump_dir=None,
    ):
        """Constructor."""
        self.dry_run = dry_run
        self.files_dump_dir = files_dump_dir
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
        record_dump.prepare_files()
        files = record_dump.files

        record_files = []

        for k in files.keys():
            # Get the latest version
            file = files[k][-1]
            full_path = file.get("full_path")
            path = full_path.replace(";1", "").replace(";2", "")
            path = path.replace("/opt/cdsweb/var/data/files", self.files_dump_dir)
            if not os.path.exists(path):
                raise ManualImportRequired(
                    message="AFS file not found!",
                    stage="transform",
                    value=path,
                    priority="critical",
                )
            record_files.append(path)
        
        return record_files

    def _owner(self, json_entry):
        """Get the weblectures-user as record owner."""
        email = current_app.config["WEBLECTURES_MIGRATION_SYSTEM_USER"]
        try:
            user = User.query.filter_by(email=email).one()
            return {"id": user.id, "email": email}
        except NoResultFound:
            raise UnexpectedValue(
                message=f"{email} not found - did you created system user?",
                stage="transform",
                recid=json_entry["legacy_recid"],
                value=email,
                priority="critical",
            )

    def check_pid_exists(self, recid, pid_type="lrecid"):
        """Check if we have minted `lrecid` pid."""
        pid = PersistentIdentifier.query.filter_by(
            pid_type=pid_type,
            pid_value=recid,
        ).one_or_none()
        return pid is not None

    def _media_files(self, entry):
        """Transform the media files (lecturemedia files) of a record."""
        # No need to check files if record is migrated (they'll be moved)
        if self.check_pid_exists(str(entry["legacy_recid"])):
            return {}

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
            raise UnexpectedValue(
                message="Multiple master folders! Multiple records should be created.",
                stage="transform",
                value="master_path",
                priority="critical",
            )
        else:
            raise UnexpectedValue(
                message="Master folder does not exists!",
                stage="transform",
                value="master_path",
                priority="critical",
            )

    def _metadata(self, entry):
        """Transform the metadata of a record."""

        def get_values_in_json(json_data, field, type=set):
            """Get the not none values in json."""
            if type == set:
                return {d for d in json_data.get(field, []) if d}
            if type == list:
                return [d for d in json_data.get(field, []) if d]

        def guess_dates(json_data, key, subkey=None):
            """Try to get `date` from other fields.

            ### Examples:
            1. **8564 tag may include digitized file information, indico information (link, date) or any url file
                json_data = {"url_files": [{"indico": {"url": "http://agenda.cern.ch/..", "date": "2002-03-18"}}], ...}
                Calling the method with `key="url_files", subkey="indico"`
                Returns all the possible:
                json_data["url_files"]["indico"]["date]

            2. **500__ tag: notes that may contain date information
                json_data = {"notes": [{"note": "note, 1 Jun 2025", "date": "2025-06-01"}], ...}
                Calling the method with `key="notes"
                Returns all the possible:
                json_data["notes"]["date"]

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
                get_values_in_json(json_data, "date")
                or get_values_in_json(json_data, "publication_date")
                or guess_dates(json_data, "url_files", subkey="indico")
                | guess_dates(json_data, "notes")
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
                get_values_in_json(json_data, "contributors", type=list)
                or get_values_in_json(json_data, "event_speakers", type=list)
                or None
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
            """Get the publication date."""
            dates = get_values_in_json(json_data, "publication_date")
            if len(dates) == 1:
                return next(iter(dates))

        def notes(json_data):
            """Get the notes."""
            notes = entry.get("notes")
            if notes:
                note_strings = [note.get("note") for note in notes]
                return "\n".join(note_strings)
            return None

        def accelerator_experiment(json_data):
            """Get the accelerator_experiment."""
            entries = json_data.get("accelerator_experiment", [])
            if len(entries) == 1:
                return entries[0]
            if not entries:
                return None
            raise UnexpectedValue(
                f"More than one accelerator_experiment field found in record: {json_data.get('recid')} values: {entries}.",
                stage="transform",
            )

        def location(json_data):
            """Get the location."""
            # Priority: indico_location (111) > lecture_location (518)
            indico_location = json_data.get("indico_information", {}).get(
                "location", ""
            )
            if indico_location:
                return indico_location
            lecture_infos = json_data.get("lecture_infos", [])
            locations = {
                item["location"] for item in lecture_infos if "location" in item
            }
            if len(locations) == 1:
                return next(iter(locations))
            elif len(locations) > 1:
                raise UnexpectedValue(
                    f"More than one location found in record: {json_data.get('recid')} values: {locations}.",
                    stage="transform",
                )

        def get_report_number(json_data):
            """Return the report number."""
            report_numbers = json_data.get("report_number", [])
            if len(report_numbers) > 1:
                raise UnexpectedValue(
                    "More than one report number found.",
                    stage="transform",
                )
            if len(report_numbers) == 1:
                # If report number exists put it in curation
                report_number = report_numbers[0]
                return report_numbers, self.check_pid_exists(
                    report_number, pid_type="rn"
                )
            return None, None

        def get_keywords(json_data):
            """Return keywords."""
            keywords = json_data.get("keywords", [])
            subject_categories = json_data.get("subject_categories", [])

            all_keywords = [
                keyword for keyword in keywords + subject_categories if keyword
            ]
            return all_keywords

        def get_related_identifiers(json_data):
            """Return related_identifiers."""
            related_identifiers = get_values_in_json(
                json_data, "related_identifiers", type=list
            )

            url_files = [
                item.get("indico") or item.get("url_file")
                for item in json_data.get("url_files", [])
                if "indico" in item or "url_file" in item
            ]
            # Get indico id's and raise if different
            event_ids = {
                item["event_id"] for item in url_files if item and "event_id" in item
            }
            # Get event id's from indico_information(111) and tag 970
            event_ids.update(
                filter(
                    None,
                    [
                        json_data.get("indico_information", {}).get("event_id"),
                        json_data.get("system_number"),
                    ],
                )
            )
            if len(event_ids) == 1:
                identifier = next(iter(event_ids))
                rel = {
                    "scheme": "Indico",
                    "identifier": identifier,
                    "relation_type": "IsPartOf",
                }
                if rel not in related_identifiers:
                    related_identifiers.append(rel)
            elif len(event_ids) > 1:
                raise UnexpectedValue(
                    f"Multiple Indico IDs found: {event_ids}",
                    stage="transform",
                )
            else:
                RDMJsonLogger(collection="weblectures").add_success_state(
                    json_data["recid"],
                    {"message": "Indico ID is missing!", "value": "indico"},
                )
            # Add the url's
            for item in url_files:
                if not item or "url" not in item:
                    continue

                rel = {
                    "scheme": "URL",
                    "identifier": item["url"],
                    "relation_type": "IsPartOf",
                }
                if rel not in related_identifiers:
                    related_identifiers.append(rel)

            return related_identifiers

        def get_curation(json_data):
            """Return _curation."""
            _curation = json_data.get("_curation", {})
            # Add volumes
            additional_titles = json_data.get("additional_titles", [])
            volumes = [item["volume"] for item in additional_titles if "volume" in item]
            if volumes:
                _curation["volumes"] = volumes

            return _curation

        def get_additional_titles(json_data):
            """Return additional_titles."""
            tag_246 = json_data.get("additional_titles", {})
            _titles = [item for item in tag_246 if "title" in item]
            additional_titles = []
            for title_item in _titles:
                additional_title = {
                    "title": title_item["title"],
                    "type": (
                        "TranslatedTitle"
                        if "lang" in title_item
                        else "AlternativeTitle"
                    ),
                }
                if "lang" in title_item:
                    additional_title["lang"] = title_item["lang"]
                additional_titles.append(additional_title)
            return additional_titles

        def get_additional_descriptions(json_data):
            """Return additional_descriptions."""
            return get_values_in_json(json_data, "additional_descriptions", type=list)

        def get_collections(json_data):
            """Return collection tags."""
            collections = get_values_in_json(json_data, "collections", type=list)
            if not collections:
                raise MissingRequiredField(message="Collection is missing!")
            return collections

        def get_access(json_data):
            """Generate access permissions based on restrictions."""
            # Update permissions
            update = [current_app.config["WEBLECTURES_MIGRATION_SYSTEM_USER"]]
            submitter = json_data.get("submitter")
            if submitter:
                # Add submitter to update list
                update.append(submitter)

            access = {"update": update}

            # Read permissions
            restrictions = json_data.get("restriction", [])
            all_emails = list({email for sublist in restrictions for email in sublist})
            if all_emails:
                access["read"] = all_emails
            return access

        record_date = reformat_date(entry)
        metadata = {
            "title": entry["title"],
            "description": description(entry),
            "contributors": format_contributors(entry),
            "language": entry.get("language"),
            "date": record_date,
            "publication_date": publication_date(entry) or record_date,
            "keywords": get_keywords(entry),
            "accelerator_experiment": accelerator_experiment(entry),
            "note": notes(entry),
            "location": location(entry),
            "legacy_recid": entry.get("legacy_recid"),
            "related_identifiers": get_related_identifiers(entry),
            "additional_titles": get_additional_titles(entry),
            "additional_descriptions": get_additional_descriptions(entry),
            "license": entry.get("license"),
            "copyright": entry.get("copyright"),
            "doi": entry.get("doi"),
            "alternate_identifiers": entry.get("alternate_identifiers"),
            "additional_languages": entry.get("additional_languages"),
            "collections": get_collections(entry),
            "_access": get_access(entry),
        }
        _curation = get_curation(entry)
        # If report number exists put it in curation
        # Report_number is a list with one value
        report_number, is_curation = get_report_number(entry)
        if report_number:
            if is_curation:
                _curation["legacy_report_number"] = report_number[0]
            else:
                metadata["report_number"] = report_number
        metadata["_curation"] = _curation

        # Add Restricted General Talks to collections
        collections = metadata.get("collections", [])
        collection_mapping = current_app.config["COLLECTION_MAPPING"]
        if metadata["_access"].get("read") and (
            (
                collection_mapping["TALK"] in collections
                and collection_mapping["Restricted_ATLAS_Talks"] not in collections
                and collection_mapping["Restricted_CMS_Talks"] not in collections
            )
            or ("Lectures,Video Lectures" in collections)
        ):
            collections.append("Lectures,Restricted General Talks")
            if "Lectures" not in collections:
                collections.append("Lectures")
            metadata["collections"] = collections

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
            "files": self._files(record_dump),
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
            files_dump_dir=self.files_dump_dir,
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
