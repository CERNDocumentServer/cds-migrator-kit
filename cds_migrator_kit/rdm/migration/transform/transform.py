# -*- coding: utf-8 -*-
#
# Copyright (C) 2022 CERN.
#
# CDS-RDM is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""CDS-RDM transform step module."""
import csv
import datetime
import json
import logging
import os.path
from collections import OrderedDict
from copy import deepcopy
from pathlib import Path

import arrow
from invenio_rdm_migrator.streams.records.transform import (
    RDMRecordEntry,
    RDMRecordTransform,
)
from opensearchpy import RequestError
from sqlalchemy.exc import NoResultFound

from cds_migrator_kit.rdm.migration.transform.users import CDSMissingUserLoad
from cds_migrator_kit.rdm.migration.transform.xml_processing.dumper import CDSRecordDump
from cds_migrator_kit.rdm.migration.transform.xml_processing.errors import (
    LossyConversion, RestrictedFileDetected, UnexpectedValue, ManualImportRequired,
)
from cds_migrator_kit.records.log import RDMJsonLogger
from invenio_access.permissions import system_identity
from invenio_search.engine import dsl
from invenio_records_resources.proxies import current_service_registry
from invenio_accounts.models import User

cli_logger = logging.getLogger("migrator")


class CDSToRDMRecordEntry(RDMRecordEntry):
    """Transform Zenodo record to RDM record."""

    def __init__(self, partial=False, missing_users_dir=None,
                 missing_users_filename="people.csv", dry_run=False):
        self.missing_users_dir = missing_users_dir
        self.missing_users_filename = missing_users_filename
        self.dry_run = dry_run
        super().__init__(partial)

    def _created(self, json_entry):
        try:
            return arrow.get(json_entry["_created"])
        except KeyError:
            return datetime.date.today().isoformat()

    def _updated(self, record_dump):
        """Returns the creation date of the record."""
        return record_dump.data["record"][0]["modification_datetime"]

    def _version_id(self, entry):
        """Returns the version id of the record."""
        return 1

    def _access(self, entry, record_dump):
        is_file_public = True

        for key, value in record_dump.files.items():
            if value[0]["hidden"]:
                is_file_public = False
        return {
            "record": "public",
            "files": "public" if is_file_public else "restricted",
        }

    def _index(self, record_dump):
        """Returns the version index of the record."""
        return 1  # in legacy we start at 0

    def _recid(self, record_dump):
        """Returns the recid of the record."""
        return str(record_dump.data["recid"])

    def _pids(self, json_entry):
        return {}

    def _bucket_id(self, json_entry):
        return

    def _custom_fields(self, json_entry):
        return {}

    def _id(self, entry):
        return

    def _media_bucket_id(self, entry):
        return

    def _media_files(self, entry):
        return {}

    def _pids(self, json_entry):
        return []

    def _files(self, record_dump):
        """Transform the files of a record."""
        record_dump.prepare_files()
        files = record_dump.files
        return {"enabled": True if files else False}

    def _communities(self, json_entry):
        return json_entry.get("communities", [])

    def _owner(self, json_entry):
        email = json_entry["submitter"]
        try:
            user = User.query.filter_by(email=email).one()
            return user.id
        except NoResultFound:
            if not self.dry_run:
                user_id = self._create_owner(email)
                return user_id
            return "-1"

    def _create_owner(self, email):
        def get_person(email):
            missing_users_dump = os.path.join(self.missing_users_dir,
                                              self.missing_users_filename)
            with open(missing_users_dump) as csv_file:
                for row in csv.reader(csv_file):
                    if email == row[0]:
                        return row

        def get_person_old_db(email):
            missing_users_dump = os.path.join(self.missing_users_dir,
                                              "missing_users.json")
            with open(missing_users_dump) as json_file:
                missing = json.load(json_file)
            person = next((item for item in missing if item["email"] == email), None)

            return person

        user_api = CDSMissingUserLoad()
        person = get_person(email)
        person_old_db = get_person_old_db(email)

        person_id = None
        displayname = None
        username = None
        extra_data = {"migration": {}}

        if person:
            # person id might be missing from people collection
            person_id = person[1] if person[1] else None
            displayname = f"{person[2]} {person[3]}"
            username = f"{person[2][0]}{person[3]}".lower().replace(" ", "")
            if len(person) == 5:
                extra_data["department"] = person[4]
            extra_data["migration"]["source"] = (
                f"PEOPLE COLLECTION, "
                f"{'PERSON_ID FOUND' if person_id else 'PERSON_ID NOT FOUND'}"
            )
        elif person_old_db:
            names = "".join(person_old_db["displayname"].split())
            username = names.lower().replace(".", "")
            if not username:
                username = f'MIGRATED{email.replace("@", "").replace(".", "")}'
            displayname = person_old_db["displayname"]
            extra_data["migration"]["source"] = "LEGACY DB, PERSON ID MISSING"
        else:
            username = email.replace("@", "").replace(".", "")
            extra_data["migration"]["source"] = "RECORD, EMAIL NOT FOUND IN ANY SOURCE"
        extra_data["migration"]["note"] = "MIGRATED INACTIVE ACCOUNT"

        user = user_api.create_user(
            email,
            name=displayname,
            username=username,
            person_id=person_id)
        return user.id

    def _metadata(self, json_entry):
        def creators(json):
            _creators = deepcopy(json.get("creators", []))
            vocab_type = "affiliations"
            service = current_service_registry.get(vocab_type)
            extra_filter = dsl.Q("term", type__id=vocab_type)
            for creator in _creators:
                affiliations = creator.get("affiliations", [])
                transformed_aff = []
                for affiliation_name in affiliations:

                    title = dsl.Q("match", **{f"title": affiliation_name})
                    acronym = dsl.Q("match_phrase",
                                    **{f"acronym.keyword": affiliation_name})
                    title_filter = dsl.query.Bool("should", should=[title, acronym])

                    vocabulary_result = (service.search(system_identity,
                                                        extra_filter=title_filter | extra_filter)
                                         .to_dict())
                    if vocabulary_result["hits"]["total"]:
                        transformed_aff.append({
                            "name": affiliation_name,
                            "id": vocabulary_result["hits"]["hits"][0]["id"]}
                        )
                    else:
                        raise UnexpectedValue(subfield="u",
                                              value=affiliation_name,
                                              field="author",
                                              message=f"Affiliation {affiliation_name} not found.")
                creator["affiliations"] = transformed_aff
            return _creators

        def _resource_type(data):
            t = "publication-technicalnote"
            st = None
            return {"id": f"{t}-{st}"} if st else {"id": t}

        return {
            "creators": creators(json_entry),
            "title": json_entry["title"],
            "resource_type": _resource_type(json_entry),
            "description": json_entry.get("description"),
            "publication_date": json_entry.get("publication_date"),
        }

    def _custom_fields(self, json_entry):

        experiment = json_entry.get("custom_fields", {}).get("cern:experiment")
        custom_fields = {}

        if experiment:
            vocab_type = "experiments"
            service = current_service_registry.get("vocabularies")
            try:
                vocabulary_result = (
                    service.search(system_identity, type=vocab_type,
                                   q=f"{experiment}")
                    .to_dict())
            except RequestError:
                raise UnexpectedValue(subfield="a",
                                      value=experiment,
                                      field="experiment",
                                      message=f"Experiment {experiment} "
                                              f"not valid search phrase.")
            if vocabulary_result["hits"]["total"]:

                custom_fields["cern:experiment"] = {
                    "id": vocabulary_result["hits"]["hits"][0]["id"]
                }

            else:
                raise UnexpectedValue(subfield="a",
                                      value=experiment,
                                      field="experiment",
                                      message=f"Experiment {experiment} not found.")
            return custom_fields

    def transform(self, entry):
        """Transform a record single entry."""
        record_dump = CDSRecordDump(
            entry,
        )
        migration_logger = RDMJsonLogger()
        migration_logger.add_recid_to_stats(entry["recid"])
        try:

            record_dump.prepare_revisions()
            timestamp, json_data = record_dump.revisions[-1]
            migration_logger.add_record(json_data)
            json_output = {
                "created": self._created(json_data),
                "updated": self._updated(record_dump),
                "pids": self._pids(json_data),
                "files": self._files(record_dump),
                "metadata": self._metadata(json_data),
                "access": self._access(json_data, record_dump),
            }
            custom_fields = self._custom_fields(json_data)
            if custom_fields:
                json_output.update({"custom_fields": custom_fields})
            return {
                "created": self._created(json_data),
                "updated": self._updated(record_dump),
                "version_id": self._version_id(record_dump),
                "index": self._index(record_dump),
                "recid": self._recid(record_dump),
                "communities": self._communities(json_data),
                "json": json_output,
                "owned_by": self._owner(json_data)
            }
        except LossyConversion as e:
            cli_logger.error("[DATA ERROR]: {0}".format(e.message))
            migration_logger.add_log(e, output=entry)
        except Exception as e:
            migration_logger.add_log(e, output=entry)
            raise e
        # TODO take only the last


class CDSToRDMRecordTransform(RDMRecordTransform):
    """CDSToRDMRecordTransform."""

    def __init__(self, workers=None, throw=True, files_dump_dir=None,
                 missing_users=None, community_slug=None, dry_run=False):
        """Constructor."""
        self.files_dump_dir = Path(files_dump_dir).absolute().as_posix()
        self.missing_users_dir = Path(missing_users).absolute().as_posix()
        self.community_slug = community_slug
        self.dry_run = dry_run
        super().__init__(workers, throw)

    def _community_id(self, entry, record):
        communities = record.get("communities", [])
        communities = [self.community_slug] + [slug for slug in communities]
        if communities:
            return {"ids": communities,
                    "default": self.community_slug
                    }
        return {}

    def _parent(self, entry, record):
        parent = {
            "created": record["created"],  # same as the record
            "updated": record["updated"],  # same as the record
            "version_id": record["version_id"],
            "json": {
                # loader is responsible for creating/updating if the PID exists.
                "id": f'{record["recid"]}-parent',
                "access": {
                    "owned_by": {"user": str(record["owned_by"])},
                },
                "communities": self._community_id(entry, record),
            },
        }

        return parent

    def _transform(self, entry):
        """Transform a single entry."""
        # the functions receive the full record/data entry
        # while in most cases the full view is not needed
        # since this is a low level tool used only by users
        # with deep system knowledge providing the flexibility
        # is future proofing and simplifying the interface
        migration_logger = RDMJsonLogger()
        try:
            record = self._record(entry)
            if record:
                return {
                    "record": record,
                    "draft": self._draft(entry),
                    "parent": self._parent(entry, record),
                    "record_files": self._record_files(entry, record),
                    "draft_files": self._draft_files(entry),
                }
        except Exception as e:
            migration_logger.add_log(e, output=entry)

    def _record(self, entry):
        # could be in draft as well, depends on how we decide to publish
        return CDSToRDMRecordEntry(missing_users_dir=self.missing_users_dir,
                                   dry_run=self.dry_run).transform(
            entry)

    def _draft(self, entry):
        return None

    def _draft_files(self, entry):
        """Point to temporary eos storage to import files from."""
        _files = entry["files"]
        draft_files = OrderedDict()
        legacy_path_root = Path("/opt/cdsweb/var/data/files/")
        tmp_eos_root = Path(self.files_dump_dir)
        for file in _files:
            full_path = Path(file["full_path"])

            if file["version"] not in draft_files:
                draft_files[file["version"]] = {}

            # TODO other access types to be dealt later, for now we make sure
            # TODO that no restricted file goes through
            if file["status"]:
                raise RestrictedFileDetected(value=file["full_name"])
            # group files by version
            # {"1": {"filename": {...}}
            draft_files[file["version"]].update(
                {
                    file["full_name"]: {
                        "eos_tmp_path": tmp_eos_root
                                        / full_path.relative_to(legacy_path_root),
                        "key": file["full_name"],
                        "metadata": {},
                        "mimetype": file["mime"],
                        "checksum": file["checksum"],
                        "version": file["version"],
                        "access": file["status"],
                        "type": file["type"],
                        "creation_date": arrow.get(file["creation_date"])
                        .date()
                        .isoformat(),
                    }
                }
            )
        versioned_files = {}

        # creates a collection of files per each version
        for version in draft_files.keys():
            versioned_files |= draft_files.get(version)
            draft_files[version] = versioned_files

        return draft_files

    def _record_files(self, entry, record):
        """Record files entries transform."""
        # TO implement if we decide not to go via draft publish
        return []

    def run(self, entries):
        return super().run(entries)

    #
    #
    # "files": [
    #   {
    #     "comment": null,
    #     "status": "firerole: allow group \"council-full [CERN]\"\ndeny until \"1996-02-01\"\nallow all",
    #     "version": 1,
    #     "encoding": null,
    #     "creation_date": "2009-11-03T12:29:06+00:00",
    #     "bibdocid": 502379,
    #     "mime": "application/pdf",
    #     "full_name": "CM-P00080632-e.pdf",
    #     "superformat": ".pdf",
    #     "recids_doctype": [[32097, "Main", "CM-P00080632-e.pdf"]],
    #     "path": "/opt/cdsweb/var/data/files/g50/502379/CM-P00080632-e.pdf;1",
    #     "size": 5033532,
    #     "license": {},
    #     "modification_date": "2009-11-03T12:29:06+00:00",
    #     "copyright": {},
    #     "url": "http://cds.cern.ch/record/32097/files/CM-P00080632-e.pdf",
    #     "checksum": "ed797ce5d024dcff0040db79c3396da9",
    #     "description": "English",
    #     "format": ".pdf",
    #     "name": "CM-P00080632-e",
    #     "subformat": "",
    #     "etag": "\"502379.pdf1\"",
    #     "recid": 32097,
    #     "flags": [],
    #     "hidden": false,
    #     "type": "Main",
    #     "full_path": "/opt/cdsweb/var/data/files/g50/502379/CM-P00080632-e.pdf;1"
    #   },]
