# -*- coding: utf-8 -*-
#
# Copyright (C) 2022 CERN.
#
# CDS-RDM is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""CDS-RDM transform step module."""
import datetime
import logging
from collections import OrderedDict
from copy import deepcopy
from pathlib import Path

import arrow
from cds_rdm.legacy.models import CDSMigrationAffiliationMapping
from idutils import normalize_ror
from idutils.validators import is_doi, is_ror
from invenio_access.permissions import system_identity
from invenio_accounts.models import User, UserIdentity
from invenio_db import db
from invenio_rdm_migrator.streams.records.transform import (
    RDMRecordEntry,
    RDMRecordTransform,
)
from invenio_records_resources.proxies import current_service_registry
from invenio_vocabularies.contrib.names.models import NamesMetadata
from opensearchpy import RequestError
from sqlalchemy.exc import NoResultFound

from cds_migrator_kit.errors import (
    ManualImportRequired,
    MissingRequiredField,
    RecordFlaggedCuration,
    RestrictedFileDetected,
    UnexpectedValue,
)
from cds_migrator_kit.rdm.migration_config import (
    RDM_RECORDS_IDENTIFIERS_SCHEMES,
    VOCABULARIES_NAMES_SCHEMES,
)
from cds_migrator_kit.rdm.records.transform.config import (
    FILE_SUBFORMATS_TO_DROP,
    IDENTIFIERS_SCHEMES_TO_DROP,
    IDENTIFIERS_VALUES_TO_DROP,
    PIDS_SCHEMES_ALLOWED,
    PIDS_SCHEMES_TO_DROP,
)
from cds_migrator_kit.reports.log import RDMJsonLogger
from cds_migrator_kit.transform.dumper import CDSRecordDump
from cds_migrator_kit.transform.errors import LossyConversion

cli_logger = logging.getLogger("migrator")


def search_vocabulary(term, vocab_type):
    """Search vocabulary utility function."""
    service = current_service_registry.get("vocabularies")
    if "/" in term:
        # escape the slashes
        term = f'"{term}"'
    try:
        vocabulary_result = service.search(
            system_identity, type=vocab_type, q=f"{term}"
        ).to_dict()
        return vocabulary_result
    except RequestError:
        raise UnexpectedValue(
            subfield="a",
            value=term,
            field=vocab_type,
            message=f"Vocabulary {vocab_type} term {term} not valid search phrase.",
            stage="vocabulary match",
        )


class CDSToRDMRecordEntry(RDMRecordEntry):
    """Transform CDS record to RDM record."""

    def __init__(
        self,
        partial=False,
        missing_users_dir=None,
        missing_users_filename="people.csv",
        affiliations_mapping=None,
        dry_run=False,
    ):
        """Constructor."""
        self.missing_users_dir = missing_users_dir
        self.missing_users_filename = missing_users_filename
        self.affiliations_mapping = affiliations_mapping
        self.dry_run = dry_run
        super().__init__(partial)

    def _created(self, json_entry):
        try:
            return arrow.get(json_entry["_created"])
        except KeyError:
            return arrow.get(datetime.date.today().isoformat()).replace(tzinfo=None)

    def _updated(self, record_dump):
        """Returns the creation date of the record."""
        return record_dump.data["record"][0]["modification_datetime"]

    def _version_id(self, entry):
        """Returns the version id of the record."""
        return 1

    def _access(self, entry, record_dump):
        restrictions = entry.get("record_restriction")

        if not restrictions:
            raise RecordFlaggedCuration(
                message="record restriction not found make sure the record should be public",
                stage="transform",
                field="record_restriction",
            )
        return restrictions

    def _index(self, record_dump):
        """Returns the version index of the record."""
        return 1  # in legacy we start at 0

    def _recid(self, record_dump):
        """Returns the recid of the record."""
        return str(record_dump.data["recid"])

    def _bucket_id(self, json_entry):
        return

    def _id(self, entry):
        return

    def _media_bucket_id(self, entry):
        return

    def _media_files(self, entry):
        return {}

    def _pids(self, json_entry):
        from flask import current_app

        DATACITE_PREFIX = current_app.config["DATACITE_PREFIX"]

        pids = json_entry.get("_pids", {})
        output_pids = deepcopy(pids)
        for key, identifier in pids.items():
            # ignoring some pids
            if key.upper() in PIDS_SCHEMES_TO_DROP:
                del output_pids[key]

            elif key and key.upper() not in PIDS_SCHEMES_ALLOWED:
                raise UnexpectedValue(
                    field=key,
                    subfield="2",
                    message="Unexpected PID scheme (should be DOI)",
                    priority="warning",
                    stage="transform",
                    value=identifier,
                )
            elif not key and is_doi(identifier):
                # assume it is DOI
                key = "DOI"
            if key.upper() == "DOI":
                doi_identifier = deepcopy(identifier)
                if identifier["identifier"].startswith(DATACITE_PREFIX):
                    if not json_entry.get("publisher"):
                        json_entry["publisher"] = "CERN"
                    doi_identifier["provider"] = "datacite"
                else:
                    doi_identifier["provider"] = "external"
                output_pids["doi"] = doi_identifier
        if output_pids:
            return output_pids
        else:
            return {}

    def _files(self, record_dump):
        """Transform the files of a record."""
        record_dump.prepare_files()
        files = record_dump.files
        return {"enabled": True if files else False}

    def _communities(self, json_entry):
        return json_entry.get("communities", [])

    def _owner(self, json_entry):
        email = json_entry.get("submitter")
        if not email:
            return "system"
        try:
            user = User.query.filter_by(email=email).one()
            return user.id
        except NoResultFound:
            return UnexpectedValue(
                message=f"{email} not found - did you run user migration?",
                stage="transform",
                recid=json_entry["legacy_recid"],
                value=email,
                priority="critical",
            )

    def _match_affiliation(self, affiliation_name):
        """Match an affiliation against `CDSMigrationAffiliationMapping` db table."""
        if is_ror(affiliation_name):
            return {"id": normalize_ror(affiliation_name)}
        # Step 1: search in the affiliation mapping (ROR organizations)
        match = self.affiliations_mapping.query.filter_by(
            legacy_affiliation_input=affiliation_name
        ).one_or_none()
        if match:
            # Step 1: check if there is a curated input
            if match.curated_affiliation:
                return match.curated_affiliation
            # Step 2: check if there is an exact match
            elif match.ror_exact_match:
                return {"id": normalize_ror(match.ror_exact_match)}
            # Step 3: check if there is not exact match
            elif match.ror_not_exact_match:
                _affiliation_ror_id = normalize_ror(match.ror_not_exact_match)
                raise RecordFlaggedCuration(
                    subfield="u",
                    value={"id": _affiliation_ror_id},
                    field="author",
                    message=f"Affiliation {_affiliation_ror_id} not found as an exact match, ROR id should be checked.",
                    stage="vocabulary match",
                )
            else:
                # Step 4: set the originally inserted value from legacy
                raise RecordFlaggedCuration(
                    subfield="u",
                    value={"name": affiliation_name},
                    field="author",
                    message=f"Affiliation {affiliation_name} not found as an exact match, custom value should be checked.",
                    stage="vocabulary match",
                )
        else:
            # Step 4: set the originally inserted value from legacy
            raise RecordFlaggedCuration(
                subfield="u",
                value={"name": affiliation_name},
                field="author",
                message=f"Affiliation {affiliation_name} not found as an exact match, custom value should be checked.",
                stage="vocabulary match",
            )

    def _metadata(self, json_entry, record_dump):

        def creator_affiliations(creator):
            affiliations = creator.get("affiliations", [])
            transformed_aff = []

            for affiliation_name in affiliations:
                try:
                    affiliation = self._match_affiliation(affiliation_name)
                    transformed_aff.append(affiliation)
                except RecordFlaggedCuration as exc:
                    # Save not exact match affiliation and reraise to flag the record
                    RDMJsonLogger().add_success_state(
                        json_entry["recid"],
                        {"message": exc.message, "value": exc.value},
                    )
                    transformed_aff.append(exc.value)
            creator["affiliations"] = transformed_aff

        def creator_identifiers(creator):
            processed_identifiers = []
            inner_dict = creator.get("person_or_org", {})
            identifiers = inner_dict.get("identifiers", [])
            for identifier in identifiers:
                # we check for unknown schemes
                if identifier["scheme"] in VOCABULARIES_NAMES_SCHEMES.keys():
                    processed_identifiers.append(identifier)
            if processed_identifiers:
                inner_dict["identifiers"] = processed_identifiers
            else:
                inner_dict.pop("identifiers", None)

        def lookup_person_id(creator):
            migrated_identifiers = deepcopy(
                creator.get("person_or_org", {}).get("identifiers", [])
            )
            name = None
            # lookup person_id
            person_id = next(
                (
                    identifier
                    for identifier in migrated_identifiers
                    if identifier["scheme"] == "cern"
                ),
                {},
            ).get("identifier")
            if person_id:
                ui = UserIdentity.query.filter_by(id=person_id).one_or_none()
                if ui:
                    user_id = ui.user.id
                    names = NamesMetadata.query.filter_by(
                        internal_id=str(user_id)
                    ).all()
                    name = next(
                        (
                            name
                            for name in names
                            if "unlisted" not in name.json.get("tags", [])
                        ),
                        None,
                    )
            # filter out cern person_id
            creator["person_or_org"]["identifiers"] = [
                identifier
                for identifier in migrated_identifiers
                if identifier["scheme"] != "cern"
            ]
            if name:
                # update identifiers of the authors to the latest known
                ids = creator["person_or_org"]["identifiers"]
                # check ids supplied by the names vocabulary and add missing
                for identifier in name.json.get("identifiers", []):
                    if identifier not in ids and identifier.get("scheme") != "cern":
                        ids.append(identifier)

                # copy names identifiers and json to assign explicitly json object
                # due to how postgres assignment of json is handled
                json_copy = deepcopy(name.json)
                existing_ids = deepcopy(name.json.get("identifiers", []))
                # update the names vocab to contain other ids found during migration
                for identifier in ids:
                    if identifier not in existing_ids:
                        existing_ids.append(identifier)

                if existing_ids:
                    # assign json explicitly to names entry
                    json_copy["identifiers"] = existing_ids
                    name.json = json_copy

                    db.session.add(name)
                    db.session.commit()

        def creators(json, key="creators"):
            _creators = deepcopy(json.get(key, []))
            _creators = list(filter(lambda x: x is not None, _creators))
            for creator in _creators:
                creator_affiliations(creator)
                lookup_person_id(creator)
                creator_identifiers(creator)
            return _creators

        def _resource_type(entry):
            return entry["resource_type"]

        def _publication_date(entry, dump_record):
            pub_date = entry.get("publication_date")
            created = entry.get("_created")
            files = dump_record["files"]
            if not (pub_date or created or files):
                raise MissingRequiredField(
                    message="missing creation or publication date", field="916"
                )
            if not pub_date:
                if created:
                    pub_date = entry["_created"]
                elif files:
                    pub_date = files[0]["creation_date"]
            return arrow.get(pub_date).date().isoformat()

        def _identifiers(json_entry):
            identifiers = json_entry.get("identifiers", [])
            for item in reversed(identifiers):
                # drop unwanted schemes
                if item is None or "scheme" not in item:
                    raise UnexpectedValue(
                        field="identifiers",
                        value=item,
                        subfield="9",
                        message="IDENTIFIER SCHEME INVALID",
                        priority="warning",
                        stage="transform",
                    )
                if (
                    item["scheme"].upper() in IDENTIFIERS_SCHEMES_TO_DROP
                    or IDENTIFIERS_VALUES_TO_DROP in item["identifier"]
                ):
                    identifiers.remove(item)
                    continue
                if item["scheme"] not in RDM_RECORDS_IDENTIFIERS_SCHEMES.keys():
                    raise UnexpectedValue(
                        field="identifiers",
                        subfield="9",
                        message="IDENTIFIER SCHEME INVALID",
                        priority="warning",
                        stage="transform",
                        value=item,
                    )
            return identifiers

        metadata = {
            "creators": creators(json_entry),
            "title": json_entry.get("title"),
            "resource_type": _resource_type(json_entry),
            "description": json_entry.get("description"),
            "publication_date": _publication_date(json_entry, record_dump),
            "contributors": creators(json_entry, key="contributors"),
            "subjects": json_entry.get("subjects"),
            "publisher": json_entry.get("publisher"),
            "additional_descriptions": json_entry.get("additional_descriptions"),
            "additional_titles": json_entry.get("additional_titles"),
            "identifiers": _identifiers(json_entry),
            "languages": json_entry.get("languages"),
            "dates": json_entry.get("dates"),
            "funding": json_entry.get("funding"),
            "related_identifiers": json_entry.get("related_identifiers"),
            "rights": json_entry.get("rights"),
            "copyright": json_entry.get("copyright"),
        }

        keys = deepcopy(list(json_entry.keys()))

        helper_keys = [
            "recid",
            "legacy_recid",
            "agency_code",
            "submitter",
            "_created",
            "record_restriction",
            "custom_fields",
            "_pids",
            "internal_notes",
        ]
        for item in helper_keys:
            if item in keys:
                keys.remove(item)

        forgotten_keys = [key for key in keys if key not in list(metadata.keys())]
        if forgotten_keys:
            raise ManualImportRequired("Unassigned metadata key", value=forgotten_keys)
        # filter empty keys
        return {k: v for k, v in metadata.items() if v}

    def _custom_fields(self, json_entry, json_output):

        def field_experiments(record_json, custom_fields_dict):
            experiments = record_json.get("custom_fields", {}).get(
                "cern:experiments", []
            )
            for experiment in experiments:
                if experiment.lower().strip() == "not applicable":
                    continue
                result = search_vocabulary(experiment, "experiments")

                if result["hits"]["total"]:
                    custom_fields_dict["cern:experiments"].append(
                        {"id": result["hits"]["hits"][0]["id"]}
                    )
                else:
                    subj = json_output["metadata"].get("subjects", [])
                    subj.append({"subject": experiment})
                    json_output["metadata"]["subjects"] = subj
                    raise RecordFlaggedCuration(
                        subfield="u",
                        value=experiment,
                        field="author",
                        message=f"Experiment {experiment} not found, added as a subject",
                        stage="vocabulary match",
                    )

        def field_programmes(record_json):
            programme = record_json.get("custom_fields", {}).get("cern:programmes")
            if programme:
                result = search_vocabulary(programme, "programmes")

                if result["hits"]["total"]:
                    return {"id": result["hits"]["hits"][0]["id"]}
                else:
                    raise UnexpectedValue(
                        value=programme,
                        field="programme",
                        message=f"programme {programme} not found",
                        stage="vocabulary match",
                    )
            else:
                if record_json["resource_type"] == "publication-thesis":

                    return {"id": "None"}
                else:
                    return

        def field_departments(record_json, custom_fields_dict):
            departments = record_json.get("custom_fields", {}).get(
                "cern:departments", []
            )
            for department in departments:
                result = search_vocabulary(department, "departments")
                if result["hits"]["total"]:
                    custom_fields_dict["cern:departments"].append(
                        {"id": result["hits"]["hits"][0]["id"]}
                    )
                else:
                    subj = json_output["metadata"].get("subjects", [])
                    subj.append({"subject": department})
                    json_output["metadata"]["subjects"] = subj
                    raise RecordFlaggedCuration(
                        subfield="a",
                        value=department,
                        field="department",
                        message=f"Department {department} not found. added as subject",
                        stage="vocabulary match",
                    )

        def field_accelerators(record_json, custom_fields_dict):
            accelerators = record_json.get("custom_fields", {}).get(
                "cern:accelerators", []
            )
            for accelerator in accelerators:
                if accelerator.lower().strip() == "not applicable":
                    continue
                result = search_vocabulary(accelerator, "accelerators")
                if result["hits"]["total"]:

                    custom_fields_dict["cern:accelerators"].append(
                        {"id": result["hits"]["hits"][0]["id"]}
                    )

                else:
                    raise UnexpectedValue(
                        subfield="a",
                        value=accelerator,
                        field="accelerators",
                        message=f"Accelerator {accelerator} not found.",
                        stage="vocabulary match",
                    )

        def field_beams(record_json, custom_fields_dict):
            beams = record_json.get("custom_fields", {}).get("cern:beams", [])
            for beam in beams:
                if beam.lower().strip() == "not applicable":
                    continue
                result = search_vocabulary(beam, "beams")
                if result["hits"]["total"]:
                    custom_fields_dict["cern:beams"].append(
                        {"id": result["hits"]["hits"][0]["id"]}
                    )

                else:
                    raise UnexpectedValue(
                        subfield="a",
                        value=beam,
                        field="beams",
                        message=f"Beam {beam} not found.",
                        stage="vocabulary match",
                    )

        custom_fields = {
            "cern:experiments": [],
            "cern:departments": [],
            "cern:accelerators": [],
            "cern:projects": json_entry.get("custom_fields", {}).get(
                "cern:projects", []
            ),
            "cern:facilities": json_entry.get("custom_fields", {}).get(
                "cern:facilities", []
            ),
            "cern:studies": json_entry.get("custom_fields", {}).get("cern:studies", []),
            "cern:beams": [],
            "cern:programmes": field_programmes(json_entry),
            "thesis:thesis": json_entry.get("custom_fields", {}).get(
                "thesis:thesis", {}
            ),
            "journal:journal": json_entry.get("custom_fields", {}).get(
                "journal:journal", {}
            ),
            "imprint:imprint": json_entry.get("custom_fields", {}).get(
                "imprint:imprint", {}
            ),
        }
        try:
            field_experiments(json_entry, custom_fields)
            field_departments(json_entry, custom_fields)

        except RecordFlaggedCuration as exc:
            RDMJsonLogger().add_success_state(
                json_entry["recid"],
                {"message": exc.message, "value": exc.value},
            )
        field_accelerators(json_entry, custom_fields)
        field_beams(json_entry, custom_fields)

        if custom_fields["cern:programmes"] is None:
            del custom_fields["cern:programmes"]

        forgotten_keys = [
            key
            for key in json_entry["custom_fields"].keys()
            if key not in custom_fields.keys()
        ]
        if forgotten_keys:
            raise ManualImportRequired(
                "Unassigned custom field key", value=forgotten_keys
            )
        return custom_fields

    def _verify_creation_date(self, entry, json_data):
        """Verify creation date.

        If the record has no files (file creation date will be used as record
        creation date) and no creation date, raise an exception.
        """
        if not entry.get("files") and not (
            json_data.get("_created") or json_data.get("publication_date")
        ):
            raise ManualImportRequired(
                message="Record missing creation date",
                field="validation",
                stage="transform",
                description="Record has no files and no creation date",
                recid=entry["recid"],
                priority="warning",
                value=None,
                subfield=None,
            )

    def transform(self, entry):
        """Transform a record single entry."""
        record_dump = CDSRecordDump(
            entry,
        )

        migration_logger = RDMJsonLogger()
        record_dump.prepare_revisions()
        timestamp, json_data = record_dump.latest_revision
        self._verify_creation_date(entry, json_data)
        migration_logger.add_record(json_data)

        clc_sync = deepcopy(json_data.get("_clc_sync", False))
        if "_clc_sync" in json_data:
            del json_data["_clc_sync"]

        record_json_output = {
            "created": self._created(json_data),
            "updated": self._updated(record_dump),
            "files": self._files(record_dump),
            "pids": self._pids(json_data),
            "metadata": self._metadata(json_data, entry),
        }
        custom_fields = self._custom_fields(json_data, record_json_output)
        internal_notes = json_data.get("internal_notes")
        if custom_fields:
            record_json_output.update({"custom_fields": custom_fields})
        if internal_notes:
            record_json_output.update(
                {"internal_notes": json_data.get("internal_notes")}
            )
        access = None
        try:
            access = self._access(json_data, record_dump)
        except RecordFlaggedCuration as exc:
            RDMJsonLogger().add_success_state(
                entry["recid"],
                {"message": exc.message, "value": exc.value},
            )
        return {
            "created": self._created(json_data),
            "updated": self._updated(record_dump),
            "version_id": self._version_id(record_dump),
            "index": self._index(record_dump),
            "recid": self._recid(record_dump),
            "communities": self._communities(json_data),
            "json": record_json_output,
            "access": access,
            "owned_by": self._owner(json_data),
            # keep the original extracted entry for storing it
            "_original_dump": entry,
            "_clc_sync": clc_sync,
        }


class CDSToRDMRecordTransform(RDMRecordTransform):
    """CDSToRDMRecordTransform."""

    def __init__(
        self,
        workers=None,
        throw=True,
        files_dump_dir=None,
        missing_users=None,
        community_id=None,
        dry_run=False,
    ):
        """Constructor."""
        self.files_dump_dir = Path(files_dump_dir).absolute().as_posix()
        self.missing_users_dir = Path(missing_users).absolute().as_posix()
        self.community_id = community_id
        self.dry_run = dry_run
        self.db_state = {"affiliations": CDSMigrationAffiliationMapping}
        super().__init__(workers, throw)

    def _community_id(self, entry, record):
        communities = record.get("communities", [])
        communities = [self.community_id] + [slug for slug in communities]
        if communities:
            return {"ids": communities, "default": self.community_id}
        return {}

    def _parent(self, entry, record):
        if record["owned_by"] == "system":
            owner = "system"
        else:
            try:
                owner = int(record["owned_by"])
            except (ValueError, TypeError):
                owner = "system"
        parent = {
            "created": record["created"],  # same as the record
            "updated": record["updated"],  # same as the record
            "version_id": record["version_id"],
            "json": {
                # loader is responsible for creating/updating if the PID exists.
                # this part will be simply omitted
                "id": f'{record["recid"]}-parent',
                "access": {
                    "owned_by": {"user": owner},
                },
                "communities": self._community_id(entry, record),
            },
        }

        return parent

    def _transform(self, entry):
        """Transform a single entry."""
        # creates the output structure for load step
        migration_logger = RDMJsonLogger()
        try:
            record = self._record(entry)
            original_dump = record.pop("_original_dump", {})
            clc_sync = record.pop("_clc_sync", {})

            if record:
                return {
                    "record": record,
                    "versions": self._versions(entry, record),
                    "parent": self._parent(entry, record),
                    "_original_dump": original_dump,
                    "_clc_sync": clc_sync,
                }
        except (
            LossyConversion,
            RestrictedFileDetected,
            UnexpectedValue,
            ManualImportRequired,
            MissingRequiredField,
        ) as e:
            migration_logger.add_log(e, record=entry)

    def _record(self, entry):
        # could be in draft as well, depends on how we decide to publish
        return CDSToRDMRecordEntry(
            missing_users_dir=self.missing_users_dir,
            affiliations_mapping=self.db_state["affiliations"],
            dry_run=self.dry_run,
        ).transform(entry)

    def _draft(self, entry):
        return None

    def _parse_file_status(self, file_status):
        pass

    def _versions(self, entry, record):

        def compute_access(file, record_access):

            if file is None:
                return {
                    "access_obj": {
                        "record": record_access,
                        "files": record_access,
                    }
                }

            if not file["status"]:
                return {
                    "access_obj": {
                        "record": record_access,
                        "files": record_access,
                    }
                }

            if file["status"]:
                # if we have anything in the status string,
                # it means the file is restricted
                # we pass this information to parse later in load step
                return {
                    "access_obj": {"record": record_access, "files": "restricted"},
                    "meta": file["status"],
                }

        def compute_files(file_dump, versions_dict):
            legacy_path_root = Path("/opt/cdsweb/var/data/files/")
            tmp_eos_root = Path(self.files_dump_dir)
            full_path = Path(file["full_path"])

            if file["subformat"] in FILE_SUBFORMATS_TO_DROP:
                RDMJsonLogger().add_success_state(
                    str(file["recid"]),
                    {
                        "message": f"File subformat {file['subformat']} dropped.",
                        "value": file["full_name"],
                    },
                )
                return

            if file["type"] == "Plot":
                # skip figures
                RDMJsonLogger().add_success_state(
                    str(file["recid"]),
                    {
                        "message": f"Plot file dropped.",
                        "value": file["full_name"],
                    },
                )
                return
            if file["hidden"]:
                # skip hidden files
                RDMJsonLogger().add_success_state(
                    str(file["recid"]),
                    {
                        "message": f"Hidden file dropped.",
                        "value": file["full_name"],
                    },
                )

            versions_dict[file_dump["version"]]["files"].update(
                {
                    file["full_name"]: {
                        "eos_tmp_path": tmp_eos_root
                        / full_path.relative_to(legacy_path_root),
                        "id_bibdoc": file["bibdocid"],
                        "key": file["full_name"],
                        "metadata": {},
                        "mimetype": file["mime"],
                        "checksum": file["checksum"],
                        "version": file["version"],
                        "access": file["status"],
                        "type": file["type"],
                        "creation_date": arrow.get(file["creation_date"])
                        .replace(tzinfo=None)
                        .date()
                        .isoformat(),
                    }
                }
            )

        # grouping draft attributes by version
        # we build temporary representation of each version
        # {"1": {"access": {...}, "files": [], "publication_date": None}
        # {"2": {"access": {...}, "files": [], "publication_date: "2021-04-21"}
        versions = OrderedDict()
        # we start versions from files (because this is the only way of
        # mapping version of files to version of records from legacy)
        _files = entry["files"]
        record_access = record["access"]
        for file in _files:
            if file["version"] not in versions:
                versions[file["version"]] = {
                    "files": {},
                    "publication_date": arrow.get(file["creation_date"]).replace(
                        tzinfo=None
                    ),
                    "access": compute_access(file, record_access),
                }

            compute_files(file, versions)

        versioned_files = {}
        # creates a collection of files per each version
        # lets say record has 2 files: A & B
        # if for file A new version was uploaded (version 2),
        # we need to preserve the file B for version 2 of the record
        for version in versions.keys():
            versioned_files |= versions.get(version, {}).get("files")
            versions[version]["files"] = versioned_files

        if not versioned_files:
            # Record has no files. Add metadata-only record as single version
            versions[1] = {
                "files": {},
                "publication_date": arrow.get(
                    record["json"]["metadata"]["publication_date"]
                ),
                "access": compute_access(
                    None, record_access
                ),  # public metadata and files
            }

        return versions

    def _record_files(self, entry, record):
        """Record files entries transform."""
        # TO implement if we decide not to go via draft publish
        return []

    def run(self, entries):
        """Run transformation step."""
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
