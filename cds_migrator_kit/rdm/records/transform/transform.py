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
import yaml
from cds_rdm.legacy.models import CDSMigrationAffiliationMapping
from dateutil.parser import ParserError, parse
from flask import current_app
from idutils import normalize_ror
from idutils.validators import is_doi, is_ror
from invenio_accounts.models import User, UserIdentity
from invenio_db import db
from invenio_pidstore.models import PersistentIdentifier, PIDStatus
from invenio_rdm_migrator.streams.records.transform import (
    RDMRecordEntry,
    RDMRecordTransform,
)
from invenio_vocabularies.contrib.affiliations.models import AffiliationsMetadata
from invenio_vocabularies.contrib.names.models import NamesMetadata
from sqlalchemy.exc import NoResultFound

from cds_migrator_kit.errors import (
    ManualImportRequired,
    MissingRequiredField,
    MultipleModelsMatched,
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
from cds_migrator_kit.transform.dumper import CDSRecordDump
from cds_migrator_kit.transform.errors import LossyConversion

cli_logger = logging.getLogger("migrator")

_VOCAB_FILENAMES = {
    "experiments": "experiments.yaml",
    "departments": "departments.yaml",
    "programmes": "programmes.yaml",
    "accelerators": "accelerators.yaml",
    "beams": "beams.yaml",
}


class VocabularyCache:
    """Vocabulary lookup cache loaded once from YAML files at startup."""

    def __init__(self, vocab_dir):
        """Load all vocabularies from the given directory into memory."""
        self._cache = {}
        vocab_dir = Path(vocab_dir)
        for vocab_type, filename in _VOCAB_FILENAMES.items():
            self._cache[vocab_type] = self._load(vocab_dir / filename)

    @staticmethod
    def _load(filepath):
        """Build a case-insensitive term→id lookup from a vocabulary YAML."""
        with open(filepath) as f:
            entries = yaml.safe_load(f)
        lookup = {}
        for entry in entries:
            entry_id = entry["id"]
            lookup[entry_id.lower()] = entry_id
            title = entry.get("title", {}).get("en", "")
            if title and title.lower() != entry_id.lower():
                lookup[title.lower()] = entry_id
        return lookup

    def get(self, term, vocab_type):
        """Return {"id": vocab_id} if term matches, else None."""
        entry_id = self._cache[vocab_type].get(term.strip().lower())
        return {"id": entry_id} if entry_id else None


_vocabulary_cache = None


def _get_vocabulary_cache():
    global _vocabulary_cache
    if _vocabulary_cache is None:
        vocab_dir = current_app.config.get("CDS_MIGRATOR_KIT_VOCABULARIES_DIR")
        if vocab_dir is None:
            import cds_rdm

            vocab_dir = Path(cds_rdm.__file__).parent / "app_data" / "vocabularies"
        else:
            vocab_dir = Path(vocab_dir)
        _vocabulary_cache = VocabularyCache(vocab_dir)
    return _vocabulary_cache


def search_vocabulary(term, vocab_type):
    """Look up a vocabulary term using the pre-loaded YAML cache.

    Returns {"id": vocab_id} if found, else None.
    """
    return _get_vocabulary_cache().get(term, vocab_type)


class CDSToRDMRecordEntry(RDMRecordEntry):
    """Transform CDS record to RDM record."""

    def __init__(
        self,
        partial=False,
        missing_users_dir=None,
        missing_users_filename="people.csv",
        affiliations_mapping=None,
        dry_run=False,
        collection=None,
        restricted=False,
        migration_logger=None,
        record_state_logger=None,
        access_grants_view=None,
    ):
        """Constructor."""
        self.missing_users_dir = missing_users_dir
        self.missing_users_filename = missing_users_filename
        self.affiliations_mapping = affiliations_mapping
        self.dry_run = dry_run
        self.collection = collection
        self.restricted = restricted
        self.access_grants_view = access_grants_view
        self.migration_logger = migration_logger
        self.record_state_logger = record_state_logger
        super().__init__(partial)

    def _created(self, entry):
        return entry["created"]

    def _updated(self, record_dump):
        """Returns the creation date of the record."""
        return record_dump.data["record"][0]["modification_datetime"]

    def _version_id(self, entry):
        """Returns the version id of the record."""
        return 1

    def _access(self, entry, record_dump):
        record_restriction = (
            r[0] if isinstance(r := entry.get("record_restriction"), list) else r
        )
        restrictions = "restricted" if self.restricted else record_restriction
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
                doi = identifier["identifier"]

                if doi.startswith(DATACITE_PREFIX):
                    doi_identifier["provider"] = "datacite"
                else:
                    doi_identifier["provider"] = "external"

                if doi.startswith(DATACITE_PREFIX) or doi.startswith("10.5170"):
                    if not json_entry.get("publisher"):
                        json_entry["publisher"] = "CERN"
                output_pids["doi"] = doi_identifier
        if output_pids:
            return output_pids
        else:
            return {}

    def _files(self, record_dump):
        """Transform the files of a record."""
        record_dump.prepare_files()
        files = record_dump.files
        return {"enabled": bool(files)}

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
            raise UnexpectedValue(
                message=f"{email} not found - did you run user migration?",
                stage="transform",
                recid=json_entry["legacy_recid"],
                value=email,
                priority="critical",
            )

    def _match_affiliation(self, affiliation_name, json_entry):
        """Match an affiliation against `CDSMigrationAffiliationMapping` db table."""
        if is_ror(affiliation_name):
            ror = normalize_ror(affiliation_name)
            name = AffiliationsMetadata.query.filter_by(pid=ror).one_or_none()
            if name is None:
                raise ManualImportRequired(
                    message="Affiliation {ror} does not exist in the AffiliationMetadata table".format(
                        ror=ror
                    ),
                    field="validation",
                    stage="transform",
                    description="Add this affiliation",
                    recid=json_entry["recid"],
                    priority="critical",
                    value=None,
                    subfield=None,
                )
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
            if match.ror_exact_match:
                return {"id": normalize_ror(match.ror_exact_match)}
            # Step 3: check if there is not exact match
            if match.ror_not_exact_match:
                _affiliation_ror_id = normalize_ror(match.ror_not_exact_match)
                raise RecordFlaggedCuration(
                    subfield="u",
                    value={"id": _affiliation_ror_id},
                    field="author",
                    message=f"Affiliation {_affiliation_ror_id} not found as an exact match, ROR id should be checked.",
                    stage="vocabulary match",
                )
        # Step 4: set the originally inserted value from legacy (no match, or match
        # found but has no ROR id of any kind)
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
                    affiliation = self._match_affiliation(affiliation_name, json_entry)
                    if affiliation not in transformed_aff:
                        transformed_aff.append(affiliation)
                except RecordFlaggedCuration as exc:
                    # Save not exact match affiliation and reraise to flag the record
                    self.migration_logger.add_information(
                        json_entry["recid"],
                        {"message": exc.message, "value": exc.value},
                    )
                    aff = {"name": affiliation_name}
                    if aff not in transformed_aff:
                        transformed_aff.append({"name": affiliation_name})
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
                    # db.session.commit()

        def creators(json, key="creators"):
            _creators = deepcopy(json.get(key, []))
            _creators = list(filter(lambda x: x is not None, _creators))
            for creator in _creators:
                creator_affiliations(creator)
                lookup_person_id(creator)
                creator_identifiers(creator)
            return _creators

        def _resource_type(entry):
            try:
                return entry["resource_type"]
            except KeyError:
                raise MissingRequiredField(message="resource_type", field="980")

        def _title(entry, resource_type):
            title = entry.get("title")
            if title:
                return title
            # 245 (title) is sometimes absent on conference proceedings
            # records; fall back to the conference name (111__a) stored on
            # the first meeting entry.
            if resource_type.get("id") == "publication-conferenceproceeding":
                meetings = entry.get("custom_fields", {}).get("meeting:meeting", [])
                for meeting_entry in meetings:
                    meeting_title = meeting_entry.get("title")
                    if meeting_title:
                        return meeting_title
            return title

        def _publication_date(entry, dump_record):
            pub_date = entry.get("publication_date")
            created = entry.get("status_week_date")
            files = dump_record["files"]
            if not (pub_date or created or files):
                raise MissingRequiredField(
                    message="missing creation or publication date", field="916"
                )
            if not pub_date:
                if created:
                    pub_date = entry["status_week_date"]
                elif not created and files:
                    pub_date = parse(files[0]["creation_date"]).date().isoformat()
            return pub_date

        def _identifiers(json_entry):
            identifiers = json_entry.get("identifiers", [])
            for item in reversed(identifiers):
                # drop unwanted schemes
                if item is None or "scheme" not in item:
                    raise UnexpectedValue(
                        field="identifiers",
                        value=item,
                        subfield="9",
                        message="IDENTIFIER SCHEME MISSING",
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

        def table_of_contents(json_entry):
            toc = json_entry.get("table_of_content", [])
            additional_desc = json_entry.get("additional_descriptions", [])
            if toc:
                additional_desc.append(
                    {"description": toc, "type": {"id": "table-of-contents"}}
                )
                json_entry["additional_descriptions"] = additional_desc
                json_entry.pop("table_of_content")

        def subjects(json_entry):
            _subjects = json_entry.get("subjects")
            if _subjects:
                for subject in reversed(_subjects):
                    if subject.get("subject", "").lower() in ["xx", "talk"]:
                        _subjects.remove(subject)
                    elif subject.get("id", "").lower() in ["xx", "talk"]:
                        _subjects.remove(subject)
            return _subjects

        _subjects = subjects(json_entry)
        table_of_contents(json_entry)

        _resource_type_value = _resource_type(json_entry)
        metadata = {
            "creators": creators(json_entry),
            "title": _title(json_entry, _resource_type_value),
            "resource_type": _resource_type_value,
            "description": json_entry.get("description"),
            "publication_date": _publication_date(json_entry, record_dump),
            "contributors": creators(json_entry, key="contributors"),
            "subjects": _subjects,
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

        # filter empty keys
        helper_keys = [
            "recid",
            "legacy_recid",
            "agency_code",
            "submitter",
            "status_week_date",
            "record_restriction",
            "access_grants",
            "custom_fields",
            "_pids",
            "internal_notes",
        ]

        keys = deepcopy(list(json_entry.keys()))
        for item in helper_keys:
            if item in keys:
                keys.remove(item)

        forgotten_keys = [key for key in keys if key not in list(metadata.keys())]
        if forgotten_keys:
            raise ManualImportRequired("Unassigned metadata key", value=forgotten_keys)
        return {k: v for k, v in metadata.items() if v}

    def _custom_fields(self, json_entry, json_output):

        def field_experiments(record_json, custom_fields_dict):
            experiments = record_json.get("custom_fields", {}).get(
                "cern:experiments", []
            )
            for experiment in experiments:
                if experiment.lower().strip() in ["not applicable", "xx"]:
                    continue
                result = search_vocabulary(experiment, "experiments")
                if result and result not in custom_fields_dict["cern:experiments"]:
                    custom_fields_dict["cern:experiments"].append(result)
                elif not result:
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
                if result:
                    return result
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
                if "-" in department:
                    units = department.split("-")
                    dep = units[0]
                else:
                    dep = department
                result = search_vocabulary(dep, "departments")
                if result and result not in custom_fields_dict["cern:departments"]:
                    custom_fields_dict["cern:departments"].append(result)
                elif not result:
                    subj = json_output["metadata"].get("subjects", [])
                    subj.append({"subject": department})
                    json_output["metadata"]["subjects"] = subj
                    custom_fields_dict["cern:administrative_unit"] = department
                    raise RecordFlaggedCuration(
                        subfield="a",
                        value=department,
                        field="department",
                        message=f"Department {department} not found. "
                        f"Added as unit and subject",
                        stage="vocabulary match",
                    )

        def field_accelerators(record_json, custom_fields_dict):
            accelerators = record_json.get("custom_fields", {}).get(
                "cern:accelerators", []
            )
            for accelerator in accelerators:
                if accelerator.lower().strip() in ["not applicable", "xx", "fermi"]:
                    continue
                result = search_vocabulary(accelerator, "accelerators")
                if result and result not in custom_fields_dict["cern:accelerators"]:
                    custom_fields_dict["cern:accelerators"].append(result)
                elif not result:
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
                if result and result not in custom_fields_dict["cern:beams"]:
                    custom_fields_dict["cern:beams"].append(result)
                elif not result:
                    raise UnexpectedValue(
                        subfield="a",
                        value=beam,
                        field="beams",
                        message=f"Beam {beam} not found.",
                        stage="vocabulary match",
                    )

        def field_journal(record_json):
            """Raise if title is missing in journal field"""
            journal = record_json.get("custom_fields", {}).get("journal:journal", {})
            if journal:
                if not journal.get("title"):
                    raise RecordFlaggedCuration(
                        message="found partial journal field, to be checked",
                        stage="transform",
                        field="773",
                    )
                return journal
            return {}

        _cf = json_entry.get("custom_fields", {})
        try:
            journal = field_journal(json_entry)
        except RecordFlaggedCuration as e:
            self.migration_logger.add_information(
                json_entry["recid"],
                {"message": e.message, "value": e.value},
            )
            journal = {}
        custom_fields = {
            "cern:experiments": [],
            "cern:departments": [],
            "cern:accelerators": [],
            "cern:administrative_unit": _cf.get("cern:administrative_unit", []),
            "cern:projects": _cf.get("cern:projects", []),
            "cern:facilities": _cf.get("cern:facilities", []),
            "cern:studies": _cf.get("cern:studies", []),
            "cern:beams": [],
            "cern:programmes": field_programmes(json_entry),
            "cern:committees": _cf.get("cern:committees"),
            "cern:oa_funding_model": _cf.get("cern:oa_funding_model"),
            "thesis:thesis": _cf.get("thesis:thesis", {}),
            "journal:journal": journal,
            "imprint:imprint": _cf.get("imprint:imprint", {}),
            "meeting:meeting": _cf.get("meeting:meeting", {}),
        }
        try:
            field_experiments(json_entry, custom_fields)
            field_departments(json_entry, custom_fields)

        except RecordFlaggedCuration as exc:
            self.migration_logger.add_information(
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
        # filter out null values
        return {k: v for k, v in custom_fields.items() if v}

    def _verify_publication_date(self, entry, json_data):
        """Verify creation date.

        If the record has no files (file creation date will be used as record
        creation date) and no creation date, raise an exception.
        """
        if not entry.get("files") and not (
            json_data.get("status_week_date") or json_data.get("publication_date")
        ):
            raise ManualImportRequired(
                message="Record missing publication date",
                field="validation",
                stage="transform",
                description="Record has no files and no publication date",
                recid=entry["recid"],
                priority="warning",
                value=None,
                subfield=None,
            )

    def _access_grants(self, json_data, record_json_output):
        access_grants = json_data.get("access_grants", [])
        if self.access_grants_view:
            for grant in self.access_grants_view:
                access_grants.append({str(grant): "view"})
        if access_grants:
            record_json_output.update({"access_grants": access_grants})

    def transform(self, entry):
        """Transform a record single entry."""
        record_dump = CDSRecordDump(
            entry,
        )

        record_dump.prepare_revisions()
        timestamp, json_data = record_dump.latest_revision

        self._verify_publication_date(entry, json_data)

        self.record_state_logger.add_record(json_data)

        clc_sync = deepcopy(json_data.get("_clc_sync", False))
        if "_clc_sync" in json_data:
            del json_data["_clc_sync"]

        request_data = json_data.pop("request_data", None)

        record_json_output = {
            "files": self._files(record_dump),
            "pids": self._pids(json_data),
            "metadata": self._metadata(json_data, entry),
        }

        self._access_grants(json_data, record_json_output)
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
            self.migration_logger.add_information(
                entry["recid"],
                {"message": exc.message, "value": exc.value},
            )
        return {
            "created": record_dump.first_created,
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
            "_request_data": request_data,
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
        communities_ids=None,
        dry_run=False,
        collection=None,
        restricted=False,
        plots=False,
        migration_logger=None,
        record_state_logger=None,
        access_grants_view=None,
    ):
        """Constructor."""
        self.files_dump_dir = Path(files_dump_dir).absolute().as_posix()
        self.missing_users_dir = Path(missing_users).absolute().as_posix()
        self.communities_ids = communities_ids
        self.dry_run = dry_run
        self.collection = collection
        self.restricted = restricted
        self.access_grants_view = access_grants_view
        self.plots = plots
        self.migration_logger = migration_logger
        self.record_state_logger = record_state_logger
        self.db_state = {"affiliations": CDSMigrationAffiliationMapping}
        super().__init__(workers, throw)

    def _communities_ids(self, entry, record):
        communities = record.get("communities", [])
        communities = self.communities_ids + [slug for slug in communities]
        if communities:
            return {"ids": communities, "default": self.communities_ids[0]}
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
                "communities": self._communities_ids(entry, record),
            },
        }

        return parent

    def _transform(self, entry):
        """Transform a single entry."""
        # creates the output structure for load step
        migration_logger = self.migration_logger
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
            MultipleModelsMatched,
        ) as e:
            migration_logger.add_log(e, record=entry)

    def _record(self, entry):
        # could be in draft as well, depends on how we decide to publish

        return CDSToRDMRecordEntry(
            missing_users_dir=self.missing_users_dir,
            affiliations_mapping=self.db_state["affiliations"],
            dry_run=self.dry_run,
            collection=self.collection,
            restricted=self.restricted,
            access_grants_view=self.access_grants_view,
            migration_logger=self.migration_logger,
            record_state_logger=self.record_state_logger,
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
                self.migration_logger.add_information(
                    str(file["recid"]),
                    {
                        "message": "Record has individual file restrictions",
                        "value": file["status"],
                    },
                )
                return {
                    "access_obj": {"record": record_access, "files": "restricted"},
                    "meta": file["status"],
                }

        def should_skip_file(file_dump):
            if file_dump["subformat"] in FILE_SUBFORMATS_TO_DROP:
                self.migration_logger.add_information(
                    str(file_dump["recid"]),
                    {
                        "message": f"File subformat {file_dump['subformat']} dropped.",
                        "value": file_dump["full_name"],
                    },
                )
                return True

            if not self.plots and file_dump["type"] == "Plot":
                # skip figures if configuration says so
                self.migration_logger.add_information(
                    str(file_dump["recid"]),
                    {
                        "message": f"Plot file dropped.",
                        "value": file_dump["full_name"],
                    },
                )
                return True
            if file_dump["hidden"]:
                # skip hidden files
                self.migration_logger.add_information(
                    str(file_dump["recid"]),
                    {
                        "message": f"Hidden file dropped.",
                        "value": file_dump["full_name"],
                    },
                )
                return True
            return False

        def compute_files(file_dump, versions_dict):
            legacy_path_root = Path("/opt/cdsweb/var/data/files/")
            tmp_eos_root = Path(self.files_dump_dir)
            full_path = Path(file_dump["full_path"])

            versions_dict[file_dump["version"]]["files"].update(
                {
                    file_dump["full_name"]: {
                        "eos_tmp_path": tmp_eos_root
                        / full_path.relative_to(legacy_path_root),
                        "id_bibdoc": file_dump["bibdocid"],
                        "key": file_dump["full_name"],
                        "metadata": {
                            "description": file_dump["description"],
                            "name": file_dump["name"],
                            "status": file_dump["status"],
                            "comment": file_dump["comment"],
                        },
                        "mimetype": file_dump["mime"],
                        "checksum": file_dump["checksum"],
                        "version": file_dump["version"],
                        "access": file_dump["status"],
                        "type": file_dump["type"],
                        "creation_date": arrow.get(file_dump["creation_date"])
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
            if should_skip_file(file):
                continue
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
            versions[version]["files"] = deepcopy(versioned_files)
        publication_date = record["json"]["metadata"]["publication_date"]

        if not versioned_files:
            # Record has no files. Add metadata-only record as single version
            versions[1] = {
                "files": {},
                "publication_date": publication_date,
                "access": compute_access(
                    None, record_access
                ),  # public metadata and files
            }

        return versions

    def _record_files(self, entry, record):
        """Record files entries transform."""
        # TO implement if we decide not to go via draft publish
        return []

    def _load_migrated_recids(self):
        """Load all already-migrated legacy record IDs into a set once."""
        return {
            pid.pid_value
            for pid in PersistentIdentifier.query.filter_by(
                pid_type="lrecid",
                status=PIDStatus.REGISTERED,
            ).all()
        }

    def should_skip(self, entry):
        return str(entry["recid"]) in self._migrated_recids

    def run(self, entries):
        """Run transformation step."""
        self._migrated_recids = self._load_migrated_recids()

        for entry in entries:
            if self.should_skip(entry):
                if current_app.config["CDS_MIGRATOR_KIT_ENV"] == "local":
                    current_app.logger.warning(f"Skipping entry {entry['recid']}")
                continue
            try:
                yield self._transform(entry)
            except Exception:
                self.logger.exception(entry, exc_info=True)
                if self._throw:
                    raise
                continue

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
