# -*- coding: utf-8 -*-
#
# Copyright (C) 2022-2026 CERN.
#
# CDS-RDM is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""Creators/contributors field mapping: affiliations and person-id lookup."""

from copy import deepcopy

from idutils import normalize_ror
from idutils.validators import is_ror
from invenio_accounts.models import UserIdentity
from invenio_db import db
from invenio_vocabularies.contrib.affiliations.models import AffiliationsMetadata
from invenio_vocabularies.contrib.names.models import NamesMetadata

from cds_migrator_kit.errors import ManualImportRequired, RecordFlaggedCuration
from cds_migrator_kit.rdm.migration_config import VOCABULARIES_NAMES_SCHEMES
from cds_migrator_kit.rdm.records.transform.mappers.base import FieldMapper

# Sentinel distinguishing "not yet cached" from "cached as None (not found)".
_MISSING = object()

# Per-process caches. Plain-dict row representations avoid SQLAlchemy object
# expiry issues that occur across UoW commits between records.
_affiliation_ror_cache: dict = (
    {}
)  # ror_normalized → bool (exists in AffiliationsMetadata)
_affiliation_legacy_cache: dict = (
    {}
)  # legacy_name → {"curated","exact","not_exact"} or None
_person_id_to_user_id: dict = {}  # cern person_id str → user_id int or None


def match_affiliation(affiliation_name, ctx):
    """Match an affiliation against `CDSMigrationAffiliationMapping` db table."""
    dojson_entry = ctx.dojson_entry
    if is_ror(affiliation_name):
        ror = normalize_ror(affiliation_name)
        exists = _affiliation_ror_cache.get(ror, _MISSING)
        if exists is _MISSING:
            exists = (
                AffiliationsMetadata.query.filter_by(pid=ror).one_or_none() is not None
            )
            _affiliation_ror_cache[ror] = exists
        if not exists:
            raise ManualImportRequired(
                message="Affiliation {ror} does not exist in the AffiliationMetadata table".format(
                    ror=ror
                ),
                field="validation",
                stage="transform",
                description="Add this affiliation",
                recid=dojson_entry["recid"],
                priority="critical",
                value=None,
                subfield=None,
            )
        return {"id": normalize_ror(affiliation_name)}

    # Legacy name lookup — cache the row fields as a plain dict to avoid
    # SQLAlchemy object expiry that occurs across UoW commits.
    cached = _affiliation_legacy_cache.get(affiliation_name, _MISSING)
    if cached is _MISSING:
        match = ctx.affiliations_mapping.query.filter_by(
            legacy_affiliation_input=affiliation_name
        ).one_or_none()
        cached = (
            {
                "curated": match.curated_affiliation,
                "exact": match.ror_exact_match,
                "not_exact": match.ror_not_exact_match,
            }
            if match is not None
            else None
        )
        _affiliation_legacy_cache[affiliation_name] = cached

    if cached is not None:
        # Step 1: check if there is a curated input
        if cached["curated"]:
            return cached["curated"]
        # Step 2: check if there is an exact match
        if cached["exact"]:
            return {"id": normalize_ror(cached["exact"])}
        # Step 3: check if there is not exact match
        if cached["not_exact"]:
            _affiliation_ror_id = normalize_ror(cached["not_exact"])
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


def _creator_affiliations(creator, ctx):
    affiliations = creator.get("affiliations", [])
    transformed_aff = []

    for affiliation_name in affiliations:
        try:
            affiliation = match_affiliation(affiliation_name, ctx)
            if affiliation not in transformed_aff:
                transformed_aff.append(affiliation)
        except RecordFlaggedCuration as exc:
            # Save not exact match affiliation and reraise to flag the record
            ctx.flag_curation(exc)
            aff = {"name": affiliation_name}
            if aff not in transformed_aff:
                transformed_aff.append({"name": affiliation_name})
    creator["affiliations"] = transformed_aff


def _creator_identifiers(creator):
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


def _lookup_person_id(creator):
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
        user_id = _person_id_to_user_id.get(person_id, _MISSING)
        if user_id is _MISSING:
            ui = UserIdentity.query.filter_by(id=person_id).one_or_none()
            user_id = ui.user.id if ui else None
            _person_id_to_user_id[person_id] = user_id
        if user_id is not None:
            names = NamesMetadata.query.filter_by(internal_id=str(user_id)).all()
            name = next(
                (name for name in names if "unlisted" not in name.json.get("tags", [])),
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


def creators_for(ctx, key="creators"):
    """Build the creators/contributors list for ``key``."""
    _creators = deepcopy(ctx.dojson_entry.get(key, []))
    _creators = list(filter(lambda x: x is not None, _creators))
    for creator in _creators:
        _creator_affiliations(creator, ctx)
        _lookup_person_id(creator)
        _creator_identifiers(creator)
    return _creators


class CreatorsMapper(FieldMapper):
    """Maps the creators list, resolving affiliations and person ids."""

    id = "creators"

    def map_value(self, ctx):
        """Build the creators list."""
        return creators_for(ctx, key="creators")


class ContributorsMapper(FieldMapper):
    """Maps the contributors list, resolving affiliations and person ids."""

    id = "contributors"

    def map_value(self, ctx):
        """Build the contributors list."""
        return creators_for(ctx, key="contributors")
