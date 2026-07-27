# -*- coding: utf-8 -*-
#
# Copyright (C) 2026 CERN.
#
# CDS-RDM is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""Tests for Staff Association migration rules."""

from pathlib import Path

import pytest
from cds_rdm.legacy.models import CDSMigrationLegacyRecord
from dojson.errors import IgnoreKey
from helpers import config
from invenio_access.permissions import system_identity
from invenio_rdm_records.proxies import current_rdm_records_service
from invenio_rdm_records.records import RDMRecord

from cds_migrator_kit.errors import UnexpectedValue
from cds_migrator_kit.rdm.records.streams import RecordStreamDefinition
from cds_migrator_kit.rdm.records.transform.xml_processing.rules.staff_association import (
    resource_type,
    staff_association_collection,
    staff_contact_person,
    title,
)
from cds_migrator_kit.runner.runner import Runner


def test_full_staff_association_stream(
    test_app,
    minimal_restricted_record,
    uploader,
    client,
    search,
    search_clear,
    superuser_identity,
    orcid_name_data,
    community,
    mocker,
    groups,
):
    """Migrate staff association dump and assert resource types."""
    stream_config = config(mocker, community, orcid_name_data)

    runner = Runner(
        stream_definitions=[RecordStreamDefinition],
        config_filepath=Path(stream_config).absolute(),
        dry_run=False,
        collection="staff_association",
        keep_logs=False,
    )
    runner.run()

    assert CDSMigrationLegacyRecord.query.count() == 2
    RDMRecord.index.refresh()

    # Regular bulletin staff article
    results = current_rdm_records_service.search(
        system_identity, q="metadata.identifiers.identifier:3000001"
    )
    assert results.total == 1
    record = current_rdm_records_service.read(
        system_identity, list(results.hits)[0]["id"]
    ).to_dict()
    assert record["metadata"]["title"] == "Staff Association Bulletin Article"
    assert record["metadata"]["resource_type"]["id"] == "publication-periodicalarticle"

    # Staff Association Journal has resource type publication-article
    results = current_rdm_records_service.search(
        system_identity, q="metadata.identifiers.identifier:3000002"
    )
    assert results.total == 1
    journal_record = current_rdm_records_service.read(
        system_identity, list(results.hits)[0]["id"]
    ).to_dict()
    assert "Staff Association Journal" in journal_record["metadata"]["title"]
    assert journal_record["metadata"]["resource_type"]["id"] == "publication-article"


# Unit tests for Staff Association rules


def test_title_sets_journal_resource_type():
    """Title containing Staff Association Journal sets publication-article."""
    record = {}
    result = title(
        record,
        "245__",
        {"a": "Staff Association Journal - Issue 12"},
    )
    assert result == "Staff Association Journal - Issue 12"
    assert record["resource_type"] == {"id": "publication-article"}


def test_title_normal_flow_without_journal():
    """Normal title does not set journal resource type and follows base rule."""
    record = {}
    result = title(record, "245__", {"a": "Staff Association Bulletin Article"})
    assert result == "Staff Association Bulletin Article"
    assert "resource_type" not in record


def test_title_with_subtitle():
    """Title with subtitle delegates to base rule for additional_titles."""
    record = {}
    result = title(
        record,
        "245__",
        {"a": "Main title", "b": "A subtitle"},
    )
    assert result == "Main title"
    assert record["additional_titles"] == [
        {"title": "A subtitle", "type": {"id": "subtitle"}}
    ]


def test_resource_type_bulletinstaff():
    """980__ BULLETINSTAFF maps to periodical article."""
    assert resource_type({}, "980__", {"a": "BULLETINSTAFF"}) == {
        "id": "publication-periodicalarticle"
    }
    assert resource_type({}, "980__", {"a": "bulletinstaff"}) == {
        "id": "publication-periodicalarticle"
    }


def test_resource_type_staffassociation():
    """980__ STAFFASSOCIATION maps to periodical article."""
    assert resource_type({}, "980__", {"a": "STAFFASSOCIATION"}) == {
        "id": "publication-periodicalarticle"
    }
    assert resource_type({}, "980__", {"a": "staffassociation"}) == {
        "id": "publication-periodicalarticle"
    }


def test_resource_type_preserves_journal_article():
    """980__ does not overwrite publication-article set by title rule."""
    record = {"resource_type": {"id": "publication-article"}}
    result = resource_type(record, "980__", {"a": "BULLETINSTAFF"})
    assert result == {"id": "publication-article"}


def test_resource_type_from_subfield_b():
    """resource_type falls back to subfield b when a is missing."""
    assert resource_type({}, "980__", {"b": "bulletinstaff"}) == {
        "id": "publication-periodicalarticle"
    }


def test_resource_type_invalid():
    """Unknown resource type raises UnexpectedValue."""
    with pytest.raises(UnexpectedValue):
        resource_type({}, "980__", {"a": "invalid_type"})


def test_collection_drops_sa_documents():
    """690C_ 'SA Documents' is ignored."""
    with pytest.raises(IgnoreKey):
        staff_association_collection({}, "690C_", {"a": "SA Documents"})


def test_collection_valid_cern():
    """Valid CERN collection is delegated and ignored by bulletin rule."""
    with pytest.raises(IgnoreKey):
        staff_association_collection({}, "690C_", {"a": "CERN"})


def test_collection_invalid():
    """Invalid collection raises UnexpectedValue via bulletin collection."""
    with pytest.raises(UnexpectedValue):
        staff_association_collection({}, "690C_", {"a": "Invalid Collection"})


def test_submitter_ignored_staff_bulletin():
    """Known staff bulletin emails do not create contact descriptions."""
    record = {}
    result = staff_contact_person(
        record, "859__", {"a": "staff.bulletin@cern.ch", "f": "submitter13@cern.ch"}
    )
    assert result == "submitter13@cern.ch"
    assert "additional_descriptions" not in record


def test_submitter_adds_contact_person():
    """Unknown contact email is stored as additional description."""
    record = {}
    result = staff_contact_person(
        record, "859__", {"a": "real.person@cern.ch", "f": "submitter13@cern.ch"}
    )
    assert result == "submitter13@cern.ch"
    assert record["additional_descriptions"] == [
        {
            "description": "<p>Contact: real.person@cern.ch</p>",
            "type": {"id": "other"},
        }
    ]


def test_submitter_empty_contact_ignored():
    """Empty contact person does not create additional description."""
    record = {}
    staff_contact_person(record, "859__", {"a": "", "f": "submitter13@cern.ch"})
    assert "additional_descriptions" not in record


def test_submitter_appends_to_existing_descriptions():
    """Contact description is appended when field already exists."""
    record = {
        "additional_descriptions": [
            {"description": "<p>Existing</p>", "type": {"id": "other"}}
        ]
    }
    staff_contact_person(
        record, "859__", {"a": "new.contact@cern.ch", "f": "submitter13@cern.ch"}
    )
    assert len(record["additional_descriptions"]) == 2
    assert record["additional_descriptions"][1]["description"] == (
        "<p>Contact: new.contact@cern.ch</p>"
    )
