# -*- coding: utf-8 -*-
#
# Copyright (C) 2025 CERN.
#
# CDS-RDM is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""Tests suites."""
import json
from pathlib import Path

import pytest
from cds_rdm.clc_sync.models import CDSToCLCSyncModel
from cds_rdm.legacy.models import CDSMigrationLegacyRecord
from dojson.errors import IgnoreKey
from helpers import config
from invenio_access.permissions import system_identity
from invenio_rdm_records.proxies import current_rdm_records_service
from invenio_rdm_records.records.api import RDMDraft, RDMParent, RDMRecord
from invenio_search.engine import dsl

from cds_migrator_kit.errors import UnexpectedValue
from cds_migrator_kit.rdm.records.streams import RecordStreamDefinition
from cds_migrator_kit.rdm.records.transform.xml_processing.rules.hr import (
    access_grants,
    additional_desc,
    additional_descriptions,
    collection,
    corpo_author,
    custom_fields,
    date,
    description,
    hr_subjects,
    note,
    record_restriction,
    rep_num,
    resource_type,
    title,
    translated_description,
)
from cds_migrator_kit.runner.runner import Runner


def test_full_hr_stream(
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

    stream_config = config(mocker, community, orcid_name_data)

    runner = Runner(
        stream_definitions=[RecordStreamDefinition],
        config_filepath=Path(stream_config).absolute(),
        dry_run=False,
        collection="hr",
        keep_logs=False,
    )
    runner.run()

    # 2 Test records
    assert CDSMigrationLegacyRecord.query.count() == 2

    legacy_recid = "2647384"
    RDMRecord.index.refresh()
    results = current_rdm_records_service.search(
        system_identity, q=f"metadata.identifiers.identifier:{legacy_recid}"
    )
    assert results.total == 1
    new_record = current_rdm_records_service.read(
        system_identity, list(results.hits)[0]["id"]
    )
    new_record = new_record.to_dict()

    # Check if sync entry created
    assert CDSToCLCSyncModel.query.filter_by(
        parent_record_pid=new_record["parent"]["id"]
    )

    # Administrative Unit
    assert new_record["custom_fields"]["cern:administrative_unit"] == "DI"

    # Title extracted from field 037__ (CERN-STAFF-RULES-ED01) when missing in 245__
    assert new_record["metadata"]["title"] == "Staff Rules and Regulations ed. 01"

    # Restricted access - field 591 --> CERN INTERNAL
    assert new_record["access"] == {
        "record": "restricted",
        "files": "restricted",
        "embargo": {"active": False, "reason": None},
        "status": "restricted",
    }

    # Dates --> 9999 not in 925__b
    new_record["metadata"]["dates"] = [
        {"date": "2021-05-01", "type": {"id": "valid", "title": {"en": "Valid"}}},
        {
            "date": "2021-05-01",
            "type": {"id": "withdrawn", "title": {"en": "Withdrawn"}},
        },
    ]

    another_legacy_recid = "2364643"

    results = current_rdm_records_service.search(
        system_identity, q=f"metadata.identifiers.identifier:{another_legacy_recid}"
    )

    assert results.total == 1
    new_record = current_rdm_records_service.read(
        system_identity, list(results.hits)[0]["id"]
    )

    new_record = new_record.to_dict()

    # File status restricted
    assert new_record["access"]["files"] == "restricted"


# Unit tests for HR rules functions


def test_access_grants_with_email():
    """Test access_grants function with email in different subfields."""
    # Test with email in subfield d
    result = access_grants({}, "5061_", {"d": "user@cern.ch"})
    assert result == [{"user@cern.ch": "view"}]

    # Test with email in subfield m
    result = access_grants({}, "5061_", {"m": "test@example.com"})
    assert result == [{"test@example.com": "view"}]

    # Test with email in subfield a
    result = access_grants({}, "5061_", {"a": "admin@cern.ch"})
    assert result == [{"admin@cern.ch": "view"}]


def test_access_grants_empty_values():
    """Test access_grants with empty or missing values."""
    # Test with empty value should raise IgnoreKey
    with pytest.raises(IgnoreKey):
        access_grants({}, "5061_", {"d": ""})

    # Test with no email fields should raise IgnoreKey
    with pytest.raises(IgnoreKey):
        access_grants({}, "5061_", {})


def test_additional_desc_empty_email():
    """Test additional_desc with empty email raises IgnoreKey."""
    # Note: additional_desc has side effects that require dojson model context
    # We can only test that it raises IgnoreKey for empty values
    with pytest.raises(IgnoreKey):
        additional_desc({}, "270__", {"m": ""})


def test_collection_chis_bulletin():
    """Test collection function with CHIS Bulletin."""
    record = {}
    with pytest.raises(IgnoreKey):
        collection(record, "690C_", {"a": "CHIS Bulletin"})
    # Should have added subjects
    assert "subjects" in record
    assert {"subject": "collection:chis bulletin"} in record["subjects"]
    assert {"subject": "CHIS"} in record["subjects"]


def test_collection_valid_collections():
    """Test collection function with valid collection values."""
    valid_collections = [
        "cern admin e-guide",
        "staff rules and regulations",
        "CERN",
        "Annual Personnel Statistics",
        "Administrative Circular",
    ]

    for coll in valid_collections:
        with pytest.raises(IgnoreKey):
            collection({}, "690C_", {"a": coll})


def test_collection_invalid_collection():
    """Test collection function with invalid collection value."""
    with pytest.raises(UnexpectedValue):
        collection({}, "690C_", {"a": "Invalid Collection"})


def test_corpo_author_valid():
    """Test corpo_author function with valid author."""
    result = corpo_author({}, "110__", {"a": "CERN"})
    assert result == [{"person_or_org": {"type": "organizational", "name": "CERN"}}]


def test_corpo_author_invalid():
    """Test corpo_author with empty or missing author."""
    # Test with empty author should raise UnexpectedValue
    with pytest.raises(UnexpectedValue):
        corpo_author({}, "110__", {"a": ""})

    # Test with missing author should raise UnexpectedValue
    with pytest.raises(UnexpectedValue):
        corpo_author({}, "110__", {})


def test_resource_type_mapping():
    """Test resource_type function with various valid resource types."""
    assert resource_type({}, "980__", {"a": "annualstats"}) == {
        "id": "publication-report"
    }
    assert resource_type({}, "980__", {"a": "cern-admin-e-guide"}) == {
        "id": "publication-other"
    }
    assert resource_type({}, "980__", {"a": "intnotehrpubl"}) == {
        "id": "publication-technicalnote"
    }
    assert resource_type({}, "980__", {"a": "admincircular"}) == {
        "id": "administrative-circular"
    }
    assert resource_type({}, "980__", {"a": "opercircular"}) == {
        "id": "administrative-operationalcircular"
    }
    assert resource_type({}, "980__", {"a": "staffrules"}) == {
        "id": "administrative-regulation"
    }
    assert resource_type({}, "980__", {"a": "staffrulesvd"}) == {
        "id": "administrative-regulation"
    }
    assert resource_type({}, "980__", {"a": "hr-smc"}) == {
        "id": "administrative-regulation"
    }
    assert resource_type({}, "980__", {"a": "ccp"}) == {"id": "other"}
    assert resource_type({}, "980__", {"a": "conferencepaper"}) == {
        "id": "publication-conferencepaper"
    }


def test_resource_type_ignored_values():
    """Test resource_type with values that should be ignored."""
    # Test article should be ignored
    with pytest.raises(IgnoreKey):
        resource_type({}, "980__", {"a": "article"})

    # Test administrativenote should be ignored
    with pytest.raises(IgnoreKey):
        resource_type({}, "980__", {"a": "administrativenote"})


def test_resource_type_with_subject_collection():
    """Test resource_type that adds subject collections."""
    # Test hr-smc adds subject
    record = {}
    result = resource_type(record, "980__", {"a": "hr-smc"})
    assert {"subject": "collection:hr-smc"} in record["subjects"]
    assert result == {"id": "administrative-regulation"}

    # Test ccp adds subject
    record = {}
    result = resource_type(record, "980__", {"a": "ccp"})
    assert {"subject": "collection:ccp"} in record["subjects"]
    assert result == {"id": "other"}


def test_resource_type_invalid():
    """Test resource_type with invalid value."""
    with pytest.raises(UnexpectedValue):
        resource_type({}, "980__", {"a": "invalid_type"})


def test_record_restriction_values():
    """Test record_restriction function with different access values."""
    # Test CERN internal
    assert record_restriction({}, "591__", {"a": "CERN INTERNAL"}) == ["restricted"]
    assert record_restriction({}, "591__", {"a": "cern internal"}) == ["restricted"]

    # Test restricted
    assert record_restriction({}, "591__", {"a": "RESTRICTED"}) == ["restricted"]
    assert record_restriction({}, "591__", {"a": "restricted"}) == ["restricted"]


def test_record_restriction_public():
    """Test record_restriction with public access."""
    # Test public should raise IgnoreKey
    with pytest.raises(IgnoreKey):
        record_restriction({}, "591__", {"a": "public"})
    with pytest.raises(IgnoreKey):
        record_restriction({}, "591__", {"a": "PUBLIC"})


def test_record_restriction_invalid():
    """Test record_restriction with invalid value."""
    with pytest.raises(UnexpectedValue):
        record_restriction({}, "591__", {"a": "invalid_access"})


def test_dates_function_valid_only():
    """Test dates function with valid date only."""
    record = {}
    with pytest.raises(IgnoreKey):
        date(record, "925__", {"a": "2021-05-01"})
    assert len(record["dates"]) == 1
    assert record["dates"][0] == {"date": "2021-05-01", "type": {"id": "valid"}}


def test_dates_function_with_withdrawn():
    """Test dates function with valid and withdrawn dates."""
    record = {}
    with pytest.raises(IgnoreKey):
        date(record, "925__", {"a": "2021-05-01", "b": "2022-06-01"})
    assert len(record["dates"]) == 2
    assert {"date": "2021-05-01", "type": {"id": "valid"}} in record["dates"]
    assert {"date": "2022-06-01", "type": {"id": "withdrawn"}} in record["dates"]


def test_dates_function_with_9999():
    """Test dates function with 9999 withdrawn date (should be ignored)."""
    record = {}
    with pytest.raises(IgnoreKey):
        date(record, "925__", {"a": "2021-05-01", "b": "9999"})
    assert len(record["dates"]) == 1
    assert record["dates"][0] == {"date": "2021-05-01", "type": {"id": "valid"}}


def test_custom_fields_administrative_unit():
    """Test custom_fields function for administrative unit."""
    # Test with administrative unit
    record = {}
    with pytest.raises(IgnoreKey):
        custom_fields(record, "710__", {"b": "DI"})
    assert record["custom_fields"]["cern:administrative_unit"] == "DI"

    # Test with another unit
    record = {}
    with pytest.raises(IgnoreKey):
        custom_fields(record, "710__", {"b": "HR"})
    assert record["custom_fields"]["cern:administrative_unit"] == "HR"


def test_description_function_valid():
    """Test description function with valid descriptions."""
    result = description({}, "520__", {"a": "This is a valid description"})
    assert result == "This is a valid description"


def test_description_function_too_short():
    """Test description with short text (less than 3 chars)."""
    # Test with short description should raise IgnoreKey
    with pytest.raises(IgnoreKey):
        description({}, "520__", {"a": "ab"})

    # Test with empty description should raise IgnoreKey
    with pytest.raises(IgnoreKey):
        description({}, "520__", {"a": ""})


def test_translated_description():
    """Test translated_description function."""
    result = translated_description({}, "590__", {"a": "Description en français"})
    assert result == [
        {
            "description": "Description en français",
            "type": {"id": "other"},
            "lang": {"id": "fra"},
        }
    ]


def test_internal_notes():
    """Test internal notes function."""
    result = note({}, "562__", {"c": "This is an internal note"})
    assert result == [{"note": "This is an internal note"}]


def test_additional_descriptions_function_valid():
    """Test additional_descriptions function with valid description."""
    result = additional_descriptions({}, "500__", {"a": "Additional information"})
    assert result == [
        {
            "description": "Additional information",
            "type": {"id": "other"},
        }
    ]


def test_additional_descriptions_function_missing_field():
    """Test additional_descriptions with missing required field."""
    # Test with missing 'a' field should raise IgnoreKey
    with pytest.raises(IgnoreKey):
        additional_descriptions({}, "500__", {})


def test_title_staff_rules_extraction():
    """Test title extraction for Staff Rules from report numbers."""
    record = {}
    result = rep_num(record, "037__", {"a": "CERN-STAFF-RULES-ED01"})
    # Should set the title on the record
    assert record.get("title") == "Staff Rules and Regulations ed. 01"
    # Should also return identifier
    assert result is not None


def test_title_staff_rules_with_revision():
    """Test title extraction for Staff Rules with revision suffix."""
    record = {}
    result = rep_num(record, "037__", {"a": "CERN-STAFF-RULES-ED02-REV1"})
    assert record.get("title") == "Staff Rules and Regulations ed. 02"


def test_title_circular_alternative_titles():
    """Test alternative title generation for circulars."""
    # Test Administrative Circular
    record = {}
    result = rep_num(record, "037__", {"a": "CERN-ADMIN-CIRCULAR-1-REV0"})
    assert "additional_titles" in record
    assert any(
        t["title"] == "Administrative Circular No.1"
        for t in record["additional_titles"]
    )

    # Test Operational Circular
    record = {}
    result = rep_num(record, "037__", {"a": "CERN-OPER-CIRCULAR-2-REV0"})
    assert "additional_titles" in record
    assert any(
        t["title"] == "Operational Circular No.2" for t in record["additional_titles"]
    )

    # Test with revision
    record = {}
    result = rep_num(record, "037__", {"a": "CERN-ADMIN-CIRCULAR-3-REV2"})
    assert "additional_titles" in record
    assert any(
        "Administrative Circular No.3 (Rev 2)" in t["title"]
        for t in record["additional_titles"]
    )


def test_access_grants_multiple_groups():
    """Test access_grants function with multiple group/email assignments."""
    # This tests the transformation phase where 506 fields are collected
    # Multiple 506 fields should create multiple access grants

    # Test with multiple emails
    grant1 = access_grants({}, "5061_", {"d": "user1@cern.ch"})
    grant2 = access_grants({}, "5061_", {"d": "user2@cern.ch"})

    assert grant1 == [{"user1@cern.ch": "view"}]
    assert grant2 == [{"user2@cern.ch": "view"}]

    # Test with group-like identifiers
    grant3 = access_grants({}, "5061_", {"d": "hr-web-gacepa"})
    assert grant3 == [{"hr-web-gacepa": "view"}]
