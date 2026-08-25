# -*- coding: utf-8 -*-
#
# Copyright (C) 2026 CERN.
#
# CDS-RDM is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""Tests for research_committee.py migration rules."""

import pytest
from dojson.errors import IgnoreKey
from dojson.utils import GroupableOrderedDict

from cds_migrator_kit.errors import MissingRequiredField
from cds_migrator_kit.rdm.records.transform.entities.record import RecordEntry
from cds_migrator_kit.rdm.records.transform.models.research_committee import (
    research_comm_model,
)
from cds_migrator_kit.rdm.records.transform.xml_processing.rules.research_committee import (
    _RANK_REPORT_NUMBER,
    _RANK_SERIES,
    _RANK_TITLE,
    imprint,
    report_number,
    series_information,
    title,
)


class TestCommitteeReportNumberType:
    """Test the <COMMITTEE>-<TYPE>-<NUMBER> report number pattern detection
    (e.g. "SPSC-I-170", see https://cds.cern.ch/record/493774 for a real
    "SPSLC-P-282" example) and its resource_type/subject derivation.
    """

    def _call(self, record, key, value):
        """Plain committee report numbers (no scheme/provenance subfield)
        always resolve to the "cdsrn" scheme, which base.report_number
        stores directly on `identifiers` and signals via IgnoreKey - see
        TestCommitteeReportNumberBaseBehaviourPreserved. Swallow it here so
        each test can focus on the resource_type/subject side effect.
        """
        with pytest.raises(IgnoreKey):
            report_number(record, key, value)

    def test_spsc_i_matches_letter(self):
        """The example from the request: SPSC-I-170 -> publication-letter."""
        record = {}
        self._call(record, "088__", {"a": "SPSC-I-170"})
        assert record["resource_type"] == {"id": "publication-letter"}

    def test_real_record_spslc_p_282_matches_proposal(self):
        """https://cds.cern.ch/record/493774 has 088__a "SPSLC-P-282"."""
        record = {}
        self._call(record, "088__", {"a": "SPSLC-P-282"})
        assert record["resource_type"] == {"id": "publication-proposal"}

    def test_real_record_292374_drdc_p_matches_proposal(self):
        """https://cds.cern.ch/record/292374 has 088__a "DRDC-P-2" (plus
        the unrelated plain report number "CERN-DRDC-90-25")."""
        record = {}
        self._call(record, "088__", {"a": "DRDC-P-2"})
        assert record["resource_type"] == {"id": "publication-proposal"}

    def test_real_record_291072_status_report_matches_report(self):
        """https://cds.cern.ch/record/291072 has 088__a
        "DRDC-Status-report-RD-30": a two-token type ("Status-report")
        followed by a number that itself contains a hyphen ("RD-30")."""
        record = {}
        self._call(record, "088__", {"a": "DRDC-Status-report-RD-30"})
        assert record["resource_type"] == {"id": "publication-report"}

    def test_spsc_m_is_memorandum(self):
        """M is SPSC-specific: publication-memorandum, not meeting minutes."""
        record = {}
        self._call(record, "088__", {"a": "SPSC-M-12"})
        assert record["resource_type"] == {"id": "publication-memorandum"}

    def test_non_spsc_m_is_meeting_minutes(self):
        """M defaults to publication-meetingminutes for other committees."""
        record = {}
        self._call(record, "088__", {"a": "TCC-M-12"})
        assert record["resource_type"] == {"id": "publication-meetingminutes"}

    def test_type_a_is_meeting_agenda(self):
        record = {}
        self._call(record, "088__", {"a": "TCC-A-3"})
        assert record["resource_type"] == {"id": "publication-meetingagenda"}

    def test_type_g_is_other(self):
        record = {}
        self._call(record, "088__", {"a": "TCC-G-3"})
        assert record["resource_type"] == {"id": "publication-other"}

    def test_type_p_is_proposal(self):
        record = {}
        self._call(record, "088__", {"a": "TCC-P-3"})
        assert record["resource_type"] == {"id": "publication-proposal"}

    def test_type_t_is_technical_note(self):
        record = {}
        self._call(record, "088__", {"a": "TCC-T-3"})
        assert record["resource_type"] == {"id": "publication-technicalnote"}

    def test_type_tdr_is_report(self):
        record = {}
        self._call(record, "088__", {"a": "TCC-TDR-3"})
        assert record["resource_type"] == {"id": "publication-report"}

    def test_type_sr_is_report(self):
        record = {}
        self._call(record, "088__", {"a": "TCC-SR-3"})
        assert record["resource_type"] == {"id": "publication-report"}

    def test_type_ug_is_report_with_upgrade_cost_group_subject(self):
        record = {}
        self._call(record, "088__", {"a": "TCC-UG-3"})
        assert record["resource_type"] == {"id": "publication-report"}
        assert record["subjects"] == [{"subject": "collection:upgrade cost group"}]

    def test_spsc_r_is_report_with_recommendation_subject(self):
        """R is only defined for SPSC: publication-report + subject."""
        record = {}
        self._call(record, "088__", {"a": "SPSC-R-3"})
        assert record["resource_type"] == {"id": "publication-report"}
        assert record["subjects"] == [{"subject": "recommendation"}]

    def test_non_spsc_r_is_not_mapped(self):
        """R has no default mapping outside SPSC - left untouched."""
        record = {}
        self._call(record, "088__", {"a": "TCC-R-3"})
        assert "resource_type" not in record
        assert "subjects" not in record

    def test_unknown_committee_prefix_not_matched(self):
        record = {}
        self._call(record, "088__", {"a": "XYZ-M-5"})
        assert "resource_type" not in record

    def test_unknown_type_code_not_matched(self):
        record = {}
        self._call(record, "088__", {"a": "SPSC-Z-5"})
        assert "resource_type" not in record

    def test_generic_cern_report_number_not_matched(self):
        """ "CERN-SPSLC-94-025" (from record 493774) is a plain CERN report
        number, not a <COMMITTEE>-<TYPE>-<NUMBER> code - must not match."""
        record = {}
        self._call(record, "088__", {"a": "CERN-SPSLC-94-025"})
        assert "resource_type" not in record

    def test_037_field_also_detected(self):
        """The pattern is checked on both 037__ and 088__."""
        record = {}
        self._call(record, "037__", {"a": "SPSC-I-170"})
        assert record["resource_type"] == {"id": "publication-letter"}

    def test_match_sets_rank_below_any_980_priority(self):
        """The rank sentinel set alongside resource_type must outrank
        anything the generic 980__ rule (research.py:resource_type) can
        produce, so a committee-report-derived type is never clobbered by
        it - see TestResearchCommitteeModelIntegration for the full
        end-to-end behaviour."""
        record = {}
        self._call(record, "088__", {"a": "SPSC-I-170"})
        assert record["_resource_type_rank"] == _RANK_REPORT_NUMBER


class TestGenericReportNumberType:
    """Test detection of a type token anywhere in a report number that
    doesn't follow the <COMMITTEE>-<TYPE>-<NUMBER> convention - CERN's
    generic CERN-<DEPT>-<TYPE>-<NUMBER> department report numbering, e.g.
    "CERN-NP-MEMO-7840"."""

    def _call(self, record, key, value):
        with pytest.raises(IgnoreKey):
            report_number(record, key, value)

    def test_real_example_cern_np_memo(self):
        record = {}
        self._call(record, "088__", {"a": "CERN-NP-MEMO-7840"})
        assert record["resource_type"] == {"id": "publication-memorandum"}
        assert record["_resource_type_rank"] == _RANK_REPORT_NUMBER

    def test_tdr_token_matched(self):
        record = {}
        self._call(record, "088__", {"a": "CERN-LHC-TDR-2020"})
        assert record["resource_type"] == {"id": "publication-report"}

    def test_status_report_token_matched(self):
        """The two-token type is matched here too, not just when anchored
        to a known committee - see TestCommitteeReportNumberType's
        DRDC-Status-report-RD-30 case."""
        record = {}
        self._call(record, "088__", {"a": "CERN-EP-STATUS-REPORT-99"})
        assert record["resource_type"] == {"id": "publication-report"}

    def test_single_letter_codes_not_matched_unanchored(self):
        """M, I, A, G, P, T (and N, S) are only trusted right after a known
        committee prefix (see TestCommitteeReportNumberType) - unanchored,
        they're too likely to be a coincidental match."""
        record = {}
        self._call(record, "088__", {"a": "CERN-EP-T-123"})
        assert "resource_type" not in record

    def test_unrelated_report_number_not_matched(self):
        record = {}
        self._call(record, "088__", {"a": "CERN-EP-2020-042"})
        assert "resource_type" not in record

    def test_committee_anchored_match_still_takes_precedence(self):
        """Both detectors run on every 037__/088__ occurrence, but they
        agree here since MEMO is a token in both - just confirming the
        two don't conflict for a genuine committee report number."""
        record = {}
        self._call(record, "088__", {"a": "SPSC-M-12"})
        assert record["resource_type"] == {"id": "publication-memorandum"}

    def test_does_not_override_existing_series_match(self):
        """490__ is processed after 037__/088__, but a report-number match
        (committee-anchored or generic) shares the top priority tier - see
        TestSeriesResourceType.test_does_not_override_existing_resource_type
        for the reverse ordering check."""
        record = {}
        self._call(record, "088__", {"a": "CERN-NP-MEMO-7840"})
        series_information(record, "490__", {"a": "Proposal"})
        assert record["resource_type"] == {"id": "publication-memorandum"}


class TestSeriesResourceType:
    """Test resource_type derived from 490__$a free text (e.g.
    "Memorandum", "Proposal", "Letter of Intent") for research committee
    records that don't carry a <COMMITTEE>-<TYPE>-<NUMBER> report number."""

    def test_memorandum(self):
        record = {}
        result = series_information(record, "490__", {"a": "Memorandum"})
        assert record["resource_type"] == {"id": "publication-memorandum"}
        assert record["_resource_type_rank"] == _RANK_SERIES
        assert result == [
            {"description": "Memorandum", "type": {"id": "series-information"}}
        ]

    def test_proposal(self):
        record = {}
        series_information(record, "490__", {"a": "Proposal"})
        assert record["resource_type"] == {"id": "publication-proposal"}

    def test_letter_of_intent(self):
        record = {}
        series_information(record, "490__", {"a": "Letter of Intent"})
        assert record["resource_type"] == {"id": "publication-letter"}

    def test_case_insensitive(self):
        record = {}
        series_information(record, "490__", {"a": "PROPOSAL"})
        assert record["resource_type"] == {"id": "publication-proposal"}

    def test_unknown_phrase_not_matched(self):
        record = {}
        series_information(record, "490__", {"a": "Yellow Report Series"})
        assert "resource_type" not in record

    def test_base_series_information_behaviour_preserved(self):
        """additional_descriptions is still populated exactly like the
        generic 490__ rule (base.series_information)."""
        record = {}
        result = series_information(record, "490__", {"a": "Proposal", "v": "12"})
        assert result == [
            {"description": "Proposal (12)", "type": {"id": "series-information"}}
        ]

    def test_does_not_override_existing_resource_type(self):
        """037__/088__ is processed before 490__ (fields are visited in tag
        order - see CdsOverdo.do), so a committee report number match must
        win over conflicting 490__ free text."""
        record = {}
        with pytest.raises(IgnoreKey):
            report_number(record, "088__", {"a": "DRDC-P-2"})
        series_information(record, "490__", {"a": "Memorandum"})
        assert record["resource_type"] == {"id": "publication-proposal"}


class TestEditionResourceType:
    """Test resource_type derived from 250__$a free text (edition
    statement) for research committee records."""

    def _call(self, record, value):
        """imprint always raises IgnoreKey("imprint_info") - see
        research.imprint - it stores the edition on custom_fields itself
        rather than returning it. Swallow it here so each test can focus
        on the resource_type side effect."""
        with pytest.raises(IgnoreKey):
            imprint(record, "250__", value)

    def test_real_record_1005022_addendum_matches_other(self):
        """https://cds.cern.ch/record/1005022 has 250__a "Addendum"."""
        record = {}
        self._call(record, {"a": "Addendum"})
        assert record["resource_type"] == {"id": "publication-other"}
        assert record["_resource_type_rank"] == _RANK_SERIES

    def test_numbered_addendum_matched(self):
        """Unlike 490__ series, 250__ values are often followed by a
        number, e.g. "Addendum 1", "addendum 2" - an exact match wouldn't
        catch these."""
        for value in ("Addendum 1", "addendum 2"):
            record = {}
            self._call(record, {"a": value})
            assert record["resource_type"] == {"id": "publication-other"}, value

    def test_case_insensitive(self):
        record = {}
        self._call(record, {"a": "ADDENDUM"})
        assert record["resource_type"] == {"id": "publication-other"}

    def test_unknown_phrase_not_matched(self):
        record = {}
        self._call(record, {"a": "2nd ed."})
        assert "resource_type" not in record

    def test_base_imprint_behaviour_preserved(self):
        """custom_fields.imprint:imprint.edition is still populated exactly
        like the generic 250__ rule (research.imprint)."""
        record = {}
        self._call(record, {"a": "Addendum"})
        assert record["custom_fields"]["imprint:imprint"]["edition"] == "Addendum"

    def test_does_not_override_existing_report_number_match(self):
        """037__/088__ is processed before 250__, so a committee report
        number match must win over conflicting 250__ free text."""
        record = {}
        with pytest.raises(IgnoreKey):
            report_number(record, "088__", {"a": "DRDC-P-2"})
        self._call(record, {"a": "Addendum"})
        assert record["resource_type"] == {"id": "publication-proposal"}

    def test_wins_over_weaker_title_guess(self):
        """245__ is processed before 250__, but a title-derived guess is
        the weakest signal - a 250__ edition match must still override
        it."""
        record = {}
        title(record, "245__", {"a": "Proposal for a new experiment"})
        self._call(record, {"a": "Addendum"})
        assert record["resource_type"] == {"id": "publication-other"}


class TestTitleResourceType:
    """Test resource_type derived from a document type mentioned anywhere
    in the 245__ title (e.g. "Letter of Intent for ...", "Draft minutes of
    ...") for research committee records that carry neither a report
    number nor a 490__ series value."""

    def test_letter_of_intent(self):
        record = {}
        title(
            record,
            "245__",
            {"a": "Letter of Intent for a General Purpose Detector at LHC"},
        )
        assert record["resource_type"] == {"id": "publication-letter"}
        assert record["_resource_type_rank"] == _RANK_TITLE

    def test_status_report(self):
        record = {}
        title(record, "245__", {"a": "Status report on the readout electronics"})
        assert record["resource_type"] == {"id": "publication-report"}

    def test_real_record_1015008_draft_minutes_matches_meetingminutes(self):
        """https://cds.cern.ch/record/1015008 has 245__a "Draft minutes of
        the third meeting of the EEC held on 21 June, 1961" - "minutes"
        isn't the first word of the title."""
        record = {}
        title(
            record,
            "245__",
            {
                "a": "Draft minutes of the third meeting of the EEC held on 21 June, 1961"
            },
        )
        assert record["resource_type"] == {"id": "publication-meetingminutes"}

    def test_phrase_matches_anywhere_in_title(self):
        """The phrase doesn't need to open the title - it's matched
        wherever it appears."""
        record = {}
        title(
            record,
            "245__",
            {"a": "A study of memorandum handling in distributed systems"},
        )
        assert record["resource_type"] == {"id": "publication-memorandum"}

    def test_case_insensitive(self):
        record = {}
        title(record, "245__", {"a": "PROPOSAL for a new experiment"})
        assert record["resource_type"] == {"id": "publication-proposal"}

    def test_phrase_must_match_a_word_boundary(self):
        """ "Reported" must not match "report"."""
        record = {}
        title(record, "245__", {"a": "Reported observations of the detector"})
        assert "resource_type" not in record

    def test_unrelated_title_not_matched(self):
        record = {}
        title(record, "245__", {"a": "A study of fluoride crystals for LHC"})
        assert "resource_type" not in record

    def test_subtitle_245__b_also_checked(self):
        """A phrase in the subtitle (245__$b) must be detected the same
        way as in the title itself (245__$a)."""
        record = {}
        title(
            record,
            "245__",
            {"a": "On the readout electronics", "b": "Status report"},
        )
        assert record["resource_type"] == {"id": "publication-report"}

    def test_title_wins_over_conflicting_subtitle(self):
        """$a is checked before $b, so a match in the title takes priority
        over a conflicting match in the subtitle."""
        record = {}
        title(
            record,
            "245__",
            {"a": "Proposal for a new experiment", "b": "Draft minutes"},
        )
        assert record["resource_type"] == {"id": "publication-proposal"}

    def test_base_title_behaviour_preserved(self):
        """title is still populated exactly like the generic 245__ rule
        (base.title)."""
        record = {}
        result = title(record, "245__", {"a": "Proposal for a new experiment"})
        assert result == "Proposal for a new experiment"

    def test_series_still_overrides_weaker_title_guess(self):
        """245__ is processed before 490__ (fields are visited in tag order
        - see CdsOverdo.do), but a title-derived guess is the weakest of
        the three signals - a later, more reliable 490__ series match must
        still override it."""
        record = {}
        title(record, "245__", {"a": "Proposal for a new experiment"})
        series_information(record, "490__", {"a": "Memorandum"})
        assert record["resource_type"] == {"id": "publication-memorandum"}

    def test_does_not_override_existing_report_number_match(self):
        """037__/088__ is processed before 245__, so a committee report
        number match must win over a conflicting title guess."""
        record = {}
        with pytest.raises(IgnoreKey):
            report_number(record, "088__", {"a": "DRDC-P-2"})
        title(record, "245__", {"a": "Memorandum on the status"})
        assert record["resource_type"] == {"id": "publication-proposal"}


class TestCommitteeReportNumberBaseBehaviourPreserved:
    """The generic report_number (identifiers/related_identifiers) behaviour
    from base.py must be fully preserved for committee records."""

    def test_plain_report_number_goes_to_identifiers(self):
        record = {}
        with pytest.raises(IgnoreKey):
            report_number(record, "088__", {"a": "SPSLC-P-282"})
        assert record["identifiers"] == [
            {"scheme": "cdsrn", "identifier": "SPSLC-P-282"}
        ]

    def test_generic_cern_report_number_goes_to_identifiers(self):
        record = {}
        with pytest.raises(IgnoreKey):
            report_number(record, "088__", {"a": "CERN-SPSLC-94-025"})
        assert record["identifiers"] == [
            {"scheme": "cdsrn", "identifier": "CERN-SPSLC-94-025"}
        ]

    def test_arxiv_still_handled(self):
        """arXiv identifiers still go through the arXiv branch of the base
        rule and are excluded from `identifiers`."""
        record = {}
        with pytest.raises(IgnoreKey):
            report_number(record, "037__", {"a": "arXiv:1234.5678", "9": "arXiv"})
        assert record["related_identifiers"][0]["scheme"] == "arxiv"


class TestResearchCommitteeModelIntegration:
    """End-to-end test through research_comm_model.do(), reproducing the
    real record https://cds.cern.ch/record/493774 (SPSLC committee, 088__
    "CERN-SPSLC-94-025" and "SPSLC-P-282")."""

    def test_record_493774_resource_type_detected_alongside_committee(self):
        blob = GroupableOrderedDict(
            (
                (
                    "088__",
                    ({"a": "CERN-SPSLC-94-025"}, {"a": "SPSLC-P-282"}),
                ),
                ("980__", {"a": "SCICOMMPUBLSPSLC"}),
            )
        )
        out = research_comm_model.do(blob)

        assert out["resource_type"] == {"id": "publication-proposal"}
        assert out["custom_fields"]["cern:committees"] == [{"id": "SPSLC"}]
        assert {"scheme": "cdsrn", "identifier": "CERN-SPSLC-94-025"} in out[
            "identifiers"
        ]
        assert {"scheme": "cdsrn", "identifier": "SPSLC-P-282"} in out["identifiers"]

    def test_committee_report_type_not_clobbered_by_generic_980_type(self):
        """037__/088__ is processed before 980__ (fields are visited in tag
        order - see CdsOverdo.do). A generic 980__ document-type tag
        arriving after a committee report number must not override the
        more specific, report-number-derived resource_type."""
        blob = GroupableOrderedDict(
            (
                ("088__", {"a": "SPSC-I-170"}),
                ("980__", ({"a": "ARTICLE"}, {"a": "SCICOMMPUBLSPSC"})),
            )
        )
        out = research_comm_model.do(blob)

        assert out["resource_type"] == {"id": "publication-letter"}


class TestResourceTypeFinalizer:
    """`_resource_type` (inside RecordEntry._metadata) must strip
    the `_resource_type_rank` bookkeeping key and raise when no rule ever
    resolved a resource_type, rather than silently defaulting."""

    @pytest.fixture
    def entry(self):
        """RecordEntry double for calling _metadata() in isolation.

        Bypasses the constructor (which needs a real dump/dojson_entry) -
        this class only exercises _metadata() directly.
        """
        record_entry = RecordEntry.__new__(RecordEntry)
        record_entry.migration_logger = None
        record_entry.affiliations_mapping = None
        return record_entry

    def _raw_dump_entry(self):
        return {"files": []}

    def test_resource_type_passed_through(self, entry):
        dojson_entry = {
            "recid": 1,
            "resource_type": {"id": "publication-proposal"},
            "status_week_date": "1994-01-01",
            "_resource_type_rank": float("-inf"),
        }
        metadata = entry._metadata(dojson_entry, self._raw_dump_entry())
        assert metadata["resource_type"] == {"id": "publication-proposal"}

    def test_rank_scratch_key_removed_from_entry(self, entry):
        dojson_entry = {
            "recid": 1,
            "resource_type": {"id": "publication-proposal"},
            "status_week_date": "1994-01-01",
            "_resource_type_rank": float("-inf"),
        }
        entry._metadata(dojson_entry, self._raw_dump_entry())
        assert "_resource_type_rank" not in dojson_entry

    def test_raises_when_no_resource_type_resolved_at_all(self, entry):
        """A record for which no rule ever resolved a resource_type (and no
        committee-report-number type either) must raise, not silently fall
        back to publication-other."""
        dojson_entry = {
            "recid": 1,
            "status_week_date": "1994-01-01",
        }
        with pytest.raises(MissingRequiredField):
            entry._metadata(dojson_entry, self._raw_dump_entry())


class TestResearchCommitteeModelDoesNotDefaultResourceType:
    """ResearchCommitteeModel must not seed resource_type with a default -
    a record where no 980__/697C_ occurrence or committee report number
    resolves a real type should end up with no resource_type key at all,
    so it's caught as a missing required field downstream."""

    def test_committee_only_record_has_no_resource_type(self):
        """A record with only a committee tag (no resolvable document type)
        must not end up with resource_type=publication-other."""
        blob = GroupableOrderedDict((("980__", {"a": "SCICOMMPUBLSPSLC"}),))
        out = research_comm_model.do(blob)
        assert "resource_type" not in out
