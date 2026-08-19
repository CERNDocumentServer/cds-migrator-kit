# -*- coding: utf-8 -*-
#
# Copyright (C) 2026 CERN.
#
# CDS-RDM is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""CDS-RDM research committee rules."""

import re

from cds_migrator_kit.transform.xml_processing.quality.decorators import (
    for_each_value,
)
from cds_migrator_kit.transform.xml_processing.quality.parsers import StringValue

from ...models.research_committee import research_comm_model as model
from .base import report_number as _report_number_rule
from .base import series_information as _series_information_rule
from .base import title as _base_title
from .research import imprint as _imprint_rule

# Unwrapped base rules (strip @for_each_value): CdsOverdo.do() already
# invokes rules once per repeated-tag occurrence (one 037__/088__/250__/
# 490__ at a time), so re-wrapping the already-wrapped rule here would
# double-apply its bookkeeping. See research.py's own `_raw_licenses` for
# the same trick. `title` isn't decorated with @for_each_value, so it's
# used as-is.
_base_report_number = _report_number_rule.__wrapped__
_base_series_information = _series_information_rule.__wrapped__
_base_imprint = _imprint_rule.__wrapped__

# Committees whose report numbers follow the `<COMMITTEE>-<TYPE>-<NUMBER>`
# convention, e.g. "SPSC-I-170", "SPSLC-P-282" (see
# https://cds.cern.ch/record/493774). Keep in sync with the `committees`
# mapping in xml_processing/rules/research.py:resource_type.
REPORT_NUMBER_COMMITTEES = {
    "DRDC",
    "EEC",
    "EMC",
    "ISC",
    "ISRC",
    "ISTC",
    "LEPC",
    "NPRC",
    "NSC",
    "PSC",
    "PSCC",
    "SCC",
    "SPSC",
    "SPSLC",
    "TCC",
}

# <TYPE> segment -> resource_type, shared by every committee in
# REPORT_NUMBER_COMMITTEES unless overridden per-committee below. Most type
# segments are a single hyphen-separated token (e.g. "I" in "SPSC-I-170"),
# but some are two tokens (e.g. "Status-report" in
# "DRDC-Status-report-RD-30", https://cds.cern.ch/record/291072) - those are
# keyed with a "-" joining both tokens and checked first, see
# `_apply_committee_report_number`.
_DEFAULT_REPORT_TYPES = {
    "M": {"id": "publication-meetingminutes"},
    "N": {"id": "publication-meetingminutes"},
    "S": {"id": "publication-meetingminutes"},
    "I": {"id": "publication-letter"},
    "A": {"id": "publication-meetingagenda"},
    "G": {"id": "publication-other"},
    "P": {"id": "publication-proposal"},
    "T": {"id": "publication-technicalnote"},
    "TDR": {"id": "publication-report"},
    "UG": {"id": "publication-report"},
    "SR": {"id": "publication-report"},
    "RD": {"id": "publication-report"},
    "STATUS-REPORT": {"id": "publication-report"},
    "MEMO": {"id": "publication-memorandum"},
    "INTERNAL-REPORT":  {"id": "publication-report"},
}

# Longest type-token-sequence first, so "STATUS-REPORT" (2 tokens) is tried
# before a hypothetical single-token match on just "STATUS".
_TYPE_TOKEN_LENGTHS = sorted(
    {len(t.split("-")) for t in _DEFAULT_REPORT_TYPES}, reverse=True
)

# Per-committee overrides of `_DEFAULT_REPORT_TYPES`. "R" has no default
# mapping - it's only defined for SPSC.
_COMMITTEE_TYPE_OVERRIDES = {
    "SPSC": {
        "M": {"id": "publication-memorandum"},
        "R": {"id": "publication-report"},
    },
}

# Extra subject tagged onto the record for specific (committee, type) pairs.
# "*" matches any committee.
_TYPE_SUBJECTS = {
    ("SPSC", "R"): "recommendation",
    ("*", "UG"): "collection:upgrade cost group",
}


# 250__$a (edition), 490__$a (series) and 245__ (title/subtitle) sometimes
# spell out the document type as free text instead of, or in addition to,
# a <COMMITTEE>-<TYPE>-<NUMBER> report number, e.g. "Memorandum",
# "Proposal", "Letter of Intent", "Addendum" (https://cds.cern.ch/record/1005022
# has 250__$a "Addendum") - see `_apply_series_resource_type`,
# `_apply_edition_resource_type` and `_apply_title_resource_type`.
_SERIES_RESOURCE_TYPES = {
    "memorandum": {"id": "publication-memorandum"},
    "proposal": {"id": "publication-proposal"},
    "proposals": {"id": "publication-proposal"},
    "letter of intent": {"id": "publication-letter"},
    "letter": {"id": "publication-letter"},
    "letter of intention": {"id": "publication-letter"},
    "minutes": {"id": "publication-meetingminutes"},
    "agenda": {"id": "publication-meetingagenda"},
    "report": {"id": "publication-report"},
    "status report": {"id": "publication-report"},
    "progress report": {"id": "publication-report"},
    "addendum": {"id": "publication-other"},
    "decisions": {"id": "publication-meetingminutes"},
    "commentaires": {"id": "publication-peerreview"},
    "comments": {"id": "publication-peerreview"},
    "proposition": {"id": "publication-proposal"},
    "rapport": {"id": "publication-report"},
    "technical note": {"id": "publication-technicalnote"},
    "note": {"id": "publication-technicalnote"},
    "decision taken at the meeting": {"id": "publication-meetingminutes"}
}


# Priority tiers for the different ways a research-committee record's
# resource_type can be derived, most to least reliable/specific:
# <COMMITTEE>-<TYPE>-<NUMBER> report number (structured, unambiguous) > a
# document type spelled out in 250__$a edition or 490__$a series (both
# structured fields holding a short, free text value - equally reliable,
# so they share a tier) > a document type mentioned anywhere in the 245__
# title (free text, least reliable). Lower = higher priority, matching the
# rank semantics `research.py:resource_type` already uses for the generic
# 980__/697C_ rule - see `_set_resource_type_if_higher_priority`.
_RANK_REPORT_NUMBER = float("-inf")
_RANK_SERIES = -2
_RANK_TITLE = -1


def _set_resource_type_if_higher_priority(self, resource_type, rank):
    """Set `resource_type` (+ its `_resource_type_rank`), but only if
    `rank` outranks (is strictly lower than) whatever has already been
    decided for this record so far - by an earlier field in tag order, or
    by a higher-priority rule matching the same field.

    All three research-committee-specific resource_type sources (report
    number, series, title) and the generic 980__/697C_ rule
    (research.py:resource_type) read/write the same `_resource_type_rank`,
    so whichever ends up with the lowest rank wins regardless of which
    field it came from or when it was processed.
    """
    current_rank = self.get("_resource_type_rank")
    if current_rank is not None and rank >= current_rank:
        return
    self["resource_type"] = resource_type
    self["_resource_type_rank"] = rank


def _committee_report_type(committee, type_code):
    """Resolve resource_type + optional subject for a report number type code."""
    overrides = _COMMITTEE_TYPE_OVERRIDES.get(committee, {})
    resource_type = overrides.get(type_code) or _DEFAULT_REPORT_TYPES.get(type_code)
    if not resource_type:
        return None, None
    subject = _TYPE_SUBJECTS.get((committee, type_code)) or _TYPE_SUBJECTS.get(
        ("*", type_code)
    )
    return resource_type, subject


def _apply_committee_report_number(self, identifier):
    """Detect a `<COMMITTEE>-<TYPE>-<NUMBER>` report number (e.g.
    "SPSC-I-170") and derive the record's resource_type - and, for some
    types, an extra subject - from the type code.

    <TYPE> is usually one token ("I"), but can be two ("Status-report" in
    "DRDC-Status-report-RD-30" - the remaining "RD-30" is just part of the
    number, not the type). We don't know up front how many tokens the type
    takes, so we try the longest known type first (see
    `_TYPE_TOKEN_LENGTHS`) and fall back to shorter ones.

    This writes `resource_type` directly (rather than through a scratch key
    resolved later) so it's the single source of truth from the moment
    037__/088__ is processed - see `_set_resource_type_if_higher_priority`
    and `_RANK_REPORT_NUMBER` for how it's kept from being clobbered by the
    other resource_type sources that run later (490__, 245__, and the
    generic 980__ rule in research.py:resource_type).
    """
    if not identifier or identifier.count("-") < 2:
        return

    parts = identifier.split("-")
    committee = parts[0].upper()
    rest = [p.upper() for p in parts[1:]]

    if committee not in REPORT_NUMBER_COMMITTEES:
        return

    resource_type = subject = None
    for token_len in _TYPE_TOKEN_LENGTHS:
        if len(rest) <= token_len:
            # The type can't consume the whole identifier - at least one
            # token must be left over for the number.
            continue
        type_code = "-".join(rest[:token_len])
        resource_type, subject = _committee_report_type(committee, type_code)
        if resource_type:
            break

    if not resource_type:
        return

    _set_resource_type_if_higher_priority(self, resource_type, _RANK_REPORT_NUMBER)
    if subject:
        subjects = self.get("subjects", [])
        new_subject = {"subject": subject}
        if new_subject not in subjects:
            subjects.append(new_subject)
        self["subjects"] = subjects


# Type tokens from `_DEFAULT_REPORT_TYPES` safe to match anywhere in a
# report number, not just right after a known committee prefix - e.g.
# "CERN-NP-MEMO-7840" (CERN's generic CERN-<DEPT>-<TYPE>-<NUMBER> report
# numbering, not tied to a research committee at all, so
# `_apply_committee_report_number`'s REPORT_NUMBER_COMMITTEES gate doesn't
# apply). Deliberately excludes the single-letter codes (M, N, S, I, A, G,
# P, T): unanchored from a committee prefix, a bare one-letter token is far
# more likely to be a coincidental match (e.g. some unrelated department or
# series code) than an actual type marker - the longer tokens here don't
# have that problem.
_UNANCHORED_REPORT_TYPES = {
    token: resource_type
    for token, resource_type in _DEFAULT_REPORT_TYPES.items()
    if len(token) > 1
}
_UNANCHORED_TOKEN_LENGTHS = sorted(
    {len(t.split("-")) for t in _UNANCHORED_REPORT_TYPES}, reverse=True
)


def _apply_generic_report_number_type(self, identifier):
    """Detect one of `_UNANCHORED_REPORT_TYPES`'s tokens anywhere in a
    report number and derive the record's resource_type from it - see the
    comment above `_UNANCHORED_REPORT_TYPES` for why this is safe without a
    committee gate, unlike `_apply_committee_report_number`.

    Same rank as a committee report number (`_RANK_REPORT_NUMBER`): both
    are derived from a structured identifier rather than free text.
    """
    if not identifier:
        return

    tokens = identifier.upper().split("-")
    for start in range(len(tokens)):
        for token_len in _UNANCHORED_TOKEN_LENGTHS:
            end = start + token_len
            if end > len(tokens):
                continue
            candidate = "-".join(tokens[start:end])
            resource_type = _UNANCHORED_REPORT_TYPES.get(candidate)
            if resource_type:
                _set_resource_type_if_higher_priority(
                    self, resource_type, _RANK_REPORT_NUMBER
                )
                return


def _apply_series_resource_type(self, value_a):
    """Detect a document type spelled out in 490__$a (see
    `_SERIES_RESOURCE_TYPES`) and derive the record's resource_type from it.

    See `_set_resource_type_if_higher_priority` and `_RANK_SERIES` for how
    this is weighed against the other resource_type sources.
    """
    resource_type = _SERIES_RESOURCE_TYPES.get(value_a.strip().lower())
    if not resource_type:
        return

    _set_resource_type_if_higher_priority(self, resource_type, _RANK_SERIES)


def _free_text_resource_type(text):
    """Return the resource_type for text containing one of
    `_SERIES_RESOURCE_TYPES`'s phrases anywhere in it (e.g. "Draft minutes
    of the third meeting of the EEC ...",
    https://cds.cern.ch/record/1015008, or "Addendum 1"), matched at a word
    boundary so e.g. "Reported" doesn't match "report" - or None if it
    doesn't.
    """
    text_lower = text.strip().lower()
    # Longest phrase first, so "letter of intent" is tried before a
    # hypothetical single-word phrase it contains.
    for phrase in sorted(_SERIES_RESOURCE_TYPES, key=len, reverse=True):
        if re.search(rf"\b{re.escape(phrase)}\b", text_lower):
            return _SERIES_RESOURCE_TYPES[phrase]
    return None


def _apply_edition_resource_type(self, value_a):
    """Detect a document type spelled out in 250__$a (edition statement,
    e.g. "Addendum", "Addendum 1" - see `_free_text_resource_type`) and
    derive the record's resource_type from it. Matched the same way as the
    title (anywhere, word boundary) rather than 490__ series' exact match,
    since 250__ values are often followed by a number ("Addendum 2").

    See `_set_resource_type_if_higher_priority` and `_RANK_SERIES` for how
    this is weighed against the other resource_type sources - 250__ shares
    a priority tier with 490__ series, see the comment above
    `_SERIES_RESOURCE_TYPES`.
    """
    resource_type = _free_text_resource_type(value_a)
    if not resource_type:
        return

    _set_resource_type_if_higher_priority(self, resource_type, _RANK_SERIES)


def _apply_title_resource_type(self, title_value):
    """Detect a document type mentioned anywhere in the 245__ title (see
    `_free_text_resource_type`) and derive the record's resource_type from
    it.

    Titles are free text and the least reliable of the three
    research-committee-specific resource_type sources, so this only wins
    over an already-decided value if that value came from an even weaker
    source (i.e. nothing at all) - see `_set_resource_type_if_higher_priority`
    and `_RANK_TITLE`. In particular, a later 490__ series match (more
    reliable, see `_apply_series_resource_type`) can still override a
    title-derived guess even though 245__ is processed first (CdsOverdo.do
    visits fields in tag order).
    """
    resource_type = _free_text_resource_type(title_value)
    if not resource_type:
        return

    _set_resource_type_if_higher_priority(self, resource_type, _RANK_TITLE)


@model.over("related_identifiers", "(^037__)|(^088__)", override_tag=True)
@for_each_value
def report_number(self, key, value):
    """Translates report_number fields for research committee records.

    In addition to the generic report_number handling (see
    base.report_number), detects research-committee report numbers like
    "SPSC-I-170" and derives the record's resource_type from the type code
    segment (see `_apply_committee_report_number`), as well as CERN's
    generic CERN-<DEPT>-<TYPE>-<NUMBER> report numbering, e.g.
    "CERN-NP-MEMO-7840" (see `_apply_generic_report_number_type`).

    This re-registers (rather than adding a second, independent rule for
    037__/088__) because dojson dispatches a single rule per MARC tag - see
    CdsOverdo.do. Delegating every call to the original `report_number`
    (including letting its `IgnoreKey` propagate untouched) preserves 100%
    of its existing identifiers/related_identifiers behaviour; only
    research-committee records (routed to this model) are affected, every
    other collection keeps using the unmodified rule.
    """
    identifier = StringValue(value.get("a", "")).parse()
    _apply_committee_report_number(self, identifier)
    _apply_generic_report_number_type(self, identifier)

    return _base_report_number(self, key, value)


@model.over("additional_descriptions", "^490__", override_tag=True)
@for_each_value
def series_information(self, key, value):
    """Translates series information for research committee records.

    In addition to the generic series_information handling (see
    base.series_information), detects a document type spelled out in
    490__$a, e.g. "Memorandum" (see `_apply_series_resource_type`).

    Re-registers rather than adding a second, independent rule for 490__ -
    see `report_number`'s docstring for why.
    """
    _apply_series_resource_type(self, value.get("a", ""))

    return _base_series_information(self, key, value)


@model.over("title", "^245__", override_tag=True)
def title(self, key, value):
    """Translates title for research committee records.

    In addition to the generic title handling (see base.title), detects a
    document type mentioned anywhere in the title or subtitle, e.g. "Draft
    minutes of the third meeting of the EEC ..." (see
    `_apply_title_resource_type`).

    Re-registers rather than adding a second, independent rule for 245__ -
    see `report_number`'s docstring for why.
    """
    _apply_title_resource_type(self, value.get("a", ""))
    _apply_title_resource_type(self, value.get("b", ""))

    return _base_title(self, key, value)


@model.over("imprint_info", "(^250__)", override_tag=True)
@for_each_value
def imprint(self, key, value):
    """Translates edition statement for research committee records.

    In addition to the generic imprint/edition handling (see
    research.imprint), detects a document type spelled out in 250__$a,
    e.g. "Addendum" (see `_apply_edition_resource_type`).

    Re-registers rather than adding a second, independent rule for 250__ -
    see `report_number`'s docstring for why.
    """
    _apply_edition_resource_type(self, value.get("a", ""))

    return _base_imprint(self, key, value)
