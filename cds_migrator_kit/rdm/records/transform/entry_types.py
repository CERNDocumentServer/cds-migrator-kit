# -*- coding: utf-8 -*-
#
# Copyright (C) 2022-2026 CERN.
#
# CDS-RDM is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""Typed shapes for the migration ETL entry.

``TypedDict`` values are plain ``dict``s at runtime (no behavior change,
nothing enforced), so these exist purely to make the envelope shapes
produced by ``transform.py`` and consumed by ``load.py``/
``ep_approval_entry.py`` visible at the definition site, instead of having
to be reconstructed by grepping across those files.

Deliberately NOT modeled here: the RDM record body itself
(``RecordEntry["json"]["metadata"|"pids"|"files"|"custom_fields"]``) - that
shape is governed by invenio_rdm_records' own record schema, not by this
package, and is already comparatively well documented by the field mappers
in ``mappers/``.
"""
from pathlib import Path
from typing import Any, Dict, List, Optional, TypedDict, Union

from arrow import Arrow


class RecordJsonOutputRequired(TypedDict):
    """Required keys of ``RecordEntry["json"]``."""

    files: dict
    pids: dict
    metadata: dict
    access_grants: List[dict]


class RecordJsonOutput(RecordJsonOutputRequired, total=False):
    """The RDM record body: ``RecordEntry["json"]``.

    ``current_rdm_records_service.create()``'s ``data`` argument - built by
    ``CDSToRDMRecordEntry.transform()``. Deliberately excludes ``access``:
    that's set per-version, after creation, via ``VersionEntry["access"]``
    (see ``load.py::_load_record_access``) rather than at record-create time.
    """

    custom_fields: dict
    internal_notes: Any


class RecordEntry(TypedDict):
    """A single record's content - ``MigrationEntry["record"]``.

    Built by ``CDSToRDMRecordEntry.transform()``. Everything here is
    record-scoped (as opposed to ``MigrationEntry``'s other top-level keys,
    which are ETL-envelope-scoped - see that type's docstring).
    """

    created: str
    updated: str
    version_id: int
    index: int
    recid: str
    communities: List[str]
    json: RecordJsonOutput
    # None when RecordFlaggedCuration was raised and caught - see
    # CDSToRDMRecordEntry._access()/.transform().
    access_status: Optional[str]
    owned_by: Union[str, int]
    # Community-inclusion request payload for this record, if any.
    _request_data: Optional[dict]
    # EP approval workflow entries for this record, if any (possibly []).
    ep_approval: List[dict]


class ParentAccess(TypedDict):
    """``ParentEntry["json"]["access"]``."""

    owned_by: Dict[str, Union[str, int]]


class ParentCommunities(TypedDict, total=False):
    """``ParentEntry["json"]["communities"]``."""

    ids: List[str]
    default: str


class ParentJson(TypedDict):
    """``ParentEntry["json"]``."""

    id: str
    access: ParentAccess
    communities: ParentCommunities


class ParentEntry(TypedDict):
    """The parent record - ``MigrationEntry["parent"]``. Built by ``CDSToRDMRecordTransform._parent()``."""

    created: str
    updated: str
    version_id: int
    json: ParentJson


class VersionAccessObj(TypedDict):
    """``VersionAccess["access_obj"]`` - mirrors the RDM record access schema."""

    record: Optional[str]
    files: Optional[str]


class VersionAccess(TypedDict, total=False):
    """A version's access - ``VersionEntry["access"]``.

    Set directly on the record post-create via
    ``load.py::_load_record_access`` (``record.access = access_dict["access_obj"]``).
    """

    access_obj: VersionAccessObj
    # Raw legacy file-restriction status string, present only when an
    # individual file carried its own restriction - see
    # CDSToRDMRecordTransform._versions()::compute_access().
    meta: str


class VersionFileMetadata(TypedDict):
    """``VersionFileEntry["metadata"]``."""

    description: Optional[str]
    name: str
    status: str
    original_path: str
    comment: Optional[str]


class VersionFileEntry(TypedDict):
    """One file within ``VersionEntry["files"]``, keyed by its ``full_name``."""

    eos_tmp_path: Path
    id_bibdoc: int
    key: str
    metadata: VersionFileMetadata
    mimetype: str
    checksum: str
    version: int
    access: str
    type: str
    creation_date: str


class VersionEntry(TypedDict):
    """One record version - a value in ``MigrationEntry["versions"]``.

    Built by ``CDSToRDMRecordTransform._versions()``, keyed there by legacy
    file version number (int), starting at 1.
    """

    files: Dict[str, VersionFileEntry]
    # Arrow instance when derived from a file's creation date; a plain ISO
    # date string in the no-files fallback branch (copied straight from
    # RecordEntry["json"]["metadata"]["publication_date"]) - see
    # CDSToRDMRecordTransform._versions().
    publication_date: Union[Arrow, str]
    access: VersionAccess


class MigrationEntry(TypedDict):
    """The full ETL entry yielded by ``CDSToRDMRecordTransform.run()``.

    Consumed by ``CDSRecordServiceLoad``/``ep_approval_entry.py``. Keys
    outside ``"record"`` are ETL-envelope-scoped (about this migration run,
    not about the record's own content): ``versions``/``parent`` are
    computed by ``CDSToRDMRecordTransform`` itself, while
    ``_original_dump``/``_clc_sync`` are carried alongside it - see
    ``CDSToRDMRecordTransform._transform()``.
    """

    record: RecordEntry
    versions: Dict[int, VersionEntry]
    parent: ParentEntry
    _original_dump: dict
    _clc_sync: Any
