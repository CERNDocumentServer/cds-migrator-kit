# -*- coding: utf-8 -*-
#
# Copyright (C) 2022-2026 CERN.
#
# CDS-RDM is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""The full ETL entry yielded by ``CDSToRDMRecordTransform.run()``."""
from typing import Any, Dict, List, TypedDict

from cds_migrator_kit.rdm.records.transform.entities.parent import RecordParent
from cds_migrator_kit.rdm.records.transform.entities.record import RecordEntryData
from cds_migrator_kit.rdm.records.transform.entities.request import RecordRequest
from cds_migrator_kit.rdm.records.transform.entities.version import VersionEntry


class MigrationEntry(TypedDict):
    """The full ETL entry yielded by ``CDSToRDMRecordTransform.run()``.

    Consumed by ``CDSRecordServiceLoad``/``ep_approval_entry.py``. Keys
    outside ``"record"`` are ETL-envelope-scoped (about this migration run,
    not about the record's own content): ``versions``/``parent`` are
    computed by ``CDSToRDMRecordTransform`` itself, while
    ``_original_dump``/``_clc_sync``/``_request_data``/``ep_approval`` are
    carried alongside it - see ``CDSToRDMRecordTransform._transform()``.
    None of the four need ``RecordEntry`` to build: ``_original_dump``
    and ``ep_approval`` are read straight off the raw harvested entry,
    which the transform already has; ``_clc_sync`` and ``_request_data``
    are popped off ``raw_json_entry`` in ``_transform()``, before
    ``RecordEntry.transform()`` is even called.

    ``parent`` is a real ``RecordParent`` object, not a dict - see
    ``entities/parent.py``.
    """

    record: RecordEntryData
    versions: Dict[int, VersionEntry]
    parent: RecordParent
    # Community-inclusion request for this record - a RecordRequest object,
    # not a plain dict; see entities/request.py.
    _request_data: RecordRequest
    # EP approval workflow entries for this record, if any (possibly []).
    ep_approval: List[dict]
    _original_dump: dict
    _clc_sync: Any
