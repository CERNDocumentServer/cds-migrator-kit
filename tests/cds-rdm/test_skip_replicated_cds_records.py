# -*- coding: utf-8 -*-
#
# Copyright (C) 2026 CERN.
#
# CDS-RDM is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""Skip hand-submitted CDS duplicates on migrate (cds-rdm#347 examples)."""

from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import MagicMock

from invenio_access.permissions import system_identity
from invenio_pidstore.models import PersistentIdentifier
from invenio_rdm_records.proxies import current_rdm_records_service
from invenio_rdm_records.records import RDMRecord

from cds_migrator_kit.rdm.records.load.load import CDSMigrationEntryLoad

# Issue pairs covered: 2804864→22ksp-syb63 (arXiv DOI), 2884473→338f3-2c304
# (CERN DOI), 2944879→dy3wv-gp518 (arXiv related id).


def _entry(recid, body):
    return {"record": SimpleNamespace(recid=str(recid), body=body), "parent": None}


def _loader(**kwargs):
    return CDSMigrationEntryLoad(migration_logger=MagicMock(), **kwargs)


def _publish(minimal_record_with_files, **metadata):
    data = deepcopy(minimal_record_with_files)
    data["files"]["enabled"] = False
    data["media_files"] = {"enabled": False}
    data["metadata"].update(metadata)
    draft = current_rdm_records_service.create(system_identity, data)
    record = current_rdm_records_service.publish(system_identity, draft.id)
    RDMRecord.index.refresh()
    return record


def _has_lrecid(recid):
    return (
        PersistentIdentifier.query.filter_by(
            pid_type="lrecid", pid_value=str(recid)
        ).one_or_none()
        is not None
    )


def test_skip_by_cern_doi(running_app, location, minimal_record_with_files, db):
    """FCC 2884473 → 338f3-2c304: CERN DOI suffix matches version id."""
    record = _publish(minimal_record_with_files, title="FCC CERN DOI")
    entry = _entry(
        "2884473",
        {
            "metadata": {"identifiers": [], "related_identifiers": []},
            "pids": {"doi": {"identifier": f"10.17181/{record.id}"}},
        },
    )
    assert _loader()._should_skip_replicated_record(entry)
    assert _has_lrecid("2884473")


def test_skip_by_arxiv_doi(
    running_app, location, minimal_record_with_files, add_pid, db
):
    """FCC 2804864 → 22ksp-syb63: arXiv related id builds 10.48550/arXiv DOI."""
    record = _publish(minimal_record_with_files, title="FCC arXiv DOI")
    add_pid("doi", "10.48550/arXiv.2203.04312", record._record.id)
    entry = _entry(
        "2804864",
        {
            "metadata": {
                "identifiers": [],
                "related_identifiers": [
                    {"scheme": "arxiv", "identifier": "arXiv:2203.04312"}
                ],
            },
            "pids": {},
        },
    )
    assert _loader()._should_skip_replicated_record(entry)
    assert _has_lrecid("2804864")


def test_skip_by_arxiv_related(running_app, location, minimal_record_with_files, db):
    """NGT 2944879 → dy3wv-gp518: match via arXiv related identifier."""
    _publish(
        minimal_record_with_files,
        title="NGT arXiv",
        related_identifiers=[
            {
                "scheme": "arxiv",
                "identifier": "arXiv:2509.24371",
                "relation_type": {"id": "isvariantformof"},
            }
        ],
    )
    entry = _entry(
        "2944879",
        {
            "metadata": {
                "identifiers": [],
                "related_identifiers": [
                    {"scheme": "arxiv", "identifier": "arXiv:2509.24371"}
                ],
            },
            "pids": {},
        },
    )
    assert _loader()._should_skip_replicated_record(entry)
    assert _has_lrecid("2944879")


def test_dry_run_no_lrecid(running_app, location, minimal_record_with_files, db):
    """Dry run skips but does not mint lrecid."""
    record = _publish(minimal_record_with_files, title="dry-run")
    entry = _entry(
        "28844730",
        {
            "metadata": {"identifiers": [], "related_identifiers": []},
            "pids": {"doi": {"identifier": f"10.17181/{record.id}"}},
        },
    )
    assert _loader(dry_run=True)._should_skip_replicated_record(entry)
    assert not _has_lrecid("28844730")


def test_no_match(running_app, location, db):
    """Unknown ids are not skipped."""
    entry = _entry(
        "9999999",
        {
            "metadata": {
                "identifiers": [],
                "related_identifiers": [
                    {"scheme": "arxiv", "identifier": "arXiv:0000.00000"}
                ],
            },
            "pids": {},
        },
    )
    loader = _loader()
    assert loader._existing_cds_record(entry) is None
    assert not loader._should_skip_replicated_record(entry)
