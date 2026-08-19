# -*- coding: utf-8 -*-
#
# Copyright (C) 2026 CERN.
#
# CDS-RDM is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""Tests for the "already migrated" skip/community-add logic.

Covers ``CDSToRDMRecordTransform.should_skip``,
``_existing_record_is_restricted``, and the corresponding branches of
``run()`` -- all exercised as unit tests (external DB/service calls mocked)
to mirror the style of ``test_transform_versions.py``.
"""

from unittest.mock import MagicMock

import pytest
from sqlalchemy.exc import NoResultFound

from cds_migrator_kit.errors import ManualImportRequired
from cds_migrator_kit.rdm.records.transform.transform import (
    CDSToRDMRecordTransform,
)


@pytest.fixture
def transform(tmp_path):
    """Transform instance, mirroring test_transform_versions.py."""
    return CDSToRDMRecordTransform(
        files_dump_dir=tmp_path,
        missing_users=tmp_path,
        communities_ids=["community-a"],
        migration_logger=MagicMock(),
    )


@pytest.fixture
def app_context(base_app):
    """Push an app context, without DB/search, just to resolve service proxies.

    ``current_rdm_records_service`` et al. are Flask-app-bound LocalProxy
    objects: reading/patching them (even under mock) requires an active app
    context with the relevant extensions registered, but not a live DB or
    search connection -- so the lightweight, module-scoped ``base_app``
    (from pytest-invenio) is enough, avoiding the full ``test_app`` DB setup.
    """
    with base_app.app_context():
        yield base_app


# -- should_skip ----------------------------------------------------------


def test_should_skip_true_when_recid_in_migrated_set(transform):
    """Recid already registered as an lrecid PID is skipped."""
    transform._migrated_recids = {"123"}

    assert transform.should_skip({"recid": 123}) is True


def test_should_skip_false_when_recid_not_in_migrated_set(transform):
    """Recid not yet registered is not skipped."""
    transform._migrated_recids = {"456"}

    assert transform.should_skip({"recid": 123}) is False


# -- _existing_record_is_restricted ----------------------------------------


def _mock_read_latest(mocker, transform, access):
    """Patch current_rdm_records_service.read_latest to return given access.

    Uses ``new_callable=MagicMock`` explicitly: ``mock.patch`` auto-detects
    the LocalProxy target as async-like otherwise, wrapping it in an
    ``AsyncMock`` whose calls return coroutines instead of the configured
    return value.
    """
    service = mocker.patch(
        "cds_migrator_kit.rdm.records.transform.transform.current_rdm_records_service",
        new_callable=MagicMock,
    )
    service.read_latest.return_value = MagicMock(data={"access": access})
    return service


def test_existing_record_is_restricted_false_when_public(
    transform, mocker, app_context
):
    """A fully public record/files is not restricted."""
    _mock_read_latest(mocker, transform, {"record": "public", "files": "public"})

    assert transform._existing_record_is_restricted("42") is False


def test_existing_record_is_restricted_true_when_record_restricted(
    transform, mocker, app_context
):
    """A restricted record (metadata) is flagged, regardless of files."""
    _mock_read_latest(mocker, transform, {"record": "restricted", "files": "public"})

    assert transform._existing_record_is_restricted("42") is True


def test_existing_record_is_restricted_true_when_files_restricted(
    transform, mocker, app_context
):
    """Restricted files alone are enough to flag the record."""
    _mock_read_latest(mocker, transform, {"record": "public", "files": "restricted"})

    assert transform._existing_record_is_restricted("42") is True


# -- run(): already-migrated branch ----------------------------------------


def _patch_run_deps(mocker, parent_pid=None, restricted=False, record_id="record-99"):
    """Patch run()'s external collaborators for the should_skip branch."""
    get_pid = mocker.patch(
        "cds_migrator_kit.rdm.records.transform.transform.get_pid_by_legacy_recid"
    )
    if parent_pid is None:
        get_pid.side_effect = NoResultFound()
    else:
        get_pid.return_value = parent_pid

    mocker.patch.object(
        CDSToRDMRecordTransform,
        "_existing_record_is_restricted",
        return_value=restricted,
    )
    # read_latest resolves the parent-level recid (what get_pid_by_legacy_recid
    # returns) to the actual latest record, whose own id is what bulk_add needs.
    read_latest = mocker.patch(
        "cds_migrator_kit.rdm.records.transform.transform.current_rdm_records_service",
        new_callable=MagicMock,
    ).read_latest
    read_latest.return_value = MagicMock(id=record_id)
    bulk_add = mocker.patch(
        "cds_migrator_kit.rdm.records.transform.transform."
        "current_record_communities_service"
    ).bulk_add
    return get_pid, bulk_add


def test_run_logs_when_no_matching_rdm_record(transform, mocker, app_context):
    """No lrecid->recid PID found: logs info, doesn't touch communities."""
    transform._load_migrated_recids = MagicMock(return_value={"123"})
    _, bulk_add = _patch_run_deps(mocker, parent_pid=None)

    list(transform.run([{"recid": 123}]))

    bulk_add.assert_not_called()
    transform.migration_logger.add_log.assert_not_called()
    messages = [
        call.args[1]["message"]
        for call in transform.migration_logger.add_information.call_args_list
    ]
    assert any("Problem with PIDs" in m for m in messages)
    transform.migration_logger.finalise_record.assert_called_once_with(123)


def test_run_adds_existing_record_to_communities_when_not_restricted(
    transform, mocker, app_context
):
    """Should-skip + not restricted -> added to all communities."""
    transform._load_migrated_recids = MagicMock(return_value={"123"})
    parent_pid = MagicMock(pid_value="42")
    _, bulk_add = _patch_run_deps(mocker, parent_pid=parent_pid, restricted=False)

    list(transform.run([{"recid": 123}]))

    bulk_add.assert_called_once_with(mocker.ANY, "community-a", ["record-99"])
    transform.migration_logger.add_log.assert_not_called()
    transform.migration_logger.finalise_record.assert_called_once_with(123)


def test_run_skips_community_add_when_restricted(transform, mocker, app_context):
    """Should-skip + restricted -> ManualImportRequired logged, no community add."""
    transform._load_migrated_recids = MagicMock(return_value={"123"})
    parent_pid = MagicMock(pid_value="42")
    _, bulk_add = _patch_run_deps(mocker, parent_pid=parent_pid, restricted=True)

    list(transform.run([{"recid": 123}]))

    bulk_add.assert_not_called()
    transform.migration_logger.add_log.assert_called_once()
    exc = transform.migration_logger.add_log.call_args.args[0]
    assert isinstance(exc, ManualImportRequired)
    transform.migration_logger.finalise_record.assert_called_once_with(123)


def test_run_transforms_normally_when_not_already_migrated(
    transform, mocker, app_context
):
    """A record not in the migrated set goes through the normal transform path."""
    transform._load_migrated_recids = MagicMock(return_value=set())
    _, bulk_add = _patch_run_deps(mocker, parent_pid=MagicMock(pid_value="42"))
    mocker.patch.object(
        CDSToRDMRecordTransform, "_transform", return_value={"record": "ok"}
    )

    results = list(transform.run([{"recid": 999}]))

    assert results == [{"record": "ok"}]
    bulk_add.assert_not_called()
    transform.migration_logger.finalise_record.assert_not_called()
