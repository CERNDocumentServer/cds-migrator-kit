# -*- coding: utf-8 -*-
#
# Copyright (C) 2026 CERN.
#
# CDS-RDM is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""Tests for comments migration workflow."""

import json
import os
import tempfile

import pytest
from invenio_access.permissions import system_identity
from invenio_accounts.models import User
from invenio_db.uow import UnitOfWork
from invenio_rdm_records.proxies import current_rdm_records_service
from invenio_rdm_records.records.api import RDMParent
from invenio_requests.proxies import current_events_service, current_requests_service

from cds_migrator_kit.rdm.comments.runner import CommenterRunner, CommentsRunner
from cds_migrator_kit.rdm.comments.streams import (
    CommenterStreamDefinition,
    CommentsStreamDefinition,
)

LEGACY_RECD_ID = "12345"


@pytest.fixture
def temp_dir():
    """Create a temporary directory for test files."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield tmpdir


@pytest.fixture
def migrated_record_with_comments(test_app, community, uploader, db, add_pid):
    """Create a migrated RDM record that will have comments and a legacy PID."""
    minimal_record = {
        "metadata": {
            "title": "Test Record with Comments",
            "publication_date": "2026-01-01",
            "resource_type": {"id": "publication-article"},
            "creators": [
                {
                    "person_or_org": {
                        "type": "personal",
                        "name": "Test Author",
                        "given_name": "Test",
                        "family_name": "Author",
                    }
                }
            ],
        },
        "access": {
            "record": "public",
            "files": "public",
        },
        "files": {
            "enabled": False,
        },
        "media_files": {
            "enabled": False,
        },
    }

    # Create the draft
    draft = current_rdm_records_service.create(
        system_identity,
        minimal_record,
    )

    # Publish the record
    record = current_rdm_records_service.publish(system_identity, draft.id)

    # Add to community
    parent = record._record.parent
    parent.communities.add(community)
    parent.communities.default = community
    parent.commit()

    # Add 'lrecid' legacy PID to the published record
    add_pid(
        pid_type="lrecid",
        pid_value=LEGACY_RECD_ID,
        object_uuid=parent.pid.object_uuid,
    )

    current_rdm_records_service.record_cls.index.refresh()

    return record


def test_create_users_from_metadata(
    temp_dir,
    db,
):
    """Test creating users from users_metadata.json."""
    # Run commenters runner
    log_dir = os.path.join(temp_dir, "logs")
    runner = CommenterRunner(
        stream_definition=CommenterStreamDefinition,
        filepath=os.path.join(
            os.path.dirname(__file__), "data", "users", "missing_users.json"
        ),
        missing_users_dir=os.path.join(os.path.dirname(__file__), "data", "users"),
        log_dir=log_dir,
        dry_run=False,
    )
    runner.run()

    # Verify users were created
    user1 = User.query.filter_by(email="submitter13@cern.ch").one_or_none()
    user2 = User.query.filter_by(email="submitter10@gmail.com").one_or_none()

    assert user1 is not None
    assert user2 is not None


def test_create_users_dry_run(
    temp_dir,
    db,
):
    """Test creating users in dry-run mode."""
    # Run commenters runner in dry-run mode
    log_dir = os.path.join(temp_dir, "logs")
    runner = CommenterRunner(
        stream_definition=CommenterStreamDefinition,
        filepath=os.path.join(
            os.path.dirname(__file__), "data", "users", "missing_users.json"
        ),
        missing_users_dir=os.path.join(os.path.dirname(__file__), "data", "users"),
        log_dir=log_dir,
        dry_run=True,
    )
    runner.run()

    # Verify users were NOT created in dry-run mode
    user1 = User.query.filter_by(email="submitter13@cern.ch").one_or_none()
    user2 = User.query.filter_by(email="submitter10@gmail.com").one_or_none()

    # In dry-run mode, users should not be created
    # For now, we just verify the runner completes without errors
    assert user1 is None
    assert user2 is None


def test_migrate_comments_from_metadata(
    temp_dir,
    migrated_record_with_comments,
    db,
):
    """Test migrating comments from comments_metadata.json."""
    # Create users first (required for comments migration)
    user1 = User(email="submitter13@cern.ch", active=True)
    user2 = User(email="submitter10@gmail.com", active=True)
    db.session.add(user1)
    db.session.add(user2)
    db.session.commit()

    # Create directory structure for attached files
    comments_dir = os.path.join(os.path.dirname(__file__), "data", "comments")
    os.makedirs(comments_dir, exist_ok=True)

    # Run comments runner
    log_dir = os.path.join(temp_dir, "logs")
    runner = CommentsRunner(
        stream_definition=CommentsStreamDefinition,
        filepath=os.path.join(
            os.path.dirname(__file__), "data", "comments", "comments_metadata.json"
        ),
        dirpath=comments_dir,
        log_dir=log_dir,
        dry_run=False,
    )
    runner.run()

    current_requests_service.record_cls.index.refresh()
    # Verify request was created
    request = current_requests_service.search(
        identity=system_identity,
        q=f"topic.record:{migrated_record_with_comments['id']}",
    )
    assert request.total == 1
    request = next(request.hits)

    current_events_service.record_cls.index.refresh()
    # Verify comments were created as request events
    comments = current_events_service.search(
        identity=system_identity,
        request_id=request["id"],
    )
    assert comments.total == 2  # 1 comment and 1 reply


def test_migrate_comments_dry_run(
    temp_dir,
    migrated_record_with_comments,
):
    """Test migrating comments in dry-run mode."""
    # Create directory structure for attached files
    comments_dir = os.path.join(os.path.dirname(__file__), "data", "comments")
    os.makedirs(comments_dir, exist_ok=True)

    # Run comments runner in dry-run mode
    log_dir = os.path.join(temp_dir, "logs")
    runner = CommentsRunner(
        stream_definition=CommentsStreamDefinition,
        filepath=os.path.join(
            os.path.dirname(__file__), "data", "comments", "comments_metadata.json"
        ),
        dirpath=comments_dir,
        log_dir=log_dir,
        dry_run=True,
    )
    runner.run()

    # In dry-run mode, request and comments should not be created
    # Verify the runner completes without errors
    current_requests_service.record_cls.index.refresh()
    request = current_requests_service.search(
        identity=system_identity,
        q=f"topic.record:{migrated_record_with_comments['id']}",
    )
    assert request.total == 0
