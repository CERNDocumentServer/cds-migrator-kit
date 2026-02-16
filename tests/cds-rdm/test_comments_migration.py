# -*- coding: utf-8 -*-
#
# Copyright (C) 2026 CERN.
#
# CDS-RDM is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""Tests for comments migration workflow."""

import os
import tempfile

import pytest
from flask import current_app
from invenio_access.permissions import system_identity
from invenio_accounts.models import User
from invenio_rdm_records.proxies import current_rdm_records_service
from invenio_requests.proxies import current_events_service, current_requests_service

from cds_migrator_kit.base_minter import legacy as legacy_minter
from cds_migrator_kit.rdm.comments.runner import CommenterRunner, CommentsRunner
from cds_migrator_kit.rdm.comments.streams import (
    CommenterStreamDefinition,
    CommentsStreamDefinition,
)

LEGACY_RECD_ID_LIST = [12345, 23456, 34567, 45678]
"""
Legacy recid list to be used in the tests.
Testcases to be used in the tests:
    - 12345: No attached files, no comments related to any file: Normal case
    - 23456: With attached files: ManualImportRequired error is raised
    - 34567: Unknown user (not in users_metadata.json): No request is created
    - 45678: Deeply nested comments related to files: Normal case with flatted replies
(Also to show the errors before doesn't affect other testcases)
"""


@pytest.fixture
def temp_dir():
    """Create a temporary directory for test files."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield tmpdir


@pytest.fixture
def migrated_records_with_comments(test_app, community, uploader, db, add_pid):
    """Create a migrated RDM record that will have comments and a legacy PID."""
    legacy_recid_list = [12345, 23456, 34567, 45678]
    records_created = {}
    for legacy_recid in legacy_recid_list:
        minimal_record = {
            "metadata": {
                "title": f"Test Record with Comments {legacy_recid}",
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
        legacy_minter(legacy_recid, parent.pid.object_uuid)

        current_rdm_records_service.record_cls.index.refresh()

        records_created[legacy_recid] = record

    return records_created


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
    migrated_records_with_comments,
    community,
    db,
):
    """Test migrating comments from comments_metadata.json."""
    # Create users first (required for comments migration)
    user1 = User(email="submitter13@cern.ch", active=True)
    user2 = User(email="submitter10@gmail.com", active=True)
    # unknown@example.com won't be created for the unknown user testcase
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
    current_events_service.record_cls.index.refresh()

    # 12345: No attached files, no comments related to any file: Normal case
    record_id = migrated_records_with_comments[12345]["id"]
    request_result = current_requests_service.search(
        identity=system_identity,
        q=f'topic.record:"{record_id}"',
    )
    assert request_result.total == 1
    request = list(request_result.hits)[0]
    # Verify comments were created as request events
    comments_result = current_events_service.search(
        identity=system_identity,
        request_id=request["id"],
    )
    assert comments_result.total == 1
    comments = list(comments_result.hits)
    assert comments[0]["payload"]["content"] == "This is a test comment"
    assert "user" in comments[0]["created_by"]
    replies = comments[0]["children"]
    assert len(replies) == 2
    assert replies[0]["payload"]["content"] == "This is a reply"
    assert "user" in replies[0]["created_by"]
    assert replies[1]["payload"]["event"] == "comment_deleted"
    assert "user" in replies[1]["created_by"]

    # 23456: With attached files: ManualImportRequired error is raised
    record_id = migrated_records_with_comments[23456]["id"]
    request_result = current_requests_service.search(
        identity=system_identity,
        q=f'topic.record:"{record_id}"',
    )
    assert request_result.total == 0

    # 34567: Unknown user (not in users_metadata.json): No request is created
    record_id = migrated_records_with_comments[34567]["id"]
    request_result = current_requests_service.search(
        identity=system_identity,
        q=f'topic.record:"{record_id}"',
    )
    assert request_result.total == 0

    # 45678: Deeply nested comments related to files: Normal case with flatted replies
    record_id = migrated_records_with_comments[45678]["id"]
    request_result = current_requests_service.search(
        identity=system_identity,
        q=f'topic.record:"{record_id}"',
    )
    assert request_result.total == 1
    request = list(request_result.hits)[0]
    comments_result = current_events_service.search(
        identity=system_identity,
        request_id=request["id"],
    )
    assert comments_result.total == 1
    comments = list(comments_result.hits)
    deep_link = (
        current_app.config["CDS_MIGRATOR_KIT_SITE_UI_URL"]
        + "/records/"
        + record_id
        + "?preview_file=example.pdf"
    )
    deep_link_html = f"<p><a href='{deep_link}'>See related record version 1</a></p>"
    assert comments[0]["payload"]["content"] == (
        deep_link_html + "\n" + "This is a comment related to a file"
    )
    assert "user" in comments[0]["created_by"]
    replies = comments[0]["children"]
    assert len(replies) == 2
    assert replies[0]["payload"]["content"] == "This is a reply to a comment."
    assert "user" in replies[0]["created_by"]
    parent_comment_id = comments[0]["id"]
    mentioned_event_id = replies[0]["id"]
    deep_link = (
        current_app.config["CDS_MIGRATOR_KIT_SITE_UI_URL"]
        + f"/communities/{community.slug}/requests/{request['id']}#commentevent-{parent_comment_id}_{mentioned_event_id}"
    )
    deep_link_html = f"<p><a href='{deep_link}'>Link to the reply</a></p>"
    assert replies[1]["payload"]["content"] == (
        deep_link_html + "\n" + "This is a reply to a reply."
    )
    assert "user" in replies[1]["created_by"]


def test_migrate_comments_dry_run(temp_dir):
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
    request = current_requests_service.search(
        identity=system_identity,
        q="",
    )
    assert request.total == 2  # Already created ones in the non dry-run mode
