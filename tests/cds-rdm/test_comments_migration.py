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
import yaml
from flask import current_app
from invenio_access.permissions import system_identity
from invenio_accounts.models import User
from invenio_db.uow import UnitOfWork
from invenio_rdm_records.proxies import current_rdm_records_service
from invenio_rdm_records.requests import CommunitySubmission
from invenio_records_resources.services.uow import RecordCommitOp
from invenio_requests.proxies import current_events_service, current_requests_service
from invenio_search import current_search, current_search_client
from invenio_users_resources.records.api import UserAggregate

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
    - 12345: No attached files, no comments related to any file: Easy case
    - 23456: With attached files: Request is created with attached file
    - 34567: Unknown user (not in users_metadata.json): No request is created
    - 45678: Deeply nested comments related to files: Normal case with flatted replies
(Also to show the errors before doesn't affect other testcases)
"""

COMMENTS_DATA_DIR = os.path.join(os.path.dirname(__file__), "data", "comments")


def write_comments_stream_config(
    temp_dir,
    collection,
    dir_path=None,
    reviewers=None,
):
    """Write a temporary streams.yaml for comments collection config."""
    config_path = os.path.join(temp_dir, "streams.yaml")
    with open(config_path, "w") as fp:
        yaml.safe_dump(
            {
                "comments": {
                    collection: {
                        "dir_path": dir_path or COMMENTS_DATA_DIR,
                        "reviewers": (
                            reviewers
                            if reviewers is not None
                            else [{"group": "test-comments-reviewers"}]
                        ),
                    }
                }
            },
            fp,
        )
    return config_path


@pytest.fixture
def temp_dir():
    """Create a temporary directory for test files."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield tmpdir


@pytest.fixture
def migrated_records_with_comments(test_app, community, uploader, db, add_pid):
    """Create a migrated RDM record that will have comments and a legacy PID."""
    records_created = {}
    for legacy_recid in LEGACY_RECD_ID_LIST:
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


def test_migrate_comments_from_metadata(
    temp_dir,
    migrated_records_with_comments,
    community,
    groups,
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

    expected_reviewers = [{"group": "test-comments-reviewers"}]

    # Create directory structure for attached files
    os.makedirs(COMMENTS_DATA_DIR, exist_ok=True)

    # Run comments runner
    log_dir = os.path.join(temp_dir, "logs")
    runner = CommentsRunner(
        stream_definition=CommentsStreamDefinition,
        config_filepath=write_comments_stream_config(temp_dir, "test-comments"),
        collection="test-comments",
        log_dir=log_dir,
        dry_run=False,
    )
    runner.run()

    current_requests_service.record_cls.index.refresh()
    current_events_service.record_cls.index.refresh()

    # 12345: No attached files, no comments related to any file: Easy case
    record_id = migrated_records_with_comments[12345]["id"]
    request_result = current_requests_service.search(
        identity=system_identity,
        q=f'topic.record:"{record_id}"',
    )
    assert request_result.total == 1
    request = list(request_result.hits)[0]
    assert request["number"] == "lrecid:12345"
    assert request["status"] == "accepted"
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

    # Check the request has correct values in the database
    request_in_db = current_requests_service.read(system_identity, request["id"])
    assert request_in_db["number"] == "lrecid:12345"
    assert request_in_db["status"] == "accepted"
    assert (
        current_requests_service.record_cls.get_record(request["id"]).get("reviewers")
        == expected_reviewers
    )

    # 23456: With attached files
    record_id = migrated_records_with_comments[23456]["id"]
    request_result = current_requests_service.search(
        identity=system_identity,
        q=f'topic.record:"{record_id}"',
    )
    assert request_result.total == 1
    request = list(request_result.hits)[0]
    assert request["number"] == "lrecid:23456"
    assert request["files"]["enabled"] == True
    assert request["status"] == "accepted"
    comments_result = current_events_service.search(
        identity=system_identity,
        request_id=request["id"],
    )
    assert comments_result.total == 1
    comments = list(comments_result.hits)
    assert len(comments[0]["payload"]["files"]) == 1

    # Check the request has correct values in the database
    request_in_db = current_requests_service.read(system_identity, request["id"])
    assert request_in_db["number"] == "lrecid:23456"
    assert request_in_db["status"] == "accepted"
    assert (
        current_requests_service.record_cls.get_record(request["id"]).get("reviewers")
        == expected_reviewers
    )

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
    assert request["number"] == "lrecid:45678"
    assert request["status"] == "accepted"
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

    # Check the request has correct values in the database
    request_in_db = current_requests_service.read(system_identity, request["id"])
    assert request_in_db["number"] == "lrecid:45678"
    assert request_in_db["status"] == "accepted"

    # Now create the missing user to check idempotency of the migration runner
    user3 = User(email="unknown@example.com", active=True)
    db.session.add(user3)
    db.session.commit()

    # Run comments runner again
    runner.run()
    current_requests_service.record_cls.index.refresh()
    current_events_service.record_cls.index.refresh()

    # Verify the errored request is created now
    record_id = migrated_records_with_comments[34567]["id"]
    request_result = current_requests_service.search(
        identity=system_identity,
        q=f'topic.record:"{record_id}"',
    )
    assert request_result.total == 1
    request = list(request_result.hits)[0]
    assert request["number"] == "lrecid:34567"
    assert request["status"] == "accepted"
    comments_result = current_events_service.search(
        identity=system_identity,
        request_id=request["id"],
    )
    assert comments_result.total == 1
    comments = list(comments_result.hits)
    assert comments[0]["payload"]["content"] == "This is a comment with an unknown user"
    assert "user" in comments[0]["created_by"]

    # Check the request has correct values in the database
    request_in_db = current_requests_service.read(system_identity, request["id"])
    assert request_in_db["number"] == "lrecid:34567"
    assert request_in_db["status"] == "accepted"

    user_id = comments[0]["created_by"]["user"]
    user = User.query.filter_by(id=user_id).one_or_none()
    assert user.email == "unknown@example.com"

    # Verify the total requests are 4 (so no duplicates are created in the second run)
    request_result = current_requests_service.search(
        identity=system_identity,
        q="",
    )
    assert request_result.total == 4


def test_migrate_comments_dry_run(temp_dir, test_app, db):
    """Test migrating comments in dry-run mode."""
    # Create directory structure for attached files
    os.makedirs(COMMENTS_DATA_DIR, exist_ok=True)

    # Run comments runner in dry-run mode
    log_dir = os.path.join(temp_dir, "logs")
    runner = CommentsRunner(
        stream_definition=CommentsStreamDefinition,
        config_filepath=write_comments_stream_config(temp_dir, "test-comments"),
        collection="test-comments",
        log_dir=log_dir,
        dry_run=True,
    )
    before = current_requests_service.search(system_identity, q="").total
    runner.run()
    # In dry-run mode, request and comments should not be created
    # Verify the runner completes without errors
    after = current_requests_service.search(system_identity, q="").total
    assert after == before


def test_create_users_from_metadata(
    temp_dir,
    db,
):
    """Test creating users from users_metadata.json."""
    # Run commenters runner
    log_dir = os.path.join(temp_dir, "logs")
    runner = CommenterRunner(
        stream_definition=CommenterStreamDefinition,
        config_filepath=write_comments_stream_config(temp_dir, "test-comments"),
        collection="test-comments",
        log_dir=log_dir,
        dry_run=False,
    )
    runner.run()

    # Verify users were created
    user1 = User.query.filter_by(email="submitter13@cern.ch").one_or_none()
    user2 = User.query.filter_by(email="submitter10@gmail.com").one_or_none()

    assert user1 is not None
    assert user2 is not None

    # Check if users are indexed
    current_search.flush_and_refresh(UserAggregate.index._name)
    search_result = current_search_client.search(
        index=UserAggregate.index._name,
    )
    assert search_result["hits"]["total"]["value"] == 2


def test_create_users_dry_run(
    temp_dir,
    db,
):
    """Test creating users in dry-run mode."""
    # Run commenters runner in dry-run mode
    log_dir = os.path.join(temp_dir, "logs")
    runner = CommenterRunner(
        stream_definition=CommenterStreamDefinition,
        config_filepath=write_comments_stream_config(temp_dir, "test-comments"),
        collection="test-comments",
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


def test_migrate_comments_onto_existing_inclusion_request(
    temp_dir, migrated_records_with_comments, community, groups, db
):
    """Reuse an existing inclusion request and skip on re-run."""
    legacy_recid = 12345
    record = migrated_records_with_comments[legacy_recid]
    parent = record._record.parent
    expected_reviewers = [{"group": "test-comments-reviewers"}]

    with UnitOfWork() as uow:
        request_item = current_requests_service.create(
            system_identity,
            data={"title": record["metadata"]["title"]},
            request_type=CommunitySubmission,
            receiver={"community": str(community.id)},
            creator={"user": str(parent.access.owned_by.owner_id)},
            topic={"record": record.id},
            uow=uow,
        )
        inclusion = request_item._record
        inclusion.status = "accepted"
        inclusion.number = f"lrecid:{legacy_recid}"
        uow.register(
            RecordCommitOp(inclusion, indexer=current_requests_service.indexer)
        )
        uow.commit()

    db.session.add(User(email="submitter13@cern.ch", active=True))
    db.session.add(User(email="submitter10@gmail.com", active=True))
    db.session.commit()

    log_dir = os.path.join(temp_dir, "logs")
    runner = CommentsRunner(
        stream_definition=CommentsStreamDefinition,
        config_filepath=write_comments_stream_config(temp_dir, "test-comments"),
        collection="test-comments",
        log_dir=log_dir,
        dry_run=False,
    )
    runner.run()
    current_requests_service.record_cls.index.refresh()
    current_events_service.record_cls.index.refresh()

    request_in_db = current_requests_service.read(system_identity, inclusion.id)
    assert request_in_db["type"] == CommunitySubmission.type_id
    assert (
        current_requests_service.record_cls.get_record(inclusion.id).get("reviewers")
        == expected_reviewers
    )
    comments = [
        h
        for h in current_events_service.search(
            system_identity, request_id=str(inclusion.id)
        ).hits
        if h.get("type") == "C"
    ]
    assert len(comments) == 1

    # Re-run should not duplicate comments
    runner.run()
    current_events_service.record_cls.index.refresh()
    comments_after = [
        h
        for h in current_events_service.search(
            system_identity, request_id=str(inclusion.id)
        ).hits
        if h.get("type") == "C"
    ]
    assert len(comments_after) == 1


def test_migrate_comments_ep_targets_internal_version(
    temp_dir, test_app, community, db, add_pid
):
    """EP comments target the restricted record via source_internal_version."""
    legacy_recid = 77777
    payload = {
        "metadata": {
            "title": "EP record",
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
        "access": {"record": "public", "files": "public"},
        "files": {"enabled": False},
        "media_files": {"enabled": False},
    }

    def _publish(title, with_community):
        draft = current_rdm_records_service.create(
            system_identity,
            {**payload, "metadata": {**payload["metadata"], "title": title}},
        )
        record = current_rdm_records_service.publish(system_identity, draft.id)
        if with_community:
            parent = record._record.parent
            parent.communities.add(community)
            parent.communities.default = community
            parent.commit()
        return record

    restricted = _publish("EP restricted", True)
    public = _publish("EP public", False)
    legacy_minter(legacy_recid, public._record.parent.pid.object_uuid)
    public_parent = public._record.parent
    public_parent["permission_flags"] = {
        "committee_approval": {"source_internal_version": restricted["id"]}
    }
    public_parent.commit()
    db.session.add(User(email="ep.commenter@cern.ch", active=True))
    db.session.commit()
    current_rdm_records_service.record_cls.index.refresh()

    metadata_path = os.path.join(temp_dir, "comments_metadata.json")
    with open(metadata_path, "w") as fp:
        json.dump(
            {
                str(legacy_recid): [
                    {
                        "comment_id": 1,
                        "content": "EP comment",
                        "status": "ok",
                        "user_email": "ep.commenter@cern.ch",
                        "created_at": "2025-01-10 10:00:00",
                        "file_relation": {},
                        "replies": [],
                    }
                ]
            },
            fp,
        )

    CommentsRunner(
        stream_definition=CommentsStreamDefinition,
        config_filepath=write_comments_stream_config(
            temp_dir,
            "test-collection",
            dir_path=temp_dir,
        ),
        collection="test-collection",
        log_dir=os.path.join(temp_dir, "logs"),
        dry_run=False,
    ).run()
    current_requests_service.record_cls.index.refresh()

    assert (
        current_requests_service.search(
            system_identity, q=f'topic.record:"{restricted["id"]}"'
        ).total
        == 1
    )
    assert (
        current_requests_service.search(
            system_identity, q=f'topic.record:"{public["id"]}"'
        ).total
        == 0
    )
