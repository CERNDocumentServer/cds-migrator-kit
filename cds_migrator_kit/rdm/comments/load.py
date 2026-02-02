# -*- coding: utf-8 -*-
#
# Copyright (C) 2024 CERN.
#
# CDS-RDM is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""CDS-RDM migration load module."""

from flask import current_app

from invenio_rdm_migrator.load.base import Load

from cds_migrator_kit.rdm.comments.log import CommentsLogger
from cds_rdm.legacy.resolver import get_pid_by_legacy_recid
from invenio_access.permissions import system_identity
from invenio_requests.proxies import current_requests_service
from invenio_rdm_records.proxies import current_rdm_records_service
from invenio_rdm_records.records.api import RDMParent
from invenio_requests.customizations.event_types import CommentEventType
from invenio_requests.proxies import current_events_service
from invenio_requests.resolvers.registry import ResolverRegistry
from invenio_accounts.models import User
from invenio_db import db
from datetime import datetime

from invenio_requests.customizations.request_types import CommunitySubmission
from invenio_requests.customizations.event_types import LogEventType

logger = CommentsLogger.get_logger()


class CDSCommentsLoad(Load):
    """CDSCommentsLoad."""

    LEGACY_REPLY_LINK_MAP = {}
    """Map of legacy reply ids to RDM comment ids."""

    def __init__(
        self,
        config,
        less_than_date,
        dry_run=False,
    ):
        """Constructor."""
        self.config = config
        self.less_than_date = less_than_date
        self.dry_run = dry_run

    def get_oldest_record(self, parent_pid_value):
        latest_record = current_rdm_records_service.read_latest(
            identity=system_identity, id_=parent_pid_value
        )
        search_result = current_rdm_records_service.scan_versions(
            identity=system_identity,
            id_=latest_record["id"],
        )
        record_versions = {hit["versions"]["index"]: hit for hit in search_result}
        oldest_version = min(record_versions.keys())
        return record_versions[oldest_version]


    def create_event(self, request, data, community, record, parent_comment_id=None):
        if not parent_comment_id:
            print("Creating event for record: ", record['id'], "request: ", request.id, "comment ID: ", data.get("comment_id"))
        else:
            print("Creating reply event for record: ", record['id'], "request: ", request.id, "comment ID: ", data.get("comment_id"), "parent comment ID: ", parent_comment_id)
        # TODO: Only add commment if the version id matches. To be finalized in discussion
        # if data.get("version") != record['versions']['index']:
        #     return

        # Create comment event
        comment_payload = {
            "payload": {
                "content": data.get("content"),
                "format": "html",
            }
        }

        comment_status = data.get("status")
        if comment_status == "da":
            comment_payload["payload"].update({"content": "comment was deleted by the author.", "event_type": "comment_deleted"})
        elif comment_status == "dm":
            comment_payload["payload"].update({"content": "comment was deleted by the moderator.", "event_type": "comment_deleted"})

        event = current_events_service.record_cls.create({}, request=request.model, request_id=str(request.id), type=CommentEventType)

        if parent_comment_id:
            # If it's a reply, 1. set parent comment id, 2. and if a nested reply, in the content add mentioned reply event's deep link
            event.parent_id = str(parent_comment_id)
            mentioned_event_id = self.LEGACY_REPLY_LINK_MAP.get(data.get("reply_to_id"), None)
            if mentioned_event_id:
                deep_link = f"<p><a href='{current_app.config['CDS_MIGRATOR_KIT_SITE_UI_URL']}/communities/{community.slug}/requests/{request.id}#commentevent-{parent_comment_id}_{mentioned_event_id}'>Link to the reply</a></p>"
                comment_payload["payload"]["content"] = deep_link + "\n" + comment_payload["payload"]["content"]

        # TODO: Add attached files to the event
        for attached_file in data.get("attached_files", []):
            print("TODO: Add attached files to the event: ", attached_file)
        # if data.get("attached_files"):
        #     comment_payload["payload"]["files"] = data.get("attached_files")

        event.update(comment_payload)

        user = User.query.filter_by(email=data.get("created_by")).one_or_none()
        # TODO: It shouldn't be not found, if so, raise manual migration error?
        if user:
            event.created_by = ResolverRegistry.resolve_entity_proxy({"user": str(user.id)}, raise_=True)
        else:
            print("User not found for email: ", data.get("created_by"))
            event.created_by = ResolverRegistry.resolve_entity_proxy({"user": "system"}, raise_=True)

        event.model.created = data.get("created_at") # Not changing the updated at because of the context of migration
        event.model.version_id = 0

        event.commit()
        db.session.commit()

        current_events_service.indexer.index(event)
        return event


    def create_accepted_community_inclusion_request(
        self,
        record,
        community,
        creator_user_id,
        comments=None, # TODO: Should an accepted request be created if there are no comments?
    ):
        # Resolve entities for references
        creator_ref = ResolverRegistry.reference_entity({"user": str(creator_user_id)}, raise_=True)
        receiver_ref = ResolverRegistry.reference_entity({"community": str(community.id)}, raise_=True)
        topic_ref = ResolverRegistry.reference_entity({"record": record['id']}, raise_=True)

        request_item = current_requests_service.create(
            system_identity,
            data={
                "title": record['metadata']["title"],
            },
            request_type=CommunitySubmission,
            receiver=receiver_ref,
            creator=creator_ref,
            topic=topic_ref,
            uow=None,
        )
        request = request_item._record
        request.status = "accepted"
        created_at = datetime.fromisoformat(record['created'])
        request.model.created = created_at
        # request.model.updated = created_at # Not changing the updated to keep the context of migration

        request.commit()
        db.session.commit()

        current_requests_service.indexer.index(request)

        for comment_data in comments:
            comment_event = self.create_event(request, comment_data, community, record)
            for reply in comment_data.get("replies", []):
                reply_event = self.create_event(request, reply, community, record, comment_event.id)
                self.LEGACY_REPLY_LINK_MAP[reply.get("comment_id")] = reply_event.id

        # Add accepted log action
        event = LogEventType(payload=dict(event="accepted"))
        _data = dict(payload=event.payload)
        log_event = current_events_service.create(
            system_identity, request.id, _data, event, uow=None
        )
        # TODO: What should be the created for the log event, record's or last comment's? To be finalized in discussion
        log_event._record.model.created = created_at

        log_event._record.commit()
        db.session.commit()

        current_events_service.indexer.index(log_event._record)

        return request

    def _process_legacy_comments_for_recid(self, recid, comments):
        """Process the legacy comments for the record."""
        logger.info(f"Processing legacy comments for recid: {recid}")
        parent_pid = get_pid_by_legacy_recid(recid)
        oldest_record = self.get_oldest_record(parent_pid.pid_value)
        parent = RDMParent.pid.resolve(parent_pid.pid_value)
        community = parent.communities.default
        record_owner_id = parent.access.owned_by.owner_id
        request = self.create_accepted_community_inclusion_request(oldest_record, community, record_owner_id, comments)
        return request

    def _load(self, entry):
        """Use the services to load the entries."""
        if entry:
            recid, comments = entry
            try:
                self._process_legacy_comments_for_recid(recid, comments)
            except Exception as ex:
                logger.error(ex)

    def _cleanup(self, *args, **kwargs):
        """Cleanup the entries."""
        pass
