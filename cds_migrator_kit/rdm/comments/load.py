# -*- coding: utf-8 -*-
#
# Copyright (C) 2024 CERN.
#
# CDS-RDM is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""CDS-RDM migration load module."""

from datetime import datetime

from cds_rdm.legacy.resolver import get_pid_by_legacy_recid
from flask import current_app, url_for
from invenio_access.permissions import system_identity
from invenio_accounts.models import User
from invenio_db import db
from invenio_rdm_migrator.load.base import Load
from invenio_rdm_records.proxies import current_rdm_records_service
from invenio_rdm_records.records.api import RDMParent
from invenio_rdm_records.requests import CommunitySubmission
from invenio_requests.customizations.event_types import CommentEventType, LogEventType
from invenio_requests.proxies import current_events_service, current_requests_service
from invenio_requests.records.api import RequestEventFormat
from invenio_requests.resolvers.registry import ResolverRegistry

from cds_migrator_kit.errors import ManualImportRequired
from cds_migrator_kit.rdm.comments.log import CommentsLogger

logger = CommentsLogger.get_logger()


class CDSCommentsLoad(Load):
    """CDSCommentsLoad."""

    LEGACY_REPLY_LINK_MAP = {}
    """Map of legacy reply ids to RDM comment ids."""

    def __init__(
        self,
        dirpath,
        dry_run=False,
    ):
        """Constructor."""
        self.dirpath = dirpath  # TODO: To be used later to load the attached files
        self.dry_run = dry_run
        self.all_record_versions = {}

    def get_oldest_record(self, parent_pid_value):
        latest_record = current_rdm_records_service.read_latest(
            identity=system_identity, id_=parent_pid_value
        )
        search_result = current_rdm_records_service.scan_versions(
            identity=system_identity,
            id_=latest_record["id"],
        )
        self.all_record_versions = {
            str(hit["versions"]["index"]): hit for hit in search_result
        }
        oldest_version = min(
            int(version) for version in self.all_record_versions.keys()
        )
        return self.all_record_versions[str(oldest_version)]

    def create_event(self, request, data, community, record, parent_comment_id=None):
        if not parent_comment_id:
            logger.info(
                "Creating event for record<{}> request<{}> comment<{}>".format(
                    record["id"],
                    request.id,
                    data.get("comment_id"),
                )
            )
        else:
            logger.info(
                "Creating reply event for record<{}> request<{}> comment<{}> parent_comment<{}>".format(
                    record["id"],
                    request.id,
                    data.get("comment_id"),
                    parent_comment_id,
                )
            )

        # Create comment event
        comment_payload = {
            "payload": {
                "content": data.get("content"),
                "format": RequestEventFormat.HTML.value,
            }
        }

        comment_status = data.get("status")
        event_type = CommentEventType
        if comment_status == "da":
            comment_payload["payload"].update(
                {
                    "content": "comment was deleted by the author.",
                    "event_type": "comment_deleted",
                }
            )
            event_type = LogEventType
        elif comment_status == "dm":
            comment_payload["payload"].update(
                {
                    "content": "comment was deleted by the moderator.",
                    "event_type": "comment_deleted",
                }
            )
            event_type = LogEventType

        event = current_events_service.record_cls.create(
            {}, request=request.model, request_id=str(request.id), type=event_type
        )

        if data.get("file_relation"):
            file_relation = data.get("file_relation")
            file_id = file_relation.get("file_id")
            version = file_relation.get("version")
            record_version = self.all_record_versions.get(str(version), None)
            if record_version:
                record_url = url_for(
                    "invenio_app_rdm_records.record_detail",
                    pid_value=record_version["id"],
                    preview_file=file_id,
                )
                version_link = f"<p><a href='{record_url}'>See related record version {version}</a></p>"
                comment_payload["payload"]["content"] = (
                    version_link + "\n" + comment_payload["payload"]["content"]
                )

        if parent_comment_id:
            logger.info(
                "Found parent event<{}> for reply event<{}>. Setting parent_id.".format(
                    event.id,
                    parent_comment_id,
                )
            )
            # If it's a reply, 1. set parent comment id, 2. and if a nested reply, in the content add mentioned reply event's deep link
            event.parent_id = str(parent_comment_id)
            mentioned_event_id = self.LEGACY_REPLY_LINK_MAP.get(
                data.get("reply_to_id"), None
            )
            if mentioned_event_id:
                logger.info(
                    "Adding deep link to the content for the deeply nested reply event<{}>.".format(
                        mentioned_event_id,
                        event.id,
                    )
                )
                deep_link = f"<p><a href='{current_app.config['CDS_MIGRATOR_KIT_SITE_UI_URL']}/communities/{community.slug}/requests/{request.id}#commentevent-{parent_comment_id}_{mentioned_event_id}'>Link to the reply</a></p>"
                comment_payload["payload"]["content"] = (
                    deep_link + "\n" + comment_payload["payload"]["content"]
                )

        # TODO: Add attached files to the event
        # https://github.com/CERNDocumentServer/cds-migrator-kit/issues/381

        event.update(comment_payload)

        user = User.query.filter_by(email=data.get("created_by")).one_or_none()
        if user:
            event.created_by = ResolverRegistry.resolve_entity_proxy(
                {"user": str(user.id)}, raise_=True
            )
        else:
            print("User not found for email: ", data.get("created_by"))
            raise ManualImportRequired(
                f"User not found for email: {data.get('created_by')}"
            )
        event.model.created = data.get("created_at")
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
        comments=None,
    ):
        """Create an accepted community inclusion request."""
        if not comments:
            logger.warning(
                f"No comments found for record<{record['id']}>. Skipping request creation."
            )
            return None

        # Resolve entities for references
        creator_ref = ResolverRegistry.reference_entity(
            {"user": str(creator_user_id)}, raise_=True
        )
        receiver_ref = ResolverRegistry.reference_entity(
            {"community": str(community.id)}, raise_=True
        )
        topic_ref = ResolverRegistry.reference_entity(
            {"record": record["id"]}, raise_=True
        )

        request_item = current_requests_service.create(
            system_identity,
            data={
                "title": record["metadata"]["title"],
            },
            request_type=CommunitySubmission,
            receiver=receiver_ref,
            creator=creator_ref,
            topic=topic_ref,
            uow=None,
        )
        request = request_item._record
        request.status = "accepted"
        created_at = datetime.fromisoformat(record["created"])
        request.model.created = created_at

        request.commit()
        db.session.commit()

        current_requests_service.indexer.index(request)
        logger.info(
            f"Created accepted community submission request<{request.id}> for record<{record['id']}>."
        )

        for comment_data in comments:
            comment_event = self.create_event(request, comment_data, community, record)
            for reply in comment_data.get("replies", []):
                reply_event = self.create_event(
                    request, reply, community, record, comment_event.id
                )
                self.LEGACY_REPLY_LINK_MAP[reply.get("comment_id")] = reply_event.id

        return request

    def _process_legacy_comments_for_recid(self, recid, comments):
        """Process the legacy comments for the record."""
        logger.info(f"Processing legacy comments for recid: {recid}")
        parent_pid = get_pid_by_legacy_recid(recid)
        oldest_record = self.get_oldest_record(parent_pid.pid_value)
        parent = RDMParent.pid.resolve(parent_pid.pid_value)
        community = parent.communities.default
        record_owner_id = parent.access.owned_by.owner_id
        request = self.create_accepted_community_inclusion_request(
            oldest_record, community, record_owner_id, comments
        )
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
