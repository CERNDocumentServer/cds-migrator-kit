# -*- coding: utf-8 -*-
#
# Copyright (C) 2025 CERN.
#
# CDS-Videos is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""CDS-Videos migration load module."""

import json
import logging

from cds.modules.deposit.api import Project, Video, deposit_video_resolver
from cds.modules.flows.api import AVCFlowCeleryTasks
from cds.modules.flows.deposit import index_deposit_project
from cds.modules.flows.files import init_object_version
from cds.modules.flows.models import FlowMetadata
from cds.modules.flows.tasks import (
    ExtractMetadataTask,
)
from celery import chain as celery_chain
from invenio_db import db
from invenio_rdm_migrator.load.base import Load

from cds_migrator_kit.errors import ManualImportRequired
from cds_migrator_kit.reports.log import RDMJsonLogger

from .helpers import load_file_to_bucket, load_frames, load_subformats


class CDSVideosLoad(Load):
    """CDS-Videos Load."""

    def __init__(
        self,
        db_uri,
        data_dir,
        tmp_dir,
        entries=None,
        dry_run=False,
    ):
        """Constructor."""
        self.dry_run = dry_run

    def _prepare(self, entry):
        """Prepare the record."""
        pass

    def create_publish_single_video_record(self, entry):
        """Create and publish project and video for single main video record."""
        metadata = entry.get("record", {}).get("json", {}).get("metadata")
        # Create project
        # TODO `type` will be changed
        project_metadata = {"category": "CERN", "type": "VIDEO"}
        project_deposit = Project.create(project_metadata)
        # Create video
        metadata["_project_id"] = project_deposit["_deposit"]["id"]
        video_deposit = Video.create(metadata)
        video_deposit_id = video_deposit["_deposit"]["id"]

        # Upload master video
        bucket_id = video_deposit["_buckets"]["deposit"]
        # dummy test video, files are not implemented yet
        video_path = (
            "cds_migrator_kit/videos/weblecture_migration/data/videos/test_video.mp4"
        )
        object_version = load_file_to_bucket(bucket_id=bucket_id, file_path=video_path)

        # Create tags for master, extract metadata, upload frames and subformats
        flow = self._run_flow(object_version, video_deposit_id)

        # Publish video
        self._publish_video_record(deposit_id=video_deposit_id)

    def _run_flow(self, object_version, deposit_id):
        """Represantation of post api/flows.

        - This methods creates/updates ObjectVersionTag for the master file,
        - Runs the celery ExtractMetadataTask
        - Loads the Frames and Subformats if they exist
        """
        # TODO Frames and Subformats always exist? What should we do if missing?
        # TODO dummy files are used it'll be changed
        deposit_id = str(deposit_id)
        payload = dict(
            version_id=str(object_version.version_id),
            key=object_version.key,
            bucket_id=str(object_version.bucket_id),
            deposit_id=deposit_id,
        )
        # Create FlowMetadata
        flow = FlowMetadata.create(
            deposit_id=deposit_id,
            user_id=2,  # TODO It's failing without user id
            payload=payload,
        )

        # Tags the master
        object_version = init_object_version(flow, has_remote_file_to_download=None)
        payload["flow_id"] = str(flow.id)

        # Run ExtractMetadata Task
        celery_task, kwargs = AVCFlowCeleryTasks.create_task(
            ExtractMetadataTask, payload, delete_copied=False
        )
        task_signature = AVCFlowCeleryTasks.create_task_signature(celery_task, **kwargs)
        celery_chain(task_signature).apply()

        frame_paths = [
            "cds_migrator_kit/videos/weblecture_migration/data/frames/a055572-posterframe-480x360-at-0-percent.jpg",
            "cds_migrator_kit/videos/weblecture_migration/data/frames/a055572-posterframe-480x360-at-10-percent.jpg",
            "cds_migrator_kit/videos/weblecture_migration/data/frames/a055572-posterframe-480x360-at-20-percent.jpg",
            "cds_migrator_kit/videos/weblecture_migration/data/frames/a055572-posterframe-480x360-at-30-percent.jpg",
            "cds_migrator_kit/videos/weblecture_migration/data/frames/a055572-posterframe-480x360-at-40-percent.jpg",
            "cds_migrator_kit/videos/weblecture_migration/data/frames/a055572-posterframe-480x360-at-50-percent.jpg",
            "cds_migrator_kit/videos/weblecture_migration/data/frames/a055572-posterframe-480x360-at-60-percent.jpg",
            "cds_migrator_kit/videos/weblecture_migration/data/frames/a055572-posterframe-480x360-at-70-percent.jpg",
            "cds_migrator_kit/videos/weblecture_migration/data/frames/a055572-posterframe-480x360-at-80-percent.jpg",
            "cds_migrator_kit/videos/weblecture_migration/data/frames/a055572-posterframe-480x360-at-90-percent.jpg",
        ]
        # Load the frames (using the paths of frames)
        load_frames(payload=payload, frame_paths=frame_paths)

        # Load the subformats
        subformat_paths = [
            "cds_migrator_kit/videos/weblecture_migration/data/videos/test_video-1080p-quality.mp4"
        ]
        load_subformats(payload=payload, subformat_paths=subformat_paths)

        # Flow and Tasks modifications need to be persisted
        db.session.commit()
        index_deposit_project(deposit_id)
        return flow

    def _publish_video_record(self, deposit_id):
        """Publish video record."""
        try:
            # Fetch record
            video_deposit = deposit_video_resolver(str(deposit_id))
            # Publish record
            video_deposit.publish().commit()
            db.session.commit()
        except Exception:
            raise ManualImportRequired("Publish failed!", stage="load")

    def _control_required_metadata(self, metadata):
        """Temporary method to check metadata, will be deleted."""
        required_keys = ["title", "description", "contributors", "language", "date"]

        # Check for missing or empty keys
        missing_keys = [
            key for key in required_keys if key not in metadata or not metadata[key]
        ]

        return missing_keys

    def _load(self, entry):
        """Use the services to load the entries."""
        if entry:
            recid = entry.get("record", {}).get("recid", {})

            migration_logger = RDMJsonLogger(collection="weblectures")

            metadata = entry.get("record", {}).get("json", {}).get("metadata")
            missing = self._control_required_metadata(metadata)
            if missing:
                exc = ManualImportRequired(
                    message="Missing required metadata fields.",
                    field="metadata",
                    value=",".join(missing),
                    stage="Load",
                    recid=metadata.get("recid"),
                    priority="warning",
                )
                migration_logger.add_log(exc, record=entry)
            else:
                self.create_publish_single_video_record(entry)
                migration_logger.add_success(recid)

    def _cleanup(self, *args, **kwargs):
        """Cleanup the entries."""
        pass
