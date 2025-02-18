# -*- coding: utf-8 -*-
#
# Copyright (C) 2024 CERN.
#
# CDS-Videos is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""CDS-Videos migration load module."""

import json
import logging
import os
from io import BytesIO

from cds.modules.deposit.api import Project, Video, deposit_video_resolver
from cds.modules.flows.api import AVCFlowCeleryTasks, FlowService
from cds.modules.flows.deposit import index_deposit_project
from cds.modules.flows.files import init_object_version
from cds.modules.flows.models import FlowMetadata
from cds.modules.flows.tasks import (
    ExtractFramesTask,
    ExtractMetadataTask,
    TranscodeVideoTask,
)
from celery import chain as celery_chain
from invenio_db import db
from invenio_files_rest.models import Bucket, ObjectVersion, ObjectVersionTag
from invenio_rdm_migrator.load.base import Load
from sqlalchemy.orm.attributes import flag_modified as db_flag_modified

from cds_migrator_kit.errors import ManualImportRequired
from cds_migrator_kit.reports.log import RDMJsonLogger


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

    def create_publish_single_video_record(self, metadata):
        """Create and publish project and video for single main video record."""
        # Create project
        # TODO `type` will be changed
        project_metadata = {"category": "CERN", "type": "VIDEO"}
        project_deposit = Project.create(project_metadata)
        # Create video
        metadata["_project_id"] = project_deposit["_deposit"]["id"]
        video_deposit = Video.create(metadata)
        video_deposit_id = video_deposit["_deposit"]["id"]

        # Upload video (main video)
        bucket_id = video_deposit["_buckets"]["deposit"]
        # dummy test video, files are not implemented yet
        video_path = (
            "cds_migrator_kit/videos/weblecture_migration/data/videos/test_video.mp4"
        )
        object_version = self._upload_video_file_to_bucket(
            bucket_id=bucket_id, video_path=video_path
        )

        # Create tags, flow tasks and run
        flow = self._run_flow(object_version, video_deposit_id)

        # Publish video
        self._publish_video_record(deposit_id=video_deposit_id)

    def _run_flow(self, object_version, deposit_id):
        """Low level represantation of post api/flows.

        This methods creates/updates ObjectVersionTag,
        Creates the flow with tasks:
        - ExtractMetadataTask
        - ExtractFramesTask
        and runs these tasks sync
        """
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
            user_id=2,  # TODO It's failing vithout user id
            payload=payload,
        )

        # create/update the object version with ObjectVersionTag, tags the master
        object_version = init_object_version(flow, has_remote_file_to_download=None)

        # Create flow with tasks: extract metadata, frames
        celery_tasks = self._create_flow_tasks(flow, payload)
        # Apply tasks sync
        result = celery_tasks.apply()
        # Flow and Tasks modifications need to be persisted
        db.session.commit()
        index_deposit_project(deposit_id)
        return flow

    def _create_flow_tasks(self, flow, payload):
        """
        Creates the flow with celery tasks.

        Instead of creating tasks, if we call:
            FlowService(flow).run()
        This is gonna create all the tasks(extract metadata, frames, subformats),
        and run them async
        """
        payload["flow_id"] = str(flow.id)
        # Create tasks
        celery_tasks = [
            AVCFlowCeleryTasks.create_task(ExtractMetadataTask, payload, delete_copied=False),
            AVCFlowCeleryTasks.create_task(ExtractFramesTask, payload)
        ]
        celery_tasks_signatures = [
            AVCFlowCeleryTasks.create_task_signature(celery_task, **kwargs)
            for celery_task, kwargs in celery_tasks
        ]

        return celery_chain(*celery_tasks_signatures)

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

    def _upload_video_file_to_bucket(self, bucket_id, video_path):
        """Upload file to bucket, return object version."""
        try:
            video_bucket = Bucket.get(bucket_id)
            video_name = os.path.basename(video_path)

            with open(video_path, "rb") as video_file:
                video_bytes = BytesIO(video_file.read())

            if video_bytes.getbuffer().nbytes == 0:
                raise ManualImportRequired(f"File '{video_name}' is empty.", stage="load")

            object_version = ObjectVersion.create(video_bucket, video_name, stream=video_bytes)
            return object_version

        except FileNotFoundError:
            raise ManualImportRequired(f"File '{video_name}' not found.", stage="load")
        except Exception as e:
            raise ManualImportRequired(f"Error uploading file to bucket'{video_name}': {e}", stage="load")

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
                self.create_publish_single_video_record(metadata)
                migration_logger.add_success(recid)


    def _cleanup(self, *args, **kwargs):
        """Cleanup the entries."""
        pass
