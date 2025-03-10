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

from cds_migrator_kit.errors import ManualImportRequired, MissingRequiredField, UnexpectedValue
from cds_migrator_kit.reports.log import RDMJsonLogger

from .helpers import (
    copy_additional_files,
    copy_file_to_bucket,
    copy_frames,
    copy_subformats,
    extract_metadata
)


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
    
    def _create_project(self, project_metadata):
        """Create a project with metadata."""
        # TODO `type` will be changed
        project_deposit = Project.create(project_metadata)
        return project_deposit
    
    def _create_video(self, project_deposit, video_metadata, video_file_path):
        """Create a video in project with metadata and master video file.
        
        Returns video_deposit and master_object"""
        # Create video_deposit
        video_metadata["_project_id"] = project_deposit["_deposit"]["id"]
        video_deposit = Video.create(video_metadata)
        
        # Copy the master video to bucket
        bucket_id = video_deposit["_buckets"]["deposit"]
        object_version = copy_file_to_bucket(bucket_id=bucket_id, file_path=video_file_path)
        
        return video_deposit, object_version
    
    def _create_flow(self, object_version, deposit_id, user_id=2):
        """Create a payload and flow."""
        # TODO user_id will be change
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
            user_id=user_id,
            payload=payload,
        )
        payload["flow_id"] = str(flow.id)
        return flow, payload
        
    def create_publish_single_video_record(self, entry):
        """Create and publish project and video for single video record."""
        # Get transformed metadata
        metadata = entry.get("record", {}).get("json", {}).get("metadata")
        # Get transformed media files
        media_files = entry.get("record", {}).get("json", {}).get("media_files")
        
        # Create project
        # TODO `type` will be changed
        project_metadata = {"category": "CERN", "type": "VIDEO"}
        project_deposit = self._create_project(project_metadata)
        
        # Create video
        video_deposit, master_object = self._create_video(project_deposit, metadata, media_files["master_video"])

        # Get the deposit_id and bucket_id
        video_deposit_id = video_deposit["_deposit"]["id"]
        bucket_id = video_deposit["_buckets"]["deposit"]
        
        # Create flow and payload
        flow, payload = self. _create_flow(master_object, video_deposit_id)
        
        # Create tags for the master video file
        init_object_version(flow, has_remote_file_to_download=None)
        
        # Extract metadata
        extract_metadata(payload)
        
        # Copy frames
        frame_paths = media_files["frames"]
        copy_frames(payload=payload, frame_paths=frame_paths)
        
        # Copy subformats
        subformat_paths = media_files["subformats"]
        copy_subformats(payload=payload, subformat_paths=subformat_paths)

        # Load additional files
        additional_files = media_files["additional_files"]
        copy_additional_files(str(bucket_id), additional_files)
        
        # Flow and Tasks modifications need to be persisted
        db.session.commit()
        index_deposit_project(video_deposit_id)

        # Publish video
        self._publish_video_record(deposit_id=video_deposit_id)

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


    def _load(self, entry):
        """Use the services to load the entries."""
        migration_logger = RDMJsonLogger(collection="weblectures")
        
        if entry:
            try:  
                recid = entry.get("record", {}).get("recid", {})

                self.create_publish_single_video_record(entry)
                migration_logger.add_success(recid)
            except (
                UnexpectedValue,
                ManualImportRequired,
                MissingRequiredField,
            ) as e:
                migration_logger.add_log(e, record=entry)

    def _cleanup(self, *args, **kwargs):
        """Cleanup the entries."""
        pass
