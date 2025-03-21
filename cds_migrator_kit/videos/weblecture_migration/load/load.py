# -*- coding: utf-8 -*-
#
# Copyright (C) 2025 CERN.
#
# CDS-Videos is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""CDS-Videos migration load module."""

import json
import logging
from pathlib import Path

from cds.modules.flows.deposit import index_deposit_project
from cds.modules.flows.files import init_object_version
from invenio_db import db
from invenio_rdm_migrator.load.base import Load

from cds_migrator_kit.errors import ManualImportRequired, MissingRequiredField, UnexpectedValue
from cds_migrator_kit.reports.log import RDMJsonLogger

from .helpers import (
    create_project,
    create_video,
    create_flow,
    extract_metadata,
    copy_frames,
    copy_additional_files,
    publish_video_record,
    transcode_task
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
    
    def _get_files(self, entry):
        """Get lecturemedia files"""
        if self.dry_run:
            # Use dummy files for loading; existence is already checked in the transform stage.
            return {
                "master_video": "tests/cds-videos/data/files/media_data/2025/1/presenter-720p.mp4", 
                "frames": [str(f) for f in Path("tests/cds-videos/data/files/media_data/2025/1/frames").iterdir() if f.is_file() and not f.name.startswith('.')],                
                "subformats":[
                    {"path":"tests/cds-videos/data/files/media_data/2025/1/presenter-360p.mp4",
                    "quality": "360p"}
                ],
                "additional_files": ["tests/cds-videos/data/files/media_data/2025/1/1_en.vtt"]
            }
        return entry.get("record", {}).get("json", {}).get("media_files")
    
    def create_publish_single_video_record(self, entry):
        """Create and publish project and video for single video record."""
        # Get transformed metadata
        metadata = entry.get("record", {}).get("json", {}).get("metadata")
        # Get transformed media files
        media_files = self._get_files(entry)
        # TODO `type` will be changed
        project_metadata = {"category": "CERN", "type": "VIDEO"}
        try:
            # Create project
            project_deposit = create_project(project_metadata)
            
            # Create video
            video_deposit, master_object = create_video(project_deposit, metadata, media_files["master_video"])

            # Get the deposit_id and bucket_id
            video_deposit_id = video_deposit["_deposit"]["id"]
            bucket_id = video_deposit["_buckets"]["deposit"]
            
            # Create flow and payload
            flow, payload = create_flow(master_object, video_deposit_id, user_id=2)
        
            # Create tags for the master video file
            init_object_version(flow.flow_metadata, has_remote_file_to_download=None)
            
            # Extract metadata
            extract_metadata(payload)
        
        except ManualImportRequired:
            db.session.rollback()
            # TODO if `copy_file_to_bucket` method failes it's deleting the file
            # but if anything else fails, should we try to delete all the files?
            raise

        # Just log if something goes wrong: Frames, Subformats, Additional Files
        frame_paths = media_files["frames"]
        copy_frames(payload=payload, frame_paths=frame_paths)

        subformat_paths = media_files["subformats"]
        transcode_task(payload=payload, subformats=subformat_paths)

        additional_files = media_files["additional_files"]
        copy_additional_files(str(bucket_id), additional_files)

        # Index deposit
        index_deposit_project(video_deposit_id)

        # TODO Create legacy PID
        recid = entry.get("record", {}).get("recid", ""),

        # Publish video
        publish_video_record(deposit_id=video_deposit_id)


    def _save_original_dumped_record(self, entry, recid_state, logger):
        """Save the original dumped record.

        This is the originally extracted record before any transformation.
        """
        # TODO implement
        _original_dump = entry["_original_dump"]
        pass

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
