# -*- coding: utf-8 -*-
#
# Copyright (C) 2025 CERN.
#
# CDS-Videos is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""CDS-Videos migration load module helper."""


import os
import re
import shutil
import tempfile
from pathlib import Path

from cds.modules.deposit.api import Project, Video, deposit_video_resolver
from cds.modules.deposit.ext import _create_tags
from cds.modules.flows.api import AVCFlowCeleryTasks
from cds.modules.flows.models import FlowTaskMetadata, FlowTaskStatus, FlowMetadata
from cds.modules.flows.tasks import (
    ExtractMetadataTask,
    ExtractFramesTask,
    TranscodeVideoTask,
)
from cds.modules.opencast.tasks import _get_opencast_subformat_info
from cds.modules.records.api import CDSVideosFilesIterator
from celery import chain as celery_chain
from flask import current_app
from invenio_db import db
from invenio_files_rest.models import (
    Bucket,
    FileInstance,
    ObjectVersion,
    ObjectVersionTag,
    as_object_version,
)
from invenio_files_rest.storage import pyfs_storage_factory

from cds_migrator_kit.errors import ManualImportRequired


def copy_file_to_bucket(bucket_id, file_path):
    """Create a FileInstance, move the video file to FileInstance storage, return the created object version."""
    try:
        video_bucket = Bucket.get(bucket_id)
        video_name = os.path.basename(file_path)
        file = FileInstance.create()

        # Get the location for the file instance
        default_location = video_bucket.location.uri
        file_storage = pyfs_storage_factory(
            fileinstance=file, default_location=default_location
        )
        fp = file_storage.open(mode="wb")
        full_path = Path(fp.name.decode()).resolve()
        
        # Check if the destination already exists
        if full_path.exists() and full_path.is_dir() and any(full_path.iterdir()):
            raise FileExistsError(f"{full_path} already exists.")

        # Copy file to storage.
        shutil.copy2(file_path, full_path)

        # Update FileInstance
        file_size = os.path.getsize(file_storage.fileurl)
        file_checksum = file_storage.checksum()
        file.set_uri(file_storage.fileurl, file_size, file_checksum)

        # Create object version
        object_version = ObjectVersion.create(
            video_bucket, video_name, _file_id=file.id
        )
        return object_version

    except FileNotFoundError:
        raise ManualImportRequired(f"File '{video_name}' not found.", stage="load")
    except Exception as e:
        raise ManualImportRequired(
            f"Error uploading file to bucket'{video_name}': {e}", stage="load"
        )


def create_project(project_metadata):
    """Create a project with metadata."""
    # TODO `type` will be changed
    project_deposit = Project.create(project_metadata)
    return project_deposit


def create_video(project_deposit, video_metadata, video_file_path):
    """Create a video in project with metadata and master video file.
    
    Returns video_deposit and master_object"""
    # Create video_deposit
    video_metadata["_project_id"] = project_deposit["_deposit"]["id"]
    video_deposit = Video.create(video_metadata)
    
    # Copy the master video to bucket
    bucket_id = video_deposit["_buckets"]["deposit"]
    object_version = copy_file_to_bucket(bucket_id=bucket_id, file_path=video_file_path)
    
    return video_deposit, object_version


def create_flow(object_version, deposit_id, user_id):
    """Create a payload and flow."""
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


def publish_video_record(deposit_id):
    """Publish video record."""
    try:
        # Fetch record
        video_deposit = deposit_video_resolver(str(deposit_id))
        # Publish record
        video_deposit.publish().commit()
        db.session.commit()
    except Exception as e:
        raise ManualImportRequired(f"Publish failed: {e}", stage="load")
    

def extract_metadata(payload):
    """Extract the metadata of the master video file."""
    celery_task, kwargs = AVCFlowCeleryTasks.create_task(
        ExtractMetadataTask, payload, delete_copied=False
    )
    task_signature = AVCFlowCeleryTasks.create_task_signature(celery_task, **kwargs)
    celery_chain(task_signature).apply()


def run_frames_task(payload):
    """Create the frames of the master video file."""
    celery_task, kwargs = AVCFlowCeleryTasks.create_task(
        ExtractFramesTask, payload, delete_copied=False
    )
    task_signature = AVCFlowCeleryTasks.create_task_signature(celery_task, **kwargs)
    celery_chain(task_signature).apply()
    

def copy_frames(payload, frame_paths):
    """Load frames for the master video file without running the celery task.

    payload (dict):
        - version_id (str): The version ID of the main video file.
        - key (str): The file name of the main video file.
        - bucket_id (str): The bucket id of the record.
        - deposit_id (str): The deposit id of the record.
        - flow_id (str): The flow id.
    frame_paths (list): A list of file paths of the frames.
    """
    if not len(frame_paths) == 10:
        # TODO should we log?
        # Missing/extra frames, don't use them, create with celery task
        run_frames_task(payload)
        return
    
    version_id = payload["version_id"]
    object_version = as_object_version(version_id)

    frames_task, kwargs = AVCFlowCeleryTasks.create_task(
        ExtractFramesTask, payload, delete_copied=False
    )
    frames_task.flow_id = payload.get("flow_id")
    # FramesTask Metadata
    flow_task_metadata = frames_task.get_or_create_flow_task()
    flow_task_metadata.status = FlowTaskStatus.STARTED

    # Calculate time positions
    options = frames_task._time_position(
        duration=object_version.get_tags()["duration"],
        frames_start=0, 
        frames_end=90,
        frames_gap=10,
    )
    # Needed for creating gif file
    output_folder = None
    try:
        # Create ObjectVersions with frames using ExtractFramesTask._create_frames
        frames = frames_task._create_frames(
            frame_paths,
            object_version,
            options.get("start_time"),
            options.get("time_step"),
        )
        # Temp folder to create gif file
        output_folder = tempfile.mkdtemp()
        frames_task._create_gif(
            bucket=str(object_version.bucket.id),
            frames=frames,
            output_dir=output_folder,
            master_id=version_id,
        )
        flow_task_metadata.status = FlowTaskStatus.SUCCESS

        # Cleanup
        shutil.rmtree(output_folder)
    except FileNotFoundError as e:
        frames_task.clean(version_id=version_id)
        raise ManualImportRequired(f"Frame file not found: {e}", stage="load")
    except Exception as e:
        frames_task.clean(version_id=version_id)
        raise ManualImportRequired(f"Frame uploading failed: {e}", stage="load")


def copy_subformats(payload, subformat_paths):
    """Load subformats for the master video file without celery task.

    payload (dict):
        - version_id (str): The version ID of the main video file.
        - key (str): The file name of the main video file.
        - bucket_id (str): The bucket id of the record.
        - deposit_id (str): The deposit id of the record.
        - flow_id (str): The flow id.
    subformat_paths (list): A list of file paths of the subformats.
    """
    # TODO add a subformats logger and log missing subformats, do we need a separate logger?

    # Create FlowTaskMetadata
    subformat_payload = payload.copy()
    transcode_task = FlowTaskMetadata.create(
        flow_id=payload["flow_id"],
        name=TranscodeVideoTask.name,
        payload=subformat_payload,
    )

    transcode_task.status = FlowTaskStatus.STARTED

    for item in subformat_paths:
        path = item["path"]
        preset_quality = item["quality"]        
        quality_config = current_app.config["CDS_OPENCAST_QUALITIES"].get(preset_quality)
        
        # If it's different subformat quality than (360,480,720...) don't upload
        if not quality_config: 
            continue

        # Copy the file to FileInstance and create ObjectVersion
        obj = copy_file_to_bucket(payload["bucket_id"], path)

        # Add tags to the subformat
        ObjectVersionTag.create(obj, "master", payload["version_id"])
        ObjectVersionTag.create(obj, "media_type", "video")
        ObjectVersionTag.create(obj, "context_type", "subformat")
        ObjectVersionTag.create(obj, "smil", "true")
        ObjectVersionTag.create(obj, "preset_quality", preset_quality)
        quality_config = current_app.config["CDS_OPENCAST_QUALITIES"][preset_quality]
        if "tags" in quality_config:
            for key, value in quality_config["tags"].items():
                ObjectVersionTag.create(obj, key, value)
        # Get subformat info from the config file
        info = _get_opencast_subformat_info({}, preset_quality)
        for key, value in info.items():
            ObjectVersionTag.create(obj, key, str(value))

    # Check the master video file has subformats
    video_deposit = deposit_video_resolver(payload["deposit_id"])
    original_file = CDSVideosFilesIterator.get_master_video_file(video_deposit)
    subformats = CDSVideosFilesIterator.get_video_subformats(original_file)
    if subformats:
        transcode_task.status = FlowTaskStatus.SUCCESS
        db.session.add(transcode_task)
        db.session.commit()
    else:
        # Should we fail if no subformats or only logging is enough?
        transcode_task.status = FlowTaskStatus.FAILURE
        raise ManualImportRequired(f"Subformat load failed.", stage="load")


def copy_additional_files(bucket_id, additional_files):
    """Load additional files for the master video file."""
    for path in additional_files:
        # Copy the file to FileInstance and create ObjectVersion
        obj = copy_file_to_bucket(bucket_id, path)

        # Add tags to the additional file
        ObjectVersionTag.create_or_update(obj, "context_type", "additional_file")
        _create_tags(obj)

