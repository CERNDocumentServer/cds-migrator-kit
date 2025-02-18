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

from cds.modules.deposit.api import deposit_video_resolver
from cds.modules.deposit.ext import _create_tags
from cds.modules.flows.api import AVCFlowCeleryTasks
from cds.modules.flows.models import FlowTaskMetadata, FlowTaskStatus
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


def extract_metadata(payload):
    """Extract the metadata of the master video file."""
    celery_task, kwargs = AVCFlowCeleryTasks.create_task(
        ExtractMetadataTask, payload, delete_copied=False
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
    try:
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
    except Exception:
        shutil.rmtree(output_folder, ignore_errors=True)
        frames_task.clean(version_id=version_id)
        raise ManualImportRequired("Frame uploading failed!", stage="load")


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

    for path in subformat_paths:
        # Extract the quality using the file name
        preset_quality = get_video_quality(path)
        qualitiy_config = current_app.config["CDS_OPENCAST_QUALITIES"].get(preset_quality)
        
        # If it's different subformat quality than (360,480,720...) don't upload
        if not qualitiy_config: 
            continue

        # Copy the file to FileInstance and create ObjectVersion
        obj = copy_file_to_bucket(payload["bucket_id"], path)

        # Add tags to the subformat
        ObjectVersionTag.create(obj, "master", payload["version_id"])
        ObjectVersionTag.create(obj, "media_type", "video")
        ObjectVersionTag.create(obj, "context_type", "subformat")
        ObjectVersionTag.create(obj, "smil", "true")
        ObjectVersionTag.create(obj, "preset_quality", preset_quality)
        qualitiy_config = current_app.config["CDS_OPENCAST_QUALITIES"][preset_quality]
        if "tags" in qualitiy_config:
            for key, value in qualitiy_config["tags"].items():
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
        _create_tags(obj)


def get_video_quality(path):
    """Extract the video quality using the file name.
    
    Examples:
    - 819161-1000-kbps-853x480-23.98-fps-audio-96-kbps-44-kHz-stereo.mp4
    - 1401254-composite-800p-quality
    """
    video_name = os.path.basename(path)
    preset_quality = None

    # Try to find resolution in standard "-480p" format
    match = re.search(r"-(\d{3,4}p)(?:-quality)?\.[a-zA-Z0-9]+$", video_name)
    if match:
        preset_quality = match.group(1)  # Extracts the quality part (e.g., "480p")

    # If the first pattern fails, try extracting from "853x480" format
    if not preset_quality:
        match = re.search(r"(\d{3,4})x(\d{3,4})", video_name)
        if match:
            preset_quality = f"{match.group(2)}p"  # Extracts the vertical resolution (e.g., "480p")

    # If no resolution found, raise an error
    if not preset_quality:
        raise ManualImportRequired(
            f"Subformat quality not found in filename: {video_name}", 
            stage="load"
        )

    return preset_quality