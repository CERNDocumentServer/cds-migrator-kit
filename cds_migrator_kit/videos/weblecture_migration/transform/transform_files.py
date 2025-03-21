# -*- coding: utf-8 -*-
#
# Copyright (C) 2025 CERN.
#
# CDS-Videos is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""CDS-Videos transform module files helper."""
from pathlib import Path
import json
import logging
import re

from flask import current_app

from cds_migrator_kit.errors import (
    ManualImportRequired,
    MissingRequiredField,
    RestrictedFileDetected,
    UnexpectedValue,
)

logger_files = logging.getLogger("files")

class TransformFiles:
    """
    Transform lecturemedia links to file paths for video record.
    
    Initialize the class and call the `transform` method
    """

    def __init__(
        self,
        recid,
        entry_files,
        collection="weblectures",
        media_folder="",
        
    ):
        """Constructor."""
        self.recid = recid
        self.entry_files = entry_files
        self.collection = collection
        self.media_folder = Path(current_app.config["MOUNTED_MEDIA_CEPH_PATH"])
        self.transformed_files_json = {}
        # composite str
        self.composite_str = "composite"
        # use_composite is true, if master folder has presenter and presentation
        self.use_composite = False
        self.record_media_data_folder = None

    def _get_master_path(self):
        master_paths = [
            item["master_path"] for item in self.entry_files if "master_path" in item
        ]
        # It should have one master_path
        if len(master_paths) != 1:
            raise UnexpectedValue(
                message="Multiple/missing master files!",
                stage="transform",
                recid=self.recid,
                value="master_path",
                priority="critical",
            ) 
        return master_paths[0]

    def _get_all_files_in_folder(self, directory):
        """Get all files in a folder."""
        # Get all the files, ignore hidden files
        try:
            return [f.name for f in Path(directory).iterdir() if f.is_file() and not f.name.startswith('.')]
        except FileNotFoundError:
            # No files are found, raise an error
            raise ManualImportRequired(
                message="Folder couldn't be found!",
                stage="transform",
                recid=self.recid,
                value=directory,
                priority="critical",
            )
    
    def _add_files_to_file_json(self, json_key, folder, files_list):
        """Append the files to the transformed file_info_json."""
        for file in files_list:
            # Subformats
            if isinstance(file, dict):  
                # Handle dict format (path + quality)
                self.transformed_files_json[json_key].append({
                    "path": str(folder / file["path"]),
                    "quality": file["quality"]
                })
            # Frames, additional files
            else:  
                # Handle str format (just the file path)
                self.transformed_files_json[json_key].append(str(folder / file))
    
    def _check_composite_exists(self):
        """Check if the folder contains `composite` files."""
        file_paths = self._get_all_files_in_folder(self.record_media_data_folder)
        for file_path in file_paths:
            file_name = Path(file_path).stem.lower()
            if self.composite_str in file_name:
                return True
        return False
            
    def _frames_exists(self, frames_list):
        """Check if there's exactly 10 frames"""
        if len(frames_list) == 10:
            return True
        # Frames will be generated
        return False
        
    def _get_highest_and_other_composites(self):
        """Find and return the highest quality composite video (1080p) and subformats (720p, 480p, 360p).
        
        Composite videos will always be inside the media_data folder."""
       
        # Get all the files in the folder
        file_list = self._get_all_files_in_folder(self.record_media_data_folder)
        
        required_resolutions = {1080, 720, 480, 360}
        composite_videos = {}

        # Extract composite videos and their resolutions
        for file in file_list:
            match = re.search(r"composite-(\d+)p-quality", file)
            if match:
                resolution = int(match.group(1))
                if resolution in required_resolutions:
                    composite_videos[resolution] = file

        # Ensure 1080p is present
        if 1080 not in composite_videos:
            raise MissingRequiredField(
                message="1080p composite file not found!",
                stage="transform",
                recid=self.recid,
                value=self.record_media_data_folder,
                priority="critical",
            )

        # Log missing subformats 
        missing_resolutions = required_resolutions - set(composite_videos.keys())
        if missing_resolutions:
            logger_files.warning(f"Folder:{self.record_media_data_folder} missing composite subformats: {sorted(missing_resolutions)}")
        
        # Get the highest quality composite (1080p)
        highest_quality_composite = composite_videos[1080]

        # Get the required subformats if they exist
        other_composites = [
            {"path": composite_videos[res], "quality": f"{res}p"}
            for res in [1080, 720, 480, 360] if res in composite_videos
        ]

        return highest_quality_composite, other_composites
    
    def _set_composite_files(self, all_files):
        """Find all the composite files and add them to transformed file_info_json."""
        # Find the master and subformat composites
        master_composite, subformats = self._get_highest_and_other_composites()
        
        # Add the master composite to file_info_json
        self.transformed_files_json["master_video"] = str(self.record_media_data_folder / master_composite)
        
        # Add subformats to file_info_json
        self._add_files_to_file_json(
            json_key="subformats", 
            folder=self.record_media_data_folder, 
            files_list=subformats
        )
 
        # Frames (generated using composite)
        frame_folder = self.record_media_data_folder / "frames"
        
        # Frame folder is missing! Log it
        if not frame_folder.is_dir():
            logger_files.warning(f"[WARNING] Record:{self.recid} frames folder for composite is missing:{frame_folder}")
            return
        
        frames_list = self._get_all_files_in_folder(frame_folder)
        # Sort frames eg:"frame-1.jpg" to "frame-10.jpg"
        sorted_frames = sorted(frames_list, key=lambda f: int(re.search(r'frame-(\d+)', f).group(1)))
        # Check if missing/extra frames
        if self._frames_exists(sorted_frames):
            # Add frames to file_info_json
            self._add_files_to_file_json(
                json_key="frames",
                folder=frame_folder,
                files_list=sorted_frames
            )

    def _check_files_in_media_data_folder(self, files_paths):
        """Check and return all the files in media_data_folder.
        files_paths: Founded paths from the record marcxml"""
        # No path found in the record
        if not files_paths:
            raise UnexpectedValue(
                message="No media file found in the record!",
                stage="transform",
                recid=self.recid,
                priority="critical",
            )
        
        # Get all files (even the subfolder files) in media_data folder
        all_files = {file.name for file in Path(self.record_media_data_folder).rglob("*") if file.is_file()} 
        # No file found in media_data folder
        if not all_files:
            raise ManualImportRequired(
                message="No file found in the media_data folder!",
                stage="transform",
                recid=self.recid,
                priority="critical",
            )
            
        # Check all files_paths exists in media_data folder
        for path in files_paths:
            file_name = Path(path).name
            if file_name not in all_files:
                raise ManualImportRequired(
                    message="File not found in the media_data folder!",
                    stage="transform",
                    recid=self.recid,
                    value=path,
                    priority="critical",
                )
        return all_files

    def _get_highest_and_subformats_from_datajson(self, datajson):
        """Get the highest quality presenter/presentation file, and subformats"""
        highest_quality_videos = []
        all_subformats = []
        streams = datajson.get("streams", [])
        # Composite record should have 2 streams
        if self.use_composite and len(streams) != 2:
            raise UnexpectedValue("Missing presenter/presentation files for composite record!")
        # One video record should have 1 stream
        elif not self.use_composite and len(streams) != 1:
            raise UnexpectedValue("Missing presenter/presentation files for composite record!")
        
        for stream in streams:
            subformats = stream.get("sources", {}).get("mp4", [])
            if not subformats:
                raise UnexpectedValue("Missing MP4 formats in one of the streams")
            sorted_subformats = sorted(
                subformats,
                key=lambda x: int(x.get("res", {}).get("h", 0)),
                reverse=True
            )
            # Add highest quality video path
            highest_quality_videos.append(sorted_subformats[0]["src"].strip("/"))
            # Add all as subformats
            for file in sorted_subformats:
                res = file.get("res", {})
                all_subformats.append({
                    "path": file["src"].strip("/"),
                    "quality": f"{res['h']}p"
                })

        return highest_quality_videos, all_subformats
   
    def _set_media_files(self, files_paths):
        """Check the media_data folder with the file paths found in recod marcxml, and set the paths to file_info_json.
        
        files_paths: file paths comes from the record.
        media_data_folder includes composite file, vtt files, subformats, frames."""

        # Check the folder and all files exists in the folder
        all_files = self._check_files_in_media_data_folder(files_paths) 
        
        try:
            # Read "data.v2.json"
            data_v2_json = self.record_media_data_folder / "data.v2.json"
            with open(data_v2_json, "r", encoding="utf-8") as file:
                data = json.load(file)
        except FileNotFoundError:
            raise ManualImportRequired(
                message="data_v2_json file not found!",
                stage="transform",
                recid=self.recid,
                value=data_v2_json,
                priority="critical",
            )
        
        # Get master and subformats in data.v2.json
        highest_presenter_presentation, subformats = self._get_highest_and_subformats_from_datajson(data)
        
        # Use the composite, composite exists
        if self.use_composite:
            # Find and set the all composite files in the folder: (Master, subformats, frames)
            self._set_composite_files(all_files=all_files)
            
            # Add presenter and presentation as additinal video
            self._add_files_to_file_json(
                json_key="additional_files", 
                folder=self.media_folder, 
                files_list=highest_presenter_presentation
            )
                
        # We dont have the composite we'll get the main files from the folder
        else:
            # ~~~~MASTER & SUBFORMATS
            if len(highest_presenter_presentation) != 1:
                raise ManualImportRequired("Master video file is missing!")
            self.transformed_files_json["master_video"] = str(self.media_folder / highest_presenter_presentation[0])
            
            # Add them as subformats
            self._add_files_to_file_json(
                json_key="subformats",
                folder=self.media_folder,
                files_list=subformats
            )
            
            # ~~~~FRAMES~~~~            
            # Extract frame list and sort by time
            frames_list = sorted(data.get("frameList", []), key=lambda frame: frame.get("time", 0))
            
            # Check missing or extra frames
            if self._frames_exists(frames_list):
                self._add_files_to_file_json(
                    json_key="frames",
                    folder=self.media_folder,
                    files_list=[frame["url"].strip("/") for frame in frames_list] # Get the paths
                )
                
        # Exclude already added files from additional files
        subformat_paths = [item["path"] for item in subformats]
        additional_files = [file for file in files_paths if file not in subformat_paths+highest_presenter_presentation]
        
        # Add additional files
        self._add_files_to_file_json(
            json_key="additional_files",
            folder=self.media_folder,
            files_list=additional_files
        )
    
    def transform(self):
        """Transform the files for the record"""
        # Initialize the output json
        self.transformed_files_json = {
            "master_video": "", # Full path of the master video file
            "additional_files": [],
            "frames": [],
            "subformats":[]
        }

        # Get master path from the record
        master_path = self._get_master_path()
        
        # Get the year and id eg: master_data/year/event_id
        path = master_path.split("master_data/", 1)[-1]
        
        self.record_media_data_folder = self.media_folder / path
        
        # Check if the record_media_data folder has the composite video
        self.use_composite = self._check_composite_exists()

        # Get the paths of the files (comes from the record marcxml)
        path_files = [item["path"].strip("/") for item in self.entry_files
            if "path" in item
        ]   
        # Check and set the media_files checking the 
        self._set_media_files(path_files)        
        
        return self.transformed_files_json