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

cli_logger = logging.getLogger("migrator")


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
        master_folder="",
        media_folder="",
        
    ):
        """Constructor."""
        self.recid = recid
        self.entry_files = entry_files
        self.collection = collection
        self.master_folder = Path(current_app.config["MOUNTED_MASTER_CEPH_PATH"])
        self.media_folder = Path(current_app.config["MOUNTED_MEDIA_CEPH_PATH"])
        self.transformed_files_json = {}
        # presenter and presentation str (camera/slides)
        self.presenter_str = None
        self.presentation_str = None
        # use_composite is true, if master folder has presenter and presentation
        self.use_composite = False
        self.master_data_folder = None
        self.media_data_folder = None

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
    
    def _check_presenter_presentation_exists(self, file_paths):
        """Check if the folder contains either ('presenter' & 'presentation') or ('camera' & 'slides') files."""
        
        valid_combinations = {
            "presenter": "presentation",
            "camera": "slides"
        }
        
        found_keywords = set()
        
        for file_path in file_paths:
            file_name = Path(file_path).stem.lower()
            for keyword, pair in valid_combinations.items():
                if keyword in file_name:
                    found_keywords.add(keyword)
                if pair in file_name:
                    found_keywords.add(pair)
                
                # If a valid pair is found, return
                if {keyword, pair} == found_keywords:
                    self.presenter_str = keyword
                    self.presentation_str = pair
                    self.use_composite = True
                    return True
        
        return False
    
    def _find_master_data_folder_path(self, path):
        """Find the full path of the master_path(comes from marcxml)"""        
        search_paths = [
            self.master_folder / path, # master_folder/year/id
            self.master_folder / path / "media" # master_folder/year/id/media
        ]

        try:
            # Try to find video files in folder
            for search_path in search_paths:
                all_files = [f.name for f in Path(search_path).iterdir() if f.is_file() and f.suffix == ".mp4"]
                if all_files:
                    return search_path # Files are found
        except FileNotFoundError:
            # No files are found, raise an error
            raise ManualImportRequired(
                message="Master folder couldn't be found!",
                stage="transform",
                field="master_data",
                recid=self.recid,
                value=search_path,
                priority="critical",
            )
    
    def _check_master_data_folder(self):
        """Check the master_data folder and set the files.
        
        - If presenter and presentation found, add them to additional files.
        - If presenter and presentation not found tries to get the `video.mp4` file as master.
        """
        # Get all files in the master data folder
        all_files = self._get_all_files_in_folder(self.master_data_folder)

        # Presenter and presentation files are found add all files as additional
        # Composite will be used as main file
        if self._check_presenter_presentation_exists(all_files):
            # Add all files as additional
            self._add_files_to_file_json(
                json_key="additional_files", 
                folder=self.master_data_folder, 
                files_list=all_files
            )
            return
        
        # Presenter and presentation doesn't exists, try to find the file named "video.mp4" or "presenter.mp4"
        video_files = [file for file in all_files if file.lower() == "video.mp4" or file.lower() == "presenter.mp4"]
        if len(video_files) == 1:
            self.transformed_files_json["master_video"] = str(self.master_data_folder / video_files[0])
        else:
            raise ManualImportRequired(
                message="Master file couldn't be found!",
                stage="transform",
                recid=self.recid,
                value="master_video",
                priority="critical",
            )
        
    def _frames_exists(self, frames_list):
        """Check if there's exactly 10 frames"""
        if len(frames_list) == 10:
            return True
        # Frames will be generated
        return False
    
    def _get_subformats_from_datav2json(self, data):
        """Get the subformat infos from data.v2.json"""
        try:
            subformats_list = []
            for file in data.get("streams", [])[0].get("sources", {}).get("mp4", []):
                # Quality is missing in data.v2.json
                if "res" not in file or "h" not in file["res"]:
                    raise MissingRequiredField(f"Missing subformat quality in data.v2.json")

                subformats_list.append({
                    "path": file["src"].strip("/"),
                    "quality": f"{file['res']['h']}p"
                })
        except:
            raise ManualImportRequired(
                message="Couldn't get the subformats from data.v2.json!",
                stage="transform",
                recid=self.recid,
                value="data_v2_json",
                priority="critical",
            )
        if not subformats_list:
            raise ManualImportRequired(
                message="No subformats found in data.v2.json!",
                stage="transform",
                recid=self.recid,
                value="data_v2_json",
                priority="critical",
            )
        return subformats_list
        
    def _get_highest_and_other_composites(self):
        """Find and return the highest quality composite video (1080p) and subformats (720p, 480p, 360p).
        
        Composite videos will always be inside the media_data folder."""
       
        # Get all the files in the folder
        file_list = self._get_all_files_in_folder(self.media_data_folder)
        
        required_resolutions = {1080, 720, 480, 360}
        composite_videos = {}

        # Extract composite videos and their resolutions
        for file in file_list:
            match = re.search(r"-composite-(\d+)p-quality", file)
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
                value=self.media_data_folder,
                priority="critical",
            )

        # Log missing subformats TODO find a better logging
        missing_resolutions = required_resolutions - set(composite_videos.keys())
        if missing_resolutions:
            logging.warning(f"Folder:{self.media_data_folder} missing composite subformats: {sorted(missing_resolutions)}")
        
        # Get the highest quality composite (1080p)
        highest_quality_composite = composite_videos[1080]

        # Get the required subformats if they exist
        other_composites = [
            {"path": composite_videos[res], "quality": f"{res}p"}
            for res in [720, 480, 360] if res in composite_videos
        ]

        return highest_quality_composite, other_composites
    
    def _set_composite_files(self, all_files):
        """Find all the composite files and add them to transformed file_info_json."""
        # Find the master and subformat composites
        master_composite, subformats = self._get_highest_and_other_composites()
        
        # Add the master composite to file_info_json
        self.transformed_files_json["master_video"] = str(self.media_data_folder / master_composite)
        
        # Add subformats to file_info_json
        self._add_files_to_file_json(
            json_key="subformats", 
            folder=self.media_data_folder, 
            files_list=subformats
        )
 
        # Frames (generated using composite)
        frame_folder = self.media_data_folder / "frames"
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
                value=path,
                priority="critical",
            )
        
        # Get all files (even the subfolder files) in media_data folder
        all_files = {file.name for file in Path(self.media_data_folder).rglob("*") if file.is_file()} 
        # No file found in media_data folder
        if not all_files:
            raise ManualImportRequired(
                message="No file found in the media_data folder!",
                stage="transform",
                recid=self.recid,
                value=path,
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

    def _set_media_files(self, files_paths):
        """Check the media_data folder with the file paths found in recod marcxml, and set the paths to file_info_json.
        
        files_paths: file paths comes from the record.
        media_data_folder includes composite file, vtt files, subformats, frames."""

        # Check the folder and the files
        all_files = self._check_files_in_media_data_folder(files_paths) 
           
        # Use the composite, presenter/presentation exists
        if self.use_composite:
            # Find and set the all composite files in the folder
            self._set_composite_files(all_files=all_files)
            # File paths found the record will be additional file.
            # If we use the composite, we dont need the subformats of the presenter and presentation.
            additional_files = [file for file in files_paths 
                if self.presenter_str not in Path(file).stem.lower() 
                and self.presentation_str not in Path(file).stem.lower()]
            self._add_files_to_file_json(
                json_key="additional_files",
                folder=self.media_folder,
                files_list=additional_files
            )
                
        # We dont have the composite we'll get the subformats and frames from the folder
        else:
            try:
                # Read "data.v2.json"
                data_v2_json = self.media_data_folder / "data.v2.json"
                # Read the data.v2.json file
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
            
            # ~~~~SUBFORMATS~~~~
            subformats = self._get_subformats_from_datav2json(data)
            # Ignore if it's master file
            subformats_list = [
                file_dict for file_dict in subformats
                if not self.transformed_files_json["master_video"].endswith(Path(file_dict["path"]).name)
            ]            
            # Add them as subformats
            self._add_files_to_file_json(
                json_key="subformats",
                folder=self.media_folder,
                files_list=subformats_list
            )
            
            # ~~~~Additional Files~~~~
            # Record marcxml usually have the paths of the subformats, but it's already added.
            additional_files = [file for file in files_paths if file not in subformats_list]
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

        # Get master path 
        master_path = self._get_master_path()
        
        # Get the year and id eg: master_data/year/event_id
        path = master_path.split("master_data/", 1)[-1]
        
        # Find the full path of master data folder
        master_data_folder = self._find_master_data_folder_path(path)
        
        # Set the full paths of the folders
        self.master_data_folder = master_data_folder
        self.media_data_folder = self.media_folder / path
            
        # Check and set the files in master_data_folder
        self._check_master_data_folder()

        # Get the paths of the files (comes from the record marcxml)
        path_files = [item["path"].strip("/") for item in self.entry_files
            if "path" in item
        ]   
        # Check and set the media_files checking the 
        self._set_media_files(path_files)        
        
        return self.transformed_files_json