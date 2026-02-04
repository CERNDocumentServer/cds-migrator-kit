"""
This script is used to copy the attached files from the old system to the migration folder in the new eos.

1. Read the attached files from the old system
2. Copy the attached files to the migration folder in the new eos system
3. Save the attached files mapping to a json file for attaching them to the comments in the new system
"""

# from invenio.webcomment import get_attached_files
import json
import os
import shutil

collection = "it_meetings"
environment = "sandbox"

source_prefix = "/opt/cdsweb/var/data/comments"
destination_prefix = "/eos/media/cds/cds-rdm/{0}/migration/{1}/comments".format(
    environment, collection
)
"""
collection_name/
|-- comments/
    |-- comments_metadata.json
    |-- recid/
        |-- comment_id (or reply_comment_id)/
            |-- This is where the attached file is copied to
    |-- ...
(We keep the recid folder to avoid confusion with the files folder and in case different comments contain the same file name)
(We keep the comment_id (or reply_comment_id) folder to avoid confusion with the files folder and in case different comments contain the same file name)
"""


def copy_comments_attached_files(comments_metadata):
    for recid in comments_metadata.keys():
        # Copy the whole /comments/{recid} folder to the destination folder
        shutil.copytree(
            os.path.join(source_prefix, recid), os.path.join(destination_prefix, recid)
        )
        print("Copied {} comments directory to {}".format(recid, destination_prefix))


# Load the comments metadata to get the recids with comments and the comment IDs
comments_metadata_json_file = os.path.join(destination_prefix, "comments_metadata.json")
with open(comments_metadata_json_file, "r") as f:
    comments_metadata = json.load(f)

copy_comments_attached_files(comments_metadata)
