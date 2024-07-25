import argparse
import json
import os
import shutil


def copy_collection_file(dump_files, destination_prefix, working_dir):
    file_log = open(os.path.join(working_dir, "files.log"), "w")

    for dump_file in dump_files:
        with open(os.path.join(working_dir, dump_file), "r") as json_dump:
            data = json.load(json_dump)
            for record in data:
                legacy_record_files = record["files"]
                for legacy_record_file in legacy_record_files:
                    full_path = legacy_record_file["full_path"]
                    # important: last slash
                    path_to_replace = "/opt/cdsweb/var/data/files/"

                    rel_path = full_path.replace(path_to_replace, "")
                    destination_path = os.path.join(destination_prefix, rel_path)
                    parent_dest_path = os.path.dirname(destination_path)
                    if not os.path.exists(parent_dest_path):
                        os.makedirs(parent_dest_path)
                    shutil.copy(full_path, destination_path)
                    file_log.writelines(
                        [
                            f"RECID: {record['recid']},"
                            f" bibdocid: {legacy_record_file['bibdocid']}"
                            f" file: {legacy_record_file['full_name']},"
                            f" destination: {destination_path}"
                        ]
                    )
    file_log.close()


def get_dump_files_paths(working_dir):
    dump_files = []
    # get all dump files in the folder
    for root, dirs, files in os.walk(working_dir, topdown=True):
        dump_files += [os.path.join(root, filename) for filename in files]
    return dump_files


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Copy files over script")
    parser.add_argument(
        "--dump-folder", metavar="path", required=True, help="the path to dump folder"
    )
    parser.add_argument(
        "--files-destination",
        metavar="path",
        required=True,
        help="path to destination folder on EOS",
    )
    args = parser.parse_args()

    dump_folder = args.dump_folder

    collection_dump_file_list = get_dump_files_paths(dump_folder)
