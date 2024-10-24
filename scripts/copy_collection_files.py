import argparse
import json
import os
import shutil
import io

def copy_collection_file(dump_files, destination_prefix, working_dir):
    file_log = open(os.path.join(working_dir, "files.log"), "wb")

    for dump_file in dump_files:
        with open(dump_file, "r") as json_dump:
            data = json.load(json_dump)
            for record in data:
                legacy_record_files = record["files"]
                recid = record["recid"]
                for legacy_record_file in legacy_record_files:
                    print("Processing {}".format(recid))
                    full_path = legacy_record_file["full_path"]
                    # important: last slash
                    path_to_replace = "/opt/cdsweb/var/data/files/"

                    rel_path = full_path.replace(path_to_replace, "")
                    destination_path = os.path.join(destination_prefix, rel_path)
                    parent_dest_path = os.path.dirname(destination_path)
                    if not os.path.exists(parent_dest_path):
                        os.makedirs(parent_dest_path)
                    if not os.path.exists(destination_path):
                        shutil.copy(full_path, destination_path)

                    filename = legacy_record_file['full_name'].encode("utf-8")
                    file_log.write(
                        u"RECID: %s bibdocid: %s file: %s, destination: %s \n" % (
                            record['recid'],
                            legacy_record_file['bibdocid'],
                            filename,
                            destination_path.encode("utf-8")
                        )
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
