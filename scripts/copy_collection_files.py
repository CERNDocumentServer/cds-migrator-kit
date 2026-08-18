import argparse
import hashlib
import json
import os
import shutil
import sys

reload(sys)
sys.setdefaultencoding("utf-8")


def file_md5(path):
    md5 = hashlib.md5()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            md5.update(chunk)
    return md5.hexdigest()


def copy_collection_file(dump_files, destination_prefix, working_dir, skip_plots=False):
    file_log = open(os.path.join(working_dir, "files.log"), "wb")

    for i, dump_file in enumerate(dump_files):
        print(dump_file)
        print(
            "FILE {} out of {}-------------------------------------------------".format(
                i, len(dump_files)
            )
        )
        with open(dump_file, "r") as json_dump:
            data = json.load(json_dump)
            for record in data:
                legacy_record_files = record["files"]
                recid = record["recid"]
                print("Processing {}".format(recid))
                for legacy_record_file in legacy_record_files:
                    if skip_plots and legacy_record_file.get("type") == "Plot":
                        continue
                    full_path = legacy_record_file["full_path"]
                    # important: last slash
                    path_to_replace = "/opt/cdsweb/var/data/files/"

                    rel_path = full_path.replace(path_to_replace, "")
                    destination_path = os.path.join(destination_prefix, rel_path)
                    destination_path = destination_path.encode("utf-8")
                    parent_dest_path = os.path.dirname(destination_path)
                    if not os.path.exists(parent_dest_path):
                        os.makedirs(parent_dest_path)
                    if not os.path.exists(destination_path):
                        shutil.copy(full_path, destination_path)

                    filename = legacy_record_file["full_name"].encode("utf-8")
                    destination_path = destination_path.encode("utf-8")
                    print(filename)
                    print(destination_path)

                    expected = legacy_record_file.get("checksum") or ""
                    dest_md5 = file_md5(destination_path)
                    checksum_mismatch = dest_md5 != expected
                    if checksum_mismatch:
                        raise ValueError(
                            "ERROR checksum mismatch: recid={} expected={} dest={}".format(
                                recid, expected, dest_md5
                            )
                        )
                    file_log.write(
                        "RECID: %s bibdocid: %s file: %s, destination: %s \n"
                        % (
                            record["recid"],
                            legacy_record_file["bibdocid"],
                            filename,
                            destination_path,
                        )
                    )
    file_log.close()


def get_dump_files_paths(json_dump_dir):
    dump_files = []
    # get all dump files in the folder
    for root, dirs, files in os.walk(json_dump_dir, topdown=True):
        dump_files += [os.path.join(root, filename) for filename in files]
    return dump_files


collection = "lep/aleph_drafts"
environment = "dev"
skip_plots = False

destination_prefix = "/eos/media/cds/cds-rdm/{0}/migration/{1}/files".format(
    environment, collection
)
working_dir = "/eos/media/cds/cds-rdm/{0}/migration/{1}".format(environment, collection)
json_dump_dir = "/eos/media/cds/cds-rdm/{0}/migration/{1}/dump".format(
    environment, collection
)

dump_files = get_dump_files_paths(json_dump_dir)
copy_collection_file(dump_files, destination_prefix, working_dir, skip_plots=skip_plots)
