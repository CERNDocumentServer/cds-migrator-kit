#!/usr/bin/env python
"""Prepare the data of a collection migration (run on `cds-migration-01`).

Everything is driven by the configuration of the collection, the file
`collections/<collection>.yaml` next to this script (see
`collections/_example.yaml`). It does not depend on the environment, and is the
same file the migrator pod reads (`invenio migration ... --collection
<collection>`), so there is no streams.yaml entry to add.

The data goes to the working directory of the collection under the root of the
migration data of the environment:

    /eos/media/cds/cds-rdm/<env>/migration/<collection>/

The environment is read from the current directory when it is on EOS, or given
with --env. Other roots (e.g. local folders) are given with --root.
The scripts are those of the cds-migrator-kit checkout on the legacy host
(/opt/cds-migrator-kit/migration_cli): this one calls the helpers in subscripts/
(analyze_collection_migration.py, copy_collection_files.py,
export_collection_comments.py).
See docs in migration_cli/migrating-a-collection.md.

    kinit cdsrdmeosdev            # an identity that can write to the EOS folder
    cd /eos/media/cds/cds-rdm/dev/migration      # or pass --env dev
    S=/opt/cds-migrator-kit/migration_cli

    python $S/collection_migration.py dump records  --collection antares_restr
    python $S/collection_migration.py dump comments --collection antares_restr
    python $S/collection_migration.py dump files    --collection antares_restr
    python $S/collection_migration.py dump users    # once per environment
    python $S/collection_migration.py prepare       --collection antares_restr

    # after the migration: tag the migrated records on the legacy system
    ipython $S/collection_migration.py -- tag migrated --collection antares_restr

`dump comments` and `tag migrated` need the legacy Invenio libraries: run them
with the interpreter that has them (`ipython $S/collection_migration.py -- dump
comments ...`).
Python 2 compatible on purpose (legacy host).

Layout of the working directory:

    dump/                          records dump (`dump records`)
    comments/                      comments_metadata.json, <recid>/ attachments,
                                   users/missing_commentors_from_ldap.json
    files/                         legacy files, same relative paths as legacy
    files.log                      one line per copied file
    duplicated_pids.json           legacy recids to redirect (`prepare`)
    ep_report_number_dups.json     EP report numbers to review (`prepare`)

Every step can be re-run: the dump files are replaced, and the files already
copied (verified by size + checksum) are skipped.
"""

from __future__ import print_function

import argparse
import json
import os
import re
import subprocess
import sys

PY2 = sys.version_info[0] == 2

SUBSCRIPTS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "subscripts")
CONFS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "collections")
EOS_ROOT = "/eos/media/cds/cds-rdm/{env}/migration"
ENV_FOLDERS = {"production": "prod"}
EOS_ROOT_RE = re.compile(r"^(/eos/media/cds/cds-rdm/([^/]+)/migration)(/|$)")
ANALYZE_SCRIPT = os.path.join(SUBSCRIPTS_DIR, "analyze_collection_migration.py")
DUPLICATES_FILENAME = "duplicated_pids.json"
LEGACY_FILES_ROOT = "/opt/cdsweb/var/data/files/"

# Applied to every collection dump.
# the duplicates analysis searches these records too (it redirects to them)
ANALYSIS_FORBIDDEN = (
    "-980:DELETED",
    "-980:HIDDEN",
    "-980__c:MIGRATED",
    "-980__a:DUMMY",
)
DUMP_EXCLUSIONS = (
    "-980:DELETED -980:HIDDEN " "-980__a:DUMMY -690C_:movingimages -980:GREYBOOK"
)
# -980__c:MIGRATED is not excluded on purpose: some records live in several
# communities.


# --------------------------------------------------------------------------
# helpers
# --------------------------------------------------------------------------
def _fs(path):
    """Return `path` in the form the filesystem calls need (py2: utf-8 bytes)."""
    if PY2 and isinstance(path, type("")):
        return path.encode("utf-8")
    return path


def _makedirs(path):
    if not os.path.isdir(_fs(path)):
        os.makedirs(_fs(path))


class Context(object):
    """Working directory, environment and configuration of the collection."""

    def __init__(self, args, need_conf=True):
        cwd = os.getcwd()
        match = EOS_ROOT_RE.match(cwd) or EOS_ROOT_RE.match(os.path.realpath(cwd))
        self.env = args.env or (match and match.group(2))
        if not self.env:
            sys.exit(
                "Cannot tell the environment from the current directory ({}): "
                "run from /eos/media/cds/cds-rdm/<env>/migration or pass "
                "--env".format(cwd)
            )
        # root of the migration data: given, else the EOS path of the environment
        self.root = args.root or os.environ.get("CDS_MIGRATION_ROOT")
        if not self.root:
            if self.env == "local":
                sys.exit("--root (or CDS_MIGRATION_ROOT) is needed for --env local")
            self.root = EOS_ROOT.format(env=ENV_FOLDERS.get(self.env, self.env))
        self.root = self.root.rstrip("/")
        self.users = os.path.join(self.root, "users")
        self.conf = self._load_conf(args) if need_conf else None

        self.base = (
            os.path.join(self.root, self.conf.get("folder", self.conf["name"]))
            if need_conf
            else None
        )
        if need_conf:
            self.dump = os.path.join(self.base, "dump")
            self.files = os.path.join(self.base, "files")
            self.comments = os.path.join(self.base, "comments")
            self.comments_users = os.path.join(self.comments, "users")

    def _load_conf(self, args):
        if not args.collection:
            sys.exit("--collection is required")
        path = os.path.join(args.confs_dir, args.collection + ".yaml")
        if not os.path.isfile(path):
            sys.exit(
                "No configuration for `{}`: {} not found".format(args.collection, path)
            )
        try:
            import yaml
        except ImportError:
            sys.exit("PyYAML is not installed on this machine")
        with open(path) as fp:
            conf = yaml.safe_load(fp) or {}
        if conf.get("name") != args.collection:
            sys.exit(
                "{} is the configuration of `{}`, not of `{}`".format(
                    path, conf.get("name"), args.collection
                )
            )
        if not conf.get("queries"):
            sys.exit(
                "`queries` is not set in {}: fill in the legacy queries".format(path)
            )
        conf.setdefault("chunk_size", 1000)
        conf.setdefault("flags", {})
        return conf

    def ensure_dirs(self):
        for path in (self.dump, self.files, self.comments_users):
            _makedirs(path)

    def comments_queries(self):
        return self.conf.get("comments_queries") or query_texts(self.conf)


# --------------------------------------------------------------------------
# dump records
# --------------------------------------------------------------------------
def query_texts(conf):
    """The legacy queries of the configuration, as plain strings."""
    return [query for query, _ in dump_jobs(conf)]


def dump_jobs(conf):
    """(query, dump file prefix) for each query of the configuration.

    Every item of `queries` is a mapping `{q: ..., dump_file_prefix: ...}`. The
    prefix names the files of the dump, and of the tagging as MIGRATED, of the
    query, so it is mandatory and different for each query (one prefix may start
    another, e.g. `clic_notes` and `clic_notes_dp`).
    """
    jobs = []
    for item in conf["queries"]:
        if not isinstance(item, dict) or not item.get("q"):
            sys.exit(
                "Each query of `queries` must be `q: ...` with a "
                "`dump_file_prefix: ...`, got {!r}".format(item)
            )
        if not item.get("dump_file_prefix"):
            sys.exit(
                "The query `{}` has no `dump_file_prefix` (the name of its dump "
                "files, mandatory)".format(item["q"])
            )
        jobs.append((item["q"], item["dump_file_prefix"]))
    seen = {}
    for query, prefix in jobs:
        if prefix in seen:
            sys.exit(
                "The `dump_file_prefix` `{}` is used by two queries, set a "
                "different one on each:\n  {}\n  {}".format(prefix, seen[prefix], query)
            )
        seen[prefix] = query
    return jobs


def cmd_dump_records(ctx, args):
    ctx.ensure_dirs()
    conf = ctx.conf
    jobs = dump_jobs(conf)
    prefixes = [prefix for _, prefix in jobs]

    # dumped straight into the dump folder: the files of the same name are replaced
    for query, prefix in jobs:
        command = [
            "inveniomigrator",
            "dump",
            "records",
            "-q",
            "{} {}".format(query.strip(), DUMP_EXCLUSIONS),
            "--file-prefix",
            prefix,
            "--chunk-size={}".format(conf["chunk_size"]),
        ]
        # no shell is involved: the query is one argument, shown quoted to be pasted
        print(
            "Running in {}: {} '{}' {}".format(
                ctx.dump, " ".join(command[:4]), command[4], " ".join(command[5:])
            )
        )
        subprocess.check_call(command, cwd=_fs(ctx.dump))

    # the files of a query are `<dump_file_prefix>_<n>.json`: matched whole
    names = os.listdir(_fs(ctx.dump))
    total, recids, files = 0, set(), 0
    for query, prefix in jobs:
        pattern = re.compile(r"^{}(_\d+)?\.json$".format(re.escape(prefix)))
        dumped = sorted(name for name in names if pattern.match(name))
        count = 0
        print("{}: {}".format(prefix, query))
        for name in dumped:
            with open(_fs(os.path.join(ctx.dump, name))) as fp:
                records = json.load(fp)
            print("  {}: {} records".format(name, len(records)))
            count += len(records)
            recids.update(record.get("recid") for record in records)
        if not dumped:
            print("  no files found for this query")
        print("  {} records in {} file(s)".format(count, len(dumped)))
        total += count
        files += len(dumped)
    if not files:
        sys.exit(
            "No dump file matches the prefixes in {} (files there: {}). Run the "
            "`inveniomigrator` command printed above by hand in that folder and "
            "see how its files are named".format(ctx.dump, sorted(names))
        )
    print(
        "Dump placed in {}: {} file(s), {} records ({} different)".format(
            ctx.dump, files, total, len(recids)
        )
    )


# --------------------------------------------------------------------------
# dump comments
# --------------------------------------------------------------------------
def _export_comments_module():
    """Import subscripts/export_collection_comments.py (needs the legacy Invenio)."""
    sys.path.insert(0, SUBSCRIPTS_DIR)
    try:
        import export_collection_comments
    except ImportError as exc:
        sys.exit(
            "The legacy Invenio libraries are not available ({}): run this "
            "command with the legacy interpreter, e.g. `ipython {} -- dump "
            "comments`".format(exc, os.path.relpath(__file__))
        )
    return export_collection_comments


def cmd_dump_comments(ctx, args):
    ctx.ensure_dirs()
    export = _export_comments_module()

    recids = export.find_recids_with_comments(ctx.comments_queries())
    comments_metadata, users = export.dump_comments(recids)
    if not comments_metadata:
        print("No comments to migrate for this collection, nothing written.")
        return

    # do not rename: the runners expect exactly these names
    metadata_path = os.path.join(ctx.comments, "comments_metadata.json")
    print("Creating {} file...".format(metadata_path))
    with open(metadata_path, "w") as fp:
        json.dump(comments_metadata, fp)

    export.dump_users(
        users,
        os.path.join(ctx.comments_users, "active_commentors.json"),
        os.path.join(ctx.comments_users, "missing_commentors_from_ldap.json"),
    )


# --------------------------------------------------------------------------
# tag migrated
# --------------------------------------------------------------------------
LEGACY_RECORD_URL = "https://repository.cern/legacy/record/"
BIBUPLOAD = "sudo -u apache /opt/cdsweb/bin/bibupload --append"


def cmd_tag_migrated(ctx, args):
    """Write the bibupload files that tag the migrated records as MIGRATED.

    The records are those of the dump (the queries of the collection, with the
    standard exclusions), the ones already tagged are left out. The files are
    named after the `dump_file_prefix` of the query. Nothing is
    changed on the legacy system: the bibupload commands are printed.
    """
    try:
        from invenio.bibrecord import (
            record_add_field,
            record_get_field_values,
            record_xml_output,
        )
        from invenio.search_engine import get_record, search_pattern
    except ImportError as exc:
        sys.exit(
            "The legacy Invenio libraries are not available ({}): run this "
            "command with the legacy interpreter, e.g. `ipython {} -- tag "
            "migrated`".format(exc, os.path.relpath(__file__))
        )

    output = "/tmp/{}-migrated".format(ctx.conf["name"])
    _makedirs(output)
    for name in os.listdir(output):  # a stale file must not be uploaded again
        if name.endswith(".xml"):
            os.remove(os.path.join(output, name))

    paths = []
    seen = set()  # a record found by several queries is tagged once
    for query, prefix in dump_jobs(ctx.conf):
        found = search_pattern(p="{} {}".format(query.strip(), DUMP_EXCLUSIONS))
        recids = sorted(int(recid) for recid in found if int(recid) not in seen)
        seen.update(recids)
        pending = [
            recid
            for recid in recids
            if "MIGRATED"
            not in record_get_field_values(get_record(recid), "980", code="c")
        ]
        print(
            "{}: {} records to tag ({} already tagged or found by a previous "
            "query)".format(prefix, len(pending), len(found) - len(pending))
        )
        for index, start in enumerate(range(0, len(pending), args.chunk_size), 1):
            collection = "<collection>\n"
            for recid in pending[start : start + args.chunk_size]:
                record = {}
                record_add_field(record, "001", controlfield_value=str(recid))
                record_add_field(
                    record,
                    "970",
                    " ",
                    " ",
                    "",
                    [("d", LEGACY_RECORD_URL + str(recid))],
                )
                record_add_field(record, "980", " ", " ", "", [("c", "MIGRATED")])
                collection += record_xml_output(record)
            collection += "</collection>"
            path = os.path.join(output, "{}_migrated_{}.xml".format(prefix, index))
            with open(path, "w") as fp:
                fp.write(collection)
            paths.append(path)

    if not paths:
        print("No records to tag.")
        return
    print(
        "Written {} file(s) in {}. To tag the records, run:".format(len(paths), output)
    )
    for path in paths:
        print("  {} {}".format(BIBUPLOAD, path))


# --------------------------------------------------------------------------
# dump files
# --------------------------------------------------------------------------
def cmd_dump_files(ctx, args):
    ctx.ensure_dirs()
    sys.path.insert(0, SUBSCRIPTS_DIR)
    import copy_collection_files

    dump_files = sorted(
        path
        for path in copy_collection_files.get_dump_files_paths(ctx.dump)
        if path.endswith(".json")
    )
    if not dump_files:
        sys.exit("No dump files in {}: run `dump records` first".format(ctx.dump))
    skip_plots = not ctx.conf["flags"].get("include_plots")
    copy_collection_files.copy_collection_file(
        dump_files,
        ctx.files,
        ctx.base,
        skip_plots=skip_plots,
        legacy_files_root=args.legacy_files_root,
    )

    # files attached to the comments, one folder per record
    metadata_path = os.path.join(ctx.comments, "comments_metadata.json")
    if not os.path.isfile(metadata_path):
        print(
            "No {}: comments attachments not copied (run `dump comments` "
            "first if the collection has comments)".format(metadata_path)
        )
        return
    with open(metadata_path) as fp:
        recids = json.load(fp).keys()
    _export_comments_module().copy_attached_files(recids, ctx.comments)


# --------------------------------------------------------------------------
# dump users
# --------------------------------------------------------------------------
def cmd_dump_users(ctx, args):
    """Dump the `people` authority records: input of `people-run`/people.csv."""
    _makedirs(ctx.users)
    command = [
        "inveniomigrator",
        "dump",
        "records",
        "-q",
        '980__:"AUTHORITY" 980__:"PEOPLE"',
        "--file-prefix",
        "peoples",
        "--chunk-size=1000",
    ]
    print("Running in {}: {}".format(ctx.users, " ".join(command)))
    subprocess.check_call(command, cwd=ctx.users)
    print(
        "On the migration pod, if people.csv has to be recreated:\n"
        "  invenio migration users people-run --filepath {0}/people.csv "
        "--dirpath {0}".format(ctx.users)
    )


# --------------------------------------------------------------------------
# prepare
# --------------------------------------------------------------------------
def _without_analysis_exclusions(query):
    """The query without the exclusions the duplicates analysis must not have."""
    for exclusion in ANALYSIS_FORBIDDEN:
        query = re.sub(r"\s*{}(?=\s|$)".format(re.escape(exclusion)), "", query)
    return query.strip()


def cmd_prepare(ctx, args):
    """Find the duplicates to redirect and check the configuration."""
    conf = ctx.conf
    # the queries of the dump, searched together; the exclusions of the dump
    # queries are left out so the deleted/hidden/migrated records, which the
    # duplicates may be redirected to, are searched too
    if conf.get("analyze_query"):  # optional override of the queries
        analyze_queries = [conf["analyze_query"]]
    else:
        analyze_queries = [
            _without_analysis_exclusions(query) for query in query_texts(conf)
        ]
    ctx.ensure_dirs()

    # the analysis writes duplicated_pids.json and ep_report_number_dups.json
    # in the working directory of the collection
    command = [
        args.python,
        args.analyze_script,
        "--collection-name",
        conf["name"],
    ]
    for query in analyze_queries:
        command += ["--collection-q", query]
    command += ["--output-dir", ctx.base]
    print("Running: {}".format(" ".join(command)))
    subprocess.check_call(command)

    print("Review ep_report_number_dups.json, then continue on the migration pod.")


# --------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------
def build_parser():
    parser = argparse.ArgumentParser(
        description=__doc__.split("\n")[0],
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument(
        "--collection", help="collection name, as in collections/<collection>.yaml"
    )
    common.add_argument(
        "--confs-dir",
        default=CONFS_DIR,
        help="folder with the collection configurations (default: %(default)s)",
    )
    common.add_argument(
        "--env", help="override the environment read from the current directory"
    )
    common.add_argument(
        "--root",
        help="root of the migration data, the folder holding users/ and the "
        "collection folders (default: read from the EOS path, else the parent "
        "of the current directory; env CDS_MIGRATION_ROOT)",
    )

    commands = parser.add_subparsers(dest="command")
    commands.required = True

    dump = commands.add_parser("dump", help="dump records / comments / files / users")
    targets = dump.add_subparsers(dest="target")
    targets.required = True
    for name, func, needs_conf, text in (
        ("records", cmd_dump_records, True, "dump the records of the collection"),
        ("comments", cmd_dump_comments, True, "dump the comments and their commenters"),
        ("files", cmd_dump_files, True, "copy record and comments files to EOS"),
        ("users", cmd_dump_users, False, "dump the people authority records"),
    ):
        sub = targets.add_parser(name, parents=[common], help=text)
        sub.set_defaults(func=func, needs_conf=needs_conf)
        if name == "files":
            sub.add_argument(
                "--legacy-files-root",
                default=LEGACY_FILES_ROOT,
                help="where the legacy files are (default: %(default)s)",
            )

    tag = commands.add_parser("tag", help="tag the records on the legacy system")
    tag_targets = tag.add_subparsers(dest="target")
    tag_targets.required = True
    migrated = tag_targets.add_parser(
        "migrated",
        parents=[common],
        help="write the bibupload files that tag the migrated records as MIGRATED",
    )
    migrated.add_argument(
        "--chunk-size",
        type=int,
        default=500,
        help="records per bibupload file (default: %(default)s)",
    )
    migrated.set_defaults(func=cmd_tag_migrated, needs_conf=True)

    prepare = commands.add_parser(
        "prepare", parents=[common], help="find the duplicates to redirect"
    )
    prepare.add_argument(
        "--python", default="python2", help="interpreter of the analysis"
    )
    prepare.add_argument("--analyze-script", default=ANALYZE_SCRIPT)
    prepare.set_defaults(func=cmd_prepare, needs_conf=True)
    return parser


def main(argv=None):
    args = build_parser().parse_args(argv)
    args.func(Context(args, need_conf=args.needs_conf), args)


if __name__ == "__main__":
    main()
