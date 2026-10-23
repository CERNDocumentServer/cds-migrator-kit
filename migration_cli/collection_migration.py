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

`dump comments` needs the legacy Invenio libraries: run it with the interpreter
that has them (`ipython $S/collection_migration.py -- dump comments ...`).
Python 2 compatible on purpose (legacy host).

Layout of the working directory:

    dump/                          records dump (`dump records`)
    comments/                      comments_metadata.json, <recid>/ attachments,
                                   users/missing_commentors_from_ldap.json
    files/                         legacy files, same relative paths as legacy
    files.log                      one line per copied file
    duplicated_pids.json           legacy recids to redirect (`prepare`)
    ep_report_number_dups.json     EP report numbers to review (`prepare`)

Every step can be re-run: copies are verified (size + checksum) and files that
are already correct are skipped.
"""

from __future__ import print_function

import argparse
import hashlib
import json
import os
import re
import shutil
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
COPY_RETRIES = 3
LEGACY_FILES_ROOT = "/opt/cdsweb/var/data/files/"

# Applied to every collection dump.
DUMP_EXCLUSIONS = (
    "-690C_:SCICOM -595:*ARCHIVE* -980:ANNOUNCEMENT -980:DELETED -980:HIDDEN "
    "-980__a:DUMMY -690C_:movingimages -970__b:MERGE-* -980:GREYBOOK"
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


def _hash(path, algo="sha256"):
    digest = hashlib.new(algo)
    with open(_fs(path), "rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _makedirs(path):
    if not os.path.isdir(_fs(path)):
        os.makedirs(_fs(path))


def copy_verified(source, destination):
    """Copy a file and check size + sha256 on the destination.

    EOS copies can end up truncated, so every copy is read back and compared;
    it is retried a few times. Returns False if the destination was already ok.
    """
    expected = _hash(source)
    if os.path.exists(_fs(destination)) and _hash(destination) == expected:
        return False
    for attempt in range(1, COPY_RETRIES + 1):
        shutil.copyfile(_fs(source), _fs(destination))
        if (
            os.path.getsize(_fs(source)) == os.path.getsize(_fs(destination))
            and _hash(destination) == expected
        ):
            return True
        print(
            "Copy verification failed ({}/{}): {}".format(
                attempt, COPY_RETRIES, destination
            )
        )
    raise IOError("Could not copy {} correctly".format(destination))


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
            self.staging = "/tmp/{}".format(self.conf["name"])

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
        conf.setdefault("filename_prefix", conf["name"])
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
    """The legacy queries of the configuration, as plain strings.

    An item of `queries` is a string, or a mapping `{q: ..., dump_file_prefix: ...}`.
    """
    texts = []
    for item in conf["queries"]:
        text = item.get("q") if isinstance(item, dict) else item
        if not text:
            sys.exit("A query of `queries` has no text (`q`): {!r}".format(item))
        texts.append(text)
    return texts


def dump_jobs(conf):
    """(query, dump file prefix) for each query, so the dump files never mix.

    The prefix is the `dump_file_prefix` of the query when it has one, else
    `filename_prefix` (one query) or `<filename_prefix>-<n>` (several queries).
    """
    base, count = conf["filename_prefix"], len(conf["queries"])
    jobs = []
    for index, (item, text) in enumerate(zip(conf["queries"], query_texts(conf))):
        prefix = item.get("dump_file_prefix") if isinstance(item, dict) else None
        if not prefix:
            prefix = base if count == 1 else "{}-{}".format(base, index)
        jobs.append((text, prefix))
    prefixes = [prefix for _, prefix in jobs]
    if len(set(prefixes)) != len(prefixes):
        sys.exit(
            "Two queries dump to the same file prefix {}: set a different "
            "`dump_file_prefix` on each".format(sorted(prefixes))
        )
    # a prefix that starts another one would claim its files when collecting them
    for prefix in prefixes:
        if any(other != prefix and other.startswith(prefix) for other in prefixes):
            sys.exit(
                "The dump file prefix `{}` is the start of another prefix: files "
                "could not be told apart, rename it".format(prefix)
            )
    return jobs


def cmd_dump_records(ctx, args):
    ctx.ensure_dirs()
    conf = ctx.conf
    if os.path.isdir(ctx.staging) and os.listdir(ctx.staging):
        sys.exit(
            "Staging folder {} is not empty, remove it first (a stale dump "
            "must not be mixed with the new one)".format(ctx.staging)
        )
    jobs = dump_jobs(conf)
    prefixes = [prefix for _, prefix in jobs]
    _makedirs(ctx.staging)

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
        print("Running: {}".format(" ".join(command)))
        subprocess.check_call(command, cwd=ctx.staging)

    dumped = sorted(
        name
        for name in os.listdir(ctx.staging)
        if any(name.startswith(prefix) for prefix in prefixes)
    )
    if not dumped:
        sys.exit("The dump produced no files in {}".format(ctx.staging))
    # dumped locally first, then copied checked: EOS can truncate a big write
    for name in dumped:
        copied = copy_verified(
            os.path.join(ctx.staging, name), os.path.join(ctx.dump, name)
        )
        print("{} {}".format("Copied" if copied else "Already there", name))
    shutil.rmtree(ctx.staging)
    print("Dump placed in {} ({} file(s))".format(ctx.dump, len(dumped)))


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
def cmd_prepare(ctx, args):
    """Find the duplicates to redirect and check the configuration."""
    conf = ctx.conf
    queries = query_texts(conf)
    analyze_query = conf.get("analyze_query") or (
        queries[0] if len(queries) == 1 else None
    )
    if not analyze_query:
        sys.exit(
            "The collection has several queries: set `analyze_query` in {} "
            "(one query, without -980:DELETED -980:HIDDEN -980__c:MIGRATED "
            "-980__a:DUMMY)".format("collections/{}.yaml".format(conf["name"]))
        )
    ctx.ensure_dirs()

    # the analysis writes duplicated_pids.json and ep_report_number_dups.json
    # in the working directory of the collection
    command = [
        args.python,
        args.analyze_script,
        "--collection-name",
        conf["name"],
        "--collection-q",
        analyze_query,
        "--output-dir",
        ctx.base,
    ]
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
