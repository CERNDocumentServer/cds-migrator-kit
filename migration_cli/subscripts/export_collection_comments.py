"""
One-shot export of a collection's legacy comments (run on cds-migration-01).

Its functions are used by migration_cli/collection_migration.py (`dump comments`
and `dump files`), which is the way to run the export. Run on its own, it does,
in this order:
  1. dump the comments    -> comments_metadata.json
                             + active_commentors.json
                             + missing_commentors_from_ldap.json
  2. copy people.csv      -> <comments dir>/users/people.csv
  3. copy the attachments -> <comments dir>/<recid>/...

Usage (Python 2 / ipython of the legacy Invenio, so no f-strings here):

    ipython ./migration_cli/subscripts/export_collection_comments.py -- \
        --env dev --collection antares_restr --queries-file antares_queries.txt

    # or pass queries inline (repeatable)
    ipython ./migration_cli/subscripts/export_collection_comments.py -- \
        --env dev --collection antares_restr --query '980__a:PRIVANTARES ...'

The queries file has one legacy search query per line (blank lines and lines
starting with '#' are ignored). Use the same queries as for the collection
migration.

Output (everything under /eos/media/cds/cds-rdm/<env>/migration/<collection>/comments/):
    comments_metadata.json
    <recid>/<comment_id>/<attached files>
    users/people.csv, users/active_commentors.json,
    users/missing_commentors_from_ldap.json

Afterwards, on the migration pod, run
`invenio migration users commenters run --collection <collection>` and
`invenio migration comments run --collection <collection>`.

The script is safe to re-run: files are overwritten and already copied
attachment folders are skipped.
"""

import argparse
import io
import json
import os
import shutil
import sys

from invenio.bibcirculation_cern_ldap import get_user_info_from_ldap
from invenio.dbquery import run_sql
from invenio.search_engine import search_pattern
from invenio.webcomment_dblayer import get_comment_to_bibdoc_relations

EOS_ROOT = "/eos/media/cds/cds-rdm/{env}/migration"
LEGACY_COMMENTS_FILES_DIR = "/opt/cdsweb/var/data/comments"


# --------------------------------------------------------------------------
# Legacy comments extraction
# --------------------------------------------------------------------------
def get_comments_from_legacy(recid, display_order="cmt.date_creation ASC"):
    """Fetch all comments of a record.

    Mirrors invenio.webcomment.query_retrieve_comments_or_remarks.
    """
    query = """SELECT user.nickname,
                      user.email,
                      user.note,
                      user.last_login,
                      cmt.id_user,
                      DATE_FORMAT(cmt.date_creation, '%%%%Y-%%%%m-%%%%d %%%%H:%%%%i:%%%%s') as created,
                      cmt.body,
                      cmt.status,
                      cmt.nb_abuse_reports,
                      cmt.id,
                      cmt.round_name,
                      cmt.restriction,
                      %(reply_to_column)s as reply_to_id,
                      cmt.body_format
               FROM   cmtRECORDCOMMENT cmt LEFT JOIN user ON
                                              user.id=cmt.id_user
               WHERE cmt.id_bibrec=%%s
               ORDER BY %(display_order)s
               """ % {
        "display_order": display_order,
        "reply_to_column": recid > 0
        and "cmt.in_reply_to_id_cmtRECORDCOMMENT"
        or "cmt.in_reply_to_id_bskRECORDCOMMENT",
    }
    return run_sql(query, (recid,), with_dict=True)


def flatten_replies(comments_list):
    """Return top-level comments with all (nested) replies flattened to one level."""
    comments_by_id = {c["id"]: dict(c, replies=[]) for c in comments_list}
    top_level_comments = []

    for c in comments_list:
        parent = c.get("reply_to_id")
        if parent is None or parent not in comments_by_id:
            top_level_comments.append(comments_by_id[c["id"]])
        else:
            comments_by_id[parent]["replies"].append(comments_by_id[c["id"]])

    for comment in top_level_comments:
        flat = []
        queue = list(comment["replies"])
        while queue:
            reply = queue.pop(0)
            flat.append(reply)
            queue.extend(reply["replies"])
        for r in flat:
            r["replies"] = []
        flat.sort(key=lambda x: x["created"])
        comment["replies"] = flat

    return top_level_comments


def find_recids_with_comments(queries):
    """Return the recids matching any of the queries that have comments."""
    recids = []
    print("Querying already migrated records...")
    for i, collection_query in enumerate(queries):
        recs = search_pattern(p=collection_query)
        print(
            "({} / {}) Found {} records for query `{}`".format(
                i + 1, len(queries), len(recs), collection_query
            )
        )
        recids.extend(recs)

    recids = sorted(set(recids))
    if not recids:
        return []

    print("Filtering records with comments...")
    rows = run_sql(
        "SELECT id_bibrec "
        "FROM cmtRECORDCOMMENT "
        "WHERE id_bibrec IN ({}) "
        "GROUP BY id_bibrec "
        "HAVING COUNT(*) > 0".format(",".join(str(recid) for recid in recids)),
    )
    recids_with_comments = [row[0] for row in rows]
    print(
        "Found {} records with comments out of {} records".format(
            len(recids_with_comments), len(recids)
        )
    )
    return recids_with_comments


def _sanitize(body):
    return body.replace("\xc2\xa0", "&nbsp;").replace("\n", "").strip()


def dump_comments(recids):
    """Build the comments metadata (see CommentsRunner) and the commenters set."""
    comments_metadata = {}
    users_metadata = set()

    for i, recid in enumerate(recids):
        print(
            "({}/{}) Processing comments for record<{}>".format(
                i + 1, len(recids), recid
            )
        )
        comments = get_comments_from_legacy(recid)

        if not comments:
            print("No comments found for record<{}>. Skipping...".format(recid))
            continue

        # skip records whose comments are all deleted/spam, e.g.
        # https://cds.cern.ch/record/1367848/comments?ln=en
        if not any(comment["status"] in ["ok", "ap"] for comment in comments):
            print(
                "No 'ok'/'ap' comment found for record<{}>. Skipping...".format(recid)
            )
            continue

        print("Found `{}` comment(s) for record<{}>".format(len(comments), recid))
        comments_metadata[recid] = []

        # comments attached to a record's file (and file version); not to be
        # confused with files attached to the comments themselves
        relations = get_comment_to_bibdoc_relations(recid)
        file_relations = {
            r["id_comment"]: {"file_id": r["id_bibdoc"], "version": r["version"]}
            for r in relations
        }

        for comment in comments:
            users_metadata.add(
                (
                    comment["id_user"],
                    comment["email"],
                    comment["nickname"],
                    comment["note"],
                    comment["last_login"],
                )
            )

        for comment in flatten_replies(comments):
            comments_metadata[recid].append(
                {
                    "comment_id": comment["id"],
                    "content": _sanitize(comment["body"]),
                    "status": comment.get("status"),
                    "user_email": comment.get("email"),
                    "created_at": comment.get("created"),
                    "file_relation": file_relations.get(comment["id"], {}),
                    "replies": [
                        {
                            "comment_id": reply["id"],
                            "content": _sanitize(reply["body"]),
                            "status": reply.get("status"),
                            "user_email": reply.get("email"),
                            "created_at": reply.get("created"),
                            "reply_to_id": reply.get("reply_to_id"),
                            "file_relation": file_relations.get(reply["id"], {}),
                        }
                        for reply in comment["replies"]
                    ],
                }
            )

    return comments_metadata, users_metadata


# --------------------------------------------------------------------------
# Commenters (from dump_users.py in the production_scripts repository:
# https://gitlab.cern.ch/cds-team/production_scripts/-/blob/master/cds-rdm/migration/dump_users.py)
# --------------------------------------------------------------------------
def _ldap_field(ldap_user, field):
    try:
        return ldap_user[field][0]
    except Exception:
        return None


def dump_users(users, users_filepath, missing_users_filepath):
    """Write active_commentors.json and missing_commentors_from_ldap.json."""
    valid_users, missing_users = [], []

    for rec in users:
        email = rec[1]
        ldap_user = get_user_info_from_ldap(email=email)
        uid_number = _ldap_field(ldap_user, "uidNumber")
        record = {
            "id": rec[0],
            "email": rec[1],
            "displayname": rec[2],
            "active": rec[3],
        }
        if uid_number:
            record["uid"] = uid_number
            department = _ldap_field(ldap_user, "department")
            if department:
                record["department"] = department
            else:
                print("No department for {}".format(email))
            valid_users.append(record)
        else:
            missing_users.append(record)

    with open(users_filepath, "w") as fp:
        json.dump(valid_users, fp, indent=2)

    # always written (possibly empty), so the pod step never reads a stale file
    print(
        "Missing users found {}. Creating {} file...".format(
            len(missing_users), missing_users_filepath
        )
    )
    with open(missing_users_filepath, "w") as fp:
        json.dump(missing_users, fp, indent=2)


# --------------------------------------------------------------------------
# Files
# --------------------------------------------------------------------------
def copy_people_csv(env, users_dir):
    source = os.path.join(EOS_ROOT.format(env=env), "users", "people.csv")
    if not os.path.exists(source):
        print("WARNING: {} not found, copy people.csv manually!".format(source))
        return
    shutil.copy(source, os.path.join(users_dir, "people.csv"))
    print("Copied people.csv to {}".format(users_dir))


def copy_attached_files(recids, comments_dir):
    """Copy the files attached to comments, one folder per record."""
    copied = 0
    for recid in recids:
        source = os.path.join(LEGACY_COMMENTS_FILES_DIR, str(recid))
        destination = os.path.join(comments_dir, str(recid))
        if not os.path.isdir(source):
            continue  # no attachments for this record
        if os.path.exists(destination):
            print("Attachments of record<{}> already copied, skipping".format(recid))
            continue
        shutil.copytree(source, destination)
        copied += 1
        print("Copied {} comments directory to {}".format(recid, destination))
    print("Copied attachments of {} record(s)".format(copied))


# --------------------------------------------------------------------------
# Main
# --------------------------------------------------------------------------
def read_queries(args):
    queries = list(args.query or [])
    if args.queries_file:
        with io.open(args.queries_file, encoding="utf-8") as fp:
            for line in fp:
                line = line.strip()
                if line and not line.startswith("#"):
                    queries.append(line.encode("utf-8") if str is bytes else line)
    if not queries:
        sys.exit("No queries given: use --query and/or --queries-file")
    return queries


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[1])
    parser.add_argument("--env", required=True, help="dev / qa / prod")
    parser.add_argument("--collection", required=True, help="collection folder name")
    parser.add_argument("--query", action="append", help="legacy search query")
    parser.add_argument("--queries-file", help="file with one query per line")
    args = parser.parse_args()

    queries = read_queries(args)
    comments_dir = os.path.join(
        EOS_ROOT.format(env=args.env), args.collection, "comments"
    )
    users_dir = os.path.join(comments_dir, "users")
    for path in (comments_dir, users_dir):
        if not os.path.exists(path):
            os.makedirs(path)

    recids = find_recids_with_comments(queries)
    comments_metadata, users = dump_comments(recids)
    if not comments_metadata:
        sys.exit("No comments to migrate for this collection, nothing written.")

    comments_filepath = os.path.join(comments_dir, "comments_metadata.json")
    print("Creating {} file...".format(comments_filepath))
    with open(comments_filepath, "w") as fp:
        json.dump(comments_metadata, fp)

    dump_users(
        users,
        os.path.join(users_dir, "active_commentors.json"),
        os.path.join(users_dir, "missing_commentors_from_ldap.json"),
    )
    copy_people_csv(args.env, users_dir)
    copy_attached_files(comments_metadata.keys(), comments_dir)

    print("\nDone. Next, on the migration pod:")
    print(
        "  invenio migration users commenters run --collection {0}\n"
        "  invenio migration comments run --collection {0}".format(args.collection)
    )


if __name__ == "__main__":
    main()
