"""
This script is used to get the comments from the legacy system and save them to a json file.
It also dumps the users metadata into valid_users.json and missing_users.json files.
"""

"""
1. Find migrated records with comments using https://digital-repositories.web.cern.ch/cds/cds.cern.ch/migration/#verify-if-the-collection-has-commentsdiscussions
2. Extract and sanitize comments (query_retrieve_comments_or_remarks)
3. Map legacy user id to cdsrdm user id
4. Create comments metadata
5. Save comments metadata to json file
6. Dump the users metadata into valid_users.json and missing_users.json files.
"""

import json
import os

from invenio.bibcirculation_cern_ldap import get_user_info_from_ldap
from invenio.dbquery import run_sql
from invenio.search_engine import search_pattern
from invenio.webcomment_dblayer import get_comment_to_bibdoc_relations

ENV = "dev"
BASE_OUTPUT_DIR = f"/eos/media/cds/cds-rdm/{ENV}/migration/"
COMMENTS_METADATA_FILEPATH = os.path.join(
    BASE_OUTPUT_DIR, "comments", "comments_metadata.json"
)

collection_queries = [
    "980__a:CNLISSUE -980:DELETED -980:HIDDEN -980__a:DUMMY",
    "980__a:CNLARTICLE -980:DELETED -980:HIDDEN -980__a:DUMMY",
    "710__5:CN OR 710__5:DD OR 710__5:IT OR 710__5:AS OR 710__5:STS 980:ARTICLE -980:DELETED -980:HIDDEN -980__a:DUMMY -980:INTNOTECMSPUBL",
    "710__5:CN OR 710__5:DD OR 710__5:IT OR 710__5:AS OR 710__5:STS 980:PREPRINT -980:DELETED -980:HIDDEN -980__a:DUMMY -980:INTNOTECMSPUBL",
    "980:INTNOTEITPUBL or 980:INTNOTEASPUBL OR 980:INTNOTEMIPUBL AND 690C:INTNOTE -980:DELETED -980:HIDDEN -980__a:DUMMY -980:INTNOTECMSPUBL",
    "710__5:IT 980:PUBLARDA -980:DELETED -980:HIDDEN -980__a:DUMMY",
    "980:REPORT AND 710__5:IT -980:DELETED -980:HIDDEN -980__a:DUMMY",
    "980:PERI AND 710__5:IT -980:DELETED -980:HIDDEN -980__a:DUMMY",
    "980:BROCHURE AND 690C:CERNITBROCHURE -980:DELETED -980:HIDDEN -980__a:DUMMY",
    "980:POSTER AND 710__5:IT -980:DELETED -980:HIDDEN -980__a:DUMMY",
    "980:ITCERNTALK -980:DELETED -980:HIDDEN -980__a:DUMMY",
    '980:PERI AND 650:"Computing and Computers" -980:DELETED -980:HIDDEN -980__a:DUMMY',
    "980:ITUDSPUBSOURCEARCHIVE -980:DELETED -980:HIDDEN -980__a:DUMMY",
]
"""
For thesis, change the query and re-run this script: "(980__:THESIS OR 980__:Thesis OR 980__:thesis) -980__:DUMMY -980__c:HIDDEN"
"""
recids_list = []
print("Querying already migrated records...")
for i, collection_query in enumerate(collection_queries):
    recs = search_pattern(p=collection_query)
    print(
        "({} / {}) Found {} records for query `{}`".format(
            i + 1, len(collection_queries), len(recs), collection_query
        )
    )
    recids_list.extend(recs)

print("Filtering records with comments...")
records_with_comments = run_sql(
    "SELECT id_bibrec "
    "FROM cmtRECORDCOMMENT "
    "WHERE id_bibrec IN ({}) "
    "GROUP BY id_bibrec "
    "HAVING COUNT(*) > 0".format(",".join([str(recid) for recid in recids_list])),
)

recids_with_comments = [record[0] for record in records_with_comments]
print(
    "Found {} records with comments out of {} migrated records".format(
        len(recids_with_comments), len(recids_list)
    )
)


def get_comments_from_legacy(recid, display_order="cmt.date_creation ASC"):
    """
    # from invenio.webcomment import query_retrieve_comments_or_remarks
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
    params = (recid,)
    res = run_sql(query, params, with_dict=True)
    return res


# Function to flatten arbitrarily nested comment replies into a 1-level replies list
def flatten_replies(comments_list):
    """
    Takes a list of comments (dicts with at least id, reply_to_id), and
    returns a list of top-level comments each with direct and indirect (flattened)
    replies under a single 'replies' key (1-level nesting).
    Each comment dict may have its own 'replies' key (list).
    """
    # Build maps for quick lookup
    comments_by_id = {c["id"]: dict(c, replies=[]) for c in comments_list}
    top_level_comments = []

    for c in comments_list:
        parent = c.get("reply_to_id")
        if parent is None or parent not in comments_by_id:
            # This is a top-level comment
            top_level_comments.append(comments_by_id[c["id"]])
        else:
            # This is a reply; add to parent's replies
            comments_by_id[parent]["replies"].append(comments_by_id[c["id"]])

    def collect_all_replies(comment):
        """Recursively flattens all indirect replies under this comment."""
        flat = []
        queue = list(comment["replies"])
        while queue:
            reply = queue.pop(0)
            flat.append(reply)
            # Add all replies to the end of the queue
            queue.extend(reply["replies"])
        # Remove their nested replies again for full flatten
        for r in flat:
            r["replies"] = []
        # Sort the replies by creation date
        flat.sort(key=lambda x: x["created"])
        comment["replies"] = flat

    for comment in top_level_comments:
        collect_all_replies(comment)

    return top_level_comments


comments_metadata = {}
"""
{
    recid: [
        {
            comment_id: comment_id,
            content: content,
            status: status,
            user_email: user_email,
            created_at: created_at,
            file_relation: {file_id: file_id, version: version},
            replies: [
                {
                    comment_id: reply_comment_id,
                    content: reply_comment_content,
                    status: reply_comment_status,
                    user_email: reply_comment_user_email,
                    created_at: reply_comment_created_at,
                    file_relation: {file_id: file_id, version: version},
                    reply_to_id: id_of_the_comment_replied_to,
                }
            ]
        }
    ]
}
"""

users_metadata = []
"""
[(user_id, user_email, user_nickname, user_note, user_last_login), ...]
"""

for i, recid in enumerate(recids_with_comments):
    print(
        "({}/{}) Processing comments for record<{}>".format(
            i + 1, len(recids_with_comments), recid
        )
    )
    comments = get_comments_from_legacy(recid)

    if not comments:
        print("No comments found for record<{}>. Skipping...".format(recid))
        continue

    # Check if the comments list have atleast 1 comment with 'ok' status or 'approved' status to avoid migrating records with spam comments that are already deleted
    # For eg.: https://cds.cern.ch/record/1367848/comments?ln=en
    if not any(comment["status"] in ["ok", "ap"] for comment in comments):
        print(
            "No comments with atleast one 'ok'/'ap' status found for record<{}>. Skipping...".format(
                recid
            )
        )
        continue

    print("Found `{}` comment(s) for record<{}>".format(len(comments), recid))
    comments_metadata[recid] = []

    # `get_comment_to_bibdoc_relations` is used to find if comments are attached to the record's files (and the version of the files)
    # This is not the same as the files attached to the comments
    comments_to_file_relations = get_comment_to_bibdoc_relations(recid)
    comment_to_version_relations = {}
    for relation in comments_to_file_relations:
        comment_to_version_relations[relation["id_comment"]] = {
            "file_id": relation["id_bibdoc"],
            "version": relation["version"],
        }
    print(
        "Found {} comments to file relations for record {} ...".format(
            len(comments_to_file_relations), recid
        )
    )

    for comment in comments:
        users_metadata.append(
            (
                comment["id_user"],
                comment["email"],
                comment["nickname"],
                comment["note"],
                comment["last_login"],
            ),
        )

    # Flatten the reply comments
    flattened_comments = flatten_replies(comments)
    # Sanitize the comment metadata for RDM
    for comment in flattened_comments:
        comment_data = {
            "comment_id": comment["id"],
            "content": comment["body"]
            .replace("\xc2\xa0", "&nbsp;")
            .replace("\n", "")
            .strip(),
            "status": comment.get("status"),
            "user_email": comment.get("email"),
            "created_at": comment.get("created"),
            "file_relation": comment_to_version_relations.get(comment["id"], {}),
            "replies": [
                {
                    "comment_id": reply["id"],
                    "content": reply["body"]
                    .replace("\xc2\xa0", "&nbsp;")
                    .replace("\n", "")
                    .strip(),
                    "status": reply.get("status"),
                    "user_email": reply.get("email"),
                    "created_at": reply.get("created"),
                    "reply_to_id": reply.get("reply_to_id"),
                    "file_relation": comment_to_version_relations.get(reply["id"], {}),
                }
                for reply in comment["replies"]
            ],
        }
        comments_metadata[recid].append(comment_data)
    print("Successfully processed comment(s) for record<{}>!!!".format(recid))

with open(COMMENTS_METADATA_FILEPATH, "w") as f:
    json.dump(comments_metadata, f)
"""
This file will be read and run by the CommentsRunner to migrate the comments.
"""

"""
The following snippet is taken from the `dump_users.py` script in the `production_scripts` repository:
https://gitlab.cern.ch/cds-team/production_scripts/-/blob/master/cds-rdm/migration/dump_users.py?ref_type=heads

It is used to dump the users metadata as valid_users.json and missing_users.json files.

After running this script, place the "active_users.json" and "missing_users.json" files in the "cds_migrator_kit/rdm/data/users/" folder along with "people.csv" file.
"""

OUTPUT_DIR = os.path.join(BASE_OUTPUT_DIR, "users")
USERS_FILEPATH = os.path.join(OUTPUT_DIR, "active_users.json")
MISSING_USERS_FILEPATH = os.path.join(OUTPUT_DIR, "missing_users.json")

if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)


def dump_users():
    """
    Dump the users metadata into valid_users.json and missing_users.json files.
    """

    def get_uid_from_ldap_user(ldap_user):
        try:
            uidNumber = ldap_user["uidNumber"][0]
            return uidNumber
        except:
            return None

    def get_department_from_ldap_user(ldap_user):
        try:
            department = ldap_user["department"][0]
            return department
        except:
            return None

    def _dump(recs):
        valid_users = []
        missing_users = []

        for rec in recs:
            # record example: (
            #   414320
            #   joe.doe@mail.com
            # )

            email = rec[1]
            ldap_user = get_user_info_from_ldap(email=email)
            uidNumber = get_uid_from_ldap_user(ldap_user)
            record = {
                "id": rec[0],
                "email": rec[1],
                "displayname": rec[2],
                "active": rec[3],
            }
            if uidNumber:
                record["uid"] = uidNumber
                department = get_department_from_ldap_user(ldap_user)
                if department:
                    record["department"] = department
                else:
                    print("No department for {}".format(email))
                valid_users.append(record)
            else:
                missing_users.append(record)

        return valid_users, missing_users

    valid_users, missing_users = _dump(users_metadata)
    with open(USERS_FILEPATH, "w") as fp:
        json.dump(valid_users, fp, indent=2)

    if missing_users:
        print("Missing users found {0}".format(len(missing_users)))
        with open(MISSING_USERS_FILEPATH, "w") as fp:
            json.dump(missing_users, fp, indent=2)
