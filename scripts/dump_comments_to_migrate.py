"""
This script is used to get the comments from the legacy system and save them to a json file.
"""

"""
1. Find migrated records with comments using https://digital-repositories.web.cern.ch/cds/cds.cern.ch/migration/#verify-if-the-collection-has-commentsdiscussions
2. Extract and sanitize comments (query_retrieve_comments_or_remarks)
3. Map legacy user id to cdsrdm user id
4. Create comments metadata
5. Save comments metadata to json file
"""

from invenio.dbquery import run_sql
from invenio.search_engine import search_pattern
from invenio.webcomment_dblayer import get_comment_to_bibdoc_relations
import json

collection_queries=[
    '980__a:CNLISSUE -980:DELETED -980:HIDDEN -980__a:DUMMY',
    '980__a:CNLARTICLE -980:DELETED -980:HIDDEN -980__a:DUMMY',
    '710__5:CN OR 710__5:DD OR 710__5:IT OR 710__5:AS OR 710__5:STS 980:ARTICLE -980:DELETED -980:HIDDEN -980__a:DUMMY -980:INTNOTECMSPUBL',
    '710__5:CN OR 710__5:DD OR 710__5:IT OR 710__5:AS OR 710__5:STS 980:PREPRINT -980:DELETED -980:HIDDEN -980__a:DUMMY -980:INTNOTECMSPUBL',
    '980:INTNOTEITPUBL or 980:INTNOTEASPUBL OR 980:INTNOTEMIPUBL AND 690C:INTNOTE -980:DELETED -980:HIDDEN -980__a:DUMMY -980:INTNOTECMSPUBL',
    '710__5:IT 980:PUBLARDA -980:DELETED -980:HIDDEN -980__a:DUMMY',
    '980:REPORT AND 710__5:IT -980:DELETED -980:HIDDEN -980__a:DUMMY',
    '980:PERI AND 710__5:IT -980:DELETED -980:HIDDEN -980__a:DUMMY',
    '980:BROCHURE AND 690C:CERNITBROCHURE -980:DELETED -980:HIDDEN -980__a:DUMMY',
    '980:POSTER AND 710__5:IT -980:DELETED -980:HIDDEN -980__a:DUMMY',
    '980:ITCERNTALK -980:DELETED -980:HIDDEN -980__a:DUMMY',
    '980:PERI AND 650:"Computing and Computers" -980:DELETED -980:HIDDEN -980__a:DUMMY',
    '980:ITUDSPUBSOURCEARCHIVE -980:DELETED -980:HIDDEN -980__a:DUMMY',
    '(980__:THESIS OR 980__:Thesis OR 980__:thesis) -980__:DUMMY -980__c:HIDDEN',
]
"""
For thesis: "(980__:THESIS OR 980__:Thesis OR 980__:thesis) -980__:DUMMY -980__c:HIDDEN"
"""
recids_list= []
for collection_query in collection_queries:
    recs = search_pattern(p=collection_query)
    recids_list.extend(recs)

records_with_comments = run_sql(
    "SELECT id_bibrec "
    "FROM cmtRECORDCOMMENT "
    "WHERE id_bibrec IN ({}) "
    "GROUP BY id_bibrec "
    "HAVING COUNT(*) > 0".format(",".join([str(recid) for recid in recids_list])),
)

recids_with_comments = [record[0] for record in records_with_comments]

def get_comments_from_legacy(recid, display_order='cmt.date_creation ASC'):
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
               'display_order': display_order,
               'reply_to_column':  recid > 0 and 'cmt.in_reply_to_id_cmtRECORDCOMMENT' or 'cmt.in_reply_to_id_bskRECORDCOMMENT'}
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
    comments_by_id = {c['id']: dict(c, replies=[]) for c in comments_list}
    top_level_comments = []

    for c in comments_list:
        parent = c.get('reply_to_id')
        if parent is None or parent not in comments_by_id:
            # This is a top-level comment
            top_level_comments.append(comments_by_id[c['id']])
        else:
            # This is a reply; add to parent's replies
            comments_by_id[parent]['replies'].append(comments_by_id[c['id']])

    def collect_all_replies(comment):
        """Recursively flattens all indirect replies under this comment. """
        flat = []
        queue = list(comment['replies'])
        while queue:
            reply = queue.pop(0)
            flat.append(reply)
            # Add all replies to the end of the queue
            queue.extend(reply['replies'])
        # Remove their nested replies again for full flatten
        for r in flat:
            r['replies'] = []
        # Sort the replies by creation date
        flat.sort(key=lambda x: x['created'])
        comment['replies'] = flat

    for comment in top_level_comments:
        collect_all_replies(comment)

    return top_level_comments

comments_metadata = {}
"""
{
    recid: [ # TODO: Can be nested with versions if multiple versions are present (to be finalized in discussion)
        {
            "comment_id": comment_id,
            "content": content,
            "status": status,
            "user_email": user_email,
            "created_at": created_at,
            'attached_files': [{"id": file_id, "path": file_path, "link": file_link}],
            "replies": [
                {
                    "comment_id": reply_comment_id,
                    "content": reply_comment_content,
                    "status": reply_comment_status,
                    "user_email": reply_comment_user_email,
                    "created_at": reply_comment_created_at,
                    'attached_files': [{"id": file_id, "path": file_path, "link": file_link}],
                    'reply_to_id': id_of_the_comment_replied_to,
                }
            ]
        }
    ]
}
"""

users_metadata = {}
"""
{
    user_id: (user_id, user_email, user_nickname, user_note, user_last_login),
    ...,
}
"""

for i, recid in enumerate(recids_with_comments):
    print("({}/{}) Processing comment for record {} -".format(i+1, len(recids_with_comments), recid))
    comments = get_comments_from_legacy(recid)

    if not comments:
        print("No comments found for record {}. Skipping...".format(recid))
        continue

    # Check if the comments list have atleast 1 comment with 'ok' status or 'approved' status to avoid migrating records with spam comments that are already deleted
    # For eg.: https://cds.cern.ch/record/1367848/comments?ln=en
    if not any(comment['status'] in ['ok', 'ap'] for comment in comments):
        print("No comments with atleast one 'ok'/'ap' status found for record {}. Skipping...".format(recid))
        continue

    print("Found {} comments for record {} ...".format(len(comments), recid))
    comments_metadata[recid] = []

    comments_to_file_relations = get_comment_to_bibdoc_relations(recid)
    comment_to_version_relations = {}
    for relation in comments_to_file_relations:
        comment_to_version_relations[relation['id_comment']] = relation['version'] # TODO: Not used for now, but can be used in the future to map the comment to the version
    print("Found {} comments to file relations for record {} ...".format(len(comments_to_file_relations), recid))

    for comment in comments:
        users_metadata[comment['id_user']] = (comment['id_user'], comment['email'], comment['nickname'], comment['note'], comment['last_login'])

    # Flatten the reply comments
    flattened_comments = flatten_replies(comments)
    # Sanitize the comment metadata for RDM
    for comment in flattened_comments:
        comment_data = {
            "comment_id": comment['id'],
            "content": comment['body'].replace('\xc2\xa0', '&nbsp;').replace('\n', '').strip(),
            "status": comment.get('status'),
            "user_email": comment.get('email'),
            "created_at": comment.get('created'),
            "replies": [
                {
                    "comment_id": reply['id'],
                    "content": reply['body'].replace('\xc2\xa0', '&nbsp;').replace('\n', '').strip(),
                    "status": reply.get('status'),
                    "user_email": reply.get('email'),
                    "created_at": reply.get('created'),
                    'reply_to_id': reply.get('reply_to_id'),
                }
                for reply in comment['replies']
            ]
        }
        comments_metadata[recid].append(comment_data)
    print("({}/{}) Successfully processed comments for record {}!".format(i+1, len(recids_with_comments), recid))

with open('comments_metadata.json', 'w') as f:
    json.dump(comments_metadata, f)

with open('users_metadata.json', 'w') as f:
    json.dump(users_metadata, f)
"""
This file will be read and then this script (with some tweaks) can be run to find out the missing users in the new system.
https://gitlab.cern.ch/cds-team/production_scripts/-/blob/master/cds-rdm/migration/dump_users.py?ref_type=heads

TODO: Might not be needed as we migrated the whole users table. To be discussed with someone.
TODO: Might need to also fix the script since it returns missing for users migrated as inactive.
"""
