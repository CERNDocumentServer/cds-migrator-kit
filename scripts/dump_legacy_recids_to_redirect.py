import json

from invenio.dbquery import run_sql
from invenio.search_engine import search_pattern

collection_query = "037__:CERN-STUDENTS-Note-* - 980__c:DELETED"
json_dump_dir = "/eos/media/cds/cds-rdm/dev/migration/it_dep"

recs = search_pattern(p=collection_query)
recids_str = ",".join([str(recid) for recid in recs])

duplicate_records_by_description = run_sql(
    "SELECT bibrec_bib52x.id_bibrec, bib52x.value "
    "FROM bib52x "
    "INNER JOIN bibrec_bib52x "
    "ON bib52x.id = bibrec_bib52x.id_bibxxx "
    "WHERE bib52x.tag = '520__a' "
    "AND bibrec_bib52x.id_bibrec IN ({}) "
    "AND bib52x.value IN ( "
    "    SELECT bib52x.value "
    "    FROM bib52x "
    "    INNER JOIN bibrec_bib52x "
    "    ON bib52x.id = bibrec_bib52x.id_bibxxx "
    "    WHERE bib52x.tag = '520__a' "
    "    AND bibrec_bib52x.id_bibrec IN ({}) "
    "    GROUP BY bib52x.value "
    "    HAVING COUNT(*) > 1 "
    ") "
    "ORDER BY bibrec_bib52x.id_bibrec; ".format(recids_str, recids_str),
    run_on_slave=True,
)


duplicate_records_by_title = run_sql(
    "SELECT bibrec_bib24x.id_bibrec, bib24x.value "
    "FROM bib24x "
    "INNER JOIN bibrec_bib24x "
    "ON bib24x.id = bibrec_bib24x.id_bibxxx "
    "WHERE bib24x.tag = '245__a' "
    "AND bibrec_bib24x.id_bibrec IN ({}) "
    "AND bib24x.value IN ( "
    "    SELECT bib24x.value "
    "    FROM bib24x "
    "    INNER JOIN bibrec_bib24x "
    "    ON bib24x.id = bibrec_bib24x.id_bibxxx "
    "    WHERE bib24x.tag = '245__a' "
    "    AND bibrec_bib24x.id_bibrec IN ({}) "
    "    GROUP BY bib24x.value "
    "    HAVING COUNT(*) > 1 "
    ") "
    "ORDER BY bibrec_bib24x.id_bibrec; ".format(recids_str, recids_str),
    run_on_slave=True,
)

dupl_recids_by_desc = [obj[0] for obj in duplicate_records_by_description]
dupl_recids_by_title = [obj[0] for obj in duplicate_records_by_title]
possible_duplicates = list(set(dupl_recids_by_desc) & set(dupl_recids_by_title))

checksums = {}
for recid in possible_duplicates:
    res = run_sql(
        "SELECT bibrec_bibdoc.id_bibdoc, bibrec_bibdoc.docname, bibdoc.status FROM bibrec_bibdoc INNER JOIN bibdoc ON bibdoc.id=bibrec_bibdoc.id_bibdoc WHERE bibrec_bibdoc.id_bibrec = %s",
        (recid,),
    )
    if not res:
        continue

    for row in res:
        id_bibdoc, docname, status = row[0], row[1], row[2]
        # omit deleted files
        if status != "DELETED":
            res = run_sql(
                "SELECT checksum FROM bibdocfsinfo WHERE id_bibdoc = %s", (id_bibdoc,)
            )
            for row in res:
                checksum = row[0]
                checksums.setdefault(checksum, [])
                checksums[checksum].append((recid, status))

print("Duplicated records to check with same title, description and file checksums:")
redirect_map = {}
for recids_list in checksums.values():
    if len(recids_list) > 1:
        # find if there is any of recids with restricted files i.e. status='SSO'
        SSO_recid = [rec for rec in recids_list if rec[1] == "SSO"]
        if SSO_recid:
            recid_to_keep = SSO_recid[0][0]
        else:
            recid_to_keep = recids_list[0][0]
        recids_to_redirect = [rec[0] for rec in recids_list if rec[0] != recid_to_keep]
        for recid_to_redirect in recids_to_redirect:
            redirect_map[recid_to_redirect] = recid_to_keep
            print(
                "Redirect recid: {} to recid: {}".format(
                    recid_to_redirect, recid_to_keep
                )
            )

# This map is calculated manually and it should be adjusted per collection
MANUAL_REDIRECT_MAP = {"2913580": 2913581, "2907803": 2907804, "2909209": 2909210}

redirect_map.update(MANUAL_REDIRECT_MAP)

with open("{}/legacy_pids_to_redirect.json".format(json_dump_dir), "w+") as fp:
    json.dump(redirect_map, fp)
