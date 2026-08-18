"""Analyze a CDS collection before migration.

Finds duplicate records (same title, description, and file checksums),
981__a merges, and checks for CERN-EP report numbers duplicates.

Steps:
    1. Connect to the CDS-legacy migration node
    2. Run this script with --collection-name and --collection-q.
    3. Duplicate recids and 981__a merges are written to
           /tmp/<collection_name>/legacy_pids_to_redirect.json
    4. EP report numbers shared with other CDS records are written to
           /tmp/<collection_name>/ep_report_number_dups.json

Do not use -980:DELETED -980:HIDDEN -980__c:MIGRATED -980__a:DUMMY in the
query. Include any 710__g clauses, e.g. 710__g:"IT" or 710__g:"CN".

Example:
    python2 scripts/analyze_collection_migration.py \\
        --collection-name faser \\
        --collection-q '980__a:ARTICLE or 980__a:PREPRINT and 693:"FASER"'
"""

import argparse
import json
import os
import re

from invenio.dbquery import run_sql
from invenio.search_engine import get_fieldvalues, record_exists, search_pattern

########################################################
# Constants
########################################################

EP_RE = re.compile(r"^(?:CERN-EP|CERN-PH-EP|CERN-PPE|CERN-TH-EP)-\d{2}(?:\d{2})?-\d+$")
RN_TAGS = (
    "037__a",
    "037__9",
    "084__a",
    "088__a",
    "088__9",
    "9031_b",
    "909C0r",
)
SEARCH_FIELDS = ("reportnumber", "037", "084", "088", "9031_", "909C0r")


def _print_green(msg):
    print("\033[32m{}\033[0m".format(msg))


########################################################
# Duplicate records
########################################################


def _search_recids(query):
    result = search_pattern(p=query)
    if hasattr(result, "tolist"):
        return [int(recid) for recid in result.tolist()]
    return [int(recid) for recid in result]


def search_dups(collection_query):
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
                    "SELECT checksum FROM bibdocfsinfo WHERE id_bibdoc = %s",
                    (id_bibdoc,),
                )
                for row in res:
                    checksum = row[0]
                    checksums.setdefault(checksum, [])
                    checksums[checksum].append((recid, status))

    _print_green(
        "Duplicated records to check with same title, description and file checksums:"
    )
    redirect_map = {}
    for recids_list in checksums.values():
        if len(recids_list) > 1:
            recid_to_keep = _choose_recid_to_keep(recids_list)
            recids_to_redirect = [
                rec[0] for rec in recids_list if rec[0] != recid_to_keep
            ]
            for recid_to_redirect in recids_to_redirect:
                redirect_map[recid_to_redirect] = recid_to_keep
    _print_green(
        "Found {} duplicates to check with same title, description and file checksums.".format(
            len(redirect_map)
        )
    )
    return redirect_map


def _choose_recid_to_keep(recids_list):
    """Keep a not-deleted record; prefer SSO among those, else the first.

    Deleted recids become redirect keys. If every recid is deleted, fall
    back to SSO then the first in the list.
    """
    not_deleted = [rec for rec in recids_list if record_exists(rec[0]) == 1]
    candidates = not_deleted if not_deleted else recids_list
    sso = [rec for rec in candidates if rec[1] == "SSO"]
    if sso:
        return sso[0][0]
    return candidates[0][0]


def search_merges(collection_query):
    """Dump redirects from MARC 981__a (duplicate record id), scoped to the collection.

    981__a marks recids that were merged into the host record (the surviving
    copy). The host (``id_bibrec``) is the redirect destination (dump value).
    Each 981__a value is a merged-away recid (redirect key). Values containing
    CER are ALEPH ids and are ignored.

    Include any 710__g clauses in ``collection_query`` (e.g. ``710__g:"IT"``).
    """
    if not collection_query:
        _print_green(
            "No collection configured; skipping 981__a (duplicate record id) merges."
        )
        return {}

    recs = _search_recids(collection_query)
    if not recs:
        _print_green(
            "No recids for collection query; skipping 981__a (duplicate record id) merges."
        )
        return {}
    recids_str = ",".join(str(recid) for recid in recs)
    rows = run_sql(
        "SELECT bibrec.id_bibrec, field.value FROM bibrec_bib98x as bibrec "
        "INNER JOIN bib98x AS field ON field.id = bibrec.id_bibxxx "
        "WHERE field.tag = '981__a' "
        "AND bibrec.id_bibrec IN ({})".format(recids_str),
        run_on_slave=True,
    )
    _print_green(
        "Found {} 981__a (duplicate record id) merge entries on collection recids.".format(
            len(rows)
        )
    )

    redirect_map = {}
    for dest_recid, merged_value in rows:
        if not merged_value or "CER" in str(merged_value).upper():
            continue
        try:
            src_recid = int(str(merged_value).strip())
        except (TypeError, ValueError):
            _print_green(
                "Skipping non-numeric 981__a (duplicate record id) value {} on {}".format(
                    merged_value, dest_recid
                )
            )
            continue
        dest_recid = int(dest_recid)
        if src_recid == dest_recid:
            continue
        redirect_map[src_recid] = dest_recid

    return redirect_map


########################################################
# EP report number duplicates
########################################################


def recids_from(query):
    recs = search_pattern(p=query)
    recs = recs.tolist() if hasattr(recs, "tolist") else list(recs)
    return [int(r) for r in recs]


def ep_numbers_for(recid):
    found = set()
    for tag in RN_TAGS:
        for val in get_fieldvalues(recid, tag) or []:
            val = (val or "").strip()
            if EP_RE.match(val):
                found.add(val)
    return found


def recids_with(report_number):
    """Find every CDS record with this report number (not limited to the collection)."""
    quoted = '"{}"'.format(report_number.replace('"', ""))
    # Build the query, example: 037:"reportnumber" or 084:"" or 088:"" or 9031_:""
    recids = set(
        recids_from(
            " or ".join("{}:{}".format(field, quoted) for field in SEARCH_FIELDS)
        )
    )
    return recids


def search_ep_report_number_dups(collection_query):
    """Return {ep_report_number: [recids]} for EP numbers found in the collection.

    The collection query only chooses which records to read ep reportnumbers from.
    For each ep report number, recids_with() searches all of CDS.
    """
    ep_map = {}
    for recid in recids_from(collection_query):
        for rn in ep_numbers_for(recid):
            if rn not in ep_map:
                ep_map[rn] = recids_with(rn)
            ep_map[rn].add(recid)
    return {rn: sorted(recids) for rn, recids in ep_map.items()}


def parse_args():
    parser = argparse.ArgumentParser(
        description=(
            "Find duplicate records and CERN-EP report numbers for a collection."
        )
    )
    parser.add_argument(
        "--collection-name",
        required=True,
        help="Collection name used for the dump directory (/tmp/<collection_name>).",
    )
    parser.add_argument(
        "--collection-q",
        required=True,
        help=(
            "CDS search query for the collection. Do not use -980:DELETED "
            "-980:HIDDEN -980__c:MIGRATED -980__a:DUMMY. Include any 710__g "
            'clauses, e.g. 710__g:"IT" or 710__g:"CN".'
        ),
    )
    return parser.parse_args()


def main():
    args = parse_args()
    collection_name = args.collection_name
    collection_q = args.collection_q

    json_dump_dir = "/tmp/{}".format(collection_name)
    if not os.path.exists(json_dump_dir):
        os.makedirs(json_dump_dir)

    _print_green("--------------------------------")
    _print_green("Collection: {}".format(collection_name))
    _print_green("--------------------------------")
    _print_green("Collection query: {}".format(collection_q))
    _print_green("--------------------------------")
    _print_green("Searching for duplicate records...")
    redirect_map = search_dups(collection_q)
    redirect_map.update(search_merges(collection_q))

    with open("{}/legacy_pids_to_redirect.json".format(json_dump_dir), "w+") as fp:
        json.dump(redirect_map, fp)
    _print_green(
        "Wrote {} duplicates to {}".format(
            len(redirect_map),
            "{}/legacy_pids_to_redirect.json".format(json_dump_dir),
        )
    )

    _print_green("--------------------------------")
    _print_green("Searching for EP report number duplicates...")
    ep_map = search_ep_report_number_dups(collection_q)
    dups = {rn: recids for rn, recids in ep_map.items() if len(recids) > 1}
    out_path = "{}/ep_report_number_dups.json".format(json_dump_dir)
    with open(out_path, "w") as f:
        json.dump(dups, f, indent=2, sort_keys=True)
    _print_green("Wrote {} duplicates to {}".format(len(dups), out_path))


if __name__ == "__main__":
    main()
