"""Find records of a collection that are duplicated elsewhere in CDS.

analyze_collection_migration.py only finds duplicates inside the collection.
This script looks for copies of the collection records anywhere in CDS.

Checks:
    1. Report numbers (always): every report number of the collection records
       is looked up with an exact match across all of CDS. Report numbers
       shared with records outside the collection are written to
           <output dir>/report_number_cross_dups.json
    2. Soft title match (--soft-title-match, fuzzy): titles are normalised
       (case, accents, punctuation, hyphenation and spaces removed) and
       compared across all of CDS. Matches are written to
           <output dir>/soft_title_cross_dups.json
       This can give false positives and must be reviewed by hand. It scans
       the whole bib24x table.

Deleted records are ignored by default (--ignore-deleted False to keep them).
The output is {recid_in_collection: [recids outside], ...}; --verbose gives
details per report number / title instead.

Nothing is written to duplicated_pids.json: the results are for review only.

Example:
    python2 find_cross_collection_duplicates.py \\
        --collection-q '980__a:ARTICLE and 693:"FASER"' \\
        --output-dir /eos/.../faser \\
        --soft-title-match
"""

import argparse
import json
import os
import re
import unicodedata

from invenio.dbquery import run_sql
from invenio.search_engine import get_fieldvalues, record_exists, search_pattern

########################################################
# Constants
########################################################

# Report number tags -> bibxxx table. 037__9 (source, e.g. "arXiv") and
# 084__a (PACS codes) are left out: they are not unique identifiers.
RN_TAGS = {
    "037__a": "03x",
    "088__a": "08x",
    "088__9": "08x",
    "9031_b": "90x",
    "909C0r": "90x",
}
TITLE_TAGS = ("245__a", "245__b", "246__a", "246__b", "242__a")
# Normalised titles shorter than this are too generic ("Minutes", "Seminar").
MIN_TITLE_KEY_LEN = 20
SQL_CHUNK = 1000
TITLE_SCAN_CHUNK = 100000


def _print_green(msg):
    print("\033[32m{}\033[0m".format(msg))


def _print_yellow(msg):
    print("\033[33m{}\033[0m".format(msg))


def _chunks(seq, size):
    seq = list(seq)
    for i in range(0, len(seq), size):
        yield seq[i : i + size]


def _search_recids(queries):
    recids = set()
    for query in queries:
        recids.update(int(recid) for recid in list(search_pattern(p=query)))
    return recids


_EXISTS_CACHE = {}


def _exists(recid):
    if recid not in _EXISTS_CACHE:
        _EXISTS_CACHE[recid] = record_exists(recid)
    return _EXISTS_CACHE[recid]


def _status(recid):
    return {1: "exists", -1: "deleted"}.get(_exists(recid), "missing")


def _keep(recid, ignore_deleted):
    """False for missing records, and for deleted ones if ignore_deleted."""
    status = _exists(recid)
    if status == 0:
        return False
    return status == 1 or not ignore_deleted


def _format(matches, simple):
    """matches: {key: (inside recids, outside recids)}.

    simple: {recid_in_collection: [recids outside]}
    else:   {key: {"in_collection": [...], "outside_collection": [...]}}
    """
    if simple:
        result = {}
        for inside, outside in matches.values():
            for recid in inside:
                result.setdefault(recid, set()).update(outside)
        return {recid: sorted(others) for recid, others in result.items()}
    return {
        key: {
            "in_collection": [_describe(r) for r in sorted(inside)],
            "outside_collection": [_describe(r) for r in sorted(outside)],
        }
        for key, (inside, outside) in matches.items()
    }


def _describe(recid):
    """Short info to review a candidate by hand."""
    return {
        "recid": recid,
        "status": _status(recid),
        "collections": sorted(set(get_fieldvalues(recid, "980__a") or [])),
        "title": (get_fieldvalues(recid, "245__a") or [""])[0],
        "year": _year(recid),
    }


def _year(recid):
    for tag in ("269__c", "260__c", "773__y"):
        for val in get_fieldvalues(recid, tag) or []:
            match = re.search(r"(1[89]|20)\d{2}", val)
            if match:
                return match.group(0)
    return None


########################################################
# Report numbers (exact, global)
########################################################


def _report_numbers_of(recids):
    """{report_number: set(recids)} for the given recids, read with SQL."""
    found = {}
    by_table = {}
    for tag, table in RN_TAGS.items():
        by_table.setdefault(table, []).append(tag)
    for table, tags in by_table.items():
        tags_sql = ",".join("'{}'".format(tag) for tag in tags)
        for chunk in _chunks(recids, SQL_CHUNK):
            rows = run_sql(
                "SELECT r.id_bibrec, b.value FROM bib{t} b "
                "JOIN bibrec_bib{t} r ON r.id_bibxxx = b.id "
                "WHERE b.tag IN ({tags}) AND r.id_bibrec IN ({ids})".format(
                    t=table, tags=tags_sql, ids=",".join(str(i) for i in chunk)
                ),
                run_on_slave=True,
            )
            for recid, value in rows:
                value = (value or "").strip()
                if value:
                    found.setdefault(value, set()).add(int(recid))
    return found


def _recids_with_report_numbers(report_numbers):
    """{report_number: set(recids)} for exact matches anywhere in CDS."""
    found = {}
    by_table = {}
    for tag, table in RN_TAGS.items():
        by_table.setdefault(table, []).append(tag)
    for table, tags in by_table.items():
        tags_sql = ",".join("'{}'".format(tag) for tag in tags)
        for chunk in _chunks(report_numbers, SQL_CHUNK):
            rows = run_sql(
                "SELECT b.value, r.id_bibrec FROM bib{t} b "
                "JOIN bibrec_bib{t} r ON r.id_bibxxx = b.id "
                "WHERE b.tag IN ({tags}) AND b.value IN ({vals})".format(
                    t=table, tags=tags_sql, vals=",".join(["%s"] * len(chunk))
                ),
                tuple(chunk),
                run_on_slave=True,
            )
            for value, recid in rows:
                found.setdefault(value.strip(), set()).add(int(recid))
    return found


def search_report_number_cross_dups(collection_recids, ignore_deleted, simple):
    own = _report_numbers_of(collection_recids)
    _print_green(
        "Found {} distinct report numbers in the collection.".format(len(own))
    )
    everywhere = _recids_with_report_numbers(own.keys())

    matches = {}
    for rn, inside in own.items():
        outside = everywhere.get(rn, set()) - collection_recids
        outside = [r for r in outside if _keep(r, ignore_deleted)]
        if outside:
            matches[rn] = (inside, outside)
    return _format(matches, simple)


########################################################
# Soft title match (fuzzy, global)
########################################################


def _title_key(value):
    """Lowercase, no accents, only letters and digits.

    Removing spaces and hyphens entirely makes "Hochenergiebe-schleuniger"
    and "Hochenergiebeschleuniger" (or "Kern-reaktor" / "Kernreaktor") match.
    """
    if isinstance(value, str):
        value = value.decode("utf-8", "ignore")
    value = unicodedata.normalize("NFKD", value)
    value = u"".join(c for c in value if not unicodedata.combining(c))
    return re.sub(r"[^a-z0-9]+", u"", value.lower())


def _title_keys(tags):
    """All keys of one record from {tag: [values]}."""
    keys = set()
    for title in tags.get("245__a", []):
        keys.add(_title_key(title))
        for subtitle in tags.get("245__b", []):
            keys.add(_title_key(title + " " + subtitle))
    for title in tags.get("246__a", []):
        keys.add(_title_key(title))
        for subtitle in tags.get("246__b", []):
            keys.add(_title_key(title + " " + subtitle))
    for title in tags.get("242__a", []):
        keys.add(_title_key(title))
    return set(k for k in keys if len(k) >= MIN_TITLE_KEY_LEN)


def _titles_by_recid(where_sql, params=None):
    tags_sql = ",".join("'{}'".format(tag) for tag in TITLE_TAGS)
    rows = run_sql(
        "SELECT r.id_bibrec, b.tag, b.value FROM bib24x b "
        "JOIN bibrec_bib24x r ON r.id_bibxxx = b.id "
        "WHERE b.tag IN ({}) AND {}".format(tags_sql, where_sql),
        params,
        run_on_slave=True,
    )
    titles = {}
    for recid, tag, value in rows:
        titles.setdefault(int(recid), {}).setdefault(tag, []).append(value or "")
    return titles


def search_soft_title_cross_dups(collection_recids, ignore_deleted, simple):
    # Keys of the collection records.
    key_to_inside = {}
    for chunk in _chunks(collection_recids, SQL_CHUNK):
        titles = _titles_by_recid(
            "r.id_bibrec IN ({})".format(",".join(str(i) for i in chunk))
        )
        for recid, tags in titles.items():
            for key in _title_keys(tags):
                key_to_inside.setdefault(key, set()).add(recid)
    _print_green(
        "Built {} normalised title keys for the collection.".format(
            len(key_to_inside)
        )
    )

    # Scan bib24x by id range, keep only records sharing a key.
    max_id = run_sql("SELECT MAX(id) FROM bib24x", run_on_slave=True)[0][0] or 0
    key_to_outside = {}
    for start in range(0, max_id + 1, TITLE_SCAN_CHUNK):
        titles = _titles_by_recid(
            "b.id >= %s AND b.id < %s", (start, start + TITLE_SCAN_CHUNK)
        )
        # A record's title parts can fall in different id ranges; a title +
        # subtitle key is only built when both parts are in the same chunk.
        # Single-field keys (the common case) are always found.
        for recid, tags in titles.items():
            if recid in collection_recids:
                continue
            for key in _title_keys(tags):
                if key in key_to_inside:
                    key_to_outside.setdefault(key, set()).add(recid)
        _print_green("Scanned bib24x ids up to {}/{}".format(
            min(start + TITLE_SCAN_CHUNK, max_id), max_id))

    matches = {}
    for key, outside in key_to_outside.items():
        outside = [r for r in outside if _keep(r, ignore_deleted)]
        if outside:
            matches[key] = (key_to_inside[key], outside)
    result = _format(matches, simple)
    if not simple:
        for entry in result.values():
            years_inside = set(
                d["year"] for d in entry["in_collection"] if d["year"]
            )
            for desc in entry["outside_collection"]:
                desc["same_year"] = bool(
                    desc["year"] and desc["year"] in years_inside
                )
    return result


########################################################
# Main
########################################################


def parse_args():
    parser = argparse.ArgumentParser(
        description="Find records of a collection duplicated elsewhere in CDS."
    )
    parser.add_argument(
        "--collection-q",
        required=True,
        action="append",
        help="CDS search query for the collection, can be given several times.",
    )
    parser.add_argument(
        "--output-dir",
        required=True,
        help="Collection working directory the results are written to.",
    )
    parser.add_argument(
        "--soft-title-match",
        action="store_true",
        help=(
            "WARNING: fuzzy. Also match normalised titles across all of CDS. "
            "Gives false positives, review the output by hand. Scans the whole "
            "bib24x table."
        ),
    )
    parser.add_argument(
        "--ignore-deleted",
        type=_str_to_bool,
        default=True,
        metavar="True|False",
        help=(
            "Ignore deleted records, in and outside the collection. "
            "Default: True. Use --ignore-deleted False to keep them."
        ),
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help=(
            "Detailed output per report number / title, with status, "
            "collections, title and year of each record. By default the "
            "output is {recid_in_collection: [recids outside], ...}."
        ),
    )
    return parser.parse_args()


def _str_to_bool(value):
    value = value.strip().lower()
    if value in ("true", "1", "yes"):
        return True
    if value in ("false", "0", "no"):
        return False
    raise argparse.ArgumentTypeError("Expected True or False, got {}".format(value))


def _dump(data, path):
    with open(path, "w") as fp:
        json.dump(data, fp, indent=2, sort_keys=True)
    _print_green("Wrote {} entries to {}".format(len(data), path))


def main():
    args = parse_args()
    if not os.path.exists(args.output_dir):
        os.makedirs(args.output_dir)

    for query in args.collection_q:
        _print_green("Collection query: {}".format(query))
    collection_recids = _search_recids(args.collection_q)
    if args.ignore_deleted:
        _print_green("Ignoring deleted records.")
        collection_recids = set(
            r for r in collection_recids if _keep(r, ignore_deleted=True)
        )
    _print_green("Collection has {} records.".format(len(collection_recids)))
    if not collection_recids:
        return

    _print_green("--------------------------------")
    _print_green("Searching report numbers shared outside the collection...")
    _dump(
        search_report_number_cross_dups(
            collection_recids, args.ignore_deleted, not args.verbose
        ),
        os.path.join(args.output_dir, "report_number_cross_dups.json"),
    )

    if args.soft_title_match:
        _print_green("--------------------------------")
        _print_yellow(
            "WARNING: soft title match is fuzzy and can give false positives. "
            "Review soft_title_cross_dups.json by hand before using it."
        )
        _dump(
            search_soft_title_cross_dups(
                collection_recids, args.ignore_deleted, not args.verbose
            ),
            os.path.join(args.output_dir, "soft_title_cross_dups.json"),
        )


if __name__ == "__main__":
    main()
