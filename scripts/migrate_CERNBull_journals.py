"""
Migrates CERN Bulletin journals form legacy (stored as DB rows) to RDM.
Then links the related periodical articles to the newly created periodical issue.

To generate the output CSV file, run the following:
migrate_bull_issues()

The output CSV (migrated issues) file will be created in the current directory.

Then, run the following to link the related articles:
link_related_articles()
"""

import csv
import os

import arrow
from invenio_access.permissions import system_identity
from invenio_communities.proxies import current_communities
from invenio_db import db
from invenio_rdm_records.proxies import current_rdm_records_service
from invenio_search.engine import dsl
from sqlalchemy.orm.exc import StaleDataError

filepath = "CERNBulletinJournals.csv"
community_slug = "cern-bulletin"
outfilepath = "CERNBulletinJournals_migrated.csv"


def migrate_bull_issues():
    migrated = load_migrated_issues()
    with open(filepath, mode="r", newline="", encoding="utf-8") as infile, open(
        outfilepath, mode="a", newline="", encoding="utf-8"
    ) as outfile:
        reader = csv.reader(infile)
        writer = csv.writer(outfile)
        for row in reader:
            id_journal, issue_number, issue_display, date_released, date_announced = row
            if not date_released:
                print(f"Skipping {issue_number} because date released is missing")
                continue

            if issue_number in migrated:
                print(
                    f"Skipping {issue_number}, already migrated as {migrated[issue_number]}"
                )
                continue

            existing_pid = find_existing_issue_record(issue_number)
            if existing_pid:
                print(f"Skipping {issue_number}, found existing record {existing_pid}")
                writer.writerow(
                    [
                        id_journal,
                        issue_number,
                        issue_display,
                        date_released,
                        date_announced,
                        existing_pid,
                    ]
                )
                migrated[issue_number] = existing_pid
                continue

            print(f"Processing {issue_number}...")
            data = {
                "metadata": {
                    "title": f"CERN Bulletin Issue {issue_display}",
                    "publication_date": arrow.get(date_released)
                    .datetime.replace(tzinfo=None)
                    .date()
                    .isoformat(),
                    "creators": [
                        {"person_or_org": {"name": "CERN", "type": "organizational"}}
                    ],
                    "resource_type": {"id": "publication-periodicalissue"},
                    "additional_descriptions": [
                        {"description": issue_number, "type": {"id": "technical-info"}}
                    ],
                },
                "files": {"enabled": False},
            }
            if date_announced:
                data["metadata"]["dates"] = [
                    {
                        "date": arrow.get(date_announced)
                        .datetime.replace(tzinfo=None)
                        .date()
                        .isoformat(),
                        "type": {"id": "issued"},
                    }
                ]

            draft = current_rdm_records_service.create(system_identity, data=data)

            parent = draft._record.parent

            community = current_communities.service.read(
                system_identity, community_slug
            )
            communities = [community.id]
            for community in communities:
                parent.communities.add(community)
                parent.communities.default = community
            parent.commit()

            try:
                record = current_rdm_records_service.publish(
                    system_identity, draft["id"]
                )
                record_obj = record._record.model
                for attempt in range(3):
                    try:
                        with db.session.begin_nested():
                            record_obj.created = arrow.get(
                                date_released
                            ).datetime.replace(tzinfo=None)
                            record._record.commit()
                    except StaleDataError as e:
                        db.session.rollback()
                        record_obj = db.session.merge(record_obj, load=False)
                        if attempt >= 2:
                            raise e

                writer.writerow(
                    [
                        id_journal,
                        issue_number,
                        issue_display,
                        date_released,
                        date_announced,
                        record.id,
                    ]
                )
                migrated[issue_number] = record.id
            except Exception as e:
                raise e


def load_migrated_issues():
    """Load already migrated issues from the output CSV."""
    migrated = {}
    if not os.path.exists(outfilepath):
        return migrated
    with open(outfilepath, mode="r", newline="", encoding="utf-8") as file:
        reader = csv.reader(file)
        for row in reader:
            if len(row) < 6:
                continue
            issue_number, record_pid = row[1], row[5]
            migrated[issue_number] = record_pid
    return migrated


def find_existing_issue_record(issue_number):
    """Find an already created bulletin issue record by issue number."""
    issue_number_query = issue_number.replace("/", "\\/")
    results = current_rdm_records_service.scan(
        system_identity,
        q=(
            f'metadata.additional_descriptions.description:"{issue_number_query}" '
            "AND metadata.resource_type.id:publication-periodicalissue"
        ),
    )
    hits = list(results)
    if not hits:
        return None
    return hits[0]["id"]


def link_related_articles():
    with open(outfilepath, mode="r", newline="", encoding="utf-8") as file:
        reader = csv.reader(file)
        for row in reader:
            (
                id_journal,
                issue_number,
                issue_display,
                date_released,
                date_announced,
                record_pid,
            ) = row
            if not record_pid:
                continue

            # The extra filter is used to find the related articles for the given issue number.
            # For example, it matches the March 2026 issue with the following issue numbers in the custom field "journal:journal.issue":
            # 2/2026-3/2026-4/2026-5/2026
            # 02/2026-03/2026-04/2026-05/2026
            # 2/2026-3/2026
            # 03/2026-04/2026
            # 03/2026
            # 3/2026
            extra_filter = dsl.Q(
                "regexp",
                **{
                    "custom_fields.journal:journal.issue.keyword": {
                        "value": f"(.*-)?0?{issue_number}(-.*)?",
                        "flags": "NONE",
                    }
                },
            )
            results = current_rdm_records_service.scan(
                system_identity,
                q="metadata.resource_type.id:publication-periodicalarticle",
                extra_filter=extra_filter,
            )
            list_res = list(results)
            print(
                f"Found {len(list_res)} related articles for issue {issue_number} {record_pid}"
            )
            for hit in list_res:
                link_article_to_issue(hit["id"], record_pid)


def link_article_to_issue(hit_id, record_pid):
    """Append an ispublishedin link to a bulletin issue if not already present."""
    record = current_rdm_records_service.read(system_identity, hit_id)
    related_identifiers = record.data["metadata"].get("related_identifiers", [])

    existing_rel_ids = {
        rel_id["identifier"]
        for rel_id in related_identifiers
        if (
            rel_id.get("relation_type", {}).get("id") == "ispublishedin"
            and rel_id.get("scheme") == "cds"
            and rel_id.get("resource_type", {}).get("id") == "publication-periodicalissue"
        )
    }
    if record_pid in existing_rel_ids:
        print(f"Skipped {hit_id}, already linked to {record_pid}")
        return

    draft = current_rdm_records_service.edit(system_identity, record["id"])
    draft.data["metadata"].setdefault("related_identifiers", []).append(
        {
            "identifier": record_pid,
            "relation_type": {"id": "ispublishedin"},
            "scheme": "cds",
            "resource_type": {"id": "publication-periodicalissue"},
        }
    )
    draft = current_rdm_records_service.update_draft(
        system_identity, draft.id, draft.data
    )
    current_rdm_records_service.publish(system_identity, draft.id)
    print(f"Linked {hit_id} to {record_pid}")
