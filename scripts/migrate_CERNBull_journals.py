from copy import deepcopy

import arrow
import csv
from invenio_access.permissions import system_identity
from invenio_communities.proxies import current_communities
from invenio_db import db
from invenio_rdm_records.proxies import current_rdm_records_service
from sqlalchemy.orm.exc import StaleDataError

filepath = "CERNBulletinJournals.csv"
community_slug = "cern-bulletin"
outfilepath = "CERNBulletinJournals_migrated.csv"


def migrate_bull_issues():
    with open(filepath, mode='r', newline='', encoding='utf-8') as infile, open(
        outfilepath, mode='w', newline='', encoding='utf-8') as outfile:
        reader = csv.reader(infile)
        writer = csv.writer(outfile)
        for row in reader:
            id_journal, issue_number, issue_display, date_released, date_announced = row
            data = {"metadata": {"title": f"CERN Bulletin Issue {issue_display}",
                                 "publication_date": arrow.get(
                                     date_released).datetime.replace(
                                     tzinfo=None).date().isoformat(),
                                 "creators": [{"person_or_org": {"name": "CERN",
                                                                 "type": "organizational"}}],
                                 "resource_type": {
                                     "id": "publication-periodicalissue"},
                                 "additional_descriptions": [
                                     {"description": issue_number,
                                      "type": {"id": "technical-info"}}],
                                 },
                    "files": {"enabled": False}
                    }
            if date_announced:
                data["metadata"]["dates"] = [{"date": arrow.get(
                    date_announced).datetime.replace(
                    tzinfo=None).date().isoformat(), "type": {
                    "id": "issued"}}]

            draft = current_rdm_records_service.create(
                system_identity, data=data
            )

            parent = draft._record.parent

            community = current_communities.service.read(system_identity,
                                                         community_slug)
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
                                date_released).datetime.replace(tzinfo=None)
                            record._record.commit()
                    except StaleDataError as e:
                        db.session.rollback()
                        record_obj = db.session.merge(record_obj, load=False)
                        if attempt == 2:
                            raise e

                writer.writerow(
                    [id_journal, issue_number, issue_display, date_released,
                     date_announced,
                     record.id])
            except Exception as e:
                raise e


def link_related_articles():
    with open(outfilepath, mode='r', newline='', encoding='utf-8') as file:
        reader = csv.reader(file)
        for row in reader:
            id_journal, issue_number, issue_display, date_released, date_announced, record_pid = row
            results = current_rdm_records_service.search(system_identity,
                                                         params={
                                                             "q": f'custom_fields.journal\:journal.issue:"{issue_number}"'
                                                         },
                                                         )
            print(
                f"Found {results.total} related articles for issue {issue_number} {record_pid}")
            for hit in results.hits:
                data = {"identifier": record_pid,
                        "relation_type": {
                            "id": "ispublishedin"}}
                record = current_rdm_records_service.read(system_identity,
                                                          hit["id"])
                _draft = current_rdm_records_service.edit(system_identity, record["id"])

                update_data = deepcopy(_draft.data)
                if "related_identifiers" not in update_data["metadata"]:
                    update_data["metadata"]["related_identifiers"] = []
                update_data["metadata"]["related_identifiers"].append(data)
                draft = current_rdm_records_service.update_draft(system_identity,
                                                                 _draft["id"],
                                                                 update_data
                                                                 )
                record = current_rdm_records_service.publish(system_identity,
                                                             draft["id"])
                print(f"Linked {hit['id']} to {record_pid}")
