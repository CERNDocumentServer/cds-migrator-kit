import re
from urllib.parse import ParseResult, urlparse

from cds_rdm.schemes import is_cds
from dateutil.parser import ParserError, parse
from dojson.errors import IgnoreKey

from cds_migrator_kit.errors import UnexpectedValue
from cds_migrator_kit.rdm.records.transform.xml_processing.rules.base import (
    related_identifiers_787 as base_related_identifiers,
)
from cds_migrator_kit.rdm.records.transform.xml_processing.rules.base import (
    report_number,
)
from cds_migrator_kit.transform.xml_processing.quality.decorators import (
    for_each_value,
    require,
)
from cds_migrator_kit.transform.xml_processing.quality.parsers import StringValue
from cds_migrator_kit.transform.xml_processing.rules.base import process_contributors

from ...models.bulletin_issue import bull_issue_model as model
from .base import additional_titles, created, normalize, subjects, urls


@model.over("creators", "^100__", override=True)
@for_each_value
@require(["a"])
def creators(self, key, value):
    """Translates the creators field."""
    if not value.get("a"):
        ## many empty values inside the bulletin
        raise IgnoreKey("creators")
    return process_contributors(key, value)


@model.over("additional_titles", "(^246_[1_])", override=True)
@for_each_value
@require(["a"])
def additional_titles_bulletin(self, key, value):
    """Translate additional titles."""

    # many records are missing main title, reuse the 246 field if missing
    title = value.get("a")
    if title and "title" not in self:
        self["title"] = value.get("a")

    # run the original rule
    additional_titles(self, key, value)
    raise IgnoreKey("additional_titles")


@model.over("description", "^520__", override=True)
def description(self, key, value):
    """Translates description."""

    description_text = value.get("a", "")
    description_text_b = value.get("b", "")
    description_text = description_text.replace("<!--HTML-->", "").strip()
    description_text_b = description_text_b.replace("<!--HTML-->", "").strip()

    if len(description_text) > 3 or len(description_text_b) > 3:
        description_text = f"<h2>{description_text}</h2><p>{description_text_b}</p>"
        return description_text
    else:
        raise IgnoreKey("description")


@model.over("collection", "^690C_", override=True)
@for_each_value
def collection(self, key, value):
    """Translates collection field."""
    collection_a = value.get("a", "").strip().lower()
    collection_b = value.get("b", "").strip().lower()
    if collection_b and collection_b in [
        "eucard2",
        "aida-2020",
        "eucard2pre",
        "aida-2020pre",
    ]:
        subjects = self.get("subjects", [])
        subjects.append({"subject": "collection:EuCARD2"})
        self["subjects"] = subjects
    elif collection_b and collection_b not in ["cern", "reviewed"]:
        raise UnexpectedValue(subfield="b", key=key, value=value, field="690C_")

    if collection_a in ["aida-2020", "eucard2", "eucard2pre", "aida-2020pre"]:
        subjects = self.get("subjects", [])
        subjects.append({"subject": f"collection:{collection_a}"})
        self["subjects"] = subjects
        raise IgnoreKey("collection")
    if collection_a not in ["cern", "cern bulletin printable version", "reviewed"]:
        raise UnexpectedValue(subfield="a", key=key, value=value, field="690C_")
    raise IgnoreKey("collection")


@model.over("publication_date", "(^260__)", override=True)
def imprint_info(self, key, value):
    """Translates imprint - WARNING - also publisher and publication_date.

    In case of summer student notes this field contains only date
    but it needs to be reimplemented for the base set of rules -
    it will contain also imprint place
    """
    _custom_fields = self.get("custom_fields", {})
    imprint = _custom_fields.get("imprint:imprint", {})

    publication_date_str = value.get("c")
    _publisher = value.get("b")
    place = value.get("a")
    if _publisher and not self.get("publisher"):
        self["publisher"] = _publisher
    if place:
        imprint["place"] = place
    self["custom_fields"]["imprint:imprint"] = imprint
    if publication_date_str:
        try:
            publication_date = normalize(publication_date_str)
            return publication_date
        except (ParserError, TypeError) as e:
            raise UnexpectedValue(
                field=key,
                value=value,
                message=f"Can't parse provided publication date. Value: {publication_date_str}",
            )
    raise IgnoreKey("publication_date")


@model.over("custom_fields", "(^773__)")
def journal(self, key, value):
    _custom_fields = self.get("custom_fields", {})
    journal_fields = _custom_fields.get("journal:journal", {})

    issue = value.get("n", "")
    pages = value.get("c", "")
    if issue:
        issue = StringValue(issue).parse()
        if "issue" in journal_fields and issue not in journal_fields["issue"]:
            journal_fields["issue"] += f"-{issue}"
        else:
            journal_fields["issue"] = issue
    if pages:
        journal_fields["pages"] = StringValue(pages).parse()

    _custom_fields["journal:journal"] = journal_fields
    return _custom_fields


@model.over("additional_descriptions", "(^500__)")
@for_each_value
def additional_descriptions(self, key, value):
    description = value.get("a", "").strip()
    curated_dm = value.get("9", "").strip()
    if "curated" in curated_dm:
        raise IgnoreKey("additional_descriptions")
    if len(description) < 3:
        raise IgnoreKey("additional_descriptions")
    if description:
        return {"description": description, "type": {"id": "technical-info"}}
    raise IgnoreKey("additional_descriptions")


@model.over("additional_descriptions", "(^590__)")
@for_each_value
def translated_description(self, key, value):
    description_text = value.get("a", "")
    description_text_b = value.get("b", "")
    description_text = description_text.replace("<!--HTML-->", "").strip()
    description_text_b = description_text_b.replace("<!--HTML-->", "").strip()

    if len(description_text) > 3 or len(description_text_b) > 3:
        description_text = f"<h2>{description_text}</h2><p>{description_text_b}</p>"
    if description_text:
        _additional_description = {
            "description": description_text,
            "type": {
                "id": "other",  # what's with the lang
            },
            "lang": {"id": "fra"},
        }
        return _additional_description
    raise IgnoreKey("additional_descriptions")


@model.over("subjects", "(^650[12_][7_])|(^6531_)", override=True)
@for_each_value
def subjects_bulletin(self, key, value):
    subject = value.get("a", "").strip()
    scheme = value.get("2", "").strip()
    if scheme in ["EuCARD2", "AIDA-2020"]:
        subjects(self, key, value)
    else:
        return {"subject": subject}


@model.over("url_identifiers", "^8564_", override=True)
@for_each_value
def urls_bulletin(self, key, value):
    content_type = value.get("x", "")
    if content_type == "icon":
        # ignore icon urls (conditionally ignoring by accessing the value
        url_q = value.get("q", "")
        url_u = value.get("u", "")
        raise IgnoreKey("url_identifiers")

    identifiers = self.get("related_identifiers", [])

    if (
        "q" in value
        and (parsed := urlparse(value.get("q", ""))).scheme
        and parsed.netloc
    ):
        _urls = urls(self, key, value, subfield="q")
    else:
        _urls = urls(self, key, value)

    identifiers += _urls

    self["related_identifiers"] = identifiers
    raise IgnoreKey("url_identifiers")


@model.over("urls_bulletin", "^856__")
def urls_bulletin_bis(self, key, value):
    """Translates 865 tags."""
    # If not implemented this way, the override in the urls_bulletin does not work
    urls_bulletin(self, key, value)
    raise IgnoreKey("urls_bulletin")


@model.over("custom_fields_journal", "(^916__)", override=True)
def issue_number(self, key, value):
    _custom_fields = self.get("custom_fields", {})

    issue = value.get("z")

    journal_fields = _custom_fields.get("journal:journal", {})
    if issue is not None:
        journal_fields["issue"] = issue

    _custom_fields["journal:journal"] = journal_fields
    # because we override 916
    self["status_week_date"] = created(self, key, value)

    self["custom_fields"] = _custom_fields
    raise IgnoreKey("custom_fields_journal")


@model.over("bulletin_report_number", "(^037__)|(^088__)", override=True)
@for_each_value
def bulletin_report_number(self, key, value):
    """Translates report number."""
    identifier = value.get("a", "")
    pattern = r"\b\d+/(19|20)\d{2}\b"
    matches = re.findall(pattern, identifier)

    if identifier and matches:
        _custom_fields = self.get("custom_fields", {})
        journal_fields = _custom_fields.get("journal:journal", {})
        if journal_fields.get("issue"):
            journal_fields["issue"] += f"-{identifier}"
        else:
            journal_fields["issue"] = identifier
        _custom_fields["journal:journal"] = journal_fields
        self["custom_fields"] = _custom_fields
        raise IgnoreKey("bulletin_report_number")
    else:
        _identifier = report_number(self, key, value)
        identifiers = self.get("identifiers", [])

        if _identifier and _identifier not in identifiers:
            identifiers += _identifier
        self["identifiers"] = identifiers
        raise IgnoreKey("bulletin_report_number")


@model.over("custom_fields", "(^925__)")
def issue_number(self, key, value):
    _custom_fields = self.get("custom_fields", {})

    issue_start = value.get("a")
    issue_end = value.get("b")

    issue = f"{issue_start}-{issue_end}"
    journal_fields = _custom_fields.get("journal:journal", {})
    if journal_fields.get("issue") is None:
        journal_fields["issue"] = issue

    _custom_fields["journal:journal"] = journal_fields
    return _custom_fields


@model.over("bull_related_identifiers_1", "(^941__)")
@for_each_value
def bull_related_identifiers(self, key, value):
    id = value.get("a")
    resource_type = value.get("t", "other")
    scheme = "other"

    res_type_map = {"photo": {"id": "image-photo"}, "other": {"id": "other"}}

    is_cds_recid = is_cds(id)
    if is_cds_recid:
        scheme = "cds"

    new_id = {
        "identifier": id,
        "scheme": scheme,
        "resource_type": res_type_map[resource_type.lower()],
        "relation_type": {"id": "references"},
    }

    rel_ids = self.get("related_identifiers", [])
    if new_id not in rel_ids:
        rel_ids.append(new_id)
    self["related_identifiers"] = rel_ids
    raise IgnoreKey("bull_related_identifiers")


@model.over("bull_related_identifiers_2", "(^962__)", override=True)
@for_each_value
def rel_identifiers(self, key, value):
    """Old aleph identifier pointing to MMD repository."""
    identifier = value.get("b", "")
    scheme = value.get("l", "")
    report_number = value.get("t", "").lower()

    if not identifier:
        raise IgnoreKey("bull_related_identifiers_2")

    if "pho" in scheme.lower():
        res_type = "photo"
    else:
        res_type = "other"
    if report_number in ["cern-videoclip-yyyy-xx"]:
        raise IgnoreKey("related_identifiers")
    res_type_map = {"photo": {"id": "image-photo"}, "other": {"id": "other"}}

    photo_regex = re.compile(r"^CERN-[A-Z]+-\d+$", flags=re.I)
    is_cern_report_number = photo_regex.match(report_number)

    identifier = identifier.replace("Photo", "").strip()
    if is_cern_report_number:
        scheme = "cdsrn"
    else:
        scheme = "other"

    new_id = {
        "identifier": f"{identifier}",
        "scheme": scheme,
        "resource_type": res_type_map[res_type.lower()],
        "relation_type": {"id": "references"},
    }

    identifiers = self.get("related_identifiers", [])
    if new_id not in identifiers:
        identifiers.append(new_id)
    self["related_identifiers"] = identifiers
    raise IgnoreKey("bull_related_identifiers_2")


@model.over("resource_type", "^980__", override=True)
def resource_type(self, key, value):
    """Translates resource_type."""
    value = value.get("a") if "a" in value else value.get("b")
    if value:
        value = value.lower()
    if value in ["aida-2020", "eucard2"]:
        subjects = self.get("subjects", [])
        subjects.append({"subject": f"collection:{value}"})
        self["subjects"] = subjects
        raise IgnoreKey("resource_type")
    if value in ["bulletin", "bulletinstaff"]:
        raise IgnoreKey("resource_type")
    map = {
        "cern_bulletin_issue": {"id": "publication-periodicalissue"},
        "cern_bulletin_article": {"id": "publication-periodicalarticle"},
        "bulletingeneral": {"id": "publication-periodicalarticle"},
        "bulletinevents": {"id": "publication-periodicalarticle"},
        "bulletinannounce": {"id": "publication-periodicalarticle"},
        "bulletinbreaking": {"id": "publication-periodicalarticle"},
        "bulletinnews": {"id": "publication-periodicalarticle"},
        "bulletinofficial": {"id": "publication-periodicalarticle"},
        "bulletinpension": {"id": "publication-periodicalarticle"},
        "bulletintraining": {"id": "publication-periodicalarticle"},
        "bulletinsocial": {"id": "publication-periodicalarticle"},
        # todo newsletter
    }
    try:
        return map[value]
    except KeyError:
        raise UnexpectedValue(
            "Unknown resource type (BULLETIN)", field=key, value=value
        )


