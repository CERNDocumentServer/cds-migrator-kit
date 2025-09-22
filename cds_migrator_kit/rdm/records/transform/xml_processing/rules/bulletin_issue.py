import re
from urllib.parse import ParseResult, urlparse

from cds_rdm.schemes import is_legacy_cds
from dateutil.parser import ParserError, parse
from dojson.errors import IgnoreKey

from cds_migrator_kit.errors import UnexpectedValue
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

    journal_fields["issue"] = StringValue(value.get("n", "")).parse()
    journal_fields["pages"] = StringValue(value.get("c", "")).parse()

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


@model.over("url_identifiers", "^856[4_]_", override=True)
@for_each_value
def urls_bulletin(self, key, value):
    content_type = value.get("x", "")

    if content_type == "icon":
        # ignore icon urls (conditionally ignoring by accessing the value
        url_q = value.get("q", "")
        url_u = value.get("u", "")
        raise IgnoreKey("url_identifiers")

    identifiers = self.get("identifiers", [])

    if "q" not in value:
        _urls = urls(self, key, value)
        identifiers += _urls
    else:
        _urls = urls(self, key, value, subfield="q")
        identifiers += _urls

    self["identifiers"] = identifiers
    raise IgnoreKey("url_identifiers")


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


@model.over("custom_fields", "(^925__)")
def issue_number(self, key, value):
    _custom_fields = self.get("custom_fields", {})

    issue_start = value.get("a")
    issue_end = value.get("b")

    issue = f"{issue_start}-{issue_end}"
    journal_fields = _custom_fields.get("journal:journal", {})
    journal_fields["issue"] = issue

    _custom_fields["journal:journal"] = journal_fields
    return _custom_fields


@model.over("related_identifiers", "^941__")
@for_each_value
def related_identifiers(self, key, value):
    id = value.get("a")
    resource_type = value.get("t", "other")
    scheme = "other"

    res_type_map = {"photo": {"id": "image-photo"}, "other": {"id": "other"}}

    is_cds_recid = is_legacy_cds(id)
    if is_cds_recid:
        scheme = "lcds"

    new_id = {
        "identifier": id,
        "scheme": scheme,
        "resource_type": res_type_map[resource_type.lower()],
        "relation_type": {"id": "references"},
    }

    rel_ids = self.get("related_identifiers", [])
    if new_id not in rel_ids:
        return new_id
    raise IgnoreKey("related_identifiers")


@model.over("related_identifiers", "(^962__)")
@for_each_value
def rel_identifiers(self, key, value):
    """Old aleph identifier pointing to MMD repository."""
    identifier = value.get("b", "")
    scheme = value.get("l", "")
    report_number = value.get("t", "").lower()

    if not identifier:
        raise IgnoreKey("related_identifiers")

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
        scheme = "cds_ref"
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
        return new_id
    raise IgnoreKey("related_identifiers")
