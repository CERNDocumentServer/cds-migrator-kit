import re
from urllib.parse import urlparse, ParseResult

from dateutil.parser import ParserError, parse
from dojson.errors import IgnoreKey
from dojson.utils import force_list

from cds_migrator_kit.errors import UnexpectedValue
from cds_migrator_kit.transform.xml_processing.quality.decorators import for_each_value
from cds_migrator_kit.transform.xml_processing.quality.parsers import clean_val
from .base import urls
from .publications import related_identifiers, journal

from ...models.courier import courier_issue_model as model
from .base import normalize


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


@model.over("courier_custom_fields", "(^269__)", override=True)
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
        dates = self.get("dates", [])
        try:
            date_obj = parse(publication_date_str)
            date = normalize(publication_date_str)
            dates.append({"date": date, "type": {"id": "issued"}})
            self["dates"] = dates
        except (ParserError, TypeError) as e:
            year_pattern = r"\b(19[0-9]{2}|20[0-9]{2})\b"
            match = re.search(year_pattern, publication_date_str)
            if match:
                year = match.group(0)
                months = (publication_date_str
                          .replace(year, "")
                          .replace("/", "-").strip().lower())
                months_mapping = {
                    "january-february": f"{year}-01/{year}-02",
                    "janvier-février": f"{year}-01/{year}-02",
                    "march-april": f"{year}-03/{year}-04",
                    "mars-avril": f"{year}-03/{year}-04",
                    "april-may": f"{year}-04/{year}-05",
                    "avril-mai": f"{year}-04/{year}-05",
                    "july-august": f"{year}-07/{year}-08",
                    "juillet-août": f"{year}-07/{year}-08",
                    "août-septembre": f"{year}-08/{year}-09",
                    "august-september": f"{year}-08/{year}-09",
                    "september-october": f"{year}-09/{year}-10",
                    "septembre-octobre": f"{year}-09/{year}-10",
                    "november-december": f"{year}-11/{year}-12",
                    "novembre-décembre": f"{year}-11/{year}-12",
                    "janvier": f"{year}-01",
                    "février": f"{year}-02",
                    "mars": f"{year}-03",
                    "avril": f"{year}-04",
                    "mai": f"{year}-05",
                    "juin": f"{year}-06",
                    "juillet": f"{year}-07",
                    "août": f"{year}-08",
                    "septembre": f"{year}-09",
                    "octobre": f"{year}-10",
                    "novembre": f"{year}-11",
                    "décembre": f"{year}-12",
                    "summer": f"{year}-06/{year}-09",
                    "été": f"{year}-06/{year}-09",
                    "winter": f"{year}-12/{int(year)+1}-03",
                    "hiver": f"{year}-12/{int(year)+1}-03",
                }
                try:
                    date = months_mapping[months]
                    dates.append({"date": date, "type": {"id": "issued"}})
                    self["dates"] = dates
                except KeyError:
                    raise UnexpectedValue(
                        field=key,
                        value=value,
                        message=f"Can't parse provided publication date. Value: {publication_date_str}",
                    )
            else:
                raise UnexpectedValue(
                    field=key,
                    value=value,
                    message=f"Can't parse provided publication date. Value: {publication_date_str}",
                )
    raise IgnoreKey("courier_custom_fields")

@model.over("additional_descriptions", "(^500__)")
@for_each_value
def additional_descriptions(self, key, value):
    description = value.get("a", "").strip()
    if len(description) < 3:
        raise IgnoreKey("additional_descriptions")
    if description:
        return {"description": description, "type": {"id": "technical-info"}}
    raise IgnoreKey("additional_descriptions")


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


@model.over("related_identifiers", "^962_")
def courier_related_identifiers(self, key, value):
    """Translates courier related identifiers."""
    # workaround for not importing all the publications rules
    return related_identifiers(self, key, value)


@model.over("courier_custom_fields", "(^773__)")
def courier_journal(self, key, value):
    """Translated periodical field."""
    # workaround for not importing all the publications rules
    result = journal(self, key, value)
    self["custom_fields"] = result
    raise IgnoreKey("journal_custom_fields")


@model.over("collection", "^690C_", override=True)
@for_each_value
def collection(self, key, value):
    """Translates collection field."""
    collection_a = value.get("a", "").strip().lower()

    if collection_a in ["aida-2020", "eucard2", "eucard2pre", "aida-2020pre"]:
        subjects = self.get("subjects", [])
        subjects.append({"subject": f"collection:{collection_a}"})
        self["subjects"] = subjects
        raise IgnoreKey("collection")
    if collection_a not in ["cern", "cern courier", "article", "publats", "fcc acc"]:  # 2265255
        raise UnexpectedValue(subfield="a", key=key, value=value, field="690C_")
    raise IgnoreKey("collection")


@model.over("editor", "^856[0_][0_]", override=True)
def record_submitter(self, key, value):
    """Translate record submitter."""
    submitter = value.get("f")
    if type(submitter) is tuple:
        submitter = submitter[0]
        raise UnexpectedValue(field=key, subfield="f", value=value.get("f"))
        # TODO handle all the other submitters
    if submitter != "cern.courier@cern.ch":
        raise UnexpectedValue(field=key, subfield="f", value=value.get("f"))
    raise IgnoreKey("submitter")


@model.over("url_identifiers", "^8564_", override=True)
@for_each_value
def urls_bulletin(self, key, value):
    content_type = value.get("x", "")

    if content_type == "icon":
        # ignore icons
        url_u = value.get("u", "")
        raise IgnoreKey("url_identifiers")

    url_q = value.get("q", "").strip()
    identifiers = self.get("identifiers", [])

    if "q" not in value:
        _urls = urls(self, key, value)
        identifiers += _urls
    else:
        p = urlparse(url_q, "http")
        netloc = p.netloc or p.path
        path = p.path if p.netloc else ""
        if not netloc.startswith("www."):
            netloc = "www." + netloc

        p = ParseResult("http", netloc, path, *p[3:])
        new_id = {"identifier": p.geturl(), "scheme": "url"}
        if new_id not in identifiers:
            identifiers.append(new_id)

    self["identifiers"] = identifiers
    raise IgnoreKey("url_identifiers")


@model.over('internal_notes', '^903__')
def internal_notes(self, key, value):
    """Translates private notes field."""
    _internal_notes = self.get('internal_notes', [])
    for v in force_list(value):
        note = clean_val('d', v, str, req=True)
        note += clean_val('s', v, str)
        internal_note = {'value': note}
        if internal_note not in _internal_notes:
            _internal_notes.append(internal_note)
    return _internal_notes


@model.over("resource_type", "^980__", override=True)
def resource_type(self, key, value):
    """Translates resource_type."""
    value = value.get("a")
    if value:
        value = value.lower()
    if value in ["article"]:
        raise IgnoreKey("resource_type")
    map = {
        "cern_courier_issue": {"id": "publication-periodicalissue"},
        "cern_courier_article": {"id": "publication-article"},
    }
    try:
        return map[value]
    except KeyError:
        raise UnexpectedValue("Unknown resource type (Courier)", field=key, value=value)
