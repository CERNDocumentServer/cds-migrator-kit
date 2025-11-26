import re

from dateutil.parser import ParserError, parse
from dojson.errors import IgnoreKey

from cds_migrator_kit.errors import UnexpectedValue
from cds_migrator_kit.transform.xml_processing.quality.decorators import (
    for_each_value,
    require,
)
from cds_migrator_kit.transform.xml_processing.quality.parsers import StringValue

from ...models.hr import hr_model as model
from .base import (
    aleph_number,
    corporate_author,
    identifiers,
    report_number,
    subjects,
    urls,
)
from .publications import journal, related_identifiers


@model.over("access_grants", "^506[1_]_")
@for_each_value
def access_grants(self, key, value):
    """Translates access permissions (by user email or group name)."""
    raw_identifier = value.get("d") or value.get("m") or value.get("a")
    subject_identifier = StringValue(raw_identifier).parse()
    if not subject_identifier:
        raise IgnoreKey("access_grants")

    permission_type = "view"
    return {str(subject_identifier): permission_type}


@model.over("additional_descriptions_hr", "^270__")
@for_each_value
def additional_desc(self, key, value):
    """Translates contact e-mail."""
    email = value.get("m", "")
    email = StringValue(email).parse()
    if not email:
        raise IgnoreKey("additional_descriptions_hr")

    additional_descriptions = self.get("additional_descriptions", [])
    additional_descriptions.append(
        {
            "description": f"Contact: {email}",
            "type": {"id": "technical-info"},
        }
    )
    self["additional_descriptions"] = additional_descriptions
    raise IgnoreKey("additional_descriptions_hr")


@model.over("additional_descriptions_hr_smc", "(^594__)")
@for_each_value
def additional_desc(self, key, value):
    """Translates contact e-mail."""
    material = value.get("a", "")
    material = StringValue(material).parse()
    if not material:
        raise IgnoreKey("additional_descriptions_hr_smc")

    additional_descriptions = self.get("additional_descriptions", [])
    additional_descriptions.append(
        {
            "description": material,
            "type": {"id": "technical-info"},
        }
    )
    raise IgnoreKey("additional_descriptions_hr")


@model.over("subjects", "(^6931_)|(^650[12_][7_])|(^653[12_]_)|(^695__)|(^694__)")
@require(["a"])
@for_each_value
def hr_subjects(self, key, value):
    if key == "6531_":
        keyword = value.get("a")
        if "," in keyword:
            keywords = keyword.split(",")
            _subjects = self.get("subjects", [])
            for key in keywords:
                _subjects.append({"subject": key})
            self["subjects"] = _subjects
            raise IgnoreKey("subjects")
        else:
            resource_type_map = {
                "Presentation": {"id": "presentation"},
            }
            resource_type = resource_type_map.get(keyword)
            if resource_type:
                self["resource_type"] = resource_type
            raise IgnoreKey("subjects")

    subjects(self, key, value)


@model.over("collection", "^690C_", override=True)
@for_each_value
def collection(self, key, value):
    """Translates collection field."""
    collection = value.get("a").strip().lower()
    if collection in ["chis bulletin"]:
        subjects = self.get("subjects", [])
        subjects.append({"subject": "collection:{}".format(collection)})
        chis = {"subject": "CHIS"}
        if chis not in subjects:
            subjects.append({"subject": "CHIS"})
        self["subjects"] = subjects
        raise IgnoreKey("collection")
    if collection not in [
        "cern admin e-guide",
        "staff rules and regulations",
        "cern",
        "annual personnel statistics",
        "administrative circular",
        "cern annual personnel statistics",
        "intnote",
        "operational circular",
        "publhr",
    ]:
        raise UnexpectedValue(subfield="a", value=value, field=key)
    raise IgnoreKey("collection")


@model.over("creators", "(^110__)")
@for_each_value
def corpo_author(self, key, value):
    author = value.get("a", "").strip()
    if not author:
        raise UnexpectedValue(subfield="a", value=value, field=key)
    author = {"person_or_org": {"type": "organizational", "name": author}}
    if author not in self.get("creators", []):
        return author
    raise IgnoreKey("creators")


@model.over("resource_type", "^980__", override=True)
def resource_type(self, key, value):
    """Translates resource_type."""
    value = value.get("a")
    if value:
        value = value.strip().lower()
    if value in ["article"]:
        raise IgnoreKey("resource_type")
    if value in ["hr-smc", "ccp"]:
        subjects = self.get("subjects", [])
        subjects.append({"subject": f"collection:{value}"})
        self["subjects"] = subjects
    if value == "administrativenote":
        raise IgnoreKey("resource_type")
    map = {
        "annualstats": {"id": "publication-report"},
        "cern-admin-e-guide": {"id": "publication-other"},
        "intnotehrpubl": {"id": "publication-technicalnote"},
        "chisbulletin": {"id": "publication-periodicalissue"},
        "bulletin": {"id": "publication-periodicalissue"},
        "admincircular": {"id": "administrative-circular"},
        "opercircular": {"id": "administrative-operationalcircular"},
        "staffrules": {"id": "administrative-regulation"},
        "staffrulesvd": {"id": "administrative-regulation"},
        "hr-smc": {"id": "administrative-regulation"},
        "ccp": {"id": "other"},
        "conferencepaper": {"id": "publication-conferencepaper"},
    }
    try:

        return map[value]
    except KeyError:
        raise UnexpectedValue("Unknown resource type (HR)", field=key, value=value)


@model.over("internal_notes", "^562__")
@for_each_value
def note(self, key, value):
    """Translates notes."""
    return {"note": StringValue(value.get("c")).parse()}


@model.over("record_restriction", "^591__")
@for_each_value
def record_restriction(self, key, value):
    """Translates notes."""
    access = value.get("a")
    if access and access.lower() in ("cern internal", "restricted"):
        return "restricted"
    elif access and access.lower() != "public":
        raise UnexpectedValue("Access field other than public", field=key, value=value)
    raise IgnoreKey("access")


@model.over("additional_descriptions", "(^500__)")
@for_each_value
@require(["a"])
def additional_descriptions(self, key, value):
    """Translates additional description."""
    description_text = value.get("a")
    if description_text:
        _additional_description = {
            "description": description_text,
            "type": {
                "id": "other",  # what's with the lang
            },
        }
        return _additional_description
    raise IgnoreKey("additional_descriptions")


@model.over("dates", "^925__")
@for_each_value
def date(self, key, value):
    """Translates dates."""
    dates = self.get("dates", [])
    valid = value.get("a")
    date = {
        "date": valid,
        "type": {"id": "valid"},
    }
    dates.append(date)
    withdrawn = value.get("b", "")
    if withdrawn and "9999" not in withdrawn:
        date = {
            "date": withdrawn,
            "type": {"id": "withdrawn"},
        }
        dates.append(date)
    self["dates"] = dates
    raise IgnoreKey("dates")


@model.over("administrative_unit", "^710__", override=True)
@for_each_value
def custom_fields(self, key, value):
    """Translates administrative_unit."""
    unit = value.get("b")
    if unit:
        _custom_fields = self.get("custom_fields", {})
        _custom_fields["cern:administrative_unit"] = unit
        self["custom_fields"] = _custom_fields
    else:
        contributors = self.get("contributors", [])
        try:
            author = corporate_author(self, key, value)
        except IgnoreKey:
            author = None
        if author:
            contributors.append(author[0])
            self["contributors"] = contributors

    raise IgnoreKey("administrative_unit")


@model.over("description", "^520__", override=True)
def description(self, key, value):
    """Translates description."""
    description_text = StringValue(value.get("a")).parse()
    if len(description_text) >= 3:
        return description_text
    raise IgnoreKey("description")


@model.over("additional_descriptions", "(^590__)")
@for_each_value
def translated_description(self, key, value):
    description_text = value.get("a", "")
    if description_text:
        _additional_description = {
            "description": description_text,
            "type": {
                "id": "other",
            },
            "lang": {"id": "fra"},
        }
        return _additional_description
    raise IgnoreKey("additional_descriptions")


@model.over(
    "identifiers", "(^035__)|(^037__)|(^088__)|(^8564_)|(^970__)", override=True
)
@for_each_value
def title(self, key, value):
    """Translates title and identifiers."""
    # ----Title-----#
    title = StringValue(value.get("a")).parse()
    if title.startswith("CERN-STAFF-RULES-"):
        match = re.match(r"^CERN-STAFF-RULES-([A-Z0-9]+)(?:-.+)?$", title)
        if match:
            suffix = match.group(1)
            self["title"] = f"Staff Rules and Regulations No.{suffix}"

    # ------Identifiers-----#
    new_id = None
    if key == "037__":
        circulars = {
            "ADMIN": "Administrative Circular",
            "OPER": "Operational Circular",
            "STAFF": "Staff Rules and Regulations",
        }
        title = ""
        rep_num = value.get("a").split("-")
        revision = rep_num[-1]
        circ_type = rep_num[1]
        number = rep_num[3]
        title += circulars[circ_type]
        number_stripped = number.replace("(", "").replace(")", "")

        if number.isalnum() or number_stripped.isalnum():
            title += " No." + number

        if revision != "REV0" and circ_type != "STAFF":
            title += " (Rev %s)" % revision[3:]
        _add_titles = self.get("additional_titles", [])
        _add_titles.append({"title": title, "type": {"id": "alternative-title"}})
        self["additional_titles"] = _add_titles

    if key in ("037__", "088__"):
        new_id = report_number(self, key, value)

    elif key == "8564_":
        new_id = urls(self, key, value)
        rel_ids = self.get("related_identifiers", [])
        rel_ids.append(new_id)
        self["related_identifiers"] = rel_ids
        raise IgnoreKey("identifiers")
    elif key == "970__":
        new_id = aleph_number(self, key, value)
    elif key == "035__":
        new_id = identifiers(self, key, value)
    if new_id:
        return new_id[0]
    raise IgnoreKey("identifiers")


@model.over("meeting_cf", "^773__")
@for_each_value
def meeting(self, key, value):
    """Translates meeting fields."""
    author = value.get("t", "").strip()
    if author:
        author = {"person_or_org": {"type": "organizational", "name": author}}
        creators = self.get("creators", [])
        if author not in creators:
            creators.append(author)
        self["creators"] = creators
    self["custom_fields"].update(journal(self, key, value))
    raise IgnoreKey("meeting_cf")


@model.over("related_identifiers", "^962__")
def related_identifiers_hr(self, key, value):
    """Translates HR related identifiers."""
    return related_identifiers(self, key, value)
