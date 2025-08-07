from dateutil.parser import ParserError, parse
from dojson.errors import IgnoreKey

from cds_migrator_kit.errors import UnexpectedValue
from cds_migrator_kit.transform.xml_processing.quality.decorators import for_each_value
from cds_migrator_kit.transform.xml_processing.quality.parsers import StringValue
from .base import created

from ...models.bulletin_issue import bull_issue_model as model


@model.over("collection", "^690C_")
@for_each_value
def collection(self, key, value):
    """Translates collection field."""
    collection = value.get("a").strip().lower()
    if collection not in ["cern", "cern bulletin printable version"]:
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
            date_obj = parse(publication_date_str)
            return date_obj.strftime("%Y-%m-%d")
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


@model.over("custom_fields_journal", "(^916__)", override=True)
def issue_number(self, key, value):
    _custom_fields = self.get("custom_fields", {})

    issue = value.get("z")

    journal_fields = _custom_fields.get("journal:journal", {})
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
