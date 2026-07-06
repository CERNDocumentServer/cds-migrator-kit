from dojson.errors import IgnoreKey

from cds_migrator_kit.errors import MissingRequiredField, UnexpectedValue
from cds_migrator_kit.transform.xml_processing.quality.decorators import (
    for_each_value,
    require,
)
from cds_migrator_kit.transform.xml_processing.quality.parsers import StringValue

from ...models.research import research_model as model


@model.over("resource_type", "^980_", override=True)
def resource_type(self, key, value):
    """Translates resource_type."""
    value_a = value.get("a")

    if value_a:
        if type(value_a) is tuple:
            resource_type = value_a[1].lower()
        else:
            resource_type = value_a.lower()
        if resource_type in ["re29_papers"]:
            raise IgnoreKey("resource_type")

    map = {
        "lcd-notes": {"id": "publication-technicalnote"},
        "alephdraft": {"id": "publication-other"},
        "preprint": {"id": "publication-preprint"},
        "conferencepaper": {"id": "publication-conferencepaper"},
    }
    try:
        return map[resource_type]
    except KeyError:
        raise UnexpectedValue("Unknown resource type", field=key, value=value)


@model.over("_approval", "(^591__)", override=True)
def status(self, key, value):

    val_a = value.get("a", "").lower().strip()
    val_b = value.get("b", "").lower().strip()

    self.setdefault("request_data", {})

    values = {val_a, val_b}
    known = {"draft", "approved", "not approved", ""}

    if values & known:
        self["request_data"]["status"] = (
            "rejected" if "not approved" in values else "accepted"
        )
    else:
        raise UnexpectedValue("Unexpected status value", field=key, value=value)

    raise IgnoreKey("_approval")


@model.over("request_reviewers", "^906__", override=True)
@for_each_value
def request_reviewers(self, key, value):
    name = StringValue(value.get("p", "")).parse().strip()

    if "," in name:
        last, first = (part.strip() for part in name.split(",", 1))
    else:
        last, first = name, ""

    reviewer = " ".join(part for part in (first, last) if part)

    if reviewer:
        request_data = self.setdefault("request_data", {})
        reviewers = request_data.setdefault("reviewers", [])

        if reviewer not in reviewers:
            reviewers.append(reviewer)

    raise IgnoreKey("request_reviewers")
