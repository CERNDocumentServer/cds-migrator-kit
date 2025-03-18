# -*- coding: utf-8 -*-
#
# Copyright (C) 2022 CERN.
#
# CDS-RDM is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""CDS-RDM migration data cleaning module."""
import re
from abc import ABC, abstractmethod

from dojson.utils import force_list

from cds_migrator_kit.errors import MissingRequiredField, UnexpectedValue


class MarcValue(ABC):
    """Abstract class for Marc value."""

    def __init__(
        self,
        raw_value,
        required_type,
        subfield=None,
        required=False,
        default_value=None,
    ):
        """Constructor."""
        if subfield:
            self.raw_value = raw_value.get(subfield)
        else:
            self.raw_value = raw_value
        self.casted_value = None
        self.required_type = required_type
        self.default_value = default_value
        self.parsed_value = None
        self.is_required = required
        self.subfield = subfield

    def type(self):
        """Require type."""
        self.casted_value = self.required_type(self.raw_value)
        return self.casted_value

    def required(self):
        """Check if value present if required."""
        if (
            (not self.raw_value or not self.parsed_value)
            and self.is_required
            and not self.default_value
        ):
            raise MissingRequiredField(subfield=self.subfield, value=self.raw_value)
        return self.is_required

    def default(self):
        """Provide default value."""
        return self.default

    def _clean(self):
        """Clean string."""
        return self.parsed_value

    def parse(self):
        """Parse value."""
        try:
            self.parsed_value = self._clean()
            self.required()
            self.parsed_value = self.type()
            self.parsed_value = self._clean()
            return self.parsed_value
        except Exception as e:
            raise UnexpectedValue(
                value=self.raw_value, message=str(e), stage="transform", exc=e
            )


class StringValue(MarcValue):
    """String value parser class."""

    def __init__(
        self,
        raw_value,
        required_type=str,
        subfield=None,
        required=False,
        default_value=None,
    ):
        """Constructor."""
        super().__init__(raw_value, required_type, subfield, required, default_value)

    def _clean(self):
        """Clean value."""
        if self.raw_value:
            return self.raw_value.strip()
        else:
            return ""

    def parse(self, filter_regex=None):
        """Parse string value."""
        super().parse()
        if filter_regex:
            self.parsed_value = self.filter_regex(filter_regex)
        return self.parsed_value

    def filter_regex(self, regex):
        """Filter value using regex."""
        return re.sub(regex, "", self.parsed_value, flags=re.UNICODE)


class ListValue(MarcValue):
    """List value class."""

    def type(self):
        """Transform to list type."""
        self.casted_value = force_list(self.raw_value)
        return super().type()

    def _clean(self):
        for value in self.casted_value:
            # list of type
            self.required_type(value).parse()


def clean_str(to_clean):
    """Cleans string values."""
    try:
        return to_clean.strip()
    except AttributeError:
        raise UnexpectedValue


def clean_val(
    subfield,
    value,
    var_type,
    req=False,
    regex_format=None,
    default=None,
    manual=False,
    transform=None,
    multiple_values=False,
):
    """
    Tests values using common rules.

    :param subfield: marcxml subfield indicator
    :param value: marcxml value
    :param var_type: expected type for value to be cleaned
    :param req: specifies if the value is required in the end schema
    :param regex_format: specifies if the value should have a pattern
    :param default: if value is missing and required it outputs default
    :param manual: if the value should be cleaned manually during the migration
    :param transform: string transform function (or callable)
    :param multiple_values: allow multiple values in subfield
    :return: cleaned output value
    """

    def _clean(value_to_clean):
        if value_to_clean is not None:
            try:
                if var_type is str:
                    return clean_str(value_to_clean)
                elif var_type is bool:
                    return bool(value_to_clean)
                elif var_type is int:
                    return int(value_to_clean)
                else:
                    raise NotImplementedError
            except ValueError:
                raise UnexpectedValue(subfield=subfield)
            except TypeError:
                raise UnexpectedValue(subfield=subfield)
            except (UnexpectedValue, MissingRequiredField) as e:
                e.subfield = subfield
                e.message += str(force_list(value))
                raise e

    to_clean = value.get(subfield)

    is_tuple = type(to_clean) is tuple
    if is_tuple and not multiple_values:
        raise UnexpectedValue(subfield=subfield)

    if multiple_values:
        if is_tuple:
            cleaned_values = []
            for v in to_clean:
                cleaned_values.append(_clean(v))
            return cleaned_values
        else:
            # always return a list when `multiple_values` is True
            return [_clean(to_clean)]
    else:
        return _clean(to_clean)
