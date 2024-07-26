# -*- coding: utf-8 -*-
#
# Copyright (C) 2022 CERN.
#
# CDS-RDM is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""CDS-RDM migration data cleaning module."""
from dojson.utils import force_list

from cds_migrator_kit.rdm.migration.transform.xml_processing.errors import (
    MissingRequiredField,
    UnexpectedValue,
)


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
                    return clean_str(value_to_clean, regex_format, req, transform)
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
