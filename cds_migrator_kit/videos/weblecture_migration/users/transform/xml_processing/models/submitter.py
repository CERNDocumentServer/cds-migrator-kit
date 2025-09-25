# -*- coding: utf-8 -*-
#
# Copyright (C) 2025 CERN.
#
# cds-migrator-kit is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""cds-migrator-kit videos submitter model."""

from cds_migrator_kit.transform.overdo import CdsOverdo
from cds_migrator_kit.videos.weblecture_migration.transform.models.base import (
    model as base_model,
)


class VideosSubmitterModel(CdsOverdo):
    """Translation Model for submitters."""

    __query__ = (
        # meant to match any record
        ""
    )

    __ignore_keys__ = base_model.__ignore_keys__ | {
        # 340: Drop streaming video, anything else we copy over to curation field.
        "340__a",  # Physical medium -> curation field
        "340__d",  # Physical medium/recording technique -> curation field
        "340__9",  # Physical medium/CD-ROM -> curation field
        "340__k",  # Physical medium/ -> curation field
        "340__j",  # Physical medium/ -> curation field
        "340__8",  # Physical medium/id? -> curation field https://cds.cern.ch/record/2234827
        "964__a",  # Item owner
        "901__u",  # Affiliation at Conversion?
        "583__a",  # Action note / curation
        "583__c",  # Action note / curation
        "583__z",  # Action note / curation
        "916__n",  # Status week
        "916__s",  # Status week
        "916__w",  # Status week
        "306__a",  # ?
        "336__a",  # ?
        # Category, Collection, Series, Keywords
        "980__a",  # collection tag
        "980__b",  # Secondary collection indicator
        "65027a",  # TODO Subject category = Event?
        "490__a",  # TODO Series
        "490__v",  # Series: volume
        "650172",  # subject provenance
        "65017a",  # subject value
        "6531_9",  # keyword provenance
        "6531_a",  # keyword
        "690C_a",  # collection name
        # Conference Information/Indico
        "111__a",  # Title (indico)
        "111__9",  # Start date (indico)
        "111__g",  # Event id (indico)
        "111__c",  # Video location (indico location)
        "518__r",  # Video/meeting location
        "518__g",  # Lectures: conference identification
        "970__a",  # alternative identifier, indico id?
        # Copyright/License
        "542__d",  # Copyright holder
        "542__g",  # Copyright date
        "542__f",  # Copyright statement
        "542__3",  # Copyright materials
        "540__a",  # License
        "540__b",  # License person/organization
        "540__u",  # License URL
        "540__3",  # License material
        # Alternative identifiers
        "962__n",  # `Presented at` note (conference/linked document)
        "962__b",  # `Presented at` record (conference/linked document)
        "088__9",  # Report number (make it alternative identifier with cds reference?)
        "088__z",  # Report number (make it alternative identifier with cds reference?)
        "035__9",  # Inspire schema (Indico/AgendaMaker)
        "035__a",  # Inspire id value
        "088__a",  # Report Number --> alternative identifier with ds reference
        # Additional Title, Volume, Note
        "246__a",  # Additional title
        "246__i",  # Additional title/display text
        "246__b",  # Additional title remaining
        "246__n",  # Volume
        "246__p",  # Volume
        "500__a",  # Note (-> internal note)
        "500__b",  # Note (-> internal note)
        "500__9",  # Note/type (-> internal note) https://cds.cern.ch/record/1561636
        # Restricted
        "5061_f",
        "5061_d",
        "5061_5",
        "5061_a",
        "5061_2",
        # Location (Shelving/Library)
        "852__c",  # Location (Shelving/Library)
        "852__b",  # Location (Shelving/?)
        "852__8",  # Location (Shelving/id?) https://cds.cern.ch/record/2234827
        "852__h",  # Location (Shelving) example: https://cds.cern.ch/record/254588/
        "852__a",  # Location (Shelving) example: https://cds.cern.ch/record/558348
        "852__x",  # Location (Shelving/ type? DVD) example: https://cds.cern.ch/record/690000/
        "852__9",  # Location (Shelving/ note?) example: https://cds.cern.ch/record/2233722
        # Date/Extra Reduntant
        "260__c",  # Redundant (more detailed value is in 269__c imprint.pub_date)
        "260__a",
        "260__b",
        # Contributor?
        "110__a",  # corporate author
        "700__m",  # author's email
        "270__p",  # document contact --> add as a contributor with a correct role
        # Internal Note
        "595__a",  # Internal Note --> curation field
        "595__z",  # SOME RECORD HAVE UNCL as value, do we keep it? what does UNCL mean
        # Collaboration --> add new role to contributors
        "710__5",  # department / organisation author
        "710__a",  # organisation author
        "710__g",  # organisation author
        # Accelerator/Facility, Experiment, Project, Study
        "693__a",  # accelerator, create a custom field?
        "693__e",  # experiments
        "693__p",  # project
        "693__s",  # study
        "520__a",  # Note (-> description.type = abstract
        "001",
        "041__a",  # languages
        "906__p",  # event speakers
        "100__9",  # #BEARD# tag
        "100__a",
        "100__u",  # Author affiliation
        "700__e",  # Contributor/Speaker role
        "700__9",  # #BEARD# tag
        "700__a",  # Contributors (full name)
        "700__u",  # Contributors (affiliation)
        "518__d",  # Full date/time
        "269__c",  # Date (full date/year)
        "269__b",  # CERN (checked for other values)
        "269__a",  # Geneva (checked for other values)
        "518__a",  # Date
        "906__u",  # Contributor Affiliation
        "511__u",  # Contributor Affiliation
        "511__a",  # Contributor
        "511__e",  # Contributor role
        "8564_q",  # File type (digitized)
        "8564_u",  # Files
        "8564_x",  # Files system field
        "8564_y",  # Files
        "8564_w",
        "8564_2",
        "8564_z",
        "961__x",  # Creation Date
        "961__c",  # modification Date
        "595__s",
        "0247_a",
        "0247_2",
        "7870_i",
        "7870_r",
        "7870_w",
        "590__a",
        "773__u",
        "773__p",
        "773__r",
        "773__a",
        "583__8",
        "650272",
        # Submitter
        # "859__f",  # email
        # "8560_f",  # email
        # "8567_u",  # File url
        # "8567_y",  # File description
        # "8567_2",  # File system? 'MediaArchive'
        # "8567_x",
        # "8567_d",
    }

    _default_fields = {
        "contributors": [],
        "additional_languages": [],
        "keywords": [],
    }


videos_submitter_model = VideosSubmitterModel(
    # use base rules - we don't need any other rule than 859 and 8560
    bases=(base_model,),
    entry_point_group="cds_migrator_kit.videos.rules.base",
)
