# -*- coding: utf-8 -*-
#
# Copyright (C) 2023 CERN.
#
# CDS-RDM is free software; you can redistribute it and/or modify it under
# the terms of the MIT License; see LICENSE file for more details.

"""Pytest fixtures."""
import os
from collections import namedtuple
from os.path import dirname, join

import pytest
from cds_rdm.custom_fields import CUSTOM_FIELDS
from cds_rdm.permissions import (
    CDSCommunitiesPermissionPolicy,
    CDSRDMRecordPermissionPolicy,
)
from cds_rdm.schemes import is_aleph, is_inspire, is_inspire_author, is_legacy_cds
from flask_security.utils import hash_password
from flask_webpackext.manifest import (
    JinjaManifest,
    JinjaManifestEntry,
    JinjaManifestLoader,
)
from invenio_access.models import ActionRoles
from invenio_access.permissions import superuser_access, system_identity
from invenio_accounts import testutils
from invenio_accounts.models import Role, UserIdentity
from invenio_administration.permissions import administration_access_action
from invenio_app import factory as app_factory
from invenio_cern_sync.sso import cern_remote_app_name
from invenio_cern_sync.users.profile import CERNUserProfileSchema
from invenio_communities.communities.records.api import Community
from invenio_i18n import lazy_gettext as _
from invenio_pidstore.models import PersistentIdentifier, PIDStatus
from invenio_rdm_records.cli import create_records_custom_field
from invenio_rdm_records.config import (
    RDM_RECORDS_IDENTIFIERS_SCHEMES,
    RDM_RECORDS_PERSONORG_SCHEMES,
    always_valid,
)
from invenio_records_resources.proxies import current_service_registry
from invenio_users_resources.proxies import current_groups_service
from invenio_users_resources.records.api import UserAggregate
from invenio_vocabularies.config import (
    VOCABULARIES_NAMES_SCHEMES as DEFAULT_VOCABULARIES_NAMES_SCHEMES,
)
from invenio_vocabularies.contrib.awards.api import Award
from invenio_vocabularies.contrib.funders.api import Funder
from invenio_vocabularies.proxies import current_service as vocabulary_service
from invenio_vocabularies.records.api import Vocabulary


class MockJinjaManifest(JinjaManifest):
    """Mock manifest."""

    def __getitem__(self, key):
        """Get a manifest entry."""
        return JinjaManifestEntry(key, [key])

    def __getattr__(self, name):
        """Get a manifest entry."""
        return JinjaManifestEntry(name, [name])


class MockManifestLoader(JinjaManifestLoader):
    """Manifest loader creating a mocked manifest."""

    def load(self, filepath):
        """Load the manifest."""
        return MockJinjaManifest()


@pytest.fixture()
def datadir():
    """Get data directory."""
    return join(dirname(__file__), "data")


@pytest.fixture(scope="module")
def app_config(app_config):
    """Mimic an instance's configuration."""
    app_config["REST_CSRF_ENABLED"] = True
    app_config["DATACITE_ENABLED"] = True
    app_config["DATACITE_PREFIX"] = "10.17181"
    app_config["OAUTH_REMOTE_APP_NAME"] = "cern"
    app_config["CERN_APP_CREDENTIALS"] = {
        "consumer_key": "CHANGE ME",
        "consumer_secret": "CHANGE ME",
    }
    app_config["CERN_LDAP_URL"] = ""  # mock
    app_config["ACCOUNTS_USER_PROFILE_SCHEMA"] = CERNUserProfileSchema()
    app_config["COMMUNITIES_PERMISSION_POLICY"] = CDSCommunitiesPermissionPolicy
    app_config["RDM_PERMISSION_POLICY"] = CDSRDMRecordPermissionPolicy
    app_config["COMMUNITIES_ALLOW_RESTRICTED"] = True
    app_config["CDS_GROUPS_ALLOW_CREATE_COMMUNITIES"] = [
        "group-allowed-create-communities"
    ]
    app_config["WEBPACKEXT_MANIFEST_LOADER"] = MockManifestLoader

    app_config["JSONSCHEMAS_HOST"] = "localhost"
    app_config["BABEL_DEFAULT_LOCALE"] = "en"
    app_config["I18N_LANGUAGES"] = [("da", "Danish")]
    app_config["RECORDS_REFRESOLVER_CLS"] = (
        "invenio_records.resolver.InvenioRefResolver"
    )
    app_config["RECORDS_REFRESOLVER_STORE"] = (
        "invenio_jsonschemas.proxies.current_refresolver_store"
    )
    app_config["VOCABULARIES_NAMES_SCHEMES"] = {
        **DEFAULT_VOCABULARIES_NAMES_SCHEMES,
        "inspire_author": {
            "label": _("Inspire"),
            "validator": is_inspire_author,
            "datacite": "INSPIRE",
        },
        "lcds": {"label": _("CDS"), "validator": is_legacy_cds, "datacite": "CDS"},
    }
    app_config["RDM_RECORDS_PERSONORG_SCHEMES"] = {
        **RDM_RECORDS_PERSONORG_SCHEMES,
        **{
            "inspire_author": {
                "label": _("Inspire"),
                "validator": is_inspire_author,
                "datacite": "INSPIRE",
            },
            "lcds": {
                "label": _("CDS"),
                "validator": is_legacy_cds,
                "datacite": "CDS",
            },
        },
    }
    app_config["RDM_RECORDS_IDENTIFIERS_SCHEMES"] = {
        **RDM_RECORDS_IDENTIFIERS_SCHEMES,
        **{
            "cds_ref": {
                "label": _("CDS Reference"),
                "validator": always_valid,
                "datacite": "CDS",
            },
            "aleph": {
                "label": _("Aleph number"),
                "validator": is_aleph,
                "datacite": "ALEPH",
            },
            "inspire": {
                "label": _("Inspire"),
                "validator": is_inspire,
                "datacite": "INSPIRE",
            },
            "inis": {"label": _("INIS"), "validator": is_inspire, "datacite": "INIS"},
            "lcds": {"label": _("CDS"), "validator": is_legacy_cds, "datacite": "CDS"},
        },
    }

    app_config["SEARCH_INDEX_PREFIX"] = ""
    app_config["CDS_MIGRATOR_KIT_STREAM_CONFIG"] = (
        "tests/cds-rdm/data/sspn/streams.yaml"
    )
    base_path = os.path.dirname(os.path.realpath(__file__))
    logs_dir = os.path.join(base_path, "tmp/logs/")
    app_config["CDS_MIGRATOR_KIT_LOGS_PATH"] = logs_dir
    app_config["RDM_CUSTOM_FIELDS"] = CUSTOM_FIELDS

    return app_config


@pytest.fixture(scope="function")
def db_session_options():
    """Database session options."""
    # TODO: Look into having this be the default in ``pytest-invenio``
    # This helps with ``sqlalchemy.orm.exc.DetachedInstanceError`` when models are not
    # bound to the session between transactions/requests/service-calls.
    return {"expire_on_commit": False}


# @pytest.fixture(scope="module")
# def create_app(instance_path):
#     """Application factory fixture."""
#     return app_factory.create_app


@pytest.fixture(scope="module")
def create_app(instance_path):
    """Application factory fixture."""
    return app_factory.create_api


RunningApp = namedtuple(
    "RunningApp",
    [
        "app",
        "superuser_identity",
        "location",
        "cache",
        "resource_type_v",
        "languages_v",
        "subjects_v",
        "experiments_v",
        "departments_v",
        "accelerators_v",
        "programmes_v",
        "funders_v",
        "awards_v",
        "licenses_v",
        "contributors_role_v",
        "description_type_v",
        "relation_type_v",
        "initialise_custom_fields",
    ],
)


@pytest.fixture
def running_app(
    app,
    superuser_identity,
    location,
    cache,
    resource_type_v,
    languages_v,
    subjects_v,
    experiments_v,
    departments_v,
    accelerators_v,
    programmes_v,
    funders_v,
    awards_v,
    licenses_v,
    contributors_role_v,
    description_type_v,
    relation_type_v,
    initialise_custom_fields,
):
    """This fixture provides an app with the typically needed db data loaded.

    All of these fixtures are often needed together, so collecting them
    under a semantic umbrella makes sense.
    """
    return RunningApp(
        app,
        superuser_identity,
        location,
        cache,
        resource_type_v,
        languages_v,
        subjects_v,
        experiments_v,
        departments_v,
        accelerators_v,
        programmes_v,
        funders_v,
        awards_v,
        licenses_v,
        contributors_role_v,
        description_type_v,
        relation_type_v,
        initialise_custom_fields,
    )


@pytest.fixture
def test_app(running_app):
    """Get current app."""
    running_app.app.config["RDM_PERSISTENT_IDENTIFIERS"]["doi"]["required"] = False
    running_app.app.config["RDM_PARENT_PERSISTENT_IDENTIFIERS"]["doi"][
        "required"
    ] = False
    return running_app.app


@pytest.fixture(scope="session")
def headers():
    """Default headers for making requests."""
    return {
        "content-type": "application/json",
        "accept": "application/json",
    }


@pytest.fixture()
def superuser_role_need(db):
    """Store 1 role with 'superuser-access' ActionNeed.

    WHY: This is needed because expansion of ActionNeed is
         done on the basis of a User/Role being associated with that Need.
         If no User/Role is associated with that Need (in the DB), the
         permission is expanded to an empty list.
    """
    role = Role(name="superuser-access")
    db.session.add(role)

    action_role = ActionRoles.create(action=superuser_access, role=role)
    db.session.add(action_role)
    db.session.commit()

    return action_role.need


@pytest.fixture()
def superuser(UserFixture, app, db, superuser_role_need):
    """Superuser."""
    u = UserFixture(
        email="superuser@inveniosoftware.org",
        password="superuser",
    )
    u.create(app, db)

    datastore = app.extensions["security"].datastore
    _, role = datastore._prepare_role_modify_args(u.user, "superuser-access")

    datastore.add_role_to_user(u.user, role)
    db.session.commit()
    return u


@pytest.fixture()
def admin_role_need(db):
    """Store 1 role with 'superuser-access' ActionNeed.

    WHY: This is needed because expansion of ActionNeed is
         done on the basis of a User/Role being associated with that Need.
         If no User/Role is associated with that Need (in the DB), the
         permission is expanded to an empty list.
    """
    role = Role(name="administration-access")
    db.session.add(role)

    action_role = ActionRoles.create(action=administration_access_action, role=role)
    db.session.add(action_role)
    db.session.commit()

    return action_role.need


@pytest.fixture()
def admin(UserFixture, app, db, admin_role_need):
    """Admin user for requests."""
    u = UserFixture(
        email="admin@inveniosoftware.org",
        password="admin",
    )
    u.create(app, db)

    datastore = app.extensions["security"].datastore
    _, role = datastore._prepare_role_modify_args(u.user, "administration-access")

    datastore.add_role_to_user(u.user, role)
    db.session.commit()
    return u


@pytest.fixture()
def superuser_identity(admin, superuser_role_need):
    """Superuser identity fixture."""
    identity = admin.identity
    identity.provides.add(superuser_role_need)
    return identity


@pytest.fixture()
def uploader(UserFixture, app, db, test_app):
    """Uploader."""
    u = UserFixture(
        email="uploader@inveniosoftware.org",
        password="uploader",
        preferences={
            "visibility": "public",
            "email_visibility": "restricted",
            "notifications": {
                "enabled": True,
            },
        },
        active=True,
        confirmed=True,
    )
    u.create(app, db)
    UserAggregate.index.refresh()

    return u


@pytest.fixture()
def archiver(UserFixture, app, db):
    """Uploader."""
    ds = app.extensions["invenio-accounts"].datastore
    user = UserFixture(
        email="archiver@inveniosoftware.org",
        password="archiver",
        preferences={
            "visibility": "public",
            "email_visibility": "restricted",
            "notifications": {
                "enabled": True,
            },
        },
        active=True,
        confirmed=True,
    )
    user_obj = user.create(app, db)
    r = ds.create_role(name="oais-archiver", description="1234")
    ds.add_role_to_user(user.user, r)

    return user


@pytest.fixture(scope="module")
def resource_type_type(app):
    """Resource type vocabulary type."""
    return vocabulary_service.create_type(system_identity, "resourcetypes", "rsrct")


@pytest.fixture(scope="module")
def resource_type_v(app, resource_type_type):
    """Resource type vocabulary record."""
    vocabulary_service.create(
        system_identity,
        {
            "id": "publication-technicalnote",
            "icon": "table",
            "props": {
                "csl": "publication-technicalnote",
                "datacite_general": "publication-technicalnote",
                "datacite_type": "",
                "openaire_resourceType": "21",
                "openaire_type": "dataset",
                "eurepo": "info:eu-repo/semantics/other",
                "schema.org": "https://schema.org/Dataset",
                "subtype": "",
                "type": "dataset",
            },
            "title": {"en": "Publication Technicalnote"},
            "tags": ["depositable", "linkable"],
            "type": "resourcetypes",
        },
    )

    vocabulary_service.create(
        system_identity,
        {  # create base resource type
            "id": "image",
            "props": {
                "csl": "figure",
                "datacite_general": "Image",
                "datacite_type": "",
                "openaire_resourceType": "25",
                "openaire_type": "dataset",
                "eurepo": "info:eu-repo/semantic/other",
                "schema.org": "https://schema.org/ImageObject",
                "subtype": "",
                "type": "image",
            },
            "icon": "chart bar outline",
            "title": {"en": "Image"},
            "tags": ["depositable", "linkable"],
            "type": "resourcetypes",
        },
    )

    vocabulary_service.create(
        system_identity,
        {
            "id": "publication-book",
            "icon": "file alternate",
            "props": {
                "csl": "book",
                "datacite_general": "Text",
                "datacite_type": "Book",
                "openaire_resourceType": "2",
                "openaire_type": "publication",
                "eurepo": "info:eu-repo/semantics/book",
                "schema.org": "https://schema.org/Book",
                "subtype": "publication-book",
                "type": "publication",
            },
            "title": {"en": "Book", "de": "Buch"},
            "tags": ["depositable", "linkable"],
            "type": "resourcetypes",
        },
    )

    vocabulary_service.create(
        system_identity,
        {
            "id": "presentation",
            "icon": "group",
            "props": {
                "csl": "speech",
                "datacite_general": "Text",
                "datacite_type": "Presentation",
                "openaire_resourceType": "0004",
                "openaire_type": "publication",
                "eurepo": "info:eu-repo/semantics/lecture",
                "schema.org": "https://schema.org/PresentationDigitalDocument",
                "subtype": "",
                "type": "presentation",
            },
            "title": {"en": "Presentation", "de": "Präsentation"},
            "tags": ["depositable", "linkable"],
            "type": "resourcetypes",
        },
    )

    vocabulary_service.create(
        system_identity,
        {
            "id": "publication",
            "icon": "file alternate",
            "props": {
                "csl": "report",
                "datacite_general": "Text",
                "datacite_type": "",
                "openaire_resourceType": "0017",
                "openaire_type": "publication",
                "eurepo": "info:eu-repo/semantics/other",
                "schema.org": "https://schema.org/CreativeWork",
                "subtype": "",
                "type": "publication",
            },
            "title": {"en": "Publication", "de": "Publikation"},
            "tags": ["depositable", "linkable"],
            "type": "resourcetypes",
        },
    )

    vocabulary_service.create(
        system_identity,
        {
            "id": "image-photo",
            "tags": ["depositable", "linkable"],
            "type": "resourcetypes",
            "title": {"en": "Image: Photo"},
            "props": {
                "csl": "graphic",
                "datacite_general": "Image",
                "datacite_type": "Photo",
                "openaire_resourceType": "0025",
                "openaire_type": "dataset",
                "eurepo": "info:eu-repo/semantics/other",
                "schema.org": "https://schema.org/Photograph",
                "subtype": "image-photo",
                "type": "image",
            },
        },
    )

    vocabulary_service.create(
        system_identity,
        {
            "id": "software",
            "icon": "code",
            "type": "resourcetypes",
            "props": {
                "csl": "software",
                "datacite_general": "Software",
                "datacite_type": "",
                "openaire_resourceType": "0029",
                "openaire_type": "software",
                "eurepo": "info:eu-repo/semantics/other",
                "schema.org": "https://schema.org/SoftwareSourceCode",
                "subtype": "",
                "type": "software",
            },
            "title": {"en": "Software", "de": "Software"},
            "tags": ["depositable", "linkable"],
        },
    )

    vocab = vocabulary_service.create(
        system_identity,
        {
            "id": "other",
            "icon": "asterisk",
            "type": "resourcetypes",
            "props": {
                "csl": "article",
                "datacite_general": "Other",
                "datacite_type": "",
                "openaire_resourceType": "0020",
                "openaire_type": "other",
                "eurepo": "info:eu-repo/semantics/other",
                "schema.org": "https://schema.org/CreativeWork",
                "subtype": "",
                "type": "other",
            },
            "title": {
                "en": "Other",
                "de": "Sonstige",
            },
            "tags": ["depositable", "linkable"],
        },
    )

    vocabulary_service.create(
        system_identity,
        {
            "id": "publication-thesis",
            "icon": "table",
            "props": {
                "csl": "publication-thesis",
                "datacite_general": "publication-thesis",
                "datacite_type": "",
                "openaire_resourceType": "21",
                "openaire_type": "dataset",
                "eurepo": "info:eu-repo/semantics/other",
                "schema.org": "https://schema.org/Dataset",
                "subtype": "",
                "type": "dataset",
            },
            "title": {"en": "Publication Thesis"},
            "tags": ["depositable", "linkable"],
            "type": "resourcetypes",
        },
    )

    vocabulary_service.create(
        system_identity,
        {
            "id": "dataset",
            "icon": "table",
            "props": {
                "csl": "dataset",
                "datacite_general": "dataset",
                "datacite_type": "",
                "openaire_resourceType": "21",
                "openaire_type": "dataset",
                "eurepo": "info:eu-repo/semantics/other",
                "schema.org": "https://schema.org/Dataset",
                "subtype": "",
                "type": "dataset",
            },
            "title": {"en": "Dataset"},
            "tags": ["depositable", "linkable"],
            "type": "resourcetypes",
        },
    )

    Vocabulary.index.refresh()

    return vocab


@pytest.fixture(scope="module")
def languages_type(app):
    """Language vocabulary type."""
    return vocabulary_service.create_type(system_identity, "languages", "lng")


@pytest.fixture(scope="module")
def languages_v(app, languages_type):
    """Language vocabulary record."""
    vocabulary_service.create(
        system_identity,
        {
            "id": "dan",
            "title": {
                "en": "Danish",
                "da": "Dansk",
            },
            "props": {"alpha_2": "da"},
            "tags": ["individual", "living"],
            "type": "languages",
        },
    )

    vocab = vocabulary_service.create(
        system_identity,
        {
            "id": "eng",
            "title": {
                "en": "English",
                "da": "Engelsk",
            },
            "tags": ["individual", "living"],
            "type": "languages",
        },
    )

    Vocabulary.index.refresh()

    return vocab


@pytest.fixture(scope="module")
def exp_type(app):
    """Experiment vocabulary type."""
    return vocabulary_service.create_type(system_identity, "experiments", "exp")


@pytest.fixture(scope="module")
def experiments_v(app, exp_type):
    """Experiment vocabulary record."""
    vocab = vocabulary_service.create(
        system_identity,
        {
            "id": "LHCB",
            "title": {
                "en": "LHCB",
            },
            "props": {"link": "http://lhcb.web.cern.ch/lhcb/"},
            "type": "experiments",
        },
    )
    vocab = vocabulary_service.create(
        system_identity,
        {
            "id": "CMS",
            "title": {
                "en": "CMS",
            },
            "props": {"link": "http://lhcb.web.cern.ch/lhcb/"},
            "type": "experiments",
        },
    )
    return vocab


# @pytest.fixture(scope="module")
# def sub_type(app):
#     """Department vocabulary type."""
#     current_service_registry.get("subjects")
#     return vocabulary_service.create_type(system_identity, "subjects", "sub")


@pytest.fixture(scope="module")
def subjects_v(app):
    """Language vocabulary record."""
    vocabulary_service = current_service_registry.get("subjects")
    vocab = vocabulary_service.create(
        system_identity,
        {
            "id": "Particle Physics - Phenomenology",
            "scheme": "CERN",
            "subject": "Particle Physics - Phenomenology",
        },
    )
    vocab = vocabulary_service.create(
        system_identity,
        {
            "id": "Particle Physics - Experiment",
            "scheme": "CERN",
            "subject": "Particle Physics - Experiment",
        },
    )
    vocab = vocabulary_service.create(
        system_identity,
        {
            "id": "Nuclear Physics - Experiment",
            "scheme": "CERN",
            "subject": "Nuclear Physics - Experiment",
        },
    )
    vocab = vocabulary_service.create(
        system_identity,
        {
            "id": "Accelerators and Storage Rings",
            "scheme": "CERN",
            "subject": "Accelerators and Storage Rings",
        },
    )
    vocab = vocabulary_service.create(
        system_identity,
        {
            "id": "Computing and Computers",
            "scheme": "CERN",
            "subject": "Computing and Computers",
        },
    )
    vocab = vocabulary_service.create(
        system_identity,
        {
            "id": "Mathematical Physics and Mathematics",
            "scheme": "CERN",
            "subject": "Mathematical Physics and Mathematics",
        },
    )
    vocab = vocabulary_service.create(
        system_identity,
        {
            "id": "Detectors and Experimental Techniques",
            "scheme": "CERN",
            "subject": "Detectors and Experimental Techniques",
        },
    )
    return vocab


@pytest.fixture(scope="module")
def dep_type(app):
    """Department vocabulary type."""
    return vocabulary_service.create_type(system_identity, "departments", "dep")


@pytest.fixture(scope="module")
def departments_v(app, dep_type):
    """Language vocabulary record."""
    vocab = vocabulary_service.create(
        system_identity,
        {
            "id": "EP",
            "title": {
                "en": "EP",
            },
            "type": "departments",
        },
    )
    vocab = vocabulary_service.create(
        system_identity,
        {
            "id": "HSE",
            "title": {
                "en": "HSE",
            },
            "type": "departments",
        },
    )
    vocab = vocabulary_service.create(
        system_identity,
        {
            "id": "PH",
            "title": {
                "en": "PH",
            },
            "type": "departments",
        },
    )
    return vocab


@pytest.fixture(scope="module")
def acc_type(app):
    """Department vocabulary type."""
    return vocabulary_service.create_type(system_identity, "accelerators", "acc")


@pytest.fixture(scope="module")
def accelerators_v(app, acc_type):
    """Language vocabulary record."""
    vocab = vocabulary_service.create(
        system_identity,
        {
            "id": "CERN AD",
            "title": {
                "en": "CERN AD",
            },
            "type": "accelerators",
        },
    )

    return vocab


@pytest.fixture(scope="module")
def prog_type(app):
    """Programmes vocabulary type."""
    return vocabulary_service.create_type(system_identity, "programmes", "pro")


@pytest.fixture(scope="module")
def programmes_v(app, prog_type):
    """Language vocabulary record."""
    vocab = vocabulary_service.create(
        system_identity,
        {
            "id": "CERN Administrative Student Program",
            "title": {
                "en": "CERN Administrative Student Program",
            },
            "type": "programmes",
        },
    )

    vocab = vocabulary_service.create(
        system_identity,
        {
            "id": "CERN Doctoral Student Program",
            "title": {
                "en": "CERN Doctoral Student Program",
            },
            "type": "programmes",
        },
    )

    vocab = vocabulary_service.create(
        system_identity,
        {
            "id": "CERN Short Term Internship Program",
            "title": {
                "en": "CERN Short Term Internship Program",
            },
            "type": "programmes",
        },
    )

    vocab = vocabulary_service.create(
        system_identity,
        {
            "id": "None",
            "title": {
                "en": "No programme participation",
            },
            "type": "programmes",
        },
    )
    Vocabulary.index.refresh()

    return vocab


@pytest.fixture(scope="module")
def funders_v(app, funder_data):
    """Funder vocabulary record."""
    funders_service = current_service_registry.get("funders")
    for funder in funder_data:
        new_funder = funders_service.create(
            system_identity,
            funder,
        )

    Funder.index.refresh()

    return new_funder


@pytest.fixture(scope="module")
def awards_v(app, funders_v):
    """Funder vocabulary record."""
    awards_service = current_service_registry.get("awards")
    award = awards_service.create(
        system_identity,
        {
            "id": "00rbzpz17::755021",
            "identifiers": [
                {
                    "identifier": "https://cordis.europa.eu/project/id/755021",
                    "scheme": "url",
                }
            ],
            "number": "755021",
            "title": {
                "en": (
                    "Personalised Treatment For Cystic Fibrosis Patients With "
                    "Ultra-rare CFTR Mutations (and beyond)"
                ),
            },
            "funder": {"id": "00rbzpz17"},
            "acronym": "HIT-CF",
            "program": "H2020",
        },
    )
    award = awards_service.create(
        system_identity,
        {
            "id": "00k4n6c32::654168",
            "identifiers": [
                {
                    "identifier": "https://cordis.europa.eu/project/id/755021",
                    "scheme": "url",
                }
            ],
            "number": "654168",
            "title": {
                "en": (
                    "Advanced European Infrastructures for Detectors at Accelerators (2020)"
                ),
            },
            "funder": {"id": "00k4n6c32"},
            "acronym": "AIDA-2020",
            "program": "H2020-EU.1.4.",
        },
    )

    Award.index.refresh()

    return award


@pytest.fixture(scope="module")
def licenses(app):
    """Licenses vocabulary type."""
    return vocabulary_service.create_type(system_identity, "licenses", "lic")


@pytest.fixture(scope="module")
def licenses_v(app, licenses):
    """Licenses vocabulary record."""
    cc_zero = vocabulary_service.create(
        system_identity,
        {
            "id": "cc0-1.0",
            "title": {
                "en": "Creative Commons Zero v1.0 Universal",
            },
            "description": {
                "en": (
                    "CC0 waives copyright interest in a work you've created and "
                    "dedicates it to the world-wide public domain. Use CC0 to opt out "
                    "of copyright entirely and ensure your work has the widest reach."
                ),
            },
            "icon": "cc-cc0-icon",
            "tags": ["recommended", "all", "data", "software"],
            "props": {
                "url": "https://creativecommons.org/publicdomain/zero/1.0/legalcode",
                "scheme": "spdx",
                "osi_approved": "",
            },
            "type": "licenses",
        },
    )
    cc_by = vocabulary_service.create(
        system_identity,
        {
            "id": "cc-by-4.0",
            "title": {
                "en": "Creative Commons Attribution 4.0 International",
            },
            "description": {
                "en": (
                    "The Creative Commons Attribution license allows re-distribution "
                    "and re-use of a licensed work on the condition that the creator "
                    "is appropriately credited."
                ),
            },
            "icon": "cc-by-icon",
            "tags": ["recommended", "all", "data"],
            "props": {
                "url": "https://creativecommons.org/licenses/by/4.0/legalcode",
                "scheme": "spdx",
                "osi_approved": "",
            },
            "type": "licenses",
        },
    )

    Vocabulary.index.refresh()

    return [cc_zero, cc_by]


@pytest.fixture(scope="module")
def contributors_role_type(app):
    """Contributor role vocabulary type."""
    return vocabulary_service.create_type(system_identity, "contributorsroles", "cor")


@pytest.fixture(scope="module")
def contributors_role_v(app, contributors_role_type):
    """Contributor role vocabulary record."""
    vocabulary_service.create(
        system_identity,
        {
            "id": "other",
            "props": {"datacite": "Other"},
            "title": {"en": "Other"},
            "type": "contributorsroles",
        },
    )

    vocab = vocabulary_service.create(
        system_identity,
        {
            "id": "datacurator",
            "props": {"datacite": "DataCurator"},
            "title": {"en": "Data curator", "de": "DatenkuratorIn"},
            "type": "contributorsroles",
        },
    )

    vocabulary_service.create(
        system_identity,
        {
            "id": "supervisor",
            "props": {"datacite": "Supervisor"},
            "title": {"en": "Supervisor"},
            "type": "contributorsroles",
        },
    )
    vocabulary_service.create(
        system_identity,
        {
            "id": "hostinginstitution",
            "props": {"datacite": "Hosting institution"},
            "title": {"en": "Hosting institution"},
            "type": "contributorsroles",
        },
    )

    Vocabulary.index.refresh()

    return vocab


@pytest.fixture(scope="module")
def description_type(app):
    """Title vocabulary type."""
    return vocabulary_service.create_type(system_identity, "descriptiontypes", "dty")


@pytest.fixture(scope="module")
def description_type_v(app, description_type):
    """Title Type vocabulary record."""
    vocabulary_service.create(
        system_identity,
        {
            "id": "methods",
            "title": {"en": "Methods"},
            "props": {"datacite": "Methods"},
            "type": "descriptiontypes",
        },
    )

    vocab = vocabulary_service.create(
        system_identity,
        {
            "id": "notes",
            "title": {"en": "Notes"},
            "props": {"datacite": "Notes"},
            "type": "descriptiontypes",
        },
    )

    vocab = vocabulary_service.create(
        system_identity,
        {
            "id": "other",
            "title": {"en": "Other"},
            "props": {"datacite": "Other"},
            "type": "descriptiontypes",
        },
    )

    vocab = vocabulary_service.create(
        system_identity,
        {
            "id": "technical-info",
            "title": {"en": "Technical info"},
            "props": {"datacite": "Technical info"},
            "type": "descriptiontypes",
        },
    )

    Vocabulary.index.refresh()

    return vocab


@pytest.fixture(scope="module")
def relation_type(app):
    """Relation type vocabulary type."""
    return vocabulary_service.create_type(system_identity, "relationtypes", "rlt")


@pytest.fixture(scope="module")
def relation_type_v(app, relation_type):
    """Relation type vocabulary record."""
    vocabulary_service.create(
        system_identity,
        {
            "id": "iscitedby",
            "props": {"datacite": "IsCitedBy"},
            "title": {"en": "Is cited by"},
            "type": "relationtypes",
        },
    )

    vocab = vocabulary_service.create(
        system_identity,
        {
            "id": "cites",
            "props": {"datacite": "Cites"},
            "title": {"en": "Cites", "de": "Zitiert"},
            "type": "relationtypes",
        },
    )

    vocab = vocabulary_service.create(
        system_identity,
        {
            "id": "isversionof",
            "props": {"datacite": "Is version of"},
            "title": {"en": "Is version of"},
            "type": "relationtypes",
        },
    )

    Vocabulary.index.refresh()

    return vocab


@pytest.fixture(scope="function")
def initialise_custom_fields(app, db, location, cli_runner):
    """Fixture initialises custom fields."""
    return cli_runner(create_records_custom_field)


@pytest.fixture(scope="module")
def funder_data():
    """Implements a funder's data."""
    return [
        {
            "id": "00rbzpz17",
            "identifiers": [
                {
                    "identifier": "00rbzpz17",
                    "scheme": "ror",
                },
                {"identifier": "10.13039/501100001665", "scheme": "doi"},
            ],
            "name": "Agence Nationale de la Recherche",
            "title": {
                "fr": "National Agency for Research",
            },
            "country": "FR",
        },
        {
            "id": " 00k4n6c32",
            "identifiers": [
                {
                    "identifier": " 00k4n6c32",
                    "scheme": "ror",
                },
            ],
            "name": "European Commission",
            "title": {
                "fr": "European Commission",
            },
            "country": "BE",
        },
    ]


@pytest.fixture()
def minimal_restricted_record():
    """Minimal record data as dict coming from the external world."""
    return {
        "pids": {},
        "access": {
            "record": "restricted",
            "files": "restricted",
        },
        "files": {
            "enabled": False,  # Most tests don't care about files
        },
        "metadata": {
            "creators": [
                {
                    "person_or_org": {
                        "family_name": "Brown",
                        "given_name": "Troy",
                        "type": "personal",
                    }
                },
            ],
            "publication_date": "2020-06-01",
            "publisher": "Acme Inc",
            "resource_type": {"id": "image-photo"},
            "title": "A Romans story",
        },
    }


@pytest.fixture()
def minimal_record_with_files():
    """Minimal record data as dict coming from the external world."""
    return {
        "pids": {},
        "access": {
            "record": "public",
            "files": "public",
        },
        "files": {
            "enabled": True,
        },
        "metadata": {
            "creators": [
                {
                    "person_or_org": {
                        "family_name": "Brown",
                        "given_name": "Troy",
                        "type": "personal",
                    }
                },
                {
                    "person_or_org": {
                        "name": "Troy Inc.",
                        "type": "organizational",
                    },
                },
            ],
            "publication_date": "2020-06-01",
            # because DATACITE_ENABLED is True, this field is required
            "publisher": "Acme Inc",
            "resource_type": {"id": "image-photo"},
            "title": "Roman files",
        },
    }


@pytest.fixture(scope="function")
def add_pid(db):
    """Fixture to add a row to the pidstore_pid table."""

    def _add_pid(
        pid_type, pid_value, object_uuid, status=PIDStatus.REGISTERED, object_type="rec"
    ):
        pid = PersistentIdentifier.create(
            pid_type=pid_type,
            pid_value=pid_value,
            status=status,
            object_uuid=object_uuid,
            object_type=object_type,
        )
        db.session.commit()
        return pid

    return _add_pid


@pytest.fixture()
def community(running_app, db):
    """A basic community fixture."""
    comm = Community.create({})
    comm.slug = "test-community"
    comm.metadata = {"title": "Test Community"}
    comm.theme = {"brand": "test-theme-brand"}
    comm.commit()
    db.session.commit()
    return comm


# @pytest.fixture()
# def users(app, db):
#     """Create example user."""
#     with db.session.begin_nested():
#         datastore = app.extensions["security"].datastore
#         user1 = datastore.create_user(
#             email="info@inveniosoftware.org",
#             password=hash_password("password"),
#             active=True,
#         )
#         user2 = datastore.create_user(
#             email="ser-testalot@inveniosoftware.org",
#             password=hash_password("beetlesmasher"),
#             active=True,
#         )
#         UserIdentity(
#             id="11115",
#             method=cern_remote_app_name,
#             id_user=user2.id,
#         )
#     db.session.commit()
#     return [user1, user2]


@pytest.fixture(scope="function")
def orcid_name_data(app):
    """Name data for orcid user."""
    return {
        "id": "0009-0007-7638-4652",
        "internal_id": "3",  # corresponding user id
        "name": "Mendoza, Diego",
        "given_name": "Diego",
        "family_name": "Mendoza",
        "identifiers": [
            {"identifier": "0009-0007-7638-4652", "scheme": "orcid"},
        ],
        "affiliations": [{"name": "CERN"}],
    }


def _create_group(id, name, description, is_managed, database, datastore):
    """Creates a Role/Group."""
    r = datastore.create_role(
        id=id, name=name, description=description, is_managed=is_managed
    )
    datastore.commit()
    return r


@pytest.fixture(scope="module")
def group1(database, app):
    """A single group."""
    ds = app.extensions["invenio-accounts"].datastore
    r = _create_group(
        id="it-dep",
        name="it-dep",
        description="IT Department",
        is_managed=True,
        database=database,
        datastore=ds,
    )
    return r


@pytest.fixture(scope="module")
def group2(database, app):
    """A single group."""
    ds = app.extensions["invenio-accounts"].datastore
    r = _create_group(
        id="hr-dep",
        name="hr-dep",
        description="HR Department",
        is_managed=True,
        database=database,
        datastore=ds,
    )
    return r


@pytest.fixture(scope="module")
def groups(database, group1, group2):
    """Available indexed groups."""
    roles = [group1, group2]

    current_groups_service.indexer.process_bulk_queue()
    current_groups_service.record_cls.index.refresh()
    return roles
