"""
InvenioRDM settings for CDS-RDM project.

This file was automatically generated by 'invenio-cli init'.

For the full list of settings and their values, see
https://inveniordm.docs.cern.ch/reference/configuration/.
"""

import json
import os
from datetime import datetime, timedelta

from cds_rdm.files import storage_factory
from cds_rdm.permissions import (
    CDSCommunitiesPermissionPolicy,
    CDSRDMRecordPermissionPolicy,
)
from invenio_app_rdm.config import CELERY_BEAT_SCHEDULE as APP_RDM_CELERY_BEAT_SCHEDULE
from invenio_app_rdm.config import *
from invenio_i18n import lazy_gettext as _
from invenio_records_resources.services.custom_fields import KeywordCF
from invenio_vocabularies.services.custom_fields import VocabularyCF


def _(x):  # needed to avoid start time failure with lazy strings
    return x


# Flask
# =====
# See https://flask.palletsprojects.com/en/1.1.x/config/

# Define the value of the cache control header `max-age` returned by the server when serving
# public files. Files will be cached by the browser for the provided number of seconds.
# See flask documentation for more information:
# https://flask.palletsprojects.com/en/2.1.x/config/#SEND_FILE_MAX_AGE_DEFAULT
SEND_FILE_MAX_AGE_DEFAULT = 300

# SECURITY WARNING: keep the secret key used in production secret!
# Do not commit it to a source code repository.
# TODO: Set
SECRET_KEY = "CHANGE_ME"

# Since HAProxy and Nginx route all requests no matter the host header
# provided, the allowed hosts variable is set to localhost. In production it
# should be set to the correct host and it is strongly recommended to only
# route correct hosts to the application.
APP_ALLOWED_HOSTS = ["0.0.0.0", "localhost", "127.0.0.1", "localhost.cern.ch"]

# Flask-SQLAlchemy
# ================
# See https://flask-sqlalchemy.palletsprojects.com/en/2.x/config/

# TODO: Set
SQLALCHEMY_DATABASE_URI = "postgresql+psycopg2://cds-rdm:cds-rdm@localhost/cds-rdm"

# Invenio-App
# ===========
# See https://invenio-app.readthedocs.io/en/latest/configuration.html

APP_DEFAULT_SECURE_HEADERS = {
    "content_security_policy": {
        "default-src": [
            "'self'",
            "data:",  # for fonts
            "'unsafe-inline'",  # for inline scripts and styles
            "blob:",  # for pdf preview
            # Add your own policies here (e.g. analytics)
        ],
    },
    "content_security_policy_report_only": False,
    "content_security_policy_report_uri": None,
    "force_file_save": False,
    "force_https": True,
    "force_https_permanent": False,
    "frame_options": "sameorigin",
    "frame_options_allow_from": None,
    "session_cookie_http_only": True,
    "session_cookie_secure": True,
    "strict_transport_security": True,
    "strict_transport_security_include_subdomains": True,
    "strict_transport_security_max_age": 31556926,  # One year in seconds
    "strict_transport_security_preload": False,
}

# Flask-Babel
# ===========
# See https://python-babel.github.io/flask-babel/#configuration

# Default locale (language)
BABEL_DEFAULT_LOCALE = "en"
# Default time zone
BABEL_DEFAULT_TIMEZONE = "Europe/Zurich"

# Invenio-I18N
# ============
# See https://invenio-i18n.readthedocs.io/en/latest/configuration.html

# Other supported languages (do not include BABEL_DEFAULT_LOCALE in list).
I18N_LANGUAGES = [
    # ('de', _('German')),
    # ('tr', _('Turkish')),
]

# Invenio-Theme
# =============
# See https://invenio-theme.readthedocs.io/en/latest/configuration.html

# Frontpage title
THEME_FRONTPAGE_TITLE = "CERN Document Server"
# Header logo
THEME_LOGO = "images/invenio-rdm.svg"
THEME_SHOW_FRONTPAGE_INTRO_SECTION = False

THEME_SITENAME = "CDS"
# Templates
# THEME_FRONTPAGE_TEMPLATE = 'cds_rdm/frontpage.html'
# THEME_FOOTER_TEMPLATE = 'cds_rdm/footer.html'
# THEME_HEADER_TEMPLATE = 'cds_rdm/header.html'

# TEMPORAL FIX - to be removed once the js bundle loading issue on the macros
# is fixed.
BASE_TEMPLATE = "cds_rdm/page.html"

# Invenio-App-RDM
# ===============
# See https://invenio-app-rdm.readthedocs.io/en/latest/configuration.html

# Instance's theme entrypoint file. Path relative to the ``assets/`` folder.
INSTANCE_THEME_FILE = "./less/theme.less"

# Invenio-communities
# ===================
# Communities permission policy
COMMUNITIES_PERMISSION_POLICY = CDSCommunitiesPermissionPolicy
COMMUNITIES_ADMINISTRATION_DISABLED = False
COMMUNITIES_ALLOW_RESTRICTED = True

# Invenio-Records-Resources
# =========================
# See https://github.com/inveniosoftware/invenio-records-resources/blob/master/invenio_records_resources/config.py

# TODO: Set with your own hostname when deploying to production
SITE_UI_URL = "https://127.0.0.1"

SITE_API_URL = "https://127.0.0.1/api"

APP_RDM_DEPOSIT_FORM_DEFAULTS = {
    "publication_date": lambda: datetime.now().strftime("%Y-%m-%d"),
    "rights": [
        {
            "id": "cc-by-4.0",
            "title": "Creative Commons Attribution 4.0 International",
            "description": (
                "The Creative Commons Attribution license allows "
                "re-distribution and re-use of a licensed work "
                "on the condition that the creator is "
                "appropriately credited."
            ),
            "link": "https://creativecommons.org/licenses/by/4.0/legalcode",
        }
    ],
    "publisher": "CERN",
}
APP_RDM_RECORD_LANDING_PAGE_TEMPLATE = "cds_rdm/records/detail.html"

# See https://github.com/inveniosoftware/invenio-app-rdm/blob/master/invenio_app_rdm/config.py
APP_RDM_DEPOSIT_FORM_AUTOCOMPLETE_NAMES = "search"  # "search_only" or "off"

# Invenio-RDM-Records
# ===================
# See https://inveniordm.docs.cern.ch/customize/dois/
DATACITE_ENABLED = False
DATACITE_USERNAME = ""
DATACITE_PASSWORD = ""
DATACITE_PREFIX = ""
DATACITE_TEST_MODE = True
DATACITE_DATACENTER_SYMBOL = ""

# Authentication - Invenio-Accounts and Invenio-OAuthclient
# =========================================================
# See: https://inveniordm.docs.cern.ch/customize/authentication/

# Invenio-Accounts
# ================
# See https://github.com/inveniosoftware/invenio-accounts/blob/master/invenio_accounts/config.py
ACCOUNTS_DEFAULT_USERS_VERIFIED = True  # ensure that users are verified by default
ACCOUNTS_DEFAULT_USER_VISIBILITY = (
    "public"  # enables users to be searchable for invites
)
ACCOUNTS_LOCAL_LOGIN_ENABLED = True  # enable local login
PERMANENT_SESSION_LIFETIME = timedelta(days=10)
SECURITY_REGISTERABLE = True  # local login: allow users to register
SECURITY_RECOVERABLE = False  # local login: allow users to reset the password
SECURITY_CHANGEABLE = False  # local login: allow users to change psw
SECURITY_CONFIRMABLE = False  # local login: users can confirm e-mail address
SECURITY_LOGIN_WITHOUT_CONFIRMATION = (
    True  # require users to confirm email before being able to login
)

# Emails sending
# Disable sending all account-related emails because of CERN SSO usage
SECURITY_SEND_PASSWORD_CHANGE_EMAIL = False
SECURITY_SEND_PASSWORD_RESET_EMAIL = False
SECURITY_SEND_PASSWORD_RESET_NOTICE_EMAIL = False
SECURITY_SEND_REGISTER_EMAIL = False

from urllib.parse import quote

from cds_rdm.oidc import (
    cern_groups_handler,
    cern_groups_serializer,
    cern_info_handler,
    cern_info_serializer,
    cern_setup_handler,
    confirm_registration_form,
)

# Invenio-OAuthclient
# ===================
# See https://github.com/inveniosoftware/invenio-oauthclient/blob/master/invenio_oauthclient/config.py
from invenio_oauthclient.contrib.keycloak import KeycloakSettingsHelper

CERN_KEYCLOAK_BASE_URL = os.environ.get(
    "INVENIO_CERN_KEYCLOAK_BASE_URL", "https://keycloak-qa.cern.ch/"
)

_keycloak_helper = KeycloakSettingsHelper(
    title="CERN",
    description="CERN SSO authentication",
    base_url=CERN_KEYCLOAK_BASE_URL,
    realm="cern",
    app_key="CERN_APP_CREDENTIALS",
    logout_url="{}auth/realms/cern/protocol/openid-connect/logout?redirect_uri={}".format(
        CERN_KEYCLOAK_BASE_URL,
        quote(os.environ.get("INVENIO_SITE_UI_URL", SITE_UI_URL)),
    ),
)
OAUTHCLIENT_CERN_REALM_URL = _keycloak_helper.realm_url
OAUTHCLIENT_CERN_USER_INFO_URL = _keycloak_helper.user_info_url
OAUTHCLIENT_CERN_VERIFY_EXP = True
OAUTHCLIENT_CERN_VERIFY_AUD = False
OAUTHCLIENT_CERN_USER_INFO_FROM_ENDPOINT = True

handlers = _keycloak_helper.get_handlers()
handlers["signup_handler"] = {
    **handlers["signup_handler"],
    "info": cern_info_handler,
    "info_serializer": cern_info_serializer,
    "groups_serializer": cern_groups_serializer,
    "groups": cern_groups_handler,
    "setup": cern_setup_handler,
}
rest_handlers = _keycloak_helper.get_rest_handlers()
rest_handlers["signup_handler"] = {
    **rest_handlers["signup_handler"],
    "info": cern_info_handler,
    "info_serializer": cern_info_serializer,
    "groups_serializer": cern_groups_serializer,
    "groups": cern_groups_handler,
    "setup": cern_setup_handler,
}

OAUTHCLIENT_SIGNUP_FORM = confirm_registration_form

OAUTH_REMOTE_APP_NAME = "cern"

OAUTHCLIENT_REMOTE_APPS = {
    OAUTH_REMOTE_APP_NAME: _keycloak_helper.remote_app,
}

CERN_APP_CREDENTIALS = {
    "consumer_key": "CHANGE ME",
    "consumer_secret": "CHANGE ME",
}

from invenio_oauthclient.views.client import auto_redirect_login

ACCOUNTS_LOGIN_VIEW_FUNCTION = (
    auto_redirect_login  # autoredirect to external login if enabled
)
OAUTHCLIENT_AUTO_REDIRECT_TO_EXTERNAL_LOGIN = True  # autoredirect to external login

# Invenio-UserProfiles
# ====================
USERPROFILES_READ_ONLY = (
    False  # allow users to change profile info (name, email, etc...)
)
USERPROFILES_EXTEND_SECURITY_FORMS = True

# OAI-PMH
# =======
# See https://github.com/inveniosoftware/invenio-oaiserver/blob/master/invenio_oaiserver/config.py
OAISERVER_ID_PREFIX = "cds-rdm.com"
"""The prefix that will be applied to the generated OAI-PMH ids."""

# Invenio-Search
# ==============
SEARCH_INDEX_PREFIX = "cds-rdm-"

# Celery
# ======
CELERY_BEAT_SCHEDULE = {
    **APP_RDM_CELERY_BEAT_SCHEDULE,
    "user-sync": {
        "task": "cds_rdm.tasks.sync_users",
        "schedule": crontab(minute=0, hour=3),  # Every day at 03:00 UTC
    },
    "groups-sync": {
        "task": "cds_rdm.tasks.sync_groups",
        "schedule": crontab(minute=0, hour=2),  # Every day at 02:00 UTC
    },
}

###############################################################################
# CDS-RDM configuration
###############################################################################
CDS_SERVICE_ELEMENT_URL = (
    "https://cern.service-now.com/service-portal?id=service_element&name=CDS-Service"
)

# AUTH/LDAP
CERN_LDAP_URL = "ldap://xldap.cern.ch"
CERN_AUTHORIZATION_SERVICE_API = (
    "https://authorization-service-api-qa.web.cern.ch/api/v1.0/"
)
CERN_AUTHORIZATION_SERVICE_API_GROUP = "Group"

# Permissions: define who can create new communities
CDS_EMAILS_ALLOW_CREATE_COMMUNITIES = []
CDS_GROUPS_ALLOW_CREATE_COMMUNITIES = []

# Invenio-Files-REST
# ==================
XROOTD_ENABLED = False
# control file download offloading
FILES_REST_STORAGE_FACTORY = storage_factory
FILES_REST_XSENDFILE_ENABLED = False
CDS_EOS_OFFLOAD_ENABLED = False
CDS_LOCAL_OFFLOAD_ENABLED = False
CDS_LOCAL_OFFLOAD_FILES = ["file.txt", "file2.txt"]
CDS_LOCAL_OFFLOAD_STORAGE = ""

CDS_EOS_OFFLOAD_HTTPHOST = ""
# Specifies whether to use X509 authentication for EOS offload
CDS_EOS_OFFLOAD_AUTH_X509 = False
# The path to the X509 certificate file
CDS_EOS_OFFLOAD_X509_CERT_PATH = ""
# The path to the X509 private key file
CDS_EOS_OFFLOAD_X509_KEY_PATH = ""
# check nginx config for more details
CDS_EOS_OFFLOAD_REDIRECT_BASE_PATH = ""

RDM_PERMISSION_POLICY = CDSRDMRecordPermissionPolicy

RDM_NAMESPACES = {
    # CERN
    "cern": "https://greybook.cern.ch/",
}

RDM_CUSTOM_FIELDS = [
    VocabularyCF(
        name="cern:experiments",
        vocabulary_id="experiments",
        dump_options=True,
        multiple=True,
    ),
    VocabularyCF(
        name="cern:departments",
        vocabulary_id="departments",
        dump_options=True,
        multiple=True,
    ),
    VocabularyCF(
        name="cern:accelerators",
        vocabulary_id="accelerators",
        dump_options=True,
        multiple=True,
    ),
    KeywordCF(name="cern:projects", multiple=True),
    KeywordCF(name="cern:facilities", multiple=True),
    KeywordCF(name="cern:studies", multiple=True),
    KeywordCF(name="cern:beams", multiple=True),
]

base_path = os.path.dirname(os.path.realpath(__file__))
logs_dir = os.path.join(base_path, "tmp/logs/")
CDS_MIGRATOR_KIT_LOGS_PATH = logs_dir
CDS_MIGRATOR_KIT_STREAM_CONFIG = "cds_migrator_kit/rdm/migration/streams.yaml"

from invenio_rdm_records.config import (
    RDM_RECORDS_IDENTIFIERS_SCHEMES,
    always_valid,
    RDM_RECORDS_PERSONORG_SCHEMES,
)
from cds_rdm import schemes

RDM_RECORDS_IDENTIFIERS_SCHEMES = {
    **RDM_RECORDS_IDENTIFIERS_SCHEMES,
    **{
        "cds_ref": {
            "label": _("CDS Reference"),
            "validator": always_valid,
            "datacite": "CDS",
        },
        "aleph": {
            "label": _("Aleph number"),
            "validator": schemes.is_aleph,
            "datacite": "ALEPH",
        },
        "inspire": {
            "label": _("Inspire"),
            "validator": schemes.is_inspire,
            "datacite": "INSPIRE",
        },
    },
}

RDM_RECORDS_PERSONORG_SCHEMES = {
    **RDM_RECORDS_PERSONORG_SCHEMES,
    **{
        "inspire": {
            "label": _("Inspire"),
            "validator": schemes.is_inspire,
            "datacite": "INSPIRE",
        }
    },
}


CDS_MIGRATOR_KIT_RECORD_STATS_STREAM_CONFIG = dict(
    ####### Search ##############
    SRC_SEARCH_HOSTS=json.loads(
        os.environ.get("CDS_MIGRATOR_KIT_SRC_SEARCH_HOSTS", "[]")
    ),
    SRC_SEARCH_SIZE=5000,
    SRC_SEARCH_SCROLL="1h",
)
"""Config for record statistics migration."""
