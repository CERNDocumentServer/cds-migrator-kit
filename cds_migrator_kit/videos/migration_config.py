"""CDS-Videos settings for CDS-Videos project."""

import json
import os
from datetime import datetime, timedelta


def _(x):  # needed to avoid start time failure with lazy strings
    return x


# Since HAProxy and Nginx route all requests no matter the host header
# provided, the allowed hosts variable is set to localhost. In production it
# should be set to the correct host and it is strongly recommended to only
# route correct hosts to the application.
APP_ALLOWED_HOSTS = ["0.0.0.0", "localhost", "127.0.0.1", "localhost.cern.ch"]

SQLALCHEMY_DATABASE_URI = (
    "postgresql+psycopg2://cds-videos:cds-videos@localhost/cds-videos"
)

# SECURITY WARNING: keep the secret key used in production secret!
# Do not commit it to a source code repository.
# TODO: Set
SECRET_KEY = "CHANGE_ME"

# TODO: Set with your own hostname when deploying to production
SITE_UI_URL = "https://127.0.0.1"

SITE_API_URL = "https://127.0.0.1/api"


DATACITE_ENABLED = True
DATACITE_USERNAME = ""
DATACITE_PASSWORD = ""
DATACITE_PREFIX = "10.17181"
DATACITE_TEST_MODE = True
DATACITE_DATACENTER_SYMBOL = ""

import cds_migrator_kit

base_path = os.path.dirname(os.path.realpath(cds_migrator_kit.__file__))
logs_dir = os.path.join(base_path, "tmp/logs/")
CDS_MIGRATOR_KIT_LOGS_PATH = logs_dir
CDS_MIGRATOR_KIT_VIDEOS_STREAM_CONFIG = (
    "cds_migrator_kit/videos/weblecture_migration/streams.yaml"
)

### CDS MIGRATOR #################################

# TODO CHANGE THEM
MOUNTED_MEDIA_CEPH_PATH = "/cephfs/media_data"

WEBLECTURES_MIGRATION_SYSTEM_USER = "weblecture-service@cern.ch"
