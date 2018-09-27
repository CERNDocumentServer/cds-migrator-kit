# -*- coding: utf-8 -*-
#
# This file is part of Invenio.
# Copyright (C) 2015-2018 CERN.
#
# cds-migrator-kit is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.

"""CDS application factories."""

import os
import sys

from invenio_base.app import create_app_factory
from invenio_base.wsgi import create_wsgi_factory
from invenio_config import create_config_loader

from cds_migrator_kit import config
from cds_migrator_kit.modules.migrator.log import set_logging

env_prefix = 'APP'

conf_loader = create_config_loader(config=config, env_prefix=env_prefix)

instance_path = os.getenv(env_prefix + '_INSTANCE_PATH') or \
    os.path.join(sys.prefix, 'var', 'instance')
"""Path to instance folder.

Defaults to ``<virtualenv>/var/instance/``. Can be overwritten using the
environment variable ``APP_INSTANCE_PATH``.
"""

static_folder = os.getenv(env_prefix + '_STATIC_FOLDER') or \
    os.path.join(instance_path, 'static')
"""Path to static folder.

Defaults to ``<virtualenv>/var/instance/static/``. Can be overwritten
using the environment variable ``APP_STATIC_FOLDER``
"""

create_api = create_app_factory(
    'migrator',
    config_loader=conf_loader,
    blueprint_entry_points=['invenio_base.api_blueprints'],
    extension_entry_points=['invenio_base.api_apps'],
    instance_path=instance_path,
)
"""Create Flask API application."""

create_app = create_app_factory(
    'migrator',
    config_loader=conf_loader,
    blueprint_entry_points=['invenio_base.blueprints'],
    extension_entry_points=['invenio_base.apps'],
    wsgi_factory=create_wsgi_factory({'/api': create_api}),
    instance_path=instance_path,
    static_folder=static_folder,
)
"""Create Flask UI application."""

set_logging()
