# -*- coding: utf-8 -*-
#
# Copyright (C) 2018 CERN.
#
# cds-migrator-kit is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.

"""Migration tool kit from old invenio to new flavours."""

import os

from setuptools import find_packages, setup

readme = open('README.rst').read()
history = open('CHANGES.rst').read()

tests_require = [
    'check-manifest>=0.35',
    'coverage>=4.4.1',
    'isort>=4.3',
    'pydocstyle>=2.0.0',
    'pytest-cov>=2.5.1',
    'pytest-pep8>=1.0.6',
    'pytest>=3.3.1',
    'pytest-invenio>=1.0.5,<1.1.0',
]

extras_require = {
    'docs': [
        'Sphinx>=1.5.1',
    ],
    'tests': tests_require,
}

extras_require['all'] = []
for reqs in extras_require.values():
    extras_require['all'].extend(reqs)

setup_requires = [
    'Babel>=1.3',
    'pytest-runner>=2.6.2',
]

install_requires = [
    'cds-dojson>=0.9.0',
    'Flask-BabelEx>=0.9.3',
    'invenio-app>=1.0.4',
    'invenio-base>=1.0.1',
    'invenio-config>=1.0.0',
    'invenio-logging>=1.0.0',
    'invenio-db[postgresql,versioning]>=1.0.0',
    'invenio-files-rest>=1.0.0a18',
    'invenio-migrator>=1.0.0a9',
    'invenio-pidstore>=1.0.0',
    'invenio-records>=1.0.0',
    'invenio-records-files>=1.0.0a10',
    'pathlib>=1.0.1',
]

packages = find_packages()

# Get the version string. Cannot be done with import!
g = {}
with open(os.path.join('cds_migrator_kit', 'version.py'), 'rt') as fp:
    exec (fp.read(), g)
    version = g['__version__']

setup(
    name='cds-migrator-kit',
    version=version,
    description=__doc__,
    long_description=readme + '\n\n' + history,
    keywords='invenio TODO',
    license='MIT',
    author='CERN',
    author_email='info@inveniosoftware.org',
    url='https://github.com/kprzerwa/cds-migrator-kit',
    packages=packages,
    zip_safe=False,
    include_package_data=True,
    platforms='any',
    entry_points={
        'console_scripts': [
            'migrator = invenio_app.cli:cli',
        ],
        'flask.commands': [
            'report = cds_migrator_kit.records.cli:report',
            'circulation = cds_migrator_kit.circulation.items.cli:circ_items',
        ],
        'invenio_base.apps': [
            'cds_migrator_kit = cds_migrator_kit:CdsMigratorKit',
        ],
        "invenio_base.blueprints": [
            'cds_migrator_kit_views'
            ' = cds_migrator_kit.records.views:blueprint',
        ],
        "invenio_config.module": [
            "00_cds_migrator_kit = cds_migrator_kit.config",
        ],
        'invenio_i18n.translations': [
            'messages = cds_migrator_kit',
        ],
    },
    extras_require=extras_require,
    install_requires=install_requires,
    setup_requires=setup_requires,
    tests_require=tests_require,
    classifiers=[
        'Environment :: Web Environment',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Topic :: Internet :: WWW/HTTP :: Dynamic Content',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.5',
        'Development Status :: 1 - Planning',
    ],
)
