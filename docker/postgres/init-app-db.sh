#!/usr/bin/env bash
# -*- coding: utf-8 -*-
#
# Copyright (C) 2018 CERN.
#
# cds-migrator-kit is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.

set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<-EOSQL
    CREATE ROLE cds-migrator-kit WITH LOGIN PASSWORD 'cds-migrator-kit';
    ALTER ROLE cds-migrator-kit CREATEDB;
    \du;
EOSQL
