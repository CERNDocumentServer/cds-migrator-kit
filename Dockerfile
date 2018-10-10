# -*- coding: utf-8 -*-
#
# Copyright (C) 2018 CERN.
#
# cds-migrator-kit is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.

FROM python:3.6

RUN apt-get update -y && apt-get upgrade -y
RUN apt-get install -y git curl vim
RUN pip install --upgrade setuptools wheel pip pipenv uwsgi uwsgitop uwsgi-tools

RUN python -m site
RUN python -m site --user-site

ENV WORKING_DIR=/opt/cds-migrator-kit
ENV INVENIO_INSTANCE_PATH=${WORKING_DIR}/var/instance

# copy everything inside /src
RUN mkdir -p ${WORKING_DIR}/src
COPY ./ ${WORKING_DIR}/src
WORKDIR ${WORKING_DIR}/src

# install all dependencies
RUN pip install -r requirements.txt

# Install/create static files
RUN mkdir -p ${INVENIO_INSTANCE_PATH}

# Set folder permissions
RUN chgrp -R 0 ${WORKING_DIR} && \
    chmod -R g=u ${WORKING_DIR}

RUN useradd invenio --uid 1000 --gid 0 && \
    chown -R invenio:root ${WORKING_DIR}
USER 1000

CMD ["/usr/local/bin/gunicorn", "-b", ":8080", "--timeout", "120", "--graceful-timeout", "60", "invenio_app.wsgi_ui:application"]
