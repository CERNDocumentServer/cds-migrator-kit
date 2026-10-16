# syntax=docker/dockerfile:1
#
# Dockerfile that builds a fully functional image of the CDS migrator kit.
#
# The Invenio base image is used to build the app, but the final image is based on
# `python:3.14-slim-trixie` to reduce the image size.
#
# Note: It is important to keep the commands in this file in sync with your
# bootstrap script located in ./scripts/bootstrap.

ARG BUILD_PLATFORM=linux/amd64

FROM --platform=${BUILD_PLATFORM} ghcr.io/inveniosoftware/invenio:debian-python3.14 AS builder

# Additional libraries needed to build the Python dependencies.
RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
    --mount=type=cache,target=/var/lib/apt,sharing=locked \
    apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install --yes --no-install-recommends \
        cmake \
        libkrb5-dev \
        libldap-dev \
        libsasl2-dev \
        pkg-config \
        uuid-dev

ENV WORKING_DIR=/opt/cds-migrator-kit
ENV INVENIO_INSTANCE_PATH=${WORKING_DIR}/var/instance
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# copy everything inside /src
RUN mkdir -p ${WORKING_DIR}/src ${INVENIO_INSTANCE_PATH}
COPY ./ ${WORKING_DIR}/src
WORKDIR ${WORKING_DIR}/src

# Install all dependencies into a virtualenv, so that it can be copied as-is into
# the final image.
RUN python -m venv .venv
ENV PATH=${WORKING_DIR}/src/.venv/bin:${PATH}

RUN echo "Install app dependencies"
RUN pip install --no-cache-dir "requests-kerberos==0.14.0"
RUN pip install --no-cache-dir ."[all,rdm]"
# XRootD
RUN pip install --no-cache-dir invenio-xrootd
# /XRootD

# The instance must be group-writable, since the container can run under an arbitrary
# UID in GID 0. Done here rather than in the final image, where a recursive chown would
# duplicate the whole tree into a second layer; COPY --from preserves owner and mode.
RUN chown -R invenio:0 ${WORKING_DIR} && chmod -R g=u ${WORKING_DIR}

# --- Application image ---
# Must stay on the same Python and Debian release as the base image above, so that the
# venv and its compiled extensions still match.
FROM --platform=${BUILD_PLATFORM} python:3.14-slim-trixie

# Shared libraries the venv links against (Kerberos auth, OpenLDAP, uuid, ...).
# procps provides ps/top/pkill/watch for interactive shells in a running container.
RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
    --mount=type=cache,target=/var/lib/apt,sharing=locked \
    apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install --yes --no-install-recommends \
        ca-certificates \
        curl \
        krb5-user \
        libgssapi-krb5-2 \
        libldap2 \
        libsasl2-2 \
        libuuid1 \
        libxml2 \
        locales \
        procps

RUN localedef -i en_US -c -f UTF-8 -A /usr/share/locale/locale.alias en_US.UTF-8

ENV WORKING_DIR=/opt/cds-migrator-kit \
    INVENIO_INSTANCE_PATH=/opt/cds-migrator-kit/var/instance \
    INVENIO_USER_ID=1000 \
    PATH=/opt/cds-migrator-kit/src/.venv/bin:${PATH} \
    LANG=en_US.UTF-8 \
    LANGUAGE=en_US:en \
    LC_ALL=en_US.UTF-8 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

RUN useradd invenio --uid 1000 --gid 0 && \
    mkdir -p ${WORKING_DIR} && \
    chown invenio:0 ${WORKING_DIR} && \
    chmod g=u ${WORKING_DIR}

# Kerberos configuration (requests-kerberos / XRootD auth)
COPY ./krb5.conf /etc/krb5.conf

COPY --from=builder ${WORKING_DIR} ${WORKING_DIR}

WORKDIR ${WORKING_DIR}/src
USER invenio
ENTRYPOINT [ "bash", "-c" ]
