# nomad-events-to-slack

FROM redhat/ubi9-minimal:latest

LABEL maintainer="Campbell McKilligan <campbell@dxclabs.com>"

ARG UID=1000
ARG GID=1000

ARG PROJECT_ROOT=/opt/app-root
WORKDIR ${PROJECT_ROOT}

RUN microdnf install --setopt=keepcache=0 --nodocs --noplugins -y shadow-utils && \
    groupadd --gid $GID appgroup && \
    adduser --no-create-home --home /opt/app-root --shell /bin/bash --uid $UID --gid $GID appuser && \
    microdnf remove -y shadow-utils

COPY requirements.txt .

RUN microdnf install --setopt=keepcache=0 --nodocs --noplugins -y glibc-langpack-en.x86_64 && \
    microdnf install --setopt=keepcache=0 --nodocs --noplugins -y python312 && \
    microdnf clean all && \
    ln -s /usr/bin/python3.12 /usr/bin/python

COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/
RUN uv pip install --system -r requirements.txt

# burn in repo/commit parameters from cloud build
ARG BRANCH_NAME=""
ARG COMMIT_SHA=""
ARG REPO_NAME=""
ARG REVISION_ID=""
ARG SHORT_SHA=""
ARG TAG_NAME=""

RUN echo BRANCH_NAME=${BRANCH_NAME} >> .env && \
    echo COMMIT_SHA=${COMMIT_SHA} >> .env && \
    echo REPO_NAME=${REPO_NAME} >> .env && \
    echo REVISION_ID=${REVISION_ID} >> .env && \
    echo SHORT_SHA=${SHORT_SHA} >> .env && \
    echo TAG_NAME=${TAG_NAME} >> .env

COPY --chown=appuser:appgroup . .

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/opt/app-root/

STOPSIGNAL SIGINT

USER appuser

CMD ["python", "app.py"]
