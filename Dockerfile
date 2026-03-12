FROM registry.access.redhat.com/ubi9/python-312:latest

USER root

RUN pip install --no-cache-dir "poetry>=2.0,<3.0"

WORKDIR /opt/app-root/src

COPY pyproject.toml poetry.lock ./

RUN poetry config virtualenvs.create false && \
    poetry install --only main --no-interaction

USER 1001

COPY app.py .

CMD ["python", "app.py"]
