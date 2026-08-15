FROM python:3.12-slim

RUN apt-get update && apt-get install -y \
    git \
    && rm -rf /var/lib/apt/lists/*

# Copy the workspace so uv can resolve workspace refs
COPY pyproject.toml /opt/prefect/workspace/pyproject.toml
COPY packages/omicidx-parsers /opt/prefect/workspace/packages/omicidx-parsers
COPY packages/omicidx-prefect /opt/prefect/workspace/packages/omicidx-prefect

WORKDIR /opt/prefect/omicidx

# Symlink for the prefect.yaml `pull` step
RUN ln -s /opt/prefect/workspace/packages/omicidx-prefect/src /opt/prefect/omicidx/src \
    && ln -s /opt/prefect/workspace/packages/omicidx-prefect/prefect.yaml /opt/prefect/omicidx/prefect.yaml

# Client pinned to the running prefect-server version. Unpinned, every image
# rebuild floats it (3.7.8 -> 3.8.3 on 2026-08-14 against a 3.7.2 server, which
# logs a version-mismatch warning on worker start). Bump this only together
# with the shared prefect-server in monode/infrastructure/compose/prefect/.
RUN pip install --no-cache-dir \
    "prefect==3.7.2" \
    /opt/prefect/workspace/packages/omicidx-parsers \
    /opt/prefect/workspace/packages/omicidx-prefect

# Default to a process-style worker; override with `command:` in compose.
CMD ["prefect", "worker", "start", "--pool", "default", "--type", "process"]
