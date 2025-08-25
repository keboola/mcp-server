# Use a Python image with uv pre-installed
FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim AS uv

# Install the project into /app
WORKDIR /app

# Enable bytecode compilation
ENV UV_COMPILE_BYTECODE=1

# Copy from the cache instead of linking since it's a mounted volume
ENV UV_LINK_MODE=copy

# Install the project's dependencies using the lockfile and settings
COPY uv.lock pyproject.toml README.md /app/
RUN --mount=type=cache,target=/root/.cache/uv     uv sync --frozen --no-install-project --no-dev --no-editable

# Then, add the rest of the project source code and install it
# Installing separately from its dependencies allows optimal layer caching
ADD src /app/src
RUN --mount=type=cache,target=/root/.cache/uv     uv sync --frozen --no-dev --no-editable
RUN --mount=type=cache,target=/root/.cache/uv     uv pip install ddtrace~=3.0

FROM python:3.12-slim-bookworm

ARG APP_USER_NAME=app
ARG APP_USER_UID=1000
ARG APP_USER_GID=1000

RUN groupadd --gid ${APP_USER_GID} ${APP_USER_NAME} \
  && useradd --uid ${APP_USER_UID} --gid ${APP_USER_GID} ${APP_USER_NAME}

WORKDIR /app
ENV LOG_CONFIG=/app/logging-json.conf

COPY --from=uv --chown=${APP_USER_UID}:${APP_USER_GID} /app/.venv /app/.venv
COPY logging-json.conf /app/logging-json.conf

# Place executables in the environment at the front of the path
ENV PATH="/app/.venv/bin:$PATH"

USER $APP_USER_NAME

# when running the container, add KBC_STORAGE_API_URL environment variable and a bind mount to the host's db file
ENTRYPOINT ["python", "-m", "keboola_mcp_server", "--log-level", "DEBUG"]
