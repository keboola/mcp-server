# syntax=docker/dockerfile:1
#
# Node multi-stage build for @keboola/mcp-server (replaces the Python/uv image).
# Build-time inputs are ARGs; runtime configuration is ENV — kept strictly
# separate so a build never needs runtime secrets and the running container is
# configured only at `docker run` / k8s.

# Pin a specific Node 22 patch rather than a floating tag (matches kai-agent's
# stance) so a bad upstream release can't silently change the runtime.
FROM node:22.20.0-slim AS base
WORKDIR /app
ENV NODE_ENV=production

# ----------------------------------------------------------------------------
# Stage: deps — install ALL deps (incl. dev) from the lockfile for the build.
# ----------------------------------------------------------------------------
FROM base AS deps
COPY package.json package-lock.json ./
RUN --mount=type=cache,target=/root/.npm npm ci --include=dev

# ----------------------------------------------------------------------------
# Stage: builder — compile TypeScript to dist/. SKIP_ENV_VALIDATION keeps the
# build from requiring any runtime env var (build-time vs run-time separation).
# ----------------------------------------------------------------------------
FROM base AS builder
# Build-time-only metadata (consumed by the build, not the runtime contract).
ARG APP_VERSION=DEV
ENV SKIP_ENV_VALIDATION=1
COPY --from=deps /app/node_modules ./node_modules
COPY . .
RUN npm run build

# Reduce node_modules to production-only for the runtime image.
RUN --mount=type=cache,target=/root/.npm npm ci --omit=dev

# ----------------------------------------------------------------------------
# Stage: runner — slim runtime image.
# ----------------------------------------------------------------------------
FROM node:22.20.0-slim AS runner
# Tell the DD agent to aggregate multiline logs (stack traces).
LABEL com.datadoghq.ad.logs='[{"auto_multi_line_detection": true}]'
WORKDIR /app

COPY --from=builder /app/dist ./dist
COPY --from=builder /app/node_modules ./node_modules
COPY --from=builder /app/package.json ./package.json

# ---- Runtime configuration (provided at `docker run` / k8s) ----
ENV NODE_ENV=production
# Bind to all interfaces inside the container; map a port on the host.
ENV HOST=0.0.0.0
ENV PORT=8000
ENV LOG_LEVEL=INFO
# Datadog APM: load the tracer before app code. dd-trace reads DD_* at runtime.
ENV NODE_OPTIONS="--import dd-trace/initialize.mjs"
ENV DD_LOGS_INJECTION=true
# Runtime config NOT baked into the image (set per deployment):
#   HOSTNAME_SUFFIX, KBC_STORAGE_API_URL, KBC_STORAGE_TOKEN, KBC_BRANCH_ID,
#   KBC_WORKSPACE_SCHEMA, KBC_OAUTH_CLIENT_ID/SECRET, KBC_JWT_SECRET, DD_SERVICE/ENV/VERSION.

# Non-root (uid 1000), matching the previous image.
USER 1000
EXPOSE 8000

# Default to the streamable-HTTP server (the deployed mode); stdio is for local
# CLI/npx use. Override args in k8s if needed.
CMD ["node", "dist/index.js", "--transport", "streamable-http"]
