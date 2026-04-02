#!/usr/bin/env bash
# Reference card: test this MCP server branch with KaiBench.
#
# Architecture:
#   KaiBench ──► Kai backend (localhost:3001) ──► MCP server (localhost:8000/mcp)
#
# NOTE: You cannot use a remote/production Kai service with a local MCP server.
#       MCP_SERVER_URL is a fixed env var in the Kai backend — there is no
#       per-request or per-project override. A local Kai backend is required.
#
# Prereqs: git, uv, yarn, docker, Python 3.10+, Node 22+
#
# Usage (prints setup instructions for each terminal):
#   bash scripts/test-with-kaibench.sh
#
# Override defaults:
#   UI_DIR=/my/kai-ui KAIBENCH_DIR=/my/KaiBench bash scripts/test-with-kaibench.sh

set -euo pipefail

BRANCH="$(git rev-parse --abbrev-ref HEAD)"
MCP_DIR="$(cd "$(dirname "$0")/.." && pwd)"
UI_DIR="${UI_DIR:-/tmp/kai-ui}"
KAIBENCH_DIR="${KAIBENCH_DIR:-/tmp/kaibench}"
MCP_PORT=8000
KAI_PORT=3001

echo "========================================"
echo " KaiBench local MCP test setup"
echo " MCP branch : $BRANCH"
echo " MCP dir    : $MCP_DIR"
echo " Kai UI dir : $UI_DIR"
echo " KaiBench   : $KAIBENCH_DIR"
echo "========================================"
echo ""

# ─── ONE-TIME SETUP ─────────────────────────────────────────────────────────

echo "=== ONE-TIME SETUP (skip if already done) ==="
echo ""
echo "# 1. Clone and configure Kai backend"
if [ ! -d "$UI_DIR" ]; then
    echo "git clone git@github.com:keboola/ui.git $UI_DIR"
else
    echo "# UI repo already at $UI_DIR — to refresh: git -C $UI_DIR pull"
fi
cat <<EOF
cd $UI_DIR
yarn install
yarn db:migrate

# Create .env.local for the Kai assistant backend:
cp apps/kai-assistant-backend/.env.example apps/kai-assistant-backend/.env.local
# Fill in required credentials (AUTH_SECRET, AI provider keys, KEBOOLA_STORAGE_API_URL, etc.)
# Then add/ensure:
#   MCP_SERVER_URL=http://localhost:${MCP_PORT}/mcp

EOF

echo "# 2. Clone and configure KaiBench"
if [ ! -d "$KAIBENCH_DIR" ]; then
    echo "git clone git@github.com:keboola/KaiBench.git $KAIBENCH_DIR"
else
    echo "# KaiBench already at $KAIBENCH_DIR"
fi
cat <<EOF
cd $KAIBENCH_DIR
uv sync --all-extras
# Create .env with project credentials (see KaiBench README):
#   KAIBENCH_STATIC_HOST=connection.<stack>.keboola.com
#   KAIBENCH_STATIC_PROJECT_ID=<project_id>
#   KAIBENCH_STATIC_TOKEN=<storage_api_token>
#   KAIBENCH_ANTHROPIC_API_KEY=<anthropic_key>
#   KAIBENCH_EVAL_KAI_BACKEND_URL=http://localhost:${KAI_PORT}

EOF

# ─── TERMINAL 1: MCP SERVER ─────────────────────────────────────────────────

echo "=== TERMINAL 1: Start local MCP server ==="
echo ""
echo "# Option A — run from this checkout:"
cat <<EOF
cd $MCP_DIR
source 3.12.venv/bin/activate  # or: python3.10 -m venv .venv && uv sync
python -m keboola_mcp_server --transport streamable-http
# Listening: http://localhost:${MCP_PORT}/mcp
EOF
echo ""
echo "# Option B — clone branch to /tmp (no local repo needed):"
cat <<EOF
git clone -b $BRANCH git@github.com:keboola/mcp-server.git /tmp/mcp-test
cd /tmp/mcp-test && uv sync --extra dev
uv run python -m keboola_mcp_server --transport streamable-http
EOF
echo ""
echo "# Option C — no clone at all (uvx):"
echo "uvx --from \"git+https://github.com/keboola/mcp-server@${BRANCH}\" keboola_mcp_server --transport streamable-http"
echo ""

# ─── TERMINAL 2: KAI BACKEND ────────────────────────────────────────────────

echo "=== TERMINAL 2: Start Kai backend ==="
echo ""
cat <<EOF
cd $UI_DIR
docker compose -f apps/kai-assistant-backend/docker-compose.yml up -d postgres redis
# (skip the keboola-mcp container — we use the local MCP server above)
yarn dev
# Kai backend at http://localhost:${KAI_PORT}
EOF
echo ""

# ─── TERMINAL 3: KAIBENCH ───────────────────────────────────────────────────

echo "=== TERMINAL 3: Run KaiBench ==="
echo ""
cat <<EOF
source $KAIBENCH_DIR/.venv/bin/activate

# Health check
kaibench health-check --backend-url http://localhost:${KAI_PORT}

# MCP Tool Validation (best for MCP tool changes)
kaibench run --backend-url http://localhost:${KAI_PORT} --type "MCP Tool Validation"

# All questions
kaibench run --backend-url http://localhost:${KAI_PORT}

# Specific question IDs
kaibench run --backend-url http://localhost:${KAI_PORT} --question 5 --question 12

# Results saved to: $KAIBENCH_DIR/results/
EOF
echo ""
echo "Tip: set KAIBENCH_EVAL_KAI_BACKEND_URL=http://localhost:${KAI_PORT} in KaiBench .env to skip --backend-url flag."
