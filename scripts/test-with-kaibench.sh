#!/usr/bin/env bash
# Reference card: test this MCP server branch with KaiBench.
#
# Architecture:
#   KaiBench ──► Kai backend (localhost:3001) ──► MCP server (localhost:8000/mcp)
#
# NOTE: You cannot use a remote/production Kai service with a local MCP server.
#       MCP_SERVER_URL is a fixed env var in the Kai backend — no per-request override.
#
# Prereqs: git, uv, yarn, docker, Python 3.10+, Node 22+
#
# Usage:
#   bash scripts/test-with-kaibench.sh            # print full setup instructions
#   bash scripts/test-with-kaibench.sh --diff      # print only relevant test phases for current branch
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

# ─── PARALLELISM LIMIT ───────────────────────────────────────────────────────
# Always run with 1 worker. The default (4) pins all cores during LLM streaming
# and can make the machine unresponsive. Single-threaded is slower but safe.
PARALLEL_WORKERS=1

# ─── CHANGED-TOOL DETECTION ──────────────────────────────────────────────────
# Maps changed source files → MCP tools → relevant KaiBench phases.
# Run with --diff to print only the phases relevant to this branch's changes.

detect_relevant_phases() {
    local base="${1:-main}"
    local changed_files
    changed_files="$(git diff "${base}..HEAD" --name-only 2>/dev/null || git diff --name-only)"

    declare -A phase_set

    while IFS= read -r f; do
        case "$f" in
            # run_job / get_jobs
            src/keboola_mcp_server/tools/jobs.py|\
            src/keboola_mcp_server/clients/jobs_queue.py)
                phase_set["MCP-01b"]=1   # TEST-15,16 → get_jobs
                phase_set["MCP-06"]=1    # TEST-38    → get_jobs
                # NOTE: run_job itself has NO KaiBench test yet — see ADD-NEW-TEST below
                echo "# ⚠  run_job changed but has no dedicated KaiBench test (see ADD-NEW-TEST section)"
                ;;
            # get_buckets
            src/keboola_mcp_server/tools/storage_buckets.py|\
            src/keboola_mcp_server/clients/storage_client.py)
                phase_set["MCP-01"]=1    # TEST-02 → get_buckets
                ;;
            # get_tables / query_data
            src/keboola_mcp_server/tools/storage_tables.py)
                phase_set["MCP-01"]=1    # TEST-03-05 → get_tables
                phase_set["MCP-02"]=1    # TEST-17-21 → query_data
                phase_set["MCP-06"]=1    # TEST-36-37 → query_data
                ;;
            # get_configs / create_config / update_config / add_config_row / get_components / find_component_id
            src/keboola_mcp_server/tools/components.py|\
            src/keboola_mcp_server/tools/configurations.py)
                phase_set["MCP-01"]=1    # TEST-07-10  → get_configs
                phase_set["MCP-01b"]=1   # TEST-11-12  → find_component_id, get_components
                phase_set["MCP-03"]=1    # TEST-25-28  → create_config, update_config, add_config_row
                ;;
            # get_flows / modify_flow
            src/keboola_mcp_server/tools/flows.py)
                phase_set["MCP-01b"]=1   # TEST-13-14  → get_flows
                phase_set["MCP-04"]=1    # TEST-29-31  → get_flow_schema, get_flow_examples
                ;;
            # create_sql_transformation / update_sql_transformation
            src/keboola_mcp_server/tools/transformations.py)
                phase_set["MCP-03"]=1    # TEST-22-24
                ;;
            # search
            src/keboola_mcp_server/tools/search.py)
                phase_set["MCP-01"]=1    # TEST-06 → search
                phase_set["MCP-06"]=1    # TEST-34 → search
                ;;
            # docs_query / get_data_apps
            src/keboola_mcp_server/tools/docs.py|\
            src/keboola_mcp_server/tools/data_apps.py)
                phase_set["MCP-05"]=1    # TEST-32-33
                ;;
            # get_config_examples / get_flow_examples / get_flow_schema
            src/keboola_mcp_server/tools/schema.py|\
            src/keboola_mcp_server/tools/examples.py)
                phase_set["MCP-04"]=1
                ;;
            # core config / server changes — test everything
            src/keboola_mcp_server/config.py|\
            src/keboola_mcp_server/server.py|\
            src/keboola_mcp_server/__init__.py)
                phase_set["MCP-01"]=1; phase_set["MCP-01b"]=1; phase_set["MCP-02"]=1
                phase_set["MCP-03"]=1; phase_set["MCP-04"]=1; phase_set["MCP-05"]=1
                phase_set["MCP-06"]=1
                ;;
        esac
    done <<< "$changed_files"

    if [ ${#phase_set[@]} -eq 0 ]; then
        echo "# No MCP tool source files changed — run full suite or skip"
        echo "kaibench run --type 'MCP Tool Validation'"
    else
        local phases
        phases="$(echo "${!phase_set[@]}" | tr ' ' '\n' | sort | tr '\n' ' ')"
        echo "# Relevant phases for changed files: $phases"
        for phase in $(echo "${!phase_set[@]}" | tr ' ' '\n' | sort); do
            echo "KAIBENCH_EVAL_PARALLEL_WORKERS=${PARALLEL_WORKERS} kaibench run --question $phase"
        done
    fi
}

# ─── --diff MODE ─────────────────────────────────────────────────────────────

if [[ "${1:-}" == "--diff" ]]; then
    echo "Branch: $BRANCH"
    echo "Changed MCP files vs main:"
    git diff "main..HEAD" --name-only 2>/dev/null | grep "src/keboola_mcp_server" || echo "  (none)"
    echo ""
    echo "Suggested KaiBench commands (activate KaiBench venv first):"
    detect_relevant_phases "main"
    echo ""
    echo "All env vars needed:"
    echo "  export KAIBENCH_EVAL_KAI_BACKEND_URL=http://localhost:${KAI_PORT}"
    echo "  export KAIBENCH_EVAL_PARALLEL_WORKERS=${PARALLEL_WORKERS}"
    exit 0
fi

# ─── FULL SETUP INSTRUCTIONS ─────────────────────────────────────────────────

echo "========================================"
echo " KaiBench local MCP test setup"
echo " MCP branch : $BRANCH"
echo " MCP dir    : $MCP_DIR"
echo " Kai UI dir : $UI_DIR"
echo " KaiBench   : $KAIBENCH_DIR"
echo " Workers    : $PARALLEL_WORKERS (always 1 to avoid CPU overload)"
echo "========================================"
echo ""

# ─── ONE-TIME SETUP ──────────────────────────────────────────────────────────

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
# Fill in: AUTH_SECRET, AI provider keys (Google Vertex or Azure)
# Set stack to match your KaiBench project, e.g. for GCP EU:
#   KEBOOLA_STORAGE_API_URL=https://connection.europe-west3.gcp.keboola.com
#   KEBOOLA_STACK=com-keboola-gcp-europe-west3
# Add:
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
#   KAIBENCH_EVAL_PARALLEL_WORKERS=${PARALLEL_WORKERS}   ← always set this

EOF

# ─── TERMINAL 1: MCP SERVER ──────────────────────────────────────────────────

echo "=== TERMINAL 1: Start local MCP server ==="
echo ""
echo "# Option A — run from this checkout (already on the right branch):"
cat <<EOF
cd $MCP_DIR
source 3.12.venv/bin/activate
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
echo "# Option C — no clone, no install (uvx):"
echo "uvx --from \"git+https://github.com/keboola/mcp-server@${BRANCH}\" keboola_mcp_server --transport streamable-http"
echo ""

# ─── TERMINAL 2: KAI BACKEND ─────────────────────────────────────────────────

echo "=== TERMINAL 2: Start Kai backend ==="
echo ""
cat <<EOF
cd $UI_DIR
docker compose -f apps/kai-assistant-backend/docker-compose.yml up -d postgres redis
# Run from the app dir (not repo root — root runs all 18 packages):
cd apps/kai-assistant-backend
nohup yarn dev >> /tmp/kai-backend.log 2>&1 &
# Kai backend at http://localhost:${KAI_PORT}
EOF
echo ""

# ─── TERMINAL 3: KAIBENCH ────────────────────────────────────────────────────

echo "=== TERMINAL 3: Run KaiBench ==="
echo ""
echo "# Activate KaiBench venv and export env:"
cat <<EOF
source $KAIBENCH_DIR/.venv/bin/activate
export KAIBENCH_EVAL_KAI_BACKEND_URL=http://localhost:${KAI_PORT}
export KAIBENCH_EVAL_PARALLEL_WORKERS=${PARALLEL_WORKERS}   # keep at 1 — prevents CPU overload

EOF

echo "# Run only the phases relevant to THIS branch's changes:"
detect_relevant_phases "main"
echo ""
cat <<EOF
# Run full MCP Tool Validation suite (all 7 phases):
KAIBENCH_EVAL_PARALLEL_WORKERS=${PARALLEL_WORKERS} kaibench run --type "MCP Tool Validation"

# Run all question types:
KAIBENCH_EVAL_PARALLEL_WORKERS=${PARALLEL_WORKERS} kaibench run

# Results saved to: $KAIBENCH_DIR/results/
EOF
echo ""
echo "Tip: add KAIBENCH_EVAL_PARALLEL_WORKERS=${PARALLEL_WORKERS} to KaiBench .env so you never forget it."

# ─── ADD-NEW-TEST: HOW TO ADD A KAIBENCH TEST FOR A NEW MCP TOOL/PARAM ───────

cat <<'ADDTEST'

=== ADD-NEW-TEST: Adding a KaiBench test for a new MCP tool or parameter ===

If your PR adds a new tool or new parameter to an existing tool (e.g. run_job now
accepts configuration_row_ids), add a test to KaiBench:

1. In KaiBench/kaibench/evaluators/mcp_tool_validation.py:
   - Add to TEST_TOOL_MAP:
       "TEST-39": "run_job",
   - Add to PHASE_TESTS (new phase or extend existing):
       "MCP-07": ["TEST-39"],

2. In KaiBench/data/questions.jsonl, append:
   {
     "id": "MCP-07",
     "question_type": "MCP Tool Validation",
     "question": "MCP Tool Validation: Phase 7 - Job Execution (TEST-39)\n\nTest run_job with configuration_row_ids:\n[TEST-39] run_job | Call run_job for component keboola.ex-db-snowflake with configuration_row_ids=[\"<row_id>\"] and verify the job starts. Report PASS if the tool accepts the parameter without error, FAIL otherwise.",
     "has_answer": true,
     "expected_answer": "TEST-39",
     "regression_testing": true,
     "edge_case": false,
     "additional_context": "",
     "notes": "Tests run_job with configuration_row_ids parameter (added in AI-2889)",
     "source": "",
     "sql": "",
     "evaluator": "mcp_tool",
     "scoring_method": "trace_verification",
     "pass_threshold": 0.8,
     "evaluation_criteria": "",
     "evaluator_config": {}
   }

3. Run: kaibench run --question MCP-07

ADDTEST
