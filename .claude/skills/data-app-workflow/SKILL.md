---
name: data-app-workflow
version: 1.0.0
description: >
  Use when the user says "create me a dashboard", "build a data app showing X",
  "deploy a new data app from my extractor", "I want a JS/TypeScript dashboard for my data",
  "create me a data app", or similar requests. Orchestrates the full lifecycle: gather
  requirements → explore available data → choose framework sub-skill → set up GitHub
  repo → create Keboola data app (git-backed) → deploy.
---

## Overview

This skill is an **orchestrator**. It gathers what the user wants, explores the data, delegates
code generation to a framework-specific sub-skill (Express, Vite, HONO, etc.), sets up the
GitHub repository, then creates and deploys the Keboola data app using the `create_git_data_app`
MCP tool.

```
Step 1: Requirements
Step 2: Data exploration
Step 3: Git repo decision (new vs existing, public vs private)
Step 4: Code generation → framework sub-skill  [new repo path only]
Step 5: GitHub repo setup + push               [new repo path only]
Step 6: create_git_data_app  (MCP tool — handles credential encryption)
Step 7: deploy_data_app
```

---

## Step 1 — Gather requirements

If the user's message is not specific enough, ask in **one combined prompt**:

- What should the dashboard display? (metric names, KPIs, charts, tables, filters)
- Which connector, extractor, or table is the data source? (bucket/table name, or describe it)
- What name should the app have?

If the user already provided all of this in their request, skip to Step 2.

---

## Step 2 — Explore available data

Use MCP tools to understand the data before any code is written:

```
search(item_types=["table", "bucket"], query=<user's connector/table name>)
get_tables(...)      → inspect schema, column names, types
query_data(...)      → SELECT * FROM <table> LIMIT 10 — confirm columns & sample values
```

Summarise what you found to the user (table name, key columns, row count) and confirm
it matches what they want to display before proceeding.

---

## Step 3 — Git repository decision

Ask the user two questions (combine into one prompt):

**3A — New or existing repo?**
- **Existing**: Ask for the repo URL and branch. Skip to Step 6.
- **New**: Proceed to Step 3B.

**3B — For a new repo:**

- GitHub organisation name and desired repo name
- Public or private?
- If **private**: username **and** one of:
  - GitHub Personal Access Token (PAT, starts with `ghp_`)  
  - SSH private key (paste the full PEM block)

Collect all details in one prompt. Store credentials in memory only for this
session — never log or echo them.

---

## Step 4 — Code generation via framework sub-skill (new repo path only)

The skill is **framework-agnostic**. Use the table below to pick a sub-skill, then
invoke it with the Skill tool (or `/sub-skill-name`). Pass the data schema, table
names, and dashboard requirements so the sub-skill generates appropriate code.

| Sub-skill | When to use |
|---|---|
| `express-data-app` | Node.js Express server + JSON API endpoints |
| `vite-data-app` | React/TypeScript SPA (Vite build, served by nginx) |
| `hono-data-app` | Hono (lightweight edge-ready Node.js) |
| _(none matched)_ | Generate code directly based on requirements |

If no sub-skill matches the user's preference, generate the JS/TypeScript code directly.

### Required repo structure for Keboola `python-js` data apps

Reference: https://help.keboola.com/data-apps/python-js/#step-3---create-the-keboola-config-folder

```
repo/
├── keboola-config/
│   ├── setup.sh                          ← install (+ build for static apps)
│   ├── nginx/
│   │   └── sites/
│   │       └── default.conf             ← reverse proxy to 127.0.0.1:<app-port>
│   └── supervisord/
│       └── services/
│           └── app.conf                 ← long-running app process
├── package.json                         ← include "serve" in dependencies for static apps
└── src/
```

**`keboola-config/setup.sh`** — runs once at container start, before the app launches.
Install deps here; also run the build step for static Vite/React apps.
Always `cd /app` first (default working dir is `/home/app`).
Use `set -Eeuo pipefail` (stricter than `set -e`):

```bash
# Node.js app (install only)
#!/bin/bash
set -Eeuo pipefail
cd /app && npm install

# Vite/React static app (install + build)
#!/bin/bash
set -Eeuo pipefail
cd /app && npm install && npm run build
```

**`keboola-config/supervisord/services/app.conf`** — defines the long-running process
that supervisord keeps alive. For a static Vite app served via `serve`:

```ini
[program:app]
command=node /app/node_modules/.bin/serve -s /app/dist -l 5000
directory=/app
autostart=true
autorestart=true
stdout_logfile=/dev/stdout
stdout_logfile_maxbytes=0
stderr_logfile=/dev/stderr
stderr_logfile_maxbytes=0
```

For a Node.js server (`server.js` listening on port 5000):

```ini
[program:app]
command=node /app/server.js
directory=/app
autostart=true
autorestart=true
stdout_logfile=/dev/stdout
stdout_logfile_maxbytes=0
stderr_logfile=/dev/stderr
stderr_logfile_maxbytes=0
```

**`keboola-config/nginx/sites/default.conf`** — nginx runs as non-root and acts as a
**reverse proxy** to the app on localhost. Port must be >= 1024; use **8888**:

```nginx
server {
    listen 8888;
    server_name _;

    location / {
        proxy_pass http://127.0.0.1:5000;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }
}
```

For apps with WebSockets (Dash, live-updating content), add inside the `location /` block:

```nginx
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";
        proxy_read_timeout 86400;
```

---

## Step 5 — GitHub repo setup and code push (new repo path only)

```bash
# 1. Create the remote repo (no --clone; we init locally instead)
gh repo create <org>/<repo-name> --public/--private --description "<description>"

# 2. Write all generated files into a temp directory
mkdir -p /tmp/<repo-name>
# ... write all files (Write tool) ...

# 3. Init, stage, and commit locally
cd /tmp/<repo-name>
git init
git add .
git commit -m "Initial dashboard scaffold"

# 4. Push — use a temporary .netrc for HTTPS PAT auth (never pass token in URL)
printf 'machine github.com login %s password %s\n' "<username>" "<pat>" \
  > /tmp/.ghcreds && chmod 600 /tmp/.ghcreds
GIT_CONFIG_COUNT=1 \
  GIT_CONFIG_KEY_0=credential.helper \
  GIT_CONFIG_VALUE_0="store --file /tmp/.ghcreds" \
  git remote add origin https://github.com/<org>/<repo-name>.git
git push -u origin main
rm -f /tmp/.ghcreds   # discard immediately after push
```

For SSH push, write the key to a temp file (`chmod 600`), add to `ssh-agent`, push, then
remove the file.

### Credential safety rules

- **Never** pass PATs or SSH keys as plain shell arguments (no `echo "ghp_..."` or `git clone https://user:token@...`).
- For HTTPS push with a PAT, use a temporary `.netrc` / `credential.helper store` file as shown above — delete it right after pushing.
- For SSH push, use `ssh-agent` or write the key to a temp file with restricted permissions,
  add to agent, then delete the file after the push.
- After the push is done, **discard** the credential — do not store or log it anywhere.

---

## Step 6 — Create the Keboola data app

Call `create_git_data_app` from the MCP server. This tool handles credential encryption
automatically — pass credentials as **plaintext**:

```
create_git_data_app(
  name=<app name>,
  description=<description>,
  git_repo=<HTTPS or SSH URL>,
  git_branch=<branch, default "main">,
  # For public repos: omit git_username, git_pat, git_ssh_key
  # For HTTPS private repos:
  git_username=<username>,
  git_pat=<plaintext PAT>,
  # For SSH private repos:
  git_ssh_key=<plaintext SSH private key>,
  authentication_type="basic-auth",   # or "no-auth" if user explicitly wants public access
)
```

Save the returned `configuration_id` — it is needed for the deploy step and for
any future updates.

---

## Step 7 — Deploy

```
deploy_data_app(action="deploy", configuration_id=<from step 6>)
```

Report to the user:
- The deployment URL (from `deployment_url` in the response)
- The simpleAuth password if `authentication_type="basic-auth"` was used
  (the user will need this to log in)
- A reminder that the app may take 1-3 minutes to cold-start on first access

---

## Updating an existing app

When the user says "update my data app", "change the dashboard", "point the app to a
different branch", etc.:

1. Find the existing app: `get_data_apps(configuration_ids=[<id>])` or `search(...)`
2. Call `create_git_data_app` with the same `configuration_id` and the changed params
3. Call `deploy_data_app(action="deploy", configuration_id=...)` to apply changes

## Redeploying after a git push

When new code is pushed to the repo (bug fix, UI change, etc.) **without** changing the
Keboola configuration, skip `create_git_data_app` and call `deploy_data_app` directly —
the platform re-clones the repo and rebuilds on every deploy:

```
deploy_data_app(action="deploy", configuration_id=<existing id>)
```

---

## CRITICAL RULES

1. **Always call `deploy_data_app` after `create_git_data_app`** — without it the app
   will not start (or the updated config won't take effect for running apps).

2. **Always call `deploy_data_app` after a `git push`** — pushing code does not
   automatically restart the app. The platform re-clones and rebuilds only on deploy.

3. **Never handle encryption yourself.** The `create_git_data_app` MCP tool encrypts
   all credentials via the project's KMS. Pass plaintext values; the tool does the rest.

4. **Credentials are session-only.** Do not write PATs or SSH keys to disk, memory files,
   or any persistent location. Use them only for the git push in Step 5, then discard.

5. **SSH URL format**: `git@github.com:org/repo.git` — not HTTPS.
   HTTPS URL format: `https://github.com/org/repo` — not SSH.

6. **Explore before generating.** Always run Step 2 (data exploration) before writing
   any code. Generating code without knowing the actual column names leads to bugs.

7. **One prompt for credentials.** Collect username, PAT/SSH key, org, and repo name
   in a single prompt — do not ask for them one at a time.

8. **`setup.sh` is for install/build only** — it lives in `keboola-config/setup.sh`, not
   the repo root. Never start a server there. The long-running process goes in
   `keboola-config/supervisord/services/app.conf`. nginx (port 8888) reverse-proxies to it.
   Always `cd /app` first — npm's default working dir is `/home/app`.

---

## Example conversation flow

```
User: "Create me a dashboard showing monthly revenue and top 10 customers from
       my Snowflake extractor"

Skill Step 1: Requirements already provided — name TBD, ask for it
Skill Step 2: search("snowflake") → find output tables → query_data(LIMIT 10)
              → confirm schema with user
Skill Step 3: "New or existing repo? Public or private?"
              → User: "New, private, org=acme, name=revenue-dashboard, PAT=ghp_..."
Skill Step 4: invoke vite-data-app sub-skill (or generate directly) with schema context
Skill Step 5: gh repo create acme/revenue-dashboard --private
              → write files to /tmp/revenue-dashboard, git init, push via temp .netrc
Skill Step 6: create_git_data_app(name="Revenue Dashboard",
                                   git_repo="https://github.com/acme/revenue-dashboard",
                                   git_username="acme-bot", git_pat="ghp_...")
Skill Step 7: deploy_data_app(action="deploy", configuration_id="cfg-abc123")
              → report URL + password to user
```
