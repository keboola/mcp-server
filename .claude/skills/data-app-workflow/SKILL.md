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

If no sub-skill matches the user's preference, generate the JS/TypeScript code
directly. Aim for the simplest structure that meets the requirements:
- Minimal dependencies (CDN preferred over npm install for libraries)
- A working `setup.sh` and a clear entry point
- nginx or Node.js to serve the app on the Keboola platform port

---

## Step 5 — GitHub repo setup and code push (new repo path only)

```bash
# Create the repo in the user's org
gh repo create <org>/<repo-name> --public/--private --clone --description "<description>"

# cd into the cloned repo, copy generated files
cd <repo-name>
# ... write all generated files ...

# Stage, commit, push
git add .
git commit -m "Initial dashboard scaffold"
git push origin main
```

### Credential safety rules

- **Never** pass PATs or SSH keys as plain shell arguments (no `echo "ghp_..."` or `git clone https://user:token@...`).
- For HTTPS push with a PAT, configure via `gh auth` or a one-time `.netrc` entry before pushing.
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

---

## CRITICAL RULES

1. **Always call `deploy_data_app` after `create_git_data_app`** — without it the app
   will not start (or the updated config won't take effect for running apps).

2. **Never handle encryption yourself.** The `create_git_data_app` MCP tool encrypts
   all credentials via the project's KMS. Pass plaintext values; the tool does the rest.

3. **Credentials are session-only.** Do not write PATs or SSH keys to disk, memory files,
   or any persistent location. Use them only for the git push in Step 5, then discard.

4. **SSH URL format**: `git@github.com:org/repo.git` — not HTTPS.
   HTTPS URL format: `https://github.com/org/repo` — not SSH.

5. **Explore before generating.** Always run Step 2 (data exploration) before writing
   any code. Generating code without knowing the actual column names leads to bugs.

6. **One prompt for credentials.** Collect username, PAT/SSH key, org, and repo name
   in a single prompt — do not ask for them one at a time.

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
Skill Step 5: gh repo create acme/revenue-dashboard --private --clone
              → push generated code
Skill Step 6: create_git_data_app(name="Revenue Dashboard",
                                   git_repo="https://github.com/acme/revenue-dashboard",
                                   git_username="acme-bot", git_pat="ghp_...")
Skill Step 7: deploy_data_app(action="deploy", configuration_id="cfg-abc123")
              → report URL + password to user
```
