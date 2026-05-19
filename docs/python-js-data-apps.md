# Python-JS Data Apps: Dev-Twin Workflow

**Linear**: [AI-3005](https://linear.app/keboola/issue/AI-3005)
**Status**: Implemented in v1.62.0; HTTPS-credential switch landed in v1.63.0.

---

## Overview

Python-JS data apps are backed by a **managed git repository**: source code lives in the repo, not in the Storage configuration. The MCP server exposes a small set of primitives (`modify_python_js_data_app`, `deploy_data_app`, `get_data_apps`) that together support a **two-app project model**:

- A persistent **prod app** that users actually run.
- One or more **dev twins** that share the prod app's git repo and serve as the LLM's iteration sandbox.

Dev twins are surfaced in the Keboola UI under their parent prod app in a **"Drafts"** section, each with its own **"Discard"** button. Cleanup is a user action — there is no platform-side GC and the MCP server does not delete dev twins.

```
┌─────────────────────────── Project ───────────────────────────┐
│                                                                │
│   ┌────────────────┐                       ┌────────────────┐ │
│   │   Prod App     │                       │   Dev Twin     │ │
│   │ (persistent)   │                       │  (Draft)       │ │
│   │                │                       │                │ │
│   │ slug: demo     │   shared managed      │ slug:          │ │
│   │ branch: main   │ ◄─── git repo R ────► │  demo-dev-xyz  │ │
│   │ mode: prod     │                       │ branch:        │ │
│   │                │                       │   feature-x    │ │
│   │                │                       │ mode: dev      │ │
│   └────────────────┘                       └────────────────┘ │
│         │                                            │         │
│         ▼                                            ▼         │
│    Prod URL                                     Preview URL    │
└────────────────────────────────────────────────────────────────┘
                                                       │
                                          (listed under prod app
                                          in UI "Drafts" section;
                                          user clicks "Discard"
                                          to remove)
```

---

## Why a dev twin?

Without a dev twin, the LLM would have to iterate against the prod app directly: every push to `main` would change what end users see, and deployment failures would take the prod URL offline. A dev twin solves this by:

1. **Isolating iteration** — feature branches are deployed against a separate app whose URL only the LLM and the user need.
2. **Sharing the repo** — once the user approves, the LLM merges `feature-x` into `main` locally and pushes; the prod app picks up `main` on its next deploy. No copy step, no drift.
3. **Predictable cleanup** — dev twins appear under the parent prod app in the UI's "Drafts" section; the user discards each one manually via a "Discard" button when they no longer need it. The MCP server never deletes apps on its own.

---

## Tool surface

The dev-twin flow extends two existing tools with new parameters and adds one dedicated tool for HTTPS-credential minting:

| Tool | Change | Purpose |
|---|---|---|
| `modify_python_js_data_app` | New `existing_repo_url` (create-only) | Bind the new app to an existing managed repo instead of provisioning a fresh one. |
| `deploy_data_app` | New `branch` | Deploy a python-js dev twin from a specific branch instead of `main`. Only valid with `mode='dev'`. |
| `create_python_js_data_app_git_credential` | New dedicated tool | Mint a one-time HTTPS token for a python-js app's managed repo. Used both right after create and to recover when the previously-cached token is gone (new Kai sandbox). |

The flow is taught to the LLM exclusively through the tool docstrings; there is no MCP prompt.

### Runtime image version

The runtime image version is currently **hardcoded** in the MCP server (constant `_HARDCODED_PYTHON_JS_IMAGE_VERSION` in `src/keboola_mcp_server/tools/data_apps.py`, value `dev-PAT-1772.4`). The tool does not expose it as an argument. Remove the hardcoded constant and re-introduce the argument (or simply drop the field from the payload) once the platform sets a default image for python-js apps.

### Per-app workspace

Newly created python-js apps carry `runtime.workspace.enabled = true` in their Storage configuration. The platform reads this flag and **auto-provisions a workspace per data app**, then injects its ID into the app's runtime as the `WORKSPACE_ID` environment variable. As a consequence:

- The MCP server does **not** inject `WORKSPACE_ID` as an app secret for python-js apps (it still does for Streamlit, which has no auto-workspace feature). The other runtime secrets — `BRANCH_ID`, `KBC_TOKEN`, `KBC_URL` — are still injected by the MCP server.
- The flag is hardcoded `true` on create; there is no tool argument to opt out.
- This is a **create-only** behaviour. The update path does not backfill `runtime.workspace` on existing apps — apps created before this change continue to operate against whichever workspace was injected at their original create time.

### Authentication: HTTPS tokens

Managed repos authenticate over **HTTPS** with one-time tokens minted by sandboxes-service. The MCP server does not surface SSH at all — no keypair generation, no `GIT_SSH_COMMAND`, no `~/.ssh` plumbing.

The `create_python_js_data_app_git_credential` tool returns a `git_clone_url` of the form `https://kai:<secret>@<host>/<path>.git` ready to pass to `git clone`. The hardcoded username `kai` is set by the constant `_MANAGED_GIT_REPO_USERNAME` in `src/keboola_mcp_server/tools/data_apps.py`; the git-service ignores the username portion and only validates the token.

The platform supports multiple credentials per app, so minting a fresh credential never invalidates earlier ones — important for the recovery flow below.

---

## Create flow (new project bootstrap)

Use when there is no prod app yet. The LLM creates a temporary dev iteration app, iterates with the user, then creates the prod app sharing the same repo.

```
Step 1: modify_python_js_data_app(
            slug='demo-iter-abc',
        )                    ──► { configuration_id: C1, repo_url: R }
                                  (R = bare HTTPS URL, no credentials)
                                        │
                                        ▼
Step 2: create_python_js_data_app_git_credential(
            configuration_id=C1,
        )                    ──► { credential_id, secret, git_clone_url: U1 }
                                  (U1 = https://kai:<secret>@host/path.git)
                                        │
                                        ▼
Step 3: git clone U1; write app.py; git push origin main
                                        │
                                        ▼
Step 4: deploy_data_app(
            action='deploy',
            configuration_id=C1,
            mode='dev',
        )                    ──► preview URL — iterate with user
                                        │
                                        ▼ (user approves)
                                        │
Step 5a: modify_python_js_data_app(
             slug='demo',    (the user-facing slug)
             existing_repo_url=R,
         )                   ──► { configuration_id: C2, repo_url: R }
                                        │
                                        ▼
Step 5b: create_python_js_data_app_git_credential(
             configuration_id=C2,
         )                   ──► { credential_id, secret, git_clone_url: U2 }
                                   (credentials are per-app; mint a fresh
                                    one for the new prod app. U1 still works
                                    against the dev iteration app's repo —
                                    same repo, different credential set.)
                                        │
                                        ▼
Step 6: deploy_data_app(
            action='deploy',
            configuration_id=C2,
        )                    ──► prod URL
                                        │
                                        ▼
Step 7: C1 stays listed under C2 in the UI's "Drafts"
        section; user clicks "Discard" to remove it.
```

Key invariants:

- The dev iteration app and the prod app share the **same repo URL**. Step 5a returns `R` unchanged.
- Step 5b mints a **new** credential because credentials are per-app. The dev iteration app's credential (`U1`) still works on the underlying repo; it just authenticates as a credential issued against `C1`.
- Step 6 uses **no `mode`** — `mode='dev'` is what marks the app as a draft in the UI. Prod must be a plain deploy.

---

## Edit flow (modifying an existing prod app)

Use when the user wants to change an existing prod app. The LLM creates a fresh dev twin bound to the prod app's repo, iterates on a feature branch, then merges into `main` and redeploys prod.

```
Step 1: get_data_apps(
            configuration_ids=[prod_id],
        )                    ──► retrieve repo_url R
                                        │
                                        ▼
Step 2: modify_python_js_data_app(
            slug='demo-dev-xyz',  (unique suffix)
            existing_repo_url=R,
        )                    ──► { configuration_id: C3, repo_url: R }
                                        │
                                        ▼
Step 3: create_python_js_data_app_git_credential(
            configuration_id=C3,
        )                    ──► { credential_id, secret, git_clone_url: U3 }
                                        │
                                        ▼
Step 4: git clone U3
        git checkout -b feature-x
        write changes
        git push origin feature-x
                                        │
                                        ▼
Step 5: deploy_data_app(
            action='deploy',
            configuration_id=C3,
            mode='dev',
            branch='feature-x',
        )                    ──► preview URL serving feature-x
                                        │
                                        ▼ (user approves)
                                        │
Step 6: git checkout main
        git merge feature-x
        git push origin main
                                        │
                                        ▼
Step 7: deploy_data_app(
            action='deploy',
            configuration_id=prod_id,
        )                    ──► prod URL now serves merged main
                                        │
                                        ▼
Step 8: C3 stays listed under prod_id in the UI's "Drafts"
        section; user clicks "Discard" to remove it.
```

Key invariants:

- The prod app's `configuration_id` (`prod_id`) is **never** modified in this flow — only its underlying git `main` is updated and the app is redeployed.
- The `branch` parameter is **only meaningful with `mode='dev'`**; the tool rejects `branch` without `mode='dev'`. Prod redeploys (Step 7) use neither.
- The slug for the dev twin needs a short unique suffix (e.g. `-dev-xyz`) — both apps live in the same project and the slug is a DNS label.

---

## Update flow (deployment metadata only)

Distinct from create / edit: when only `auto_suspend_after_seconds`, `name`, or `description` need to change on an existing app, call `modify_python_js_data_app(configuration_id=<id>, ...)`. The update path:

- Updates the Storage configuration in place.
- **Rejects** `slug` (immutable subdomain) and `existing_repo_url` (repo binding is fixed at creation).
- After updating, the caller MUST call `deploy_data_app(...)` to restart the app so changes take effect.

The update flow does NOT involve git — source code changes go through the edit flow. To rotate or add a token, use `create_python_js_data_app_git_credential` (the data-science API accepts multiple credentials per app).

---

## Recovering when the cached HTTPS token is lost

The Kai sandbox the LLM iterates in is ephemeral: when a user returns later to continue an old draft, a fresh sandbox spins up and the conversation is restored, but the `git_clone_url` returned in the previous session is gone with the wiped filesystem. The LLM now holds a `configuration_id` for an existing python-js app but cannot `git clone`/`pull`/`push` against its managed repo.

The data-science API accepts **multiple credentials per app**, so registering a fresh one never invalidates credentials already held by other clients (e.g. a teammate iterating against the same prod app's repo).

One-call recovery:

```
1. create_python_js_data_app_git_credential(
       configuration_id=<existing app's cfg id>,
   )                   ──► { credential_id, secret, git_clone_url }
                            git access restored — clone with git_clone_url
```

This works regardless of whether the app is a dev twin or a prod app — both expose the same per-app credentials endpoint.

---

## Parameter reference

### `modify_python_js_data_app(existing_repo_url=...)`

- **Type**: `Optional[str]`
- **When valid**: create only (raises `ValueError` if set on update).
- **Semantics**: when set, the new app is bound to the existing managed repo URL (the bare HTTPS URL without credentials). No fresh repo is provisioned. Credential creation is a separate per-app step — call `create_python_js_data_app_git_credential` on the new app after create.
- **Returned**: the same URL is returned as `repo_url` unchanged.

### `deploy_data_app(branch=...)`

- **Type**: `Optional[str]`
- **When valid**: only with `mode='dev'`. Raises `ValueError` otherwise.
- **Semantics**: for python-js apps, deploys from this git branch instead of `main`. Silently ignored for Streamlit apps (which have no managed git repo).
- **Without `branch`** (and `mode='dev'`): the dev twin deploys `main`.

### `create_python_js_data_app_git_credential(configuration_id=...)`

- **`configuration_id`** (`str`, required): Storage configuration ID of an existing python-js data app. The tool resolves it to the underlying `data_app_id` and rejects Streamlit apps with a clear error.
- **Returns**:
  - `credential_id` — UUID of the credential row on sandboxes-service. Useful only for diagnostics; the MCP surface does not expose list/delete endpoints.
  - `secret` — the one-time HTTPS token. **Cannot be retrieved again** by any subsequent read — store it if you need to reuse it outside of `git_clone_url`.
  - `git_clone_url` — ready-to-use authenticated URL of the form `https://kai:<secret>@<host>/<path>.git`. Pass directly to `git clone`. The username portion is the hardcoded constant `kai` (`_MANAGED_GIT_REPO_USERNAME`); the git-service ignores it and only validates the token portion.
  - `permissions` — always `readWrite` (the tool does not expose a permissions knob).

### `modify_python_js_data_app(authentication_type=...)`

- **Type**: `'no-auth' | 'basic-auth' | 'default'` (default: `'default'`).
- **Semantics on create**: `'default'` and `'basic-auth'` both apply HTTP basic authentication (safe-by-default for new apps); `'no-auth'` exposes the app publicly.
- **Semantics on update**: `'default'` leaves the existing `authorization` block untouched (so OIDC and other advanced setups configured outside the MCP survive); `'basic-auth'` and `'no-auth'` overwrite it.
- **Wire shape**: identical to Streamlit — `authorization.app_proxy.{auth_providers, auth_rules}`. The DSAPI's python-js endpoint accepts this block alongside `useManagedGitRepo: true`.

---

## What's intentionally NOT a separate tool

Several variants of this flow could have been packaged as dedicated tools but were left out:

- **`promote_to_prod(dev_configuration_id)`** — composing `modify_python_js_data_app(existing_repo_url=...)` + `create_python_js_data_app_git_credential(...)` + `deploy_data_app(...)` keeps the surface small and reuses primitives. Revisit if real usage shows the multi-call orchestration is error-prone.
- **`create_dev_twin_data_app(parent_configuration_id)`** — same reasoning. The dev twin reuses the same hardcoded runtime image as every other python-js app, so no parent inspection is needed.
- **MCP-side deletion of dev twins** — the UI lists each dev twin under its parent prod app in the "Drafts" section with a "Discard" button. Cleanup is an explicit user action; there's no platform-side GC and no need for an MCP tool to delete twins.
- **Credential listing/deletion** — out of scope; per-app credentials are append-only from the MCP surface (sandboxes-service supports listing/getting/deleting via `GET /apps/{id}/git-repo/credentials`, `GET .../credentials/{credentialId}`, and `DELETE .../credentials/{credentialId}`, but the MCP server does not expose them). The platform UI is the rotation/cleanup affordance.
- **SSH credential support** — the swagger lets sandboxes-service issue `ssh_key` credentials as well, but the MCP surface mints only `http_token` credentials. Users who need SSH access provision it through the platform UI directly.

---

## Wire-level details (data-science API)

The tool surface maps to the data-science API as follows. Confirm field names with the platform team if they ever change — adjusting `DataScienceClient` is a one-line tweak per field.

| Tool parameter | API endpoint | Field on the wire |
|---|---|---|
| `existing_repo_url` | `POST /apps` | `existingRepoUrl` (alongside `useManagedGitRepo: true`) |
| `branch` | `PATCH /apps/{id}` | `branch` (alongside `desiredState: 'running'`, `mode: 'dev'`) |
| `create_python_js_data_app_git_credential` | `POST /apps/{id}/git-repo/credentials` | Request: `{type: 'http_token', permissions: 'readWrite'}`. Response: `{id, type, permissions, secret, ...}`. The one-time `secret` is what we embed (with `kai` as username) into `git_clone_url`. |
| (clone URL lookup) | `GET /apps/{id}/git-repo` | Response: `{sshUrl, httpsUrl, isManagedGitRepo}`. The MCP server uses `httpsUrl` only. |
| auto-workspace flag (hardcoded `true` on create) | `POST /apps` | `configuration.runtime.workspace.enabled` |

---

## End-to-end verification checklist

Against `data-science.canary-orion.keboola.dev`:

**Create flow**

- [ ] `modify_python_js_data_app(slug='demo-iter-abc')` returns `(C1, R)`. `R` starts with `https://` (no `git@`).
- [ ] `create_python_js_data_app_git_credential(configuration_id=C1)` returns `(credential_id, secret, git_clone_url)`. `git_clone_url` matches `https://kai:<secret>@<host>/<path>.git`.
- [ ] `git clone <git_clone_url>` works with no local key plumbing; push minimal `app.py` to `main`.
- [ ] `deploy_data_app(configuration_id=C1, mode='dev')` produces a working preview URL.
- [ ] `modify_python_js_data_app(slug='demo', existing_repo_url=R)` returns `(C2, R)`. (Same `R`.)
- [ ] `create_python_js_data_app_git_credential(configuration_id=C2)` returns a new credential. The credential from `C1` still works against the shared repo.
- [ ] `deploy_data_app(configuration_id=C2)` produces a working prod URL serving the same code.
- [ ] `C1` is listed under `C2` in the UI's "Drafts" section; clicking "Discard" removes it.

**Edit flow**

- [ ] `get_data_apps(configuration_ids=[C2])` returns `repo_url == R`.
- [ ] `modify_python_js_data_app(slug='demo-dev-xyz', existing_repo_url=R)` returns `(C3, R)`.
- [ ] `create_python_js_data_app_git_credential(configuration_id=C3)` returns a fresh credential.
- [ ] Push `feature-x` to `R`.
- [ ] `deploy_data_app(configuration_id=C3, mode='dev', branch='feature-x')` previews the branch.
- [ ] Merge `feature-x` into `main` and push.
- [ ] `deploy_data_app(configuration_id=C2)` now serves the merged code.
- [ ] `C3` is listed under `C2` in the UI's "Drafts" section; clicking "Discard" removes it.

**Lost-token recovery flow**

- [ ] Pretend the `git_clone_url` from a prior session is gone.
- [ ] `create_python_js_data_app_git_credential(configuration_id=C2)` returns a fresh `git_clone_url`.
- [ ] Cloning `R` with the new URL works.
- [ ] Existing clones from before the recovery remain usable (server accepts multiple credentials; old one is not revoked).
- [ ] `create_python_js_data_app_git_credential(configuration_id=<streamlit cfg>)` raises a clear "only python-js apps" error.
