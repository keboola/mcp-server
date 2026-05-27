# Python-JS Data Apps: Prod + External-Git Dev Twin

**Linear**: [AI-3005](https://linear.app/keboola/issue/AI-3005)
**Status**: MVP shipped in v1.63.0. Replaces the broken shared-managed-repo design from v1.62.0.
**Long-term tracker (platform-side drafts)**: [AI-3240](https://linear.app/keboola/issue/AI-3240)

---

## Overview

Python-JS data apps are backed by a **git repository**: source code lives in the repo, not in the Storage configuration. The MCP server exposes a small set of primitives (`modify_python_js_data_app`, `deploy_data_app`, `create_python_js_data_app_git_credential`, `get_data_apps`) that together support a **two-app project model**:

- A persistent **prod app** that users actually run. The prod app **owns the only managed git repo** in the project.
- One or more **dev twins** that are *external-git* apps configured to clone the prod app's repo on every deploy. Dev twins serve as the LLM's iteration sandbox.

Dev twins are surfaced in the Keboola UI under their parent prod app in a **"Drafts"** section, each with its own **"Discard"** button. Cleanup is a user action — there is no platform-side GC and the MCP server does not delete dev twins.

```
┌─────────────────────────── Project ───────────────────────────┐
│                                                                │
│   ┌────────────────┐                       ┌────────────────┐ │
│   │   Prod App     │                       │   Dev Twin     │ │
│   │ (persistent)   │                       │  (Draft)       │ │
│   │                │                       │                │ │
│   │ slug: demo     │                       │ slug:          │ │
│   │ branch: main   │                       │  demo-dev-xyz  │ │
│   │ mode: prod     │                       │ mode: dev      │ │
│   │                │  external-git config  │ parameters.    │ │
│   │ owns managed   │ ◄──── points at ──────┤   dataApp.git: │ │
│   │   git repo R   │      repo R, branch   │  { repo: R,    │ │
│   │                │      iter-feat, with  │    #password,  │ │
│   │                │      prod-issued      │    branch:     │ │
│   │                │      token            │     iter-feat }│ │
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

## Why prod owns the repo

The original design (v1.62.0) was the opposite: the dev iteration app was created first with its own managed repo, and the prod app was created via `existing_repo_url=<dev's repo>` so both apps shared one managed repo. End-to-end testing on canary-orion proved that `existingRepoUrl` is silently dropped by `POST /apps` — the platform always provisions a fresh managed repo per app and provides no mechanism to share or link managed repos across apps.

The MVP workaround (this design): **flip the ownership** so the prod app is the canonical managed-repo owner, and the dev twin is an *external-git* app whose `parameters.dataApp.git` block tells the data-app runtime to clone the prod repo on deploy. The prod-side credential is minted by the MCP server when creating the dev twin (the platform accepts unlimited per-app credentials, so this is non-destructive).

This unlocks the same dev/prod iteration loop without requiring shared-repo support from the platform. The long-term fix — a first-class "drafts" mechanism — is tracked in [AI-3240](https://linear.app/keboola/issue/AI-3240).

---

## Tool surface

| Tool | Change in v1.63.0 | Purpose |
|---|---|---|
| `modify_python_js_data_app` | Replaced `existing_repo_url` (create-only) with `parent_configuration_id` (create-only) and `branch` (create-only, dev-twin only) | Create a dev twin bound to the parent prod app's managed repo. |
| `deploy_data_app` | No change | Same as before: `mode='dev'` plus optional `branch` to override the branch the dev twin deploys from. |
| `create_python_js_data_app_git_credential` | No change | Mint a one-time HTTPS token on a prod app's managed repo. Used for the lost-token recovery flow; **not** needed for the standard dev-twin create flow (the token is minted and embedded into the returned `git_clone_url` automatically). |

The flow is taught to the LLM exclusively through the tool docstrings; there is no MCP prompt.

### Runtime image version

The runtime image version is currently **hardcoded** in the MCP server (constant `_HARDCODED_PYTHON_JS_IMAGE_VERSION` in `src/keboola_mcp_server/tools/data_apps.py` — see the source for the current value). The tool does not expose it as an argument. Remove the hardcoded constant and re-introduce the argument (or simply drop the field from the payload) once the platform sets a default image for python-js apps.

### Per-app workspace

Newly created python-js apps carry `runtime.workspace.enabled = true` in their Storage configuration. The platform reads this flag and **auto-provisions a workspace per data app**, then injects its ID into the app's runtime as the `WORKSPACE_ID` environment variable. As a consequence:

- The MCP server does **not** write `WORKSPACE_ID` (or `BRANCH_ID`, `KBC_TOKEN`, `KBC_URL`) into the stored python-js configuration. The platform side is responsible for surfacing those at runtime. The one exception is **legacy projects without the `data-apps-storage-workspace` feature**: there the MCP server falls back to writing `WORKSPACE_ID` into `parameters.dataApp.secrets` so the app still has a workspace ID to read. Streamlit apps still receive the full secret set (`WORKSPACE_ID`, `BRANCH_ID`, `KBC_TOKEN`, `KBC_URL`) from the MCP server because they have no auto-workspace feature.
- The flag is hardcoded `true` on create; there is no tool argument to opt out.
- This is a **create-only** behaviour. The update path does not backfill `runtime.workspace` on existing apps — apps created before this change continue to operate against whichever workspace was injected at their original create time.

### Authentication: HTTPS tokens

Managed repos authenticate over **HTTPS** with one-time tokens minted by sandboxes-service. The MCP server does not surface SSH at all — no keypair generation, no `GIT_SSH_COMMAND`, no `~/.ssh` plumbing.

For the standard dev-twin create flow, `modify_python_js_data_app` mints the prod-side token internally and returns a ready-to-use `git_clone_url` of the form `https://kai:<secret>@<host>/<path>.git`. For the lost-token recovery flow, `create_python_js_data_app_git_credential` does the same on demand. The hardcoded username `kai` is set by the constant `_MANAGED_GIT_REPO_USERNAME` in `src/keboola_mcp_server/tools/data_apps.py`; the git-service ignores the username portion and only validates the token.

The platform supports multiple credentials per app, so minting a fresh credential never invalidates earlier ones — important for the recovery flow below and for parallel iteration.

---

## Create flow (new project bootstrap)

Use when there is no prod app yet. The LLM creates the prod app first (which owns the managed repo), then creates a dev twin bound to that repo, iterates with the user on an iteration branch, and finally merges into `main` and redeploys prod.

```
Step 1: modify_python_js_data_app(
            slug='demo',
        )                    ──► { configuration_id: PROD, repo_url: R }
                                  (R = bare HTTPS URL of PROD's managed repo)
                                        │
                                        ▼
Step 2: modify_python_js_data_app(
            slug='demo-dev-abc',
            parent_configuration_id=PROD,
        )                    ──► { configuration_id: DEV, repo_url: R,
                                   git_clone_url: U, branch: B }
                                  (U = https://kai:<secret>@host/path.git,
                                   B = 'iter-<6-hex>' or caller-supplied)
                                        │
                                        ▼
Step 3: git clone U; git checkout B; write app.py; git push origin B
                                        │
                                        ▼
Step 4: deploy_data_app(
            action='deploy',
            configuration_id=DEV,
            mode='dev',
        )                    ──► preview URL serving B — iterate with user
                                        │
                                        ▼ (user approves)
                                        │
Step 5: git checkout main; git merge B; git push origin main
                                        │
                                        ▼
Step 6: deploy_data_app(
            action='deploy',
            configuration_id=PROD,
        )                    ──► prod URL now serves merged main
                                        │
                                        ▼
Step 7: DEV stays listed under PROD in the UI's "Drafts"
        section; user clicks "Discard" to remove it.
```

Key invariants:

- Only **PROD** owns a managed repo (`use_managed_git_repo=True`). The dev twin is created with `use_managed_git_repo=False` and a populated `parameters.dataApp.git` block.
- The dev twin's `repo_url` equals PROD's `repo_url` — they are the same physical repo on the git-service side.
- The token embedded in `git_clone_url` was minted against **PROD**, not the dev twin. Existing prod-side tokens are not invalidated.
- Step 4 uses `mode='dev'` to mark the app as a draft in the UI and trigger the data-app runtime to clone the configured external branch. Step 6 (prod redeploy) uses no `mode` / no `branch` — prod always deploys `main` from its own managed repo.

---

## Edit flow (modifying an existing prod app)

Use when the user wants to change an existing prod app. Same shape as steps 2–6 of the create flow — the agent already has the prod's `configuration_id`, so it goes straight to dev-twin creation.

```
Step 1: modify_python_js_data_app(
            slug='demo-dev-xyz',
            parent_configuration_id=PROD,
            branch='feature-x',   (optional; auto-generated if omitted)
        )                    ──► { configuration_id: DEV, repo_url: R,
                                   git_clone_url: U, branch: B }
                                        │
                                        ▼
Step 2: git clone U; git checkout B; write changes; git push origin B
                                        │
                                        ▼
Step 3: deploy_data_app(
            action='deploy',
            configuration_id=DEV,
            mode='dev',
        )                    ──► preview URL serving B
                                        │
                                        ▼ (user approves)
                                        │
Step 4: git checkout main; git merge B; git push origin main
                                        │
                                        ▼
Step 5: deploy_data_app(
            action='deploy',
            configuration_id=PROD,
        )                    ──► prod URL now serves merged main
                                        │
                                        ▼
Step 6: DEV stays listed under PROD in the UI's "Drafts"
        section; user clicks "Discard" to remove it.
```

Key invariants:

- The prod app's `configuration_id` is **never** modified in this flow — only its underlying git `main` is updated and the app is redeployed.
- The dev twin's pinned branch is set in its `parameters.dataApp.git.branch` at create time. Passing `branch` to `deploy_data_app(mode='dev')` lets the agent override that pin for an ad-hoc preview, but normally it is not needed because the dev-twin config already points at the correct branch.
- The slug for the dev twin needs a short unique suffix (e.g. `-dev-xyz`) — both apps live in the same project and the slug is a DNS label.

---

## Update flow (deployment metadata only)

Distinct from create / edit: when only `auto_suspend_after_seconds`, `name`, `description`, `authentication_type`, or `storage` need to change on an existing app, call `modify_python_js_data_app(configuration_id=<id>, ...)`. The update path:

- Updates the Storage configuration in place.
- **Rejects** `slug` (immutable subdomain), `parent_configuration_id` (repo binding is fixed at creation), and `branch` (only meaningful for dev-twin creates).
- After updating, the caller MUST call `deploy_data_app(...)` to restart the app so changes take effect.

The update flow does NOT involve git — source code changes go through the edit flow. To rotate or add a token, use `create_python_js_data_app_git_credential` on the prod app.

---

## Recovering when the cached HTTPS token is lost

The Kai sandbox the LLM iterates in is ephemeral: when a user returns later to continue an old draft, a fresh sandbox spins up and the conversation is restored, but the `git_clone_url` returned in the previous session is gone with the wiped filesystem. The LLM now holds a `configuration_id` for an existing prod app (and maybe a dev twin) but cannot `git clone`/`pull`/`push` against the managed repo.

The data-science API accepts **multiple credentials per app**, so registering a fresh one never invalidates credentials already held by other clients (e.g. a teammate iterating against the same prod app's repo).

One-call recovery:

```
1. create_python_js_data_app_git_credential(
       configuration_id=<existing PROD app's cfg id>,
   )                   ──► { credential_id, secret, git_clone_url }
                            git access restored — clone with git_clone_url
```

Always call against the **prod** app's configuration ID — the dev twin has no managed repo of its own. (Calling against a dev twin's configuration ID returns a clear "only python-js managed-repo apps have a credentials endpoint" error.)

---

## Parameter reference

### `modify_python_js_data_app(parent_configuration_id=...)`

- **Type**: `Optional[str]`
- **When valid**: create only (raises `ValueError` if set on update).
- **Semantics**: when set, the new app is created as a dev twin: `use_managed_git_repo=False`, and its `parameters.dataApp.git` block is populated with the parent's managed repo URL, a freshly minted prod-side HTTPS token (encrypted via the Keboola encryption service), and the iteration branch. The parent app must be a python-js app with a managed repo — Streamlit and dev-twin parents are rejected.
- **Returned**: the parent's `repo_url` is returned as `repo_url` unchanged; a `git_clone_url` with the embedded prod-issued token is returned alongside.

### `modify_python_js_data_app(branch=...)`

- **Type**: `Optional[str]`
- **When valid**: dev-twin create only (must be paired with `parent_configuration_id`). Rejected on prod create and on update.
- **Semantics**: pins the dev twin to this iteration branch (`parameters.dataApp.git.branch`). When unset, defaults to `iter-<6-hex>`. Must not be `main` (reserved for the prod app); must be non-empty and contain no whitespace.

### `deploy_data_app(branch=...)`

- **Type**: `Optional[str]`
- **When valid**: only with `mode='dev'`. Raises `ValueError` otherwise.
- **Semantics**: for python-js apps, overrides the branch the dev twin deploys from for this single deploy. Without `branch`, the dev twin deploys whatever branch is pinned in its `parameters.dataApp.git.branch`. Silently ignored for Streamlit apps (which have no managed git repo).

### `create_python_js_data_app_git_credential(configuration_id=...)`

- **`configuration_id`** (`str`, required): Storage configuration ID of an existing python-js **prod** app (i.e. one with a managed repo). The tool resolves it to the underlying `data_app_id` and rejects Streamlit apps with a clear error.
- **Returns**:
  - `credential_id` — UUID of the credential row on sandboxes-service. Useful only for diagnostics; the MCP surface does not expose list/delete endpoints.
  - `secret` — the one-time HTTPS token. **Cannot be retrieved again** by any subsequent read — store it if you need to reuse it outside of `git_clone_url`.
  - `git_clone_url` — ready-to-use authenticated URL of the form `https://kai:<secret>@<host>/<path>.git`. Pass directly to `git clone`.
  - `permissions` — always `readWrite` (the tool does not expose a permissions knob).

### `modify_python_js_data_app(authentication_type=...)`

- **Type**: `'no-auth' | 'basic-auth' | 'default'` (default: `'default'`).
- **Semantics on create**: `'default'` and `'basic-auth'` both apply HTTP basic authentication (safe-by-default for new apps); `'no-auth'` exposes the app publicly.
- **Semantics on update**: `'default'` leaves the existing `authorization` block untouched (so OIDC and other advanced setups configured outside the MCP survive); `'basic-auth'` and `'no-auth'` overwrite it.
- **Wire shape**: identical to Streamlit — `authorization.app_proxy.{auth_providers, auth_rules}`. The DSAPI's python-js endpoint accepts this block alongside `useManagedGitRepo: true`.

---

## What's intentionally NOT a separate tool

Several variants of this flow could have been packaged as dedicated tools but were left out:

- **`create_dev_twin_data_app(parent_configuration_id)`** — `modify_python_js_data_app` with the new `parent_configuration_id` parameter is already exactly that; adding a separate tool name would just duplicate the surface.
- **MCP-side deletion of dev twins** — the UI lists each dev twin under its parent prod app in the "Drafts" section with a "Discard" button. Cleanup is an explicit user action; there's no platform-side GC and no need for an MCP tool to delete twins.
- **Credential listing/deletion** — out of scope; per-app credentials are append-only from the MCP surface (sandboxes-service supports listing/getting/deleting via `GET /apps/{id}/git-repo/credentials`, `GET .../credentials/{credentialId}`, and `DELETE .../credentials/{credentialId}`, but the MCP server does not expose them). The platform UI is the rotation/cleanup affordance.
- **SSH credential support** — the swagger lets sandboxes-service issue `ssh_key` credentials as well, but the MCP surface mints only `http_token` credentials. Users who need SSH access provision it through the platform UI directly.

---

## Wire-level details (data-science API)

The tool surface maps to the data-science API as follows. Confirm field names with the platform team if they ever change — adjusting `DataScienceClient` is a one-line tweak per field.

| Tool parameter | API endpoint | Field on the wire |
|---|---|---|
| Prod create (default) | `POST /apps` | `useManagedGitRepo: true`. No `parameters.dataApp.git` block. |
| `parent_configuration_id` (dev twin) | `POST /apps` | `useManagedGitRepo` omitted/false. `configuration.parameters.dataApp.git = {repository, username, '#password' (encrypted via EncryptionClient), branch}`. |
| `deploy_data_app(branch=...)` | `PATCH /apps/{id}` | `branch` (alongside `desiredState: 'running'`, `mode: 'dev'`) |
| `create_python_js_data_app_git_credential` | `POST /apps/{id}/git-repo/credentials` | Request: `{type: 'http_token', permissions: 'readWrite'}`. Response: `{id, type, permissions, secret, ...}`. The one-time `secret` is what we embed (with `kai` as username) into `git_clone_url`. |
| (clone URL lookup) | `GET /apps/{id}/git-repo` | Response: `{sshUrl, httpsUrl, isManagedGitRepo}`. The MCP server uses `httpsUrl` only. |
| auto-workspace flag (hardcoded `true` on create) | `POST /apps` | `configuration.runtime.workspace.enabled` |

---

## End-to-end verification checklist

Against `data-science.canary-orion.keboola.dev`:

**Create flow**

- [ ] `modify_python_js_data_app(slug='demo')` returns `(PROD, R)`. `R` starts with `https://` (no `git@`).
- [ ] `modify_python_js_data_app(slug='demo-dev-abc', parent_configuration_id=PROD)` returns `(DEV, R, git_clone_url, branch)`. `git_clone_url` matches `https://kai:<secret>@<host>/<path>.git`; `branch` matches `iter-[0-9a-f]{6}`.
- [ ] Inspect `DEV`'s Storage config in the UI:
  - [ ] `parameters.dataApp.git.repository == R`.
  - [ ] `parameters.dataApp.git.#password` is encrypted ciphertext (`KBC::ConfigSecureGKMS::...`).
  - [ ] `parameters.dataApp.git.branch == <returned branch>`.
- [ ] `git clone <git_clone_url>` works with no local key plumbing.
- [ ] Push a minimal `app.py` on the returned branch.
- [ ] `deploy_data_app(configuration_id=DEV, mode='dev')` produces a working preview URL serving the branch.
- [ ] Locally `git checkout main && git merge <branch> && git push`.
- [ ] `deploy_data_app(configuration_id=PROD)` produces a working prod URL serving merged `main`.
- [ ] `DEV` is listed under `PROD` in the UI's "Drafts" section; clicking "Discard" removes it.

**Edit flow**

- [ ] Given an existing `PROD`, `modify_python_js_data_app(slug='demo-dev-xyz', parent_configuration_id=PROD, branch='feature-x')` returns `(DEV2, R, U, 'feature-x')`.
- [ ] Push `feature-x` to `R` via the embedded credential.
- [ ] `deploy_data_app(configuration_id=DEV2, mode='dev')` previews the branch.
- [ ] Merge `feature-x` into `main` and push.
- [ ] `deploy_data_app(configuration_id=PROD)` now serves the merged code.
- [ ] `DEV2` is listed under `PROD` in the UI's "Drafts" section; clicking "Discard" removes it.

**Lost-token recovery flow**

- [ ] Pretend the `git_clone_url` from a prior session is gone.
- [ ] `create_python_js_data_app_git_credential(configuration_id=PROD)` returns a fresh `git_clone_url`.
- [ ] Cloning `R` with the new URL works.
- [ ] Existing clones from before the recovery remain usable (server accepts multiple credentials; old ones are not revoked).
- [ ] `create_python_js_data_app_git_credential(configuration_id=<streamlit cfg>)` raises a clear "only python-js apps" error.

---

## v1.62.0 → v1.63.0 migration

`existing_repo_url` (introduced in v1.62.0) has been **removed**. It was never honored by the platform — `POST /apps` silently dropped the field and provisioned a fresh managed repo for every app, so any caller relying on it was hitting silent failures. Replace it with `parent_configuration_id`:

```diff
- modify_python_js_data_app(slug='dev', existing_repo_url=R)     # broken, ignored
+ modify_python_js_data_app(slug='dev', parent_configuration_id=PROD)
```

The ownership flips at the same time:

| | v1.62.0 (broken) | v1.63.0 (MVP) |
|---|---|---|
| Created first | Dev iteration app (managed repo) | **Prod app** (managed repo) |
| Created second | Prod (`existing_repo_url=...` — silently ignored) | **Dev twin** (`parent_configuration_id=PROD`, external-git) |
| Repo ownership | Dev (intended); both apps in practice | **Prod** |
| Tool docs to read | `docs/python-js-data-apps.md` v1.62 | this file |

For background on why the platform behaves this way and the long-term fix, see [AI-3240](https://linear.app/keboola/issue/AI-3240).
