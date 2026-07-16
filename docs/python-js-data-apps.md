# Python-JS Data Apps: Prod + Drafts

**Linear**: [AI-3286](https://linear.app/keboola/issue/AI-3286)
(supersedes the MVP shipped in v1.63.0 under [AI-3005](https://linear.app/keboola/issue/AI-3005))
**Status**: shipped in v1.64.0.

---

## Overview

Python-JS data apps are backed by a **git repository**: source code lives in the repo, not in the Storage configuration. The MCP server exposes a small set of primitives — `modify_python_js_data_app`, `deploy_data_app`, `create_python_js_data_app_git_credential`, `get_data_apps`, `delete_python_js_data_app_draft` — that together support a **two-app project model**:

- A persistent **prod app** that users actually run. The prod app **owns the only managed git repo** in the project.
- Zero or more **drafts** parented to that prod app. A draft is a Storage configuration with `parameters.dataApp.isDraft=true` and `parameters.dataApp.parentConfigurationId=<prod cfg id>`; it's an *external-git* app that clones the parent prod's repo at a pinned branch on every deploy. Drafts serve as the LLM's iteration sandbox.

Drafts are surfaced in the Keboola UI under their parent prod app. They are also discoverable via `get_data_apps`: detail responses for a python-js **prod** app carry a `drafts: [...]` array. Cleanup is explicit — the agent calls `delete_python_js_data_app_draft` once the draft's branch has been promoted to `main`.

**MCP never invokes git.** All git operations (clone, branch, commit, push, merge, branch-delete) are owned by the agent. MCP owns: credentials minting, configs, deploys, draft lifecycle (create + delete).

```
┌─────────────────────────── Project ───────────────────────────┐
│                                                                │
│   ┌────────────────┐                       ┌────────────────┐ │
│   │   Prod App     │                       │     Draft      │ │
│   │ (persistent)   │                       │                │ │
│   │                │                       │ slug:          │ │
│   │ slug: demo     │                       │  demo-draft    │ │
│   │ branch: main   │  external-git config  │ isDraft: true  │ │
│   │ mode: prod     │ ◄──── points at ──────┤ parentConfig:  │ │
│   │                │                       │  PROD          │ │
│   │ owns managed   │                       │ parameters.    │ │
│   │   git repo R   │                       │  dataApp.git:  │ │
│   │                │                       │  { repo: R,    │ │
│   │                │                       │    #password,  │ │
│   │                │                       │    branch:     │ │
│   │                │                       │     init }     │ │
│   └────────────────┘                       └────────────────┘ │
│         │                                            │         │
│         ▼                                            ▼         │
│    Prod URL                              Preview URL (dev mode)│
└────────────────────────────────────────────────────────────────┘
                         ▲
                         │
                         └── get_data_apps([PROD]) returns
                             prod detail + drafts: [DRAFT]
```

---

## Why prod owns the repo

The data-science platform does not yet support sharing a managed repo across apps (`existingRepoUrl` on `POST /apps` is silently dropped; every app gets a fresh managed repo). Until the platform gains native draft support, we **flip the ownership**: the prod app is the canonical managed-repo owner, and each draft is an *external-git* app whose `parameters.dataApp.git` block tells the data-app runtime to clone the prod repo on deploy. The prod-side credential is minted by the MCP server when creating the draft (the platform accepts unlimited per-app credentials, so this is non-destructive).

---

## Tool surface

| Tool | Change in v1.64.0 | Purpose |
|---|---|---|
| `modify_python_js_data_app` | Default draft branch is now `'init'` (was `iter-<6-hex>`). Drafts persist `parameters.dataApp.parentConfigurationId` so they can be discovered cheaply. Docstring rewritten to drop "dev twin". | Create/update a prod app; create a draft bound to the parent prod app's managed repo. |
| `deploy_data_app` | No behaviour change. Docstring reframes `mode='dev'` as "deploys the draft as a **dev version of the data app**" (hot reload + auto-auth for iframe preview). | Deploy/redeploy or stop. `mode='dev'` only meaningful on drafts. |
| `create_python_js_data_app_git_credential` | No behaviour change. Docstring tightens the prod-only contract: drafts have no managed repo, always mint against prod. | Mint a one-time HTTPS token on a python-js prod app's managed repo. |
| `get_data_apps` | Detail responses for python-js **prod** apps now include a `drafts: [...]` array of `DataAppSummary` entries — every draft (`isDraft=true`, `parentConfigurationId == <prod-cfg>`) parented to that prod, fetched with one extra `configuration_list` round-trip. Empty for drafts themselves and for Streamlit apps. | List or detail-fetch data apps; discover drafts. |
| **NEW** `delete_python_js_data_app_draft` | New tool. Deletes a draft's data-app instance (DSAPI) and its Storage configuration. Refuses prod apps (no `isDraft` flag) and Streamlit apps. | Cleanup primitive — call after a draft's branch has been promoted to `main`. |

The flow is taught to the LLM exclusively through tool docstrings; there is no MCP prompt.

### Runtime image version

The data-science platform now picks a default runtime image for python-js apps, so the MCP server **does not write `runtime.image.version`** into newly created python-js configs. Legacy configs written by earlier MCP versions may still carry a pinned image — those values are preserved verbatim on update via deepcopy, but the MCP never sets or overwrites the pin. To change the image used by a specific app, edit the config directly in the Keboola UI.

### Per-app workspace

Newly created python-js apps carry `runtime.workspace.enabled = true` in their Storage configuration. The platform reads this flag and **auto-provisions a workspace per data app**, then injects its ID into the app's runtime as the `WORKSPACE_ID` environment variable. As a consequence:

- The MCP server does **not** write `WORKSPACE_ID` (or `BRANCH_ID`, `KBC_TOKEN`, `KBC_URL`) into the stored python-js configuration. The platform side is responsible for surfacing those at runtime. The one exception is **legacy projects without the `data-apps-storage-workspace` feature**: there the MCP server falls back to writing `WORKSPACE_ID` into `parameters.dataApp.secrets` so the app still has a workspace ID to read. Streamlit apps still receive the full secret set (`WORKSPACE_ID`, `BRANCH_ID`, `KBC_TOKEN`, `KBC_URL`) from the MCP server because they have no auto-workspace feature.
- The flag is hardcoded `true` on create; there is no tool argument to opt out.
- This is a **create-only** behaviour. The update path does not backfill `runtime.workspace` on existing apps — apps created before this change continue to operate against whichever workspace was injected at their original create time.

### Authentication: HTTPS tokens

Managed repos authenticate over **HTTPS** with one-time tokens minted by sandboxes-service. The MCP server does not surface SSH at all — no keypair generation, no `GIT_SSH_COMMAND`, no `~/.ssh` plumbing.

For the standard draft create flow, `modify_python_js_data_app` mints the prod-side token internally and returns a ready-to-use `git_clone_url` of the form `https://kai:<secret>@<host>/<path>.git`. For the lost-token recovery flow, `create_python_js_data_app_git_credential` does the same on demand. The hardcoded username `kai` is set by the constant `_MANAGED_GIT_REPO_USERNAME` in `src/keboola_mcp_server/tools/data_apps.py`; the git-service ignores the username portion and only validates the token.

The platform supports multiple credentials per app, so minting a fresh credential never invalidates earlier ones — important for the recovery flow below and for parallel iteration.

### Credential lifecycle

- A new credential is minted on the parent **prod** app every time a draft is created. The prod-side credential is encrypted into the draft's `parameters.dataApp.git.#password`.
- **Deleting a draft via `delete_python_js_data_app_draft` does NOT revoke that credential.** The MCP surface has no list/delete-credential affordance; rotation is the user's job via the Keboola UI. (DSAPI does support `DELETE /apps/{id}/git-repo/credentials/{credentialId}`, but the MCP server does not store the credential ID on the draft to make targeted revocation possible.)
- Because the platform accepts unlimited per-app credentials and ignores stale ones for auth purposes, accumulated drafts produce stale-but-harmless credentials on the prod app over time. Treat this as a known trade-off until the platform gains native draft support.

---

## Create flow (new project bootstrap)

Use when there is no prod app yet. The agent creates the prod app first (which owns the managed repo), then creates a draft bound to that repo, iterates with the user on the draft branch, finally merges into `main`, redeploys prod, and tears down the draft.

```
Step 1: modify_python_js_data_app(
            slug='demo',
        )                    ──► { configuration_id: PROD, repo_url: R }
                                  (R = bare HTTPS URL of PROD's managed repo)
                                        │
                                        ▼
Step 2: modify_python_js_data_app(
            slug='demo-draft',
            parent_configuration_id=PROD,
        )                    ──► { configuration_id: DRAFT, repo_url: R,
                                   git_clone_url: U, branch: 'init' }
                                  (U = https://kai:<secret>@host/path.git)
                                        │
                                        ▼
Step 3: YOU: git clone U; git checkout init (create if empty);
        write app.py; git push origin init
                                        │
                                        ▼
Step 4: deploy_data_app(
            action='deploy',
            configuration_id=DRAFT,
            mode='dev',
        )                    ──► preview URL serving 'init' as a dev version
                                 (hot reload + auto-auth iframe preview)
                                        │
                                        ▼ (user approves)
                                        │
Step 5: YOU: git checkout main; git merge init;
        git push origin main; git push origin --delete init
                                        │
                                        ▼
Step 6: deploy_data_app(
            action='deploy',
            configuration_id=PROD,
        )                    ──► prod URL now serves merged main
                                        │
                                        ▼
Step 7: delete_python_js_data_app_draft(
            configuration_id=DRAFT,
        )                    ──► DRAFT (config + data-app instance) torn down
```

Key invariants:

- Only **PROD** owns a managed repo (`use_managed_git_repo=True`). The draft is created with `use_managed_git_repo=False`, a populated `parameters.dataApp.git` block, `isDraft=true`, and `parentConfigurationId=PROD`.
- The draft's `repo_url` equals PROD's `repo_url` — they are the same physical repo on the git-service side.
- The token embedded in `git_clone_url` was minted against **PROD**, not the draft. Existing prod-side tokens are not invalidated.
- Step 4 uses `mode='dev'` to deploy the draft as a dev version (hot reload + iframe auto-auth). Step 6 (prod redeploy) uses no `mode` / no `branch` — prod always deploys `main` from its own managed repo.
- Step 7 deletes the draft via the MCP tool. The draft's prod-side credential is **not** revoked.

---

## Edit flow (modifying an existing data app)

Use when the user wants to change an existing prod app. The agent already has the prod's `configuration_id`, so it goes straight to a fresh credential + draft creation with a descriptive branch.

```
Step 1: create_python_js_data_app_git_credential(
            configuration_id=PROD,
        )                    ──► { git_clone_url: U }
                                 (mint a fresh prod-side token)
                                        │
                                        ▼
Step 2: modify_python_js_data_app(
            slug='demo-draft-<suffix>',
            parent_configuration_id=PROD,
            branch='add-revenue-filter',   (descriptive — pick a useful name)
        )                    ──► { configuration_id: DRAFT, repo_url: R,
                                   git_clone_url: U2, branch: <as supplied> }
                                        │
                                        ▼
Step 3: YOU: git clone U2; git checkout <branch> (create from main);
        edit source; git push origin <branch>
                                        │
                                        ▼
Step 4: deploy_data_app(
            action='deploy',
            configuration_id=DRAFT,
            mode='dev',
        )                    ──► preview URL serving the draft branch
                                        │
                                        ▼ (user approves)
                                        │
Step 5: YOU: git checkout main; git merge <branch>;
        git push origin main; git push origin --delete <branch>
                                        │
                                        ▼
Step 6: deploy_data_app(
            action='deploy',
            configuration_id=PROD,
        )                    ──► prod URL now serves merged main
                                        │
                                        ▼
Step 7: delete_python_js_data_app_draft(
            configuration_id=DRAFT,
        )                    ──► DRAFT torn down
```

Key invariants:

- The prod app's `configuration_id` is **never** modified in this flow — only its underlying git `main` is updated and the app is redeployed.
- The draft's pinned branch is set in its `parameters.dataApp.git.branch` at create time. There is no deploy-time override — `deploy_data_app(mode='dev')` always deploys the pinned branch.
- Slugs must be unique across the prod and its drafts — append a short suffix (e.g. `-draft-abc123`).

---

## Continue-draft flow (resuming an unfinished iteration)

Use when the user wants to continue work on a draft they started earlier but never promoted. The agent has the prod's `configuration_id` but no working clone and no draft handle.

```
Step 1: get_data_apps(
            configuration_ids=[PROD],
        )                    ──► prod detail with drafts: [...]
                                 (Pick the draft the user means — ask if
                                  multiple and unclear. Each entry exposes
                                  configuration_id, slug, and pinned branch
                                  via its config.)
                                        │
                                        ▼
Step 2: create_python_js_data_app_git_credential(
            configuration_id=PROD,         (always mint against prod —
        )                    ──►            drafts have no repo of their own)
                                 { git_clone_url: U }
                                        │
                                        ▼
Step 3: YOU: git clone U; git checkout <draft's pinned branch>;
        resume work; git push
                                        │
                                        ▼
Step 4: deploy_data_app(
            action='deploy',
            configuration_id=<DRAFT>,
            mode='dev',
        )                    ──► preview URL — the draft's branch is already
                                 pinned in its config, no override needed
                                        │
                                        ▼ (user approves)
                                        │
Step 5-7: Same promote/cleanup sequence as Scenario A steps 5–7.
```

Key invariants:

- `get_data_apps(configuration_ids=[PROD])` is the discovery primitive. There is no `list_drafts` tool — the drafts surface inline on the prod's detail response.
- Drafts in trash (deleted via `delete_python_js_data_app_draft` or the UI) are not included in `drafts: [...]`.

---

## Update flow (deployment metadata only)

Distinct from create / edit: when only `auto_suspend_after_seconds`, `name`, `description`, `authentication_type`, or `storage` need to change on an existing app, call `modify_python_js_data_app(configuration_id=<id>, ...)`. The update path:

- Updates the Storage configuration in place — works on both prod apps and drafts.
- **Rejects** `slug` (immutable subdomain), `parent_configuration_id` (repo binding is fixed at creation), and `branch` (only meaningful for draft creates).
- After updating, the caller MUST call `deploy_data_app(...)` to restart the app so changes take effect.

The update flow does NOT involve git — source code changes go through the edit flow. To rotate or add a token, use `create_python_js_data_app_git_credential` on the prod app.

---

## Recovering when the cached HTTPS token is lost

The sandbox the LLM iterates in is ephemeral: when a user returns later to continue an old draft, a fresh sandbox spins up and the conversation is restored, but the `git_clone_url` returned in the previous session is gone with the wiped filesystem. The LLM now holds a `configuration_id` for an existing prod app (and maybe a draft via `get_data_apps`) but cannot `git clone`/`pull`/`push` against the managed repo.

The data-science API accepts **multiple credentials per app**, so registering a fresh one never invalidates credentials already held by other clients (e.g. a teammate iterating against the same prod app's repo).

One-call recovery:

```
1. create_python_js_data_app_git_credential(
       configuration_id=<existing PROD app's cfg id>,
   )                   ──► { credential_id, secret, git_clone_url }
                            git access restored — clone with git_clone_url
```

Always call against the **prod** app's configuration ID — the draft has no managed repo of its own. (Calling against a draft's configuration ID returns a clear "only python-js managed-repo apps have a credentials endpoint" error.)

---

## Parameter reference

### `modify_python_js_data_app(parent_configuration_id=...)`

- **Type**: `Optional[str]`
- **When valid**: create only (raises `ValueError` if set on update).
- **Semantics**: when set, the new app is created as a draft: `use_managed_git_repo=False`, `isDraft=true`, `parentConfigurationId=<this value>`, and `parameters.dataApp.git` populated with the parent's managed repo URL, a freshly minted prod-side HTTPS token (encrypted via the Keboola encryption service), and the draft branch. The parent app must be a python-js app with a managed repo — Streamlit and draft parents are rejected.
- **Returned**: the parent's `repo_url` is returned as `repo_url` unchanged; a `git_clone_url` with the embedded prod-issued token is returned alongside.
- **Stored**: `parentConfigurationId` is **create-only** and immutable. It powers cheap draft discovery via `get_data_apps` detail.

### `modify_python_js_data_app(branch=...)`

- **Type**: `Optional[str]`
- **When valid**: draft create only (must be paired with `parent_configuration_id`). Rejected on prod create and on update.
- **Semantics**: pins the draft to this branch (`parameters.dataApp.git.branch`). When unset, defaults to `'init'` (a sensible default for the first draft of a brand-new prod app — descriptive branches are agent-supplied on edit-existing flows). Must not be `main` (reserved for the prod app); must be non-empty and contain no whitespace. **Uniqueness across drafts is the agent's responsibility** — if `'init'` collides with an existing branch on the prod's repo, the agent sees the error from its own `git push` or from `deploy_data_app`.

### `deploy_data_app(mode=...)`

- **Type**: `Optional[Literal['dev', 'production']]`
- **Semantics**: `mode='dev'` deploys the target as a **dev version of the data app** — the runtime uses a development `setup.sh` (hot reload), and the data-app proxy enables an auto-auth path so an iframe preview can render without a manual login. Only meaningful on drafts (python-js apps with `isDraft=true`). For prod redeploys, omit `mode`.

### `create_python_js_data_app_git_credential(configuration_id=...)`

- **`configuration_id`** (`str`, required): Storage configuration ID of an existing python-js **prod** app (i.e. one with a managed repo). The tool resolves it to the underlying `data_app_id` and rejects Streamlit apps with a clear error. Drafts also reject — always mint against prod.
- **Returns**:
  - `credential_id` — UUID of the credential row on sandboxes-service. Useful only for diagnostics; the MCP surface does not expose list/delete endpoints.
  - `secret` — the one-time HTTPS token. **Cannot be retrieved again** by any subsequent read — store it if you need to reuse it outside of `git_clone_url`.
  - `git_clone_url` — ready-to-use authenticated URL of the form `https://kai:<secret>@<host>/<path>.git`. Pass directly to `git clone`.
  - `permissions` — always `readWrite` (the tool does not expose a permissions knob).

### `delete_python_js_data_app_draft(configuration_id=...)`

- **`configuration_id`** (`str`, required): Storage configuration ID of an existing python-js **draft** (i.e. one with `parameters.dataApp.isDraft=true`). Refuses prod apps and Streamlit apps.
- **Behaviour**: calls DSAPI `DELETE /apps/{data_app_id}` (deletes the data-app instance) followed by Storage `DELETE /branch/{branch}/components/keboola.data-apps/configs/{cfg}` (with `skip_trash=False`, so the config goes to trash for the platform-standard 7 days). Stale prod-side credentials are NOT revoked.
- **Returns**: `{response: 'deleted', configuration_id, data_app_id, parent_configuration_id, links}`. The parent configuration_id is surfaced so the agent can pivot back to the prod app for the next step (e.g. a prod redeploy).

### `get_data_apps(configuration_ids=[<prod-cfg>])` — `drafts: [...]`

- For a python-js **prod** app, the detail response includes a `drafts: list[DataAppSummary]` field containing every draft whose `parameters.dataApp.parentConfigurationId == <prod-cfg>`.
- Empty `[]` for: drafts themselves, Streamlit apps, prod apps with no drafts.
- Drafts in trash are not listed (the storage configuration list endpoint omits them).
- Cost: one extra `configuration_list` call + up to N parallel detail fetches (N = number of drafts). Acceptable in practice — N is typically 0–3 and detail fetches are already heavier than the summary list path.

### `modify_python_js_data_app(authentication_type=...)`

- **Type**: `'no-auth' | 'basic-auth' | 'default'` (default: `'default'`).
- **Semantics on create**: `'default'` and `'basic-auth'` both apply HTTP basic authentication (safe-by-default for new apps); `'no-auth'` exposes the app publicly.
- **Semantics on update**: `'default'` leaves the existing `authorization` block untouched (so OIDC and other advanced setups configured outside the MCP survive); `'basic-auth'` and `'no-auth'` overwrite it.
- **Wire shape**: identical to Streamlit — `authorization.app_proxy.{auth_providers, auth_rules}`. The DSAPI's python-js endpoint accepts this block alongside `useManagedGitRepo: true`.

---

## What's intentionally NOT a separate tool

Several variants of this flow could have been packaged as dedicated tools but were left out:

- **`create_draft_data_app(parent_configuration_id)`** — `modify_python_js_data_app` with the `parent_configuration_id` parameter is already exactly that; adding a separate tool name would just duplicate the surface.
- **Credential listing/deletion** — out of scope; per-app credentials are append-only from the MCP surface (sandboxes-service supports listing/getting/deleting via `GET /apps/{id}/git-repo/credentials`, `GET .../credentials/{credentialId}`, and `DELETE .../credentials/{credentialId}`, but the MCP server does not expose them). The platform UI is the rotation/cleanup affordance.
- **Single promote-to-prod tool** — the agent does merge+push+branch-delete locally, then `delete_python_js_data_app_draft` + `deploy_data_app(prod_id)`. No bundled MCP tool, because MCP does not run git.
- **SSH credential support** — the swagger lets sandboxes-service issue `ssh_key` credentials as well, but the MCP surface mints only `http_token` credentials. Users who need SSH access provision it through the platform UI directly.

---

## Wire-level details (data-science API + Storage)

The tool surface maps to the underlying APIs as follows. Confirm field names with the platform team if they ever change.

| Tool parameter | API endpoint | Field on the wire |
|---|---|---|
| Prod create (default) | `POST /apps` | `useManagedGitRepo: true`. No `parameters.dataApp.git` block. |
| `parent_configuration_id` (draft) | `POST /apps` | `useManagedGitRepo` omitted/false. `configuration.parameters.dataApp.git = {repository, username, '#password' (encrypted via EncryptionClient), branch}`. Also writes `isDraft: true` and `parentConfigurationId: <prod cfg id>`. |
| `deploy_data_app(mode='dev')` | `PATCH /apps/{id}` | `mode: 'dev'` (alongside `desiredState: 'running'`). The deployed branch is whatever the draft's config pins — there is no deploy-time branch field. |
| `create_python_js_data_app_git_credential` | `POST /apps/{id}/git-repo/credentials` | Request: `{type: 'http_token', permissions: 'readWrite'}`. Response: `{id, type, permissions, secret, ...}`. The one-time `secret` is what we embed (with `kai` as username) into `git_clone_url`. |
| (clone URL lookup) | `GET /apps/{id}/git-repo` | Response: `{sshUrl, httpsUrl, isManagedGitRepo}`. The MCP server uses `httpsUrl` only. |
| auto-workspace flag (hardcoded `true` on create) | `POST /apps` | `configuration.runtime.workspace.enabled` |
| `delete_python_js_data_app_draft` | `DELETE /apps/{id}` + `DELETE /branch/{branch}/components/keboola.data-apps/configs/{cfg}` | Two-call sequence: DSAPI delete first, then Storage delete (without `skip_trash`). |
| `get_data_apps` drafts lookup | `GET /branch/{branch}/components/keboola.data-apps/configs` | One Storage list call per prod detail fetch; results filtered by `configuration.parameters.dataApp.parentConfigurationId`. |
| `parentConfigurationId` (in draft config) | (none — Storage-only field) | Lives at `configuration.parameters.dataApp.parentConfigurationId`. Create-only, immutable. |

---

## End-to-end verification checklist

Against `data-science.canary-orion.keboola.dev`:

**Create flow**

- [ ] `modify_python_js_data_app(slug='demo')` returns `(PROD, R)`. `R` starts with `https://` (no `git@`).
- [ ] `modify_python_js_data_app(slug='demo-draft', parent_configuration_id=PROD)` returns `(DRAFT, R, git_clone_url, 'init')`. `git_clone_url` matches `https://kai:<secret>@<host>/<path>.git`.
- [ ] Inspect `DRAFT`'s Storage config in the UI:
  - [ ] `parameters.dataApp.git.repository == R`.
  - [ ] `parameters.dataApp.git.#password` is encrypted ciphertext (`KBC::ConfigSecureGKMS::...`).
  - [ ] `parameters.dataApp.git.branch == 'init'`.
  - [ ] `parameters.dataApp.isDraft == true`.
  - [ ] `parameters.dataApp.parentConfigurationId == <PROD cfg id>`.
- [ ] `git clone <git_clone_url>` works with no local key plumbing.
- [ ] Push a minimal `app.py` on the `init` branch.
- [ ] `deploy_data_app(configuration_id=DRAFT, mode='dev')` produces a working preview URL serving the branch as a dev version.
- [ ] `get_data_apps(configuration_ids=[PROD])` returns prod detail with `DRAFT` listed in `drafts: [...]`.
- [ ] Locally `git checkout main && git merge init && git push && git push origin --delete init`.
- [ ] `deploy_data_app(configuration_id=PROD)` produces a working prod URL serving merged `main`.
- [ ] `delete_python_js_data_app_draft(configuration_id=DRAFT)` returns `{response: 'deleted', parent_configuration_id: PROD}` and `DRAFT` disappears from `drafts: [...]` on the next `get_data_apps(configuration_ids=[PROD])`.

**Edit flow**

- [ ] Given an existing `PROD`, `create_python_js_data_app_git_credential(configuration_id=PROD)` returns a fresh `git_clone_url`.
- [ ] `modify_python_js_data_app(slug='demo-draft-xyz', parent_configuration_id=PROD, branch='add-revenue-filter')` returns `(DRAFT2, R, U, 'add-revenue-filter')`.
- [ ] Push `add-revenue-filter` to `R` via the embedded credential.
- [ ] `deploy_data_app(configuration_id=DRAFT2, mode='dev')` previews the branch.
- [ ] Merge into `main` and push.
- [ ] `deploy_data_app(configuration_id=PROD)` now serves the merged code.
- [ ] `delete_python_js_data_app_draft(configuration_id=DRAFT2)` cleans up.

**Continue-draft flow**

- [ ] Given an existing `PROD` with a draft `DRAFT3` pinned to branch `wip-add-chart`, `get_data_apps(configuration_ids=[PROD])` returns `drafts: [...]` with `DRAFT3` and its branch visible.
- [ ] `create_python_js_data_app_git_credential(configuration_id=PROD)` returns a fresh `git_clone_url`.
- [ ] `git clone`, `git checkout wip-add-chart`, push another commit.
- [ ] `deploy_data_app(configuration_id=DRAFT3, mode='dev')` deploys the branch as a dev version (the draft's config pins the branch).

**Lost-token recovery flow**

- [ ] Pretend the `git_clone_url` from a prior session is gone.
- [ ] `create_python_js_data_app_git_credential(configuration_id=PROD)` returns a fresh `git_clone_url`.
- [ ] Cloning `R` with the new URL works.
- [ ] Existing clones from before the recovery remain usable (server accepts multiple credentials; old ones are not revoked).
- [ ] `create_python_js_data_app_git_credential(configuration_id=<streamlit cfg>)` raises a clear "only python-js apps" error.

**Safety**

- [ ] `delete_python_js_data_app_draft(configuration_id=PROD)` raises a clear "prod app, not a draft" error and does not delete anything.
- [ ] `delete_python_js_data_app_draft(configuration_id=<streamlit cfg>)` raises "only supports python-js data apps".

---

## v1.63 → v1.64 migration

The dev-twin terminology is gone. Internally, "dev twin" → "draft"; in the wire, drafts now persist `parentConfigurationId` and the default branch is `'init'` (not `iter-<6-hex>`). User-facing tool parameters did not change shape — `parent_configuration_id` and `branch` keep their names — but the doc framing, docstrings, and default branch differ. A new tool `delete_python_js_data_app_draft` ships in v1.64.0.

| | v1.63 (MVP) | v1.64 (this doc) |
|---|---|---|
| Iteration entity name | "dev twin" | "draft" |
| Default iteration branch | `iter-<6-hex>` (random) | `'init'` (literal; descriptive branches agent-supplied for edits) |
| Draft → prod linkage on the wire | (not stored) | `parameters.dataApp.parentConfigurationId` (Storage config; create-only) |
| Drafts of a prod, discovery | Scan all configs and filter by repo URL (no MCP surface) | `get_data_apps(configuration_ids=[PROD])` returns `drafts: [...]` |
| Cleanup affordance | UI "Discard" button only | `delete_python_js_data_app_draft` MCP tool |
| Tool docs | `docs/python-js-data-apps.md` v1.63 | this file |

For background on why the platform behaves this way, see the **Why prod owns the repo** section above.
