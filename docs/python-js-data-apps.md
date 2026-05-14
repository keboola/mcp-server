# Python-JS Data Apps: Dev-Twin Workflow

**Linear**: [AI-3005](https://linear.app/keboola/issue/AI-3005)
**Status**: Implemented in v1.62.0

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

The dev-twin flow extends two existing tools with new parameters and adds one dedicated tool for SSH-key registration:

| Tool | Change | Purpose |
|---|---|---|
| `modify_python_js_data_app` | New `existing_repo_url` (create-only) | Bind the new app to an existing managed repo instead of provisioning a fresh one. |
| `deploy_data_app` | New `branch` | Deploy a python-js dev twin from a specific branch instead of `main`. Only valid with `mode='dev'`. |
| `register_python_js_data_app_ssh_key` | New dedicated tool | Attach an SSH public key to a python-js app — used both right after create and to recover when the private key is gone (new Kai sandbox). |

The flow is taught to the LLM exclusively through the tool docstrings; there is no MCP prompt.

### Runtime image version

The runtime image version is currently **hardcoded** in the MCP server (constant `_HARDCODED_PYTHON_JS_IMAGE_VERSION` in `src/keboola_mcp_server/tools/data_apps.py`, value `dev-PAT-1772.4`). The tool does not expose it as an argument. Remove the hardcoded constant and re-introduce the argument (or simply drop the field from the payload) once the platform sets a default image for python-js apps.

---

## Create flow (new project bootstrap)

Use when there is no prod app yet. The LLM creates a temporary dev iteration app, iterates with the user, then creates the prod app sharing the same repo.

```
Step 1: ssh-keygen           ──► local SSH keypair (public K, private)
                                        │
                                        ▼
Step 2a: modify_python_js_data_app(
            slug='demo-iter-abc',
        )                    ──► { configuration_id: C1, repo_url: R }
                                        │
                                        ▼
Step 2b: register_python_js_data_app_ssh_key(
             configuration_id=C1,
             public_key=K,
         )                   ──► SSH key registered on C1
                                        │
                                        ▼
Step 3: git clone R; write app.py; git push origin main
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
Step 5b: register_python_js_data_app_ssh_key(
             configuration_id=C2,
             public_key=K,   (same K works; SSH keys are per-app)
         )                   ──► SSH key registered on C2
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
- Step 5b reuses the same `public_key` because SSH keys are registered **per-app**; the same public key works for both apps but must be registered on each.
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
Step 2: ssh-keygen           ──► fresh SSH keypair K2 (keys are per-app)
                                        │
                                        ▼
Step 3: modify_python_js_data_app(
            slug='demo-dev-xyz',  (unique suffix)
            existing_repo_url=R,
        )                    ──► { configuration_id: C3, repo_url: R }
                                        │
                                        ▼
Step 4: register_python_js_data_app_ssh_key(
            configuration_id=C3,
            public_key=K2,
        )                    ──► SSH key registered on C3
                                        │
                                        ▼
Step 5: git clone R
        git checkout -b feature-x
        write changes
        git push origin feature-x
                                        │
                                        ▼
Step 6: deploy_data_app(
            action='deploy',
            configuration_id=C3,
            mode='dev',
            branch='feature-x',
        )                    ──► preview URL serving feature-x
                                        │
                                        ▼ (user approves)
                                        │
Step 7: git checkout main
        git merge feature-x
        git push origin main
                                        │
                                        ▼
Step 8: deploy_data_app(
            action='deploy',
            configuration_id=prod_id,
        )                    ──► prod URL now serves merged main
                                        │
                                        ▼
Step 9: C3 stays listed under prod_id in the UI's "Drafts"
        section; user clicks "Discard" to remove it.
```

Key invariants:

- The prod app's `configuration_id` (`prod_id`) is **never** modified in this flow — only its underlying git `main` is updated and the app is redeployed.
- The `branch` parameter is **only meaningful with `mode='dev'`**; the tool rejects `branch` without `mode='dev'`. Prod redeploys (Step 8) use neither.
- The slug for the dev twin needs a short unique suffix (e.g. `-dev-xyz`) — both apps live in the same project and the slug is a DNS label.

---

## Update flow (deployment metadata only)

Distinct from create / edit: when only `auto_suspend_after_seconds`, `name`, or `description` need to change on an existing app, call `modify_python_js_data_app(configuration_id=<id>, ...)`. The update path:

- Updates the Storage configuration in place.
- **Rejects** `slug` (immutable subdomain) and `existing_repo_url` (repo binding is fixed at creation).
- After updating, the caller MUST call `deploy_data_app(...)` to restart the app so changes take effect.

The update flow does NOT involve git — source code changes go through the edit flow. To rotate or add an SSH key, use `register_python_js_data_app_ssh_key` (the data-science API accepts multiple keys per app).

---

## Recovering from a lost SSH key

The Kai sandbox the LLM iterates in is ephemeral: when a user returns later to continue an old draft, a fresh sandbox spins up and the conversation is restored, but the private key generated in the previous session is gone with the wiped filesystem. The LLM now holds a `configuration_id` for an existing python-js app but cannot `git clone`/`pull`/`push` against its managed repo.

The data-science API accepts **multiple SSH keys per app**, so registering a fresh key never invalidates keys already held by other clients (e.g. a teammate iterating against the same prod app's repo).

One-call recovery:

```
1. ssh-keygen -t ed25519 -N '' -f ~/.ssh/keboola-app-<slug>
                       │
                       ▼
2. register_python_js_data_app_ssh_key(
       configuration_id=<existing app's cfg id>,
       public_key=<contents of id_ed25519.pub>,
   )                   ──► fresh SSH key registered, git access restored
```

This works regardless of whether the app is a dev twin or a prod app — both expose the same per-app SSH-key endpoint.

---

## Parameter reference

### `modify_python_js_data_app(existing_repo_url=...)`

- **Type**: `Optional[str]`
- **When valid**: create only (raises `ValueError` if set on update).
- **Semantics**: when set, the new app is bound to the existing managed repo URL. No fresh repo is provisioned. SSH-key registration is a separate per-app step — call `register_python_js_data_app_ssh_key` on the new app after create.
- **Returned**: the same URL is returned as `repo_url` unchanged.

### `deploy_data_app(branch=...)`

- **Type**: `Optional[str]`
- **When valid**: only with `mode='dev'`. Raises `ValueError` otherwise.
- **Semantics**: for python-js apps, deploys from this git branch instead of `main`. Silently ignored for Streamlit apps (which have no managed git repo).
- **Without `branch`** (and `mode='dev'`): the dev twin deploys `main`.

### `register_python_js_data_app_ssh_key(configuration_id=..., public_key=...)`

- **`configuration_id`** (`str`, required): Storage configuration ID of an existing python-js data app. The tool resolves it to the underlying `data_app_id` and rejects Streamlit apps with a clear error.
- **`public_key`** (`str`, required): full SSH public key contents (e.g. the contents of an `id_ed25519.pub` file). Always registered with `readWrite` permissions — the tool does not expose a permissions knob.

### `modify_python_js_data_app(authentication_type=...)`

- **Type**: `'no-auth' | 'basic-auth' | 'default'` (default: `'default'`).
- **Semantics on create**: `'default'` and `'basic-auth'` both apply HTTP basic authentication (safe-by-default for new apps); `'no-auth'` exposes the app publicly.
- **Semantics on update**: `'default'` leaves the existing `authorization` block untouched (so OIDC and other advanced setups configured outside the MCP survive); `'basic-auth'` and `'no-auth'` overwrite it.
- **Wire shape**: identical to Streamlit — `authorization.app_proxy.{auth_providers, auth_rules}`. The DSAPI's python-js endpoint accepts this block alongside `useManagedGitRepo: true`.

---

## What's intentionally NOT a separate tool

Several variants of this flow could have been packaged as dedicated tools but were left out:

- **`promote_to_prod(dev_configuration_id)`** — composing `modify_python_js_data_app(existing_repo_url=...)` + `register_python_js_data_app_ssh_key(...)` + `deploy_data_app(...)` keeps the surface small and reuses primitives. Revisit if real usage shows the multi-call orchestration is error-prone.
- **`create_dev_twin_data_app(parent_configuration_id)`** — same reasoning. The dev twin reuses the same hardcoded runtime image as every other python-js app, so no parent inspection is needed.
- **MCP-side deletion of dev twins** — the UI lists each dev twin under its parent prod app in the "Drafts" section with a "Discard" button. Cleanup is an explicit user action; there's no platform-side GC and no need for an MCP tool to delete twins.
- **SSH key listing/deletion endpoints** — out of scope; per-app keys are append-only from the MCP surface (the data-science API may support listing/deletion but the MCP server doesn't expose them). The platform UI is the rotation/cleanup affordance.

---

## Wire-level details (data-science API)

The two parameters map to the data-science API as follows. Confirm field names with the platform team if they ever change — adjusting `DataScienceClient` is a one-line tweak per field.

| Tool parameter | API endpoint | Field on the wire |
|---|---|---|
| `existing_repo_url` | `POST /apps` | `existingRepoUrl` (alongside `useManagedGitRepo: true`) |
| `branch` | `PATCH /apps/{id}` | `branch` (alongside `desiredState: 'running'`, `mode: 'dev'`) |
| `register_python_js_data_app_ssh_key` | `POST /apps/{id}/git-repo/ssh-keys` | `publicKey`, `permissions: 'readWrite'` |

---

## End-to-end verification checklist

Against `data-science.canary-orion.keboola.dev`:

**Create flow**

- [ ] `modify_python_js_data_app(slug='demo-iter-abc')` returns `(C1, R)`.
- [ ] `register_python_js_data_app_ssh_key(configuration_id=C1, public_key=K)` returns the registered key id.
- [ ] Clone `R`, push minimal `app.py` to `main`.
- [ ] `deploy_data_app(configuration_id=C1, mode='dev')` produces a working preview URL.
- [ ] `modify_python_js_data_app(slug='demo', existing_repo_url=R)` returns `(C2, R)`. (Same `R`.)
- [ ] `register_python_js_data_app_ssh_key(configuration_id=C2, public_key=K)` returns the registered key id (same `K` works; keys are per-app).
- [ ] `deploy_data_app(configuration_id=C2)` produces a working prod URL serving the same code.
- [ ] `C1` is listed under `C2` in the UI's "Drafts" section; clicking "Discard" removes it.

**Edit flow**

- [ ] `get_data_apps(configuration_ids=[C2])` returns `repo_url == R`.
- [ ] `modify_python_js_data_app(slug='demo-dev-xyz', existing_repo_url=R)` returns `(C3, R)`.
- [ ] `register_python_js_data_app_ssh_key(configuration_id=C3, public_key=K2)` returns the registered key id.
- [ ] Push `feature-x` to `R`.
- [ ] `deploy_data_app(configuration_id=C3, mode='dev', branch='feature-x')` previews the branch.
- [ ] Merge `feature-x` into `main` and push.
- [ ] `deploy_data_app(configuration_id=C2)` now serves the merged code.
- [ ] `C3` is listed under `C2` in the UI's "Drafts" section; clicking "Discard" removes it.

**Lost-key recovery flow**

- [ ] Pretend the private key from a prior session is gone; generate a fresh keypair `K3` locally.
- [ ] `register_python_js_data_app_ssh_key(configuration_id=C2, public_key=K3)` returns the registered key id.
- [ ] Cloning `R` with the new private key works.
- [ ] Existing clones from before the recovery remain usable (server accepts multiple keys; old key is not revoked).
- [ ] `register_python_js_data_app_ssh_key(configuration_id=<streamlit cfg>, public_key=K3)` raises a clear "only python-js apps" error.
