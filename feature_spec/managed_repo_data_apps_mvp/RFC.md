# RFC: Prod-owned managed repo + external-git dev twin (MVP)

Linear: [AI-3005](https://linear.app/keboola/issue/AI-3005) (this work)
Long-term tracker: [AI-3240](https://linear.app/keboola/issue/AI-3240) (platform-side drafts)

## Problem

The two-app python-js flow shipped in v1.62.0 (see [`docs/python-js-data-apps.md`](../../docs/python-js-data-apps.md))
relied on a dev iteration app being created first with its own managed git repo, and the
prod app being created later via `existing_repo_url=<dev's repo URL>` so both apps would
share one managed repo. Live testing on `data-science.canary-orion.keboola.dev`, plus
confirmation from Pepa Martinec (sandboxes-service), established that this does not work:

- `POST /apps` **silently drops** the `existingRepoUrl` field. Every app gets its own
  fresh managed repo provisioned for it, regardless of what the caller passed.
- The platform has no other mechanism today to link or share managed repos across apps.

Consequence: every v1.62.0 caller using `existing_repo_url` was hitting silent failures —
the two apps the LLM created were bound to two *different* managed repos and the
"share-the-repo" promise was a no-op. Push to one app's repo did not change what the
other app served.

A proper "drafts" concept on the platform is the long-term fix and is tracked in
[AI-3240](https://linear.app/keboola/issue/AI-3240). Until that ships, we need an MVP
that delivers the same dev/prod iteration UX without requiring platform-side shared-repo
support.

## Required Behavior

`modify_python_js_data_app` must support a two-app project model where:

1. The **prod app** is created first and owns the only managed git repo for the project
   (`useManagedGitRepo=True`).
2. A **dev twin** can be created later, bound to the prod app's repo for iteration. The
   dev twin must:
   - Be created with `useManagedGitRepo` omitted/false — no managed repo of its own.
   - Have `parameters.dataApp.git = {repository, username, '#password', branch}` written
     into its Storage configuration, pointing at the prod app's HTTPS clone URL with a
     prod-issued HTTPS token and a non-`main` iteration branch.
   - Have `#password` encrypted via the Keboola encryption service before being written
     to Storage, so the platform can decrypt it at runtime (`KBC::ConfigSecureGKMS::...`).
3. After approval, the agent merges the iteration branch into the prod app's `main`
   locally and pushes; the prod app picks up `main` on its next deploy.

The user-visible primitives (one prod + iteration twins; twins appear under prod in the
UI's "Drafts" section) are unchanged.

## Resolution Strategy

### Tool-surface change

Replace the `existing_repo_url` parameter on `modify_python_js_data_app` (introduced in
v1.62.0; always broken end-to-end) with:

- **`parent_configuration_id: Optional[str]`** — Storage configuration ID of the prod
  python-js app the new dev twin will iterate against. Set on create only; rejected on
  update. When set, the new app is created as a dev twin (`use_managed_git_repo=False`)
  with a populated `parameters.dataApp.git` block. When unset on create, the new app is
  created as a prod app (`use_managed_git_repo=True`).
- **`branch: Optional[str]`** — Iteration branch the dev twin is pinned to. Defaults to
  `iter-<6-hex>` when unset; rejected when not paired with `parent_configuration_id`;
  rejected on update.

Output: `ModifiedPythonJsDataAppOutput` gains optional `git_clone_url` and `branch`
fields. Both are populated on the dev-twin create path so the agent can clone immediately
without a separate credential-mint call (the credential is minted on the parent prod app
inside the tool and embedded into `git_clone_url`).

### Config-model change

Add a `Git` nested model to `CodeDataAppConfig.Parameters.DataApp` so the external-git
block can be serialized into `POST /apps`:

```python
class Git(BaseModel):
    repository: str
    username: str
    password: str   # serialization_alias='#password'
    branch: str | None
```

The `#password` field is encrypted via `EncryptionClient.encrypt` (which walks the dict
and only encrypts keys starting with `#`) before being sent to data-science.

### Client-surface change

`DataScienceClient.create_data_app` loses its `existing_repo_url` parameter (it was
never honored on the wire). `useManagedGitRepo` stays. The git binding now travels
inside the configuration body, not as a top-level request field.

### Why `parent_configuration_id` over keeping `existing_repo_url`

- `existing_repo_url` always implied a wire-level `existingRepoUrl` field that the
  platform ignored. Keeping the same name would be misleading even if the tool now
  routes the URL through `parameters.dataApp.git`.
- `parent_configuration_id` matches how the agent already thinks about the
  prod-app/dev-twin relationship (the UI surfaces twins under their parent in the
  "Drafts" section). It also lets the tool internalize the repo-URL lookup
  (`_fetch_data_app(configuration_id=parent)` already returns both `data_app_id` and
  `repo_url`), so the agent doesn't need a separate `get_data_apps` call to obtain the
  URL.
- Anyone using v1.62.0's `existing_repo_url` was getting silent failures. A hard rename
  surfaces the migration; a backwards-compat alias would silently "fix itself" and the
  agent would have no way to know that the underlying flow has flipped.

## Scope

In scope:

- Code changes to `src/keboola_mcp_server/clients/data_science.py` and
  `src/keboola_mcp_server/tools/data_apps.py`.
- Test changes to `tests/tools/test_data_apps.py` and `integtests/tools/test_data_apps.py`.
- Docs rewrite in `docs/python-js-data-apps.md` and the
  `modify_python_js_data_app` docstring (which is the source of `TOOLS.md`).
- Version bump `pyproject.toml` 1.62.0 → 1.63.0 and `uv.lock`.

Out of scope:

- A first-class "drafts" mechanism on the data-science platform. Tracked separately in
  [AI-3240](https://linear.app/keboola/issue/AI-3240).
- Backwards-compatibility shims for `existing_repo_url`. The parameter was never
  honored end-to-end, so any caller relying on it was already broken — a hard removal
  surfaces the migration.
- SSH-key support for managed-repo authentication. The MCP surface mints only
  `http_token` credentials.
- Listing/deletion of credentials via the MCP. The sandboxes-service supports these but
  per-app credentials remain append-only from the MCP surface.

## Verification

Unit tests (`tests/tools/test_data_apps.py`) cover:

- Dev-twin create wires `use_managed_git_repo=False` and the external-git block,
  encrypts the config via `EncryptionClient`, and mints the credential on the parent.
- Dev-twin create generates an `iter-<6-hex>` branch when none is supplied.
- Dev-twin create rejects Streamlit parents and parents with no managed repo URL.
- Update path rejects `parent_configuration_id` and `branch`.
- Prod create always goes through `get_app_git_repo` (no short-circuit).

Integration test (`integtests/tools/test_data_apps.py::test_python_js_data_app_prod_and_dev_twin_lifecycle`)
runs end-to-end on canary-orion:

1. Create prod (managed repo).
2. Create dev twin pointing at prod with an iteration branch.
3. Clone via the embedded credential, push the branch.
4. `deploy_data_app(mode='dev')` on the dev twin.
5. Merge the iteration branch into `main`, push.
6. `deploy_data_app(prod)`.
7. Assert the dev twin's stored config carries the external-git block (`repository ==
   prod's repo`, `branch == iter-...`, `#password` encrypted).
8. Teardown both apps.

Manual end-to-end (per [`CLAUDE.md`](../../CLAUDE.md) "Local End-to-End Testing"):
ask Kai to build a small python-js app, inspect the tool-call sequence and the
resulting Storage configs in the Keboola UI, confirm the dev twin appears under the
prod app in the "Drafts" section. If the UI grouping does not work (it relies on
platform-side recognition of the prod-issued credential or repo URL), surface as a
platform-side dependency to Pepa and document the deviation here.

## Compatibility

Breaking change for callers passing `existing_repo_url` to `modify_python_js_data_app`.
Pydantic raises a clear `unexpected keyword argument` error so the change is loud rather
than silent. The replacement parameter (`parent_configuration_id`) is straightforward
to adopt — see the migration table in
[`docs/python-js-data-apps.md`](../../docs/python-js-data-apps.md#v1620--v1630-migration).
