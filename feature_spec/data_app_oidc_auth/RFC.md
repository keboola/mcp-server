# RFC: OIDC authentication for data apps

Linear: [AJDA-3349](https://linear.app/keboola/issue/AJDA-3349/mcpkbagent-create-and-update-data-apps-with-oidc-auth-today-only)

## Problem

Agents (Claude Code, Kai, kbagent) can create data apps with HTTP basic auth or no auth
only. `AuthenticationType` is `Literal['no-auth', 'basic-auth', 'default']`
(`tools/data_apps.py:146`) and `_get_authorization()` (`tools/data_apps.py:2134`) emits either a
single `simpleAuth` password provider or an empty provider list. There is no third option.

Enterprise customers (Groupon, ČS) require SSO, so today the agent path stops halfway: a human
must open the UI and configure OIDC by hand. Worse, any later agent-driven update that passes
`authentication_type='basic-auth'` overwrites the hand-made OIDC block and silently downgrades
the app to a shared password (AI-1848). The platform already accepts OIDC — no backend change is
needed — so this is purely a gap in what the MCP tools can express.

Two smaller problems surface alongside it:

1. **Plaintext secret at rest on one write path.** `StorageClient._encrypt_secrets`
   (`clients/storage.py:360`) encrypts every `#`-prefixed key fail-closed inside
   `configuration_create` / `configuration_update`, so anything written through Storage is safe —
   this covers the python-js *update* path (`tools/data_apps.py:1186`) and the streamlit update
   path. Data-app **creates**, however, POST to the data-science API
   (`data_science_client.create_data_app`), bypassing Storage entirely. The streamlit create path
   compensates by encrypting explicitly (`tools/data_apps.py:673`), but python-js create only
   encrypts inside `if git_block is not None:` (`tools/data_apps.py:1305`) — i.e. for drafts. A
   python-js **prod** create writes its config unencrypted. No `#` key reaches that path today, so
   nothing leaks yet; introducing `#client_secret` would be the first.

2. **No way for an agent to produce a cipher.** Nothing outside `tools/data_apps.py` uses
   `encryption_client`, and there is no tool that turns a plaintext secret into a
   `KBC::ProjectSecure::…` value. An agent therefore cannot complete any flow that requires one.

## Required Behavior

### `authentication_type='oidc'`

Both `modify_streamlit_data_app` and `modify_python_js_data_app` accept `'oidc'` and a new
optional structured `oidc` parameter.

| Field | Required | Behavior |
| --- | --- | --- |
| `client_id` | yes | Written verbatim. |
| `issuer_url` | yes | Written verbatim. No IdP presets — see *Rejected alternatives*. |
| `client_secret` | no | Must already be a `KBC::…` cipher. Plaintext is rejected. Omitted on create → provider skeleton, secret entered in the UI. Omitted on update → the stored cipher is preserved. |
| `logout_url` | no | Key omitted from the stored config when `None`. |
| `allowed_roles` | no | Key omitted from the stored config when `None`. |
| `provider_id` | no | Defaults to `'oidc'`. A stable id is what makes update-merge deterministic. |

Rejections, each with a message naming the fix:

| Condition | Result |
| --- | --- |
| `authentication_type='oidc'` without `oidc` | `ValueError` |
| `oidc` passed with any other `authentication_type` | `ValueError` — never silently ignored |
| `client_secret` not starting with `KBC::` | `ValueError` pointing at `encrypt_secret` and at the skeleton route |
| `authentication_type='oidc'` on a python-js **draft** | `ValueError` — a draft has its own slug, therefore its own callback URL; OIDC belongs on the prod app |

Streamlit apps have no draft concept, so the last check is python-js only.

### Stored shape

`authorization` must equal what the UI writes, so an app configured either way is
indistinguishable and passes `AppProxyDefinition` validation:

```json
{
  "app_proxy": {
    "auth_providers": [
      {
        "id": "oidc",
        "type": "oidc",
        "client_id": "…",
        "#client_secret": "KBC::ProjectSecure::…",
        "issuer_url": "https://…",
        "logout_url": "https://…",
        "allowed_roles": ["…"]
      }
    ],
    "auth_rules": [
      {"type": "pathPrefix", "value": "/", "auth_required": true, "auth": ["oidc"]}
    ]
  }
}
```

`logout_url` and `allowed_roles` keys are absent, not null, when unset.

### Update semantics

| `authentication_type` on update | Effect on the stored `authorization` |
| --- | --- |
| `'default'` | Preserved verbatim, including OIDC configured outside the MCP (AI-1848). Unchanged from today. |
| `'basic-auth'` / `'no-auth'` | Overwritten, as today. |
| `'oidc'`, no existing OIDC provider | Replaced with a fresh OIDC block. |
| `'oidc'`, existing provider with the same `provider_id` | Merged field-by-field: only keys present in the new `oidc` param overwrite. Omitting `client_secret` preserves the stored cipher. |

The merge is what lets an operator rotate `issuer_url` or `allowed_roles` without re-supplying
the secret — the common case, since the secret is often entered in the UI and never held by the
agent at all. Non-OIDC providers (`simpleAuth`) are dropped: OIDC replaces the scheme rather
than stacking on it.

### Callback URL

Both output models gain `oidc_callback_url: str | None`, set to
`f'{deployment_url}/_proxy/callback'` whenever the resulting app uses OIDC — on update as well as
create, since an operator switching an existing app needs it just as much.

The tool docstrings document the ordering this forces, because the IdP will not accept a client
registration without a redirect URI and the URI is not known until the app exists:

1. Create with `authentication_type='basic-auth'`.
2. Read `oidc_callback_url` from the response.
3. Register it as a redirect URI at the IdP; obtain `client_id` / secret.
4. `encrypt_secret(...)` → cipher (or skip and enter the secret in the UI).
5. Update with `authentication_type='oidc'` and the `oidc` block.
6. `deploy_data_app`.

### `encrypt_secret` tool

```
encrypt_secret(value: str, component_id: str, project_id: ProjectIdArg = None)
    -> {cipher: str, already_encrypted: bool}
```

| Requirement | Rationale |
| --- | --- |
| Always `KBC::ProjectSecure::` scope (`project_id` + `component_id`) | Per Keboola encryption guidance this is the right default; see below for why `configuration_id` is not exposed. |
| A `KBC::…` input returns unchanged with `already_encrypted=True` | Idempotent, so a retry cannot double-wrap a cipher. |
| `value` never appears in the output or in any error message | The cipher is safe to log and pass around; the plaintext is not. |
| `readOnlyHint=True`, `destructiveHint=False` | It mutates nothing. Clients with auto-approval policies should not treat it as a write. |

`configuration_id` is deliberately not a parameter: supplying it yields a `ConfigSecure` cipher,
which is valid only inside one exact config and makes config copy fail. That is a footgun worth
withholding from an agent; the handful of cases that genuinely need `ConfigSecure` can use the
Encryption API directly.

**Security boundary — stated plainly.** This tool does not keep plaintext out of the model's
context. The agent passes the secret as a tool argument, so it lands in the context window and
the transcript exactly as it would in a plaintext `client_secret` field. What it provides is
narrower and still worth having: plaintext is accepted at exactly one auditable call site
instead of four data-app write paths; the resulting cipher is safe to reuse, store and log; and
the capability is generic across components rather than OIDC-specific. Operators who require
that the plaintext never enter the agent's context at all must use the skeleton route and enter
the secret in the UI, which stays fully supported.

## Resolution Strategy

### `tools/data_apps.py`

- `AuthenticationType` gains `'oidc'`.
- New `OidcAuthConfig(BaseModel)` with the fields above and a validator rejecting a non-`KBC::`
  `client_secret` (reusing `is_encrypted_value` from `clients/encryption.py`).
- `_get_authorization(auth_with_password: bool)` (`:2134`) is replaced by
  `_build_authorization(authentication_type, oidc=None, existing=None)`. The two existing
  branches are preserved unchanged; the OIDC branch builds the block above and performs the merge
  against `existing`. All four current call sites are updated: python-js create (`:1238`),
  python-js update (`:1601`), streamlit create via `_build_data_app_config` (`:1879`), and
  streamlit update via `_update_existing_data_app_config` (`:1913`).
- `_uses_basic_authentication()` (`:2261`) needs no change — it matches `simpleAuth` and already
  returns `False` for an OIDC app, which is the correct input to `get_data_app_links`.
- New `_reject_oidc_on_draft()` alongside `_reject_no_auth_on_draft()` (`:158`), called at the
  same two python-js draft checkpoints (`:1156`, `:1175`).
- `ModifiedDataAppOutput` (`:401`) and `ModifiedPythonJsDataAppOutput` (`:411`) gain
  `oidc_callback_url`.
- **Encryption fix:** dedent the `encryption_client.encrypt` call at `:1305`–`:1315` out of the
  `if git_block is not None:` branch so python-js prod create encrypts unconditionally, matching
  streamlit create at `:673`. The encryption service walks the payload and touches only
  `#`-prefixed keys, passing already-encrypted values through untouched, so this is safe for
  configs that contain no secrets at all. This costs one extra API call on prod create; that is
  the correct trade against a silent plaintext write, and it removes the standing trap for the
  next `#` field anyone adds.

### `tools/encryption.py` (new)

Holds `encrypt_secret` and `add_encryption_tools(mcp)`, registered in `server.py` beside the
other `add_*_tools` calls. A dedicated module rather than an addition to
`tools/components/tools.py`, whose `check_suitable` (`tools/components/utils.py:1093`) actively
refuses `keboola.data-apps` — the main caller here.

### `clients/data_science.py`

`DataAppConfig.Authorization.AppProxy.auth_providers` is already `list[dict[str, Any]]` (`:88`),
so no client model change is required. A typed `OidcProvider` model is deliberately **not**
introduced: the provider dict is a platform contract owned by `AppProxyDefinition.php`, and
mirroring it in a Pydantic model here would add a second place to update whenever the platform
adds a provider field, with no validation benefit the tool-level `OidcAuthConfig` does not
already give.

### Rejected alternatives

- **IdP presets** (`entra` / `google` / `okta` / `auth0` / `generic`, as sketched in the ticket).
  Each preset is a hardcoded URL template that rots when an IdP changes its scheme, and an LLM
  can already produce the issuer URL for any mainstream provider. `issuer_url` alone keeps the
  surface minimal and nothing to maintain.
- **Accepting a plaintext `client_secret` on the `oidc` param.** Would have made the field's
  contract ambiguous (cipher or plaintext?) and spread plaintext acceptance across four write
  paths. One rule, one entry point.
- **Skeleton-only, no `encrypt_secret`.** Leaves every OIDC app with a mandatory manual UI step,
  which is the thing the ticket set out to remove.

## Scope

**In scope** (`keboola/mcp-server`):

- `oidc` on `modify_streamlit_data_app` and `modify_python_js_data_app`, with merge-on-update.
- The `encrypt_secret` tool.
- Unconditional encryption on the python-js prod create path.
- `oidc_callback_url` on both output models.
- Tool docstrings and regenerated `TOOLS.md`.

**Out of scope:**

- kbagent `data-app create --auth oidc` (`keboola/cli`) — follow-up; consumes the shape this RFC
  defines and cannot be reviewed in this repo's PR.
- ai-kit `dataapp-development/references/authentication.md` — follow-up, same reason.
- `github` / `gitlab` / `jumpcloud` providers.
- Stack-level `enforcedAppsAuth` (ČS stacks already gate every app).
- Exposing `allowed_roles` in the UI.
- Opening the generic `create_config` / `update_config` tools to `keboola.data-apps`.

## Testing / Verification

Unit tests extend the existing parametrized tests in `tests/tools/test_data_apps.py` rather than
adding parallel functions, per the project's testing conventions:

| Case | Assertion |
| --- | --- |
| Create with `oidc` (both tools) | Stored `authorization` equals the documented shape byte-for-byte, via a fixture pinning the `AppProxyDefinition` contract |
| `basic-auth` → `oidc` update | `simpleAuth` provider gone, OIDC provider and rule present |
| `oidc` → `oidc` update, `client_secret` omitted | Stored cipher preserved; other supplied fields overwritten |
| `default` on an app with OIDC | `authorization` unchanged — explicit AI-1848 regression |
| Optional fields omitted | `logout_url` / `allowed_roles` keys absent, not null |
| Four rejection conditions | `ValueError`, each message naming the remedy |
| All create paths | `contains_plaintext_secrets()` is `False` on the payload handed to `create_data_app` — this is the test that fails today on python-js prod create |
| `encrypt_secret` | Cipher returned; `KBC::` input idempotent; plaintext absent from output and from raised errors |

`tox` must pass in full (pytest, ruff, check-tools-docs).

Manual verification on a dev stack, since neither unit tests nor CI can exercise a real IdP:

1. Create a python-js app with `basic-auth`; note `oidc_callback_url`.
2. Register the callback URL at an Entra test tenant; obtain `client_id` + secret.
3. `encrypt_secret(secret, component_id='keboola.data-apps')`.
4. Update with `authentication_type='oidc'`; confirm the stored config matches a UI-configured
   app and that the config version diff shows no plaintext.
5. `deploy_data_app`; confirm the browser is redirected to the IdP and login succeeds.
6. Re-run the update with `authentication_type='default'`; confirm the OIDC block survives.
7. Repeat steps 1–5 for a streamlit app.
