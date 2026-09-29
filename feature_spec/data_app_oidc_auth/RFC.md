# RFC: OIDC authentication for data apps

Linear: [AJDA-3349](https://linear.app/keboola/issue/AJDA-3349/mcpkbagent-create-and-update-data-apps-with-oidc-auth-today-only)

## Problem

Agents (Claude Code, Kai, kbagent) can create data apps with HTTP basic auth or no auth only.
`AuthenticationType` is `Literal['no-auth', 'basic-auth', 'default']` (`tools/data_apps.py:146`)
and `_get_authorization()` (`tools/data_apps.py:2134`) emits either a single `simpleAuth` password
provider or an empty provider list. There is no third option.

Enterprise customers (Groupon, ČS) require SSO, so the agent path stops halfway: a human opens the
UI and configures the whole OIDC block by hand. Worse, a later agent-driven update passing
`authentication_type='basic-auth'` overwrites that hand-made block and silently downgrades the app
to a shared password (AI-1848). The platform already accepts OIDC — no backend change is needed.

## Approach: skeleton + UI

The tools write everything about an OIDC provider **except the client secret**, and a human enters
the secret once in the existing UI form.

This is a deliberate trade. The alternative — accepting the secret as a tool argument — was
designed and then rejected: whoever hands a secret to an agent has already put it in the model's
context window and the transcript, and no amount of encrypting it afterwards takes it back out.
Encrypting on write protects the secret *at rest*; it does nothing about the context. For customers
whose reason for wanting SSO is a compliance posture, that distinction is the whole point. Keeping
the secret out of the agent entirely is the only design that delivers it.

The cost is honest and bounded: one manual step, on one field, once per app. Everything else the
ticket set out to remove — hand-building the provider block, the issuer URL, the auth rule, and the
silent basic-auth downgrade on update — is removed.

### Why this works: the UI form prefills

Verified in `kbc-ui` `modules/data-apps/legacy/components/AuthenticationSettings.tsx`:

- `resolveSavedMode()` reads `auth_providers[0].type`, so a provider written by the MCP with
  `type: 'oidc'` opens the form already in OIDC mode.
- `prepareProviderFormData()` seeds the form from the stored provider, so `client_id`, `issuer_url`
  and `logout_url` are prefilled; `resolveProvider()` even preselects the right IdP preset by
  matching `issuer_url` against each preset's domain.
- `isDisabled()` requires `client_id`, `#client_secret` and every non-optional preset input before
  Save is enabled. With the skeleton in place the only empty field is the secret, so the form
  *cannot* be saved half-configured and the human's job is unambiguous: type the secret, save.
- The form already renders the callback URL with "Register this as the redirect URI in your
  identity provider", built by `getDataAppCallbackUrl()` as `<app url>/_proxy/callback` — the same
  value this RFC returns from the tools.

## Required Behavior

### `authentication_type='oidc'`

Both `modify_streamlit_data_app` and `modify_python_js_data_app` accept `'oidc'` plus a structured
`oidc` parameter. There is no client-secret field on either tool.

| Field | Required | Behavior |
| --- | --- | --- |
| `client_id` | yes | Written verbatim. |
| `issuer_url` | yes | Written verbatim. No IdP presets — see *Rejected alternatives*. |
| `logout_url` | no | Key omitted when `None`. (The UI writes `''` rather than omitting; both are accepted downstream.) |
| `allowed_roles` | no | Written when set — but read the ordering constraint below, it is sharp. |
| `provider_id` | no | Defaults to **`'oidc-1'`**, the literal the UI hardcodes. Matching it is what keeps the MCP and the UI operating on the same provider instead of overwriting each other. |

Rejections, each with a message naming the fix:

| Condition | Result |
| --- | --- |
| `authentication_type='oidc'` without `oidc` | `ValueError` |
| `oidc` passed with any other `authentication_type` | `ValueError` — never silently ignored |
| `allowed_roles=[]` (empty list) | `ValueError` — rejected on both sides downstream; pass `None` for "no role requirement" |
| `authentication_type='oidc'` on a python-js **draft** | `ValueError` — a draft has its own slug *and* its own app id, therefore its own callback URL; OIDC belongs on the prod app |

Streamlit apps have no draft concept, so the last check is python-js only.

### `allowed_roles` and the UI round trip

The UI's OIDC branch rebuilds the provider from form state on every save, and its form has no
roles input — it writes exactly `id`, `type`, `client_id`, `#client_secret`, `issuer_url`,
`logout_url`. (Its GitLab and GitHub branches do write `allowed_roles`; OIDC does not.)

So **any `allowed_roles` the MCP writes is discarded the moment a human saves the form** — including
the mandatory save that enters the secret. Roles must therefore be applied *after* the secret, as a
second `authentication_type='oidc'` update, and any later UI save drops them again.

This is documented rather than designed around because the alternative is worse: silently dropping
`allowed_roles` from the tool surface would remove a capability the ticket asks for (ČS role
gating), and there is no way for the MCP to prevent a UI save. The tool docstring states the
ordering, and the create/update output repeats it whenever `allowed_roles` is set.

### Stored shape

```json
{
  "app_proxy": {
    "auth_providers": [
      {
        "id": "oidc-1",
        "type": "oidc",
        "client_id": "…",
        "issuer_url": "https://…",
        "logout_url": "https://…"
      }
    ],
    "auth_rules": [
      {"type": "pathPrefix", "value": "/", "auth_required": true, "auth": ["oidc-1"]}
    ]
  }
}
```

No `#client_secret` key — the UI adds it. The app validates and deploys in this state but cannot
complete a login until the secret is entered; that is the intended intermediate state, and the tool
output says so plainly.

### Verified downstream contract

Traced end-to-end rather than inferred. References recorded so a future change can re-check cheaply.

| Stage | Where | What it establishes |
| --- | --- | --- |
| UI writer | kbc-ui `AuthenticationSettings.tsx` (OIDC branch), `constants.ts` (`basicProxyAuthorization`) | Provider id is the literal `'oidc-1'`; fields are `id`, `type`, `client_id`, `#client_secret`, `issuer_url`, `logout_url`; the auth rule comes from `basicProxyAuthorization` with `auth` repointed. Its basic and no-auth blocks are byte-identical to what `_get_authorization` already emits. |
| Validation | `job-queue-job-configuration` `AppProxyDefinition.php`, vendored into sandboxes-service | `id` + `type` required per provider; `auth_rules` requires `type` and boolean `auth_required`; extra keys preserved (`ignoreExtraKeys(false)`), which is how `client_id` / `issuer_url` survive. Provider `type` is **not** enum-constrained — a typo passes validation and fails later in the proxy. |
| Golden fixture | sandboxes-service `tests/Unit/AppConfig/AppConfigValidatorTest.php` | A validator-passing OIDC provider with exactly these field names. |
| Decrypt + rename | sandboxes-service `ProxyConfigProvider::provideProxyConfig` → `ProxyConfigNormalizer::normalizeConfigKeys` | `decryptForConfiguration` resolves `KBC::` ciphers, then keys go snake_case→camelCase with the `#` prefix stripped: `#client_secret` → `clientSecret`, `issuer_url` → `issuerUrl`. |
| Consumption | keboola-as-code `appsproxy/dataapps/auth/provider/oidc.go` | Reads `clientId`, `clientSecret`, `issuerUrl`, `logoutUrl`, `allowedRoles`. `AllowedRoles` is `*[]string`: nil means no requirement, an **empty slice is an explicit error**. |
| Provider id visibility | keboola-as-code `authproxy/selector/selector.go` (`:108`), `provider/base.go` | The id labels the provider-selection page only when an app has 2+ providers; with one the selector short-circuits straight to the IdP. An OIDC app has one, so the id stays internal. |
| Callback URL | keboola-as-code `config/static.go`, `authproxy/oauthproxy/config.go`, `dataapps/api/config.go`; kbc-ui `getDataAppCallbackUrl()` | `<scheme>://<slug>-<appId>.<hostname>/_proxy/callback` — one per **app**, not per provider. Depends on the app id, which does not exist before create: this is why the two-step flow is unavoidable and why a draft cannot share the prod app's block. MCP and UI derive it identically. |

keboola-operator is **not** in this path — it provisions Kubernetes workloads and handles no
data-app auth.

Two coupling rules fall out of the validator and must hold in `_build_authorization`:

- `auth_required` and `auth` are strictly paired — `true` requires `auth`, `false` requires its
  *absence*. The definition rejects either mismatch.
- Every provider id in `auth_rules[].auth` must exist in `auth_providers`.

Both hold in today's code; they are pinned as tests because `_build_authorization` rewrites the
function that produces them.

### Update semantics

| `authentication_type` on update | Effect on the stored `authorization` |
| --- | --- |
| `'default'` | Preserved verbatim, including OIDC configured in the UI (AI-1848). Unchanged from today. |
| `'basic-auth'` / `'no-auth'` | Overwritten, as today. |
| `'oidc'`, no existing OIDC provider | Fresh skeleton written. |
| `'oidc'`, existing provider with the same `provider_id` | Merged field-by-field: only keys present in the new `oidc` param overwrite. |

**The merge must preserve `#client_secret` verbatim.** In this design the UI is the only writer of
that key, so a merge that dropped it would destroy the one thing the human was asked to supply and
silently break every login. This is the single most important invariant in the change and gets a
dedicated test.

Non-OIDC providers (`simpleAuth`) are dropped when switching to OIDC: it replaces the scheme rather
than stacking on it, matching the UI.

### Output

Both output models gain:

- `oidc_callback_url: str | None` — `f'{deployment_url}/_proxy/callback'`, whenever the app uses
  OIDC, on update as well as create.
- `oidc_setup_required: bool` — true when the stored provider has no `#client_secret`. The agent
  must not report success without relaying this; an app in that state deploys and then fails every
  login, which is otherwise indistinguishable from a misconfigured IdP.

`change_summary` carries the human-readable next step, including the app's UI link (the links
manager already produces it) and, when `allowed_roles` was set, the ordering warning above.

The docstrings document the full sequence:

1. Create with `authentication_type='basic-auth'`.
2. Read `oidc_callback_url`; register it as a redirect URI at the IdP; obtain `client_id`.
3. Update with `authentication_type='oidc'` and the `oidc` block → skeleton written.
4. **Human opens the app's Authentication section in the UI, types the client secret, saves.**
5. `deploy_data_app`.
6. Only if using `allowed_roles`: a second `authentication_type='oidc'` update to reapply them.

## Resolution Strategy

### `tools/data_apps.py`

- `AuthenticationType` gains `'oidc'`.
- New `OidcAuthConfig(BaseModel)`: the fields above, a validator rejecting an empty `allowed_roles`,
  and no secret field.
- `_get_authorization(auth_with_password: bool)` (`:2134`) is replaced by
  `_build_authorization(authentication_type, oidc=None, existing=None)`. The two existing branches
  are preserved unchanged; the OIDC branch builds the skeleton and performs the
  secret-preserving merge against `existing`, upholding the two coupling rules above. All four call
  sites are updated: python-js create (`:1238`), python-js update (`:1601`), streamlit create via
  `_build_data_app_config` (`:1879`), streamlit update via `_update_existing_data_app_config`
  (`:1913`).
- `_uses_basic_authentication()` (`:2261`) needs no change — it matches `simpleAuth` and already
  returns `False` for an OIDC app, the correct input to `get_data_app_links`.
- New `_reject_oidc_on_draft()` beside `_reject_no_auth_on_draft()` (`:158`), called at the same two
  python-js draft checkpoints (`:1156`, `:1175`).
- `ModifiedDataAppOutput` (`:401`) and `ModifiedPythonJsDataAppOutput` (`:411`) gain
  `oidc_callback_url` and `oidc_setup_required`.

### Independent hygiene fix (splittable)

python-js prod create posts to the data-science API and bypasses Storage, and its
`encryption_client.encrypt` call sits inside `if git_block is not None:` (`:1305`) — so it only runs
for drafts. Streamlit create, on the same DSAPI route, encrypts unconditionally (`:673`); both
update paths are covered fail-closed by `StorageClient._encrypt_secrets` (`clients/storage.py:360`).
Dedenting that call closes the gap.

**This is no longer load-bearing for OIDC** — under skeleton + UI the MCP never writes a `#` key on
that path, and the UI's secret entry goes through Storage, which encrypts fail-closed. It is a
latent bug that would bite the next `#` field anyone adds, kept here because this change is already
in the function. It can be split into its own PR without affecting the rest.

### `clients/data_science.py`

`DataAppConfig.Authorization.AppProxy.auth_providers` is already `list[dict[str, Any]]` (`:88`), so
no client model change is required. A typed `OidcProvider` model is deliberately **not** introduced:
the provider dict is a platform contract owned by `AppProxyDefinition.php`, and mirroring it here
would add a second place to update whenever the platform adds a field, with no validation benefit
`OidcAuthConfig` does not already give.

### Rejected alternatives

- **Accepting the client secret as a tool argument** (plaintext encrypted on write, or a
  pre-encrypted `KBC::…` value produced by a new `encrypt_secret` tool). Both were designed in full
  before being rejected. Neither keeps the secret out of the model's context, which is the only
  property that matters here; the cipher variant additionally made OIDC the one field in the
  product refusing a plaintext `#` value, and added a permanent tool and a round trip to buy
  nothing. If the context exposure is ever deemed acceptable, accepting plaintext and letting
  `_encrypt_secrets` handle it is the right shape — it is what every other component config does.
- **IdP presets** (`entra` / `google` / `okta` / `auth0` / `generic`). Each is a hardcoded URL
  template that rots when an IdP changes its scheme, and an LLM can produce the issuer URL for any
  mainstream provider. Note the UI *does* carry presets (`OIDC_PROVIDERS`), so an agent-written
  `issuer_url` still preselects the right one when the human opens the form — the ergonomics are
  had without the maintenance.
- **A `client_secret_env` reference resolved by the MCP process.** Works for a locally run stdio
  server and for kbagent, but not for the hosted remote server, whose environment is Keboola's, not
  the user's. A parameter that silently does nothing for most users is worse than no parameter.
  Revisit if the platform grows a Vault reference for `app_proxy`, or federated credentials that
  remove the static secret altogether — apps-proxy reads a literal `ClientSecret` today.

## Scope

**In scope** (`keboola/mcp-server`):

- `oidc` on `modify_streamlit_data_app` and `modify_python_js_data_app`, with secret-preserving
  merge on update.
- `oidc_callback_url` and `oidc_setup_required` on both output models.
- The python-js prod-create encryption fix (splittable — see above).
- Tool docstrings and regenerated `TOOLS.md`.

**Out of scope:**

- kbagent `data-app create --auth oidc` (`keboola/cli`) — follow-up; consumes the shape this RFC
  defines and cannot be reviewed in this repo's PR.
- ai-kit `dataapp-development/references/authentication.md` — follow-up, same reason.
- `github` / `gitlab` / `jumpcloud` providers.
- Exposing `allowed_roles` in the UI. Doing so would remove the round-trip footgun documented above
  and is the natural follow-up if ČS actually uses roles.
- Stack-level `enforcedAppsAuth`. Worth knowing while it stays out of scope: `ProxyConfigProvider`
  uses the stack's `ENFORCED_APPS_AUTH` config *instead of* the app's own `app_proxy` block when set,
  rather than merging. On such a stack an agent can write a valid OIDC block and see it silently
  ignored, with nothing in the tools able to detect it. A documentation matter for the ai-kit page.
- Opening the generic `create_config` / `update_config` tools to `keboola.data-apps`.

## Testing / Verification

Unit tests extend the existing parametrized tests in `tests/tools/test_data_apps.py` rather than
adding parallel functions, per the project's testing conventions:

| Case | Assertion |
| --- | --- |
| Create with `oidc` (both tools) | Stored block equals the documented skeleton; `provider_id` defaults to `'oidc-1'`; no `#client_secret` key present |
| `basic-auth` → `oidc` update | `simpleAuth` gone, OIDC provider and rule present |
| **`oidc` → `oidc` update with a stored `#client_secret`** | **Cipher preserved verbatim; other supplied fields overwritten** — the critical invariant |
| `default` on an app with OIDC | `authorization` unchanged — explicit AI-1848 regression |
| `allowed_roles` set | Written to the provider; `change_summary` carries the UI-round-trip warning |
| Optional fields omitted | `logout_url` / `allowed_roles` keys absent, not null |
| Generated `auth_rules` | `auth_required: true` ⇒ `auth` present; `false` ⇒ `auth` absent; every id in `auth` exists in `auth_providers` |
| Golden fixture | Generated provider matches the sandboxes-service `AppConfigValidatorTest.php` fixture field-for-field, and the UI writer's field set |
| `oidc_setup_required` | True while no `#client_secret` is stored, false once one is |
| Four rejection conditions | `ValueError`, each message naming the remedy |
| Encryption hygiene fix | `contains_plaintext_secrets()` is `False` on the payload handed to `create_data_app` — fails today on python-js prod create |

`tox` must pass in full (pytest, ruff, check-tools-docs).

Manual verification on a dev stack, since neither unit tests nor CI can exercise a real IdP:

1. Create a python-js app with `basic-auth`; note `oidc_callback_url`.
2. Register the callback URL at an Entra test tenant; obtain `client_id`.
3. Update with `authentication_type='oidc'`; confirm `oidc_setup_required` is true and the stored
   block matches the skeleton.
4. Open the app's Authentication section in the UI: confirm the form opens in OIDC mode with the
   preset preselected, `client_id` / `issuer_url` prefilled, and Save disabled until the secret is
   typed. Enter the secret and save.
5. `deploy_data_app`; confirm the browser is redirected to the IdP and login succeeds.
6. Re-run the update with `authentication_type='oidc'`, changing only `issuer_url`; confirm login
   still works — i.e. the stored cipher survived the merge.
7. Re-run with `authentication_type='default'`; confirm the whole block survives.
8. Repeat 1–5 for a streamlit app.
