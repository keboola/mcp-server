# RFC: OIDC authentication for data apps

Linear: [AJDA-3349](https://linear.app/keboola/issue/AJDA-3349/mcpkbagent-create-and-update-data-apps-with-oidc-auth-today-only)

## Problem

Agents (Claude Code, Kai, kbagent) can create data apps with HTTP basic auth or no auth only.
`AuthenticationType` is `Literal['no-auth', 'basic-auth', 'default']` (`tools/data_apps.py:170`)
and `_get_authorization()` (`tools/data_apps.py:2755`) emits either a single `simpleAuth` password
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

Checked against the UI's data app authentication form:

- A stored provider with `type: 'oidc'` opens the form already in OIDC mode.
- `client_id`, `issuer_url` and `logout_url` are prefilled from the stored provider, and the IdP
  preset is preselected by matching `issuer_url`.
- Save stays disabled until `client_id`, `#client_secret` and every required preset input are set.
  With the skeleton in place the only empty field is the secret, so the form *cannot* be saved
  half-configured and the human's job is clear: type the secret, save.
- The form already shows the callback URL (`<app url>/_proxy/callback`) as the redirect URI to
  register — the same value this RFC returns from the tools.

### Opening the UI form: URL-mode elicitation

The human step is delivered with URL-mode elicitation, reusing `UrlElicitationMiddleware` and the
helpers in `elicitation.py` (see [url_elicitation](../url_elicitation/RFC.md)). When the stored
provider has no `#client_secret`, the tool asks the client to open the app's configuration page
(`get_data_app_config_link`), with a message telling the user to enter the client secret in the
Authentication section and save.

The secret goes from the user's browser to the UI, and from the UI to Storage. It never passes
through the MCP client, the server or the model — the property this design is built on.

| Client / protocol | Behavior after the skeleton is written |
| --- | --- |
| URL-capable, 2026-07-28 | The call returns an `InputRequiredResult` keyed `open_url`. The client opens the page after consent and retries; the retry re-runs the same update — safe, because writing the same skeleton is idempotent and the merge keeps any `#client_secret` already saved — and returns the normal output. |
| URL-capable, older protocol with a back-channel | `open_url_in_client()` during the call, then the normal output. |
| Not URL-capable, or no way to ask | The normal output only; `change_summary` carries the link. |

Unlike the Data Streams unlock, the tool call succeeds, so this is not a `UrlActionRequiredError`.
A small success-path helper sits next to the middleware and shares `open_url_input_request()` and
`open_url_in_client()`. Like the unlock flow, it does not wait for the user to finish
(`URLElicitationRequiredError` / -32042 is not used): the agent cannot know when the secret is
saved, so `oidc_setup_required` in the next tool output is the source of truth.

Form-mode elicitation is not an option: the MCP spec forbids requesting sensitive data with it, and
the typed secret would pass through the client.

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

### Downstream contract

No change is needed outside this repo — the platform already accepts this shape. The facts below
were checked against the UI, the config validator and apps-proxy, and the design depends on them:

- **UI writer:** the provider id is the literal `'oidc-1'`; fields are `id`, `type`, `client_id`,
  `#client_secret`, `issuer_url`, `logout_url`. Its basic and no-auth blocks are identical to what
  `_get_authorization` already emits.
- **Validation:** `id` and `type` are required per provider, `auth_rules` needs `type` and a boolean
  `auth_required`, and extra keys such as `client_id` / `issuer_url` are kept. Provider `type` is
  not an enum — a typo passes validation and fails later in the proxy.
- **Decrypt + rename:** `KBC::` ciphers are decrypted, then keys go snake_case → camelCase with the
  `#` prefix removed (`#client_secret` → `clientSecret`).
- **Consumption:** apps-proxy reads `clientId`, `clientSecret`, `issuerUrl`, `logoutUrl`,
  `allowedRoles`. A missing `allowedRoles` means no requirement; an **empty list is an error**.
- **Provider id:** shown only on the provider-selection page, which appears with 2+ providers. An
  OIDC app has one, so the id stays internal.
- **Callback URL:** `<scheme>://<slug>-<appId>.<hostname>/_proxy/callback` — one per **app**, not
  per provider. It depends on the app id, which does not exist before create: this is why the
  two-step flow is needed and why a draft cannot share the prod app's block.

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
manager already produces it, and it is the page offered via URL elicitation) and, when
`allowed_roles` was set, the ordering warning above.

The docstrings document the full sequence:

1. Create with `authentication_type='basic-auth'`.
2. Read `oidc_callback_url`; register it as a redirect URI at the IdP; obtain `client_id`.
3. Update with `authentication_type='oidc'` and the `oidc` block → skeleton written; URL-capable
   clients are asked to open the app's configuration page.
4. **Human opens the app's Authentication section in the UI, types the client secret, saves.**
5. `deploy_data_app`.
6. Only if using `allowed_roles`: a second `authentication_type='oidc'` update to reapply them.

## Resolution Strategy

### `tools/data_apps.py`

- `AuthenticationType` gains `'oidc'`.
- New `OidcAuthConfig(BaseModel)`: the fields above, a validator rejecting an empty `allowed_roles`,
  and no secret field.
- `_get_authorization(auth_with_password: bool)` (`:2755`) is replaced by
  `_build_authorization(authentication_type, oidc=None, existing=None)`. The two existing branches
  are preserved unchanged; the OIDC branch builds the skeleton and performs the
  secret-preserving merge against `existing`, upholding the two coupling rules above. All four call
  sites are updated: python-js create (`:1509`), python-js update via
  `_update_existing_code_data_app_config` (`:1941`), streamlit create via `_build_data_app_config`
  (`:2405`), streamlit update via `_update_existing_data_app_config` (`:2439`).
- `_uses_basic_authentication()` (`:2980`) needs no change — it matches `simpleAuth` and already
  returns `False` for an OIDC app, the correct input to `get_data_app_links`.
- New `_reject_oidc_on_draft()` beside `_reject_no_auth_on_draft()` (`:182`), called at the same two
  python-js draft checkpoints (`:1365`, `:1388`).
- `ModifiedDataAppOutput` (`:518`) and `ModifiedPythonJsDataAppOutput` (`:528`) gain
  `oidc_callback_url` and `oidc_setup_required`.

### `elicitation.py`

- A success-path helper that offers the app's configuration page while `oidc_setup_required` is
  true, built on the existing `open_url_input_request()` / `open_url_in_client()`. On the
  2026-07-28 retry (`open_url` in `ctx.input_responses`) it does not ask again.

### Independent hygiene fix (splittable)

python-js prod create posts to the data-science API and bypasses Storage, and its
`encryption_client.encrypt` call sits inside `if git_block is not None:` (`:1576`) — so it only runs
for drafts. Streamlit create, on the same DSAPI route, encrypts unconditionally (`:824`); both
update paths are covered fail-closed by `StorageClient._encrypt_secrets` (`clients/storage.py:360`).
Dedenting that call closes the gap.

**This is no longer load-bearing for OIDC** — under skeleton + UI the MCP never writes a `#` key on
that path, and the UI's secret entry goes through Storage, which encrypts fail-closed. It is a
latent bug that would bite the next `#` field anyone adds, kept here because this change is already
in the function. It can be split into its own PR without affecting the rest.

### `clients/data_science.py`

`DataAppConfig.Authorization.AppProxy.auth_providers` is already `list[dict[str, Any]]` (`:88`), so
no client model change is required. A typed `OidcProvider` model is deliberately **not** introduced:
the provider dict is a platform contract owned by the platform's config validator, and mirroring it here
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
  mainstream provider. Note the UI *does* carry presets, so an agent-written
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
- URL elicitation of the app's configuration page while the secret is missing.
- The python-js prod-create encryption fix (splittable — see above).
- Tool docstrings and regenerated `TOOLS.md`.

**Out of scope:**

- OIDC support in kbagent and the ai-kit data app docs — follow-ups in their own repos, built on
  the shape this RFC defines.
- `github` / `gitlab` / `jumpcloud` providers.
- Exposing `allowed_roles` in the UI. Doing so would remove the round-trip footgun documented above
  and is the natural follow-up if ČS actually uses roles.
- Stack-level `enforcedAppsAuth`. Worth knowing while it stays out of scope: the platform
  uses the stack's enforced auth config *instead of* the app's own `app_proxy` block when set,
  rather than merging. On such a stack an agent can write a valid OIDC block and see it silently
  ignored, with nothing in the tools able to detect it. A documentation matter for the ai-kit page.
- Setting OIDC through the generic `create_config` / `update_config` tools. They already send
  agents to the data-app tools for `keboola.data-apps`, and that stays: all data-app auth goes
  through `modify_streamlit_data_app` / `modify_python_js_data_app`.

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
| Golden fixture | Generated provider matches the platform validator's OIDC fixture and the UI writer's field set, field for field |
| `oidc_setup_required` | True while no `#client_secret` is stored, false once one is |
| URL elicitation (in `tests/test_elicitation.py` style, real in-memory server) | Secret missing: URL-capable client asked once to open the config link, retry returns the normal output; secret stored: not asked; plain client: not asked, link in `change_summary` |
| Four rejection conditions | `ValueError`, each message naming the remedy |
| Encryption hygiene fix | `contains_plaintext_secrets()` is `False` on the payload handed to `create_data_app` — fails today on python-js prod create |

`tox` must pass in full (pytest, ruff, check-tools-docs).

Manual verification on a dev stack, since neither unit tests nor CI can exercise a real IdP:

1. Create a python-js app with `basic-auth`; note `oidc_callback_url`.
2. Register the callback URL at an Entra test tenant; obtain `client_id`.
3. Update with `authentication_type='oidc'`; confirm `oidc_setup_required` is true and the stored
   block matches the skeleton.
4. In Claude Code, confirm the "wants to open a URL" prompt appears and opens the app's
   configuration page. In its Authentication section confirm the form opens in OIDC mode with the
   preset preselected, `client_id` / `issuer_url` prefilled, and Save disabled until the secret is
   typed. Enter the secret and save.
5. `deploy_data_app`; confirm the browser is redirected to the IdP and login succeeds.
6. Re-run the update with `authentication_type='oidc'`, changing only `issuer_url`; confirm login
   still works — i.e. the stored cipher survived the merge.
7. Re-run with `authentication_type='default'`; confirm the whole block survives.
8. Repeat 1–5 for a streamlit app.
