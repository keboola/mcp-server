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

One further problem has to be fixed before OIDC can ship, because OIDC is what makes it bite.

**Plaintext secret at rest on one write path.** `StorageClient._encrypt_secrets`
(`clients/storage.py:360`) encrypts every `#`-prefixed key fail-closed inside
`configuration_create` / `configuration_update`, so anything written through Storage is safe — that
covers the python-js *update* path (`tools/data_apps.py:1186`) and the streamlit update path.
Data-app **creates**, however, POST to the data-science API
(`data_science_client.create_data_app`), bypassing Storage entirely. The streamlit create path
compensates by encrypting explicitly (`tools/data_apps.py:673`), but python-js create only encrypts
inside `if git_block is not None:` (`tools/data_apps.py:1305`) — i.e. for drafts only. A python-js
**prod** create writes its config unencrypted.

No `#` key reaches that path today, so nothing leaks yet. `#client_secret` would be the first, which
turns a latent inconsistency into a live one: without the fix, creating a python-js data app with
OIDC in a single call stores the client secret in plaintext.

## Required Behavior

### `authentication_type='oidc'`

Both `modify_streamlit_data_app` and `modify_python_js_data_app` accept `'oidc'` and a new
optional structured `oidc` parameter.

| Field | Required | Behavior |
| --- | --- | --- |
| `client_id` | yes | Written verbatim. |
| `issuer_url` | yes | Written verbatim. No IdP presets — see *Rejected alternatives*. |
| `client_secret` | **on create** | Plaintext, encrypted via the Encryption API before any write — the same contract `_encrypt_secrets` applies to every other component config. An already-encrypted `KBC::…` value is accepted and passed through unchanged. Required whenever the app has no stored OIDC secret yet, so a create can never produce an app that validates and deploys but can never complete a login. On an update that already has one, omitting it preserves the stored cipher, which is what makes rotating `issuer_url` or `allowed_roles` possible without re-supplying the secret. |
| `logout_url` | no | Key omitted from the stored config when `None`. |
| `allowed_roles` | no | Key omitted from the stored config when `None`. |
| `provider_id` | no | Defaults to `'oidc'`. Internal key, not user-facing copy: it is what `auth_rules[].auth` references and what makes update-merge deterministic. See *Verified downstream contract* for why it stays invisible in the single-provider case. |

Rejections, each with a message naming the fix:

| Condition | Result |
| --- | --- |
| `authentication_type='oidc'` without `oidc` | `ValueError` |
| `oidc` passed with any other `authentication_type` | `ValueError` — never silently ignored |
| `client_secret` omitted with no stored cipher to preserve | `ValueError` — an OIDC app without a secret can never complete a login |
| `allowed_roles=[]` (empty list) | `ValueError` — rejected on both sides downstream; pass `None` to mean "no role requirement" |
| `authentication_type='oidc'` on a python-js **draft** | `ValueError` — a draft has its own slug *and* its own app id, therefore its own callback URL; OIDC belongs on the prod app |

Streamlit apps have no draft concept, so the last check is python-js only.

### Stored shape

`authorization` must pass `AppProxyDefinition` validation and be consumed correctly by apps-proxy.
Both were verified directly (see *Verified downstream contract*). Note the weaker claim: this RFC
does **not** assert the block is byte-identical to what the UI writes — the UI's writer was not
examined. If an app configured through the UI turns out to differ, the difference is worth
reconciling, but conformance to the validator and the proxy is the contract that actually matters.

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

### Verified downstream contract

The shape above is not inferred — it was traced end-to-end through the three services that consume
it. Recording the references here so a future change can re-check them cheaply.

| Stage | Where | What it establishes |
| --- | --- | --- |
| Validation | `job-queue-job-configuration` `AppProxyDefinition.php`, vendored into sandboxes-service | `id` + `type` required on every provider; `auth_rules` requires `type` and boolean `auth_required`; extra keys are preserved (`ignoreExtraKeys(false)`), which is how `client_id` / `#client_secret` / `issuer_url` survive. Provider `type` is **not** enum-constrained here — a typo passes validation and fails later in the proxy. |
| Golden fixture | sandboxes-service `tests/Unit/AppConfig/AppConfigValidatorTest.php` | A validator-passing OIDC provider written exactly as this RFC specifies: `id`, `type: oidc`, `client_id`, `#client_secret`, `issuer_url`, `logout_url`, `allowed_roles`. |
| Decrypt + rename | sandboxes-service `ProxyConfigProvider::provideProxyConfig` → `ProxyConfigNormalizer::normalizeConfigKeys` | `decryptForConfiguration` resolves `KBC::` ciphers, then keys are snake_case→camelCase with the `#` prefix stripped. `#client_secret` → `clientSecret`, `issuer_url` → `issuerUrl`, `auth_providers` → `authProviders`. |
| Consumption | keboola-as-code `appsproxy/dataapps/auth/provider/oidc.go` | Reads `clientId`, `clientSecret`, `issuerUrl`, `logoutUrl`, `allowedRoles`. `AllowedRoles` is `*[]string`: nil means no role requirement, and an **empty slice is an explicit error**. |
| Provider id visibility | keboola-as-code `authproxy/selector/selector.go` (`:108`, `selectorPageData`), `provider/base.go` (`Base.Name()`) | `Base.Name()` falls back to the provider id when `name` is empty and feeds the provider-selection page — **but that page is only rendered when an app has more than one provider**. With exactly one, the selector short-circuits and sends the user straight to the IdP. Switching an app to OIDC leaves exactly one provider, so the id is never displayed in the normal flow and needs no presentable default. |
| Callback URL | keboola-as-code `appsproxy/config/static.go` (`InternalPrefix = "/_proxy"`), `authproxy/oauthproxy/config.go`, `dataapps/api/config.go` (`Domain()`, `CookieDomain()`) | Redirect URL is `<scheme>://<slug>-<appId>.<hostname>/_proxy/callback` — i.e. `<deployment_url>/_proxy/callback`. One callback per **app**, not per provider. It depends on the app id, which does not exist before create: this is why the two-step flow is unavoidable and why a draft cannot share the prod app's OIDC block. |

keboola-operator is **not** in this path — it provisions Kubernetes workloads and handles no data-app
auth. Nothing there to change.

Two coupling rules fall out of the validator and must hold in `_build_authorization`:

- `auth_required` and `auth` are strictly paired — `auth_required: true` requires `auth`,
  `auth_required: false` requires its *absence*. The definition rejects either mismatch.
- Every provider id named in `auth_rules[].auth` must exist in `auth_providers`.

Both already hold in today's code; they are pinned as tests because `_build_authorization` rewrites
the function that produces them.

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
4. Update with `authentication_type='oidc'` and the `oidc` block, secret included.
5. `deploy_data_app`.

### Secret handling

`client_secret` is accepted as plaintext and encrypted before any write. This is not a weaker
position than requiring a pre-encrypted value — it is the same one, reached in one step instead of
two.

The encrypted-only rule was originally proposed to keep plaintext secrets out of the model's
context. It does not achieve that: whoever hands the secret to the agent has already put it in the
context window and the transcript, whether the agent then forwards it to a dedicated encryption
tool or straight to `modify_*_data_app`. An `encrypt_secret` tool was considered and dropped for
exactly this reason — it added a tool, a round trip and a field contract agents get wrong, in
exchange for no reduction in exposure.

What the design does guarantee is that the secret is **never at rest in plaintext**:
`_encrypt_secrets` (`clients/storage.py:360`) is fail-closed on every Storage write, and the two
data-app create paths that bypass Storage encrypt explicitly (which is what the fix below makes
true of the fourth path). Reads are safe by construction — the stored value is a `KBC::` cipher,
which is opaque.

Keeping the plaintext out of the agent's context entirely is a different requirement and this
design does not meet it. The only mechanism that would is out-of-band entry: the agent writes the
provider skeleton and a human types the secret into the UI. If a customer's compliance posture
demands that, it is a follow-up, and the indirection patterns worth evaluating then are a
reference-not-value parameter (`client_secret_env`, resolved by the MCP process — viable for local
stdio and kbagent, **not** for the hosted remote server, whose environment belongs to Keboola), a
Keboola Vault reference if the platform grows one for `app_proxy`, or dropping the static secret
altogether via federated credentials. That last is the real fix and the largest change: apps-proxy
reads a literal `ClientSecret` string today (`provider/oidc.go`).

## Resolution Strategy

### `tools/data_apps.py`

- `AuthenticationType` gains `'oidc'`.
- New `OidcAuthConfig(BaseModel)` with the fields above, a validator rejecting a non-`KBC::`
  `client_secret` (reusing `is_encrypted_value` from `clients/encryption.py`), and a validator
  rejecting an empty `allowed_roles` list.
- `_get_authorization(auth_with_password: bool)` (`:2134`) is replaced by
  `_build_authorization(authentication_type, oidc=None, existing=None)`. The two existing
  branches are preserved unchanged; the OIDC branch builds the block above and performs the merge
  against `existing`, upholding the two coupling rules recorded under *Verified downstream
  contract*. All four current call sites are updated: python-js create (`:1238`),
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
  configs that contain no secrets at all. This is **load-bearing, not hygiene**: with plaintext
  accepted on the tool surface, `#client_secret` is the first `#` value to travel this path, and
  without the fix a python-js prod create stores it in plaintext. It costs one extra API call on
  prod create.

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
- **Requiring a pre-encrypted `KBC::…` `client_secret`, with a new `encrypt_secret` tool to
  produce one.** Rejected: it does not reduce what reaches the model's context (see *Secret
  handling*), makes OIDC the only field in the product that refuses a plaintext `#` value, and adds
  a permanent tool plus a round trip to buy nothing.
- **Skeleton-only — write the provider, leave the secret to the UI.** Leaves every OIDC app with a
  mandatory manual step, which is the thing the ticket set out to remove. It remains the answer for
  a customer who requires the plaintext never reach the agent at all, and is noted as a follow-up.

## Scope

**In scope** (`keboola/mcp-server`):

- `oidc` on `modify_streamlit_data_app` and `modify_python_js_data_app`, with merge-on-update.
- Unconditional encryption on the python-js prod create path.
- `oidc_callback_url` on both output models.
- Tool docstrings and regenerated `TOOLS.md`.

**Out of scope:**

- kbagent `data-app create --auth oidc` (`keboola/cli`) — follow-up; consumes the shape this RFC
  defines and cannot be reviewed in this repo's PR.
- ai-kit `dataapp-development/references/authentication.md` — follow-up, same reason.
- `github` / `gitlab` / `jumpcloud` providers.
- Stack-level `enforcedAppsAuth` (ČS stacks already gate every app). Worth knowing while it stays
  out of scope: `ProxyConfigProvider` uses the stack's `ENFORCED_APPS_AUTH` config *instead of* the
  app's own `app_proxy` block when it is set, rather than merging. On such a stack an agent can
  write a perfectly valid OIDC block and see it silently ignored. The tools cannot currently detect
  this, so it is a documentation matter for the follow-up ai-kit page.
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
| Generated `auth_rules` | `auth_required: true` ⇒ `auth` present; `auth_required: false` ⇒ `auth` absent; every id in `auth` exists in `auth_providers` |
| Golden fixture | Generated provider dict matches the validator-passing fixture from sandboxes-service `AppConfigValidatorTest.php` field-for-field |
| Five rejection conditions | `ValueError`, each message naming the remedy |
| All four write paths | `contains_plaintext_secrets()` is `False` on the payload handed to `create_data_app` / `configuration_update` — the create-path case is the one that fails today on python-js prod create |
| Plaintext `client_secret` | Encrypted before the write on all four paths; an already-`KBC::` value passes through unchanged |

`tox` must pass in full (pytest, ruff, check-tools-docs).

Manual verification on a dev stack, since neither unit tests nor CI can exercise a real IdP:

1. Create a python-js app with `basic-auth`; note `oidc_callback_url`.
2. Register the callback URL at an Entra test tenant; obtain `client_id` + secret.
3. Update with `authentication_type='oidc'`, passing the secret as plaintext; confirm the stored
   config holds a `KBC::` cipher and that the config version diff shows no plaintext anywhere.
4. `deploy_data_app`; confirm the browser is redirected to the IdP and login succeeds.
5. Re-run the update with `authentication_type='default'`; confirm the OIDC block survives.
6. Repeat steps 1–4 for a streamlit app.
7. Create a python-js prod app with `oidc` in a single call; confirm the create-path encryption fix
   holds (this is the path that stores plaintext today).
