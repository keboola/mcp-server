# OIDC Authentication for Data Apps — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let `modify_python_js_data_app` and `modify_streamlit_data_app` switch an existing prod app to OIDC
(everything except the client secret), report the callback URL and whether setup is complete, and offer the UI
page where the human types the secret.

**Architecture:** One pure builder, `_build_authorization`, owns every `authorization` block the tools write,
including the merge with an existing OIDC provider. Both tools validate `oidc` arguments up front, write through
the builder on update only, and compute two new output fields from the config they wrote. URL elicitation reuses
`UrlElicitationMiddleware`: a tool records a "please open this page" request on success, and the middleware
asks the client after the tool returns.

**Tech Stack:** Python 3.11, FastMCP 4.x, pydantic v2, pytest + pytest-asyncio, tox (pytest, ruff, check-tools-docs).

**Spec:** `feature_spec/data_app_oidc_auth/RFC.md` (read it first). The RFC already includes the fixes from
the *Architecture review* below.

---

## Architecture review of the RFC

The RFC is sound in its main choice (skeleton + UI, secret never in the agent). Reading it against the code on
`main` turned up two defects that would ship broken behavior, and four places to simplify.

### Defects (must fix)

1. **The callback URL formula fails exactly when it is needed.** The RFC says
   `oidc_callback_url = f'{deployment_url}/_proxy/callback'`. The data-science API reports no URL until the
   app's first run (`_app_url` docstring, `tools/data_apps.py:2914`), and step 2 of the setup flow reads the
   callback URL right after a `basic-auth` create — before any deploy. `deployment_url` is `None` there.
   **Fix:** build it from the stored slug and app id, the same formula the operator uses:
   `https://{slug}-{data_app_id}.hub.{client.hostname_suffix}/_proxy/callback`.
2. **A rename moves the slug and silently breaks the registered callback URL.** The callback host contains the
   slug. Today:
   - Streamlit update rewrites the slug from `name` on every call (`_update_existing_data_app_config`,
     `:2468`), and `name` is a required argument.
   - python-js moves the slug with the name while the app was never deployed (`_resolve_slug_update`) — and the
     whole OIDC setup (steps 1–5) happens before the first deploy.

   Either way the IdP keeps the old redirect URI and every login fails with a redirect-URI mismatch.
   **Fix:** while the config being written has an OIDC provider, never move the slug implicitly. An explicit
   python-js `slug` stays allowed, but `change_summary` tells the agent to register the new `oidc_callback_url`.

### Simplifications (recommended)

3. **Drop the `provider_id` argument.** The UI hardcodes `'oidc-1'` and rebuilds the provider on every save, so
   any other id is overwritten by the mandatory secret save. The argument can only create a mismatch. Always
   write `'oidc-1'`.
4. **Merge with the existing OIDC provider by `type`, not by id.** Hand-made or older configs exist with other
   ids (the repo's own fixtures use `'oidc'`). Matching on `type == 'oidc'` and normalizing the id to `'oidc-1'`
   keeps the stored secret in every case; matching on id would drop it for those apps.
5. **Define "omitted" vs "null" for the optional fields.** The RFC says "only keys present in the new `oidc`
   param overwrite", which leaves no way to remove `allowed_roles` or `logout_url`. Use pydantic
   `model_fields_set`: omitted = keep the stored value, explicit `null` = remove the key.
6. **Drop the "independent hygiene fix" from this PR.** On python-js prod create the config has no `#` key:
   the git block (with `#password`) exists only on drafts, and the legacy `WORKSPACE_ID` secret is not
   `#`-prefixed. The gap is real only for a future `#` field, and with OIDC-on-create rejected this change adds
   none. YAGNI; it can be its own PR if a `#` field is ever added.

### Elicitation mechanics (clarification, not a defect)

`UrlElicitationMiddleware` handles errors only. Here the tool succeeds, so the tool needs a way to say "offer
this page" without changing its return type. Use a request-scoped holder: the middleware puts a fresh list in a
`ContextVar` before `call_next`, the tool appends `(message, url)` to it, and after a successful `call_next` the
middleware asks the client:

- 2026-07-28 protocol, no `open_url` answer yet: return `open_url_input_request(...)` instead of the result.
  The client retries; the retry re-runs the update (idempotent — same skeleton, merge keeps any saved secret),
  sees the `open_url` answer and returns the normal output. Cost: one extra config version per OIDC update on
  these clients. Accepted.
- Older protocol: `open_url_in_client(...)`, then the normal result.
- Not URL-capable: the normal result; `change_summary` carries the link.

A mutable holder (not `ContextVar.set` from the tool) is needed so the write is visible to the middleware even
if FastMCP runs the tool in a copied context.

### Process notes

- CONTRIBUTING requires the RFC to be agreed **before** the implementation PR, so implement on a new branch
  after https://github.com/keboola/mcp-server/pull/727 merges.
- New capability → **minor** version bump.
- Kai uses the data-app tools; ship with an `agent-vX.Y.Z` tag as well as `vX.Y.Z` (a `vX.Y.Z`-only release
  leaves Kai on the old tools).

---

## Global Constraints

- No Linear ticket IDs in code, tests, docstrings or markdown docs (public repo). Commit messages and PR text only.
- Comments only for a non-obvious *why*. No section dividers, no change history.
- Tests: extend existing parametrized tests where one fits; parameter names as a tuple of strings.
- Provider id is always the literal `'oidc-1'`.
- OIDC provider fields written by the MCP: `id`, `type` (`'oidc'`), `client_id`, `issuer_url`, optional
  `logout_url`, optional `allowed_roles`. Never `#client_secret`.
- Auth rule for OIDC: `{'type': 'pathPrefix', 'value': '/', 'auth_required': True, 'auth': ['oidc-1']}`.
- `allowed_roles=[]` is rejected (apps-proxy treats an empty list as an error).
- Callback URL: `https://{slug}-{data_app_id}.hub.{hostname_suffix}/_proxy/callback`.
- `tox` passes in full before every push.

## Review Focus

1. **Rename during setup:** an agent passes a new `name` in the same call as `authentication_type='oidc'`, or in
   a later update of an OIDC app. Expect: slug unchanged, callback URL unchanged. → Task 4 test.
2. **UI-made OIDC block with a different provider id and a saved secret**, e.g. provider id `'oidc'` with
   `#client_secret: 'KBC::ProjectSecure::…'`, then an `'oidc'` update. Expect: secret kept, id becomes
   `'oidc-1'`, rule points at `'oidc-1'`, `oidc_setup_required` false. → Task 1 test.
3. **UI's empty `logout_url: ''` and keys the MCP does not know** (anything the UI or platform adds later).
   Expect: kept verbatim through a merge. → Task 1 test.
4. **`#client_secret: ''`** (blank, not missing). Expect: `oidc_setup_required` true. → Task 1 test.
5. **Retry on the 2026-07-28 protocol:** the second round must not ask again and must return the normal output
   with the same `oidc_callback_url`. → Task 5 test.

---

### Task 1: Authorization builder and OIDC model

**Files:**
- Modify: `src/keboola_mcp_server/tools/data_apps.py` (`AuthenticationType` `:170`, `_get_authorization` `:2834`)
- Test: `tests/tools/test_data_apps.py` (replace `test_get_authorization_mapping` `:135`, extend `test_is_authorized_behavior` `:147`)

**Interfaces:**
- Produces:
  - `AuthenticationType = Literal['no-auth', 'basic-auth', 'oidc', 'default']`
  - `OIDC_PROVIDER_ID = 'oidc-1'`
  - `class OidcAuthConfig(BaseModel)`: `client_id: str`, `issuer_url: str`, `logout_url: str | None = None`,
    `allowed_roles: list[str] | None = None`; field validator rejects `allowed_roles == []` with a message
    that says to pass `null` for "no role requirement". Field descriptions are agent-facing (they land in
    TOOLS.md).
  - `_build_authorization(authentication_type: Literal['no-auth', 'basic-auth', 'oidc'], oidc: OidcAuthConfig | None = None, existing: Mapping[str, Any] | None = None) -> dict[str, Any]`
    — `existing` is the stored `authorization` block. `'basic-auth'` / `'no-auth'` return exactly what
    `_get_authorization(True/False)` returns today. `'oidc'` returns one provider + one rule (Global
    Constraints): it starts from the first existing provider with `type == 'oidc'` (all its keys, including
    `#client_secret` and unknown keys), sets `id='oidc-1'`, `type='oidc'`, `client_id`, `issuer_url`, then for
    each of `logout_url` / `allowed_roles` in `oidc.model_fields_set`: value → set, `None` → delete key. Fields
    not in `model_fields_set` keep the stored value. Non-OIDC providers are dropped.
  - `_oidc_provider(authorization: Mapping[str, Any]) -> Mapping[str, Any] | None` — first provider with `type == 'oidc'`.
  - `_oidc_setup_required(authorization: Mapping[str, Any]) -> bool` — True when an OIDC provider exists and its `#client_secret` is missing or blank.
  - `_get_authorization` is deleted; its three callers move to `_build_authorization` in Tasks 2–3
    (`_build_data_app_config` keeps `authentication_type in ('basic-auth', 'default')` → `'basic-auth'`).

- [ ] **Step 1: Write the failing tests** — one parametrized `test_build_authorization` with cases:
  - `('basic-auth', None, None)` → simpleAuth block (today's literal)
  - `('no-auth', None, None)` → empty providers, `auth_required: False`, no `auth` key
  - `('oidc', OidcAuthConfig(client_id='c', issuer_url='https://i'), None)` → `[{'id': 'oidc-1', 'type': 'oidc', 'client_id': 'c', 'issuer_url': 'https://i'}]`, no `logout_url` / `allowed_roles` / `#client_secret` keys
  - existing `{'id': 'oidc', 'type': 'oidc', 'client_id': 'old', 'issuer_url': 'https://old', '#client_secret': 'KBC::ProjectSecure::x', 'logout_url': '', 'extra': 1}` + new `client_id='c'` → id `'oidc-1'`, `client_id == 'c'`, `#client_secret`, `logout_url == ''` and `extra` kept
  - existing with `allowed_roles: ['a']` + new with `allowed_roles=None` passed explicitly → key removed; new without `allowed_roles` → `['a']` kept
  - existing simpleAuth + `'oidc'` → simpleAuth gone

  Plus, for every case: `auth_required is True` ⇔ `'auth' in rule`, and every id in `rule['auth']` is a provider id.
  Plus `test_oidc_auth_config_rejects_empty_roles` (`pytest.raises(ValidationError, match='null')`), and
  `_oidc_setup_required` cases: no OIDC provider → False; secret missing → True; `''` → True; cipher → False.
- [ ] **Step 2: Run** `pytest tests/tools/test_data_apps.py -k "build_authorization or oidc" -v` — expect FAIL (import errors).
- [ ] **Step 3: Implement** the Interfaces above; delete `_get_authorization`; update `test_is_authorized_behavior` to use `_build_authorization`.
- [ ] **Step 4: Run** the same command — expect PASS. Callers of `_get_authorization` will fail to import until Tasks 2–3; keep this task's commit green by switching the three call sites mechanically to `_build_authorization('basic-auth' if <old bool> else 'no-auth')` here.
- [ ] **Step 5: Run** `pytest tests/tools/test_data_apps.py -q` — expect PASS.
- [ ] **Step 6: Commit** `feat: add OIDC authorization builder for data apps`

### Task 2: `oidc` on python-js updates

**Files:**
- Modify: `src/keboola_mcp_server/tools/data_apps.py` — `modify_python_js_data_app` (`:1057`; argument checks `:1355-1373`, draft check `:1395`), `_update_existing_code_data_app_config` (`:1904`)
- Test: `tests/tools/test_data_apps.py` — extend `test_modify_python_js_data_app_no_auth_rejected_only_on_drafts` (`:1574`) into a rejection test; add update cases next to `test_update_existing_code_data_app_config_default_auth_preserves_existing` (`:2452`)

**Interfaces:**
- Consumes: Task 1 `AuthenticationType`, `OidcAuthConfig`, `_build_authorization`.
- Produces:
  - tool argument `oidc: OidcAuthConfig | None = None` (description: required with `'oidc'`, update only, no secret field — the human enters it in the UI)
  - `_validate_oidc_args(authentication_type: AuthenticationType, oidc: OidcAuthConfig | None, *, is_create: bool) -> None` — raises `ValueError` for: `'oidc'` without `oidc`; `oidc` with another type; `'oidc'` on create. Each message names the fix (create with `'basic-auth'` first; pass `authentication_type='oidc'`; …).
  - `_reject_oidc_on_draft(authentication_type: AuthenticationType) -> None` beside `_reject_no_auth_on_draft`
  - `_update_existing_code_data_app_config(..., oidc: OidcAuthConfig | None = None)` — non-`'default'` types go through `_build_authorization(authentication_type, oidc, existing_config.get('authorization'))`.

- [ ] **Step 1: Write the failing tests**
  - Rejections (parametrized, both assert no `create_data_app` / `configuration_update` call): `'oidc'` without `oidc`; `oidc` with `'basic-auth'`; `'oidc'` on create; `'oidc'` on draft create (`parent_configuration_id`); `'oidc'` on draft update. Match on a word from each message.
  - `_update_existing_code_data_app_config` with `'oidc'` on a basic-auth config → OIDC skeleton; on a config holding `#client_secret` → cipher kept verbatim (the RFC's critical invariant).
- [ ] **Step 2: Run** `pytest tests/tools/test_data_apps.py -k "python_js and (oidc or rejected)" -v` — expect FAIL.
- [ ] **Step 3: Implement** — call `_validate_oidc_args` first thing in the tool (before any client call), `_reject_oidc_on_draft` at both draft checkpoints, and pass `oidc` to `_update_existing_code_data_app_config`.
- [ ] **Step 4: Run** the same command — expect PASS.
- [ ] **Step 5: Commit** `feat: accept OIDC on python-js data app updates`

### Task 3: `oidc` on Streamlit updates

**Files:**
- Modify: `src/keboola_mcp_server/tools/data_apps.py` — `modify_streamlit_data_app` (`:689`), `modify_streamlit_data_app_internal` (`:910`), `_update_existing_data_app_config` (`:2457`), `_build_data_app_config` (`:2429`)
- Test: `tests/tools/test_data_apps.py` — extend `test_update_existing_data_app_config_basic_auth_overwrites_oidc` (`:569`) into a parametrized auth test

**Interfaces:**
- Consumes: Task 1 builder, Task 2 `_validate_oidc_args`.
- Produces: tool argument `oidc: OidcAuthConfig | None = None` (same description as Task 2);
  `modify_streamlit_data_app_internal(..., oidc: OidcAuthConfig | None = None)`;
  `_update_existing_data_app_config(..., oidc: OidcAuthConfig | None = None)`.

- [ ] **Step 1: Write the failing tests** — parametrize the existing overwrite test over `('basic-auth', None)`, `('oidc', cfg)` with a stored cipher (kept) and `('default', None)` (block unchanged). Add the three Streamlit-relevant rejections (no `oidc`; `oidc` with other type; OIDC on create) at tool level.
- [ ] **Step 2: Run** `pytest tests/tools/test_data_apps.py -k "streamlit or update_existing_data_app_config" -v` — expect FAIL.
- [ ] **Step 3: Implement** — `_validate_oidc_args(..., is_create=not configuration_id)` at the top of the tool; thread `oidc` through to `_build_authorization`.
- [ ] **Step 4: Run** the same command — expect PASS.
- [ ] **Step 5: Commit** `feat: accept OIDC on Streamlit data app updates`

### Task 4: Output fields, change summary, slug pinning

**Files:**
- Modify: `src/keboola_mcp_server/tools/data_apps.py` — `ModifiedDataAppOutput` (`:548`), `ModifiedPythonJsDataAppOutput` (`:558`), both tools' create/update return paths, `_resolve_slug_update` (`:2933`), `_update_existing_data_app_config` slug line (`:2468`)
- Test: `tests/tools/test_data_apps.py` — extend `test_modify_python_js_data_app_update_slug` (`:1349`), `test_modify_python_js_data_app_create_authentication_type` (`:1513`)

**Interfaces:**
- Consumes: Task 1 `_oidc_provider`, `_oidc_setup_required`.
- Produces:
  - both outputs gain `oidc_callback_url: str | None = None` and `oidc_setup_required: bool = False`
  - `_oidc_callback_url(client: KeboolaClient, data_app_id: str, configuration: Mapping[str, Any]) -> str | None` — None for drafts or without a slug
  - `_oidc_change_summary(authorization: Mapping[str, Any], config_link: str, roles_requested: bool) -> str | None` — when setup is required: tell the agent the app cannot log anyone in yet, the human must open `config_link`, Authentication section, enter the client secret, save; then (if roles requested) run the `'oidc'` update again with `allowed_roles`; and deploy last. When roles were requested and setup is already done: remind that a later UI save drops them.
- Rules:
  - `oidc_callback_url` on every prod create and update, any auth type; computed from the config just written (`updated_config` / create config), not a re-fetch.
  - `oidc_setup_required` from the `authorization` just written.
  - Partial-success outputs (`_partial_update_output`, `_partial_create_output`) leave both fields at defaults.
  - Slug: `_resolve_slug_update` returns None (no implicit move) when the authorization being written has an OIDC provider; Streamlit keeps the stored slug in that case. An explicit python-js `slug` on an OIDC app adds to `change_summary`: register the new `oidc_callback_url` at the IdP before redeploying.

- [ ] **Step 1: Write the failing tests**
  - `basic-auth` create (python-js and Streamlit, mocked `hostname_suffix`) → `oidc_callback_url == 'https://my-app-<id>.hub.<suffix>/_proxy/callback'`, `oidc_setup_required is False`
  - draft create → `oidc_callback_url is None`
  - `'oidc'` update with no stored secret → `oidc_setup_required is True`, `change_summary` contains the config link; with `allowed_roles` → also contains "allowed_roles"
  - never-deployed python-js OIDC app renamed → slug unchanged (extend the slug test's params); Streamlit OIDC app renamed → slug unchanged; explicit slug on OIDC app → slug changes and summary mentions the callback URL
- [ ] **Step 2: Run** `pytest tests/tools/test_data_apps.py -k "callback or setup_required or slug" -v` — expect FAIL.
- [ ] **Step 3: Implement** per Interfaces and Rules.
- [ ] **Step 4: Run** the same command, then `pytest tests/tools/test_data_apps.py -q` — expect PASS.
- [ ] **Step 5: Commit** `feat: return OIDC callback URL and setup state from data app tools`

### Task 5: Offer the secret page via URL elicitation

**Files:**
- Modify: `src/keboola_mcp_server/elicitation.py`, `src/keboola_mcp_server/tools/data_apps.py` (both tools' update return paths)
- Test: `tests/test_elicitation.py` (extend the parametrized test and `_server()`)

**Interfaces:**
- Consumes: `open_url_input_request`, `open_url_in_client`, `client_supports_url_elicitation`, `uses_input_required_rounds`, `OPEN_URL_INPUT_KEY` (existing).
- Produces:
  - `class UrlAction(NamedTuple)`: `message: str`, `url: str`
  - `offer_url(message: str, url: str) -> None` — appends to the request's holder; no-op outside a request (no holder set)
  - `UrlElicitationMiddleware.on_call_tool` sets a fresh holder before `call_next`; after a successful result with a pending action and a URL-capable client: 2026-07-28 without an `open_url` answer → `open_url_input_request(...)`; with the answer → the result; older protocol → `open_url_in_client(...)`, then the result. `open_url_input_request` / `open_url_in_client` take `(message, url)` (or `UrlAction`) instead of the error; update the error path to match.
  - The tools call `offer_url(...)` after the committed write when `oidc_setup_required` is true, with the config link from `links_manager.get_data_app_config_link(...)`.

- [ ] **Step 1: Write the failing tests** — add a `needs_setup` tool to `_server()` that calls `offer_url('Enter the client secret.', SETUP_URL)` and returns `'ok'`. Parametrize: capable client accept → result `'ok'`, one URL elicitation with `SETUP_URL`; capable decline → `'ok'`, one elicitation; plain client → `'ok'`, none. Assert `not result.is_error`. Keep the existing unlock cases green.
- [ ] **Step 2: Run** `pytest tests/test_elicitation.py -v` — expect FAIL.
- [ ] **Step 3: Implement** per Interfaces; wire `offer_url` into both tools.
- [ ] **Step 4: Run** `pytest tests/test_elicitation.py tests/tools/test_data_apps.py -q` — expect PASS. Confirm the in-memory client runs the 2026-07-28 round trip (the existing test already does); if it does not, add one case that asserts the retry returns `'ok'` and the handler is called once.
- [ ] **Step 5: Commit** `feat: offer the OIDC secret page via URL elicitation`

### Task 6: Docs, version, full check

**Files:**
- Modify: both tool docstrings (Authentication sections: the six-step sequence, deploy last), `docs/python-js-data-apps.md` (OIDC section), `TOOLS.md` (regenerated), `pyproject.toml` (minor bump), `uv.lock`

- [ ] **Step 1:** Update docstrings and `docs/python-js-data-apps.md`; no ticket IDs.
- [ ] **Step 2:** Bump minor version; run `uv lock` with a current `uv` (an old one rewrites the whole lock — check `git diff uv.lock` is one line).
- [ ] **Step 3:** Regenerate `TOOLS.md` (see the `check-tools-docs` tox env for the command).
- [ ] **Step 4: Run** `tox` — expect all envs to pass.
- [ ] **Step 5: Commit** `docs: document OIDC setup for data apps`

### Task 7: Manual end-to-end on a dev stack

Run the RFC's *Manual verification* list against a local server (`.mcp.json` pointing at the worktree venv),
plus: rename the app between steps 2 and 3 and confirm `oidc_callback_url` did not change. Record results in the
PR description.
