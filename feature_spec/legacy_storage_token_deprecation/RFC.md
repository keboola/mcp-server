# RFC: Deprecate legacy (non-programmatic) Storage API token as a client credential

Linear: [AI-3774](https://linear.app/keboola/issue/AI-3774/deprecate-legacy-non-programmatic-storage-api-token-as-an-accepted-mcp)

## Problem

PSGO-261 added three richer auth modes on top of the original design: programmatic
multi-project tokens (`kbc_at_`/`kbc_pat_`) narrowed per request via `X-KBC-ProjectId`
(`feature_spec/programmatic_token_project_scope`), OAuth login exchanged into a
programmatic session (`feature_spec/oauth_session_exchange`,
`feature_spec/oauth_session_persistence`), and PAT token exchange
(`feature_spec/pat_token_support`). All three give the server a durable, introspectable,
revocable session and enable multi-project scoping.

The original credential mode — a bare, per-project Storage API token supplied directly by
the caller, with no `kbc_at_`/`kbc_pat_` prefix — still works, unconditionally, with no
deprecation signal anywhere:

- It can be supplied via `--storage-token` (`cli.py:58`), `KBC_STORAGE_TOKEN` /
  `STORAGE_API_TOKEN` env vars (`config.py:35`, aliased), or per-request as the
  `X-StorageApi-Token` / `Authorization` header (`storage_token` is in
  `Config._HEADER_ELIGIBLE_FIELDS`, `config.py:91-95`).
- `SessionStateMiddleware.create_session_state` (`mcp.py:795-854`) branches on
  `is_programmatic_token(storage_token)` (`mcp.py:829`). When `False`, it silently builds a
  single-project `KeboolaClient` using the legacy header format — no log line, no
  client-visible signal, nothing distinguishes this from the "normal" path.
- Contrast this with the *newer* surface: `get_accessible_projects`/`set_project_scope`
  already hard-reject a legacy token outright — `_parent_subject_token`
  (`tools/project.py:91-95`) raises `ValueError` telling the caller to use a programmatic
  token or run `login`. The inconsistency is that the *original* single-project flow never
  got the same treatment, so a caller has no signal that they're on a path with no
  multi-project support, no revocation, and no session model.

Visible symptom: there is currently no way — for us or for a caller — to tell how much
legacy-token traffic still exists. Any future decision to actually remove support for bare
tokens is currently unmakeable, because we have zero data on who would break.

**Out of scope for this RFC, and not something to lose sight of:** the legacy Storage
token *format* itself is not going away. `StorageTokenResolver`
(`clients/auth_bridge.py`) exchanges a programmatic token *into* a legacy per-project
Storage token for downstream services (Storage, Queue, AI) that only understand that
format — that mechanism is required regardless of which client credential mode wins. This
RFC is only about whether the MCP server should keep accepting a bare legacy token
*directly from an external caller* as their primary credential.

## Required Behavior

| Scenario | Required behavior |
| --- | --- |
| A session is built from a token where `is_programmatic_token()` is `False` (any acceptance path: CLI arg, env var, header, `Authorization` fallback) | Log a single deprecation warning identifying the acceptance path, without changing behavior or failing the request. |
| A session is built from a programmatic token, an OAuth-exchanged session, or via `login` | No warning — these paths are unaffected. |
| Anyone reading `README.md`'s "Authenticating without a browser" section | Told that a bare per-project token is deprecated and given the programmatic-token alternative, not just shown as one of two equivalent options. |
| A future decision to sunset the legacy path | Has real usage data (log-based) to decide against, instead of being made blind. |

Explicitly **not** required by this RFC: rejecting legacy tokens, changing any tool
response shape, or committing to a removal date. Those are follow-up decisions this RFC
sets up, not ones it makes.

## Resolution Strategy

Minimal, additive, no behavior change — mirrors how `_parent_subject_token` already
reports the same distinction, just as a warning instead of a hard error:

1. **`mcp.py:829`** (`create_session_state`) — add an `else` branch alongside the existing
   `if is_programmatic_token(storage_token):` that logs one `LOG.warning(...)`, e.g.:
   `"Session authenticated with a legacy per-project Storage API token. This credential "
   "mode is deprecated; migrate to a Keboola programmatic token (kbc_at_/kbc_pat_) or "
   "OAuth login. See README.md#authenticating-without-a-browser."` No token material in
   the message (matches existing logging discipline in `auth_bridge.py`).
2. **`README.md`** — reword the "Authenticating without a browser" section (currently
   line 161) and the container quick-start (line 359) to state the bare-token mode is
   deprecated and lead with the programmatic-token path; keep the bare-token instructions
   present (still supported) but no longer presented as the default.
3. No changes to `create_session_state`'s control flow, `StorageTokenResolver`, or any
   tool-facing behavior. No changes to `_parent_subject_token` — its existing hard
   rejection for the multi-project tools is correct as-is and out of scope here.

This gives us a log-based signal (queryable in Datadog by the warning message) to measure
real legacy-token usage before any follow-up RFC proposes an actual sunset date or a
harder signal (e.g. a response-visible notice, which would be a response-shape change
needing its own RFC and version bump).

## Scope

In scope:

- One `LOG.warning` added at the single existing branch point that already distinguishes
  legacy vs. programmatic tokens (`mcp.py:829`).
- `README.md` wording update in the two sections that currently present the bare token as
  a first-class option.
- Unit test asserting the warning fires for a non-programmatic token and does not fire for
  a programmatic one (extend the existing `create_session_state` test coverage rather than
  adding a new test function).
- Patch version bump (behavior-invisible logging change) + `uv lock`.

Out of scope (tracked as follow-ups once this RFC's telemetry gives us data):

- Actually rejecting legacy tokens, or any sunset date/timeline commitment.
- A client-visible deprecation signal (response field, header, or tool notice) — this
  would change a response shape and needs its own RFC.
- Any change to `StorageTokenResolver` / the exchange-to-legacy-token mechanism used
  internally for downstream services — that stays regardless of this deprecation's outcome.
- Any change to `_parent_subject_token`'s existing hard rejection in the multi-project
  tools — already correct, not touched.

## Testing / Verification

1. Extend `tests/test_mcp.py`'s (or wherever `create_session_state` is currently covered)
   parametrized case set with a `token_kind` axis (`programmatic` / `legacy`) asserting:
   `caplog` contains the deprecation warning iff `token_kind == 'legacy'`.
2. `tox` — pytest, ruff, check-tools-docs all exit 0 (no tool signatures change, so
   `TOOLS.md` is unaffected).
3. Manual: run the local server with a legacy per-project token and confirm the warning
   appears in the log once per session build; run with a `kbc_at_`/`kbc_pat_` token and
   confirm it does not.
