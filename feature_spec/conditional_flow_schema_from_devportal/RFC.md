# RFC: Fetch the conditional-flow JSON schema from the Developer Portal

Linear: [AJDA-2810 — CF variables: Update MCP](https://linear.app/keboola/issue/AJDA-2810/cf-variables-update-mcp)
(parent: AJDA-2290 · project: Conditional Flows · blocked-by: AJDA-2351 · related: AJDA-2581)

## Problem

Conditional Flows (`keboola.flow`) are gaining a new **Variables** capability, delivered by updating
the `keboola.flow` JSON schema **in the Developer Portal** (tracked by the AJDA-2351 blocker — not in
this repo).

> **Relationship to `keboola.variables`:** flow variables are *not* implemented via the
> `keboola.variables` component. They are defined inline in the `keboola.flow` configuration. However,
> they can be used to **override component-level variables**, which *are* implemented with
> `keboola.variables`. See
> [How variables reach component jobs](https://help.keboola.com/flows/#how-variables-reach-component-jobs).
> No `keboola.variables` integration is required on the MCP side — fetching the updated `keboola.flow`
> schema is sufficient to surface and validate the new flow-variables fields.

The MCP server, however, ships a **static, bundled copy** of that schema at
`src/keboola_mcp_server/resources/conditional-flow-schema.json`. It is read synchronously in two places:

- `tools/validation.py:358` — `_load_schema(ConfigurationSchemaResources.FLOW)` for jsonschema validation.
- `tools/flow/utils.py:36` — `_load_schema(flow_type)` for the `get_flow_schema` markdown output.

Because the schema is bundled, the MCP server will not pick up the new variables fields (or any future
schema change) until someone re-bundles the file and cuts a new release. The visible symptom:
`get_flow_schema` advertises an outdated schema and conditional-flow validation rejects valid configs
(or accepts invalid ones) relative to what the live `keboola.flow` component actually supports.

## Required Behavior

| Aspect | Behavior after the change |
| --- | --- |
| Conditional schema source | The **live** `configuration_schema` of the `keboola.flow` component, fetched from the Developer Portal. |
| Fetch mechanism | Reuse the existing `fetch_component()` path (AI Service `docs/components/keboola.flow`, with the built-in Storage API 404-fallback). |
| Bundled conditional schema | **Removed.** `conditional-flow-schema.json` is deleted; there is no local/bundled fallback for conditional flows. |
| Fetch failure (network / 5xx / empty schema) | **Hard-fail** with a clear, agent-recoverable error. The schema is always the live version — never stale. |
| Legacy orchestrator (`keboola.orchestrator`) | Unchanged — stays bundled. |
| Storage schema | Unchanged — stays bundled. |
| `get_flow_schema` tool signature | Unchanged (still returns the schema as markdown); only its source changes. |

## Resolution Strategy

1. **Add an async resolver** in `tools/flow/utils.py`:
   ```python
   async def resolve_flow_schema(client: KeboolaClient, flow_type: FlowType) -> JsonDict:
       if flow_type == CONDITIONAL_FLOW_COMPONENT_ID:
           component = await fetch_component(client, CONDITIONAL_FLOW_COMPONENT_ID)
           schema = component.configuration_schema
           if not schema:
               raise ValueError(
                   'Could not retrieve the conditional flow (keboola.flow) configuration schema '
                   'from the Developer Portal. The schema is required to create or validate '
                   'conditional flows. Please retry; if this persists the keboola.flow component '
                   'schema may be unavailable on this stack.'
               )
           return cast(JsonDict, schema)
       return _load_schema(flow_type)  # legacy orchestrator stays bundled
   ```
   - `fetch_component` is from `tools/components/utils.py` (already imported by `validation.py`, so no
     new circular-import risk; use a function-local import if a cycle appears).
   - Wrap non-404 `HTTPStatusError` / network errors so the agent gets the clear message above.
   - **Optional but recommended:** cache the fetched `keboola.flow` schema per session (mirroring
     `KeboolaClient._features_cache`) to avoid re-fetching on every create/update/get-schema call.
     Session-scoped so it is never stale across runs.

2. **Make schema output async.** `get_schema_as_markdown(flow_type)` →
   `async get_schema_as_markdown(client, flow_type)` calling `resolve_flow_schema`. Update
   `get_flow_schema` (`tools/flow/tools.py:150`) to pass the client and `await`. The existing
   conditional-flows project gate (`tools/flow/tools.py:141`) stays, so we only fetch when enabled.

3. **Validation accepts the resolved schema.** Give
   `validate_flow_configuration_against_schema(flow, flow_type, ...)` an optional
   `schema: JsonDict | None = None`: when provided, validate against it; otherwise fall back to the
   bundled load by `flow_type` (now legacy-only). Remove `ConfigurationSchemaResources.FLOW`
   (`validation.py:38`) and its branch (`:319`). This keeps the legacy path and its existing tests
   (`test_validation.py:133,153`) untouched.

4. **Wire the three conditional call sites** in `tools/flow/tools.py` (`:188` create_flow, `:282`
   create_conditional_flow, `:622` update_flow_internal):
   ```python
   schema = await resolve_flow_schema(client, flow_type)
   validate_flow_configuration_against_schema(flow_configuration, flow_type, schema=schema)
   ```
   Move the `KeboolaClient.from_state(...)` line above validation in `create_flow` /
   `create_conditional_flow`; `update_flow_internal` already has `client`.

5. **Remove the bundled conditional schema.** Delete
   `src/keboola_mcp_server/resources/conditional-flow-schema.json` and the
   `CONDITIONAL_FLOW_COMPONENT_ID` entry in `FLOW_SCHEMAS` (`tools/flow/utils.py:31`).

**Trade-off:** reusing `fetch_component` issues an extra Storage API call (it merges the `data`
section) on top of the AI Service call. The optional per-session cache mitigates this; the alternative
(a leaner AI-Service-only fetch) was rejected to keep the well-tested 404-fallback behavior.

**Sequencing:** this MCP change is independent of AJDA-2351. It can merge first; the variables
behavior "lights up" once the Developer-Portal schema is published. Verify against a stack whose
`keboola.flow` schema already includes variables before closing the ticket.

## Scope

**In scope:** sourcing the `keboola.flow` conditional-flow schema live from the Developer Portal;
deleting its bundled copy; hard-fail on fetch failure.

**Out of scope:** the legacy orchestrator schema, the storage schema, any change to the
`get_flow_schema` tool signature, the Developer-Portal schema update itself (AJDA-2351), the
documentation linking ("Link documentation?" bullet, tracked separately), and any new conditional-flow
condition types / capabilities.

## Testing / Verification

**Unit tests**
- Add a representative `keboola.flow` schema fixture under `tests/tools/flow/fixtures/` (not packaged)
  so conditional validation tests run offline.
- `tests/tools/flow/test_utils.py`: `resolve_flow_schema` returns the component's
  `configuration_schema` for `keboola.flow` (mock `fetch_component`); raises a clear `ValueError` on
  fetch failure / empty schema; still returns the bundled schema for `keboola.orchestrator`.
- `tests/tools/flow/test_tools.py`: `get_flow_schema` returns the live (mocked) schema for
  `keboola.flow`, with no real network call.
- `tests/tools/components/test_validation.py`: legacy cases unchanged; optionally add a conditional
  case passing `schema=<fixture>`.
- Mock at the client-method level (`AsyncMock` on `ai_service_client.get_component_detail` /
  `fetch_component`) per repo convention — avoid over-mocking.

**Integration tests** (required — new external data flow)
- `integtests/tools/flow/test_tools.py`: call `get_flow_schema(flow_type="keboola.flow")` against a
  real stack with conditional flows enabled; assert a non-empty schema is returned (proving it comes
  from the Developer Portal). A create/validate conditional-flow test exercises the path end to end.
- **Update the existing conditional-flow integration tests** to cover the new **variables** fields,
  driven by the live `docs/components/keboola.flow` schema. The existing CF create/update/validate
  integration scenarios should be extended with flow-variable definitions (and a task that consumes /
  overrides a variable) so they exercise the new schema fields rather than only the pre-variables
  shape. This keeps the integration suite aligned with whatever the Developer-Portal schema currently
  advertises once AJDA-2351 lands.

**Manual E2E** (local `.mcp.json` per `CLAUDE.md`)
1. Point at a stack with `keboola.flow` enabled; reload the server.
2. `get_flow_schema(flow_type="keboola.flow")` → live schema returned.
3. `create_conditional_flow` valid → succeeds; invalid → schema-validation error from the live schema.
4. Hard-fail path → clear error, no stale bundled schema used.

**Pre-PR checklist** (`CONTRIBUTING.md` / `CLAUDE.md`)
- Branch `miro-ajda-2810-cf-variables-update-mcp`; commits prefixed with `AJDA-2810:`.
- This RFC linked in the PR.
- Bump `pyproject.toml` (**minor** — new data flow) and run `uv lock`.
- Full `tox` green (pytest + black + isort + flake8 + check-tools-docs); confirm `TOOLS.md` unchanged
  via `tox -e check-tools-docs`.
