# RFC: Support CF variables end-to-end in the MCP flow models (`variableOverrides` + JMESPath values)

> **Status:** Implemented in its own PR (follow-up to AJDA-2810, which shipped in #564). The model
> changes and tests described below are delivered alongside this RFC.

Linear: _TBD_ — CF variables: support `variableOverrides` and JMESPath values in MCP flow models
(parent: AJDA-2290 · project: Conditional Flows · related: AJDA-2810, AJDA-2351)

## Problem

AJDA-2810 makes the MCP server source the `keboola.flow` configuration schema **live** from the
Developer Portal, so `get_flow_schema` and jsonschema validation now reflect the current schema —
including the new **Variables** fields. However, the create/update path does **not** only validate
against that schema. Before validation, `create_conditional_flow` / `update_flow` route the
agent-supplied `phases`/`tasks` through Pydantic models in `tools/flow/model.py`
(`get_flow_configuration()` → `ConditionalFlowTask.model_validate(...).model_dump(exclude_unset=True,
by_alias=True)`, and `validate_flow_structure()`).

Those models predate the variables feature and do not carry two field shapes the feature relies on:

1. **`variableOverrides` is silently dropped.** `JobTaskConfiguration` (`model.py:248`) does not
   define `variableOverrides`, and the conditional-flow models use Pydantic's default
   `extra='ignore'`. A job task that passes a flow variable into a component
   (`"task": {"type": "job", ..., "variableOverrides": ["importedRowsSum"]}`) round-trips to:
   ```json
   {"type": "job", "componentId": "...", "configId": "...", "mode": "run"}
   ```
   The override is gone before the config reaches Storage — no error, the flow is created **without**
   the variable wiring.

2. **JMESPath values are rejected (hard fail).** `TaskCondition.value` (`model.py:149`) is a closed
   `Literal[...]` of ~13 fixed property paths (`'job.result.output.tables'`, `'job.status'`, …). A
   dynamic value such as
   `sum(job.result.output.tables[].metrics[?name=='importedRowsCount'][].value)` is not in that set,
   so it raises `ValidationError`. This blocks both:
   - the `variable` task `source.value` (setting a Flow variable from a prior job's result), and
   - phase transition `condition` operands (`{"type": "task", "task": "...", "value": "<jmespath>"}`).

Because the Pydantic round-trip runs **before** `validate_flow_configuration_against_schema`, the live
schema cannot save either case: case 1 is already stripped, case 2 has already raised.

**Net effect:** after AJDA-2810, an agent still cannot create or update a conditional flow that uses
flow variables (override-into-task) or JMESPath-driven dynamic values/conditions through the MCP
tools, even though `get_flow_schema` correctly advertises them.

### Reproduction (against the current models on `main`)

Input job task with `variableOverrides` → field dropped; variable task / phase condition using a
JMESPath `value` → `ValidationError`:
```
task.variable.source.task.value
  Input should be 'taskId','phaseId','status','job.id', ... or 'job.result.message'
```

## Scope

**In scope:** make the MCP conditional-flow models faithfully accept, round-trip, and render the
variables fields the live `keboola.flow` schema permits.

**Out of scope:** the schema-sourcing change itself (AJDA-2810, already done), the Developer-Portal
schema definition (AJDA-2351), legacy orchestrator flows.

## Proposed changes (`src/keboola_mcp_server/tools/flow/model.py`)

1. **Add `variableOverrides` to `JobTaskConfiguration`:**
   ```python
   variable_overrides: Optional[list[str]] = Field(
       default=None,
       description='Names of flow variables to pass into this job as variable overrides',
       alias='variableOverrides',
   )
   ```

2. **Accept free-form (JMESPath) `value` strings.** Relax the closed `Literal` on the `value` field
   wherever a dynamic value is allowed — at minimum `TaskCondition.value`, and any variable-source
   `value`. Replace the `Literal[...]` with `str` (keep the former enum members as docstring examples
   so agents still get guidance). Confirm against the live schema exactly which fields are free-form
   vs. enumerated before widening, so we don't over-relax.

3. **Decide the forward-compat policy for unknown fields.** The silent `variableOverrides` drop was a
   direct consequence of `extra='ignore'`. Options:
   - Explicitly model every known field (this RFC) and keep `extra='ignore'`; or
   - Switch the task/condition models to `extra='allow'` so future Developer-Portal fields pass
     through untouched (resilient, but the model stops being a strict contract); or
   - `extra='forbid'` to fail loudly on anything unmodeled (safest against silent loss, but brittle
     against schema evolution — would require a model change for every new field).
   Recommended: explicitly model the known variables fields now, and adopt **`extra='allow'`** on the
   `*TaskConfiguration` and condition models so the MCP server forwards future `keboola.flow` fields
   without a code change (mirrors the AJDA-2810 goal of not lagging the live schema). Validation
   against the live schema remains the real gate.

4. **Verify the read/display path.** `Flow.from_api_response` / `GetFlowsDetailOutput` use the same
   models — confirm an existing variable flow round-trips for display without dropping fields.

## Testing / Verification

- **Unit:** add round-trip tests in `tests/tools/flow/test_utils.py` using a representative flow
  (variable task setting `importedRowsSum` from a JMESPath over a prior job's result; a job task with
  `variableOverrides: ["importedRowsSum"]`; a phase transition `condition` of type `operator` with a
  `task` operand using JMESPath). Assert `get_flow_configuration(...)` preserves `variableOverrides`
  and the JMESPath `value` verbatim, and that `validate_flow_structure(...)` accepts the flow.
- **Integration:** flesh out the AJDA-2810 guarded test
  `test_conditional_flow_variables_when_advertised` (currently self-skipping) to create/validate the
  variables flow once the live schema advertises the fields, then clean up.
- **Pre-PR:** branch + commit prefix per the new ticket ID; version bump (**minor** — broader config
  acceptance); `uv lock`; full `tox` green incl. `check-tools-docs`.

## Sequencing

Independent of AJDA-2810 (which can merge first). This is the "make CF variables actually work through
MCP create/update" piece; pair its verification with a stack whose `keboola.flow` schema already
includes the variables fields (post AJDA-2351).
