# Conditional-Flow Schema from Developer Portal — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Source the conditional-flow (`keboola.flow`) JSON schema live from the Developer Portal instead of a bundled file, deleting the bundled copy and hard-failing on fetch failure.

**Architecture:** Add an async `resolve_flow_schema(client, flow_type)` resolver in `tools/flow/utils.py` that fetches the live `keboola.flow` `configuration_schema` via the existing `fetch_component()` path (with a session-scoped cache on `KeboolaClient`), while the legacy orchestrator schema stays bundled. `get_schema_as_markdown` and the schema-validation call sites become schema-aware: conditional flows pass the resolved schema; legacy keeps the bundled load. The bundled `conditional-flow-schema.json` and its `ConfigurationSchemaResources.FLOW` enum entry are removed.

**Tech Stack:** Python 3.10, `pytest`/`pytest-mock`/`pytest-asyncio`, `jsonschema`, `httpx`, `uv`, `tox` (pytest + black + isort + flake8 + check-tools-docs).

**Reference:** `feature_spec/conditional_flow_schema_from_devportal/RFC.md` (approved).

**Branch:** Work happens on the current branch `miro-ajda-2810-cf-variables-update-mcp`. All commit messages start with `AJDA-2810:`.

**Decisions baked in (from planning):**
- The per-session schema cache (mirroring `KeboolaClient._features_cache`) **is included** (Task 3).
- The variables-specific integration coverage is **schema-driven & guarded** — tests read the live schema and exercise variables fields only when the live schema advertises them (Task 8).

---

## File Structure

**Modified:**
- `src/keboola_mcp_server/clients/client.py` — add session-scoped flow-schema cache (`_flow_schema_cache` + two small accessors).
- `src/keboola_mcp_server/tools/flow/utils.py` — add `resolve_flow_schema`; make `get_schema_as_markdown` async; remove the `keboola.flow` entry from `FLOW_SCHEMAS`.
- `src/keboola_mcp_server/tools/validation.py` — `validate_flow_configuration_against_schema` gains optional `schema`; remove `ConfigurationSchemaResources.FLOW` and its selection branch.
- `src/keboola_mcp_server/tools/flow/tools.py` — `get_flow_schema` resolves via client+await; wire the three validation call sites to `resolve_flow_schema(...)` + `schema=`; move `KeboolaClient.from_state(...)` above validation in the two create tools.

**Created:**
- `tests/tools/flow/fixtures/conditional_flow_schema.json` — offline copy of a representative `keboola.flow` schema (so conditional unit tests run without network).

**Deleted:**
- `src/keboola_mcp_server/resources/conditional-flow-schema.json` — the bundled conditional schema (after all references are removed).

**Test files touched:**
- `tests/tools/flow/test_utils.py` — `resolve_flow_schema` + async `get_schema_as_markdown` tests.
- `tests/tools/flow/conftest.py` — shared `conditional_flow_schema` fixture loader.
- `tests/tools/flow/test_tools.py` — update `get_flow_schema` conditional test; update `create_conditional_flow` / `update_conditional_flow` tests to mock `fetch_component`.
- `tests/tools/components/test_validation.py` — keep legacy cases; add a conditional case passing `schema=<fixture>`.
- `integtests/tools/flow/test_tools.py` — assert live non-empty schema; schema-driven variables coverage.

---

## Task 1: Offline schema fixture for conditional unit tests

Copy the current bundled schema into a test fixture **before** it is deleted, so conditional unit tests have a realistic offline schema.

**Files:**
- Create: `tests/tools/flow/fixtures/conditional_flow_schema.json`
- Create: `tests/tools/flow/conftest.py` fixture (modify existing file)

- [ ] **Step 1: Copy the bundled schema to the fixtures folder**

Run:
```bash
mkdir -p tests/tools/flow/fixtures
cp src/keboola_mcp_server/resources/conditional-flow-schema.json \
   tests/tools/flow/fixtures/conditional_flow_schema.json
```

- [ ] **Step 2: Verify the fixture is valid JSON and contains conditional markers**

Run:
```bash
python -c "import json; s=json.load(open('tests/tools/flow/fixtures/conditional_flow_schema.json')); print('phases' in s['properties'], 'next' in s['properties']['phases']['items']['properties'])"
```
Expected: `True True`

- [ ] **Step 3: Add a shared fixture loader to `tests/tools/flow/conftest.py`**

Append to `tests/tools/flow/conftest.py` (top imports already include `pytest`; add `json` and `pathlib` if missing):

```python
import json
from pathlib import Path

from keboola_mcp_server.clients.storage import JsonDict


@pytest.fixture
def conditional_flow_schema() -> JsonDict:
    """Representative offline keboola.flow configuration schema for conditional-flow tests."""
    fixture_path = Path(__file__).parent / 'fixtures' / 'conditional_flow_schema.json'
    with fixture_path.open('r', encoding='utf-8') as f:
        return json.load(f)
```

- [ ] **Step 4: Commit**

```bash
git add tests/tools/flow/fixtures/conditional_flow_schema.json tests/tools/flow/conftest.py
git commit -m "AJDA-2810: add offline keboola.flow schema fixture for conditional-flow tests"
```

---

## Task 2: Session-scoped flow-schema cache on `KeboolaClient`

Add a per-session cache so the live `keboola.flow` schema is fetched once per session (never stale across runs). Mirrors `_features_cache`.

**Files:**
- Modify: `src/keboola_mcp_server/clients/client.py`
- Test: `tests/clients/test_client.py` (create test if file exists; otherwise add to nearest client test module — see Step 1)

- [ ] **Step 1: Locate the client test module**

Run:
```bash
ls tests/clients/ 2>/dev/null || find tests -name 'test_client*.py'
```
Expected: a path such as `tests/clients/test_client.py`. Use that path as the test file below. If no client test module exists, create `tests/clients/test_client.py` with a module docstring and `import pytest`.

- [ ] **Step 2: Write the failing test for the cache accessors**

Add to the client test module:

```python
from keboola_mcp_server.clients.client import KeboolaClient


def test_flow_schema_cache_roundtrip():
    client = KeboolaClient(
        storage_api_url='https://connection.keboola.com',
        storage_api_token='dummy-token',
    )
    assert client.get_cached_flow_schema('keboola.flow') is None
    schema = {'type': 'object'}
    client.cache_flow_schema('keboola.flow', schema)
    assert client.get_cached_flow_schema('keboola.flow') is schema
    # other flow types are independent
    assert client.get_cached_flow_schema('keboola.orchestrator') is None
```

> Note: `KeboolaClient.__init__` requires `storage_api_url` + `storage_api_token`. If the constructor signature differs, mirror an existing client-construction test in the repo.

- [ ] **Step 3: Run the test to verify it fails**

Run: `tox -e py310 -- tests/clients/test_client.py::test_flow_schema_cache_roundtrip -v`
Expected: FAIL with `AttributeError: 'KeboolaClient' object has no attribute 'get_cached_flow_schema'`

- [ ] **Step 4: Add the cache attribute and accessors**

In `src/keboola_mcp_server/clients/client.py`, add `JsonDict` to the storage import:

```python
from keboola_mcp_server.clients.storage import AsyncStorageClient, JsonDict
```

In `KeboolaClient.__init__`, right after the `self._features_cache` line, add:

```python
        self._features_cache: set[str] | None = None
        # Session-scoped cache of flow configuration schemas keyed by flow type (component id).
        # Mirrors _features_cache: fetched once per session so it is never stale across runs.
        self._flow_schema_cache: dict[str, JsonDict] = {}
```

Add these two methods next to `has_feature` (after it):

```python
    def get_cached_flow_schema(self, flow_type: str) -> JsonDict | None:
        """Return the cached configuration schema for the given flow type, or None if not cached."""
        return self._flow_schema_cache.get(flow_type)

    def cache_flow_schema(self, flow_type: str, schema: JsonDict) -> None:
        """Cache the configuration schema for the given flow type for the rest of the session."""
        self._flow_schema_cache[flow_type] = schema
```

- [ ] **Step 5: Run the test to verify it passes**

Run: `tox -e py310 -- tests/clients/test_client.py::test_flow_schema_cache_roundtrip -v`
Expected: PASS

- [ ] **Step 6: Stub the cache on the shared mocked client fixture (CRITICAL)**

`tests/conftest.py`'s `keboola_client` fixture is `mocker.AsyncMock(KeboolaClient)`. Once the new sync method `get_cached_flow_schema` exists on the class, the spec'd mock returns a **truthy MagicMock** (not `None`) — which would make `resolve_flow_schema` short-circuit on the cache and never call the (patched) `fetch_component`, producing false-green or wrong-reason failures in Tasks 4 and 5.

Fix it centrally. In `tests/conftest.py`, inside the `keboola_client` fixture, after `client.with_branch_id = mocker.AsyncMock(return_value=client)` (line 29), add:

```python
    # New per-session flow-schema cache: default to "empty cache" so resolve_flow_schema()
    # always exercises the (patched) fetch_component in tests instead of returning a MagicMock.
    client.get_cached_flow_schema = mocker.Mock(return_value=None)
    client.cache_flow_schema = mocker.Mock()
```

- [ ] **Step 7: Run the full flow + tools unit suites to confirm the fixture change is harmless**

Run: `tox -e py310 -- tests/tools/flow/test_tools.py -v`
Expected: PASS (no behavior change yet — the stub only affects the not-yet-wired conditional path).

- [ ] **Step 8: Commit**

```bash
git add src/keboola_mcp_server/clients/client.py tests/clients/test_client.py tests/conftest.py
git commit -m "AJDA-2810: add session-scoped flow-schema cache to KeboolaClient"
```

---

## Task 3: `resolve_flow_schema` resolver in `tools/flow/utils.py`

Resolve the conditional schema live (cached) and hard-fail clearly on failure; legacy stays bundled.

**Files:**
- Modify: `src/keboola_mcp_server/tools/flow/utils.py`
- Test: `tests/tools/flow/test_utils.py`

- [ ] **Step 1: Write the failing tests**

Add to `tests/tools/flow/test_utils.py`. It already imports `pytest`, `CONDITIONAL_FLOW_COMPONENT_ID`, and `ORCHESTRATOR_COMPONENT_ID` — do NOT re-add those (duplicate lines). Only add the new imports, and fold `resolve_flow_schema` into the existing `from keboola_mcp_server.tools.flow.utils import (...)` block:

```python
from httpx import HTTPStatusError, Request, Response
```

Add `resolve_flow_schema` to the existing flow.utils import block (alphabetical):

```python
from keboola_mcp_server.tools.flow.utils import (
    _check_legacy_circular_dependencies,
    _reachable_ids,
    ensure_legacy_phase_ids,
    ensure_legacy_task_ids,
    get_flow_configuration,
    resolve_flow_schema,
    validate_flow_structure,
)
```

```python
class TestResolveFlowSchema:
    """Tests for resolve_flow_schema."""

    @pytest.mark.asyncio
    async def test_returns_live_schema_for_conditional(self, mocker, conditional_flow_schema):
        client = mocker.Mock()
        client.get_cached_flow_schema.return_value = None
        component = mocker.Mock()
        component.configuration_schema = conditional_flow_schema
        mocker.patch(
            'keboola_mcp_server.tools.flow.utils.fetch_component',
            mocker.AsyncMock(return_value=component),
        )

        result = await resolve_flow_schema(client, CONDITIONAL_FLOW_COMPONENT_ID)

        assert result == conditional_flow_schema
        client.cache_flow_schema.assert_called_once_with(CONDITIONAL_FLOW_COMPONENT_ID, conditional_flow_schema)

    @pytest.mark.asyncio
    async def test_uses_cache_and_does_not_refetch(self, mocker, conditional_flow_schema):
        client = mocker.Mock()
        client.get_cached_flow_schema.return_value = conditional_flow_schema
        fetch = mocker.patch(
            'keboola_mcp_server.tools.flow.utils.fetch_component',
            mocker.AsyncMock(),
        )

        result = await resolve_flow_schema(client, CONDITIONAL_FLOW_COMPONENT_ID)

        assert result == conditional_flow_schema
        fetch.assert_not_called()

    @pytest.mark.asyncio
    async def test_raises_on_empty_schema(self, mocker):
        client = mocker.Mock()
        client.get_cached_flow_schema.return_value = None
        component = mocker.Mock()
        component.configuration_schema = None
        mocker.patch(
            'keboola_mcp_server.tools.flow.utils.fetch_component',
            mocker.AsyncMock(return_value=component),
        )

        with pytest.raises(ValueError, match='Could not retrieve the conditional flow'):
            await resolve_flow_schema(client, CONDITIONAL_FLOW_COMPONENT_ID)

    @pytest.mark.asyncio
    async def test_raises_on_fetch_http_error(self, mocker):
        client = mocker.Mock()
        client.get_cached_flow_schema.return_value = None
        error = HTTPStatusError(
            'boom',
            request=Request('GET', 'https://ai.keboola.com'),
            response=Response(500),
        )
        mocker.patch(
            'keboola_mcp_server.tools.flow.utils.fetch_component',
            mocker.AsyncMock(side_effect=error),
        )

        with pytest.raises(ValueError, match='Could not retrieve the conditional flow'):
            await resolve_flow_schema(client, CONDITIONAL_FLOW_COMPONENT_ID)

    @pytest.mark.asyncio
    async def test_returns_bundled_schema_for_legacy(self, mocker):
        client = mocker.Mock()
        fetch = mocker.patch(
            'keboola_mcp_server.tools.flow.utils.fetch_component',
            mocker.AsyncMock(),
        )

        result = await resolve_flow_schema(client, ORCHESTRATOR_COMPONENT_ID)

        assert result['properties']['phases']['items']['properties']  # bundled legacy schema
        assert 'dependsOn' in str(result)
        fetch.assert_not_called()
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `tox -e py310 -- tests/tools/flow/test_utils.py::TestResolveFlowSchema -v`
Expected: FAIL with `ImportError: cannot import name 'resolve_flow_schema'`

- [ ] **Step 3: Implement `resolve_flow_schema` and update imports**

In `src/keboola_mcp_server/tools/flow/utils.py`, update the typing import and add the new imports near the top:

```python
from typing import Any, Mapping, Sequence, cast

from httpx import HTTPStatusError
```

Add this import with the other `keboola_mcp_server.tools` imports (top-level is safe — `tools/components/utils.py` does not import `tools/flow`):

```python
from keboola_mcp_server.tools.components.utils import fetch_component
```

Add the resolver after `_load_schema`:

```python
async def resolve_flow_schema(client: KeboolaClient, flow_type: FlowType) -> JsonDict:
    """
    Resolve the JSON schema for a flow type.

    Conditional flows (``keboola.flow``) are sourced live from the Developer Portal via
    ``fetch_component`` and cached per session. Legacy orchestrator flows stay bundled.

    :param client: Authenticated Keboola client instance.
    :param flow_type: The flow type / component id to resolve the schema for.
    :return: The configuration schema as a JSON dict.
    :raises ValueError: If the live conditional schema cannot be retrieved or is empty.
    """
    if flow_type != CONDITIONAL_FLOW_COMPONENT_ID:
        return _load_schema(flow_type)  # legacy orchestrator stays bundled

    cached = client.get_cached_flow_schema(flow_type)
    if cached is not None:
        return cached

    failure_message = (
        'Could not retrieve the conditional flow (keboola.flow) configuration schema from the '
        'Developer Portal. The schema is required to create or validate conditional flows. '
        'Please retry; if this persists the keboola.flow component schema may be unavailable on '
        'this stack.'
    )
    try:
        component = await fetch_component(client, CONDITIONAL_FLOW_COMPONENT_ID)
    except HTTPStatusError as e:
        raise ValueError(failure_message) from e

    schema = component.configuration_schema
    if not schema:
        raise ValueError(failure_message)

    schema = cast(JsonDict, schema)
    client.cache_flow_schema(flow_type, schema)
    return schema
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `tox -e py310 -- tests/tools/flow/test_utils.py::TestResolveFlowSchema -v`
Expected: PASS (all 5)

- [ ] **Step 5: Commit**

```bash
git add src/keboola_mcp_server/tools/flow/utils.py tests/tools/flow/test_utils.py
git commit -m "AJDA-2810: add resolve_flow_schema fetching live keboola.flow schema"
```

---

## Task 4: Async `get_schema_as_markdown` + wire `get_flow_schema`

Make the markdown helper resolve the schema (async, client-aware), then update the tool to pass the client and await.

**Files:**
- Modify: `src/keboola_mcp_server/tools/flow/utils.py`
- Modify: `src/keboola_mcp_server/tools/flow/tools.py`
- Test: `tests/tools/flow/test_tools.py`

- [ ] **Step 1: Update the failing existing conditional schema test**

In `tests/tools/flow/test_tools.py`, replace `test_get_conditional_flow_schema_when_conditional_flows_enabled` body to mock `fetch_component` (no network) and use the offline fixture:

```python
    @pytest.mark.asyncio
    async def test_get_conditional_flow_schema_when_conditional_flows_enabled(
        self,
        mocker: MockerFixture,
        mcp_context_client: Context,
        conditional_flow_schema: dict,
    ):
        """Conditional schema is sourced live (mocked) when conditional flows are enabled."""
        mock_project_info = mocker.Mock()
        mock_project_info.conditional_flows = True
        mocker.patch('keboola_mcp_server.tools.flow.tools.get_project_info', return_value=mock_project_info)

        component = mocker.Mock()
        component.configuration_schema = conditional_flow_schema
        mocker.patch(
            'keboola_mcp_server.tools.flow.utils.fetch_component',
            mocker.AsyncMock(return_value=component),
        )

        result = await get_flow_schema(ctx=mcp_context_client, flow_type=CONDITIONAL_FLOW_COMPONENT_ID)

        assert isinstance(result, str)
        assert result.startswith('```json\n')
        assert result.endswith('\n```')
        assert 'next' in result
```

> The `conditional_flow_schema` fixture from `tests/tools/flow/conftest.py` (Task 1) is auto-discovered by pytest.

- [ ] **Step 2: Run the test to verify it fails**

Run: `tox -e py310 -- tests/tools/flow/test_tools.py::TestGetFlowSchemaTool::test_get_conditional_flow_schema_when_conditional_flows_enabled -v`
Expected: FAIL (current `get_schema_as_markdown` is sync and ignores the client; with the bundled file about to be removed and `fetch_component` mocked, the assertions/await mismatch).

- [ ] **Step 3: Make `get_schema_as_markdown` async**

In `src/keboola_mcp_server/tools/flow/utils.py`, replace:

```python
def get_schema_as_markdown(flow_type: FlowType) -> str:
    """Return the flow schema as a markdown formatted string."""
    schema = _load_schema(flow_type=flow_type)
    return f'```json\n{json.dumps(schema, indent=2)}\n```'
```

with:

```python
async def get_schema_as_markdown(client: KeboolaClient, flow_type: FlowType) -> str:
    """Return the flow schema as a markdown formatted string."""
    schema = await resolve_flow_schema(client, flow_type)
    return f'```json\n{json.dumps(schema, indent=2)}\n```'
```

- [ ] **Step 4: Wire `get_flow_schema` in `tools/flow/tools.py`**

Replace the final two lines of `get_flow_schema` (currently `LOG.info(...)` + `return get_schema_as_markdown(flow_type=flow_type)`):

```python
    LOG.info(f'Returning flow configuration schema for flow type: {flow_type}')
    client = KeboolaClient.from_state(ctx.session.state)
    return await get_schema_as_markdown(client, flow_type)
```

(`KeboolaClient` and `get_schema_as_markdown` are already imported in `tools.py`.)

- [ ] **Step 5: Run the test to verify it passes**

Run: `tox -e py310 -- tests/tools/flow/test_tools.py::TestGetFlowSchemaTool -v`
Expected: PASS (all 4 — the two legacy cases still load the bundled legacy schema).

- [ ] **Step 6: Commit**

```bash
git add src/keboola_mcp_server/tools/flow/utils.py src/keboola_mcp_server/tools/flow/tools.py tests/tools/flow/test_tools.py
git commit -m "AJDA-2810: source get_flow_schema conditional schema live"
```

---

## Task 5: Accept an explicit schema in validation AND wire the call sites (single commit)

> **Why merged:** Making `validate_flow_configuration_against_schema` *require* an explicit schema for conditional flows must land in the **same commit** as wiring the three call sites to pass that schema. If split, the commit after the validation change leaves `create_conditional_flow` / `update_flow` (and their tests) red, because those call sites would still call validation with `schema=None`. So validation change + call-site wiring + all affected test updates are one task / one commit.

Add an optional `schema` arg to validation (provided → validate against it; `None` → bundled legacy only, raise for conditional). Wire `create_flow`, `create_conditional_flow`, and `update_flow_internal` to resolve the schema and pass `schema=`. Move `KeboolaClient.from_state(...)` above validation in the two create tools. (The bundled `FLOW` enum + file are removed in Task 6.)

**Files:**
- Modify: `src/keboola_mcp_server/tools/validation.py`
- Modify: `src/keboola_mcp_server/tools/flow/tools.py`
- Test: `tests/tools/components/test_validation.py`
- Test: `tests/tools/flow/test_tools.py`

- [ ] **Step 1: Update `validate_flow_configuration_against_schema` (implementation)**

In `src/keboola_mcp_server/tools/validation.py`, replace the whole function with:

```python
def validate_flow_configuration_against_schema(
    flow: JsonDict,
    flow_type: FlowType,
    schema: Optional[JsonDict] = None,
    initial_message: Optional[str] = None,
    validation_context: ValidationContext | None = None,
) -> JsonDict:
    """
    Validate the flow configuration using jsonschema.
    :flow: json data to validate
    :flow_type: the type of flow schema to validate against (legacy flow or conditional flow)
    :schema: explicit schema to validate against; required for conditional flows (resolved live).
             When None, only the bundled legacy orchestrator schema is available.
    :initial_message: initial message to include in the error message
    :returns: The validated flow configuration
    """
    if schema is None:
        if flow_type != ORCHESTRATOR_COMPONENT_ID:
            raise ValueError(
                f'No schema provided for flow type "{flow_type}". The conditional flow schema must be '
                f'resolved from the Developer Portal via resolve_flow_schema() and passed explicitly.'
            )
        schema = _load_schema(ConfigurationSchemaResources.LEGACY_FLOW)
    _validate_json_against_schema(
        json_data=flow,
        schema=schema,
        initial_message=initial_message,
        validation_context=validation_context,
    )
    return flow
```

- [ ] **Step 2: Import the resolver in `tools/flow/tools.py`**

Add `resolve_flow_schema` to the existing `keboola_mcp_server.tools.flow.utils` import block:

```python
from keboola_mcp_server.tools.flow.utils import (
    get_all_flows,
    get_flow_configuration,
    get_schema_as_markdown,
    resolve_flow_by_id,
    resolve_flow_schema,
    validate_flow_structure,
)
```

- [ ] **Step 3: Wire `create_flow` (legacy)**

Replace the validation block in `create_flow`:

```python
    # Validate flow structure before to catch semantic errors in the structure
    validate_flow_structure(cast(JsonDict, flow_configuration), flow_type=flow_type)
    # Validate flow configuration against schema to catch syntax errors in the configuration
    validate_flow_configuration_against_schema(cast(JsonDict, flow_configuration), flow_type=flow_type)

    LOG.info(f'Creating new flow: {name} (type: {ORCHESTRATOR_COMPONENT_ID})')
    client = KeboolaClient.from_state(ctx.session.state)
```

with (client moved up, schema resolved and passed):

```python
    LOG.info(f'Creating new flow: {name} (type: {ORCHESTRATOR_COMPONENT_ID})')
    client = KeboolaClient.from_state(ctx.session.state)

    # Validate flow structure before to catch semantic errors in the structure
    validate_flow_structure(cast(JsonDict, flow_configuration), flow_type=flow_type)
    # Validate flow configuration against schema to catch syntax errors in the configuration
    schema = await resolve_flow_schema(client, flow_type)
    validate_flow_configuration_against_schema(cast(JsonDict, flow_configuration), flow_type=flow_type, schema=schema)
```

(Verify there is exactly **one** `client = KeboolaClient.from_state(ctx.session.state)` assignment left in `create_flow`. The `links_manager = await ProjectLinksManager.from_client(client)` line stays.)

- [ ] **Step 4: Wire `create_conditional_flow`**

Apply the same shape in `create_conditional_flow` — move `client = KeboolaClient.from_state(ctx.session.state)` above validation and pass `schema=`:

```python
    flow_type = CONDITIONAL_FLOW_COMPONENT_ID
    flow_configuration = get_flow_configuration(phases=phases, tasks=tasks, flow_type=flow_type)

    LOG.info(f'Creating new enhanced conditional flow: {name} (type: {flow_type})')
    client = KeboolaClient.from_state(ctx.session.state)

    # Validate flow structure to catch semantic errors in the structure
    validate_flow_structure(flow_configuration=flow_configuration, flow_type=flow_type)
    # Validate flow configuration against schema to catch syntax errors in the configuration
    schema = await resolve_flow_schema(client, flow_type)
    validate_flow_configuration_against_schema(cast(JsonDict, flow_configuration), flow_type=flow_type, schema=schema)
```

Ensure the old `client = KeboolaClient.from_state(ctx.session.state)` that previously sat right before `links_manager = await ProjectLinksManager.from_client(client)` is removed (only one assignment remains).

- [ ] **Step 5: Wire `update_flow_internal`**

In `update_flow_internal` (already has `client`), replace:

```python
    # Validate flow structure to catch semantic errors in the structure
    validate_flow_structure(flow_configuration=flow_configuration, flow_type=flow_type)
    # Validate flow configuration against schema to catch syntax errors in the configuration
    validate_flow_configuration_against_schema(cast(JsonDict, flow_configuration), flow_type=flow_type)
```

with:

```python
    # Validate flow structure to catch semantic errors in the structure
    validate_flow_structure(flow_configuration=flow_configuration, flow_type=flow_type)
    # Validate flow configuration against schema to catch syntax errors in the configuration
    schema = await resolve_flow_schema(client, flow_type)
    validate_flow_configuration_against_schema(cast(JsonDict, flow_configuration), flow_type=flow_type, schema=schema)
```

- [ ] **Step 6: Add validation unit tests (conditional explicit-schema + raise)**

Add to `tests/tools/components/test_validation.py` (imports already include `validation`, `json`, `ORCHESTRATOR_COMPONENT_ID`; add `CONDITIONAL_FLOW_COMPONENT_ID`):

```python
from keboola_mcp_server.clients.client import CONDITIONAL_FLOW_COMPONENT_ID, ORCHESTRATOR_COMPONENT_ID
```

```python
def test_validate_conditional_flow_with_explicit_schema():
    """A conditional flow validates against an explicitly provided schema."""
    with open('tests/tools/flow/fixtures/conditional_flow_schema.json', 'r') as f:
        schema = json.load(f)
    valid_flow = {
        'phases': [
            {'id': 'p1', 'name': 'Phase 1', 'next': [{'id': 't1', 'name': 'End', 'goto': None}]},
        ],
        'tasks': [
            {
                'id': 'task1',
                'name': 'Notify',
                'phase': 'p1',
                'task': {
                    'type': 'notification',
                    'title': 'Done',
                    'recipients': [{'channel': 'email', 'address': 'ops@example.com'}],
                },
            }
        ],
    }
    result = validation.validate_flow_configuration_against_schema(
        valid_flow, flow_type=CONDITIONAL_FLOW_COMPONENT_ID, schema=schema
    )
    assert result == valid_flow


def test_validate_flow_conditional_without_schema_raises():
    """Conditional flow without an explicit schema is a programming error (no bundled fallback)."""
    with pytest.raises(ValueError, match='No schema provided for flow type'):
        validation.validate_flow_configuration_against_schema(
            {'phases': [], 'tasks': []}, flow_type=CONDITIONAL_FLOW_COMPONENT_ID
        )
```

> If the example flow above trips a schema constraint, adjust the minimal `valid_flow` to satisfy the fixture schema's `required` fields — inspect `tests/tools/flow/fixtures/conditional_flow_schema.json` for the exact `phases`/`tasks` requirements. The intent is one valid conditional config.

- [ ] **Step 7: Update conditional create/update/folder tests in `test_tools.py`**

These tests drive `create_conditional_flow` / `update_flow` through the new live-schema path. The cache stub from Task 2 (`get_cached_flow_schema` returns `None` on the mocked client) ensures `resolve_flow_schema` actually calls the patched `fetch_component` instead of returning a MagicMock.

In `tests/tools/flow/test_tools.py`, update `test_create_conditional_flow` (add `conditional_flow_schema` param + mock):

```python
    @pytest.mark.asyncio
    async def test_create_conditional_flow(
        self,
        mocker: MockerFixture,
        mcp_context_client: Context,
        mock_conditional_flow_create_update: Dict[str, Any],
        conditional_flow_schema: dict,
    ):
        """Test conditional flow creation."""
        component = mocker.Mock()
        component.configuration_schema = conditional_flow_schema
        mocker.patch(
            'keboola_mcp_server.tools.flow.utils.fetch_component',
            mocker.AsyncMock(return_value=component),
        )
        keboola_client = KeboolaClient.from_state(mcp_context_client.session.state)
        keboola_client.storage_client.configuration_create = mocker.AsyncMock(
            return_value=mock_conditional_flow_create_update
        )

        result = await create_conditional_flow(
            ctx=mcp_context_client,
            name='Advanced Data Pipeline',
            description='Advanced pipeline with conditional logic and error handling',
            phases=mock_conditional_flow_create_update['configuration']['phases'],
            tasks=mock_conditional_flow_create_update['configuration']['tasks'],
        )

        assert isinstance(result, FlowToolOutput)
        assert result.success is True
        assert result.configuration_id == mock_conditional_flow_create_update['id']
        assert result.component_id == 'keboola.flow'
        assert result.description == mock_conditional_flow_create_update['description']
        assert result.timestamp is not None
        assert len(result.links) == 3
        assert result.version == mock_conditional_flow_create_update['version']

        keboola_client.storage_client.configuration_create.assert_called_once()
```

In `test_update_conditional_flow`, add the same `conditional_flow_schema` param and `fetch_component` patch at the top of the test body (before constructing `keboola_client`):

```python
        component = mocker.Mock()
        component.configuration_schema = conditional_flow_schema
        mocker.patch(
            'keboola_mcp_server.tools.flow.utils.fetch_component',
            mocker.AsyncMock(return_value=component),
        )
```

(and add `conditional_flow_schema: dict` to its signature).

> Sanity: the simple `mock_conditional_flow_phases` / `mock_conditional_flow_tasks` (notification task, single phase ending with `goto: None`) are valid under the offline schema, so validation passes.

Also update **`test_create_conditional_flow_folder`** (a module-level parametrized test near line 1100 that calls `create_conditional_flow` and would otherwise hit the live path). Add the `conditional_flow_schema` fixture param and the same `fetch_component` patch shown above at the top of its body:

```python
        component = mocker.Mock()
        component.configuration_schema = conditional_flow_schema
        mocker.patch(
            'keboola_mcp_server.tools.flow.utils.fetch_component',
            mocker.AsyncMock(return_value=component),
        )
```

(and add `conditional_flow_schema: dict` to its signature). The folder-only update tests (`test_modify_flow_folder*`) use `flow_type=ORCHESTRATOR_COMPONENT_ID` → legacy bundled resolve → no network → no change needed.

- [ ] **Step 8: Run the full flow tools module + validation flow tests**

Run: `tox -e py310 -- tests/tools/flow/test_tools.py tests/tools/components/test_validation.py -k "flow or Flow" -v`
Expected: PASS — conditional create/update/folder tests resolve the mocked live schema; legacy `create_flow`/`update` resolve the bundled legacy schema; the two new validation tests pass.

If any *other* conditional create/update test surfaces (e.g. in lifecycle/update parametrizations), add the same `fetch_component` patch + `conditional_flow_schema` param to it. Grep to find them all:
```bash
grep -n "create_conditional_flow\|flow_type=CONDITIONAL_FLOW_COMPONENT_ID\|CONDITIONAL_FLOW_COMPONENT_ID," tests/tools/flow/test_tools.py
```

- [ ] **Step 9: Commit (single commit — validation + wiring + tests together)**

```bash
git add src/keboola_mcp_server/tools/validation.py src/keboola_mcp_server/tools/flow/tools.py \
        tests/tools/components/test_validation.py tests/tools/flow/test_tools.py
git commit -m "AJDA-2810: validate conditional flows against the live resolved schema"
```

---

## Task 6: Remove the bundled conditional schema and its references

Now that nothing reads the bundled conditional schema, delete it and remove the dead enum/branch.

**Files:**
- Modify: `src/keboola_mcp_server/tools/validation.py`
- Modify: `src/keboola_mcp_server/tools/flow/utils.py`
- Delete: `src/keboola_mcp_server/resources/conditional-flow-schema.json`

- [ ] **Step 1: Remove the `FLOW` enum member**

In `src/keboola_mcp_server/tools/validation.py`, change:

```python
class ConfigurationSchemaResources(str, Enum):
    STORAGE = 'storage-schema.json'
    LEGACY_FLOW = 'flow-schema.json'
    FLOW = 'conditional-flow-schema.json'
```

to:

```python
class ConfigurationSchemaResources(str, Enum):
    STORAGE = 'storage-schema.json'
    LEGACY_FLOW = 'flow-schema.json'
```

- [ ] **Step 2: Remove the conditional entry from `FLOW_SCHEMAS`**

In `src/keboola_mcp_server/tools/flow/utils.py`, change:

```python
FLOW_SCHEMAS: Mapping[FlowType, str] = {
    CONDITIONAL_FLOW_COMPONENT_ID: 'conditional-flow-schema.json',
    ORCHESTRATOR_COMPONENT_ID: 'flow-schema.json',
}
```

to:

```python
FLOW_SCHEMAS: Mapping[FlowType, str] = {
    ORCHESTRATOR_COMPONENT_ID: 'flow-schema.json',
}
```

- [ ] **Step 3: Delete the bundled file**

Run:
```bash
git rm src/keboola_mcp_server/resources/conditional-flow-schema.json
```

- [ ] **Step 4: Confirm no remaining references**

Run:
```bash
grep -rn "conditional-flow-schema.json\|ConfigurationSchemaResources.FLOW\b" src/ tests/ integtests/
```
Expected: no matches (other than `LEGACY_FLOW` / `STORAGE`).

- [ ] **Step 5: Run the affected unit suites**

Run: `tox -e py310 -- tests/tools/flow/ tests/tools/components/test_validation.py -v`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/keboola_mcp_server/tools/validation.py src/keboola_mcp_server/tools/flow/utils.py
git commit -m "AJDA-2810: remove bundled conditional-flow schema and dead references"
```

---

## Task 7: Full unit-suite + lint pass

Catch any remaining sync→async fallout (e.g. another caller of `get_schema_as_markdown`).

- [ ] **Step 1: Search for any other callers of the changed signatures**

Run:
```bash
grep -rn "get_schema_as_markdown\|validate_flow_configuration_against_schema\|_load_schema(" src/ tests/ integtests/
```
Expected: every `get_schema_as_markdown(` call passes `(client, flow_type)` and is awaited; `validate_flow_configuration_against_schema(` conditional calls pass `schema=`.

- [ ] **Step 2: Run the full unit test suite**

Run: `tox -e py310`
Expected: PASS (no network; conditional paths are mocked).

- [ ] **Step 3: Run black + isort + flake8**

Run: `tox -e black -e isort -e flake8` (or just `tox` to run all envs)
Expected: PASS. Fix any formatting/import-order issues reported.

- [ ] **Step 4: Commit any fixups**

```bash
git add -A
git commit -m "AJDA-2810: fix up callers and formatting after live-schema change"
```

(Skip if nothing changed.)

---

## Task 8: Integration tests — live schema + schema-driven variables coverage

Integration tests run against a real stack (see `integtests/README.md`). The variables coverage is **schema-driven & guarded**: it reads the live `keboola.flow` schema and only exercises variables fields when the schema advertises them.

**Files:**
- Modify: `integtests/tools/flow/test_tools.py`

- [ ] **Step 1: Strengthen `test_get_flow_schema` to prove the schema is live & non-empty**

In `integtests/tools/flow/test_tools.py`, inside `test_get_flow_schema`, in the branch where `project_info.conditional_flows` is True (after `parsed_conditional_schema` is computed), add:

```python
        # The conditional schema is sourced live from the Developer Portal — it must be non-empty
        # and structurally a flow schema (not a stale/empty bundled placeholder).
        assert parsed_conditional_schema  # non-empty dict
        assert parsed_conditional_schema['properties']['phases']['items']['properties']
        assert parsed_conditional_schema['properties']['tasks']['items']['properties']
```

- [ ] **Step 2: Add a schema-driven variables helper + assertion**

Add a module-level helper near the top of `integtests/tools/flow/test_tools.py` (after imports):

```python
def _schema_advertises_variables(parsed_schema: dict) -> bool:
    """True if the live keboola.flow schema advertises flow-level variables fields."""
    # Defensive: the exact location of the variables block is owned by AJDA-2351 / Developer Portal.
    # Treat any top-level or phase-level 'variables' property as "advertised".
    top = parsed_schema.get('properties', {})
    if 'variables' in top:
        return True
    phase_props = top.get('phases', {}).get('items', {}).get('properties', {})
    return 'variables' in phase_props
```

- [ ] **Step 3: Add a guarded variables integration test**

Append a new test to `integtests/tools/flow/test_tools.py`:

```python
@pytest.mark.asyncio
async def test_conditional_flow_variables_when_advertised(
    mcp_context: Context, configs: list[ConfigDef]
) -> None:
    """
    If the live keboola.flow schema advertises flow variables, exercise the variables fields
    end to end. Otherwise skip — the Developer-Portal schema (AJDA-2351) is not yet published
    on this stack, and there is intentionally no bundled fallback.
    """
    from keboola_mcp_server.clients.client import CONDITIONAL_FLOW_COMPONENT_ID

    client = KeboolaClient.from_state(mcp_context.session.state)
    project_info = await get_project_info(mcp_context)
    if not project_info.conditional_flows:
        pytest.skip('Conditional flows not enabled on this stack.')

    schema_md = await get_flow_schema(mcp_context, CONDITIONAL_FLOW_COMPONENT_ID)
    parsed = json.loads(schema_md[8:-4])  # strip ```json\n and \n```
    if not _schema_advertises_variables(parsed):
        pytest.skip('Live keboola.flow schema does not advertise variables yet (AJDA-2351 not live).')

    # Build a minimal conditional flow that defines and consumes a variable, shaped from the
    # live schema's variables definition. Read `parsed` to populate the exact required fields.
    assert configs and configs[0].configuration_id is not None
    # The concrete variables block is derived from `parsed`; assert the create path validates
    # the variables fields against the live schema (i.e. create succeeds), then clean up.
    # NOTE: fill the `variables`/task-override fields per `parsed` once AJDA-2351 lands; until then
    # this test self-skips above, so it never asserts a guessed shape.
```

> Per the planning decision, this test is deliberately self-skipping until the live schema advertises variables, and it derives the concrete variables block from `parsed` rather than a guessed shape. When AJDA-2351 is confirmed live on the target stack, replace the trailing NOTE with a concrete `create_conditional_flow(...)` call that defines a flow variable and a task consuming/overriding it, wrapped in `try/finally` cleanup mirroring `test_create_and_retrieve_conditional_flow`.

- [ ] **Step 4: Confirm `get_project_info` import is present**

Run:
```bash
grep -n "get_project_info\|^import json\|^import pytest" integtests/tools/flow/test_tools.py | head
```
Expected: `get_project_info`, `json`, and `pytest` are importable in the module. If `get_project_info` is not imported, add `from keboola_mcp_server.tools.project import get_project_info`.

- [ ] **Step 5: Run integration tests (requires a configured stack)**

Run (only if integration env is configured per `integtests/README.md`):
```bash
tox -e integtests -- integtests/tools/flow/test_tools.py -k "get_flow_schema or variables" -v
```
Expected: `test_get_flow_schema` PASS; the variables test PASS or SKIP depending on whether the live schema advertises variables on the stack.

> If the integration environment is not configured locally, note this and rely on the maintainer/CI to run it before closing the ticket (the RFC requires verifying against a variables-enabled stack).

- [ ] **Step 6: Commit**

```bash
git add integtests/tools/flow/test_tools.py
git commit -m "AJDA-2810: integration coverage for live schema + guarded variables"
```

---

## Task 9: Version bump, lock, final `tox`, TOOLS.md

**Files:**
- Modify: `pyproject.toml`
- Modify: `uv.lock`

- [ ] **Step 1: Bump the version (minor — new external data flow)**

In `pyproject.toml`, change `version = "1.63.4"` to `version = "1.64.0"`.

- [ ] **Step 2: Sync the lock file**

Run (with the project venv activated):
```bash
uv lock
```
Expected: `uv.lock` updated with the new version; no dependency changes.

- [ ] **Step 3: Confirm TOOLS.md is unchanged (no tool signature change)**

Run: `tox -e check-tools-docs`
Expected: PASS. (Tool names/signatures are unchanged by this RFC; if it reports drift, run the documented regeneration command and inspect the diff — it should be empty.)

- [ ] **Step 4: Run the complete `tox` suite**

Run: `tox`
Expected: all envs (pytest, black, isort, flake8, check-tools-docs) exit 0.

- [ ] **Step 5: Commit**

```bash
git add pyproject.toml uv.lock
git commit -m "AJDA-2810: bump version to 1.64.0 and sync uv.lock"
```

---

## Self-Review (run before handing off)

**Spec coverage vs RFC §"Required Behavior" / §"Resolution Strategy":**
- Conditional schema source = live `configuration_schema` → Task 3 (`resolve_flow_schema` via `fetch_component`). ✓
- Reuse `fetch_component` path (404-fallback intact) → Task 3 imports the existing `fetch_component`. ✓
- Bundled conditional schema removed (no local fallback) → Task 6 deletes the file + enum + `FLOW_SCHEMAS` entry. ✓
- Hard-fail on network/5xx/empty schema with clear message → Task 3 (`ValueError` on `HTTPStatusError` and empty schema). ✓
- Legacy orchestrator unchanged (bundled) → Task 3 legacy branch + Task 5 legacy fallback; `flow-schema.json` retained. ✓
- Storage schema unchanged → untouched (`ConfigurationSchemaResources.STORAGE` kept). ✓
- `get_flow_schema` signature unchanged (markdown out) → Task 4 only changes the source; project gate retained. ✓
- Step 1 resolver (incl. per-session cache) → Tasks 2–3. ✓
- Step 2 async markdown output → Task 4. ✓
- Step 3 validation accepts resolved schema; remove `FLOW` enum + branch → Task 5 (accept schema) & Task 6 (remove enum). ✓
- Step 4 wire three call sites + move `from_state` up → Task 5. ✓
- Step 5 remove bundled schema + `FLOW_SCHEMAS` entry → Task 6. ✓
- Tests: resolver unit tests, `get_flow_schema` live (mocked), validation conditional case, integration live + guarded variables → Tasks 3,4,5,8. ✓
- Pre-PR: branch/commit prefix, minor bump + `uv lock`, full `tox`, TOOLS.md unchanged → Task 9. ✓

**Placeholder scan:** The only intentionally-deferred content is the concrete variables block in Task 8 Step 3 — this is by explicit planning decision (schema-driven & guarded; the exact shape is owned by AJDA-2351 and the test self-skips until the live schema advertises it). All code steps contain runnable code.

**Type consistency:** `resolve_flow_schema(client, flow_type) -> JsonDict`; `get_schema_as_markdown(client, flow_type) -> str`; `validate_flow_configuration_against_schema(flow, flow_type, schema=None, ...)`; cache accessors `get_cached_flow_schema(str) -> JsonDict | None` / `cache_flow_schema(str, JsonDict) -> None`. Names match across Tasks 2–7.
