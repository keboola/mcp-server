# Configuration Diff Viewer MCP App — Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add a `preview_config_diff` MCP App tool that shows a side-by-side visual diff of configuration changes before mutation, reusing the existing preview.py diff computation logic.

**Architecture:** The MCP tool calls a shared `compute_config_diff()` function extracted from preview.py, returning `originalConfig` and `updatedConfig` as JSON dicts. The HTML app uses jsondiffpatch (CDN) to compute and render the structural diff client-side in a side-by-side view.

**Tech Stack:** Python (FastMCP, Pydantic), jsondiffpatch (CDN), MCP Apps SDK 1.1.2

---

## Task 1: Refactor preview.py — extract `compute_config_diff()`

Extract the core diff computation from the HTTP handler into a reusable function that both the HTTP endpoint and the new MCP tool can call.

**Files:**
- Modify: `src/keboola_mcp_server/preview.py:227-308`

**Step 1: Extract the shared function**

Add this function above the existing `preview_config_diff` HTTP handler (around line 227). This is the core logic from lines 241-306 pulled into its own function:

```python
async def compute_config_diff(
    tool_name: str,
    tool_params: dict[str, Any],
    client: KeboolaClient,
    workspace_manager: WorkspaceManager,
) -> PreviewConfigDiffResp:
    """
    Compute a configuration diff preview for the given mutation tool.

    Shared by both the HTTP preview endpoint and the MCP preview_config_diff tool.

    :param tool_name: Name of the mutation tool (e.g. 'update_config', 'modify_flow').
    :param tool_params: Parameters that would be passed to the mutation tool.
    :param client: KeboolaClient instance for API operations.
    :param workspace_manager: WorkspaceManager instance for workspace operations.
    :return: PreviewConfigDiffResp with original and updated configs.
    """
    preview_rq = PreviewConfigDiffRq(tool_name=tool_name, tool_params=tool_params)
    coordinates = await _extract_coordinates(preview_rq.tool_name, preview_rq.tool_params, workspace_manager)
    mutator_fn, mutator_params = _prepare_mutator(preview_rq, client, workspace_manager)

    try:
        original_config, new_config, *mutator_preview = await mutator_fn(**mutator_params)
        mutator_preview = mutator_preview[0] if mutator_preview else None
        if isinstance(original_config, BaseModel):
            original_config = original_config.model_dump()

        updated_config = copy.deepcopy(original_config)
        updated_config['configuration'] = new_config
        if name := preview_rq.tool_params.get('name'):
            updated_config['name'] = name
        description = preview_rq.tool_params.get('description')
        if description:
            updated_config['description'] = description
        if (is_disabled := preview_rq.tool_params.get('is_disabled')) is not None:
            updated_config['isDisabled'] = is_disabled
        if change_description := preview_rq.tool_params.get('change_description'):
            updated_config['changeDescription'] = change_description
        if mutator_preview is not None and isinstance(mutator_preview, dict):
            for key, value in mutator_preview.items():
                if key.startswith('original_'):
                    original_config[key.replace('original_', '')] = value
                elif key.startswith('updated_'):
                    updated_config[key.replace('updated_', '')] = value
                else:
                    raise ValueError(f'Invalid mutator preview key: "{key}"')

        return PreviewConfigDiffResp(
            coordinates=coordinates,
            original_config=original_config,
            updated_config=updated_config,
            is_valid=True,
            validation_errors=None,
        )

    except (pydantic.ValidationError, jsonschema.ValidationError, ValueError) as ex:
        LOG.exception(f'[compute_config_diff] {ex}')
        return PreviewConfigDiffResp(
            coordinates=coordinates,
            original_config={},
            updated_config={},
            is_valid=False,
            validation_errors=[str(ex)],
        )
```

**Step 2: Refactor the HTTP handler to call the shared function**

Replace the body of `preview_config_diff()` (lines 228-308) with:

```python
async def preview_config_diff(rq: Request) -> Response:
    preview_rq = PreviewConfigDiffRq.model_validate(await rq.json())

    LOG.info(f'[preview_config_diff] {preview_rq}')

    server_state = ServerState.from_starlette(rq.app)
    config = SessionStateMiddleware.apply_request_config(rq, server_state.config)
    state = await SessionStateMiddleware.create_session_state(config, server_state.runtime_info, readonly=True)
    client = KeboolaClient.from_state(state)
    workspace_manager = WorkspaceManager.from_state(state)

    # Validate tool params against JSON schema (HTTP-only, MCP tools validate via their own schema)
    if tool_input_schema := rq.app.state.mcp_tools_input_schema.get(preview_rq.tool_name):
        is_valid, validation_errors = await _validate_tool_params(
            tool_name=preview_rq.tool_name,
            tool_params=preview_rq.tool_params,
            tool_input_schema=tool_input_schema,
        )

        if not is_valid:
            coordinates = await _extract_coordinates(
                preview_rq.tool_name, preview_rq.tool_params, workspace_manager
            )
            preview_resp = PreviewConfigDiffResp(
                coordinates=coordinates,
                original_config={},
                updated_config={},
                is_valid=False,
                validation_errors=[validation_errors],
            )
            return JSONResponse(preview_resp.model_dump(by_alias=True, exclude_none=True))
    else:
        LOG.warning(f'[preview_config_diff] No input schema found for tool "{preview_rq.tool_name}"')

    preview_resp = await compute_config_diff(
        tool_name=preview_rq.tool_name,
        tool_params=preview_rq.tool_params,
        client=client,
        workspace_manager=workspace_manager,
    )

    return JSONResponse(preview_resp.model_dump(by_alias=True, exclude_none=True))
```

**Step 3: Run existing preview tests to verify refactor**

Run: `source 3.10.venv/bin/activate && pytest tests/test_preview.py -v`
Expected: All existing tests PASS (no behavior change)

**Step 4: Commit**

```bash
git add src/keboola_mcp_server/preview.py
git commit -m "AI-2690: extract compute_config_diff from preview HTTP handler"
```

---

## Task 2: Create `src/keboola_mcp_server/tools/config_diff.py`

The MCP tool module. Single model-visible `preview_config_diff` tool.

**Files:**
- Create: `src/keboola_mcp_server/tools/config_diff.py`
- Modify: `src/keboola_mcp_server/tools/constants.py`

**Step 1: Add the tag constant**

In `src/keboola_mcp_server/tools/constants.py`, add at the end:

```python
CONFIG_DIFF_TAG = 'config_diff'
```

**Step 2: Create the tool module**

Create `src/keboola_mcp_server/tools/config_diff.py`:

```python
"""
MCP App tool for configuration diff preview.

The ``preview_config_diff`` tool is model-visible — the LLM calls it with
a mutation tool name and its parameters, and the MCP App renders a side-by-side
diff of the original vs updated configuration before the mutation is applied.
"""

import importlib.resources
import logging
from typing import Any

from fastmcp import Context
from fastmcp.tools import FunctionTool
from mcp.types import ToolAnnotations

from keboola_mcp_server.apps import APP_RESOURCE_MIME_TYPE, build_app_resource_meta, build_app_tool_meta
from keboola_mcp_server.clients.client import KeboolaClient
from keboola_mcp_server.errors import tool_errors
from keboola_mcp_server.mcp import KeboolaMcpServer
from keboola_mcp_server.preview import compute_config_diff
from keboola_mcp_server.tools.constants import CONFIG_DIFF_TAG
from keboola_mcp_server.workspace import WorkspaceManager

LOG = logging.getLogger(__name__)

CONFIG_DIFF_RESOURCE_URI = 'ui://keboola/config-diff'

SUPPORTED_TOOLS = frozenset({
    'update_config',
    'update_config_row',
    'update_sql_transformation',
    'update_flow',
    'modify_flow',
    'modify_data_app',
})


def add_config_diff_tools(mcp: KeboolaMcpServer) -> None:
    """Register the Config Diff MCP App tool and resource."""
    html_content = importlib.resources.read_text('keboola_mcp_server.apps', 'config_diff.html')

    resource_meta = build_app_resource_meta(
        csp_resource_domains=['https://unpkg.com'],
    )

    @mcp.resource(
        CONFIG_DIFF_RESOURCE_URI,
        name='Config Diff',
        description='Side-by-side configuration diff viewer.',
        mime_type=APP_RESOURCE_MIME_TYPE,
        meta=resource_meta,
    )
    def config_diff_resource() -> str:
        return html_content

    app_meta = build_app_tool_meta(
        resource_uri=CONFIG_DIFF_RESOURCE_URI,
    )

    mcp.add_tool(
        FunctionTool.from_function(
            preview_config_diff,
            annotations=ToolAnnotations(readOnlyHint=True),
            tags={CONFIG_DIFF_TAG},
            meta=app_meta,
        )
    )

    LOG.info('Config Diff MCP App tool registered.')


@tool_errors()
async def preview_config_diff(
    ctx: Context,
    tool_name: str,
    tool_params: dict[str, Any],
) -> dict[str, Any]:
    """
    Preview configuration changes before applying a mutation.

    Shows a side-by-side diff of the original and updated configuration.
    Call this BEFORE calling any mutation tool (update_config, update_config_row,
    update_sql_transformation, update_flow, modify_flow, modify_data_app) to
    let the user review changes before they are applied.

    Pass the same tool_name and tool_params you would use for the mutation tool.

    EXAMPLES:
    - tool_name="update_config", tool_params={"component_id": "keboola.ex-aws-s3",
      "configuration_id": "123", "change_description": "Update bucket",
      "parameter_updates": [{"op": "set", "path": "bucket", "value": "new-bucket"}]}
    - tool_name="modify_flow", tool_params={"configuration_id": "456",
      "flow_type": "keboola.orchestrator", "change_description": "Update phases", ...}
    """
    if tool_name not in SUPPORTED_TOOLS:
        return {
            'coordinates': {},
            'originalConfig': {},
            'updatedConfig': {},
            'isValid': False,
            'validationErrors': [
                f'Unsupported tool_name "{tool_name}". '
                f'Supported: {", ".join(sorted(SUPPORTED_TOOLS))}'
            ],
        }

    client = KeboolaClient.from_state(ctx.session.state)
    workspace_manager = WorkspaceManager.from_state(ctx.session.state)

    result = await compute_config_diff(
        tool_name=tool_name,
        tool_params=tool_params,
        client=client,
        workspace_manager=workspace_manager,
    )

    return result.model_dump(by_alias=True, exclude_none=True)
```

**Step 3: Commit**

```bash
git add src/keboola_mcp_server/tools/constants.py src/keboola_mcp_server/tools/config_diff.py
git commit -m "AI-2690: add preview_config_diff MCP App tool"
```

---

## Task 3: Create `src/keboola_mcp_server/apps/config_diff.html`

The HTML app that renders the side-by-side diff using jsondiffpatch.

**Files:**
- Create: `src/keboola_mcp_server/apps/config_diff.html`

**Key implementation details:**

- Load jsondiffpatch from `https://unpkg.com/jsondiffpatch@0.6.0/dist/jsondiffpatch.umd.min.js`
- Load jsondiffpatch CSS from `https://unpkg.com/jsondiffpatch@0.6.0/dist/formatters-styles/html.css` and `annotated.css`
- Load MCP Apps SDK from `https://unpkg.com/@anthropic-ai/sdk-mcp-apps@0.0.8/dist/index.js`
  - **Important:** Check the actual SDK import path used in `data_chart.html` and match it exactly
- No header (host card provides title)
- Container with `id="diff-container"` for the rendered diff
- Loading spinner, error display, "no changes" message as states
- Theme: CSS custom properties for light/dark
- jsondiffpatch `html` formatter produces side-by-side HTML natively

**JS flow:**
1. `app.ontoolresult` → extract `structuredContent`
2. If `!isValid` → show `validationErrors` in error panel
3. `const delta = jsondiffpatch.diff(originalConfig, updatedConfig)`
4. If no delta → show "No changes detected"
5. `document.getElementById('diff-container').innerHTML = jsondiffpatch.formatters.html.format(delta, originalConfig)`
6. Call `jsondiffpatch.formatters.html.showUnchanged()` to show context around changes
7. `app.onhostcontextchanged` → toggle `.dark`/`.light` class on body

**CSS overrides for jsondiffpatch:**
- Override background colors to use CSS custom properties
- Make `.jsondiffpatch-delta` full-width
- Adjust font sizes for readability in iframe
- Dark mode: invert the green/red tones for better contrast on dark backgrounds

**Reference** the `data_chart.html` SDK import pattern exactly. Check the actual file at `src/keboola_mcp_server/apps/data_chart.html` for the import URL.

**Step 1: Create the HTML file**

Write the complete HTML file following the patterns above. Keep it self-contained.

**Step 2: Verify it loads**

```bash
python -c "import importlib.resources; print(len(importlib.resources.read_text('keboola_mcp_server.apps', 'config_diff.html')))"
```
Expected: Prints a number > 0 (file loads from package)

**Step 3: Commit**

```bash
git add src/keboola_mcp_server/apps/config_diff.html
git commit -m "AI-2690: add config diff HTML app with jsondiffpatch rendering"
```

---

## Task 4: Wire into server and generate_tool_docs

**Files:**
- Modify: `src/keboola_mcp_server/server.py:29-251`
- Modify: `src/keboola_mcp_server/generate_tool_docs.py:16-196`

**Step 1: Add to server.py**

Add import (after the `from keboola_mcp_server.tools.components import add_component_tools` line, keep imports alphabetically sorted):

```python
from keboola_mcp_server.tools.config_diff import add_config_diff_tools
```

Add call (after `add_component_tools(mcp)`, keep calls in alphabetical order):

```python
    add_config_diff_tools(mcp)
```

**Step 2: Add to generate_tool_docs.py**

Add import (after the `from keboola_mcp_server.tools.components.tools import COMPONENT_TOOLS_TAG` line):

```python
from keboola_mcp_server.tools.config_diff import CONFIG_DIFF_TAG
```

Wait — `CONFIG_DIFF_TAG` is in `constants.py`, not `config_diff.py`. Use:

```python
from keboola_mcp_server.tools.constants import CONFIG_DIFF_TAG
```

Actually, check what the data_chart pattern does. In `generate_tool_docs.py:18`, `DATA_CHART_TAG` is imported from `keboola_mcp_server.tools.data_chart`. Follow the same pattern — import from the tool module:

```python
from keboola_mcp_server.tools.config_diff import CONFIG_DIFF_TAG
```

But wait — `CONFIG_DIFF_TAG` is defined in `constants.py` and re-used in `config_diff.py`. The import should work either way. Import from `config_diff.py` to be consistent with the data_chart pattern. **But** `config_diff.py` imports `CONFIG_DIFF_TAG` from constants, it doesn't define it. So either:
- Re-export it from `config_diff.py` (add to module-level), or
- Import directly from `constants.py`

The cleanest approach: define `CONFIG_DIFF_TAG` in `config_diff.py` (like `DATA_CHART_TAG` is defined in `data_chart.py`), and do NOT put it in `constants.py`. This avoids the re-export issue and matches the existing pattern.

**Revised Step 1 for Task 2:** Do NOT add `CONFIG_DIFF_TAG` to `constants.py`. Instead define it directly in `config_diff.py`:

```python
CONFIG_DIFF_TAG = 'config_diff'
```

Then in `generate_tool_docs.py`, import:

```python
from keboola_mcp_server.tools.config_diff import CONFIG_DIFF_TAG
```

Add the category to the list (alphabetical order, after `ToolCategory('Component Tools', COMPONENT_TOOLS_TAG)`):

```python
            ToolCategory('Config Diff App', CONFIG_DIFF_TAG),
```

**Step 3: Run the server creation to verify wiring**

```bash
source 3.10.venv/bin/activate && python -c "
from keboola_mcp_server.config import Config, ServerRuntimeInfo
from keboola_mcp_server.server import create_server
import asyncio

async def check():
    s = create_server(Config(), runtime_info=ServerRuntimeInfo(transport='stdio'))
    tools = await s.get_tools()
    assert 'preview_config_diff' in tools, f'Tool not found in: {list(tools.keys())}'
    print('OK: preview_config_diff registered')

asyncio.run(check())
"
```
Expected: `OK: preview_config_diff registered`

**Step 4: Commit**

```bash
git add src/keboola_mcp_server/server.py src/keboola_mcp_server/generate_tool_docs.py
git commit -m "AI-2690: wire config diff tool into server and generate_tool_docs"
```

---

## Task 5: Tests — tool unit tests

**Files:**
- Create: `tests/tools/test_config_diff.py`

**Step 1: Write the tests**

```python
import copy

import pytest
from fastmcp import Context

from keboola_mcp_server.clients.client import KeboolaClient
from keboola_mcp_server.tools.config_diff import preview_config_diff


ORIGINAL_CONFIG = {
    'id': 'config-123',
    'name': 'My Config',
    'description': 'Original description',
    'configuration': {
        'parameters': {
            'bucket': 's3-bucket',
            'prefix': '/data',
        }
    },
}


@pytest.mark.asyncio
async def test_preview_config_diff_returns_valid_diff(mcp_context_client: Context, mocker):
    """Test that a valid update_config preview returns originalConfig and updatedConfig."""
    mock_client = KeboolaClient.from_state(mcp_context_client.session.state)

    async def mock_config_detail(**kwargs):
        return copy.deepcopy(ORIGINAL_CONFIG)

    mock_client.storage_client.configuration_detail = mocker.AsyncMock(side_effect=mock_config_detail)

    from keboola_mcp_server.clients.storage import ComponentAPIResponse

    async def mock_fetch_component(**kwargs):
        return ComponentAPIResponse.model_validate({
            'id': 'keboola.ex-test',
            'name': 'Test',
            'type': 'extractor',
            'configurationSchema': {},
            'component_flags': [],
        })

    mocker.patch(
        'keboola_mcp_server.tools.components.tools.fetch_component',
        side_effect=mock_fetch_component,
    )

    result = await preview_config_diff(
        ctx=mcp_context_client,
        tool_name='update_config',
        tool_params={
            'component_id': 'keboola.ex-test',
            'configuration_id': 'config-123',
            'change_description': 'Update bucket',
            'parameter_updates': [
                {'op': 'set', 'path': 'bucket', 'value': 'new-bucket'},
            ],
        },
    )

    assert result['isValid'] is True
    assert result['coordinates']['componentId'] == 'keboola.ex-test'
    assert result['coordinates']['configurationId'] == 'config-123'
    assert result['originalConfig']['configuration']['parameters']['bucket'] == 's3-bucket'
    assert result['updatedConfig']['configuration']['parameters']['bucket'] == 'new-bucket'


@pytest.mark.asyncio
async def test_preview_config_diff_unsupported_tool(mcp_context_client: Context):
    """Test that an unsupported tool_name returns isValid=False."""
    result = await preview_config_diff(
        ctx=mcp_context_client,
        tool_name='create_config',
        tool_params={},
    )

    assert result['isValid'] is False
    assert len(result['validationErrors']) == 1
    assert 'Unsupported tool_name' in result['validationErrors'][0]


@pytest.mark.asyncio
async def test_preview_config_diff_mutator_error(mcp_context_client: Context, mocker):
    """Test that a mutator error returns isValid=False with error message."""
    mock_client = KeboolaClient.from_state(mcp_context_client.session.state)

    async def mock_config_detail(**kwargs):
        raise ValueError('Configuration not found')

    mock_client.storage_client.configuration_detail = mocker.AsyncMock(side_effect=mock_config_detail)

    from keboola_mcp_server.clients.storage import ComponentAPIResponse

    async def mock_fetch_component(**kwargs):
        return ComponentAPIResponse.model_validate({
            'id': 'keboola.ex-test',
            'name': 'Test',
            'type': 'extractor',
            'configurationSchema': {},
            'component_flags': [],
        })

    mocker.patch(
        'keboola_mcp_server.tools.components.tools.fetch_component',
        side_effect=mock_fetch_component,
    )

    result = await preview_config_diff(
        ctx=mcp_context_client,
        tool_name='update_config',
        tool_params={
            'component_id': 'keboola.ex-test',
            'configuration_id': 'config-123',
            'change_description': 'Test',
            'parameter_updates': [{'op': 'set', 'path': 'x', 'value': 'y'}],
        },
    )

    assert result['isValid'] is False
    assert 'Configuration not found' in result['validationErrors'][0]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    'tool_name',
    [
        'update_config',
        'update_config_row',
        'update_sql_transformation',
        'update_flow',
        'modify_flow',
        'modify_data_app',
    ],
)
async def test_preview_config_diff_supported_tools(mcp_context_client: Context, tool_name: str):
    """Test that all 6 supported tool names are accepted (not rejected as unsupported)."""
    # We only check that the tool_name validation passes (not the full mutator flow).
    # Any error from the mutator itself means the tool_name was accepted.
    result = await preview_config_diff(
        ctx=mcp_context_client,
        tool_name=tool_name,
        tool_params={},
    )

    # Should NOT be "Unsupported tool_name" error
    if not result['isValid'] and result.get('validationErrors'):
        assert 'Unsupported tool_name' not in result['validationErrors'][0]
```

**Step 2: Run the tests**

Run: `source 3.10.venv/bin/activate && pytest tests/tools/test_config_diff.py -v`
Expected: All tests PASS

**Step 3: Commit**

```bash
git add tests/tools/test_config_diff.py
git commit -m "AI-2690: add preview_config_diff tool unit tests"
```

---

## Task 6: Tests — HTML smoke tests

**Files:**
- Create: `tests/apps/test_config_diff_html.py`

**Step 1: Write the tests**

```python
import importlib.resources


def _load_html() -> str:
    """Load the config diff HTML from the package."""
    return importlib.resources.read_text('keboola_mcp_server.apps', 'config_diff.html')


def test_config_diff_html_is_valid_mcp_app():
    """Verify the HTML contains required MCP App SDK wiring."""
    html = _load_html()
    assert '<!DOCTYPE html>' in html
    assert 'app.connect()' in html
    assert 'ontoolresult' in html


def test_config_diff_html_loads_jsondiffpatch():
    """Verify the HTML loads jsondiffpatch library."""
    html = _load_html()
    assert 'jsondiffpatch' in html
    assert 'unpkg.com' in html


def test_config_diff_html_has_diff_container():
    """Verify the HTML has a container for the diff output."""
    html = _load_html()
    assert 'diff-container' in html


def test_config_diff_html_supports_dark_mode():
    """Verify the HTML supports dark mode theming."""
    html = _load_html()
    assert 'prefers-color-scheme' in html or 'color-scheme' in html


def test_config_diff_html_handles_errors():
    """Verify the HTML has error display handling."""
    html = _load_html()
    assert 'isValid' in html or 'validationErrors' in html
```

**Step 2: Run the tests**

Run: `source 3.10.venv/bin/activate && pytest tests/apps/test_config_diff_html.py -v`
Expected: All tests PASS

**Step 3: Commit**

```bash
git add tests/apps/test_config_diff_html.py
git commit -m "AI-2690: add config diff HTML smoke tests"
```

---

## Task 7: Tests — registration and server integration

**Files:**
- Modify: `tests/apps/test_registration.py`
- Modify: `tests/test_server.py`

**Step 1: Add config diff registration tests**

Append to `tests/apps/test_registration.py` after the data chart section:

```python
# ── Config Diff App ──


@pytest.mark.asyncio
async def test_config_diff_tool_registered(mcp_server):
    """Test that preview_config_diff tool is registered."""
    tools = await mcp_server.get_tools()
    assert 'preview_config_diff' in tools


@pytest.mark.asyncio
async def test_config_diff_resource_registered(mcp_server):
    """Test that the ui://keboola/config-diff resource is registered."""
    resources = await mcp_server.get_resources()
    resource_keys = list(resources.keys())
    assert any('config-diff' in str(k) for k in resource_keys), f'Expected config-diff resource, got: {resource_keys}'


@pytest.mark.asyncio
async def test_config_diff_tool_has_app_meta(mcp_server):
    """Test that preview_config_diff tool has _meta.ui with resourceUri."""
    tools = await mcp_server.get_tools()
    tool = tools['preview_config_diff']
    meta = tool.meta
    assert meta is not None
    assert 'ui' in meta
    assert meta['ui']['resourceUri'] == 'ui://keboola/config-diff'


@pytest.mark.asyncio
async def test_config_diff_tool_meta_has_no_csp(mcp_server):
    """CSP must be on the resource, not the tool (per MCP Apps spec)."""
    tools = await mcp_server.get_tools()
    tool = tools['preview_config_diff']
    assert 'csp' not in tool.meta['ui']


@pytest.mark.asyncio
async def test_config_diff_resource_meta_has_csp(mcp_server):
    """The config-diff resource should carry CSP for unpkg.com."""
    resources = await mcp_server.get_resources()
    resource = None
    for key, res in resources.items():
        if 'config-diff' in str(key):
            resource = res
            break
    assert resource is not None
    meta = resource.meta
    assert meta is not None
    assert 'ui' in meta
    assert 'csp' in meta['ui']
    assert 'https://unpkg.com' in meta['ui']['csp']['resourceDomains']
```

**Step 2: Add to test_server.py tool list**

In `tests/test_server.py`, find the `test_list_tools` method. Add `'preview_config_diff'` in alphabetical order (after `'modify_flow'` and before `'query_data'`).

**Step 3: Add to test_server.py parametrize**

In the `test_tool_annotations_tags_values` parametrize block, add after the `# data chart` entry:

```python
        # config diff
        ('preview_config_diff', True, None, None, {CONFIG_DIFF_TAG}),
```

Add the import at the top of `test_server.py`:

```python
from keboola_mcp_server.tools.config_diff import CONFIG_DIFF_TAG
```

**Step 4: Run all tests**

Run: `source 3.10.venv/bin/activate && pytest tests/apps/test_registration.py tests/test_server.py -v`
Expected: All tests PASS

**Step 5: Commit**

```bash
git add tests/apps/test_registration.py tests/test_server.py
git commit -m "AI-2690: add config diff registration and server integration tests"
```

---

## Task 8: Regenerate TOOLS.md and run full tox

**Files:**
- Regenerate: `TOOLS.md`

**Step 1: Regenerate TOOLS.md**

```bash
source 3.10.venv/bin/activate && python -m keboola_mcp_server.generate_tool_docs
```

**Step 2: Run full tox**

```bash
source 3.10.venv/bin/activate && tox
```

Expected: All 4 environments pass (pytest, black, flake8, check-tools-docs).

If flake8 fails with isort (I001) errors, fix import ordering. If black fails, run `black` on the affected files. If check-tools-docs fails, commit TOOLS.md first then re-run.

**Step 3: Commit TOOLS.md**

```bash
git add TOOLS.md
git commit -m "AI-2690: regenerate TOOLS.md with config diff tool"
```

**Step 4: Verify check-tools-docs passes**

```bash
source 3.10.venv/bin/activate && tox -e check-tools-docs
```

Expected: PASS
