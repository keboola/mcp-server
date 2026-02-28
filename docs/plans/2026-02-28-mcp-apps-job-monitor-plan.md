# MCP Apps: Job Monitor Dashboard — Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add an interactive Job Monitor MCP App that renders a live-updating dashboard with jobs table and expandable log viewer inside the conversation.

**Architecture:** A new `job_monitor` tool returns structured job data and links to a `ui://keboola/job-monitor` HTML resource. The HTML app polls for updates every 5 seconds via an app-only `poll_job_monitor` tool. Built on FastMCP 2.14.1 using raw MCP primitives — `FunctionTool.from_function(meta=...)` for `_meta.ui` and `mcp.resource()` with `mime_type='text/html;profile=mcp-app'` for the UI resource.

**Tech Stack:** Python 3.10, FastMCP 2.14.1, MCP Apps extension protocol, vanilla HTML/CSS/JS, `@modelcontextprotocol/ext-apps` SDK from CDN

---

### Task 1: Extract shared job+log fetching helpers from jobs.py

The existing `get_jobs` tool has inline job-fetching and log-fetching logic. Extract these into
reusable helper functions so `job_monitor` and `poll_job_monitor` can call them without duplication.

**Files:**
- Modify: `src/keboola_mcp_server/tools/jobs.py`
- Modify: `tests/tools/test_jobs.py`

**Step 1: Write tests verifying existing behavior still passes**

No new tests needed — the existing tests in `tests/tools/test_jobs.py` already cover `get_jobs`.
Run them to establish baseline:

Run: `source 3.10.venv/bin/activate && pytest tests/tools/test_jobs.py -v`
Expected: All 21 tests PASS

**Step 2: Extract `fetch_job_details` helper**

In `src/keboola_mcp_server/tools/jobs.py`, add a new function **above** `get_jobs` (after the model
definitions, around line 183):

```python
async def fetch_job_details(
    client: KeboolaClient,
    links_manager: ProjectLinksManager,
    job_ids: Sequence[str],
    include_logs: bool = False,
    log_tail_lines: int = 50,
    log_event_types: Optional[Sequence[Literal['info', 'warn', 'error', 'success']]] = None,
) -> list[JobDetail]:
    """
    Fetch full details (and optionally logs) for a list of job IDs.

    Shared helper used by both get_jobs (MODE 1) and job_monitor tools.
    """
    async def _fetch_one(job_id: str) -> JobDetail:
        raw_job = await client.jobs_queue_client.get_job_detail(job_id)
        links = links_manager.get_job_links(job_id)
        LOG.info(f'Found job details for {job_id}.' if raw_job else f'Job {job_id} not found.')
        return JobDetail.model_validate(raw_job | {'links': links})

    results = await process_concurrently(job_ids, _fetch_one)
    jobs = unwrap_results(results, 'Failed to fetch one or more jobs')

    if include_logs:

        async def _fetch_logs(job: JobDetail) -> JobDetail:
            if not job.id:
                return job
            raw_events = await client.storage_client.list_events(
                job_id=job.id,
                limit=log_tail_lines,
            )
            if log_event_types:
                type_set = set(log_event_types)
                raw_events = [e for e in raw_events if e.get('type') in type_set]
            raw_events.reverse()
            job.logs = [JobLogEvent.model_validate(e) for e in raw_events]
            return job

        log_results = await process_concurrently(jobs, _fetch_logs)
        jobs = unwrap_results(log_results, 'Failed to fetch logs for one or more jobs')

    return jobs
```

**Step 3: Update `get_jobs` to use the helper**

Replace the inline logic in the `if job_ids:` branch (lines ~365-399) of `get_jobs` with:

```python
    if job_ids:
        jobs = await fetch_job_details(
            client=client,
            links_manager=links_manager,
            job_ids=job_ids,
            include_logs=include_logs,
            log_tail_lines=log_tail_lines,
            log_event_types=log_event_types,
        )
        LOG.info(f'Retrieved full details for {len(jobs)} jobs.')
        return GetJobsDetailOutput(jobs=jobs)
```

**Step 4: Run tests to verify refactor is behavior-preserving**

Run: `source 3.10.venv/bin/activate && pytest tests/tools/test_jobs.py -v`
Expected: All 21 tests PASS (identical to Step 1)

**Step 5: Commit**

```bash
git add src/keboola_mcp_server/tools/jobs.py
git commit -m "AI-XXXX: extract fetch_job_details helper from get_jobs"
```

---

### Task 2: Create the apps registration helper module

Create `src/keboola_mcp_server/apps/__init__.py` with helper functions to register MCP App
resources and tools using FastMCP 2.14.1's existing `meta` parameter.

**Files:**
- Create: `src/keboola_mcp_server/apps/__init__.py`
- Create: `tests/apps/__init__.py` (empty)
- Create: `tests/apps/test_app_helpers.py`

**Step 1: Write the failing tests**

Create `tests/apps/__init__.py` (empty file).

Create `tests/apps/test_app_helpers.py`:

```python
import pytest
from fastmcp.tools import FunctionTool
from mcp.types import ToolAnnotations

from keboola_mcp_server.apps import build_app_tool_meta
from keboola_mcp_server.mcp import KeboolaMcpServer


def test_build_app_tool_meta_model_visible():
    """Test that build_app_tool_meta generates correct _meta.ui for model-visible tools."""
    meta = build_app_tool_meta(
        resource_uri='ui://keboola/job-monitor',
        csp_resource_domains=['https://unpkg.com'],
    )
    assert meta == {
        'ui': {
            'resourceUri': 'ui://keboola/job-monitor',
            'csp': {
                'resource_domains': ['https://unpkg.com'],
            },
        },
    }


def test_build_app_tool_meta_app_only():
    """Test that build_app_tool_meta includes visibility for app-only tools."""
    meta = build_app_tool_meta(
        resource_uri='ui://keboola/job-monitor',
        visibility=['app'],
        csp_resource_domains=['https://unpkg.com'],
    )
    assert meta['ui']['visibility'] == ['app']


def test_build_app_tool_meta_no_csp():
    """Test that build_app_tool_meta works without CSP domains."""
    meta = build_app_tool_meta(resource_uri='ui://keboola/test')
    assert 'csp' not in meta['ui']
    assert meta['ui']['resourceUri'] == 'ui://keboola/test'
```

**Step 2: Run tests to verify they fail**

Run: `source 3.10.venv/bin/activate && pytest tests/apps/test_app_helpers.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'keboola_mcp_server.apps'`

**Step 3: Write the implementation**

Create `src/keboola_mcp_server/apps/__init__.py`:

```python
"""
Helpers for registering MCP App resources and tools.

MCP Apps allow tools to render interactive HTML UIs in sandboxed iframes.
This module provides utilities for wiring the MCP Apps protocol on FastMCP 2.x
using standard MCP primitives (tool meta and resources).

See: https://modelcontextprotocol.io/docs/extensions/apps
"""

import logging
from typing import Any

LOG = logging.getLogger(__name__)

APP_RESOURCE_MIME_TYPE = 'text/html;profile=mcp-app'


def build_app_tool_meta(
    resource_uri: str,
    visibility: list[str] | None = None,
    csp_resource_domains: list[str] | None = None,
    csp_connect_domains: list[str] | None = None,
) -> dict[str, Any]:
    """
    Build the ``_meta.ui`` dict for an MCP App tool.

    :param resource_uri: The ``ui://`` URI linking the tool to its HTML resource.
    :param visibility: Who can call the tool — ``["model"]``, ``["app"]``, or ``["model", "app"]``.
        Default (None) means model-visible only.
    :param csp_resource_domains: Allowed domains for loading external scripts/styles.
    :param csp_connect_domains: Allowed domains for fetch/XHR from the app.
    :return: A dict suitable for passing as ``meta=`` to ``FunctionTool.from_function()``.
    """
    ui: dict[str, Any] = {
        'resourceUri': resource_uri,
    }
    if visibility:
        ui['visibility'] = visibility

    csp: dict[str, Any] = {}
    if csp_resource_domains:
        csp['resource_domains'] = csp_resource_domains
    if csp_connect_domains:
        csp['connect_domains'] = csp_connect_domains
    if csp:
        ui['csp'] = csp

    return {'ui': ui}
```

**Step 4: Run tests to verify they pass**

Run: `source 3.10.venv/bin/activate && pytest tests/apps/test_app_helpers.py -v`
Expected: 3 tests PASS

**Step 5: Commit**

```bash
git add src/keboola_mcp_server/apps/__init__.py tests/apps/__init__.py tests/apps/test_app_helpers.py
git commit -m "AI-XXXX: add MCP Apps registration helper module"
```

---

### Task 3: Create the Job Monitor HTML app

Create the self-contained HTML file that renders the Job Monitor dashboard.

**Files:**
- Create: `src/keboola_mcp_server/apps/job_monitor.html`
- Create: `tests/apps/test_job_monitor_html.py`

**Step 1: Write the HTML smoke test**

Create `tests/apps/test_job_monitor_html.py`:

```python
import importlib.resources

import pytest


def _load_html() -> str:
    """Load the job monitor HTML from the package."""
    return importlib.resources.read_text('keboola_mcp_server.apps', 'job_monitor.html')


def test_job_monitor_html_is_valid_mcp_app():
    """Verify the HTML contains required MCP App SDK wiring."""
    html = _load_html()
    assert '<!DOCTYPE html>' in html
    assert '@modelcontextprotocol/ext-apps' in html
    assert 'app.connect()' in html
    assert 'ontoolresult' in html


def test_job_monitor_html_has_poll_call():
    """Verify the HTML calls poll_job_monitor for auto-refresh."""
    html = _load_html()
    assert 'poll_job_monitor' in html


def test_job_monitor_html_supports_dark_mode():
    """Verify the HTML supports dark mode theming."""
    html = _load_html()
    assert 'prefers-color-scheme' in html or 'color-scheme' in html
```

**Step 2: Run tests to verify they fail**

Run: `source 3.10.venv/bin/activate && pytest tests/apps/test_job_monitor_html.py -v`
Expected: FAIL — file not found

**Step 3: Create the HTML app**

Create `src/keboola_mcp_server/apps/job_monitor.html`. This is a self-contained HTML file with
inline CSS and JavaScript. Key requirements:

- Import `App` from `https://unpkg.com/@modelcontextprotocol/ext-apps@0.4.0/app-with-deps`
- Call `app.connect()` on load
- Handle `app.ontoolresult` to render initial job data
- Every 5 seconds, call `app.callServerTool({ name: 'poll_job_monitor', arguments: {...} })` to
  fetch fresh data
- Pause polling when all jobs are in terminal states (`success`, `error`, `cancelled`, `terminated`,
  `warning`)
- Resume polling on manual refresh button click
- Render a table with columns: Status badge, Component, Config ID, Duration, Created Time
- Click a row to expand/collapse an inline log viewer
- Log events color-coded: info=grey, warn=amber, error=red, success=green
- Status badges: processing=blue+pulse, waiting/created=grey, success=green, error=red,
  warning=amber, terminating/terminated/cancelled=grey+strikethrough
- Support light/dark mode via `<meta name="color-scheme" content="light dark">` and
  `prefers-color-scheme` media query
- Responsive to container size

The HTML should store the current filter arguments (received from the initial tool result) so it
can pass them back in poll calls.

Note: The HTML will receive tool results as JSON. The data structure is:
```json
{
  "content": [{"type": "text", "text": "...text fallback..."}],
  "structuredContent": {
    "jobs": [...],
    "filterParams": { "job_ids": [...], "component_id": "...", "status": "...", "limit": 20 }
  }
}
```

The app should read `structuredContent` for rendering and pass `filterParams` back when polling.

**Step 4: Run tests to verify they pass**

Run: `source 3.10.venv/bin/activate && pytest tests/apps/test_job_monitor_html.py -v`
Expected: 3 tests PASS

**Step 5: Commit**

```bash
git add src/keboola_mcp_server/apps/job_monitor.html tests/apps/test_job_monitor_html.py
git commit -m "AI-XXXX: add Job Monitor HTML app with live polling and log viewer"
```

---

### Task 4: Create the job_monitor and poll_job_monitor tool functions

Create the server-side tool functions that serve data to the MCP App.

**Files:**
- Create: `src/keboola_mcp_server/tools/job_monitor.py`
- Create: `tests/tools/test_job_monitor.py`

**Step 1: Write failing tests**

Create `tests/tools/test_job_monitor.py`:

```python
import datetime
from typing import Any

import pytest
from fastmcp import Context
from pytest_mock import MockerFixture

from keboola_mcp_server.clients.client import KeboolaClient
from keboola_mcp_server.tools.job_monitor import job_monitor, poll_job_monitor


@pytest.fixture
def mock_job() -> dict[str, Any]:
    return {
        'id': '123',
        'status': 'success',
        'isFinished': True,
        'componentId': 'keboola.snowflake-transformation',
        'configId': '456',
        'createdTime': '2024-01-01T00:00:00Z',
        'startTime': '2024-01-01T00:00:05Z',
        'endTime': '2024-01-01T00:01:30Z',
        'durationSeconds': 85.0,
        'runId': '789',
        'configData': {},
        'result': {'message': 'ok'},
    }


@pytest.fixture
def mock_events() -> list[dict[str, Any]]:
    return [
        {'uuid': 'e2', 'message': 'Finished', 'type': 'success', 'created': '2024-01-01T00:01:30Z'},
        {'uuid': 'e1', 'message': 'Started', 'type': 'info', 'created': '2024-01-01T00:00:05Z'},
    ]


@pytest.mark.asyncio
async def test_job_monitor_returns_structured_content(
    mocker: MockerFixture, mcp_context_client: Context, mock_job: dict[str, Any], mock_events: list[dict[str, Any]]
):
    """Tests job_monitor returns both structured content and text fallback."""
    context = mcp_context_client
    keboola_client = KeboolaClient.from_state(context.session.state)
    keboola_client.jobs_queue_client.get_job_detail = mocker.AsyncMock(return_value=mock_job)
    keboola_client.storage_client.list_events = mocker.AsyncMock(return_value=mock_events)

    result = await job_monitor(ctx=context, job_ids=('123',))

    # Should have structured content with jobs and filterParams
    assert 'jobs' in result
    assert 'filterParams' in result
    assert len(result['jobs']) == 1
    assert result['jobs'][0]['id'] == '123'
    assert result['jobs'][0]['status'] == 'success'
    # Logs should be included by default
    assert result['jobs'][0]['logs'] is not None
    assert len(result['jobs'][0]['logs']) == 2


@pytest.mark.asyncio
async def test_job_monitor_filter_params_passthrough(
    mocker: MockerFixture, mcp_context_client: Context, mock_job: dict[str, Any], mock_events: list[dict[str, Any]]
):
    """Tests that job_monitor includes filterParams so the app can poll with the same filters."""
    context = mcp_context_client
    keboola_client = KeboolaClient.from_state(context.session.state)
    keboola_client.jobs_queue_client.get_job_detail = mocker.AsyncMock(return_value=mock_job)
    keboola_client.storage_client.list_events = mocker.AsyncMock(return_value=mock_events)

    result = await job_monitor(ctx=context, job_ids=('123',), log_tail_lines=100)

    assert result['filterParams']['job_ids'] == ['123']
    assert result['filterParams']['log_tail_lines'] == 100
    assert result['filterParams']['include_logs'] is True


@pytest.mark.asyncio
async def test_job_monitor_listing_mode(
    mocker: MockerFixture, mcp_context_client: Context, mock_job: dict[str, Any]
):
    """Tests job_monitor in listing mode (no job_ids) returns summaries with logs=None."""
    context = mcp_context_client
    keboola_client = KeboolaClient.from_state(context.session.state)
    keboola_client.jobs_queue_client.search_jobs_by = mocker.AsyncMock(return_value=[mock_job])

    result = await job_monitor(ctx=context, status='processing', limit=10)

    assert 'jobs' in result
    assert result['filterParams']['status'] == 'processing'
    assert result['filterParams']['limit'] == 10


@pytest.mark.asyncio
async def test_poll_job_monitor_returns_same_format(
    mocker: MockerFixture, mcp_context_client: Context, mock_job: dict[str, Any], mock_events: list[dict[str, Any]]
):
    """Tests poll_job_monitor returns the same format as job_monitor."""
    context = mcp_context_client
    keboola_client = KeboolaClient.from_state(context.session.state)
    keboola_client.jobs_queue_client.get_job_detail = mocker.AsyncMock(return_value=mock_job)
    keboola_client.storage_client.list_events = mocker.AsyncMock(return_value=mock_events)

    result = await poll_job_monitor(ctx=context, job_ids=('123',))

    assert 'jobs' in result
    assert 'filterParams' in result
    assert len(result['jobs']) == 1
```

**Step 2: Run tests to verify they fail**

Run: `source 3.10.venv/bin/activate && pytest tests/tools/test_job_monitor.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'keboola_mcp_server.tools.job_monitor'`

**Step 3: Write the implementation**

Create `src/keboola_mcp_server/tools/job_monitor.py`:

```python
"""
MCP App tools for the Job Monitor dashboard.

The ``job_monitor`` tool is model-visible (the LLM invokes it to open the dashboard).
The ``poll_job_monitor`` tool is app-only (called by the iframe for live updates).
"""

import logging
from typing import Annotated, Any, Literal, Optional, Sequence

from fastmcp import Context
from fastmcp.tools import FunctionTool
from mcp.types import ToolAnnotations
from pydantic import Field

from keboola_mcp_server.apps import APP_RESOURCE_MIME_TYPE, build_app_tool_meta
from keboola_mcp_server.clients.client import KeboolaClient
from keboola_mcp_server.errors import tool_errors
from keboola_mcp_server.links import ProjectLinksManager
from keboola_mcp_server.mcp import KeboolaMcpServer
from keboola_mcp_server.tools.jobs import (
    JOB_STATUS,
    JobDetail,
    JobListItem,
    fetch_job_details,
)

LOG = logging.getLogger(__name__)

JOB_MONITOR_TAG = 'job_monitor'
JOB_MONITOR_RESOURCE_URI = 'ui://keboola/job-monitor'


def add_job_monitor_tools(mcp: KeboolaMcpServer) -> None:
    """Register the Job Monitor MCP App tools and resource."""
    import importlib.resources

    # Register the HTML resource
    html_content = importlib.resources.read_text('keboola_mcp_server.apps', 'job_monitor.html')

    @mcp.resource(
        JOB_MONITOR_RESOURCE_URI,
        name='Job Monitor',
        description='Interactive job monitoring dashboard with live updates and log viewer.',
        mime_type=APP_RESOURCE_MIME_TYPE,
    )
    def job_monitor_resource() -> str:
        return html_content

    # Common meta for linking to the UI resource
    app_meta = build_app_tool_meta(
        resource_uri=JOB_MONITOR_RESOURCE_URI,
        csp_resource_domains=['https://unpkg.com'],
    )
    app_only_meta = build_app_tool_meta(
        resource_uri=JOB_MONITOR_RESOURCE_URI,
        visibility=['app'],
        csp_resource_domains=['https://unpkg.com'],
    )

    # Model-visible tool — LLM calls this to launch the dashboard
    mcp.add_tool(
        FunctionTool.from_function(
            job_monitor,
            annotations=ToolAnnotations(readOnlyHint=True),
            tags={JOB_MONITOR_TAG},
            meta=app_meta,
        )
    )

    # App-only tool — iframe calls this for live polling
    mcp.add_tool(
        FunctionTool.from_function(
            poll_job_monitor,
            annotations=ToolAnnotations(readOnlyHint=True),
            tags={JOB_MONITOR_TAG},
            meta=app_only_meta,
        )
    )

    LOG.info('Job Monitor MCP App tools added to the MCP server.')


def _build_result(
    jobs: list[Any],
    filter_params: dict[str, Any],
) -> dict[str, Any]:
    """Build the structured result dict returned by both job_monitor and poll_job_monitor."""
    serialized_jobs = []
    for job in jobs:
        if isinstance(job, JobDetail):
            job_dict = job.model_dump(by_alias=True, exclude_none=True)
        elif isinstance(job, JobListItem):
            job_dict = job.model_dump(by_alias=True, exclude_none=True)
            job_dict['logs'] = None
        else:
            job_dict = job
        serialized_jobs.append(job_dict)

    return {
        'jobs': serialized_jobs,
        'filterParams': filter_params,
    }


@tool_errors()
async def job_monitor(
    ctx: Context,
    job_ids: Annotated[
        Sequence[str],
        Field(
            description=(
                'IDs of specific jobs to monitor. When provided, shows full details with logs. '
                'When empty, lists recent jobs with optional filtering.'
            )
        ),
    ] = tuple(),
    component_id: Annotated[
        str,
        Field(description='Filter by component ID (only used when job_ids is empty).'),
    ] = None,
    status: Annotated[
        JOB_STATUS,
        Field(description='Filter by job status (only used when job_ids is empty).'),
    ] = None,
    limit: Annotated[
        int,
        Field(description='Max jobs to show (default 20, max 100).', ge=1, le=100),
    ] = 20,
    include_logs: Annotated[
        bool,
        Field(description='Include execution logs for each job. Default True.'),
    ] = True,
    log_tail_lines: Annotated[
        int,
        Field(description='Max log events per job (default 50, max 500).', ge=1, le=500),
    ] = 50,
) -> dict[str, Any]:
    """
    Opens an interactive Job Monitor dashboard.

    Displays a live-updating table of jobs with status badges, timing info, and expandable
    log viewers. The dashboard auto-refreshes every 5 seconds while jobs are still running.

    Use this tool when you want to visually monitor job execution, debug failed jobs,
    or show the user an overview of recent activity.

    EXAMPLES:
    - job_ids=["12345"] → monitor a specific job with live log updates
    - job_ids=["12345", "67890"] → monitor multiple jobs side by side
    - status="processing" → show all currently running jobs
    - component_id="keboola.snowflake-transformation" → show recent transformation jobs
    """
    client = KeboolaClient.from_state(ctx.session.state)
    links_manager = await ProjectLinksManager.from_client(client)

    filter_params = {
        'job_ids': list(job_ids),
        'component_id': component_id,
        'status': status,
        'limit': limit,
        'include_logs': include_logs,
        'log_tail_lines': log_tail_lines,
    }

    if job_ids:
        jobs = await fetch_job_details(
            client=client,
            links_manager=links_manager,
            job_ids=job_ids,
            include_logs=include_logs,
            log_tail_lines=log_tail_lines,
        )
        return _build_result(jobs, filter_params)

    # Listing mode
    _status = [status] if status else None
    raw_jobs = await client.jobs_queue_client.search_jobs_by(
        component_id=component_id,
        limit=limit,
        status=_status,
        sort_by='createdTime',
        sort_order='desc',
    )
    jobs = [JobListItem.model_validate(raw_job) for raw_job in raw_jobs]
    return _build_result(jobs, filter_params)


@tool_errors()
async def poll_job_monitor(
    ctx: Context,
    job_ids: Annotated[
        Sequence[str],
        Field(description='IDs of specific jobs to poll.'),
    ] = tuple(),
    component_id: Annotated[
        str,
        Field(description='Filter by component ID.'),
    ] = None,
    status: Annotated[
        JOB_STATUS,
        Field(description='Filter by job status.'),
    ] = None,
    limit: Annotated[
        int,
        Field(description='Max jobs to return.', ge=1, le=100),
    ] = 20,
    include_logs: Annotated[
        bool,
        Field(description='Include execution logs.'),
    ] = True,
    log_tail_lines: Annotated[
        int,
        Field(description='Max log events per job.', ge=1, le=500),
    ] = 50,
) -> dict[str, Any]:
    """
    Polls for fresh job data. Called by the Job Monitor app for live updates.
    This tool is app-only — it is not visible to the LLM.
    """
    # Delegates to the same logic as job_monitor
    return await job_monitor(
        ctx=ctx,
        job_ids=job_ids,
        component_id=component_id,
        status=status,
        limit=limit,
        include_logs=include_logs,
        log_tail_lines=log_tail_lines,
    )
```

**Step 4: Run tests to verify they pass**

Run: `source 3.10.venv/bin/activate && pytest tests/tools/test_job_monitor.py -v`
Expected: All 4 tests PASS

**Step 5: Commit**

```bash
git add src/keboola_mcp_server/tools/job_monitor.py tests/tools/test_job_monitor.py
git commit -m "AI-XXXX: add job_monitor and poll_job_monitor tool functions"
```

---

### Task 5: Register tools in server.py and update TOOLS.md generation

Wire the new tools into the server startup and update the doc generator to handle
the new tag.

**Files:**
- Modify: `src/keboola_mcp_server/server.py`
- Modify: `src/keboola_mcp_server/generate_tool_docs.py`

**Step 1: Write a test verifying the tools are registered**

Create `tests/apps/test_registration.py`:

```python
import pytest

from keboola_mcp_server.config import Config, ServerRuntimeInfo
from keboola_mcp_server.server import create_server


def test_job_monitor_tools_registered():
    """Test that job_monitor and poll_job_monitor tools are registered on the server."""
    config = Config(storage_api_url='https://connection.keboola.com')
    runtime_info = ServerRuntimeInfo(
        app_version='test',
        server_version='test',
        mcp_library_version='test',
        fastmcp_library_version='test',
    )
    mcp = create_server(config, runtime_info=runtime_info, custom_routes_handling='add')

    tools = mcp.get_tools()
    assert 'job_monitor' in tools
    assert 'poll_job_monitor' in tools


def test_job_monitor_resource_registered():
    """Test that the ui://keboola/job-monitor resource is registered."""
    config = Config(storage_api_url='https://connection.keboola.com')
    runtime_info = ServerRuntimeInfo(
        app_version='test',
        server_version='test',
        mcp_library_version='test',
        fastmcp_library_version='test',
    )
    mcp = create_server(config, runtime_info=runtime_info, custom_routes_handling='add')

    resources = mcp.get_resources()
    # The resource key should be the URI
    resource_keys = list(resources.keys())
    assert any('job-monitor' in k for k in resource_keys), f'Expected job-monitor resource, got: {resource_keys}'


def test_job_monitor_tool_has_app_meta():
    """Test that job_monitor tool has _meta.ui with resourceUri."""
    config = Config(storage_api_url='https://connection.keboola.com')
    runtime_info = ServerRuntimeInfo(
        app_version='test',
        server_version='test',
        mcp_library_version='test',
        fastmcp_library_version='test',
    )
    mcp = create_server(config, runtime_info=runtime_info, custom_routes_handling='add')

    tool = mcp.get_tools()['job_monitor']
    meta = tool.get_meta()
    assert 'ui' in meta
    assert meta['ui']['resourceUri'] == 'ui://keboola/job-monitor'


def test_poll_job_monitor_is_app_only():
    """Test that poll_job_monitor is marked as app-only (visibility: ['app'])."""
    config = Config(storage_api_url='https://connection.keboola.com')
    runtime_info = ServerRuntimeInfo(
        app_version='test',
        server_version='test',
        mcp_library_version='test',
        fastmcp_library_version='test',
    )
    mcp = create_server(config, runtime_info=runtime_info, custom_routes_handling='add')

    tool = mcp.get_tools()['poll_job_monitor']
    meta = tool.get_meta()
    assert meta['ui']['visibility'] == ['app']
```

**Step 2: Run tests to verify they fail**

Run: `source 3.10.venv/bin/activate && pytest tests/apps/test_registration.py -v`
Expected: FAIL — `job_monitor` not in tools dict

**Step 3: Wire into server.py**

Add import to `src/keboola_mcp_server/server.py` (after the `add_job_tools` import):

```python
from keboola_mcp_server.tools.job_monitor import add_job_monitor_tools
```

Add call in `create_server()` (after `add_storage_tools(mcp)`, before `add_keboola_prompts(mcp)`):

```python
    add_job_monitor_tools(mcp)
```

**Step 4: Update generate_tool_docs.py**

Add import (after the other tag imports):

```python
from keboola_mcp_server.tools.job_monitor import JOB_MONITOR_TAG
```

Add to the categories list (find where categories are defined — look for `ToolCategory` instances):

```python
ToolCategory('Job Monitor App', JOB_MONITOR_TAG),
```

**Step 5: Run tests to verify they pass**

Run: `source 3.10.venv/bin/activate && pytest tests/apps/test_registration.py -v`
Expected: All 4 tests PASS

**Step 6: Run full tox suite**

Run: `source 3.10.venv/bin/activate && tox`
Expected: All environments pass (pytest, black, flake8, check-tools-docs).

If `check-tools-docs` fails because TOOLS.md is out of date, regenerate it:

```bash
source 3.10.venv/bin/activate && python -m keboola_mcp_server.generate_tool_docs
```

Then re-run tox.

**Step 7: Commit**

```bash
git add src/keboola_mcp_server/server.py src/keboola_mcp_server/generate_tool_docs.py tests/apps/test_registration.py TOOLS.md
git commit -m "AI-XXXX: register Job Monitor tools in server and update TOOLS.md"
```

---

### Task 6: Final integration test and cleanup

Run all tests, ensure linting passes, verify the app works end-to-end.

**Step 1: Run full tox suite**

Run: `source 3.10.venv/bin/activate && tox`
Expected: All 4 environments pass.

**Step 2: Fix any issues**

If black reformats files, re-commit. If flake8 complains, fix and re-commit.
If check-tools-docs fails, regenerate TOOLS.md and re-commit.

**Step 3: Final commit (if needed)**

```bash
git add -A
git commit -m "AI-XXXX: final cleanup for Job Monitor MCP App"
```
