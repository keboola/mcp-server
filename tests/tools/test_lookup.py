import json
import logging
import re
from collections.abc import Awaitable, Callable
from typing import Any

import httpx
import pytest
from fastmcp import Context
from pytest_mock import MockerFixture

from keboola_mcp_server.clients.client import ORCHESTRATOR_COMPONENT_ID, KeboolaClient
from keboola_mcp_server.errors import NotFoundError
from keboola_mcp_server.mcp import AggregateError, PlainFunctionTool, ToonCompactFunctionTool, _exclude_none_serializer
from keboola_mcp_server.tools.components.api_models import ComponentAPIResponse
from keboola_mcp_server.tools.components.model import FullConfigId
from keboola_mcp_server.tools.components.tools import add_component_tools, get_components, get_configs
from keboola_mcp_server.tools.flow.tools import add_flow_tools, get_flows
from keboola_mcp_server.tools.jobs import add_job_tools, get_jobs
from keboola_mcp_server.tools.lookup import LookupFailure, unwrap_lookup_results

COMPONENT_ID = 'keboola.ex-aws-s3'

Outcomes = dict[str, int | None]


def _http_error(status: int, api_error: str | None = None) -> httpx.HTTPStatusError:
    request = httpx.Request('GET', 'https://connection.test.keboola.com/v2/storage/x')
    message = f'Client error for url {request.url}' + (f'\nAPI error: {api_error}' if api_error else '')
    return httpx.HTTPStatusError(message, request=request, response=httpx.Response(status, request=request))


def _outcome(outcomes: Outcomes, item_id: str) -> None:
    if (status := outcomes[item_id]) is not None:
        raise _http_error(status)


def _component(component_id: str) -> dict[str, Any]:
    return {'id': component_id, 'name': f'Component {component_id}', 'type': 'extractor'}


async def _run_get_jobs(
    mocker: MockerFixture, ctx: Context, outcomes: Outcomes, **kwargs: Any
) -> tuple[Any, list[str]]:
    async def get_job_detail(job_id: str) -> dict[str, Any]:
        _outcome(outcomes, job_id)
        return {'id': job_id, 'status': 'success', 'component': COMPONENT_ID, 'url': f'https://x/{job_id}'}

    KeboolaClient.from_state(ctx.session.state).jobs_queue_client.get_job_detail = mocker.AsyncMock(
        side_effect=get_job_detail
    )
    result = await get_jobs(ctx=ctx, job_ids=list(outcomes), **kwargs)
    return result, [job.id for job in result.jobs]


async def _run_get_configs(mocker: MockerFixture, ctx: Context, outcomes: Outcomes) -> tuple[Any, list[str]]:
    async def configuration_detail(component_id: str, configuration_id: str) -> dict[str, Any]:
        _outcome(outcomes, configuration_id)
        return {'id': configuration_id, 'name': f'Config {configuration_id}', 'version': 1, 'configuration': {}}

    async def fetch_component(client: KeboolaClient, component_id: str) -> ComponentAPIResponse:
        return ComponentAPIResponse.model_validate(_component(component_id))

    KeboolaClient.from_state(ctx.session.state).storage_client.configuration_detail = mocker.AsyncMock(
        side_effect=configuration_detail
    )
    mocker.patch('keboola_mcp_server.tools.components.tools.fetch_component', side_effect=fetch_component)
    configs = [FullConfigId(component_id=COMPONENT_ID, configuration_id=config_id) for config_id in outcomes]
    result = await get_configs(ctx=ctx, configs=configs)
    return result, [config.configuration_root.configuration_id for config in result.configs]


async def _run_get_flows(mocker: MockerFixture, ctx: Context, outcomes: Outcomes) -> tuple[Any, list[str]]:
    async def configuration_detail(component_id: str, configuration_id: str) -> dict[str, Any]:
        _outcome(outcomes, configuration_id)
        return {
            'component_id': ORCHESTRATOR_COMPONENT_ID,
            'configuration_id': configuration_id,
            'name': f'Flow {configuration_id}',
            'version': 1,
            'isDisabled': False,
            'isDeleted': False,
            'configuration': {'phases': [], 'tasks': []},
            'created': '2025-01-15T10:30:00Z',
            'metadata': [],
        }

    client = KeboolaClient.from_state(ctx.session.state)
    client.storage_client.configuration_detail = mocker.AsyncMock(side_effect=configuration_detail)
    client.scheduler_client.list_schedules_by_config_id = mocker.AsyncMock(return_value=[])
    result = await get_flows(ctx=ctx, flow_ids=list(outcomes))
    return result, [flow.configuration_id for flow in result.flows]


async def _run_get_components(mocker: MockerFixture, ctx: Context, outcomes: Outcomes) -> tuple[Any, list[str]]:
    async def fetch_component(client: KeboolaClient, component_id: str) -> ComponentAPIResponse:
        _outcome(outcomes, component_id)
        return ComponentAPIResponse.model_validate(_component(component_id))

    mocker.patch('keboola_mcp_server.tools.components.tools.fetch_component', side_effect=fetch_component)
    result = await get_components(ctx=ctx, component_ids=list(outcomes))
    return result, [component.component_id for component in result.components]


ToolRunner = Callable[[MockerFixture, Context, Outcomes], Awaitable[tuple[Any, list[str]]]]


def _failure_id(tool: ToolRunner, item_id: str) -> str:
    return f'{COMPONENT_ID}/{item_id}' if tool is _run_get_configs else item_id


_REGISTRATION = {
    _run_get_jobs: (add_job_tools, 'get_jobs', ToonCompactFunctionTool),
    _run_get_configs: (add_component_tools, 'get_configs', ToonCompactFunctionTool),
    _run_get_flows: (add_flow_tools, 'get_flows', ToonCompactFunctionTool),
    _run_get_components: (add_component_tools, 'get_components', PlainFunctionTool),
}


def _serialize_as_registered(mocker: MockerFixture, tool: ToolRunner, result: Any) -> str:
    add_tools, name, expected_tool_class = _REGISTRATION[tool]
    mcp = mocker.Mock()
    add_tools(mcp)
    registered = next(call.args[0] for call in mcp.add_tool.call_args_list if call.args[0].name == name)
    assert type(registered) is expected_tool_class
    return registered.convert_result(result).content[0].text


def _top_level_keys(text: str) -> set[str]:
    try:
        return set(json.loads(text))
    except ValueError:
        return set(re.findall(r'^(\w+)[\[{:]', text, re.MULTILINE))


def _assert_failure_keys(text: str, key: str, expected_ids: list[str] | None) -> None:
    keys = _top_level_keys(text)
    if expected_ids is None:
        assert key not in keys
    else:
        assert key in keys
        assert all(item_id in text for item_id in expected_ids)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    'tool',
    [_run_get_jobs, _run_get_configs, _run_get_flows, _run_get_components],
    ids=['get_jobs', 'get_configs', 'get_flows', 'get_components'],
)
@pytest.mark.parametrize(
    ('outcomes', 'expected_found', 'expected_not_found', 'expected_errors'),
    [
        ({'a': None, 'b': None}, ['a', 'b'], None, None),
        ({'a': None, 'b': 404}, ['a'], ['b'], None),
        ({'a': None, 'b': 404, 'c': 503}, ['a'], ['b'], ['c']),
    ],
    ids=['all_found', 'mixed_found_404', 'mixed_found_404_5xx'],
)
async def test_batch_lookup_partial_results(
    tool: ToolRunner,
    outcomes: Outcomes,
    expected_found: list[str],
    expected_not_found: list[str] | None,
    expected_errors: list[str] | None,
    mocker: MockerFixture,
    mcp_context_client: Context,
):
    result, found = await tool(mocker, mcp_context_client, outcomes)

    assert found == expected_found
    assert [f.id for f in result.not_found or []] == [_failure_id(tool, i) for i in expected_not_found or []]
    assert [f.id for f in result.errors or []] == [_failure_id(tool, i) for i in expected_errors or []]
    assert result.not_found is None if expected_not_found is None else all(f.reason for f in result.not_found)
    assert result.errors is None if expected_errors is None else all(f.reason for f in result.errors)

    not_found_ids = None if expected_not_found is None else [_failure_id(tool, i) for i in expected_not_found]
    error_ids = None if expected_errors is None else [_failure_id(tool, i) for i in expected_errors]
    for text in (_exclude_none_serializer(result), _serialize_as_registered(mocker, tool, result)):
        _assert_failure_keys(text, 'not_found', not_found_ids)
        _assert_failure_keys(text, 'errors', error_ids)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    'tool',
    [_run_get_jobs, _run_get_configs, _run_get_flows, _run_get_components],
    ids=['get_jobs', 'get_configs', 'get_flows', 'get_components'],
)
@pytest.mark.parametrize(
    ('outcomes', 'expected_category', 'expected_level'),
    [
        ({'a': 404, 'b': 404}, 'client', logging.WARNING),
        ({'a': 500, 'b': 503}, 'internal', logging.ERROR),
        ({'a': 404, 'b': 500}, 'internal', logging.ERROR),
    ],
    ids=['all_404', 'all_5xx', 'mixed_404_5xx'],
)
async def test_batch_lookup_all_failed(
    tool: ToolRunner,
    outcomes: Outcomes,
    expected_category: str,
    expected_level: int,
    mocker: MockerFixture,
    mcp_context_client: Context,
    caplog: pytest.LogCaptureFixture,
):
    with pytest.raises(AggregateError) as exc_info:
        await tool(mocker, mcp_context_client, outcomes)

    assert len(exc_info.value.exceptions) == len(outcomes)
    records = [r for r in caplog.records if r.name == 'keboola_mcp_server.errors']
    assert len(records) == 1
    assert records[0].error_category == expected_category
    assert records[0].levelno == expected_level


@pytest.mark.parametrize(
    ('results', 'expected_not_found', 'expected_errors'),
    [
        (
            ['ok', _http_error(404, 'Configuration x not found')],
            [LookupFailure(id='2', reason='HTTP 404 Not Found: Configuration x not found')],
            [],
        ),
        (
            ['ok', _http_error(500), NotFoundError('Flow "3" not found\ndetails')],
            [LookupFailure(id='3', reason='Flow "3" not found')],
            [LookupFailure(id='2', reason='HTTP 500 Internal Server Error')],
        ),
        (['ok', _http_error(429)], [], [LookupFailure(id='2', reason='HTTP 429 Too Many Requests')]),
        (['ok', RuntimeError('x' * 300)], [], [LookupFailure(id='2', reason='x' * 197 + '...')]),
    ],
    ids=['404_with_api_error', 'mixed_5xx_not_found_error', '429_is_retryable', 'long_reason_truncated'],
)
def test_unwrap_lookup_results(
    results: list[Any], expected_not_found: list[LookupFailure], expected_errors: list[LookupFailure]
):
    lookup = unwrap_lookup_results([str(i) for i in range(1, len(results) + 1)], results)

    assert lookup.found == ['ok']
    assert lookup.not_found == expected_not_found
    assert lookup.errors == expected_errors


@pytest.mark.asyncio
async def test_get_jobs_include_logs_with_failed_id(mocker: MockerFixture, mcp_context_client: Context):
    """Logs are fetched only for the jobs that were found; the failed ID is reported in not_found."""
    client = KeboolaClient.from_state(mcp_context_client.session.state)
    client.storage_client.list_events = mocker.AsyncMock(
        return_value=[{'message': 'Started', 'type': 'info', 'created': '2024-01-01T00:00:01Z'}]
    )

    result, found = await _run_get_jobs(mocker, mcp_context_client, {'a': None, 'b': 404}, include_logs=True)

    assert found == ['a']
    assert [event.message for event in result.jobs[0].logs] == ['Started']
    assert result.not_found == [LookupFailure(id='b', reason='HTTP 404 Not Found')]
    assert result.errors is None
    client.storage_client.list_events.assert_awaited_once_with(job_id='a', limit=50)
