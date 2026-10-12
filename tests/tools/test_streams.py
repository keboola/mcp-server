from typing import Any

import pytest
from fastmcp import Context
from fastmcp.exceptions import ToolError
from pytest_mock import MockerFixture

from keboola_mcp_server.clients.client import KeboolaClient
from keboola_mcp_server.clients.stream import StreamTaskError, TableColumn
from keboola_mcp_server.tools.streams import (
    DATA_STREAMS_FEATURE,
    HTTP_DEFAULT_COLUMNS,
    OTLP_RAW_COLUMN,
    OTLP_SIGNAL_COLUMNS,
    PROTECTED_DEFAULT_BRANCH_FEATURE,
    GetStreamsOutput,
    StreamSource,
    create_stream,
    get_streams,
)

STREAMS_PAGE_URL = 'https://connection.test.keboola.com/admin/projects/69420/storage/data-streams'

HTTP_SOURCE: dict[str, Any] = {
    'sourceId': 'github',
    'name': 'GitHub',
    'description': 'GitHub webhooks',
    'type': 'http',
    'http': {'url': 'https://stream-in.test.keboola.com/69420/github/SECRET'},
    'sinks': [
        {
            'sinkId': 'github',
            'name': 'GitHub',
            'type': 'table',
            'table': {
                'type': 'keboola',
                'tableId': 'in.c-data-stream-github.events',
                'mapping': {'columns': [{'type': 'path', 'name': 'action', 'path': 'action', 'rawString': True}]},
            },
        }
    ],
}

OTLP_SOURCE: dict[str, Any] = {
    'sourceId': 'otel',
    'name': 'OTel',
    'type': 'otlp',
    'otlp': {
        'url': 'https://stream-in.test.keboola.com/otlp/69420/otel/SECRET',
        'baseUrl': 'https://stream-in.test.keboola.com/otlp/69420/otel',
        'secret': 'SECRET',
    },
    'disabled': {'at': '2026-10-08T10:00:00Z', 'by': {'type': 'user'}, 'reason': 'paused'},
    'sinks': [
        {
            'sinkId': 'logs',
            'name': 'Logs',
            'type': 'table',
            'allowedSignals': ['logs'],
            'table': {'type': 'keboola', 'tableId': 'in.c-otlp-otel.logs', 'mapping': {'columns': []}},
        }
    ],
}


@pytest.fixture
def streams_client(mcp_context_client: Context, mocker: MockerFixture) -> KeboolaClient:
    client = KeboolaClient.from_state(mcp_context_client.session.state)
    client.has_feature = mocker.AsyncMock(side_effect=lambda feature: feature == DATA_STREAMS_FEATURE)
    client.stream_client.list_sources.return_value = [HTTP_SOURCE, OTLP_SOURCE]
    client.stream_client.create_source.return_value = {'sourceId': 'new-stream'}
    client.readonly = False
    client.storage_client.verify_token.return_value = {'admin': {'role': 'admin'}}
    return client


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ('branch_id', 'features', 'expected_error'),
    [
        (None, set(), 'Data Streams are not enabled in this project'),
        (None, {DATA_STREAMS_FEATURE, PROTECTED_DEFAULT_BRANCH_FEATURE}, 'protected default branch'),
        ('1234', {DATA_STREAMS_FEATURE}, 'only in the main production branch'),
    ],
)
@pytest.mark.parametrize('call_tool', [get_streams, create_stream])
async def test_tools_require_data_streams(
    call_tool: Any,
    branch_id: str | None,
    features: set[str],
    expected_error: str,
    streams_client: KeboolaClient,
    mcp_context_client: Context,
) -> None:
    streams_client.branch_id = branch_id
    streams_client.has_feature.side_effect = lambda feature: feature in features
    kwargs = {'name': 'New stream'} if call_tool is create_stream else {}

    with pytest.raises(ToolError, match=expected_error) as exc_info:
        await call_tool(ctx=mcp_context_client, **kwargs)

    if not features:
        assert STREAMS_PAGE_URL in str(exc_info.value)
        assert 'Unlock Data Streams' in str(exc_info.value)
    streams_client.stream_client.list_sources.assert_not_called()
    streams_client.stream_client.create_source.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ('source_ids', 'expected_ids'),
    [
        ((), ['github', 'otel']),
        (('otel',), ['otel']),
    ],
)
@pytest.mark.parametrize(
    ('readonly', 'token_role', 'expect_secret'),
    [
        (False, 'admin', True),
        (True, 'admin', False),
        (False, 'readOnly', False),
    ],
)
async def test_get_streams(
    source_ids: tuple[str, ...],
    expected_ids: list[str],
    readonly: bool,
    token_role: str,
    expect_secret: bool,
    streams_client: KeboolaClient,
    mcp_context_client: Context,
) -> None:
    streams_client.readonly = readonly
    streams_client.storage_client.verify_token.return_value = {'admin': {'role': token_role}}

    result = await get_streams(ctx=mcp_context_client, source_ids=source_ids)

    assert isinstance(result, GetStreamsOutput)
    assert [s.source_id for s in result.streams] == expected_ids
    assert result.links[0].url == STREAMS_PAGE_URL
    streams = {s.source_id: s for s in result.streams}
    if http := streams.get('github'):
        assert http.endpoint_url == (HTTP_SOURCE['http']['url'] if expect_secret else None)
        assert http.secret_redacted is not expect_secret
        assert http.otlp_base_url is None
        assert http.disabled is False
        assert http.sinks[0].table_id == 'in.c-data-stream-github.events'
        assert http.sinks[0].columns == [TableColumn(name='action', type='path', path='action', raw_string=True)]
        assert http.links[0].url == f'{STREAMS_PAGE_URL}/github'
    otlp = streams['otel']
    assert otlp.endpoint_url == (OTLP_SOURCE['otlp']['url'] if expect_secret else None)
    assert otlp.otlp_base_url == OTLP_SOURCE['otlp']['baseUrl']
    assert otlp.otlp_secret == ('SECRET' if expect_secret else None)
    assert otlp.secret_redacted is not expect_secret
    assert otlp.disabled is True
    assert otlp.sinks[0].allowed_signals == ['logs']


@pytest.mark.asyncio
async def test_get_streams_missing_id(streams_client: KeboolaClient, mcp_context_client: Context) -> None:
    with pytest.raises(ToolError, match='Data Streams not found: nope.'):
        await get_streams(ctx=mcp_context_client, source_ids=['github', 'nope'])


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ('table_id', 'columns', 'expected_table_id', 'expected_columns'),
    [
        (None, None, 'in.c-data-stream-new-stream.events', HTTP_DEFAULT_COLUMNS),
        (
            'in.c-github.events',
            [TableColumn(name='action', type='path', path='action')],
            'in.c-github.events',
            [TableColumn(name='action', type='path', path='action')],
        ),
    ],
)
async def test_create_http_stream(
    table_id: str | None,
    columns: list[TableColumn] | None,
    expected_table_id: str,
    expected_columns: list[TableColumn],
    streams_client: KeboolaClient,
    mcp_context_client: Context,
) -> None:
    streams_client.stream_client.list_sources.return_value = [HTTP_SOURCE | {'sourceId': 'new-stream'}]

    result = await create_stream(
        ctx=mcp_context_client, name='New stream', description='desc', table_id=table_id, columns=columns
    )

    assert isinstance(result, StreamSource)
    assert result.source_id == 'new-stream'
    assert result.endpoint_url == HTTP_SOURCE['http']['url']
    streams_client.stream_client.create_source.assert_called_once_with(
        name='New stream', source_type='http', description='desc'
    )
    streams_client.stream_client.create_table_sink.assert_called_once_with(
        'new-stream', name='New stream', table_id=expected_table_id, columns=expected_columns
    )


@pytest.mark.asyncio
@pytest.mark.parametrize('include_raw', [False, True])
async def test_create_otlp_stream(
    include_raw: bool, streams_client: KeboolaClient, mcp_context_client: Context
) -> None:
    streams_client.stream_client.list_sources.return_value = [OTLP_SOURCE | {'sourceId': 'new-stream'}]

    result = await create_stream(
        ctx=mcp_context_client, name='New stream', source_type='otlp', include_raw_otlp_record=include_raw
    )

    assert result.otlp_secret == 'SECRET'
    calls = streams_client.stream_client.create_table_sink.call_args_list
    assert [c.kwargs['table_id'] for c in calls] == [
        'in.c-otlp-new-stream.logs',
        'in.c-otlp-new-stream.metrics',
        'in.c-otlp-new-stream.traces',
    ]
    assert [c.kwargs['allowed_signals'] for c in calls] == [['logs'], ['metrics'], ['traces']]
    for call, (signal, columns) in zip(calls, OTLP_SIGNAL_COLUMNS.items()):
        assert call.kwargs['name'] == signal.capitalize()
        assert call.kwargs['columns'] == ([*columns, OTLP_RAW_COLUMN] if include_raw else columns)


@pytest.mark.asyncio
@pytest.mark.parametrize('kwargs', [{'table_id': 'in.c-x.y'}, {'columns': [TableColumn(name='body', type='body')]}])
async def test_create_otlp_stream_rejects_http_options(
    kwargs: dict[str, Any], streams_client: KeboolaClient, mcp_context_client: Context
) -> None:
    with pytest.raises(ToolError, match='apply only to HTTP streams'):
        await create_stream(ctx=mcp_context_client, name='New stream', source_type='otlp', **kwargs)

    streams_client.stream_client.create_source.assert_not_called()


@pytest.mark.asyncio
async def test_create_stream_deletes_source_when_sink_fails(
    streams_client: KeboolaClient, mcp_context_client: Context
) -> None:
    streams_client.stream_client.create_table_sink.side_effect = StreamTaskError('Table name is invalid')

    with pytest.raises(StreamTaskError, match='Table name is invalid'):
        await create_stream(ctx=mcp_context_client, name='New stream', table_id='bad')

    streams_client.stream_client.delete_source.assert_called_once_with('new-stream')
