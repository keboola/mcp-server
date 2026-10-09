from typing import Any

import pytest
from pytest_mock import MockerFixture

from keboola_mcp_server.clients.stream import ColumnTemplate, StreamClient, StreamTaskError, TableColumn


@pytest.fixture
def stream_client(mocker: MockerFixture) -> StreamClient:
    client = StreamClient.create(root_url='https://stream.test.keboola.com', token='test-token')
    mocker.patch.object(StreamClient, 'TASK_POLL_INTERVAL_SECONDS', 0)
    return client


def _task(is_finished: bool, **extra: Any) -> dict[str, Any]:
    return {'taskId': 'task-1', 'isFinished': is_finished, 'status': 'success' if is_finished else 'processing'} | extra


@pytest.mark.asyncio
async def test_list_sources_paginates(stream_client: StreamClient, mocker: MockerFixture) -> None:
    get = mocker.patch.object(
        stream_client,
        'get',
        side_effect=[
            {'sources': [{'sourceId': 'a'}], 'page': {'totalCount': 2, 'lastId': 'a'}},
            {'sources': [{'sourceId': 'b'}], 'page': {'totalCount': 2, 'lastId': 'b'}},
        ],
    )

    assert await stream_client.list_sources() == [{'sourceId': 'a'}, {'sourceId': 'b'}]
    assert [c.kwargs['params']['afterId'] for c in get.call_args_list] == ['', 'a']
    assert get.call_args.kwargs['endpoint'] == 'v1/branches/default/aggregation/sources'


@pytest.mark.asyncio
async def test_create_table_sink_payload(stream_client: StreamClient, mocker: MockerFixture) -> None:
    post = mocker.patch.object(stream_client, 'post', return_value=_task(True, outputs={'sinkId': 'logs'}))
    columns = [
        TableColumn(name='user', type='path', path='user.id', default_value='', raw_string=True),
        TableColumn(name='msg', type='template', template=ColumnTemplate(content="Body('body', null)")),
    ]

    outputs = await stream_client.create_table_sink(
        'otel', name='Logs', table_id='in.c-otlp-otel.logs', columns=columns, allowed_signals=['logs']
    )

    assert outputs == {'sinkId': 'logs'}
    post.assert_called_once_with(
        endpoint='v1/branches/default/sources/otel/sinks',
        data={
            'name': 'Logs',
            'type': 'table',
            'table': {
                'type': 'keboola',
                'tableId': 'in.c-otlp-otel.logs',
                'mapping': {
                    'columns': [
                        {'name': 'user', 'type': 'path', 'path': 'user.id', 'defaultValue': '', 'rawString': True},
                        {
                            'name': 'msg',
                            'type': 'template',
                            'template': {'language': 'jsonnet', 'content': "Body('body', null)"},
                        },
                    ]
                },
            },
            'allowedSignals': ['logs'],
        },
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ('polled', 'expected_error'),
    [
        ([_task(False), _task(True, outputs={'sourceId': 'github'})], None),
        ([_task(True, status='error', error='Source already exists.')], 'Source already exists.'),
    ],
)
async def test_wait_for_task(
    polled: list[dict[str, Any]], expected_error: str | None, stream_client: StreamClient, mocker: MockerFixture
) -> None:
    get = mocker.patch.object(stream_client, 'get', side_effect=polled)

    if expected_error:
        with pytest.raises(StreamTaskError, match=expected_error):
            await stream_client.wait_for_task(_task(False))
    else:
        assert await stream_client.wait_for_task(_task(False)) == {'sourceId': 'github'}
    assert get.call_args.kwargs['endpoint'] == 'v1/tasks/task-1'


@pytest.mark.asyncio
async def test_wait_for_task_times_out(stream_client: StreamClient, mocker: MockerFixture) -> None:
    mocker.patch.object(StreamClient, 'TASK_TIMEOUT_SECONDS', 0)
    mocker.patch.object(stream_client, 'get', return_value=_task(False))

    with pytest.raises(StreamTaskError, match='did not finish in time'):
        await stream_client.wait_for_task(_task(False))
