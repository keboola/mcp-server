from datetime import datetime
from typing import Any, Type, Union

import pytest
from httpx import HTTPError
from mcp.server.fastmcp import Context
from pytest_mock import MockerFixture

from keboola_mcp_server.clients.client import KeboolaClient
from keboola_mcp_server.links import Link
from keboola_mcp_server.tools.jobs import (
    JobDetail,
    JobListItem,
    ListJobsOutput,
    get_job,
    list_jobs,
    run_job,
)


@pytest.fixture
def mock_jobs() -> list[dict[str, Any]]:
    """list of mock jobs - simulating the api response."""
    return [
        {
            'id': '123',
            'status': 'success',
            'component': 'keboola.ex-aws-s3',
            'config': 'config-123',
            'isFinished': True,
            'createdTime': '2024-01-01T00:00:00Z',
            'startTime': '2024-01-01T00:00:01Z',
            'endTime': '2024-01-01T00:00:02Z',
            'not_a_desired_field': 'Should not be in the result',
        },
        {
            'id': '124',
            'status': 'processing',
            'component': 'keboola.ex-aws-s3',
            'config': 'config-124',
            'isFinished': False,
            'createdTime': '2024-01-01T00:00:00Z',
            'startTime': '2024-01-01T00:00:01Z',
            'endTime': '2024-01-01T00:00:02Z',
            'not_a_desired_field': 'Should not be in the result',
        },
    ]


@pytest.fixture
def mock_job() -> dict[str, Any]:
    """mock job - simulating the api response."""
    return {
        'id': '123',
        'status': 'success',
        'component': 'keboola.ex-aws-s3',
        'config': 'config-123',
        'isFinished': True,
        'createdTime': '2024-01-01T00:00:00Z',
        'startTime': '2024-01-01T00:00:01Z',
        'endTime': '2024-01-01T00:00:02Z',
        'url': 'https://connection.keboola.com/jobs/123',
        'configData': {'source': 'file.csv'},
        'configRow': '1',
        'runId': '456',
        'durationSeconds': 100,
        'result': {'import': 'successful'},
        'metrics': {'rows': 1000},
    }


@pytest.fixture
def iso_format() -> str:
    return '%Y-%m-%dT%H:%M:%SZ'


@pytest.mark.asyncio
async def test_list_jobs(
    mocker: MockerFixture,
    mcp_context_client: Context,
    mock_jobs: list[dict[str, Any]],
    iso_format: str,
):
    """Tests retrieve_jobs tool."""
    context = mcp_context_client
    keboola_client = KeboolaClient.from_state(context.session.state)
    keboola_client.jobs_queue_client.search_jobs_by = mocker.AsyncMock(return_value=mock_jobs)

    result = await list_jobs(context)

    assert isinstance(result, ListJobsOutput)
    assert len(result.jobs) == 2
    assert all(isinstance(job, JobListItem) for job in result.jobs)
    assert all(returned.id == expected['id'] for returned, expected in zip(result.jobs, mock_jobs))
    assert all(returned.status == expected['status'] for returned, expected in zip(result.jobs, mock_jobs))
    assert all(returned.component_id == expected['component'] for returned, expected in zip(result.jobs, mock_jobs))
    assert all(returned.config_id == expected['config'] for returned, expected in zip(result.jobs, mock_jobs))
    assert all(returned.is_finished == expected['isFinished'] for returned, expected in zip(result.jobs, mock_jobs))
    assert all(
        returned.created_time is not None
        and returned.created_time.replace(tzinfo=None) == datetime.strptime(expected['createdTime'], iso_format)
        for returned, expected in zip(result.jobs, mock_jobs)
    )
    assert all(
        returned.start_time is not None
        and returned.start_time.replace(tzinfo=None) == datetime.strptime(expected['startTime'], iso_format)
        for returned, expected in zip(result.jobs, mock_jobs)
    )
    assert all(
        returned.end_time is not None
        and returned.end_time.replace(tzinfo=None) == datetime.strptime(expected['endTime'], iso_format)
        for returned, expected in zip(result.jobs, mock_jobs)
    )
    assert all(hasattr(returned, 'not_a_desired_field') is False for returned in result.jobs)
    assert len(result.links) == 1

    keboola_client.jobs_queue_client.search_jobs_by.assert_called_once_with(
        status=None,
        component_id=None,
        config_id=None,
        limit=100,
        offset=0,
        sort_by='startTime',
        sort_order='desc',
    )


@pytest.mark.asyncio
async def test_get_job(mocker: MockerFixture, mcp_context_client: Context, mock_job: dict[str, Any], iso_format: str):
    """Tests get_job_detail tool."""
    context = mcp_context_client
    keboola_client = KeboolaClient.from_state(context.session.state)
    keboola_client.jobs_queue_client.get_job_detail = mocker.AsyncMock(return_value=mock_job)

    result = await get_job(ctx=context, job_id='123')

    assert isinstance(result, JobDetail)
    assert result.id == mock_job['id']
    assert result.status == mock_job['status']
    assert result.component_id == mock_job['component']
    assert result.config_id == mock_job['config']
    assert result.is_finished == mock_job['isFinished']
    assert result.created_time is not None
    assert result.created_time.replace(tzinfo=None) == datetime.strptime(mock_job['createdTime'], iso_format)
    assert result.start_time is not None
    assert result.start_time.replace(tzinfo=None) == datetime.strptime(mock_job['startTime'], iso_format)
    assert result.end_time is not None
    assert result.end_time.replace(tzinfo=None) == datetime.strptime(mock_job['endTime'], iso_format)
    assert result.url == mock_job['url']
    assert result.config_data == mock_job['configData']
    assert result.config_row == mock_job['configRow']
    assert result.run_id == mock_job['runId']
    assert result.duration_seconds == mock_job['durationSeconds']
    assert result.result == mock_job['result']

    keboola_client.jobs_queue_client.get_job_detail.assert_called_once_with('123')


@pytest.mark.asyncio
async def test_list_jobs_with_component_and_config_id(
    mocker: MockerFixture, mcp_context_client: Context, mock_jobs: list[dict[str, Any]]
):
    """
    Tests retrieve_jobs tool with config_id and component_id. With config_id, the tool will return
    only jobs for the given config_id and component_id.
    """
    context = mcp_context_client
    keboola_client = KeboolaClient.from_state(context.session.state)
    keboola_client.jobs_queue_client.search_jobs_by = mocker.AsyncMock(return_value=mock_jobs)

    result = await list_jobs(ctx=context, component_id='keboola.ex-aws-s3', config_id='config-123')

    assert len(result.jobs) == 2
    assert all(isinstance(job, JobListItem) for job in result.jobs)
    assert all(returned.id == expected['id'] for returned, expected in zip(result.jobs, mock_jobs))
    assert all(returned.status == expected['status'] for returned, expected in zip(result.jobs, mock_jobs))

    keboola_client.jobs_queue_client.search_jobs_by.assert_called_once_with(
        status=None,
        component_id='keboola.ex-aws-s3',
        config_id='config-123',
        sort_by='startTime',
        sort_order='desc',
        limit=100,
        offset=0,
    )


@pytest.mark.asyncio
async def test_list_jobs_with_component_id_without_config_id(
    mocker: MockerFixture, mcp_context_client: Context, mock_jobs: list[dict[str, Any]]
):
    """Tests retrieve_jobs tool with component_id and without config_id.
    It will return all jobs for the given component_id."""
    context = mcp_context_client
    keboola_client = KeboolaClient.from_state(context.session.state)
    keboola_client.jobs_queue_client.search_jobs_by = mocker.AsyncMock(return_value=mock_jobs)

    result = await list_jobs(ctx=context, component_id='keboola.ex-aws-s3')

    assert len(result.jobs) == 2
    assert all(isinstance(job, JobListItem) for job in result.jobs)
    assert all(returned.id == expected['id'] for returned, expected in zip(result.jobs, mock_jobs))
    assert all(returned.status == expected['status'] for returned, expected in zip(result.jobs, mock_jobs))

    keboola_client.jobs_queue_client.search_jobs_by.assert_called_once_with(
        status=None,
        component_id='keboola.ex-aws-s3',
        config_id=None,
        limit=100,
        offset=0,
        sort_by='startTime',
        sort_order='desc',
    )


@pytest.mark.asyncio
async def test_run_job(
    mocker: MockerFixture,
    mcp_context_client: Context,
    mock_job: dict[str, Any],
):
    """Tests run_job tool.
    :param mock_job: The newly created job details - expecting api response.
    :param mcp_context_client: The MCP context client.
    """
    context = mcp_context_client
    keboola_client = KeboolaClient.from_state(context.session.state)
    mock_job['result'] = []  # simulate empty list as returned by create job endpoint
    mock_job['status'] = 'created'  # simulate created status as returned by create job endpoint
    keboola_client.jobs_queue_client.create_job = mocker.AsyncMock(return_value=mock_job)

    component_id = mock_job['component']
    configuration_id = mock_job['config']
    job_detail = await run_job(ctx=context, component_id=component_id, configuration_id=configuration_id)

    assert isinstance(job_detail, JobDetail)
    assert job_detail.result == {}
    assert job_detail.id == mock_job['id']
    assert job_detail.status == mock_job['status']
    assert job_detail.component_id == component_id
    assert job_detail.config_id == configuration_id
    assert job_detail.result == {}
    assert set(job_detail.links) == {
        Link(
            type='ui-detail', title='Job: 123', url='https://connection.test.keboola.com/admin/projects/69420/queue/123'
        ),
        Link(
            type='ui-dashboard',
            title='Jobs in the project',
            url='https://connection.test.keboola.com/admin/projects/69420/queue',
        ),
    }

    keboola_client.jobs_queue_client.create_job.assert_called_once_with(
        component_id=component_id,
        configuration_id=configuration_id,
    )


@pytest.mark.asyncio
async def test_run_job_fail(mocker: MockerFixture, mcp_context_client: Context, mock_job: dict[str, Any]):
    """Tests run_job tool when job creation fails."""
    context = mcp_context_client
    keboola_client = KeboolaClient.from_state(context.session.state)
    keboola_client.jobs_queue_client.create_job = mocker.AsyncMock(side_effect=HTTPError('Job creation failed'))

    component_id = mock_job['component']
    configuration_id = mock_job['config']

    with pytest.raises(HTTPError):
        await run_job(ctx=context, component_id=component_id, configuration_id=configuration_id)

    keboola_client.jobs_queue_client.create_job.assert_called_once_with(
        component_id=component_id,
        configuration_id=configuration_id,
    )


@pytest.mark.parametrize(
    ('field_name', 'input_value', 'expected_result'),
    [
        ('result', [], {}),  # empty list is not a valid result type but we convert it to {}, no error
        ('result', {}, {}),  # expected empty dict, no error
        ('result', {'result': []}, {'result': []}),  # expected result type, no error
        ('result', None, {}),  # None is valid and converted to {}
        (
            'result',
            ['result1', 'result2'],
            ValueError,
        ),  # list is not a valid result type, we raise an error
        ('configData', [], {}),  # empty list is not a valid config_data type but we convert it to {}, no error
        ('configData', {}, {}),  # expected empty dict, no error
        ('configData', ['configData1', 'configData2'], ValueError),  # list is not a valid config_data type,
    ],
)
def test_job_detail_model_validate_dict_fields(
    field_name: str,
    input_value: Union[list, dict, None],
    expected_result: Union[dict, Type[Exception]],
    mock_job: dict[str, Any],
):
    """Tests JobDetail model validate for result field.
    :param input_value: The input value to validate - simulating the api response.
    :param expected_result: The expected result.
    :param mock_job: The mock job details - expecting api response.
    """
    mock_job[field_name] = input_value
    mock_job['links'] = []
    if isinstance(expected_result, type) and issubclass(expected_result, Exception):
        with pytest.raises(expected_result):
            JobDetail.model_validate(mock_job)
    else:
        job_detail = JobDetail.model_validate(mock_job)
        if field_name == 'result':
            assert job_detail.result == expected_result
        elif field_name == 'configData':
            assert job_detail.config_data == expected_result
