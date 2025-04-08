from typing import Any, get_args
from unittest.mock import MagicMock

import pytest

from keboola_mcp_server.jobs_tools import (
    JobDetail,
    JobListItem,
    get_job_details,
    retrieve_component_config_jobs,
    retrieve_jobs_in_project,
)


@pytest.fixture
def mock_jobs() -> list[dict[str, Any]]:
    """Mock mock_job tool."""
    return [
        {
            "id": "123",
            "status": "success",
            "component": "keboola.ex-aws-s3",
            "config": "config-123",
            "isFinished": True,
            "createdTime": "2024-01-01T00:00:00Z",
            "startTime": "2024-01-01T00:00:01Z",
            "endTime": "2024-01-01T00:00:02Z",
            "not_a_desired_field": "Should not be in the result",
        },
        {
            "id": "124",
            "status": "processing",
            "component": "keboola.ex-aws-s3",
            "config": "config-124",
            "isFinished": False,
            "createdTime": "2024-01-01T00:00:00Z",
            "startTime": "2024-01-01T00:00:01Z",
            "endTime": "2024-01-01T00:00:02Z",
            "not_a_desired_field": "Should not be in the result",
        },
    ]


@pytest.fixture
def mock_job() -> dict[str, Any]:
    """Mock mock_job tool."""
    return {
        "id": "123",
        "status": "success",
        "component": "keboola.ex-aws-s3",
        "config": "config-123",
        "isFinished": True,
        "createdTime": "2024-01-01T00:00:00Z",
        "startTime": "2024-01-01T00:00:01Z",
        "endTime": "2024-01-01T00:00:02Z",
        "url": "https://connection.keboola.com/jobs/123",
        "configData": [{"source": "file.csv"}],
        "configRowIds": ["1", "2", "3"],
        "runId": "456",
        "parentRunId": "789",
        "durationSeconds": 100,
        "result": {"import": "successful"},
        "metrics": {"rows": 1000},
    }


@pytest.mark.asyncio
async def test_retrieve_jobs_in_project(mcp_context_client, mock_jobs):
    """Test retrieve_jobs_in_project tool."""
    context = mcp_context_client
    mock_client = context.session.state["sapi_client"]

    mock_client.jobs_queue.list = MagicMock(return_value=mock_jobs)
    result = await retrieve_jobs_in_project(context)

    assert len(result) == 2
    assert all(isinstance(job, JobListItem) for job in result)
    assert all(j.id == item["id"] for j, item in zip(result, mock_jobs))
    assert all(j.status == item["status"] for j, item in zip(result, mock_jobs))
    assert all(j.component_id == item["component"] for j, item in zip(result, mock_jobs))
    assert all(j.config_id == item["config"] for j, item in zip(result, mock_jobs))
    assert all(j.is_finished == item["isFinished"] for j, item in zip(result, mock_jobs))
    assert all(j.created_time == item["createdTime"] for j, item in zip(result, mock_jobs))
    assert all(j.start_time == item["startTime"] for j, item in zip(result, mock_jobs))
    assert all(j.end_time == item["endTime"] for j, item in zip(result, mock_jobs))
    assert all(hasattr(j, "not_a_desired_field") is False for j in result)

    mock_client.jobs_queue.list.assert_called_once_with(
        limit=100,
        offset=0,
        status=None,
        sort_by="startTime",
        sort_order="desc",
    )


@pytest.mark.asyncio
async def test_get_job_details(mcp_context_client, mock_job):
    """Test get_job_details tool."""
    context = mcp_context_client
    mock_client = context.session.state["sapi_client"]

    # Setup mock to return test data
    mock_client.jobs_queue.detail = MagicMock(return_value=mock_job)

    result = await get_job_details("123", context)

    assert isinstance(result, JobDetail)
    assert result.id == mock_job["id"]
    assert result.status == mock_job["status"]
    assert result.component_id == mock_job["component"]
    assert result.config_id == mock_job["config"]
    assert result.is_finished == mock_job["isFinished"]
    assert result.created_time == mock_job["createdTime"]
    assert result.start_time == mock_job["startTime"]
    assert result.end_time == mock_job["endTime"]
    assert result.url == mock_job["url"]
    assert result.config_data == mock_job["configData"]
    assert result.config_row_ids == mock_job["configRowIds"]
    assert result.run_id == mock_job["runId"]
    assert result.parent_run_id == mock_job["parentRunId"]
    assert result.duration_seconds == mock_job["durationSeconds"]
    assert result.result == mock_job["result"]
    assert result.metrics == mock_job["metrics"]
    # table_id is not present in the mock_job, should be None
    assert result.table_id == None

    mock_client.jobs_queue.detail.assert_called_once_with("123")


@pytest.mark.asyncio
async def test_retrieve_component_config_jobs_with_config_id(mcp_context_client, mock_jobs):
    """
    Test retrieve_component_config_jobs tool with config_id and component_id. With config_id, the tool will return
    only jobs for the given config_id and component_id.
    """
    context = mcp_context_client
    mock_client = context.session.state["sapi_client"]

    mock_client.jobs_queue.search = MagicMock(return_value=mock_jobs)

    result = await retrieve_component_config_jobs(
        ctx=context, component_id="keboola.ex-aws-s3", config_id="config-123"
    )

    assert len(result) == 2
    assert all(isinstance(job, JobListItem) for job in result)
    assert all(j.id == item["id"] for j, item in zip(result, mock_jobs))
    assert all(j.status == item["status"] for j, item in zip(result, mock_jobs))

    mock_client.jobs_queue.search.assert_called_once_with(
        {
            "componentId": "keboola.ex-aws-s3",
            "configId": "config-123",
            "status": None,
            "sortBy": "startTime",
            "sortOrder": "desc",
            "limit": 100,
            "offset": 0,
        }
    )


@pytest.mark.asyncio
async def test_retrieve_component_config_jobs_without_config_id(mcp_context_client, mock_jobs):
    """Test retrieve_component_config_jobs tool without config_id. It will return all jobs for the given component_id."""
    context = mcp_context_client
    mock_client = context.session.state["sapi_client"]

    mock_client.jobs_queue.search = MagicMock(return_value=mock_jobs)

    result = await retrieve_component_config_jobs(ctx=context, component_id="keboola.ex-aws-s3")

    assert len(result) == 2
    assert all(isinstance(job, JobListItem) for job in result)
    assert all(j.id == item["id"] for j, item in zip(result, mock_jobs))
    assert all(j.status == item["status"] for j, item in zip(result, mock_jobs))

    mock_client.jobs_queue.search.assert_called_once_with(
        {
            "componentId": "keboola.ex-aws-s3",
            "configId": None,
            "status": None,
            "limit": 100,
            "offset": 0,
            "sortBy": "startTime",
            "sortOrder": "desc",
        }
    )
