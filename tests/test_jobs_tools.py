import datetime
from typing import Any, get_args
from unittest.mock import MagicMock

import dateutil.parser
import pytest
from mcp.server.fastmcp import Context

from keboola_mcp_server.client import KeboolaClient
from keboola_mcp_server.jobs_tools import (
    JobDetail,
    JobListItem,
    get_job_details,
    retrieve_jobs_in_project,
)


@pytest.fixture
def mock_jobs() -> list[dict[str, Any]]:
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
async def test_retrieve_jobs_in_project(
    mcp_context_client: Context, mock_jobs: list[dict[str, Any]]
):
    """Test retrieve_jobs_in_project tool."""
    context = mcp_context_client
    keboola_client = KeboolaClient.from_state(context.session.state)
    keboola_client.jobs_queue.search_jobs_by = MagicMock(return_value=mock_jobs)

    result = await retrieve_jobs_in_project(context)

    assert len(result) == 2
    assert all(isinstance(job, JobListItem) for job in result)
    assert all(returned.id == expected["id"] for returned, expected in zip(result, mock_jobs))
    assert all(
        returned.status == expected["status"] for returned, expected in zip(result, mock_jobs)
    )
    assert all(
        returned.component_id == expected["component"]
        for returned, expected in zip(result, mock_jobs)
    )
    assert all(
        returned.config_id == expected["config"] for returned, expected in zip(result, mock_jobs)
    )
    assert all(
        returned.is_finished == expected["isFinished"]
        for returned, expected in zip(result, mock_jobs)
    )
    assert all(
        returned.created_time == dateutil.parser.isoparse(expected["createdTime"])
        for returned, expected in zip(result, mock_jobs)
    )
    assert all(
        returned.start_time == dateutil.parser.isoparse(expected["startTime"])
        for returned, expected in zip(result, mock_jobs)
    )
    assert all(
        returned.end_time == dateutil.parser.isoparse(expected["endTime"])
        for returned, expected in zip(result, mock_jobs)
    )
    assert all(hasattr(returned, "not_a_desired_field") is False for returned in result)

    keboola_client.jobs_queue.search_jobs_by.assert_called_once_with(
        status=None,
        component_id=None,
        config_id=None,
        limit=100,
        offset=0,
        sort_by="startTime",
        sort_order="desc",
    )


@pytest.mark.asyncio
async def test_get_job_details(mcp_context_client: Context, mock_job: dict[str, Any]):
    """Test get_job_details tool."""
    context = mcp_context_client
    keboola_client = KeboolaClient.from_state(context.session.state)
    keboola_client.jobs_queue.detail = MagicMock(return_value=mock_job)

    result = await get_job_details("123", context)

    assert isinstance(result, JobDetail)
    assert result.id == mock_job["id"]
    assert result.status == mock_job["status"]
    assert result.component_id == mock_job["component"]
    assert result.config_id == mock_job["config"]
    assert result.is_finished == mock_job["isFinished"]
    assert result.created_time == dateutil.parser.isoparse(mock_job["createdTime"])
    assert result.start_time == dateutil.parser.isoparse(mock_job["startTime"])
    assert result.end_time == dateutil.parser.isoparse(mock_job["endTime"])
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

    keboola_client.jobs_queue.detail.assert_called_once_with("123")


@pytest.mark.asyncio
async def retrieve_jobs_in_project_with_component_and_config_id(
    mcp_context_client: Context, mock_jobs: list[dict[str, Any]]
):
    """
    Test retrieve_jobs_in_project tool with config_id and component_id. With config_id, the tool will return
    only jobs for the given config_id and component_id.
    """
    context = mcp_context_client
    keboola_client = KeboolaClient.from_state(context.session.state)
    keboola_client.jobs_queue.search_jobs_by = MagicMock(return_value=mock_jobs)

    result = await retrieve_jobs_in_project(
        ctx=context, component_id="keboola.ex-aws-s3", config_id="config-123"
    )

    assert len(result) == 2
    assert all(isinstance(job, JobListItem) for job in result)
    assert all(returned.id == expected["id"] for returned, expected in zip(result, mock_jobs))
    assert all(
        returned.status == expected["status"] for returned, expected in zip(result, mock_jobs)
    )

    keboola_client.jobs_queue.search_jobs_by.assert_called_once_with(
        status=None,
        component_id="keboola.ex-aws-s3",
        config_id="config-123",
        sort_by="startTime",
        sort_order="desc",
        limit=100,
        offset=0,
    )


@pytest.mark.asyncio
async def retrieve_jobs_in_project_with_component_id_without_config_id(
    mcp_context_client: Context, mock_jobs: list[dict[str, Any]]
):
    """Test retrieve_jobs_in_project tool with component_id and without config_id.
    It will return all jobs for the given component_id."""
    context = mcp_context_client
    keboola_client = KeboolaClient.from_state(context.session.state)
    keboola_client.jobs_queue.search_jobs_by = MagicMock(return_value=mock_jobs)

    result = await retrieve_jobs_in_project(ctx=context, component_id="keboola.ex-aws-s3")

    assert len(result) == 2
    assert all(isinstance(job, JobListItem) for job in result)
    assert all(returned.id == expected["id"] for returned, expected in zip(result, mock_jobs))
    assert all(
        returned.status == expected["status"] for returned, expected in zip(result, mock_jobs)
    )

    keboola_client.jobs_queue.search_jobs_by.assert_called_once_with(
        status=None,
        component_id="keboola.ex-aws-s3",
        config_id=None,
        limit=100,
        offset=0,
        sort_by="startTime",
        sort_order="desc",
    )
