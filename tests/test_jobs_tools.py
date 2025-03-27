from typing import Any, Dict
from unittest.mock import AsyncMock, MagicMock

import pytest
from mcp.server.fastmcp import Context

from keboola_mcp_server.client import KeboolaClient
from keboola_mcp_server.jobs_tools import (
    JobDetail,
    JobListItem,
    get_job_details,
    list_jobs,
)


@pytest.mark.asyncio
async def test_list_jobs(mcp_context_client):
    """Test list_jobs tool."""
    context = mcp_context_client
    mock_client = context.session.state["sapi_client"]
    mock_client.storage_client.jobs = MagicMock()

    # Mock data
    mock_jobs = [
        {
            "id": "123",
            "status": "success",
            "createdTime": "2024-01-01T00:00:00Z",
            "startTime": "2024-01-01T00:00:01Z",
            "endTime": "2024-01-01T00:00:02Z",
        },
        {
            "id": "124",
            "status": "processing",
            "createdTime": "2024-01-01T00:00:00Z",
            "startTime": "2024-01-01T00:00:01Z",
            "endTime": "2024-01-01T00:00:02Z",
        },
    ]
    mock_client.storage_client.jobs.list = MagicMock(return_value=mock_jobs)

    result = await list_jobs(context)

    assert len(result) == 2
    assert all(isinstance(job, JobListItem) for job in result)
    assert all(j.id == item["id"] for j, item in zip(result, mock_jobs))
    assert all(j.status == item["status"] for j, item in zip(result, mock_jobs))
    assert all(j.created_time == item["createdTime"] for j, item in zip(result, mock_jobs))
    assert all(j.start_time == item["startTime"] for j, item in zip(result, mock_jobs))
    assert all(j.end_time == item["endTime"] for j, item in zip(result, mock_jobs))

    mock_client.storage_client.jobs.list.assert_called_once()


@pytest.mark.asyncio
async def test_get_job_details(mcp_context_client):
    """Test get_job_details tool."""
    context = mcp_context_client
    mock_client = context.session.state["sapi_client"]
    mock_client.storage_client.jobs = MagicMock()

    # Mock data
    mock_job = {
        "id": "123",
        "status": "success",
        "createdTime": "2024-01-01T00:00:00Z",
        "startTime": "2024-01-01T00:00:01Z",
        "endTime": "2024-01-01T00:00:02Z",
        "url": "https://connection.keboola.com/jobs/123",
        "tableId": "in.c-main.table",
        "operationName": "tableImport",
        "operationParams": {"source": "file.csv"},
        "runId": "456",
        "results": {"import": "successful"},
        "metrics": {"rows": 1000},
        "additional_field": "some value",  # this will be removed
    }

    # Setup mock to return test data
    mock_client.storage_client.jobs.detail = MagicMock(return_value=mock_job)

    result = await get_job_details("123", context)

    assert isinstance(result, JobDetail)
    assert result.id == "123"
    assert result.status == "success"
    assert result.url == "https://connection.keboola.com/jobs/123"
    assert result.table_id == "in.c-main.table"
    assert result.operation_name == "tableImport"
    assert result.operation_params == {"source": "file.csv"}
    assert result.run_id == "456"
    assert result.results == {"import": "successful"}
    assert result.metrics == {"rows": 1000}

    mock_client.storage_client.jobs.detail.assert_called_once_with("123")
