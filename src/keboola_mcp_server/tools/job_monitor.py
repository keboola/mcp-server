"""
MCP App tools for the Job Monitor dashboard.

The ``job_monitor`` tool is model-visible (the LLM invokes it to open the dashboard).
The ``poll_job_monitor`` tool is app-only (called by the iframe for live updates).
"""

import importlib.resources
import logging
from typing import Annotated, Any, Optional, Sequence

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
    # Register the HTML resource
    html_content = importlib.resources.read_text('keboola_mcp_server.apps', 'job_monitor.html')

    @mcp.resource(
        JOB_MONITOR_RESOURCE_URI,
        name='Job Monitor',
        description=('Interactive job monitoring dashboard with live updates ' 'and log viewer.'),
        mime_type=APP_RESOURCE_MIME_TYPE,
    )
    def job_monitor_resource() -> str:
        return html_content

    # Build MCP App metadata
    app_meta = build_app_tool_meta(
        resource_uri=JOB_MONITOR_RESOURCE_URI,
        csp_resource_domains=['https://unpkg.com'],
    )
    app_only_meta = build_app_tool_meta(
        resource_uri=JOB_MONITOR_RESOURCE_URI,
        visibility=['app'],
        csp_resource_domains=['https://unpkg.com'],
    )

    # Model-visible tool
    mcp.add_tool(
        FunctionTool.from_function(
            job_monitor,
            annotations=ToolAnnotations(readOnlyHint=True),
            tags={JOB_MONITOR_TAG},
            meta=app_meta,
        )
    )

    # App-only tool (hidden from LLM)
    mcp.add_tool(
        FunctionTool.from_function(
            poll_job_monitor,
            annotations=ToolAnnotations(readOnlyHint=True),
            tags={JOB_MONITOR_TAG},
            meta=app_only_meta,
        )
    )

    LOG.info('Job Monitor MCP App tools registered.')


def _build_result(
    jobs: list[Any],
    filter_params: dict[str, Any],
) -> dict[str, Any]:
    """Build the structured result dict returned by both tools."""
    serialized_jobs = []
    for job in jobs:
        if isinstance(job, (JobDetail, JobListItem)):
            job_dict = job.model_dump(by_alias=True, exclude_none=True)
        else:
            job_dict = job
        if isinstance(job, JobListItem) and not isinstance(job, JobDetail):
            job_dict['logs'] = None
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
                'IDs of specific jobs to monitor. When provided, shows '
                'full details with logs. When empty, lists recent jobs '
                'with optional filtering.'
            )
        ),
    ] = tuple(),
    component_id: Annotated[
        Optional[str],
        Field(
            description=('Filter by component ID (only used when job_ids is empty).'),
        ),
    ] = None,
    status: Annotated[
        Optional[JOB_STATUS],
        Field(
            description=('Filter by job status (only used when job_ids is empty).'),
        ),
    ] = None,
    limit: Annotated[
        int,
        Field(
            description='Max jobs to show (default 20, max 100).',
            ge=1,
            le=100,
        ),
    ] = 20,
    include_logs: Annotated[
        bool,
        Field(
            description=('Include execution logs for each job. Default True.'),
        ),
    ] = True,
    log_tail_lines: Annotated[
        int,
        Field(
            description='Max log events per job (default 50, max 500).',
            ge=1,
            le=500,
        ),
    ] = 50,
) -> dict[str, Any]:
    """
    Opens an interactive Job Monitor dashboard.

    Displays a live-updating table of jobs with status badges, timing info,
    and expandable log viewers. The dashboard auto-refreshes every 5 seconds
    while jobs are still running.

    Use this tool when you want to visually monitor job execution, debug
    failed jobs, or show the user an overview of recent activity.

    EXAMPLES:
    - job_ids=["12345"] -> monitor a specific job with live log updates
    - job_ids=["12345", "67890"] -> monitor multiple jobs side by side
    - status="processing" -> show all currently running jobs
    - component_id="keboola.snowflake-transformation" -> show recent
      transformation jobs
    """
    client = KeboolaClient.from_state(ctx.session.state)
    links_manager = await ProjectLinksManager.from_client(client)

    filter_params: dict[str, Any] = {
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
        Optional[str],
        Field(description='Filter by component ID.'),
    ] = None,
    status: Annotated[
        Optional[JOB_STATUS],
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
    This tool is app-only -- it is not visible to the LLM.
    """
    return await job_monitor(
        ctx=ctx,
        job_ids=job_ids,
        component_id=component_id,
        status=status,
        limit=limit,
        include_logs=include_logs,
        log_tail_lines=log_tail_lines,
    )
