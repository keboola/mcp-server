import logging
from typing import Annotated, Any, Dict, List, Literal, Optional, Union, cast, get_args

from mcp.server.fastmcp import Context, FastMCP
from pydantic import AliasChoices, BaseModel, Field, field_validator

from keboola_mcp_server.client import KeboolaClient

logger = logging.getLogger(__name__)

######################################## Job Base Models ########################################

JOB_STATUS = Literal[
    "waiting",
    "processing",
    "success",
    "error",
]


class JobListItem(BaseModel):
    """Represents a summary of a job with minimal information, used in lists where detailed job data is not required."""

    id: str = Field(description="The ID of the job.")
    status: JOB_STATUS = Field(description="The status of the job.")
    component_id: Optional[str] = Field(
        description="The ID of the component that the job is running on.",
        validation_alias=AliasChoices("component", "componentId", "component_id", "component-id"),
        serialization_alias="component",
        default=None,
    )
    config_id: Optional[str] = Field(
        description="The ID of the component configuration that the job is running on.",
        validation_alias=AliasChoices("config", "configId", "config_id", "config-id"),
        serialization_alias="config",
        default=None,
    )
    is_finished: bool = Field(
        description="Whether the job is finished.",
        validation_alias=AliasChoices("isFinished", "is_finished", "is-finished"),
        serialization_alias="isFinished",
        default=False,
    )
    created_time: Optional[str] = Field(
        description="The creation time of the job.",
        validation_alias=AliasChoices("createdTime", "created_time", "created-time"),
        serialization_alias="createdTime",
        default=None,
    )
    start_time: Optional[str] = Field(
        description="The start time of the job.",
        validation_alias=AliasChoices("startTime", "start_time", "start-time"),
        serialization_alias="startTime",
        default=None,
    )
    end_time: Optional[str] = Field(
        description="The end time of the job.",
        validation_alias=AliasChoices("endTime", "end_time", "end-time"),
        serialization_alias="endTime",
        default=None,
    )


class JobDetail(JobListItem):
    """Represents a detailed job with all available information."""

    url: str = Field(description="The URL of the job.")
    table_id: Optional[str] = Field(
        description="The ID of the table that the job is running on.",
        validation_alias=AliasChoices("tableId", "table_id", "table-id"),
        serialization_alias="tableId",
        default=None,
    )
    config_data: Optional[List[Any]] = Field(
        description="The data of the configuration.",
        validation_alias=AliasChoices("configData", "config_data", "config-data"),
        serialization_alias="configData",
        default=None,
    )
    config_row_ids: Optional[List[str]] = Field(
        description="The row IDs of the configuration.",
        validation_alias=AliasChoices("configRowIds", "config_row_ids", "config-row-ids"),
        serialization_alias="configRowIds",
        default=None,
    )
    run_id: Optional[str] = Field(
        description="The ID of the run that the job is running on.",
        validation_alias=AliasChoices("runId", "run_id", "run-id"),
        serialization_alias="runId",
        default=None,
    )
    parent_run_id: Optional[str] = Field(
        description="The ID of the parent run that the job is running on.",
        validation_alias=AliasChoices("parentRunId", "parent_run_id", "parent-run-id"),
        serialization_alias="parentRunId",
        default=None,
    )
    duration_seconds: Optional[float] = Field(
        description="The duration of the job in seconds.",
        validation_alias=AliasChoices("durationSeconds", "duration_seconds", "duration-seconds"),
        serialization_alias="durationSeconds",
        default=None,
    )
    result: Optional[Dict[str, Any]] = Field(
        description="The results of the job.",
        validation_alias="result",
        serialization_alias="result",
        default=None,
    )
    metrics: Optional[Dict[str, Any]] = Field(
        description="The metrics of the job.",
        validation_alias="metrics",
        serialization_alias="metrics",
        default=None,
    )


######################################## End of Job Base Models ########################################

######################################## MCP tools ########################################

SORT_BY_VALUES = Literal["startTime", "endTime", "createdTime", "durationSeconds", "id"]
SORT_ORDER_VALUES = Literal["asc", "desc"]


def add_jobs_tools(mcp: FastMCP) -> None:
    """Add tools to the MCP server."""
    jobs_tools = [
        retrieve_jobs_in_project,
        get_job_details,
        retrieve_component_config_jobs,
    ]
    for tool in jobs_tools:
        logger.info(f"Adding tool {tool.__name__} to the MCP server.")
        mcp.add_tool(tool)

    logger.info("Jobs tools initialized.")


async def retrieve_jobs_in_project(
    ctx: Context,
    status: Annotated[
        JOB_STATUS,
        Field(
            Optional[JOB_STATUS],
            description="The status of the jobs to filter by.",
        ),
    ] = None,
    limit: Annotated[
        int, Field(int, description="The number of jobs to list.", ge=1, le=500)
    ] = 100,
    offset: Annotated[int, Field(int, description="The offset of the jobs to list.", ge=0)] = 0,
    sort_by: Annotated[
        SORT_BY_VALUES,
        Field(Optional[SORT_BY_VALUES], description="The field to sort the jobs by."),
    ] = "startTime",
    sort_order: Annotated[
        SORT_ORDER_VALUES,
        Field(Optional[SORT_ORDER_VALUES], description="The order to sort the jobs by."),
    ] = "desc",
) -> List[JobListItem]:
    """
    Retrieve jobs in the project and optionally filter them by status, limit, offset, sort_by, sort_order.
    PARAMETERS:
        status (optional): The status of the jobs to filter by, if None then default will be all.
        limit (optional): The number of jobs to list, default = 100, max = 500.
        offset (optional): The offset of the jobs to list, default = 0.
        sort_by (optional): The field to sort the jobs by, default = "startTime".
        sort_order (optional): The order to sort the jobs by, default = "desc".
    RETURNS:
        - A list of job list items, if empty then no jobs were found.
    EXAMPLES:
        - if status = "error", only jobs with status "error" will be listed.
        - if status = None, then all jobs with arbitrary status will be listed.
        - if limit = 100 and offset = 0, the first 100 jobs will be listed.
        - if limit = 100 and offset = 100, the second 100 jobs will be listed.
        - if sort_by = "startTime" and sort_order = "asc", the jobs will be sorted by the start time in ascending order.
    """
    client = KeboolaClient.from_state(ctx.session.state)
    _status = [status] if status else None

    r_jobs = client.jobs_queue.list(
        limit=limit, offset=offset, status=_status, sort_by=sort_by, sort_order=sort_order
    )
    logger.info(f"Found {len(r_jobs)} jobs for limit {limit}, offset {offset}, status {status}.")
    return [JobListItem.model_validate(r_job) for r_job in r_jobs]


async def get_job_details(
    job_id: Annotated[
        str,
        Field(description="The unique identifier of the job whose details should be retrieved."),
    ],
    ctx: Context,
) -> JobDetail:
    """
    Retrieve a detailed information about a specific job, identified by the job_id, including its status, parameters,
    results, and any relevant metadata.
    PARAMETERS:
        job_id: The unique identifier of the job whose details should be retrieved.
    RETURNS:
        - A job detail object.
    """
    client = KeboolaClient.from_state(ctx.session.state)

    r_job = client.jobs_queue.detail(job_id)
    logger.info(f"Found job details for {job_id}." if r_job else f"Job {job_id} not found.")
    return JobDetail.model_validate(r_job)


async def retrieve_component_config_jobs(
    ctx: Context,
    component_id: Annotated[
        str, Field(str, description="The ID of the component whose jobs you want to list.")
    ],
    config_id: Annotated[
        str,
        Field(
            Optional[str],
            description="The ID of the component configuration whose jobs you want to list.",
        ),
    ] = None,
    status: Annotated[
        JOB_STATUS,
        Field(
            Optional[JOB_STATUS],
            description="The status of the jobs to filter by.",
        ),
    ] = None,
    limit: Annotated[
        int, Field(int, description="The number of jobs to list.", ge=1, le=500)
    ] = 100,
    offset: Annotated[int, Field(int, description="The offset of the jobs to list.", ge=0)] = 0,
    sort_by: Annotated[
        SORT_BY_VALUES,
        Field(Optional[SORT_BY_VALUES], description="The field to sort the jobs by."),
    ] = "startTime",
    sort_order: Annotated[
        SORT_ORDER_VALUES,
        Field(Optional[SORT_ORDER_VALUES], description="The order to sort the jobs by."),
    ] = "desc",
) -> List[JobListItem]:
    """
    Retrieve jobs that ran for a given component id and optionally for a given configuration id and filter them
    by status, limit, offset, sort_by, sort_order.
    RETURNS:
        - A list of job list items for given component (configuration), if empty then no jobs were found.
    PARAMETERS:
        component_id: The ID of the component whose jobs you want to list.
        config_id (optional): The ID of the component configuration whose jobs you want to list.
        status (optional): The status of the jobs to filter by, if None then default will be all.
        limit (optional): The number of jobs to list, default = 100, max = 500.
        offset (optional): The offset of the jobs to list, default = 0.
        sort_by (optional): The field to sort the jobs by, default = "startTime".
        sort_order (optional): The order to sort the jobs by, default = "desc".
    EXAMPLES:
        - if component_id = "123" and config_id = "456", then the jobs for the component with id "123" and configuration
          with id "456" will be listed.
        - if limit = 100 and offset = 0, the first 100 jobs will be listed.
        - if limit = 100 and offset = 100, the second 100 jobs will be listed.

    """
    client = KeboolaClient.from_state(ctx.session.state)

    _status = [status] if status else None
    params = {
        "componentId": component_id,
        "configId": config_id,
        "status": _status,
        "limit": limit,
        "offset": offset,
        "sortBy": sort_by,
        "sortOrder": sort_order,
    }
    r_jobs = client.jobs_queue.search(params)
    logger.info(
        f"Found {len(r_jobs)} jobs for component {component_id}, configuration {config_id}, with limit {limit}, "
        f"offset {offset}, status {status}."
    )
    return [JobListItem.model_validate(r_job) for r_job in r_jobs]


######################################## End of MCP tools ########################################
