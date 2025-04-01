import logging
from typing import Annotated, Any, Dict, List, Literal, Optional, Union, cast

from mcp.server.fastmcp import Context, FastMCP
from pydantic import AliasChoices, BaseModel, Field, field_validator

from keboola_mcp_server.client import KeboolaClient

logger = logging.getLogger(__name__)

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


######################################## Util functions ########################################


def handle_status_param(status: Optional[Union[List[JOB_STATUS], JOB_STATUS]]) -> List[JOB_STATUS]:
    """
    Handles the status parameter, converting it to a list of all possible statuses if it is None;
    otherwise returns the statuses as is.
    """
    if status is None:
        return list(JOB_STATUS.__args__)
    elif isinstance(status, str):
        return [status]
    else:
        return status


######################################## End of util functions ########################################

######################################## MCP tools ########################################


def add_jobs_tools(mcp: FastMCP) -> None:
    """Add tools to the MCP server."""
    mcp.add_tool(list_jobs)
    mcp.add_tool(get_job_details)
    mcp.add_tool(list_component_config_jobs)
    mcp.add_tool(list_component_jobs)
    logger.info("Jobs tools added to the MCP server.")


async def list_jobs(
    ctx: Context,
    status: Annotated[
        List[JOB_STATUS],
        Field(Optional[list[JOB_STATUS]], description="The status of the jobs to list."),
    ] = None,
    limit: Annotated[
        int, Field(int, description="The number of jobs to list.", ge=1, le=500)
    ] = 100,
    offset: Annotated[int, Field(int, description="The offset of the jobs to list.", ge=0)] = 0,
) -> List[JobListItem]:
    """
    List most recent jobs limited by the limit and shifted by the offset.
    :param status (optional): The status of the jobs to list, if None then default = all.
        - E.g. if status = ["success", "error"], only jobs with status "success" or "error" will be listed.
        - E.g. if status = None, then all jobs will be listed.
    :param limit (optional): The number of jobs to list, default = 100, max = 500.
    :param offset (optional): The offset of the jobs to list, default = 0.
        - E.g. if limit = 100 and offset = 0, the first 100 jobs will be listed.
        - E.g. if limit = 100 and offset = 100, the second 100 jobs will be listed.
    :return: A list of job list items, if empty then no jobs were found.
    """
    client = ctx.session.state["sapi_client"]
    assert isinstance(client, KeboolaClient)

    status = handle_status_param(status=status)

    r_jobs = client.jobs_queue.list(limit=limit, offset=offset, status=status)
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
    Retrieves detailed information about a specific job, identified by the job_id, including its status, parameters,
    results, and any relevant metadata.
    :param job_id: The unique identifier of the job whose details should be retrieved.
    :return: A job detail object
    """
    client = ctx.session.state["sapi_client"]
    assert isinstance(client, KeboolaClient)

    r_job = client.jobs_queue.detail(job_id)
    logger.info(f"Found job details for {job_id}." if r_job else f"Job {job_id} not found.")
    return JobDetail.model_validate(r_job)


async def list_component_config_jobs(
    ctx: Context,
    component_id: Annotated[
        str, Field(str, description="The ID of the component whose jobs you want to list.")
    ],
    config_id: Annotated[
        str,
        Field(
            str, description="The ID of the component configuration whose jobs you want to list."
        ),
    ],
    status: Annotated[
        List[JOB_STATUS],
        Field(
            default=None,
            description="The status of the jobs by which the jobs are filtered.",
        ),
    ] = None,
    limit: Annotated[
        int, Field(int, description="The number of jobs to list.", ge=1, le=500)
    ] = 100,
    offset: Annotated[int, Field(int, description="The offset of the jobs to list.", ge=0)] = 0,
) -> List[JobListItem]:
    """
    List most recent jobs that ran for a given component id and configuration id.
    :param component_id: The ID of the component whose jobs you want to list.
    :param config_id: The ID of the component configuration whose jobs you want to list.
    :param status (optional): The status of the jobs to list, if None then default = all.
        - E.g. if status = ["error"], only failed jobs will be listed.
        - E.g. if status = None, then all jobs will be listed.
    :param limit (optional): The number of jobs to list, default = 100, max = 500.
    :param offset (optional): The offset of the jobs to list, default = 0.
        - E.g. if limit = 100 and offset = 0, the first 100 jobs will be listed.
        - E.g. if limit = 100 and offset = 100, the second 100 jobs will be listed.
    :return: A list of job list items.
    """
    client = ctx.session.state["sapi_client"]
    assert isinstance(client, KeboolaClient)

    status = handle_status_param(status=status)
    params = {
        "componentId": component_id,
        "configId": config_id,
        "status": status,
        "limit": limit,
        "offset": offset,
        "sortBy": "startTime",
        "sortOrder": "desc",
    }
    r_jobs = client.jobs_queue.search(params)
    logger.info(
        f"Found {len(r_jobs)} jobs for component {component_id}, configuration {config_id}, with limit {limit}, "
        f"offset {offset}, status {status}."
    )
    return [JobListItem.model_validate(r_job) for r_job in r_jobs]


async def list_component_jobs(
    ctx: Context,
    component_id: Annotated[
        str, Field(str, description="The ID of the component whose jobs you want to list.")
    ],
    status: Annotated[
        List[JOB_STATUS],
        Field(default=None, description="The status of the jobs by which the jobs are filtered."),
    ] = None,
    limit: Annotated[
        int, Field(int, description="The number of jobs to list.", ge=1, le=500)
    ] = 100,
    offset: Annotated[int, Field(int, description="The offset of the jobs to list.", ge=0)] = 0,
) -> List[JobListItem]:
    """
    List most recent jobs that ran for a given component id.
    :param component_id: The ID of the component whose jobs you want to list.
    :param status (optional): The status of the jobs to list, if None then default = all.
        - E.g. if status = ["error"], only failed jobs will be listed.
        - E.g. if status = None, then all jobs will be listed.
    :param limit (optional): The number of jobs to list, default = 100, max = 500.
    :param offset (optional): The offset of the jobs to list, default = 0.
        - E.g. if limit = 100 and offset = 0, the first 100 jobs will be listed.
        - E.g. if limit = 100 and offset = 100, the second 100 jobs will be listed.
    :return: A list of job list items.
    """
    client = ctx.session.state["sapi_client"]
    assert isinstance(client, KeboolaClient)

    status = handle_status_param(status=status)
    params = {
        "componentId": component_id,
        "status": status,
        "limit": limit,
        "offset": offset,
        "sortBy": "startTime",
        "sortOrder": "desc",
    }
    r_jobs = client.jobs_queue.search(params)
    logger.info(
        f"Found {len(r_jobs)} jobs for component {component_id}, with limit {limit}, offset {offset}, status {status}."
    )
    return [JobListItem.model_validate(r_job) for r_job in r_jobs]
