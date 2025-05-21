import datetime
import logging
from typing import Any, Literal, Optional, Union

from mcp.server.fastmcp import Context, FastMCP
from pydantic import AliasChoices, BaseModel, ConfigDict, Field, field_validator

from keboola_mcp_server.client import KeboolaClient
from keboola_mcp_server.errors import tool_errors

LOG = logging.getLogger(__name__)


# Add jobs tools to MCP SERVER ##################################


def add_job_tools(mcp: FastMCP) -> None:
    """Add job tools to the MCP server."""
    jobs_tools = [
        retrieve_jobs,
        get_job_detail,
        start_job,
    ]
    for tool in jobs_tools:
        LOG.info(f'Adding tool {tool.__name__} to the MCP server.')
        mcp.add_tool(tool)

    LOG.info('Job tools initialized.')


# Job Base Models ########################################

JOB_STATUS = Literal[
    'waiting',
    'processing',
    'success',
    'error',
    'created',
]


class JobListItem(BaseModel):
    """Represents a summary of a job with minimal information, used in lists where detailed job data is not required."""

    id: str = Field(description='The ID of the job.')
    status: JOB_STATUS = Field(description='The status of the job.')
    component_id: Optional[str] = Field(
        description='The ID of the component that the job is running on.',
        validation_alias=AliasChoices('component', 'componentId', 'component_id', 'component-id'),
        serialization_alias='componentId',
        default=None,
    )
    config_id: Optional[str] = Field(
        description='The ID of the component configuration that the job is running on.',
        validation_alias=AliasChoices('config', 'configId', 'config_id', 'config-id'),
        serialization_alias='configId',
        default=None,
    )
    is_finished: bool = Field(
        description='Whether the job is finished.',
        validation_alias=AliasChoices('isFinished', 'is_finished', 'is-finished'),
        serialization_alias='isFinished',
        default=False,
    )
    created_time: Optional[datetime.datetime] = Field(
        description='The creation time of the job.',
        validation_alias=AliasChoices('createdTime', 'created_time', 'created-time'),
        serialization_alias='createdTime',
        default=None,
    )
    start_time: Optional[datetime.datetime] = Field(
        description='The start time of the job.',
        validation_alias=AliasChoices('startTime', 'start_time', 'start-time'),
        serialization_alias='startTime',
        default=None,
    )
    end_time: Optional[datetime.datetime] = Field(
        description='The end time of the job.',
        validation_alias=AliasChoices('endTime', 'end_time', 'end-time'),
        serialization_alias='endTime',
        default=None,
    )
    duration_seconds: Optional[float] = Field(
        description='The duration of the job in seconds.',
        validation_alias=AliasChoices('durationSeconds', 'duration_seconds', 'duration-seconds'),
        serialization_alias='durationSeconds',
        default=None,
    )

    model_config = ConfigDict(extra='forbid')


class JobDetail(JobListItem):
    """Represents a detailed job with all available information."""

    url: str = Field(description='The URL of the job.')
    table_id: Optional[str] = Field(
        description='The ID of the table that the job is running on.',
        validation_alias=AliasChoices('tableId', 'table_id', 'table-id'),
        serialization_alias='tableId',
        default=None,
    )
    config_data: Optional[list[Any]] = Field(
        description='The data of the configuration.',
        validation_alias=AliasChoices('configData', 'config_data', 'config-data'),
        serialization_alias='configData',
        default=None,
    )
    config_row_ids: Optional[list[str]] = Field(
        description='The row IDs of the configuration.',
        validation_alias=AliasChoices('configRowIds', 'config_row_ids', 'config-row-ids'),
        serialization_alias='configRowIds',
        default=None,
    )
    run_id: Optional[str] = Field(
        description='The ID of the run that the job is running on.',
        validation_alias=AliasChoices('runId', 'run_id', 'run-id'),
        serialization_alias='runId',
        default=None,
    )
    parent_run_id: Optional[str] = Field(
        description='The ID of the parent run that the job is running on.',
        validation_alias=AliasChoices('parentRunId', 'parent_run_id', 'parent-run-id'),
        serialization_alias='parentRunId',
        default=None,
    )
    result: Optional[dict[str, Any]] = Field(
        description='The results of the job.',
        validation_alias='result',
        serialization_alias='result',
        default=None,
    )

    model_config = ConfigDict(extra='forbid')

    @field_validator('result', mode='before')
    @classmethod
    def validate_result_field(cls, current_value: Union[list[Any], dict[str, Any], None]) -> dict[str, Any]:
        # Ensures that if the result field is passed as an empty list [] or None, it gets converted to an empty dict {}.
        # Why? Because the result is expected to be an Object, but create job endpoint sends [], perhaps it means
        # "empty". This avoids type errors.
        if not isinstance(current_value, dict):
            if not current_value:
                return dict()
            if isinstance(current_value, list):
                raise ValueError(f'Field "result" cannot be a list, expecting dictionary, got: {current_value}.')
        return current_value


class JobListResponse(BaseModel):
    jobs: list[JobListItem]
    model_config = ConfigDict(extra='forbid')


# End of Job Base Models ########################################

# MCP tools ########################################

# Simply using the standard dict for the first parameter
@tool_errors()
async def retrieve_jobs(
        component_id: str,
        config_id: str,
        limit: int,
        offset: int,
        sort_by: str,
        sort_order: str,
        status: str,
        ctx: Context
) -> JobListResponse:
    """
    Retrieves jobs in the project, optionally filtered by status, component ID, or configuration ID.

    USAGE:
    - Use when you want to see jobs in the project, optionally filtered by various criteria.

    EXAMPLES:
    - user_input: `give me all jobs`
        - returns all jobs in the project
    - user_input: `list me all successful jobs`
        - set status to "success"
        - returns all successful jobs in the project
    - user_input: `give me jobs for component ID 'component-123'`
        - set component_id to 'component-123'
        - returns all jobs for that component
    """
    client = KeboolaClient.from_state(ctx.session.state)

    # Process empty strings as None for API compatibility
    status_param = status if status else None
    component_id_param = component_id if component_id else None
    config_id_param = config_id if config_id else None

    # Validate sort_by and sort_order
    valid_sort_by = ['startTime', 'endTime', 'createdTime', 'durationSeconds', 'id']
    valid_sort_order = ['asc', 'desc']

    sort_by_param = sort_by if sort_by in valid_sort_by else 'startTime'
    sort_order_param = sort_order if sort_order in valid_sort_order else 'desc'

    raw_jobs = await client.jobs_queue_client.search_jobs_by(
        status=status_param,
        component_id=component_id_param,
        config_id=config_id_param,
        limit=limit,
        offset=offset,
        sort_by=sort_by_param,
        sort_order=sort_order_param,
    )
    return JobListResponse(jobs=[JobListItem.model_validate(j) for j in raw_jobs])


@tool_errors()
async def get_job_detail(job_id: str, ctx: Context) -> JobDetail:
    """
    Retrieves detailed information about a specific job, identified by the job_id, including its status, parameters,
    results, and any relevant metadata.

    EXAMPLES:
    - If job_id = "123", then the details of the job with id "123" will be retrieved.
    """
    client = KeboolaClient.from_state(ctx.session.state)

    raw_job = await client.jobs_queue_client.get_job_detail(job_id)
    LOG.info(f'Found job details for {job_id}.' if raw_job else f'Job {job_id} not found.')
    return JobDetail.model_validate(raw_job)


@tool_errors()
async def start_job(component_id: str, configuration_id: str, ctx: Context) -> JobDetail:
    """
    Starts a new job for a given component or transformation.
    """
    client = KeboolaClient.from_state(ctx.session.state)

    try:
        raw_job = await client.jobs_queue_client.create_job(
            component_id=component_id, configuration_id=configuration_id
        )
        job = JobDetail.model_validate(raw_job)
        LOG.info(
            f'Started a new job with id: {job.id} for component {component_id} and configuration {configuration_id}.'
        )
        return job
    except Exception as exception:
        LOG.exception(
            f'Error when starting a new job for component {component_id} and configuration {configuration_id}: '
            f'{exception}'
        )
        raise exception


# End of MCP tools ########################################
