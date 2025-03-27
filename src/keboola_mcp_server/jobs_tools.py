import logging
from typing import Annotated, Any, Dict, List, Literal, Optional, Union, cast

from mcp.server.fastmcp import Context, FastMCP
from pydantic import BaseModel, Field

from keboola_mcp_server.client import KeboolaClient

logger = logging.getLogger(__name__)

STR_INT = Union[str, int]


class JobListItem(BaseModel):
    """A list item representing a Keboola job."""

    id: STR_INT = Field(description="The ID of the job.")
    status: Literal["waiting", "processing", "success", "error"] = Field(
        description="The status of the job."
    )
    created_time: Optional[str] = Field(
        description="The creation time of the job.", alias="createdTime", default=None
    )
    start_time: Optional[str] = Field(
        description="The start time of the job.", alias="startTime", default=None
    )
    end_time: Optional[str] = Field(
        description="The end time of the job.", alias="endTime", default=None
    )


class JobDetail(JobListItem):
    """Detailed information about a Keboola job."""

    url: str = Field(description="The URL of the job.")
    table_id: Optional[str] = Field(
        description="The ID of the table that the job is running on.", alias="tableId",
        default=None
    )
    operation_name: str = Field(description="The name of the operation.", alias="operationName", default='')
    operation_params: Dict[str, Any] = Field(
        description="The parameters of the operation.", alias="operationParams", default={}
    )
    run_id: Optional[STR_INT] = Field(
        description="The ID of the run that the job is running on.", alias="runId",
        default=None
    )
    results: Optional[Dict[str, Any]] = Field(description="The results of the job.", default=None)
    metrics: Optional[Dict[str, Any]] = Field(description="The metrics of the job.", default=None)

    def to_job_list_item(self) -> JobListItem:
        return JobListItem.model_validate(
            {
                "id": self.id,
                "status": self.status,
                "createdTime": self.created_time,
                "startTime": self.start_time,
                "endTime": self.end_time,
            }
        )
#### Util functions ####


def filter_by_config_id(
    job: JobDetail,
    config_id: Annotated[str, Field(str, description="The ID of the configuration by which the job is filtered.")],
) -> bool:
    if job.operation_params is None:
        return False
    else:
        return job.operation_params.get("configurationId", None) == config_id


def filter_by_component_id(
    job: JobDetail,
    component_id: Annotated[str, Field(str, description="The ID of the component by which the job is filtered.")],
) -> bool:
    if job.operation_params is None:
        return False
    else:
        return job.operation_params.get("componentId", None) == component_id


#### End of util functions ####

#### MCP tools ####


def add_jobs_tools(mcp: FastMCP) -> None:
    """Add tools to the MCP server."""
    mcp.add_tool(list_jobs)
    mcp.add_tool(get_job_details)
    mcp.add_tool(list_component_config_jobs)
    mcp.add_tool(list_component_jobs)
    logger.info("Jobs tools added to the MCP server.")


async def list_jobs(ctx: Context) -> List[JobListItem]:
    """List all jobs."""
    client = ctx.session.state["sapi_client"]
    assert isinstance(client, KeboolaClient)

    r_jobs = client.storage_client.jobs.list()
    logger.info(f"Found {len(r_jobs)} jobs.")
    return [JobListItem.model_validate(r_job) for r_job in r_jobs]


async def get_job_details(
    job_id: Annotated[str, Field(str, description="The ID of the job you want to get details about.")], ctx: Context
) -> JobDetail:
    """Get the details of a job from the job ID."""
    client = ctx.session.state["sapi_client"]
    assert isinstance(client, KeboolaClient)

    r_job = client.storage_client.jobs.detail(job_id)
    logger.info(f"Found job details for {job_id}." if r_job else f"Job {job_id} not found.")
    return JobDetail.model_validate(r_job)


async def list_component_config_jobs(
    component_id: Annotated[str, Field(str, description="The ID of the component whose jobs you want to list.")],
    config_id: Annotated[str, Field(str, description="The ID of the component configuration whose jobs you want to list.")],
    ctx: Context,
) -> List[JobListItem]:
    """List jobs that ran for a given component id and configuration id."""
    client = ctx.session.state["sapi_client"]
    assert isinstance(client, KeboolaClient)

    r_jobs = client.storage_client.jobs.list()
    jobs = [JobDetail.model_validate(r_job) for r_job in r_jobs]
    jobs = filter(
        lambda j: filter_by_config_id(j, config_id) and filter_by_component_id(j, component_id),
        jobs,
    )
    jobs = list(jobs)
    # Convert JobDetail to JobListItem
    return [j.to_job_list_item() for j in jobs]


async def list_component_jobs(
    component_id: Annotated[str, Field(str, description="The ID of the component whose jobs you want to list.")],
    ctx: Context,
) -> List[JobListItem]:
    """List jobs that ran for a given component id."""
    client = ctx.session.state["sapi_client"]
    assert isinstance(client, KeboolaClient)

    r_jobs = client.storage_client.jobs.list()
    jobs = [JobDetail.model_validate(r_job) for r_job in r_jobs]
    jobs = filter(lambda j: filter_by_component_id(j, component_id), jobs)
    jobs = list(jobs)
    # Convert JobDetail to JobListItem
    return [j.to_job_list_item() for j in jobs]
