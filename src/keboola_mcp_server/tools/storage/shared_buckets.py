"""Data Catalog "shared with me" tools: discover buckets shared with the project that are
not yet linked, and link them in.
"""

import logging
from typing import Annotated, Literal, cast

from fastmcp import Context
from fastmcp.tools import FunctionTool
from mcp.types import ToolAnnotations
from pydantic import AliasChoices, BaseModel, Field, model_validator

from keboola_mcp_server.clients.client import KeboolaClient
from keboola_mcp_server.errors import tool_errors
from keboola_mcp_server.mcp import KeboolaMcpServer, toon_serializer_compact
from keboola_mcp_server.tools.components.utils import get_nested
from keboola_mcp_server.tools.storage.tools import STORAGE_TOOLS_TAG, BucketDetail

LOG = logging.getLogger(__name__)

DEFAULT_SHARED_BUCKETS_LIMIT = 50
MAX_SHARED_BUCKETS_LIMIT = 100


def add_shared_bucket_tools(mcp: KeboolaMcpServer) -> None:
    """Adds the Data Catalog discovery/link tools to the MCP server."""
    mcp.add_tool(
        FunctionTool.from_function(
            get_shared_buckets,
            annotations=ToolAnnotations(readOnlyHint=True),
            serializer=toon_serializer_compact,
            tags={STORAGE_TOOLS_TAG},
        )
    )
    mcp.add_tool(
        FunctionTool.from_function(
            link_shared_bucket,
            annotations=ToolAnnotations(destructiveHint=False),
            serializer=toon_serializer_compact,
            tags={STORAGE_TOOLS_TAG},
        )
    )
    LOG.info('Shared bucket tools added to the MCP server.')


class SharedBucketDetail(BaseModel):
    """A bucket shared with this project (Data Catalog "Shared with me") that may not be linked yet."""

    id: str = Field(description='Unique identifier of the shared bucket in its source project.')
    display_name: str = Field(
        description='The display name of the shared bucket.',
        validation_alias=AliasChoices('displayName', 'display_name'),
    )
    stage: str = Field(description='Stage of the shared bucket (in for input stage, out for output stage).')
    description: str | None = Field(None, description='Description of the shared bucket.')
    project_id: str | None = Field(default=None, description='The ID of the project the shared bucket belongs to.')
    project_name: str | None = Field(default=None, description='The name of the project the shared bucket belongs to.')
    sharing: str | None = Field(
        default=None,
        description=(
            "Sharing scope: 'organization' (all projects in the organization), "
            "'organization-project' (all projects in the source project's organization), or "
            "'specific-projects' (only projects explicitly targeted)."
        ),
    )
    linked_by: list[str] | None = Field(
        default=None,
        description='IDs of projects that have already linked this shared bucket, if reported by the API.',
        validation_alias=AliasChoices('linkedBy', 'linked_by'),
    )
    tables_count: int | None = Field(
        default=None,
        description='Number of tables in the shared bucket.',
        validation_alias=AliasChoices('tablesCount', 'tables_count'),
    )
    rows_count: int | None = Field(
        default=None,
        description='Total number of rows across all tables in the shared bucket.',
        validation_alias=AliasChoices('rowsCount', 'rows_count'),
    )
    data_size_bytes: int | None = Field(
        default=None,
        description='Total data size of the shared bucket in bytes.',
        validation_alias=AliasChoices('dataSizeBytes', 'data_size_bytes'),
    )

    @model_validator(mode='before')
    @classmethod
    def set_project_fields(cls, values: dict) -> dict:
        if project := cast(dict | None, get_nested(values, 'project')):
            values.setdefault('project_id', project.get('id'))
            values.setdefault('project_name', project.get('name'))
        return values


class GetSharedBucketsOutput(BaseModel):
    shared_buckets: list[SharedBucketDetail] = Field(description='Page of shared buckets, per limit/offset.')
    total_count: int = Field(description='Total number of shared buckets available, independent of limit/offset.')
    message: str | None = Field(
        default=None, description='Human-readable note about pagination, e.g. when more results are available.'
    )


@tool_errors()
async def get_shared_buckets(
    ctx: Context,
    limit: Annotated[
        int, Field(description='Maximum number of shared buckets to return.')
    ] = DEFAULT_SHARED_BUCKETS_LIMIT,
    offset: Annotated[int, Field(description='Number of shared buckets to skip, for pagination.')] = 0,
) -> GetSharedBucketsOutput:
    """
    Lists buckets shared with this project by other Keboola projects that are not necessarily
    linked into this project yet (the Data Catalog "Shared with me" view). Use this to answer
    "what data is shared with this project?" or "what could I link?" — `get_buckets` only
    returns buckets already in this project (including already-linked ones).

    Results are paginated (`limit`/`offset`) because the number of shared buckets can be large
    for projects in big organizations — always check `total_count` and the `message` to see
    whether more results exist, and page through with `offset` rather than assuming a single
    call returns everything.

    Use `link_shared_bucket` to link a shared bucket returned here into this project.
    """
    if not 0 < limit <= MAX_SHARED_BUCKETS_LIMIT:
        LOG.warning(
            f'The "limit" parameter is out of range (0, {MAX_SHARED_BUCKETS_LIMIT}], setting to default value '
            f'{DEFAULT_SHARED_BUCKETS_LIMIT}.'
        )
        limit = DEFAULT_SHARED_BUCKETS_LIMIT
    offset = max(0, offset)

    client = KeboolaClient.from_state(ctx.session.state)
    raw_shared_buckets = await client.storage_client.shared_bucket_list()
    raw_shared_buckets = sorted(raw_shared_buckets, key=lambda raw: raw['id'])

    total_count = len(raw_shared_buckets)
    raw_page = raw_shared_buckets[offset : offset + limit]
    page = [SharedBucketDetail.model_validate(raw) for raw in raw_page]

    message: str | None = None
    if offset + len(page) < total_count:
        message = (
            f'Returning {len(page)} of {total_count} shared buckets. ' f'Use offset={offset + len(page)} to see more.'
        )
    elif total_count:
        message = f'Returning {len(page)} of {total_count} shared buckets.'

    return GetSharedBucketsOutput(shared_buckets=page, total_count=total_count, message=message)


@tool_errors()
async def link_shared_bucket(
    ctx: Context,
    source_project_id: Annotated[str, Field(description='The ID of the project the shared bucket belongs to.')],
    source_bucket_id: Annotated[
        str, Field(description='The ID of the shared bucket to link, from `get_shared_buckets`.')
    ],
    target_bucket_name: Annotated[str, Field(description='The name the linked bucket should have in this project.')],
    target_stage: Annotated[
        Literal['in', 'out'] | None,
        Field(
            description=(
                "Stage for the linked bucket in this project. Defaults to the source bucket's own stage "
                '(parsed from its "in."/"out." ID prefix) — only pass this to deliberately re-stage on link.'
            )
        ),
    ] = None,
    display_name: Annotated[str | None, Field(description='Optional display name for the linked bucket.')] = None,
) -> BucketDetail:
    """
    Links a bucket shared with this project (found via `get_shared_buckets`) into this project
    as a new local bucket, so its tables become directly queryable/joinable like any other
    bucket in the project.
    """
    stage = target_stage or (source_bucket_id.split('.', 1)[0] if '.' in source_bucket_id else None)
    if stage not in ('in', 'out'):
        raise ValueError(
            f'Could not determine stage from source_bucket_id={source_bucket_id!r}; pass target_stage explicitly.'
        )

    client = KeboolaClient.from_state(ctx.session.state)
    raw_bucket = await client.storage_client.bucket_link(
        name=target_bucket_name,
        stage=stage,
        source_project_id=source_project_id,
        source_bucket_id=source_bucket_id,
        display_name=display_name,
    )
    return BucketDetail.model_validate(raw_bucket)
