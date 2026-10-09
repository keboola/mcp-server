import logging
from collections.abc import Sequence
from typing import Annotated, cast

from fastmcp import Context
from fastmcp.exceptions import ToolError
from mcp.types import ToolAnnotations
from pydantic import AliasChoices, BaseModel, Field

from keboola_mcp_server.clients.base import JsonDict
from keboola_mcp_server.clients.client import KeboolaClient
from keboola_mcp_server.clients.stream import ColumnTemplate, OtlpSignal, SourceType, TableColumn
from keboola_mcp_server.errors import tool_errors
from keboola_mcp_server.links import Link, ProjectLinksManager
from keboola_mcp_server.mcp import KeboolaMcpServer, ToolsFilteringMiddleware, ToonCompactFunctionTool
from keboola_mcp_server.mcp import PlainFunctionTool as FunctionTool
from keboola_mcp_server.scope import ProjectIdArg

LOG = logging.getLogger(__name__)

STREAM_TOOLS_TAG = 'streams'

DATA_STREAMS_FEATURE = 'data-streams'
PROTECTED_DEFAULT_BRANCH_FEATURE = 'protected-default-branch'


def add_stream_tools(mcp: KeboolaMcpServer) -> None:
    """Add Data Streams tools to the MCP server."""
    mcp.add_tool(
        ToonCompactFunctionTool.from_function(
            get_streams,
            annotations=ToolAnnotations(readOnlyHint=True),
            tags={STREAM_TOOLS_TAG},
        )
    )
    mcp.add_tool(
        FunctionTool.from_function(
            create_stream,
            annotations=ToolAnnotations(destructiveHint=False),
            tags={STREAM_TOOLS_TAG},
        )
    )
    LOG.info('Stream tools added to the MCP server.')


# Default sink columns ########################################

HTTP_DEFAULT_COLUMNS: list[TableColumn] = [
    TableColumn(name='id', type='uuid'),
    TableColumn(name='datetime', type='datetime'),
    TableColumn(name='ip', type='ip'),
    TableColumn(name='body', type='body'),
    TableColumn(name='headers', type='headers'),
]


def _optional_body_value(name: str, path: str) -> TableColumn:
    """A template column reading `path` from the OTLP record, NULL when missing (so the record is never rejected)."""
    return TableColumn(name=name, type='template', template=ColumnTemplate(content=f"Body('{path}', null)"))


OTLP_RESOURCE_COLUMNS: list[TableColumn] = [
    _optional_body_value('service', 'resource.service.name'),
    _optional_body_value('service_version', 'resource.service.version'),
    _optional_body_value('service_instance_id', 'resource.service.instance.id'),
    _optional_body_value('host_name', 'resource.host.name'),
    _optional_body_value('k8s_pod_name', 'resource.k8s.pod.name'),
    _optional_body_value('k8s_namespace', 'resource.k8s.namespace.name'),
    _optional_body_value('deployment_environment', 'resource.deployment.environment'),
]

OTLP_SIGNAL_COLUMNS: dict[OtlpSignal, list[TableColumn]] = {
    'logs': [
        TableColumn(name='datetime', type='datetime'),
        _optional_body_value('timestamp', 'timestamp'),
        _optional_body_value('severity', 'severity_text'),
        _optional_body_value('severity_number', 'severity_number'),
        _optional_body_value('message', 'body'),
        *OTLP_RESOURCE_COLUMNS,
        _optional_body_value('trace_id', 'trace_id'),
        _optional_body_value('span_id', 'span_id'),
        _optional_body_value('attributes', 'attributes'),
        _optional_body_value('resource', 'resource'),
    ],
    'metrics': [
        TableColumn(name='datetime', type='datetime'),
        _optional_body_value('timestamp', 'timestamp'),
        _optional_body_value('start_timestamp', 'start_timestamp'),
        _optional_body_value('metric_name', 'metric_name'),
        _optional_body_value('value', 'value'),
        *OTLP_RESOURCE_COLUMNS,
        _optional_body_value('attributes', 'attributes'),
        _optional_body_value('resource', 'resource'),
    ],
    'traces': [
        TableColumn(name='datetime', type='datetime'),
        _optional_body_value('timestamp', 'timestamp'),
        _optional_body_value('end_timestamp', 'end_timestamp'),
        _optional_body_value('trace_id', 'trace_id'),
        _optional_body_value('span_id', 'span_id'),
        _optional_body_value('parent_span_id', 'parent_span_id'),
        _optional_body_value('name', 'name'),
        *OTLP_RESOURCE_COLUMNS,
        _optional_body_value('attributes', 'attributes'),
        _optional_body_value('resource', 'resource'),
    ],
}

OTLP_RAW_COLUMN = TableColumn(name='raw', type='body')


# Models ########################################


class StreamSink(BaseModel):
    """A sink writing the events received by a source into a Storage table."""

    sink_id: str = Field(validation_alias=AliasChoices('sinkId', 'sink_id'), description='The sink ID.')
    name: str = Field(description='The sink name.')
    table_id: str | None = Field(default=None, description='The destination Storage table ID.')
    columns: list[TableColumn] = Field(default_factory=list, description='The destination table columns mapping.')
    allowed_signals: list[OtlpSignal] = Field(
        default_factory=list,
        validation_alias=AliasChoices('allowedSignals', 'allowed_signals'),
        description='OTLP signals this sink accepts; empty means all. Ignored for HTTP sources.',
    )
    disabled: bool = Field(default=False, description='Whether the sink is disabled.')

    @classmethod
    def from_api(cls, raw: JsonDict) -> 'StreamSink':
        table = cast(JsonDict, raw.get('table') or {})
        mapping = cast(JsonDict, table.get('mapping') or {})
        return cls.model_validate(
            {
                **raw,
                'table_id': table.get('tableId'),
                'columns': mapping.get('columns') or [],
                'disabled': bool(raw.get('disabled')),
            }
        )


class StreamSource(BaseModel):
    """A Data Stream source: an endpoint receiving events, with the sinks storing them."""

    source_id: str = Field(validation_alias=AliasChoices('sourceId', 'source_id'), description='The source ID.')
    name: str = Field(description='The source name.')
    description: str | None = Field(default=None, description='The source description.')
    type: SourceType = Field(description='The source type: "http" (webhooks/HTTP events) or "otlp" (OpenTelemetry).')
    endpoint_url: str | None = Field(
        default=None,
        description='The URL to send events to. It embeds the source secret: share it only with the user.',
    )
    otlp_base_url: str | None = Field(
        default=None,
        description=(
            'OTLP only: the endpoint without the secret. Use it as OTEL_EXPORTER_OTLP_ENDPOINT together with '
            'the "Authorization: Bearer <otlp_secret>" header. The SDK appends /v1/logs|metrics|traces itself.'
        ),
    )
    otlp_secret: str | None = Field(default=None, description='OTLP only: the secret for the Bearer header.')
    secret_redacted: bool = Field(
        default=False,
        description='True when `endpoint_url` and `otlp_secret` are hidden because this session has read-only access.',
    )
    disabled: bool = Field(default=False, description='Whether the source is disabled.')
    sinks: list[StreamSink] = Field(default_factory=list, description='The sinks of the source.')
    links: list[Link] = Field(default_factory=list, description='Links to the source in the Keboola UI.')

    @classmethod
    def from_api(cls, raw: JsonDict, links: list[Link], *, include_secret: bool = True) -> 'StreamSource':
        http = cast(JsonDict, raw.get('http') or {})
        otlp = cast(JsonDict, raw.get('otlp') or {})
        return cls.model_validate(
            {
                **raw,
                'endpoint_url': (http.get('url') or otlp.get('url')) if include_secret else None,
                'otlp_base_url': otlp.get('baseUrl'),
                'otlp_secret': otlp.get('secret') if include_secret else None,
                'secret_redacted': not include_secret,
                'disabled': bool(raw.get('disabled')),
                'sinks': [StreamSink.from_api(cast(JsonDict, s)) for s in cast(list, raw.get('sinks') or [])],
                'links': links,
            }
        )


class GetStreamsOutput(BaseModel):
    streams: list[StreamSource] = Field(description='The Data Streams in the project.')
    links: list[Link] = Field(description='Links to the Data Streams in the Keboola UI.')


# Helpers ########################################


async def ensure_data_streams_available(client: KeboolaClient, links_manager: ProjectLinksManager) -> None:
    """Raises a ToolError explaining why Data Streams cannot be used in the current project/branch."""
    if client.branch_id:
        raise ToolError('Data Streams are supported only in the main production branch.')
    if await client.has_feature(PROTECTED_DEFAULT_BRANCH_FEATURE):
        raise ToolError('Data Streams are not available in projects with a protected default branch.')
    if not await client.has_feature(DATA_STREAMS_FEATURE):
        unlock_link = links_manager.get_data_streams_dashboard_link()
        raise ToolError(
            'Data Streams are not enabled in this project. Give the user this link to the Data Streams page, '
            f'where they can request the feature by clicking "Unlock Data Streams": {unlock_link.url}'
        )


async def can_write(client: KeboolaClient) -> bool:
    """Whether the session may write; read-only sessions must not see the stream secrets that authorize writes."""
    if client.readonly:
        return False
    token_info = await client.storage_client.verify_token()
    return ToolsFilteringMiddleware.get_token_role(token_info).lower() != 'readonly'


def http_bucket_id(source_id: str) -> str:
    return f'in.c-data-stream-{source_id}'


def otlp_bucket_id(source_id: str) -> str:
    return f'in.c-otlp-{source_id}'


async def _get_stream(client: KeboolaClient, links_manager: ProjectLinksManager, source_id: str) -> StreamSource:
    raw_sources = await client.stream_client.list_sources()
    raw = next((s for s in raw_sources if s.get('sourceId') == source_id), None)
    if raw is None:
        raise ToolError(f'Data Stream "{source_id}" not found.')
    return StreamSource.from_api(raw, links_manager.get_data_stream_links(source_id, cast(str, raw.get('name'))))


async def _create_sinks(
    client: KeboolaClient,
    *,
    source_id: str,
    source_name: str,
    source_type: SourceType,
    table_id: str | None,
    columns: list[TableColumn] | None,
    include_raw_otlp_record: bool,
) -> None:
    if source_type == 'http':
        await client.stream_client.create_table_sink(
            source_id,
            name=source_name,
            table_id=table_id or f'{http_bucket_id(source_id)}.events',
            columns=columns or HTTP_DEFAULT_COLUMNS,
        )
        return

    for signal, signal_columns in OTLP_SIGNAL_COLUMNS.items():
        await client.stream_client.create_table_sink(
            source_id,
            name=signal.capitalize(),
            table_id=f'{otlp_bucket_id(source_id)}.{signal}',
            columns=[*signal_columns, OTLP_RAW_COLUMN] if include_raw_otlp_record else signal_columns,
            allowed_signals=[signal],
        )


# MCP tools ########################################


@tool_errors()
async def get_streams(
    ctx: Context,
    source_ids: Annotated[
        Sequence[str],
        Field(description='IDs of the Data Streams (sources) to retrieve. Empty [] lists all of them.'),
    ] = (),
) -> GetStreamsOutput:
    """
    Retrieves the Data Streams in the project: sources receiving events and the sinks writing them to tables.

    Data Streams receive events over HTTP (webhooks, apps) or OTLP (OpenTelemetry logs/metrics/traces) and
    store them in Storage tables in near real time, without any component configuration.

    Each stream includes its `endpoint_url` (with the secret embedded) and, for OTLP, `otlp_base_url` plus
    `otlp_secret`. Give these only to the user who asked; they authenticate writes into the project.
    Sessions with read-only access get them hidden (`secret_redacted=true`).

    If Data Streams are not enabled in the project, the tool fails with a link to the Data Streams page
    where the user can request the feature. Always pass that link on to the user.

    EXAMPLES:
    - source_ids=[] -> all Data Streams with their sinks and endpoints
    - source_ids=["github-webhooks"] -> only that stream
    """
    client = KeboolaClient.from_state(ctx.session.state)
    links_manager = await ProjectLinksManager.from_client(client)
    await ensure_data_streams_available(client, links_manager)

    raw_sources = await client.stream_client.list_sources()
    if source_ids:
        wanted = set(source_ids)
        raw_sources = [s for s in raw_sources if s.get('sourceId') in wanted]
        if missing := wanted - {cast(str, s.get('sourceId')) for s in raw_sources}:
            raise ToolError(f'Data Streams not found: {", ".join(sorted(missing))}.')

    include_secret = await can_write(client)
    streams = [
        StreamSource.from_api(
            raw,
            links_manager.get_data_stream_links(cast(str, raw.get('sourceId')), cast(str, raw.get('name'))),
            include_secret=include_secret,
        )
        for raw in raw_sources
    ]
    LOG.info(f'Found {len(streams)} Data Streams.')
    return GetStreamsOutput(
        streams=streams,
        links=[links_manager.get_data_streams_dashboard_link(), links_manager.get_data_streams_docs_link()],
    )


@tool_errors()
async def create_stream(
    ctx: Context,
    name: Annotated[str, Field(description='Human readable name of the stream, max 40 characters.', max_length=40)],
    source_type: Annotated[
        SourceType,
        Field(description='"http" for webhooks/HTTP events, "otlp" for OpenTelemetry logs, metrics and traces.'),
    ] = 'http',
    description: Annotated[str, Field(description='Optional description of the stream.')] = '',
    table_id: Annotated[
        str | None,
        Field(
            description=(
                'HTTP only: the destination table, e.g. "in.c-github.events". It is created if it does not exist. '
                'Defaults to "in.c-data-stream-<source_id>.events".'
            )
        ),
    ] = None,
    columns: Annotated[
        list[TableColumn] | None,
        Field(
            description=(
                'HTTP only: the destination table columns. Defaults to id (uuid), datetime, ip, body and headers. '
                'Use "path" columns to extract fields from a JSON body, e.g. '
                '{"name": "user_id", "type": "path", "path": "user.id"}.'
            )
        ),
    ] = None,
    include_raw_otlp_record: Annotated[
        bool,
        Field(description='OTLP only: also store the whole raw OTLP record in a "raw" column (voluminous).'),
    ] = False,
    project_id: ProjectIdArg = None,
) -> StreamSource:
    """
    Creates a Data Stream: a source with an endpoint receiving events and the sinks storing them in tables.

    - source_type="http": one sink into `table_id` with the given `columns`. Send events as HTTP POST
      requests (any body, JSON recommended) to the returned `endpoint_url`.
    - source_type="otlp": three sinks storing logs, metrics and traces into the tables
      "in.c-otlp-<source_id>.logs|metrics|traces". Configure the OpenTelemetry SDK/Collector OTLP/HTTP exporter:
      OTEL_EXPORTER_OTLP_ENDPOINT=<otlp_base_url> and OTEL_EXPORTER_OTLP_HEADERS="Authorization=Bearer <otlp_secret>".

    Rows appear in the tables in batches, typically within a few minutes.

    The returned endpoint embeds a secret: give it only to the user. If Data Streams are not enabled in the
    project, the tool fails with a link to the Data Streams page where the user can request the feature.
    Always pass that link on to the user.
    """
    if source_type == 'otlp' and (table_id or columns):
        raise ToolError('"table_id" and "columns" apply only to HTTP streams; OTLP streams use fixed tables.')

    client = KeboolaClient.from_state(ctx.session.state)
    links_manager = await ProjectLinksManager.from_client(client)
    await ensure_data_streams_available(client, links_manager)

    outputs = await client.stream_client.create_source(name=name, source_type=source_type, description=description)
    source_id = cast(str, outputs['sourceId'])
    LOG.info(f'Created Data Stream source "{source_id}".')

    try:
        await _create_sinks(
            client,
            source_id=source_id,
            source_name=name,
            source_type=source_type,
            table_id=table_id,
            columns=columns,
            include_raw_otlp_record=include_raw_otlp_record,
        )
    except Exception:
        LOG.exception(f'Failed to create sinks for Data Stream "{source_id}", deleting the source.')
        await client.stream_client.delete_source(source_id)
        raise

    return await _get_stream(client, links_manager, source_id)
