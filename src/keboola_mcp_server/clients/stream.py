"""
Keboola Stream API client.

This client handles communication with the Stream API (stream.keboola.com) for managing
Data Streams: sources receiving events over HTTP or OTLP and table sinks writing them to Storage.
"""

import asyncio
import logging
import time
from typing import Any, Literal, cast

from pydantic import AliasChoices, BaseModel, ConfigDict, Field

from keboola_mcp_server.clients.base import JsonDict, KeboolaServiceClient, RawKeboolaClient

LOG = logging.getLogger(__name__)

# Data Streams live only in the main/production branch, which the Stream API addresses as "default".
DEFAULT_BRANCH = 'default'
# The Stream API caps the page size at 100, which is also the max number of sources per branch.
MAX_PAGE_SIZE = 100

SourceType = Literal['http', 'otlp']
OtlpSignal = Literal['logs', 'metrics', 'traces']
ColumnType = Literal['uuid', 'datetime', 'ip', 'body', 'headers', 'path', 'template']


class StreamTaskError(RuntimeError):
    """Raised when an asynchronous Stream API task finishes with an error or does not finish in time."""


class ColumnTemplate(BaseModel):
    language: Literal['jsonnet'] = Field(default='jsonnet', description='The template language.')
    content: str = Field(description='The Jsonnet expression, e.g. "Body(\'user.id\', null)".')


class TableColumn(BaseModel):
    """One column of the destination table and where its value comes from."""

    model_config = ConfigDict(populate_by_name=True)

    name: str = Field(description='The column name in the destination table.')
    type: ColumnType = Field(
        description=(
            'Where the value comes from: "uuid" (generated id), "datetime" (receive time), "ip" (sender IP), '
            '"body" (whole request body), "headers" (request headers), "path" (a value at `path` in the JSON '
            'body), "template" (a Jsonnet expression in `template`).'
        )
    )
    path: str | None = Field(default=None, description='JSON path in the body, e.g. "user.id". Only for "path".')
    default_value: str | None = Field(
        default=None,
        validation_alias=AliasChoices('defaultValue', 'default_value'),
        serialization_alias='defaultValue',
        description='Fallback value when `path` does not exist. Only for "path".',
    )
    raw_string: bool | None = Field(
        default=None,
        validation_alias=AliasChoices('rawString', 'raw_string'),
        serialization_alias='rawString',
        description='Store a string value without JSON quotes. Only for "path".',
    )
    template: ColumnTemplate | None = Field(default=None, description='The template. Only for "template".')


class StreamTask(BaseModel):
    task_id: str = Field(validation_alias=AliasChoices('taskId', 'task_id'))
    is_finished: bool = Field(validation_alias=AliasChoices('isFinished', 'is_finished'))
    status: str
    error: str | None = None
    outputs: dict[str, Any] = Field(default_factory=dict)


class StreamClient(KeboolaServiceClient):
    """Client for interacting with the Keboola Stream API."""

    TASK_TIMEOUT_SECONDS = 60.0
    TASK_POLL_INTERVAL_SECONDS = 0.5

    def __init__(self, raw_client: RawKeboolaClient) -> None:
        super().__init__(raw_client=raw_client)

    @classmethod
    def create(
        cls,
        root_url: str,
        token: str | None,
        headers: dict[str, Any] | None = None,
        readonly: bool | None = None,
    ) -> 'StreamClient':
        """
        Creates a StreamClient.

        :param root_url: The root URL of the Stream API, e.g. "https://stream.keboola.com".
        :param token: The Keboola Storage API token or a "Bearer ..." credential.
        :param headers: Additional headers for the requests.
        :param readonly: If True, the client will only use HTTP GET, HEAD operations.
        """
        return cls(
            raw_client=RawKeboolaClient(base_api_url=root_url, api_token=token, headers=headers, readonly=readonly)
        )

    @staticmethod
    def _sources_endpoint(suffix: str = '') -> str:
        return f'v1/branches/{DEFAULT_BRANCH}/sources{suffix}'

    async def list_sources(self) -> list[JsonDict]:
        """Lists all sources in the main branch, each with its sinks."""
        sources: list[JsonDict] = []
        after_id = ''
        while True:
            response = cast(
                JsonDict,
                await self.get(
                    endpoint=f'v1/branches/{DEFAULT_BRANCH}/aggregation/sources',
                    params={'afterId': after_id, 'limit': MAX_PAGE_SIZE},
                ),
            )
            page_sources = cast(list[JsonDict], response.get('sources') or [])
            sources.extend(page_sources)
            page = cast(JsonDict, response.get('page') or {})
            if len(sources) >= cast(int, page.get('totalCount') or 0) or not page_sources:
                return sources
            after_id = cast(str, page.get('lastId'))

    async def get_source(self, source_id: str) -> JsonDict:
        return cast(JsonDict, await self.get(endpoint=self._sources_endpoint(f'/{source_id}')))

    async def create_source(self, name: str, source_type: SourceType, description: str | None = None) -> JsonDict:
        """Creates a source and returns the outputs of the finished creation task (incl. `sourceId`)."""
        payload: dict[str, Any] = {'name': name, 'type': source_type}
        if description:
            payload['description'] = description
        task = await self.post(endpoint=self._sources_endpoint(), data=payload)
        return await self.wait_for_task(cast(JsonDict, task))

    async def create_table_sink(
        self,
        source_id: str,
        name: str,
        table_id: str,
        columns: list[TableColumn],
        allowed_signals: list[OtlpSignal] | None = None,
    ) -> JsonDict:
        """Creates a table sink for the source and returns the outputs of the finished creation task."""
        payload: dict[str, Any] = {
            'name': name,
            'type': 'table',
            'table': {
                'type': 'keboola',
                'tableId': table_id,
                'mapping': {
                    'columns': [c.model_dump(by_alias=True, exclude_none=True) for c in columns],
                },
            },
        }
        if allowed_signals:
            payload['allowedSignals'] = allowed_signals
        task = await self.post(endpoint=self._sources_endpoint(f'/{source_id}/sinks'), data=payload)
        return await self.wait_for_task(cast(JsonDict, task))

    async def delete_source(self, source_id: str) -> None:
        task = await self.delete(endpoint=self._sources_endpoint(f'/{source_id}'))
        if task:
            await self.wait_for_task(cast(JsonDict, task))

    async def wait_for_task(self, raw_task: JsonDict) -> JsonDict:
        """Polls an asynchronous Stream API task until it finishes and returns its outputs."""
        task = StreamTask.model_validate(raw_task)
        deadline = time.monotonic() + self.TASK_TIMEOUT_SECONDS
        while not task.is_finished:
            if time.monotonic() > deadline:
                raise StreamTaskError(f'Stream API task "{task.task_id}" did not finish in time.')
            await asyncio.sleep(self.TASK_POLL_INTERVAL_SECONDS)
            task = StreamTask.model_validate(await self.get(endpoint=f'v1/tasks/{task.task_id}'))
        if task.error or task.status == 'error':
            raise StreamTaskError(f'Stream API task "{task.task_id}" failed: {task.error or "unknown error"}')
        return task.outputs
