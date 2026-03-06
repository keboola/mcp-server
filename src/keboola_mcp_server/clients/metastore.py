"""Keboola Metastore API client."""

from __future__ import annotations

from typing import Any

from pydantic import AliasChoices, BaseModel, ConfigDict, Field

from keboola_mcp_server.clients.base import JsonStruct, KeboolaServiceClient, RawKeboolaClient

INTERNAL_META_FIELDS = {
    'uuid',
    'name',
    'revision',
    'revisionCreatedAt',
    'createdAt',
    'updatedAt',
    'deletedAt',
    'lastUpdated',
    'projectId',
    'organizationId',
    'objectType',
    'schemaVersion',
    'branch',
}


class HealthCheckResponse(BaseModel):
    status: str = Field(description='Service health status.')

    @property
    def is_ok(self) -> bool:
        return self.status == 'ok'


class SchemaDocument(BaseModel):
    model_config = ConfigDict(extra='allow')

    title: str | None = Field(default=None, description='Schema title, usually objectType.')
    version: str | None = Field(default=None, description='Schema version.')


class JsonApiResource(BaseModel):
    model_config = ConfigDict(extra='allow')

    type: str = Field(default='', description='Resource type.')
    id: str | None = Field(default=None, description='Resource id.')
    attributes: dict[str, Any] = Field(default_factory=dict, description='Resource attributes.')
    meta: dict[str, Any] = Field(default_factory=dict, description='Resource metadata.')


class JsonApiListEnvelope(BaseModel):
    data: list[JsonApiResource] = Field(default_factory=list)


class JsonApiObjectEnvelope(BaseModel):
    data: JsonApiResource


class MetastoreClient(KeboolaServiceClient):
    """Client for interacting with the Metastore API."""

    def __init__(self, raw_client: RawKeboolaClient) -> None:
        super().__init__(raw_client=raw_client)

    @classmethod
    def create(
        cls,
        root_url: str,
        token: str | None,
        *,
        headers: dict[str, Any] | None = None,
        readonly: bool | None = None,
    ) -> 'MetastoreClient':
        return cls(
            raw_client=RawKeboolaClient(
                base_api_url=root_url,
                api_token=token,
                headers=headers,
                readonly=readonly,
            )
        )

    async def health_check(self) -> HealthCheckResponse:
        response = await self.get(endpoint='health-check')
        if not isinstance(response, dict):
            raise ValueError('Unexpected metastore health-check response format.')
        return HealthCheckResponse.model_validate(response)

    async def get_schema(self, object_type: str, version: str | None = None) -> SchemaDocument:
        endpoint = f'api/v1/schema/{object_type}/{version}' if version else f'api/v1/schema/{object_type}'
        response = await self.get(endpoint=endpoint)
        if not isinstance(response, dict):
            raise ValueError('Unexpected metastore schema response format.')
        body = response.get('schema') if isinstance(response.get('schema'), dict) else response
        return SchemaDocument.model_validate(body)

    async def list_objects(
        self,
        object_type: str,
        *,
        filter_by: str | None = None,
        limit: int | None = None,
        offset: int | None = None,
        simplified: bool | None = None,
        organization_scope: bool = False,
    ) -> list[JsonApiResource]:
        endpoint = (
            f'api/v1/repository/{object_type}/organization'
            if organization_scope
            else f'api/v1/repository/{object_type}'
        )
        params: dict[str, Any] = {}
        if filter_by is not None:
            params['filter'] = filter_by
        if limit is not None:
            params['limit'] = limit
        if offset is not None:
            params['offset'] = offset
        if simplified is not None:
            params['simplified'] = simplified

        response = await self.get(
            endpoint=endpoint,
            params=params or None,
        )
        return self._parse_jsonapi_list(response)

    async def get_object(
        self,
        object_type: str,
        uuid: str,
        *,
        simplified: bool | None = None,
    ) -> JsonApiResource:
        params = {'simplified': simplified} if simplified is not None else None
        response = await self.get(
            endpoint=f'api/v1/repository/{object_type}/{uuid}',
            params=params,
        )
        return self._parse_jsonapi_object(response)

    async def create_object(
        self,
        object_type: str,
        *,
        name: str | None = None,
        data: dict[str, Any],
        schema_version: str | None = None,
        scope: str | None = None,
        branch: str | None = None,
    ) -> JsonApiResource:
        payload: dict[str, Any] = {'data': data}
        if name is not None:
            payload['name'] = name
        if schema_version is not None:
            payload['schemaVersion'] = schema_version
        if scope is not None:
            payload['scope'] = scope
        if branch is not None:
            payload['branch'] = branch

        response = await self.post(endpoint=f'api/v1/repository/{object_type}', data=payload)
        return self._parse_jsonapi_object(response)

    async def patch_object(
        self,
        object_type: str,
        uuid: str,
        *,
        name: str | None = None,
        data: dict[str, Any] | None = None,
    ) -> JsonApiResource:
        payload: dict[str, Any] = {}
        if name is not None:
            payload['name'] = name
        if data is not None:
            payload['data'] = data

        response = await self.patch(endpoint=f'api/v1/repository/{object_type}/{uuid}', data=payload)
        return self._parse_jsonapi_object(response)

    async def put_object(
        self,
        object_type: str,
        uuid: str,
        *,
        name: str,
        data: dict[str, Any],
    ) -> JsonApiResource:
        response = await self.put(
            endpoint=f'api/v1/repository/{object_type}/{uuid}',
            data={'name': name, 'data': data},
        )
        return self._parse_jsonapi_object(response)

    async def delete_object(self, object_type: str, uuid: str) -> JsonStruct | None:
        return await self.delete(endpoint=f'api/v1/repository/{object_type}/{uuid}')

    async def list_revisions(
        self,
        object_type: str,
        *,
        filter_by: str | None = None,
        simplified: bool | None = None,
    ) -> list[JsonApiResource]:
        params: dict[str, Any] = {}
        if filter_by is not None:
            params['filter'] = filter_by
        if simplified is not None:
            params['simplified'] = simplified
        response = await self.get(
            endpoint=f'api/v1/repository/{object_type}/revisions',
            params=params or None,
        )
        return self._parse_jsonapi_list(response)

    async def get_revision(
        self,
        object_type: str,
        uuid: str,
        revision: int,
        *,
        simplified: bool | None = None,
    ) -> JsonApiResource:
        params = {'simplified': simplified} if simplified is not None else None
        response = await self.get(
            endpoint=f'api/v1/repository/{object_type}/{uuid}/revisions/{revision}',
            params=params,
        )
        return self._parse_jsonapi_object(response)

    async def delete_revision(self, object_type: str, uuid: str, revision: int) -> JsonStruct | None:
        return await self.delete(endpoint=f'api/v1/repository/{object_type}/{uuid}/revisions/{revision}')

    @staticmethod
    def _parse_jsonapi_list(response: JsonStruct) -> list[JsonApiResource]:
        if not isinstance(response, dict):
            raise ValueError('Unexpected metastore response format: expected JSON object.')
        envelope = JsonApiListEnvelope.model_validate(response)
        return envelope.data

    @staticmethod
    def _parse_jsonapi_object(response: JsonStruct) -> JsonApiResource:
        if not isinstance(response, dict):
            raise ValueError('Unexpected metastore response format.')
        envelope = JsonApiObjectEnvelope.model_validate(response)
        return envelope.data
