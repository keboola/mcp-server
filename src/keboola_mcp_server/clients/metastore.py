"""Keboola Metastore API client."""

from typing import Any

from pydantic import AliasChoices, BaseModel, Field, TypeAdapter

from keboola_mcp_server.clients.base import JsonDict, JsonStruct, KeboolaServiceClient, RawKeboolaClient


class MetaObjectMeta(BaseModel):
    """Metadata from the JSON:API 'meta' field — same structure for all object types."""

    branch: str | None = Field(default=None)
    name: str | None = Field(default=None)
    revision: int | None = Field(default=None)
    schema_version: str | None = Field(
        validation_alias=AliasChoices('schemaVersion', 'schema_version'),
        serialization_alias='schemaVersion',
        default=None,
    )
    project_id: int | None = Field(
        validation_alias=AliasChoices('projectId', 'project_id'),
        serialization_alias='projectId',
        default=None,
    )
    organization_id: str | None = Field(
        validation_alias=AliasChoices('organizationId', 'organization_id'),
        serialization_alias='organizationId',
        default=None,
    )
    created_at: str | None = Field(
        validation_alias=AliasChoices('createdAt', 'created_at'),
        serialization_alias='createdAt',
        default=None,
    )
    last_updated: str | None = Field(
        validation_alias=AliasChoices('lastUpdated', 'last_updated'),
        serialization_alias='lastUpdated',
        default=None,
    )
    deleted_at: str | None = Field(
        validation_alias=AliasChoices('deletedAt', 'deleted_at'),
        serialization_alias='deletedAt',
        default=None,
    )
    revision_created_at: str | None = Field(
        validation_alias=AliasChoices('revisionCreatedAt', 'revision_created_at'),
        serialization_alias='revisionCreatedAt',
        default=None,
    )


class MetastoreObject(BaseModel):
    """Single object from the Metastore JSON:API response."""

    type: str | None = Field(default=None)
    id: str | None = Field(default=None)
    attributes: dict[str, Any] | None = Field(default=None)
    relationships: dict[str, Any] | None = Field(default=None)
    meta: MetaObjectMeta | None = Field(default=None)


LIST_ADAPTER: TypeAdapter[list[MetastoreObject]] = TypeAdapter(list[MetastoreObject])


class MetastoreClient(KeboolaServiceClient):
    """Client for interacting with the Metastore API."""

    def __init__(self, raw_client: RawKeboolaClient, branch_id: str | None = None) -> None:
        super().__init__(raw_client=raw_client)
        self._branch_id: str | None = branch_id

    @classmethod
    def create(
        cls,
        root_url: str,
        token: str | None,
        *,
        branch_id: str | None = None,
        headers: dict[str, Any] | None = None,
        readonly: bool | None = None,
    ) -> 'MetastoreClient':
        client = cls(
            raw_client=RawKeboolaClient(
                base_api_url=root_url,
                api_token=token,
                headers=headers,
                readonly=readonly,
            ),
            branch_id=branch_id,
        )
        return client

    async def get_schema(self, object_type: str, version: str | None = None) -> JsonDict:
        endpoint = f'api/v1/schema/{object_type}/{version}' if version else f'api/v1/schema/{object_type}'
        response = await self.get(endpoint=endpoint)
        if not isinstance(response, dict):
            raise ValueError('Unexpected metastore schema response format.')
        return response

    async def list_objects(
        self,
        object_type: str,
        *,
        filter_by: str | None = None,
        limit: int | None = None,
        offset: int | None = None,
        organization_scope: bool = False,
    ) -> list[MetastoreObject]:
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

        response = await self.get(
            endpoint=endpoint,
            params=params or None,
        )
        return self._parse_list(response)

    async def get_object(
        self,
        object_type: str,
        uuid: str,
    ) -> MetastoreObject:
        response = await self.get(
            endpoint=f'api/v1/repository/{object_type}/{uuid}',
        )
        return self._parse_object(response)

    async def create_object(
        self,
        object_type: str,
        *,
        name: str | None = None,
        data: dict[str, Any],
        schema_version: str | None = None,
        scope: str | None = None,
    ) -> MetastoreObject:
        payload: dict[str, Any] = {'data': data}
        if name is not None:
            payload['name'] = name
        if schema_version is not None:
            payload['schemaVersion'] = schema_version
        if scope is not None:
            payload['scope'] = scope
        if self._branch_id is not None:
            payload['branch'] = self._branch_id

        response = await self.post(endpoint=f'api/v1/repository/{object_type}', data=payload)
        return self._parse_object(response)

    async def patch_object(
        self,
        object_type: str,
        uuid: str,
        *,
        name: str | None = None,
        data: dict[str, Any] | None = None,
    ) -> MetastoreObject:
        payload: dict[str, Any] = {}
        if name is not None:
            payload['name'] = name
        if data is not None:
            payload['data'] = data

        response = await self.patch(endpoint=f'api/v1/repository/{object_type}/{uuid}', data=payload)
        return self._parse_object(response)

    async def put_object(
        self,
        object_type: str,
        uuid: str,
        *,
        name: str,
        data: dict[str, Any],
    ) -> MetastoreObject:
        response = await self.put(
            endpoint=f'api/v1/repository/{object_type}/{uuid}',
            data={'name': name, 'data': data},
        )
        return self._parse_object(response)

    async def delete_object(self, object_type: str, uuid: str) -> JsonStruct | None:
        return await self.delete(endpoint=f'api/v1/repository/{object_type}/{uuid}')

    async def list_revisions(
        self,
        object_type: str,
        *,
        filter_by: str | None = None,
        limit: int | None = None,
        offset: int | None = None,
    ) -> list[MetastoreObject]:
        params: dict[str, Any] = {}
        if filter_by is not None:
            params['filter'] = filter_by
        if limit is not None:
            params['limit'] = limit
        if offset is not None:
            params['offset'] = offset
        response = await self.get(
            endpoint=f'api/v1/repository/{object_type}/revisions',
            params=params or None,
        )
        return self._parse_list(response)

    async def get_revision(
        self,
        object_type: str,
        uuid: str,
        revision: int,
    ) -> MetastoreObject:
        response = await self.get(
            endpoint=f'api/v1/repository/{object_type}/{uuid}/revisions/{revision}',
        )
        return self._parse_object(response)

    async def delete_revision(self, object_type: str, uuid: str, revision: int) -> JsonStruct | None:
        return await self.delete(endpoint=f'api/v1/repository/{object_type}/{uuid}/revisions/{revision}')

    @staticmethod
    def _parse_list(response: JsonStruct) -> list[MetastoreObject]:
        if not isinstance(response, dict):
            raise ValueError('Unexpected metastore response format: expected JSON object with "data" key.')
        data = response.get('data')
        if not isinstance(data, list):
            raise ValueError('Unexpected metastore response format: "data" is not an array.')
        return LIST_ADAPTER.validate_python(data)

    @staticmethod
    def _parse_object(response: JsonStruct) -> MetastoreObject:
        if not isinstance(response, dict):
            raise ValueError('Unexpected metastore response format: expected JSON object.')
        data = response.get('data', response)
        if not isinstance(data, dict):
            raise ValueError('Unexpected metastore response format: "data" is not an object.')
        return MetastoreObject.model_validate(data)
