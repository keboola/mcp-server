import logging
import math
from datetime import datetime
from typing import Any, Iterable, Literal, Mapping, Optional, Sequence, cast

from pydantic import AliasChoices, BaseModel, Field, field_validator

from keboola_mcp_server.clients.base import JsonDict, KeboolaServiceClient, RawKeboolaClient

LOG = logging.getLogger(__name__)


ComponentResource = Literal['configuration', 'rows', 'state']
StorageEventType = Literal['info', 'success', 'warn', 'error']

# Project features that can be checked with the is_enabled method
ProjectFeature = Literal['global-search']

ItemType = Literal[
    'flow',
    'bucket',
    'table',
    'transformation',
    'configuration',
    'configuration-row',
    'workspace',
    'shared-code',
    'rows',
    'state',
]


class GlobalSearchResponse(BaseModel):
    """The SAPI global search response."""

    class Item(BaseModel):
        id: str = Field(description='The id of the item.')
        name: str = Field(description='The name of the item.')
        type: ItemType = Field(description='The type of the item.')
        full_path: dict[str, Any] = Field(
            description=(
                'The full path of the item containing project, branch and other information depending on the '
                'type of the item.'
            ),
            alias='fullPath',
        )
        component_id: Optional[str] = Field(
            default=None, description='The id of the component the item belongs to.', alias='componentId'
        )
        organization_id: int = Field(
            description='The id of the organization the item belongs to.', alias='organizationId'
        )
        project_id: int = Field(description='The id of the project the item belongs to.', alias='projectId')
        project_name: str = Field(description='The name of the project the item belongs to.', alias='projectName')
        created: datetime = Field(description='The date and time the item was created in ISO format.')

    all: int = Field(description='Total number of found results.')
    items: list[Item] = Field(description='List of search results of the GlobalSearchType.')
    by_type: dict[str, int] = Field(
        description='Mapping of found types to the number of corresponding results.', alias='byType'
    )
    by_project: dict[str, str] = Field(description='Mapping of project id to project name.', alias='byProject')

    @field_validator('by_type', 'by_project', mode='before')
    @classmethod
    def validate_dict_fields(cls, current_value: Any) -> Any:
        # If the value is empty-list/None, return an empty dictionary, otherwise return the value
        if not current_value:
            return dict()
        return current_value


class APIFlowResponse(BaseModel):
    """
    Raw API response for configuration endpoints.

    Note: will be removed soon due to removal of flow specific client methods.
    """

    # Core identification fields
    configuration_id: str = Field(
        description='The ID of the flow configuration',
        validation_alias=AliasChoices('id', 'configuration_id', 'configurationId', 'configuration-id'),
        serialization_alias='id',
    )
    name: str = Field(description='The name of the flow configuration')
    description: Optional[str] = Field(default=None, description='The description of the flow configuration')

    # Versioning and state
    version: int = Field(description='The version of the flow configuration')
    is_disabled: bool = Field(
        default=False,
        description='Whether the flow configuration is disabled',
        validation_alias=AliasChoices('isDisabled', 'is_disabled', 'is-disabled'),
        serialization_alias='isDisabled',
    )
    is_deleted: bool = Field(
        default=False,
        description='Whether the flow configuration is deleted',
        validation_alias=AliasChoices('isDeleted', 'is_deleted', 'is-deleted'),
        serialization_alias='isDeleted',
    )

    # Flow-specific configuration data (as returned by API)
    configuration: dict[str, Any] = Field(
        description='The nested flow configuration object containing phases and tasks'
    )

    # Change tracking
    change_description: Optional[str] = Field(
        default=None,
        description='The description of the latest changes',
        validation_alias=AliasChoices('changeDescription', 'change_description', 'change-description'),
        serialization_alias='changeDescription',
    )

    # Metadata
    metadata: list[dict[str, Any]] = Field(
        default_factory=list,
        description='Flow configuration metadata',
        validation_alias=AliasChoices('metadata', 'configuration_metadata', 'configurationMetadata'),
    )

    # Timestamps
    created: Optional[str] = Field(None, description='Creation timestamp')
    updated: Optional[str] = Field(None, description='Last update timestamp')


class ComponentAPIResponse(BaseModel):
    """
    Raw component response that can handle both Storage API and AI Service API responses.

    Storage API (/v2/storage/components/{id}) returns just the core fields.
    AI Service API (/docs/components/{id}) returns core fields + optional documentation metadata.

    The optional fields will be None when parsing Storage API responses.
    """

    # Core fields present in both APIs (SAPI and AI service)
    component_id: str = Field(
        description='The ID of the component',
        validation_alias=AliasChoices('component_id', 'id', 'componentId', 'component-id'),
    )
    component_name: str = Field(
        description='The name of the component',
        validation_alias=AliasChoices(
            'name',
            'component_name',
            'componentName',
            'component-name',
        ),
    )
    type: str = Field(
        description='Component type (extractor, writer, application)',
        validation_alias=AliasChoices('type', 'component_type', 'componentType', 'component-type'),
    )
    flags: list[str] = Field(
        default_factory=list,
        description='Developer portal flags',
        validation_alias=AliasChoices('flags', 'component_flags', 'componentFlags', 'component-flags'),
    )
    categories: list[str] = Field(
        default_factory=list,
        description='Component categories',
        validation_alias=AliasChoices(
            'categories',
            'component_categories',
            'componentCategories',
            'component-categories',
        ),
    )

    # Optional metadata fields only present in AI Service API responses
    documentation_url: str | None = Field(
        default=None,
        description='Documentation URL',
        validation_alias=AliasChoices('documentationUrl', 'documentation_url', 'documentation-url'),
    )
    documentation: str | None = Field(
        default=None,
        description='Component documentation',
        validation_alias=AliasChoices('documentation'),
    )
    configuration_schema: dict[str, Any] | None = Field(
        default=None,
        description='Configuration schema',
        validation_alias=AliasChoices('configurationSchema', 'configuration_schema', 'configuration-schema'),
    )
    configuration_row_schema: dict[str, Any] | None = Field(
        default=None,
        description='Configuration row schema',
        validation_alias=AliasChoices('configurationRowSchema', 'configuration_row_schema', 'configuration-row-schema'),
    )


class ConfigurationAPIResponse(BaseModel):
    """
    Raw API response for configuration endpoints.

    Mirrors the actual JSON structure returned by Keboola Storage API for:
    - configuration_detail()
    - configuration_list()
    - configuration_create()
    - configuration_update()
    """

    component_id: str = Field(
        description='The ID of the component',
        validation_alias=AliasChoices('component_id', 'componentId', 'component-id'),
    )
    configuration_id: str = Field(
        description='The ID of the configuration',
        validation_alias=AliasChoices('configuration_id', 'id', 'configurationId', 'configuration-id'),
    )
    name: str = Field(description='The name of the configuration')
    description: Optional[str] = Field(default=None, description='The description of the configuration')
    version: int = Field(description='The version of the configuration')
    is_disabled: bool = Field(
        default=False,
        description='Whether the configuration is disabled',
        validation_alias=AliasChoices('isDisabled', 'is_disabled', 'is-disabled'),
    )
    is_deleted: bool = Field(
        default=False,
        description='Whether the configuration is deleted',
        validation_alias=AliasChoices('isDeleted', 'is_deleted', 'is-deleted'),
    )
    configuration: dict[str, Any] = Field(
        description='The nested configuration object containing parameters and storage'
    )
    rows: Optional[list[dict[str, Any]]] = Field(
        default=None, description='The row configurations within this configuration'
    )
    change_description: Optional[str] = Field(
        default=None,
        description='The description of the latest changes',
        validation_alias=AliasChoices('changeDescription', 'change_description', 'change-description'),
    )
    metadata: list[dict[str, Any]] = Field(
        default_factory=list,
        description='Configuration metadata',
        validation_alias=AliasChoices('metadata', 'configuration_metadata', 'configurationMetadata'),
    )


class CreateConfigurationAPIResponse(BaseModel):
    id: str = Field(description='Unique identifier of the newly created configuration.')
    name: str = Field(description='Human-readable name of the configuration.')
    description: Optional[str] = Field(default='', description='Optional description of the configuration.')
    created: datetime = Field(description='Timestamp when the configuration was created (ISO 8601).')
    creator_token: dict[str, Any] = Field(
        description='Metadata about the token that created the configuration.', alias='creatorToken'
    )
    version: int = Field(description='Version number of the configuration.')
    change_description: Optional[str] = Field(
        description='Optional description of the change that introduced this configuration version.',
        alias='changeDescription',
    )
    is_disabled: bool = Field(
        description='Indicates whether the configuration is currently disabled.', alias='isDisabled'
    )
    is_deleted: bool = Field(
        description='Indicates whether the configuration has been marked as deleted.', alias='isDeleted'
    )
    configuration: Optional[dict[str, Any]] = Field(
        description='User-defined configuration payload (key-value structure).'
    )
    state: Optional[dict[str, Any]] = Field(
        description='Internal runtime state data associated with the configuration.'
    )
    current_version: Optional[dict[str, Any]] = Field(
        description='Metadata about the currently deployed version of the configuration.', alias='currentVersion'
    )


class AsyncStorageClient(KeboolaServiceClient):

    def __init__(self, raw_client: RawKeboolaClient, branch_id: str | None = None) -> None:
        """
        Creates an AsyncStorageClient from a RawKeboolaClient and a branch id.

        :param raw_client: The raw client to use
        :param branch_id: The id of the Keboola project branch to work on
        """
        super().__init__(raw_client=raw_client)
        self._branch_id: str = branch_id or 'default'

    @classmethod
    def create(
        cls,
        *,
        root_url: str,
        token: Optional[str],
        version: str = 'v2',
        branch_id: str | None = None,
        headers: dict[str, Any] | None = None,
    ) -> 'AsyncStorageClient':
        """
        Creates an AsyncStorageClient from a Keboola Storage API token.

        :param root_url: The root URL of the service API
        :param token: The Keboola Storage API token, If None, the client will not send any authorization header.
        :param version: The version of the API to use (default: 'v2')
        :param branch_id: The id of the Keboola project branch to work on
        :param headers: Additional headers for the requests
        :return: A new instance of AsyncStorageClient
        """
        return cls(
            raw_client=RawKeboolaClient(
                base_api_url=f'{root_url}/{version}/storage',
                api_token=token,
                headers=headers,
            ),
            branch_id=branch_id,
        )

    async def branch_metadata_get(self) -> list[JsonDict]:
        """
        Retrieves metadata for the current branch.

        :return: Branch metadata as a list of dictionaries. Each dictionary contains the 'key' and 'value' keys.
        """
        return cast(list[JsonDict], await self.get(endpoint=f'branch/{self._branch_id}/metadata'))

    async def branch_metadata_update(self, metadata: dict[str, Any]) -> list[JsonDict]:
        """
        Updates metadata for the current branch.

        :param metadata: The metadata to update.
        :return: The SAPI call response - updated metadata or raise an error.
        """
        payload = {
            'metadata': [{'key': key, 'value': value} for key, value in metadata.items()],
        }
        return cast(list[JsonDict], await self.post(endpoint=f'branch/{self._branch_id}/metadata', data=payload))

    async def bucket_detail(self, bucket_id: str) -> JsonDict:
        """
        Retrieves information about a given bucket.

        :param bucket_id: The id of the bucket
        :return: Bucket details as dictionary
        """
        return cast(JsonDict, await self.get(endpoint=f'buckets/{bucket_id}'))

    async def bucket_list(self, include: list[str] | None = None) -> list[JsonDict]:
        """
        Lists all buckets.

        :param include: List of fields to include in the response ('metadata' or 'linkedBuckets')
        :return: List of buckets as dictionary
        """
        params = {}
        if include is not None and isinstance(include, list):
            params['include'] = ','.join(include)
        return cast(list[JsonDict], await self.get(endpoint='buckets', params=params))

    async def bucket_metadata_delete(self, bucket_id: str, metadata_id: str) -> None:
        """
        Deletes metadata for a given bucket.

        :param bucket_id: The id of the bucket
        :param metadata_id: The id of the metadata
        """
        await self.delete(endpoint=f'buckets/{bucket_id}/metadata/{metadata_id}')

    async def bucket_metadata_get(self, bucket_id: str) -> list[JsonDict]:
        """
        Retrieves metadata for a given bucket.

        :param bucket_id: The id of the bucket
        :return: Bucket metadata as a list of dictionaries. Each dictionary contains the 'key' and 'value' keys.
        """
        return cast(list[JsonDict], await self.get(endpoint=f'buckets/{bucket_id}/metadata'))

    async def bucket_metadata_update(
        self,
        bucket_id: str,
        metadata: dict[str, Any],
        provider: str = 'user',
    ) -> list[JsonDict]:
        """
        Updates metadata for a given bucket.

        :param bucket_id: The id of the bucket
        :param metadata: The metadata to update.
        :param provider: The provider of the metadata ('user' by default).
        :return: Bucket metadata as a list of dictionaries. Each dictionary contains the 'key' and 'value' keys.
        """
        payload = {
            'provider': provider,
            'metadata': [{'key': key, 'value': value} for key, value in metadata.items()],
        }
        return cast(list[JsonDict], await self.post(endpoint=f'buckets/{bucket_id}/metadata', data=payload))

    async def bucket_table_list(self, bucket_id: str, include: list[str] | None = None) -> list[JsonDict]:
        """
        Lists all tables in a given bucket.

        :param bucket_id: The id of the bucket
        :param include: List of fields to include in the response
        :return: List of tables as dictionary
        """
        params = {}
        if include is not None and isinstance(include, list):
            params['include'] = ','.join(include)
        return cast(list[JsonDict], await self.get(endpoint=f'buckets/{bucket_id}/tables', params=params))

    async def column_metadata_delete(self, column_id: str, metadata_id: str) -> None:
        """
        Deletes metadata for a given column.

        :param column_id: The id of the column
        :param metadata_id: The id of the metadata
        """
        await self.delete(endpoint=f'columns/{column_id}/metadata/{metadata_id}')

    async def column_metadata_get(self, column_id: str) -> list[JsonDict]:
        """
        Retrieves metadata for a given column.

        :param column_id: The id of the column
        :return: Column metadata as a list of dictionaries. Each dictionary contains the 'key' and 'value' keys.
        """
        return cast(list[JsonDict], await self.get(endpoint=f'columns/{column_id}/metadata'))

    async def component_detail(self, component_id: str) -> JsonDict:
        """
        Retrieves information about a given component.

        :param component_id: The id of the component
        :return: Component details as a dictionary
        """
        return cast(JsonDict, await self.get(endpoint=f'branch/{self._branch_id}/components/{component_id}'))

    async def component_list(
        self, component_type: str, include: list[ComponentResource] | None = None
    ) -> list[JsonDict]:
        """
        Lists all components of a given type.

        :param component_type: The type of the component (extractor, writer, application, etc.)
        :param include: Comma separated list of resources to include.
            Available resources: configuration, rows and state.
        :return: List of components as dictionary
        """
        endpoint = f'branch/{self._branch_id}/components'
        params = {'componentType': component_type}
        if include is not None and isinstance(include, list):
            params['include'] = ','.join(include)

        return cast(list[JsonDict], await self.get(endpoint=endpoint, params=params))

    async def configuration_create(
        self,
        component_id: str,
        name: str,
        description: str,
        configuration: dict[str, Any],
    ) -> JsonDict:
        """
        Creates a new configuration for a component.

        :param component_id: The id of the component for which to create the configuration.
        :param name: The name of the configuration.
        :param description: The description of the configuration.
        :param configuration: The configuration definition as a dictionary.

        :return: The SAPI call response - created configuration or raise an error.
        """
        endpoint = f'branch/{self._branch_id}/components/{component_id}/configs'

        payload = {
            'name': name,
            'description': description,
            'configuration': configuration,
        }
        return cast(JsonDict, await self.post(endpoint=endpoint, data=payload))

    async def configuration_delete(self, component_id: str, configuration_id: str, skip_trash: bool = False) -> None:
        """
        Deletes a configuration.

        :param component_id: The id of the component.
        :param configuration_id: The id of the configuration.
        :param skip_trash: If True, the configuration is deleted without moving to the trash.
            (Technically it means the API endpoint is called twice.)
        :raises httpx.HTTPStatusError: If the (component_id, configuration_id) is not found.
        """
        endpoint = f'branch/{self._branch_id}/components/{component_id}/configs/{configuration_id}'
        await self.delete(endpoint=endpoint)
        if skip_trash:
            await self.delete(endpoint=endpoint)

    async def configuration_detail(self, component_id: str, configuration_id: str) -> JsonDict:
        """
        Retrieves information about a given configuration.

        :param component_id: The id of the component.
        :param configuration_id: The id of the configuration.
        :return: The parsed json from the HTTP response.
        :raises ValueError: If the component_id or configuration_id is invalid.
        """
        if not isinstance(component_id, str) or component_id == '':
            raise ValueError(f"Invalid component_id '{component_id}'.")
        if not isinstance(configuration_id, str) or configuration_id == '':
            raise ValueError(f"Invalid configuration_id '{configuration_id}'.")
        endpoint = f'branch/{self._branch_id}/components/{component_id}/configs/{configuration_id}'

        return cast(JsonDict, await self.get(endpoint=endpoint))

    async def configuration_list(self, component_id: str) -> list[JsonDict]:
        """
        Lists configurations of the given component.

        :param component_id: The id of the component.
        :return: List of configurations.
        :raises ValueError: If the component_id is invalid.
        """
        if not isinstance(component_id, str) or component_id == '':
            raise ValueError(f"Invalid component_id '{component_id}'.")
        endpoint = f'branch/{self._branch_id}/components/{component_id}/configs'

        return cast(list[JsonDict], await self.get(endpoint=endpoint))

    async def configuration_metadata_get(self, component_id: str, configuration_id: str) -> list[JsonDict]:
        """
        Retrieves metadata for a given configuration.

        :param component_id: The id of the component.
        :param configuration_id: The id of the configuration.
        :return: Configuration metadata as a list of dictionaries. Each dictionary contains the 'key' and 'value' keys.
        """
        endpoint = f'branch/{self._branch_id}/components/{component_id}/configs/{configuration_id}/metadata'
        return cast(list[JsonDict], await self.get(endpoint=endpoint))

    async def configuration_metadata_update(
        self,
        component_id: str,
        configuration_id: str,
        metadata: dict[str, Any],
    ) -> list[JsonDict]:
        """
        Updates metadata for the given configuration.

        :param component_id: The id of the component.
        :param configuration_id: The id of the configuration.
        :param metadata: The metadata to update.
        :return: Configuration metadata as a list of dictionaries. Each dictionary contains the 'key' and 'value' keys.
        """
        endpoint = f'branch/{self._branch_id}/components/{component_id}/configs/{configuration_id}/metadata'
        payload = {
            'metadata': [{'key': key, 'value': value} for key, value in metadata.items()],
        }
        return cast(list[JsonDict], await self.post(endpoint=endpoint, data=payload))

    async def configuration_update(
        self,
        component_id: str,
        configuration_id: str,
        configuration: dict[str, Any],
        change_description: str,
        updated_name: Optional[str] = None,
        updated_description: Optional[str] = None,
        is_disabled: bool = False,
    ) -> JsonDict:
        """
        Updates a component configuration.

        :param component_id: The id of the component.
        :param configuration_id: The id of the configuration.
        :param configuration: The updated configuration dictionary.
        :param change_description: The description of the modification to the configuration.
        :param updated_name: The updated name of the configuration, if None, the original
            name is preserved.
        :param updated_description: The entire description of the updated configuration, if None, the original
            description is preserved.
        :param is_disabled: Whether the configuration should be disabled.
        :return: The SAPI call response - updated configuration or raise an error.
        """
        endpoint = f'branch/{self._branch_id}/components/{component_id}/configs/{configuration_id}'

        payload = {
            'configuration': configuration,
            'changeDescription': change_description,
        }
        if updated_name:
            payload['name'] = updated_name

        if updated_description:
            payload['description'] = updated_description

        if is_disabled:
            payload['isDisabled'] = is_disabled

        return cast(JsonDict, await self.put(endpoint=endpoint, data=payload))

    async def configuration_row_create(
        self,
        component_id: str,
        config_id: str,
        name: str,
        description: str,
        configuration: dict[str, Any],
    ) -> JsonDict:
        """
        Creates a new row configuration for a component configuration.

        :param component_id: The ID of the component.
        :param config_id: The ID of the configuration.
        :param name: The name of the row configuration.
        :param description: The description of the row configuration.
        :param configuration: The configuration data to create row configuration.
        :return: The SAPI call response - created row configuration or raise an error.
        """
        payload = {
            'name': name,
            'description': description,
            'configuration': configuration,
        }

        return cast(
            JsonDict,
            await self.post(
                endpoint=f'branch/{self._branch_id}/components/{component_id}/configs/{config_id}/rows',
                data=payload,
            ),
        )

    async def configuration_row_update(
        self,
        component_id: str,
        config_id: str,
        configuration_row_id: str,
        configuration: dict[str, Any],
        change_description: str,
        updated_name: Optional[str] = None,
        updated_description: Optional[str] = None,
    ) -> JsonDict:
        """
        Updates a row configuration for a component configuration.

        :param configuration: The configuration data to update row configuration.
        :param component_id: The ID of the component.
        :param config_id: The ID of the configuration.
        :param configuration_row_id: The ID of the row.
        :param change_description: The description of the changes made.
        :param updated_name: The updated name of the configuration, if None, the original
            name is preserved.
        :param updated_description: The updated description of the configuration, if None, the original
            description is preserved.
        :return: The SAPI call response - updated row configuration or raise an error.
        """

        payload = {
            'configuration': configuration,
            'changeDescription': change_description,
        }
        if updated_name:
            payload['name'] = updated_name

        if updated_description:
            payload['description'] = updated_description

        return cast(
            JsonDict,
            await self.put(
                endpoint=f'branch/{self._branch_id}/components/{component_id}/configs/{config_id}'
                f'/rows/{configuration_row_id}',
                data=payload,
            ),
        )

    async def configuration_row_detail(self, component_id: str, config_id: str, configuration_row_id: str) -> JsonDict:
        """
        Retrieves details of a specific configuration row.

        :param component_id: The id of the component.
        :param config_id: The id of the configuration.
        :param configuration_row_id: The id of the configuration row.
        :return: Configuration row details.
        """
        endpoint = f'branch/{self._branch_id}/components/{component_id}/configs/{config_id}/rows/{configuration_row_id}'
        return cast(JsonDict, await self.get(endpoint=endpoint))

    async def configuration_versions(self, component_id: str, config_id: str) -> list[JsonDict]:
        """
        Retrieves details of a specific configuration version.
        """
        endpoint = f'branch/{self._branch_id}/components/{component_id}/configs/{config_id}/versions'
        return cast(list[JsonDict], await self.get(endpoint=endpoint))

    async def configuration_version_latest(self, component_id: str, config_id: str) -> int:
        """
        Retrieves details of the last configuration version.
        """
        versions = await self.configuration_versions(component_id, config_id)
        latest_version = 0
        for data in versions:
            assert isinstance(data, dict)
            assert isinstance(data['version'], int)
            if latest_version is None or data['version'] > latest_version:
                latest_version = data['version']
        return latest_version

    async def job_detail(self, job_id: str | int) -> JsonDict:
        """
        NOTE: To get info for regular jobs, use the Job Queue API.
        Retrieves information about a given job.

        :param job_id: The id of the job
        :return: Job details as dictionary
        """
        return cast(JsonDict, await self.get(endpoint=f'jobs/{job_id}'))  # TODO: no branch support

    async def global_search(
        self,
        query: str,
        limit: int = 100,
        offset: int = 0,
        types: Sequence[ItemType] = tuple(),
    ) -> GlobalSearchResponse:
        """
        Searches for items in the storage. It allows you to search for entities by name across all projects within an
        organization, even those you do not have direct access to. The search is conducted only through entity names to
        ensure confidentiality. We restrict the search to the project and branch production type of the user.

        :param query: The query to search for.
        :param limit: The maximum number of items to return.
        :param offset: The offset to start from, pagination parameter.
        :param types: The types of items to search for.
        """
        params: dict[str, Any] = {
            'query': query,
            'projectIds[]': [await self.project_id()],
            'types[]': types,
            'limit': limit,
            'offset': offset,
        }
        if self._branch_id == 'default':
            params['branchTypes[]'] = 'production'
        else:
            params['branchTypes[]'] = 'development'
            params['branchIds[]'] = self._branch_id
        params = {k: v for k, v in params.items() if v}
        raw_resp = await self.get(endpoint='global-search', params=params)
        return GlobalSearchResponse.model_validate(raw_resp)

    async def table_detail(self, table_id: str) -> JsonDict:
        """
        Retrieves information about a given table.

        :param table_id: The id of the table
        :return: Table details as dictionary
        """
        return cast(JsonDict, await self.get(endpoint=f'tables/{table_id}'))

    async def table_metadata_delete(self, table_id: str, metadata_id: str) -> None:
        """
        Deletes metadata for a given table.

        :param table_id: The id of the table
        :param metadata_id: The id of the metadata
        """
        await self.delete(endpoint=f'tables/{table_id}/metadata/{metadata_id}')

    async def table_metadata_get(self, table_id: str) -> list[JsonDict]:
        """
        Retrieves metadata for a given table.

        :param table_id: The id of the table
        :return: Table metadata as a list of dictionaries. Each dictionary contains the 'key' and 'value' keys.
        """
        return cast(list[JsonDict], await self.get(endpoint=f'tables/{table_id}/metadata'))

    async def table_metadata_update(
        self,
        table_id: str,
        metadata: dict[str, Any] | None = None,
        columns_metadata: dict[str, list[dict[str, Any]]] | None = None,
        provider: str = 'user',
    ) -> JsonDict:
        """
        Updates metadata for a given table. At least one of the `metadata` or `columns_metadata` arguments
        must be provided.

        :param table_id: The id of the table
        :param metadata: The metadata to update.
        :param columns_metadata: The column metadata to update. Mapping of column names to a list of dictionaries.
            Each dictionary contains the 'key' and 'value' keys.
        :param provider: The provider of the metadata ('user' by default).
        :return: Dictionary with 'metadata' key under which the table metadata is stored as a list of dictionaries.
            Each dictionary contains the 'key' and 'value' keys. Under 'columnsMetadata' key, the column metadata
            is stored as a mapping of column names to a list of dictionaries.
        """
        if not metadata and not columns_metadata:
            raise ValueError('At least one of the `metadata` or `columns_metadata` arguments must be provided.')

        payload: dict[str, Any] = {'provider': provider}
        if metadata:
            payload['metadata'] = [{'key': key, 'value': value} for key, value in metadata.items()]
        if columns_metadata:
            payload['columnsMetadata'] = columns_metadata

        return cast(JsonDict, await self.post(endpoint=f'tables/{table_id}/metadata', data=payload))

    # TODO: no branch support
    async def trigger_event(
        self,
        message: str,
        component_id: str,
        configuration_id: str | None = None,
        event_type: StorageEventType | None = None,
        params: Mapping[str, Any] | None = None,
        results: Mapping[str, Any] | None = None,
        duration: float | None = None,
        run_id: str | None = None,
    ) -> JsonDict:
        """
        Sends a Storage API event.

        :param message: The event message.
        :param component_id: The ID of the component triggering the event.
        :param configuration_id: The ID of the component configuration triggering the event.
        :param event_type: The type of event.
        :param params: The component parameters. The structure of the params object must follow the JSON schema
            registered for the component_id.
        :param results: The component results. The structure of the results object must follow the JSON schema
            registered for the component_id.
        :param duration: The component processing duration in seconds.
        :param run_id: The ID of the associated component job.

        :return: Dictionary with the new event ID.
        """
        payload: dict[str, Any] = {
            'message': message,
            'component': component_id,
        }
        if configuration_id:
            payload['configurationId'] = configuration_id
        if event_type:
            payload['type'] = event_type
        if params:
            payload['params'] = params
        if results:
            payload['results'] = results
        if duration is not None:
            # The events API ignores floats, so we round up to the nearest integer.
            payload['duration'] = int(math.ceil(duration))
        if run_id:
            payload['runId'] = run_id

        LOG.info(f'[trigger_event] payload={payload}')

        return cast(JsonDict, await self.post(endpoint='events', data=payload))

    async def workspace_create(
        self,
        login_type: str,
        backend: str,
        async_run: bool = True,
        read_only_storage_access: bool = False,
    ) -> JsonDict:
        """
        Creates a new workspace.

        :param async_run: If True, the workspace creation is run asynchronously.
        :param read_only_storage_access: If True, the workspace has read-only access to the storage.
        :return: The SAPI call response - created workspace or raise an error.
        """
        return cast(
            JsonDict,
            await self.post(
                endpoint=f'branch/{self._branch_id}/workspaces',
                params={'async': async_run},
                data={
                    'readOnlyStorageAccess': read_only_storage_access,
                    'loginType': login_type,
                    'backend': backend,
                },
            ),
        )

    async def workspace_detail(self, workspace_id: int) -> JsonDict:
        """
        Retrieves information about a given workspace.

        :param workspace_id: The id of the workspace
        :return: Workspace details as dictionary
        """
        return cast(JsonDict, await self.get(endpoint=f'branch/{self._branch_id}/workspaces/{workspace_id}'))

    async def workspace_query(self, workspace_id: int, query: str) -> JsonDict:
        """
        Executes a query in a given workspace.

        :param workspace_id: The id of the workspace
        :param query: The query to execute
        :return: The SAPI call response - query result or raise an error.
        """
        return cast(
            JsonDict,
            await self.post(
                endpoint=f'branch/{self._branch_id}/workspaces/{workspace_id}/query',
                data={'query': query},
            ),
        )

    async def workspace_list(self) -> list[JsonDict]:
        """
        Lists all workspaces in the project.

        :return: List of workspaces
        """
        return cast(list[JsonDict], await self.get(endpoint=f'branch/{self._branch_id}/workspaces'))

    async def verify_token(self) -> JsonDict:
        """
        Checks the token privileges and returns information about the project to which the token belongs.

        :return: Token and project information
        """
        return cast(JsonDict, await self.get(endpoint='tokens/verify'))

    async def project_id(self) -> str:
        """
        Retrieves the project id.
        :return: Project id.
        """
        raw_data = cast(JsonDict, await self.get(endpoint='tokens/verify'))
        assert isinstance(raw_data['owner'], dict)
        return str(raw_data['owner']['id'])

    async def is_enabled(self, features: ProjectFeature | Iterable[ProjectFeature]) -> bool:
        """
        Checks if the features are enabled in the project - conjunction of features.
        :param features: The features to check.
        :return: True if the features are enabled, False otherwise.
        """
        features = [features] if isinstance(features, str) else features
        verified_info = await self.verify_token()
        project_data = cast(JsonDict, verified_info['owner'])
        project_features = cast(list[str], project_data.get('features', []))
        return all(feature in project_features for feature in features)

    async def token_create(
        self,
        description: str,
        component_access: list[str] | None = None,
        expires_in: int | None = None,
    ) -> JsonDict:
        """
        Creates a new Storage API token.

        :param description: Description of the token
        :param component_access: List of component IDs the token should have access to
        :param expires_in: Token expiration time in seconds
        :return: Token creation response containing the token and its details
        """
        token_data: dict[str, Any] = {'description': description}

        if component_access:
            token_data['componentAccess'] = component_access

        if expires_in:
            token_data['expiresIn'] = expires_in

        return cast(JsonDict, await self.post(endpoint='tokens', data=token_data))
