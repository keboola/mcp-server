"""
Raw API Models - Pure data transfer objects that mirror API responses exactly.

These models represent the raw data returned by Keboola APIs.
They contain no business logic and use the exact field names and structures from the APIs.
"""
from datetime import datetime
from typing import Any, Optional

from pydantic import AliasChoices, BaseModel, Field


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
        default=None,
        description='The row configurations within this configuration'
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
        description='Metadata about the token that created the configuration.',
        alias='creatorToken'
    )
    version: int = Field(description='Version number of the configuration.')
    change_description: Optional[str] = Field(
        description='Optional description of the change that introduced this configuration version.',
        alias='changeDescription'
    )
    is_disabled: bool = Field(
        description='Indicates whether the configuration is currently disabled.',
        alias='isDisabled'
    )
    is_deleted: bool = Field(
        description='Indicates whether the configuration has been marked as deleted.',
        alias='isDeleted'
    )
    configuration: Optional[dict[str, Any]] = Field(
        description='User-defined configuration payload (key-value structure).'
    )
    state: Optional[dict[str, Any]] = Field(
        description='Internal runtime state data associated with the configuration.'
    )
    current_version: Optional[dict[str, Any]] = Field(
        description='Metadata about the currently deployed version of the configuration.',
        alias='currentVersion'
    )
