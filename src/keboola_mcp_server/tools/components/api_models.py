"""
Raw API Models - Pure data transfer objects that mirror API responses exactly.

These models represent the raw data returned by Keboola APIs.
They contain no business logic and use the exact field names and structures from the APIs.
"""

from typing import Any

from pydantic import AliasChoices, BaseModel, Field


class APIComponentResponse(BaseModel):
    """
    Raw component response that can handle both Storage API and AI Service API responses.

    Storage API (/v2/storage/components/{id}) returns just the core fields.
    AI Service API (/docs/components/{id}) returns core fields + optional documentation metadata.

    The optional fields will be None when parsing Storage API responses.
    """

    # Core fields present in both APIs (SAPI and AI service)
    component_id: str = Field(
        description='The ID of the component',
        validation_alias=AliasChoices('id', 'component_id', 'componentId', 'component-id'),
        serialization_alias='componentId',
    )
    component_name: str = Field(
        description='The name of the component',
        validation_alias=AliasChoices(
            'name',
            'component_name',
            'componentName',
            'component-name',
        ),
        serialization_alias='componentName',
    )
    type: str = Field(
        description='Component type (extractor, writer, application)',
        validation_alias=AliasChoices('type', 'component_type', 'componentType', 'component-type'),
        serialization_alias='componentType',
    )
    flags: list[str] = Field(
        default_factory=list,
        description='Developer portal flags',
        validation_alias=AliasChoices('flags', 'component_flags', 'componentFlags', 'component-flags'),
        serialization_alias='componentFlags',
    )
    categories: list[str] = Field(
        default_factory=list,
        description='Component categories',
        validation_alias=AliasChoices('categories', 'component_categories', 'componentCategories', 'component-categories'),
        serialization_alias='componentCategories',
    )

    # Optional metadata fields only present in AI Service API responses
    documentation_url: str | None = Field(
        default=None,
        description='Documentation URL',
        validation_alias=AliasChoices('documentationUrl', 'documentation_url', 'documentation-url'),
        serialization_alias='documentationUrl',
    )
    documentation: str | None = Field(
        default=None,
        description='Component documentation',
        validation_alias=AliasChoices('documentation'),
        serialization_alias='documentation',
    )
    configuration_schema: dict[str, Any] | None = Field(
        default=None,
        description='Configuration schema',
        validation_alias=AliasChoices('configurationSchema', 'configuration_schema', 'configuration-schema'),
        serialization_alias='configurationSchema',
    )
    configuration_row_schema: dict[str, Any] | None = Field(
        default=None,
        description='Configuration row schema',
        validation_alias=AliasChoices('configurationRowSchema', 'configuration_row_schema', 'configuration-row-schema'),
        serialization_alias='configurationRowSchema',
    )
