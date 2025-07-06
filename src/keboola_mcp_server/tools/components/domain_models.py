"""
Domain Models - Clean business objects representing Keboola concepts.

These models represent business entities with clear responsibilities and no API concerns.
"""

from typing import Any

from pydantic import BaseModel, Field
from pydantic.aliases import AliasChoices

from keboola_mcp_server.links import Link


class ComponentCapabilities(BaseModel):
    """
    Component capabilities derived from developer portal flags.

    Represents what a component can do in terms of data processing.
    """

    is_row_based: bool = Field(
        default=False,
        description='Whether the component supports configuration rows',
        validation_alias=AliasChoices('is_row_based', 'isRowBased', 'is-row-based'),
        serialization_alias='isRowBased',
    )
    has_table_input: bool = Field(
        default=False,
        description='Whether the component can read from tables',
        validation_alias=AliasChoices('has_table_input_mapping', 'hasTableInputMapping', 'has-table-input-mapping'),
        serialization_alias='hasTableInputMapping',
    )
    has_table_output: bool = Field(
        default=False,
        description='Whether the component can write to tables',
        validation_alias=AliasChoices('has_table_output_mapping', 'hasTableOutputMapping', 'has-table-output-mapping'),
        serialization_alias='hasTableOutputMapping',
    )
    has_file_input: bool = Field(
        default=False,
        description='Whether the component can read from files',
        validation_alias=AliasChoices('has_file_input_mapping', 'hasFileInputMapping', 'has-file-input-mapping'),
        serialization_alias='hasFileInputMapping',
    )
    has_file_output: bool = Field(
        default=False,
        description='Whether the component can write to files',
        validation_alias=AliasChoices('has_file_output_mapping', 'hasFileOutputMapping', 'has-file-output-mapping'),
        serialization_alias='hasFileOutputMapping',
    )
    requires_oauth: bool = Field(
        default=False,
        description='Whether the component requires OAuth authorization',
        validation_alias=AliasChoices('has_oauth', 'hasOauth', 'has-oauth'),
        serialization_alias='hasOauth',
    )


class ComponentSummary(BaseModel):
    """
    Business representation of a Keboola component summary.

    Contains essential information for list views and lightweight operations.
    Used by list tools where many components are returned.
    """

    # Core business data with proper aliases
    component_id: str = Field(
        description='Component ID',
        validation_alias=AliasChoices('id', 'component_id', 'componentId', 'component-id'),
        serialization_alias='componentId',
    )
    component_name: str = Field(
        description='Component name',
        validation_alias=AliasChoices('name', 'component_name', 'componentName', 'component-name'),
        serialization_alias='componentName',
    )
    component_type: str = Field(
        description='Component type',
        validation_alias=AliasChoices('type', 'component_type', 'componentType', 'component-type'),
        serialization_alias='componentType',
    )
    capabilities: ComponentCapabilities = Field(description='Component capabilities')


class Component(BaseModel):
    """
    Business representation of a detailed Keboola component.

    Contains comprehensive information including documentation and schemas.
    Used by get tools where a single component's full details are needed.
    """

    # Core business data with proper aliases (inherits from summary conceptually)
    component_id: str = Field(
        description='Component ID',
        validation_alias=AliasChoices('id', 'component_id', 'componentId', 'component-id'),
        serialization_alias='componentId',
    )
    component_name: str = Field(
        description='Component name',
        validation_alias=AliasChoices('name', 'component_name', 'componentName', 'component-name'),
        serialization_alias='componentName',
    )
    component_type: str = Field(
        description='Component type',
        validation_alias=AliasChoices('type', 'component_type', 'componentType', 'component-type'),
        serialization_alias='componentType',
    )
    component_categories: list[str] = Field(
        default_factory=list,
        description='Component categories',
        validation_alias=AliasChoices('componentCategories',
                                      'component_categories',
                                      'component-categories',
                                      'categories'),
        serialization_alias='categories',
    )
    capabilities: ComponentCapabilities = Field(description='Component capabilities')

    # Detailed metadata (only in full detail view)
    documentation_url: str | None = Field(
        default=None,
        description='URL to component documentation',
        validation_alias=AliasChoices('documentationUrl', 'documentation_url', 'documentation-url'),
        serialization_alias='documentationUrl',
    )
    documentation: str | None = Field(
        default=None,
        description='Component documentation text',
        serialization_alias='documentation',
    )
    configuration_schema: dict[str, Any] | None = Field(
        default=None,
        description='JSON schema for configuration',
        validation_alias=AliasChoices('configurationSchema', 'configuration_schema', 'configuration-schema'),
        serialization_alias='configurationSchema',
    )
    configuration_row_schema: dict[str, Any] | None = Field(
        default=None,
        description='JSON schema for configuration rows',
        validation_alias=AliasChoices('configurationRowSchema', 'configuration_row_schema', 'configuration-row-schema'),
        serialization_alias='configurationRowSchema',
    )

    # Optional MCP-specific metadata (populated when needed)
    links: list[Link] = Field(default_factory=list, description='MCP-specific links for UI navigation')
