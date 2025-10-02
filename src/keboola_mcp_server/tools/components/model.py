"""
Domain models for Keboola component and configuration management.

This module contains the business domain models used throughout the MCP server for representing
Keboola components and their configurations. The models are organized into logical groups:

## Component Models
- Component: Full component details with schemas and documentation
- ComponentSummary: Lightweight component info for list operations
- ComponentCapabilities: What a component can do (derived from developer portal flags)

## Configuration Models
The new configuration models provide a structured approach separating shared settings
from individual tasks:

    ### Detail Models (for get operations)
    - Configuration: Complete config with root + rows + component context
    - ConfigurationRoot: Shared settings (credentials, global config)
    - ConfigurationRow: Individual tasks (table mappings, specific parameters)

    ### Summary Models (for list operations)
    - ConfigurationSummary: Lightweight config structure
    - ConfigurationRootSummary: Essential root metadata only
    - ConfigurationRowSummary: Essential row metadata only

## Tool Output Models
- ConfigToolOutput: Standard response for config create/update operations
- ListConfigsOutput: Response for list_configs tool
- ListTransformationsOutput: Response for list_transformations tool

## Legacy Models
- ComponentConfigurationResponseBase: Base class used by Flow tools (FlowConfigurationResponse)
"""

from datetime import datetime
from typing import Annotated, Any, List, Literal, Optional, Union

from pydantic import AliasChoices, BaseModel, Field

from keboola_mcp_server.clients.storage import ComponentAPIResponse, ConfigurationAPIResponse
from keboola_mcp_server.links import Link

# ============================================================================
# TYPE DEFINITIONS
# ============================================================================

ComponentType = Literal['application', 'extractor', 'writer']
TransformationType = Literal['transformation']
AllComponentTypes = Union[ComponentType, TransformationType]


# ============================================================================
# COMPONENT MODELS
# ============================================================================


class ComponentCapabilities(BaseModel):
    """
    Component capabilities derived from developer portal flags.

    Represents what a component can do in terms of data processing:
    - Row-based: Can have multiple configuration rows for different tasks
    - Table I/O: Can read from or write to data tables
    - File I/O: Can read from or write to files
    - OAuth: Requires OAuth authentication setup
    """

    is_row_based: bool = Field(default=False, description='Whether the component supports row configurations')
    has_table_input: bool = Field(default=False, description='Whether the component can read from tables')
    has_table_output: bool = Field(default=False, description='Whether the component can write to tables')
    has_file_input: bool = Field(default=False, description='Whether the component can read from files')
    has_file_output: bool = Field(default=False, description='Whether the component can write to files')
    requires_oauth: bool = Field(default=False, description='Whether the component requires OAuth authorization')

    @classmethod
    def from_flags(cls, flags: list[str]) -> 'ComponentCapabilities':
        """
        Derive component capabilities from developer portal flags.

        :param flags: List of developer portal flags from API response
        :return: Structured component capabilities
        """
        return cls(
            is_row_based='genericDockerUI-rows' in flags,
            has_table_input=any(
                flag in flags for flag in ['genericDockerUI-tableInput', 'genericDockerUI-simpleTableInput']
            ),
            has_table_output='genericDockerUI-tableOutput' in flags,
            has_file_input='genericDockerUI-fileInput' in flags,
            has_file_output='genericDockerUI-fileOutput' in flags,
            requires_oauth='genericDockerUI-authorization' in flags,
        )


class ComponentSummary(BaseModel):
    """Lightweight component representation for list operations."""

    component_id: str = Field(description='Component ID')
    component_name: str = Field(description='Component name')
    component_type: str = Field(description='Component type')
    capabilities: ComponentCapabilities = Field(description='Component capabilities')

    @classmethod
    def from_api_response(cls, api_response: ComponentAPIResponse) -> 'ComponentSummary':
        """
        Create ComponentSummary from API response.

        :param api_response: Parsed API response from Storage or AI Service API
        :return: Lightweight component domain model for list operations
        """
        capabilities = ComponentCapabilities.from_flags(api_response.flags)

        return cls.model_construct(
            component_id=api_response.component_id,
            component_name=api_response.component_name,
            component_type=api_response.type,
            capabilities=capabilities,
        )


class Component(BaseModel):
    """
    Complete component representation with full details.

    Contains comprehensive component information including documentation,
    configuration schemas, and metadata. Used by get tools where detailed
    component information is needed.
    """

    # Core component metadata (shared with ComponentSummary)
    component_id: str = Field(description='Component ID')
    component_name: str = Field(description='Component name')
    component_type: str = Field(description='Component type')
    component_categories: list[str] = Field(
        default_factory=list,
        description='Component categories',
    )
    capabilities: ComponentCapabilities = Field(description='Component capabilities')

    # Additional metadata
    documentation_url: str | None = Field(
        default=None,
        description='URL to component documentation',
    )
    documentation: str | None = Field(
        default=None,
        description='Component documentation text',
    )
    configuration_schema: dict[str, Any] | None = Field(
        default=None,
        description='JSON schema for configuration root validation',
    )
    configuration_row_schema: dict[str, Any] | None = Field(
        default=None,
        description='JSON schema for configuration row validation',
    )

    links: list[Link] = Field(default_factory=list, description='Links for UI navigation')

    @classmethod
    def from_api_response(cls, api_response: ComponentAPIResponse) -> 'Component':
        """
        Create Component from API response.

        :param api_response: Parsed API response from Storage or AI Service API
        :return: Complete component domain model with detailed metadata
        """
        capabilities = ComponentCapabilities.from_flags(api_response.flags)

        return cls.model_construct(
            component_id=api_response.component_id,
            component_name=api_response.component_name,
            component_type=api_response.type,
            component_categories=api_response.categories,
            capabilities=capabilities,
            documentation_url=api_response.documentation_url,
            documentation=api_response.documentation,
            configuration_schema=api_response.configuration_schema,
            configuration_row_schema=api_response.configuration_row_schema,
        )


# ============================================================================
# CONFIGURATION PARAMETER UPDATE MODELS
# ============================================================================


class ConfigParamSet(BaseModel):
    """
    Set or create a parameter value at the specified path.

    Use this operation to:
    - Update an existing parameter value
    - Create a new parameter key
    - Replace a nested parameter value
    """

    op: Literal['set']  # name 'op' inspired by JSON Patch (https://datatracker.ietf.org/doc/html/rfc6902)
    path: str = Field(description='JSONPath to the parameter key to set (e.g., "api_key", "database.host")')
    new_val: Any = Field(description='New value to set')


class ConfigParamReplace(BaseModel):
    """Replace a substring in a string parameter."""

    op: Literal['str_replace']
    path: str = Field(description='JSONPath to the parameter key to modify')
    search_for: str = Field(description='Substring to search for (non-empty)')
    replace_with: str = Field(description='Replacement string (can be empty for deletion)')


class ConfigParamRemove(BaseModel):
    """Remove a parameter key."""

    op: Literal['remove']
    path: str = Field(description='JSONPath to the parameter key to remove')


# Discriminated union of all parameter update operations
ConfigParamUpdate = Annotated[Union[ConfigParamSet, ConfigParamReplace, ConfigParamRemove], Field(discriminator='op')]


# ============================================================================
# CONFIGURATION MODELS
# ============================================================================


class ConfigurationRoot(BaseModel):
    """
    Complete configuration root with all data.

    Represents the shared configuration settings for a component including
    credentials, global parameters, and shared storage mappings. For row-based
    components, this contains the common settings that apply to all rows.
    """

    component_id: str = Field(description='The ID of the component')
    configuration_id: str = Field(description='The ID of this configuration root')
    name: str = Field(description='The name of the configuration')
    description: Optional[str] = Field(default=None, description='The description of the configuration')
    version: int = Field(description='The version of the configuration')
    is_disabled: bool = Field(default=False, description='Whether the configuration is disabled')
    is_deleted: bool = Field(default=False, description='Whether the configuration is deleted')
    parameters: dict[str, Any] = Field(
        description='The configuration parameters, adhering to the configuration root schema'
    )
    storage: Optional[dict[str, Any]] = Field(
        default=None, description='The table and/or file input/output mapping configuration'
    )
    configuration_metadata: list[dict[str, Any]] = Field(
        default_factory=list, description='Configuration metadata including MCP tracking'
    )

    @classmethod
    def from_api_response(cls, api_config: 'ConfigurationAPIResponse') -> 'ConfigurationRoot':
        """
        Create ConfigurationRoot from API response.

        Handles the flattening of nested configuration.parameters and configuration.storage
        from the API response structure into the domain model.

        :param api_config: Validated API configuration response
        :return: Complete configuration root domain model
        """
        return cls.model_construct(
            component_id=api_config.component_id,
            configuration_id=api_config.configuration_id,
            name=api_config.name,
            description=api_config.description,
            version=api_config.version,
            is_disabled=api_config.is_disabled,
            is_deleted=api_config.is_deleted,
            parameters=api_config.configuration.get('parameters', {}),
            storage=api_config.configuration.get('storage'),
            configuration_metadata=api_config.metadata,
        )


class ConfigurationRow(BaseModel):
    """
    Complete configuration row with all data.

    Represents an individual task or extraction within a configuration.
    For row-based components, each row typically handles a specific data source,
    destination, or transformation operation.
    """

    component_id: str = Field(description='The ID of the component')
    configuration_id: str = Field(description='The ID of the corresponding configuration root')
    configuration_row_id: str = Field(description='The ID of this configuration row')
    name: str = Field(description='The name of the configuration row')
    description: Optional[str] = Field(default=None, description='The description of the configuration row')
    version: int = Field(description='The version of the configuration row')
    is_disabled: bool = Field(default=False, description='Whether the configuration row is disabled')
    is_deleted: bool = Field(default=False, description='Whether the configuration row is deleted')
    parameters: dict[str, Any] = Field(
        description='The configuration row parameters, adhering to the configuration row schema'
    )
    storage: Optional[dict[str, Any]] = Field(
        default=None, description='The table and/or file input/output mapping configuration'
    )
    configuration_metadata: list[dict[str, Any]] = Field(default_factory=list, description='Configuration row metadata')

    @classmethod
    def from_api_row_data(
        cls,
        row_data: dict[str, Any],
        component_id: str,
        configuration_id: str,
    ) -> 'ConfigurationRow':
        """
        Create ConfigurationRow from API row data.

        Converts individual row data from the API into a structured domain model.
        Handles the nested structure of configuration row data.

        :param row_data: Raw row data from API response
        :param component_id: ID of the parent component
        :param configuration_id: ID of the parent configuration
        :return: Complete configuration row domain model
        """
        return cls(
            component_id=component_id,
            configuration_id=configuration_id,
            configuration_row_id=row_data['id'],
            name=row_data['name'],
            description=row_data.get('description'),
            version=row_data['version'],
            is_disabled=row_data.get('isDisabled', False),
            is_deleted=row_data.get('isDeleted', False),
            parameters=row_data.get('configuration', {}).get('parameters', {}),
            storage=row_data.get('configuration', {}).get('storage'),
            configuration_metadata=row_data.get('configuration', {}).get('metadata', []),
        )


class ConfigurationRootSummary(BaseModel):
    """Lightweight configuration root for list operations."""

    component_id: str = Field(description='The ID of the component')
    configuration_id: str = Field(description='The ID of this configuration root')
    name: str = Field(description='The name of the configuration')
    description: Optional[str] = Field(default=None, description='The description of the configuration')
    is_disabled: bool = Field(default=False, description='Whether the configuration is disabled')
    is_deleted: bool = Field(default=False, description='Whether the configuration is deleted')

    @classmethod
    def from_api_response(cls, api_config: 'ConfigurationAPIResponse') -> 'ConfigurationRootSummary':
        """Create lightweight configuration root summary from API response."""
        return cls.model_construct(
            component_id=api_config.component_id,
            configuration_id=api_config.configuration_id,
            name=api_config.name,
            description=api_config.description,
            is_disabled=api_config.is_disabled,
            is_deleted=api_config.is_deleted,
        )


class ConfigurationRowSummary(BaseModel):
    """Lightweight configuration row for list operations."""

    component_id: str = Field(description='The ID of the component')
    configuration_id: str = Field(description='The ID of the corresponding configuration root')
    row_configuration_id: str = Field(description='The ID of this configuration row')
    name: str = Field(description='The name of the configuration row')
    description: Optional[str] = Field(default=None, description='The description of the configuration row')
    is_disabled: bool = Field(default=False, description='Whether the configuration row is disabled')
    is_deleted: bool = Field(default=False, description='Whether the configuration row is deleted')

    @classmethod
    def from_api_row_data(
        cls,
        row_data: dict[str, Any],
        component_id: str,
        configuration_id: str,
    ) -> 'ConfigurationRowSummary':
        """Create lightweight configuration row summary from API row data."""
        return cls(
            component_id=component_id,
            configuration_id=configuration_id,
            row_configuration_id=row_data['id'],
            name=row_data['name'],
            description=row_data.get('description'),
            is_disabled=row_data.get('isDisabled', False),
            is_deleted=row_data.get('isDeleted', False),
        )


class ConfigurationSummary(BaseModel):
    """
    Lightweight configuration structure for list operations.

    Container model that mirrors the structure of the full Configuration model
    but with lightweight summary data. Used by list operations where many
    configurations are returned.
    """

    configuration_root: ConfigurationRootSummary = Field(description='The configuration root summary')
    configuration_rows: Optional[list[ConfigurationRowSummary]] = Field(
        default=None, description='The configuration row summaries'
    )

    @classmethod
    def from_api_response(cls, api_config: 'ConfigurationAPIResponse') -> 'ConfigurationSummary':
        """
        Create ConfigurationSummary from API response.

        Builds a lightweight configuration structure by creating summary models
        for both configuration root and configurations row from the API response data.

        :param api_config: Validated API configuration response
        :return: Lightweight configuration structure for list operations
        """
        configuration_root = ConfigurationRootSummary.from_api_response(api_config)

        configuration_rows = None
        if api_config.rows:
            configuration_rows = [
                ConfigurationRowSummary.from_api_row_data(
                    row_data=row,
                    component_id=api_config.component_id,
                    configuration_id=api_config.configuration_id,
                )
                for row in api_config.rows
            ]

        return cls.model_construct(
            configuration_root=configuration_root,
            configuration_rows=configuration_rows,
        )


class Configuration(BaseModel):
    """
    Complete configuration structure for detailed views.

    Container model that holds both configuration root and configuration rows along with
    component context and UI links. Used by get operations where detailed
    configuration information is needed.
    """

    configuration_root: ConfigurationRoot = Field(description='The complete configuration root')
    configuration_rows: Optional[list[ConfigurationRow]] = Field(
        default=None, description='The complete configuration rows'
    )
    component: Optional[ComponentSummary] = Field(
        default=None, description='The component this configuration belongs to'
    )
    links: list[Link] = Field(default_factory=list, description='Navigation links for the web interface')

    @classmethod
    def from_api_response(
        cls,
        api_config: 'ConfigurationAPIResponse',
        component: Optional[ComponentSummary] = None,
        links: Optional[list[Link]] = None,
    ) -> 'Configuration':
        """
        Create Configuration from API response.

        Builds the complete configuration structure including full root and row
        data, along with component context and UI links when provided.

        :param api_config: Validated API configuration response
        :param component: Lightweight component context (optional)
        :param links: UI navigation links (optional)
        :return: Complete configuration model for detailed operations
        """
        configuration_root = ConfigurationRoot.from_api_response(api_config)

        configuration_rows = None
        if api_config.rows:
            configuration_rows = [
                ConfigurationRow.from_api_row_data(
                    row_data=row,
                    component_id=api_config.component_id,
                    configuration_id=api_config.configuration_id,
                )
                for row in api_config.rows
            ]

        return cls.model_construct(
            configuration_root=configuration_root,
            configuration_rows=configuration_rows,
            component=component,
            links=links or [],
        )


# ============================================================================
# TOOL OUTPUT MODELS
# ============================================================================


class ConfigToolOutput(BaseModel):
    """Response model for configuration tool operations."""

    component_id: str = Field(description='The ID of the component.')
    configuration_id: str = Field(description='The ID of the configuration.')
    description: str = Field(description='The description of the configuration.')
    version: int = Field(description='The version number of the configuration.')
    timestamp: datetime = Field(description='The timestamp of the operation.')
    success: bool = Field(default=True, description='Indicates if the operation succeeded.')
    links: list[Link] = Field(description='The links relevant to the configuration.')


class ComponentWithConfigurations(BaseModel):
    """Grouping of a component and its associated configuration summaries."""

    component: ComponentSummary = Field(description='The Keboola component.')
    configurations: List[ConfigurationSummary] = Field(
        description='The list of configuration summaries associated with the component.',
    )


class ListConfigsOutput(BaseModel):
    """Response model for list_configs tool."""

    components_with_configurations: List[ComponentWithConfigurations] = Field(
        description='The groupings of components and their respective configurations.'
    )
    links: List[Link] = Field(
        description='The list of links relevant to the listing of components with configurations.',
    )


class ListTransformationsOutput(BaseModel):
    """Response model for list_transformations tool."""

    components_with_configurations: List[ComponentWithConfigurations] = Field(
        description='The groupings of transformation components and their respective configurations.'
    )
    links: List[Link] = Field(
        description='The list of links relevant to the listing of transformation components with configurations.',
    )


# ============================================================================
# LEGACY MODELS (minimal set for Flow tools compatibility)
# ============================================================================


class ComponentConfigurationResponseBase(BaseModel):
    """
    Legacy base model for component configurations.

    DEPRECATED: Use ConfigurationRootSummary or ConfigurationRowSummary instead.
    Maintained for backward compatibility with existing code.
    """

    component_id: str = Field(
        description='The ID of the component',
        validation_alias=AliasChoices('component_id', 'componentId', 'component-id'),
    )
    configuration_id: str = Field(
        description='The ID of the component configuration',
        validation_alias=AliasChoices(
            'configuration_id',
            'id',
            'configurationId',
            'configuration-id',
        ),
    )
    configuration_name: str = Field(
        description='The name of the component configuration',
        validation_alias=AliasChoices(
            'configuration_name',
            'name',
            'configurationName',
            'configuration-name',
        ),
    )
    configuration_description: Optional[str] = Field(
        description='The description of the component configuration',
        validation_alias=AliasChoices(
            'configuration_description',
            'description',
            'configurationDescription',
            'configuration-description',
        ),
        default=None,
    )
    is_disabled: bool = Field(
        description='Whether the component configuration is disabled',
        validation_alias=AliasChoices('is_disabled', 'isDisabled', 'is-disabled'),
        default=False,
    )
    is_deleted: bool = Field(
        description='Whether the component configuration is deleted',
        validation_alias=AliasChoices('is_deleted', 'isDeleted', 'is-deleted'),
        default=False,
    )
