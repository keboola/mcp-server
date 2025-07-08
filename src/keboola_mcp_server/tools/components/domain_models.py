from datetime import datetime
from typing import Any, List, Literal, Optional, Union

from pydantic import AliasChoices, BaseModel, Field

from keboola_mcp_server.links import Link
from keboola_mcp_server.tools.components.api_models import APIComponentResponse, APIConfigurationResponse

ComponentType = Literal['application', 'extractor', 'writer']
TransformationType = Literal['transformation']
AllComponentTypes = Union[ComponentType, TransformationType]


class ConfigToolOutput(BaseModel):
    component_id: str = Field(description='The ID of the component.')
    configuration_id: str = Field(description='The ID of the configuration.')
    description: str = Field(description='The description of the configuration.')
    timestamp: datetime = Field(description='The timestamp of the operation.')
    # success is always true unless the tool fails - to inform agent and prevent need to fetch objects
    success: bool = Field(default=True, description='Indicates if the operation succeeded.')
    links: list[Link] = Field(description='The links relevant to the configuration.')


class ComponentConfigurationResponseBase(BaseModel):
    """
    A Reduced Component Configuration containing the Keboola Component ID and the reduced information about
    configuration used in a list.
    """

    component_id: str = Field(
        description='The ID of the component',
        validation_alias=AliasChoices('component_id', 'componentId', 'component-id'),
        serialization_alias='componentId',
    )
    configuration_id: str = Field(
        description='The ID of the component configuration',
        validation_alias=AliasChoices(
            'id',
            'configuration_id',
            'configurationId',
            'configuration-id',
        ),
        serialization_alias='configurationId',
    )
    configuration_name: str = Field(
        description='The name of the component configuration',
        validation_alias=AliasChoices(
            'name',
            'configuration_name',
            'configurationName',
            'configuration-name',
        ),
        serialization_alias='configurationName',
    )
    configuration_description: Optional[str] = Field(
        description='The description of the component configuration',
        validation_alias=AliasChoices(
            'description',
            'configuration_description',
            'configurationDescription',
            'configuration-description',
        ),
        serialization_alias='configurationDescription',
        default=None,
    )
    is_disabled: bool = Field(
        description='Whether the component configuration is disabled',
        validation_alias=AliasChoices('isDisabled', 'is_disabled', 'is-disabled'),
        serialization_alias='isDisabled',
        default=False,
    )
    is_deleted: bool = Field(
        description='Whether the component configuration is deleted',
        validation_alias=AliasChoices('isDeleted', 'is_deleted', 'is-deleted'),
        serialization_alias='isDeleted',
        default=False,
    )


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

    @classmethod
    def from_flags(cls, flags: list[str]) -> 'ComponentCapabilities':
        """
        Derive component capabilities from developer portal flags.

        :param flags: List of developer portal flags
        :return: Component capabilities
        """
        return cls(
            is_row_based='genericDockerUI-rows' in flags,
            has_table_input=any(flag in flags for flag in [
                'genericDockerUI-tableInput',
                'genericDockerUI-simpleTableInput'
            ]),
            has_table_output='genericDockerUI-tableOutput' in flags,
            has_file_input='genericDockerUI-fileInput' in flags,
            has_file_output='genericDockerUI-fileOutput' in flags,
            requires_oauth='genericDockerUI-authorization' in flags,
        )


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

    @classmethod
    def from_api_response(cls, api_response: APIComponentResponse) -> 'Component':
        """
        Create Component from API response.

        :param api_response: Parsed API response (works for both Storage API and AI Service API)
        :return: Full component domain model with detailed metadata
        """
        capabilities = ComponentCapabilities.from_flags(api_response.flags)

        return cls(
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

    @classmethod
    def from_api_response(cls, api_response: APIComponentResponse) -> 'ComponentSummary':
        """
        Create ComponentSummary from API response.

        :param api_response: Parsed API response (works for both Storage API and AI Service API)
        :return: Lightweight component domain model for list operations
        """
        capabilities = ComponentCapabilities.from_flags(api_response.flags)

        return cls(
            component_id=api_response.component_id,
            component_name=api_response.component_name,
            component_type=api_response.type,
            capabilities=capabilities,
        )


class ComponentConfigurationResponse(ComponentConfigurationResponseBase):
    """
    Detailed information about a Keboola Component Configuration, containing all the relevant details.
    """

    version: int = Field(description='The version of the component configuration')
    configuration: dict[str, Any] = Field(description='The configuration of the component')
    rows: Optional[list[dict[str, Any]]] = Field(description='The rows of the component configuration', default=None)
    change_description: Optional[str] = Field(
        description='The description of the changes made to the component configuration',
        default=None,
        validation_alias=AliasChoices('changeDescription', 'change_description', 'change-description'),
    )
    configuration_metadata: list[dict[str, Any]] = Field(
        description='The metadata of the component configuration',
        default_factory=list,
        validation_alias=AliasChoices(
            'metadata', 'configuration_metadata', 'configurationMetadata', 'configuration-metadata'
        ),
        serialization_alias='configurationMetadata',
    )
    component: Optional[Component] = Field(
        description='The component this configuration belongs to',
        default=None,
    )


class ComponentRowConfiguration(ComponentConfigurationResponseBase):
    """
    Detailed information about a Keboola Component Row Configuration.
    """

    version: int = Field(description='The version of the component configuration')
    storage: Optional[dict[str, Any]] = Field(
        description='The table and/or file input / output mapping of the component configuration. '
        'It is present only for components that are not row-based and have tables or '
        'file input mapping defined.',
        default=None,
    )
    parameters: dict[str, Any] = Field(
        description='The user parameters, adhering to the row configuration schema',
    )
    configuration_metadata: list[dict[str, Any]] = Field(
        description='The metadata of the component configuration',
        default_factory=list,
        validation_alias=AliasChoices(
            'metadata', 'configuration_metadata', 'configurationMetadata', 'configuration-metadata'
        ),
        serialization_alias='configurationMetadata',
    )


class ComponentRootConfiguration(ComponentConfigurationResponseBase):
    """
    Detailed information about a Keboola Component Root Configuration.
    """

    version: int = Field(description='The version of the component configuration')
    storage: Optional[dict[str, Any]] = Field(
        description='The table and/or file input / output mapping of the component configuration. '
        'It is present only for components that are not row-based and have tables or '
        'file input mapping defined',
        default=None,
    )
    parameters: dict[str, Any] = Field(
        description='The component configuration parameters, adhering to the root configuration schema',
    )
    configuration_metadata: list[dict[str, Any]] = Field(
        description='The metadata of the component configuration',
        default_factory=list,
        validation_alias=AliasChoices(
            'metadata', 'configuration_metadata', 'configurationMetadata', 'configuration-metadata'
        ),
        serialization_alias='configurationMetadata',
    )


class ComponentConfigurationOutput(BaseModel):
    """
    The MCP tools' output model for component configuration, containing the root configuration and optional
    row configurations.
    """

    root_configuration: ComponentRootConfiguration = Field(
        description='The root configuration of the component configuration'
    )
    row_configurations: Optional[list[ComponentRowConfiguration]] = Field(
        description='The row configurations of the component configuration',
        default=None,
    )
    component: Optional[Component] = Field(
        description='The component this configuration belongs to',
        default=None,
    )
    links: list[Link] = Field(..., description='The links relevant to the component configuration.')


class ComponentConfigurationMetadata(BaseModel):
    """
    Metadata model for component configuration, containing the root configuration metadata and optional
    row configurations metadata.
    """

    root_configuration: ComponentConfigurationResponseBase = Field(
        description='The root configuration metadata of the component configuration'
    )
    row_configurations: Optional[list[ComponentConfigurationResponseBase]] = Field(
        description='The row configurations metadata of the component configuration',
        default=None,
    )

    @classmethod
    def from_component_configuration_response(
        cls, configuration: ComponentConfigurationResponse
    ) -> 'ComponentConfigurationMetadata':
        """
        Create a ComponentConfigurationMetadata instance from a ComponentConfigurationResponse instance.
        """
        root_configuration = ComponentConfigurationResponseBase.model_validate(configuration.model_dump())
        row_configurations = None
        if configuration.rows:
            component_id = root_configuration.component_id
            row_configurations = [
                ComponentConfigurationResponseBase.model_validate(row | {'component_id': component_id})
                for row in configuration.rows
                if row is not None
            ]
        return cls(root_configuration=root_configuration, row_configurations=row_configurations)


class ComponentWithConfigurations(BaseModel):
    """
    Grouping of a Keboola Component and its associated configurations metadata.
    """

    component: ComponentSummary = Field(description='The Keboola component.')
    configurations: List[ComponentConfigurationMetadata] = Field(
        description='The list of configurations metadata associated with the component.',
    )


class ListConfigsOutput(BaseModel):
    """Output of list_configs tool."""

    components_with_configurations: List[ComponentWithConfigurations] = Field(
        description='The groupings of components and their respective configurations.')
    links: List[Link] = Field(
        description='The list of links relevant to the listing of components with configurations.',
    )


class ListTransformationsOutput(BaseModel):
    """Output of list_transformations tool."""

    components_with_configurations: List[ComponentWithConfigurations] = Field(
        description='The groupings of transformation components and their respective configurations.')
    links: List[Link] = Field(
        description='The list of links relevant to the listing of transformation components with configurations.',
    )


# ============================================================================
# NEW CONFIGURATION MODELS (Phase 1 - Non-breaking additions)
# ============================================================================


class ConfigurationRoot(BaseModel):
    """
    Domain model for root configuration settings.

    Represents the main configuration parameters and storage mappings.
    Contains identical fields to ConfigurationRow - semantic difference only.
    """

    # Core identification
    component_id: str = Field(description='The ID of the component')
    configuration_id: str = Field(description='The ID of the configuration')
    name: str = Field(description='The name of the configuration')
    description: Optional[str] = Field(default=None, description='The description of the configuration')

    # Versioning and state
    version: int = Field(description='The version of the configuration')
    is_disabled: bool = Field(default=False, description='Whether the configuration is disabled')
    is_deleted: bool = Field(default=False, description='Whether the configuration is deleted')

    # Configuration content
    parameters: dict[str, Any] = Field(
        description='The configuration parameters, adhering to the root configuration schema'
    )
    storage: Optional[dict[str, Any]] = Field(
        default=None,
        description='The table and/or file input/output mapping configuration'
    )

    # Metadata
    configuration_metadata: list[dict[str, Any]] = Field(
        default_factory=list,
        description='Configuration metadata'
    )

    @classmethod
    def from_api_response(cls, api_config: 'APIConfigurationResponse') -> 'ConfigurationRoot':
        """
        Create ConfigurationRoot from API response.

        Handles the flattening of nested configuration.parameters and configuration.storage.
        """
        return cls(
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
    Domain model for individual row configuration.

    Represents a specific task/extraction within a configuration.
    Contains identical fields to ConfigurationRoot - semantic difference only.
    """

    # Core identification
    component_id: str = Field(description='The ID of the component')
    configuration_id: str = Field(description='The ID of the parent configuration')
    row_id: str = Field(description='The ID of this row configuration')
    name: str = Field(description='The name of the row configuration')
    description: Optional[str] = Field(default=None, description='The description of the row configuration')

    # Versioning and state
    version: int = Field(description='The version of the row configuration')
    is_disabled: bool = Field(default=False, description='Whether the row configuration is disabled')
    is_deleted: bool = Field(default=False, description='Whether the row configuration is deleted')

    # Configuration content
    parameters: dict[str, Any] = Field(
        description='The row configuration parameters, adhering to the row configuration schema'
    )
    storage: Optional[dict[str, Any]] = Field(
        default=None,
        description='The table and/or file input/output mapping configuration'
    )

    # Metadata
    configuration_metadata: list[dict[str, Any]] = Field(
        default_factory=list,
        description='Row configuration metadata'
    )

    @classmethod
    def from_api_row_data(
        cls,
        row_data: dict[str, Any],
        component_id: str,
        configuration_id: str,
    ) -> 'ConfigurationRow':
        """
        Create ConfigurationRow from API row data.

        Row data comes from the 'rows' array in the main configuration response.
        """
        return cls(
            component_id=component_id,
            configuration_id=configuration_id,
            row_id=row_data.get('id', ''),
            name=row_data.get('name', ''),
            description=row_data.get('description'),
            version=row_data.get('version', 0),
            is_disabled=row_data.get('isDisabled', False),
            is_deleted=row_data.get('isDeleted', False),
            parameters=row_data.get('configuration', {}).get('parameters', {}),
            storage=row_data.get('configuration', {}).get('storage'),
            configuration_metadata=row_data.get('metadata', []),
        )


class ConfigurationSummary(BaseModel):
    """
    Lightweight domain model for configuration listings.

    Contains only essential metadata without heavyweight configuration data.
    Used by list operations and groupings where many configurations are returned.
    """

    # Core identification
    component_id: str = Field(description='The ID of the component')
    configuration_id: str = Field(description='The ID of the configuration')
    name: str = Field(description='The name of the configuration')
    description: Optional[str] = Field(default=None, description='The description of the configuration')

    # State information
    is_disabled: bool = Field(default=False, description='Whether the configuration is disabled')
    is_deleted: bool = Field(default=False, description='Whether the configuration is deleted')

    @classmethod
    def from_api_response(cls, api_config: 'APIConfigurationResponse') -> 'ConfigurationSummary':
        """Create ConfigurationSummary from API response."""
        return cls(
            component_id=api_config.component_id,
            configuration_id=api_config.configuration_id,
            name=api_config.name,
            description=api_config.description,
            is_disabled=api_config.is_disabled,
            is_deleted=api_config.is_deleted,
        )


class Configuration(BaseModel):
    """
    Full domain model for detailed configuration views.

    Contains complete configuration data including component summary and UI links.
    Used by get operations where detailed configuration information is needed.
    """

    # Configuration structure
    root_configuration: ConfigurationRoot = Field(
        description='The root configuration of this configuration'
    )
    row_configurations: Optional[list[ConfigurationRow]] = Field(
        default=None,
        description='The row configurations within this configuration'
    )

    # Additional context
    component: Optional[ComponentSummary] = Field(
        default=None,
        description='The component this configuration belongs to'
    )
    links: list[Link] = Field(
        default_factory=list,
        description='MCP-specific links for UI navigation'
    )

    @classmethod
    def from_api_response(
        cls,
        api_config: 'APIConfigurationResponse',
        component: Optional[ComponentSummary] = None,
        links: Optional[list[Link]] = None,
    ) -> 'Configuration':
        """
        Create Configuration from API response.

        Converts the API response into a full domain model with root and row configurations.
        """
        # Create root configuration
        root_config = ConfigurationRoot.from_api_response(api_config)

        # Create row configurations if they exist
        row_configs = None
        if api_config.rows:
            row_configs = []
            for row_data in api_config.rows:
                if row_data is None:
                    continue
                row_config = ConfigurationRow.from_api_row_data(
                    row_data=row_data,
                    component_id=api_config.component_id,
                    configuration_id=api_config.configuration_id,
                )
                row_configs.append(row_config)

        return cls(
            component_id=api_config.component_id,
            configuration_id=api_config.configuration_id,
            name=api_config.name,
            description=api_config.description,
            is_disabled=api_config.is_disabled,
            is_deleted=api_config.is_deleted,
            root_configuration=root_config,
            row_configurations=row_configs,
            component=component,
            links=links or [],
        )
