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

## Legacy Models
- ComponentConfigurationResponseBase: Base class used by Flow tools (FlowConfigurationResponse)
"""

import asyncio
from datetime import datetime
from typing import Annotated, Any, List, Literal, Optional, Sequence, Union, get_args

from pydantic import AliasChoices, BaseModel, Field, model_validator

from keboola_mcp_server.clients.storage import ComponentAPIResponse, ConfigurationAPIResponse
from keboola_mcp_server.links import Link

# ============================================================================
# TYPE DEFINITIONS
# ============================================================================

ComponentType = Literal['application', 'extractor', 'transformation', 'writer']
ALL_COMPONENT_TYPES = tuple(component_type for component_type in get_args(ComponentType))


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
    processors: Optional[dict[str, Any]] = Field(
        default=None, description='The processors that run before or after the configured component.'
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
            processors=api_config.configuration.get('processors'),
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
    processors: Optional[dict[str, Any]] = Field(
        default=None, description='The processors that run before or after the configured component row.'
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
            processors=row_data.get('configuration', {}).get('processors'),
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
    value: Any = Field(description='New value to set')


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


class ConfigParamListAppend(BaseModel):
    """Append a value to a list parameter."""

    op: Literal['list_append']
    path: str = Field(description='JSONPath to the list parameter')
    value: Any = Field(description='Value to append to the list')


# Discriminated union of all parameter update operations
ConfigParamUpdate = Annotated[
    Union[ConfigParamSet, ConfigParamReplace, ConfigParamRemove, ConfigParamListAppend], Field(discriminator='op')
]


# ============================================================================
# TRANSFORMATION MODELS
# ============================================================================


class TransformationConfiguration(BaseModel):
    """
    Creates the transformation configuration, a schema for the transformation configuration in the API.
    Currently, the storage configuration uses only input and output tables, excluding files, etc.
    """

    class Parameters(BaseModel):
        """The parameters for the transformation."""

        class Block(BaseModel):
            """The transformation block."""

            class Code(BaseModel):
                """The code block for the transformation block."""

                name: str = Field(description='The name of the current code block describing the purpose of the block')
                script: Sequence[str] = Field(
                    description=(
                        'The executable SQL query statements written in the current SQL dialect. '
                        'Each statement must be executable and a separate item in the list.'
                    ),
                )

            name: str = Field(description='The name of the current block')
            codes: list[Code] = Field(description='The code scripts')

        blocks: list[Block] = Field(description='The blocks for the transformation')

        async def to_simplified_parameters(self) -> 'SimplifiedTfBlocks':
            # Avoid circular import
            from keboola_mcp_server.tools.components.sql_utils import join_sql_statements

            """Convert the raw parameters to simplified parameters."""
            return SimplifiedTfBlocks(
                blocks=[
                    SimplifiedTfBlocks.Block(
                        name=block.name,
                        codes=[
                            SimplifiedTfBlocks.Block.Code(name=code.name, script=join_sql_statements(code.script))
                            for code in block.codes
                        ],
                    )
                    for block in self.blocks
                ]
            )

    class Storage(BaseModel):
        """The storage configuration for the transformation. For now it stores only input and output tables."""

        class Destination(BaseModel):
            """Tables' destinations for the transformation. Either input or output tables."""

            class Table(BaseModel):
                """The table used in the transformation"""

                destination: Optional[str] = Field(description='The destination table name', default=None)
                source: Optional[str] = Field(description='The source table name', default=None)

            tables: list[Table] = Field(description='The tables used in the transformation', default_factory=list)

        input: Destination = Field(description='The input tables for the transformation', default_factory=Destination)
        output: Destination = Field(description='The output tables for the transformation', default_factory=Destination)

    parameters: Parameters = Field(description='The parameters for the transformation')
    storage: Storage = Field(description='The storage configuration for the transformation')


# Type alias for TransformationConfiguration.Parameters for convenience
TransformationBlocks = TransformationConfiguration.Parameters


class SimplifiedTfBlocks(BaseModel):
    """
    Transformations parameters blocks simplified for the agent use:
    `script` is a string instead of a list of statements
    """

    class Block(BaseModel):
        """The transformation block."""

        class Code(BaseModel):
            """The code block for the transformation block."""

            name: str = Field(description='A descriptive name for the code block')
            script: str = Field(description='The SQL script of the code block')

            async def to_raw_code(self) -> TransformationConfiguration.Parameters.Block.Code:
                # Avoid circular import
                from keboola_mcp_server.tools.components.sql_utils import split_sql_statements

                """Convert the simplified code to the raw code."""
                return TransformationConfiguration.Parameters.Block.Code(
                    name=self.name, script=await split_sql_statements(self.script)
                )

        name: str = Field(description='A descriptive name for the code block')
        codes: list[Code] = Field(description='SQL code sub-blocks')

    blocks: list[Block] = Field(description='SQL code blocks')

    async def to_raw_parameters(self) -> TransformationConfiguration.Parameters:
        """Convert the simplified transformation parameters to raw (SAPI) parameters."""
        return TransformationConfiguration.Parameters(
            blocks=[
                TransformationConfiguration.Parameters.Block(
                    name=block.name, codes=await asyncio.gather(*[code.to_raw_code() for code in block.codes])
                )
                for block in self.blocks
            ]
        )


# ============================================================================
# TRANSFORMATION PARAMETER UPDATE MODELS
# ============================================================================


TfPosition = Literal['start', 'end']


class TfAddBlock(BaseModel):
    """Add a new block to the transformation."""

    op: Literal['add_block']
    block: SimplifiedTfBlocks.Block = Field(description='The block to add')
    position: TfPosition = Field(description='The position of the block to add', default='end')


class TfRemoveBlock(BaseModel):
    """Remove an existing block from the transformation."""

    op: Literal['remove_block']
    block_id: str = Field(description='The ID of the block to remove')


class TfRenameBlock(BaseModel):
    """Rename an existing block in the transformation."""

    op: Literal['rename_block']
    block_id: str = Field(description='The ID of the block to rename')
    block_name: str = Field(description='The new name of the block')


class TfAddCode(BaseModel):
    """Add a new code to an existing block in the transformation."""

    op: Literal['add_code']
    block_id: str = Field(description='The ID of the block to add the code to')
    code: SimplifiedTfBlocks.Block.Code = Field(description='The code to add')
    position: TfPosition = Field(description='The position of the code to add', default='end')


class TfRemoveCode(BaseModel):
    """Remove an existing code from an existing block in the transformation."""

    op: Literal['remove_code']
    block_id: str = Field(description='The ID of the block to remove the code from')
    code_id: str = Field(description='The ID of the code to remove')


class TfRenameCode(BaseModel):
    """Rename an existing code in an existing block in the transformation."""

    op: Literal['rename_code']
    block_id: str = Field(description='The ID of the block to rename the code in')
    code_id: str = Field(description='The ID of the code to rename')
    code_name: str = Field(description='The new name of the code')


class TfSetCode(BaseModel):
    """Set the SQL script of an existing code in an existing block in the transformation."""

    op: Literal['set_code']
    block_id: str = Field(description='The ID of the block to set the code in')
    code_id: str = Field(description='The ID of the code to set')
    script: str = Field(description='The SQL script of the code to set')


class TfAddScript(BaseModel):
    """Append or prepend SQL script text to an existing code in an existing block in the transformation."""

    op: Literal['add_script']
    block_id: str = Field(description='The ID of the block to add the script to')
    code_id: str = Field(description='The ID of the code to add the script to')
    script: str = Field(description='The SQL script to add')
    position: TfPosition = Field(description='The position of the script to add', default='end')


class TfStrReplace(BaseModel):
    """Replace a substring in SQL statements in the transformation."""

    op: Literal['str_replace']
    block_id: Optional[str] = Field(
        description='The ID of the block to replace substrings in. If not provided, all blocks will be updated.',
        default=None,
    )
    code_id: Optional[str] = Field(
        description='The ID of the code to replace substrings in. '
        'If not provided, all codes in the block will be updated.',
        default=None,
    )
    search_for: str = Field(description='Substring to search for (non-empty)')
    replace_with: str = Field(description='Replacement string (can be empty for deletion)')

    @model_validator(mode='after')
    def validate_code_id_requires_block_id(self) -> 'TfStrReplace':
        """Validate that code_name can only be specified if block_name is also specified."""
        if self.block_id is None and self.code_id is not None:
            raise ValueError('code_id must be None if block_id is None')
        return self


# Discriminated union of all transformation parameter update operations
TfParamUpdate = Annotated[
    Union[
        TfAddBlock,
        TfRemoveBlock,
        TfRenameBlock,
        TfAddCode,
        TfRemoveCode,
        TfRenameCode,
        TfSetCode,
        TfAddScript,
        TfStrReplace,
    ],
    Field(discriminator='op'),
]


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
    change_summary: Optional[str] = Field(
        description="Optional summary of the change to update the agent's context.",
        default=None,
    )


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
