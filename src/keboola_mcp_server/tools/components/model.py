from typing import Any, List, Literal, Optional, Union

from pydantic import AliasChoices, BaseModel, Field, model_validator

from keboola_mcp_server.client import ORCHESTRATOR_COMPONENT_ID

ComponentType = Literal['application', 'extractor', 'writer']
TransformationType = Literal['transformation']
AllComponentTypes = Union[ComponentType, TransformationType]


class ReducedComponent(BaseModel):
    """
    A Reduced Component containing basic information about the Keboola Component and its capabilities.
    This model is used in list views or when only basic component information is needed.
    """

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
    component_type: str = Field(
        description='The type of the component',
        validation_alias=AliasChoices('type', 'component_type', 'componentType', 'component-type'),
        serialization_alias='componentType',
    )

    component_flags: list[str] = Field(
        default_factory=list,
        description='List of developer portal flags.',
        validation_alias=AliasChoices('flags', 'component_flags', 'componentFlags', 'component-flags'),
        serialization_alias='componentFlags',
    )

    # Capability flags derived from component_flags
    is_row_based: bool = Field(
        default=False,
        description='Whether the component is row-based (e.g. have configuration rows) or not.',
        validation_alias=AliasChoices('is_row_based', 'isRowBased', 'is-row-based'),
        serialization_alias='isRowBased',
    )

    has_table_input_mapping: bool = Field(
        default=False,
        description='Whether the component configuration has table input mapping or not.',
        validation_alias=AliasChoices('has_table_input_mapping', 'hasTableInputMapping', 'has-table-input-mapping'),
        serialization_alias='hasTableInputMapping',
    )

    has_table_output_mapping: bool = Field(
        default=False,
        description='Whether the component configuration has table output mapping or not.',
        validation_alias=AliasChoices('has_table_output_mapping', 'hasTableOutputMapping', 'has-table-output-mapping'),
        serialization_alias='hasTableOutputMapping',
    )

    has_file_input_mapping: bool = Field(
        default=False,
        description='Whether the component configuration has file input mapping or not.',
        validation_alias=AliasChoices('has_file_input_mapping', 'hasFileInputMapping', 'has-file-input-mapping'),
        serialization_alias='hasFileInputMapping',
    )

    has_file_output_mapping: bool = Field(
        default=False,
        description='Whether the component configuration has file output mapping or not.',
        validation_alias=AliasChoices('has_file_output_mapping', 'hasFileOutputMapping', 'has-file-output-mapping'),
        serialization_alias='hasFileOutputMapping',
    )

    has_oauth: bool = Field(
        default=False,
        description='Whether the component configuration requires OAuth authorization or not.',
        validation_alias=AliasChoices('has_oauth', 'hasOauth', 'has-oauth'),
        serialization_alias='hasOauth',
    )

    @model_validator(mode='after')
    def derive_capabilities(self) -> 'ReducedComponent':
        table_input_mapping_flags = ('genericDockerUI-tableInput', 'genericDockerUI-simpleTableInput')

        self.is_row_based = 'genericDockerUI-rows' in self.component_flags
        self.has_table_input_mapping = any(f in self.component_flags for f in table_input_mapping_flags)
        self.has_table_output_mapping = 'genericDockerUI-tableOutput' in self.component_flags
        self.has_file_input_mapping = 'genericDockerUI-fileInput' in self.component_flags
        self.has_file_output_mapping = 'genericDockerUI-fileOutput' in self.component_flags
        self.has_oauth = 'genericDockerUI-authorization' in self.component_flags

        return self


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


class Component(ReducedComponent):
    """
    A Component containing detailed information about the Keboola Component, including its capabilities,
    documentation, and configuration schemas.
    """

    component_categories: list[str] = Field(
        default_factory=list,
        description='The categories the component belongs to.',
        validation_alias=AliasChoices(
            'componentCategories', 'component_categories', 'component-categories', 'categories'
        ),
        serialization_alias='categories',
    )
    documentation_url: Optional[str] = Field(
        default=None,
        description='The url where the documentation can be found.',
        validation_alias=AliasChoices('documentationUrl', 'documentation_url', 'documentation-url'),
        serialization_alias='documentationUrl',
    )
    documentation: Optional[str] = Field(
        default=None,
        description='The documentation of the component.',
        serialization_alias='documentation',
    )
    configuration_schema: Optional[dict[str, Any]] = Field(
        default=None,
        description='The configuration schema for the component.',
        validation_alias=AliasChoices('configurationSchema', 'configuration_schema', 'configuration-schema'),
        serialization_alias='configurationSchema',
    )
    configuration_row_schema: Optional[dict[str, Any]] = Field(
        default=None,
        description='The configuration row schema of the component.',
        validation_alias=AliasChoices('configurationRowSchema', 'configuration_row_schema', 'configuration-row-schema'),
        serialization_alias='configurationRowSchema',
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

    component: ReducedComponent = Field(description='The Keboola component.')
    configurations: List[ComponentConfigurationMetadata] = Field(
        description='The list of configurations metadata associated with the component.',
    )


class FlowPhase(BaseModel):
    """Represents a phase in a flow configuration."""

    id: Union[int, str] = Field(description='Unique identifier of the phase')
    name: str = Field(description='Name of the phase', min_length=1)
    description: str = Field(default_factory=str, description='Description of the phase')
    depends_on: List[Union[int, str]] = Field(
        default_factory=list,
        description='List of phase IDs this phase depends on',
        validation_alias=AliasChoices('dependsOn', 'depends_on', 'depends-on'),
        serialization_alias='dependsOn',
    )


class FlowTask(BaseModel):
    """Represents a task in a flow configuration."""

    id: Union[int, str] = Field(description='Unique identifier of the task')
    name: str = Field(description='Name of the task')
    phase: Union[int, str] = Field(description='ID of the phase this task belongs to')
    enabled: bool = Field(default=True, description='Whether the task is enabled')
    continue_on_failure: bool = Field(
        default=False,
        description='Whether to continue if task fails',
        validation_alias=AliasChoices('continueOnFailure', 'continue_on_failure', 'continue-on-failure'),
        serialization_alias='continueOnFailure',
    )
    task: dict[str, Any] = Field(description='Task configuration containing componentId, configId, etc.')


class FlowConfiguration(BaseModel):
    """Represents a complete flow configuration."""

    phases: List[FlowPhase] = Field(description='List of phases in the flow')
    tasks: List[FlowTask] = Field(description='List of tasks in the flow')


class FlowConfigurationResponse(ComponentConfigurationResponseBase):
    """
    Detailed information about a Keboola Flow Configuration, extending the base configuration response.
    """

    version: int = Field(description='The version of the flow configuration')
    configuration: FlowConfiguration = Field(description='The flow configuration containing phases and tasks')
    change_description: Optional[str] = Field(
        description='The description of the changes made to the flow configuration',
        default=None,
        validation_alias=AliasChoices('changeDescription', 'change_description', 'change-description'),
        serialization_alias='changeDescription',
    )
    configuration_metadata: list[dict[str, Any]] = Field(
        description='The metadata of the flow configuration',
        default_factory=list,
        validation_alias=AliasChoices(
            'metadata', 'configuration_metadata', 'configurationMetadata', 'configuration-metadata'
        ),
        serialization_alias='configurationMetadata',
    )
    created: Optional[str] = Field(None, description='Creation timestamp')

    @classmethod
    def from_raw_config(cls, raw_config: dict[str, Any]) -> 'FlowConfigurationResponse':
        """Create a FlowConfigurationResponse object from raw API response."""

        config_data = raw_config.get('configuration', {})

        # Parse phases and tasks directly from configuration
        phases = [FlowPhase.model_validate(phase) for phase in config_data.get('phases', [])]
        tasks = [FlowTask.model_validate(task) for task in config_data.get('tasks', [])]

        flow_config = FlowConfiguration(phases=phases, tasks=tasks)

        return cls(
            component_id=ORCHESTRATOR_COMPONENT_ID,
            configuration_id=raw_config['id'],
            configuration_name=raw_config['name'],
            configuration_description=raw_config.get('description', ''),
            version=raw_config.get('version', 1),
            is_disabled=raw_config.get('isDisabled', False),
            is_deleted=raw_config.get('isDeleted', False),
            configuration=flow_config,
            change_description=raw_config.get('changeDescription'),
            configuration_metadata=raw_config.get('metadata', []),
            created=raw_config.get('created'),
        )


class ReducedFlow(BaseModel):
    """Lightweight flow summary for listing operations - consistent with ReducedComponent naming."""

    id: str = Field(
        description='Configuration ID of the flow',
        validation_alias=AliasChoices('id', 'configuration_id', 'configurationId'),
    )
    name: str = Field(description='Name of the flow')
    description: str = Field(description='Description of the flow')
    created: Optional[str] = Field(None, description='Creation timestamp')
    version: int = Field(description='Version number of the flow')
    is_disabled: bool = Field(
        default=False,
        description='Whether the flow is disabled',
        validation_alias=AliasChoices('isDisabled', 'is_disabled', 'is-disabled'),
        serialization_alias='isDisabled',
    )
    is_deleted: bool = Field(
        default=False,
        description='Whether the flow is deleted',
        validation_alias=AliasChoices('isDeleted', 'is_deleted', 'is-deleted'),
        serialization_alias='isDeleted',
    )
    phases_count: int = Field(description='Number of phases in the flow')
    tasks_count: int = Field(description='Number of tasks in the flow')

    @classmethod
    def from_raw_config(cls, raw_config: dict[str, Any]) -> 'ReducedFlow':
        """Create a ReducedFlow object from raw API response."""

        config_data = raw_config.get('configuration', {})

        return cls(
            id=raw_config['id'],
            name=raw_config['name'],
            description=raw_config.get('description', ''),
            created=raw_config.get('created'),
            version=raw_config.get('version', 1),
            is_disabled=raw_config.get('isDisabled', False),
            is_deleted=raw_config.get('isDeleted', False),
            phases_count=len(config_data.get('phases', [])),
            tasks_count=len(config_data.get('tasks', [])),
        )
