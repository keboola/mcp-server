from typing import Any, List, Literal, Optional, Union

from pydantic import AliasChoices, BaseModel, Field

ComponentType = Literal['application', 'extractor', 'writer']
TransformationType = Literal['transformation']
AllComponentTypes = Union[ComponentType, TransformationType]


class ReducedComponent(BaseModel):
    """
    A Reduced Component containing reduced information about the Keboola Component used in a list or comprehensive view.
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

    flags: list[str] = Field(
        default_factory=list,
        description='List of developer portal flags.',
    )


class ComponentConfigurationResponseBase(BaseModel):
    """
    A Reduced Component Configuration containing the Keboola Component ID and the reduced information about configuration
    used in a list.
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
        validation_alias=AliasChoices(
            'configurationSchema', 'configuration_schema', 'configuration-schema'
        ),
        serialization_alias='configurationSchema',
    )
    configuration_row_schema: Optional[dict[str, Any]] = Field(
        default=None,
        description='The configuration row schema of the component.',
        validation_alias=AliasChoices(
            'configurationRowSchema', 'configuration_row_schema', 'configuration-row-schema'
        ),
        serialization_alias='configurationRowSchema',
    )


class ComponentConfigurationResponse(ComponentConfigurationResponseBase):
    """
    Detailed information about a Keboola Component Configuration, containing all the relevant details.
    """

    version: int = Field(description='The version of the component configuration')
    configuration: dict[str, Any] = Field(description='The configuration of the component')
    rows: Optional[list[dict[str, Any]]] = Field(
        description='The rows of the component configuration', default=None
    )
    configuration_metadata: list[dict[str, Any]] = Field(
        description='The metadata of the component configuration',
        default=[],
        validation_alias=AliasChoices(
            'metadata', 'configuration_metadata', 'configurationMetadata', 'configuration-metadata'
        ),
        serialization_alias='configurationMetadata',
    )
    component: Optional[Component] = Field(
        description='The Keboola component.',
        validation_alias=AliasChoices('component'),
        serialization_alias='component',
        default=None,
    )


# ### Tool input/output models


class ReducedComponentDetail(BaseModel):
    component_id: str = Field(
        description='The ID of the component',
    )
    component_name: str = Field(
        description='The name of the component',
    )
    component_type: str = Field(
        description='The type of the component',
    )

    is_row_based: bool = Field(
        default=False,
        description='Whether the component is row-based (e.g. have configuration rows) or not.',
    )

    has_table_input_mapping: bool = Field(
        default=False,
        description='Whether the component configuration has table input mapping or not.',
    )
    has_table_output_mapping: bool = Field(
        default=False,
        description='Whether the component configuration has table output mapping or not.',
    )
    has_file_input_mapping: bool = Field(
        default=False,
        description='Whether the component configuration has file input mapping or not.',
    )
    has_file_output_mapping: bool = Field(
        default=False,
        description='Whether the component configuration has file output mapping or not.',
    )

    has_oauth: bool = Field(
        default=False,
        description='Whether the component configuration requires OAuth authorization or not.',
    )

    @classmethod
    def from_component_response(cls, component: ReducedComponent) -> 'ReducedComponentDetail':
        """
        Create a ComponentDetail instance from a Component instance.
        """

        is_row_based = 'genericDockerUI-rows' in component.flags
        has_table_input_mapping = 'genericDockerUI-tableInput' in component.flags
        has_table_output_mapping = 'genericDockerUI-tableOutput' in component.flags
        has_file_input_mapping = 'genericDockerUI-fileInput' in component.flags
        has_file_output_mapping = 'genericDockerUI-fileOutput' in component.flags
        has_oauth = 'genericDockerUI-authorization' in component.flags

        return cls(
            component_id=component.component_id,
            component_name=component.component_name,
            component_type=component.component_type,
            is_row_based=is_row_based,
            has_table_input_mapping=has_table_input_mapping,
            has_table_output_mapping=has_table_output_mapping,
            has_file_input_mapping=has_file_input_mapping,
            has_file_output_mapping=has_file_output_mapping,
            has_oauth=has_oauth,
        )


class ComponentDetail(ReducedComponentDetail):
    """
    Detailed information about a Keboola Component, containing all the relevant details.
    """

    component_categories: list[str] = Field(
        default_factory=list,
        description='The categories the component belongs to.',
        serialization_alias='categories',
    )
    documentation_url: Optional[str] = Field(
        default=None,
        description='The url where the documentation can be found.',
    )
    documentation: Optional[str] = Field(
        default=None,
        description='The documentation of the component.',
        serialization_alias='documentation',
    )
    root_configuration_schema: Optional[dict[str, Any]] = Field(
        default=None,
        description='The configuration schema of the component root configuration.',
    )
    row_configuration_schema: Optional[dict[str, Any]] = Field(
        default=None,
        description='The configuration schema of the component row configuration.',
    )

    @classmethod
    def from_component_response(cls, component: Component) -> 'ComponentDetail':
        core_details = ReducedComponentDetail.from_component_response(component)
        return cls(
            component_categories=component.component_categories,
            documentation_url=component.documentation_url,
            documentation=component.documentation,
            root_configuration_schema=component.configuration_schema,
            row_configuration_schema=component.configuration_row_schema,
            **core_details.model_dump(),
        )


class ComponentRowConfiguration(ComponentConfigurationResponseBase):
    """
    Detailed information about a Keboola Component Configuration, containing all the relevant details.
    """

    version: int = Field(description='The version of the component configuration')
    storage: Optional[dict[str, Any]] = Field(
        description='The table and/or file input / output mapping of the component configuration. It is present only for components that are not row-based and have tables or file input mapping defined.',
        default=None,
    )
    parameters: dict[str, Any] = Field(
        description='The user parameters, adhering to the row configuration schema'
    )
    configuration_metadata: list[dict[str, Any]] = Field(
        description='The metadata of the component configuration',
        default=[],
        validation_alias=AliasChoices(
            'metadata', 'configuration_metadata', 'configurationMetadata', 'configuration-metadata'
        ),
        serialization_alias='configurationMetadata',
    )


class ComponentRootConfiguration(ComponentConfigurationResponseBase):
    """
    Detailed information about a Keboola Component Configuration, containing all the relevant details.
    """

    version: int = Field(description='The version of the component configuration')
    storage: Optional[dict[str, Any]] = Field(
        description='The table and/or file input / output mapping of the component configuration. It is present only for components that are not row-based and have tables or file input mapping defined',
        default=None,
    )
    parameters: dict[str, Any] = Field(
        description='The component configuration parameters, adhering to the root configuration schema'
    )


class ComponentConfigurationOutput(BaseModel):
    root_configuration: ComponentRootConfiguration = Field(
        description='The root configuration of the component configuration'
    )
    row_configurations: Optional[list[ComponentRowConfiguration]] = Field(
        description='The row configurations of the component configuration',
        default=None,
    )
    component_details: Optional[ComponentDetail] = Field(
        description='Details of the component including documentation and configuration schemas',
        default=None,
    )

    @classmethod
    def from_component_configuration_response(
        cls,
        configuration_response: ComponentConfigurationResponse,
        component_details: Optional[ComponentDetail] = None,
    ) -> 'ComponentConfigurationOutput':
        """
        Create a ComponentConfigurationOutput instance from a ComponentConfigurationResponse instance.
        """

        root_configuration = ComponentRootConfiguration(
            **configuration_response.model_dump(exclude={'configuration'}),
            parameters=configuration_response.configuration.get('parameters', {}),
            storage=configuration_response.configuration.get('storage'),
        )
        row_configurations = []
        for row in configuration_response.rows or []:
            if row is None:
                continue

            row_configuration = ComponentRowConfiguration(
                **row,
                component_id=configuration_response.component_id,
                parameters=row['configuration']['parameters'],
                storage=row['configuration'].get('storage'),
            )
            row_configurations.append(row_configuration)

        return cls(
            root_configuration=root_configuration,
            row_configurations=row_configurations,
            component_details=component_details,
        )


class ComponentConfigurationMetadata(BaseModel):
    root_configuration: ComponentConfigurationResponseBase = Field(
        description='The root configuration metadata of the component configuration'
    )
    row_configurations: Optional[list[ComponentConfigurationResponseBase]] = Field(
        description='The row configuration metadata of the component configuration',
        default=None,
    )

    @classmethod
    def from_component_configuration_response(
        cls, configuration: ComponentConfigurationResponse
    ) -> 'ComponentConfigurationMetadata':
        """
        Create a ComponentConfigurationMetadata instance from a ComponentConfigurationResponse instance.
        """
        root_configuration: ComponentConfigurationResponseBase = configuration
        row_configurations = None
        if configuration.rows:
            row_configurations = [
                ComponentConfigurationResponse.model_validate(row)
                for row in configuration.rows
                if row is not None
            ]
        return cls(root_configuration=root_configuration, row_configurations=row_configurations)


class ComponentWithConfigurations(BaseModel):
    """
    Grouping of a Keboola Component and its associated configurations metadata.
    """

    component: ReducedComponentDetail = Field(description='The Keboola component.')
    configurations: List[ComponentConfigurationMetadata] = Field(
        description='The list of component configuration metadata for the given component.'
    )
