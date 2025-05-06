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


class ReducedComponentConfiguration(BaseModel):
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


class ComponentWithConfigurations(BaseModel):
    """
    Grouping of a Keboola Component and its associated configurations.
    """

    component: ReducedComponent = Field(description='The Keboola component.')
    configurations: List[ReducedComponentConfiguration] = Field(
        description='The list of component configurations for the given component.'
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
        validation_alias=AliasChoices('configurationSchema', 'configuration_schema', 'configuration-schema'),
        serialization_alias='configurationSchema',
    )
    configuration_row_schema: Optional[dict[str, Any]] = Field(
        default=None,
        description='The configuration row schema of the component.',
        validation_alias=AliasChoices('configurationRowSchema', 'configuration_row_schema', 'configuration-row-schema'),
        serialization_alias='configurationRowSchema',
    )


class ComponentConfiguration(ReducedComponentConfiguration):
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
