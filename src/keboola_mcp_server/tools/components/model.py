from typing import Any, Dict, List, Literal, Optional, Union

from pydantic import AliasChoices, BaseModel, Field

ComponentType = Literal["application", "extractor", "writer"]
TransformationType = Literal["transformation"]
AllComponentTypes = Union[ComponentType, TransformationType]


class ReducedComponent(BaseModel):
    """
    A Reduced Component containing reduced information about the Keboola Component used in a list or comprehensive view.
    """

    component_id: str = Field(
        description="The ID of the component",
        validation_alias=AliasChoices("id", "component_id", "componentId", "component-id"),
        serialization_alias="componentId",
    )
    component_name: str = Field(
        description="The name of the component",
        validation_alias=AliasChoices(
            "name",
            "component_name",
            "componentName",
            "component-name",
        ),
        serialization_alias="componentName",
    )
    component_type: str = Field(
        description="The type of the component",
        validation_alias=AliasChoices("type", "component_type", "componentType", "component-type"),
        serialization_alias="componentType",
    )
    component_description: Optional[str] = Field(
        description="The description of the component",
        default=None,
        validation_alias=AliasChoices(
            "description", "component_description", "componentDescription", "component-description"
        ),
        serialization_alias="componentDescription",
    )


class ReducedComponentConfiguration(BaseModel):
    """
    A Reduced Component Configuration containing the Keboola Component ID and the reduced information about configuration
    used in a list.
    """

    component_id: str = Field(
        description="The ID of the component",
        validation_alias=AliasChoices("component_id", "componentId", "component-id"),
        serialization_alias="componentId",
    )
    configuration_id: str = Field(
        description="The ID of the component configuration",
        validation_alias=AliasChoices(
            "id",
            "configuration_id",
            "configurationId",
            "configuration-id",
        ),
        serialization_alias="configurationId",
    )
    configuration_name: str = Field(
        description="The name of the component configuration",
        validation_alias=AliasChoices(
            "name",
            "configuration_name",
            "configurationName",
            "configuration-name",
        ),
        serialization_alias="configurationName",
    )
    configuration_description: Optional[str] = Field(
        description="The description of the component configuration",
        validation_alias=AliasChoices(
            "description",
            "configuration_description",
            "configurationDescription",
            "configuration-description",
        ),
        serialization_alias="configurationDescription",
    )
    is_disabled: bool = Field(
        description="Whether the component configuration is disabled",
        validation_alias=AliasChoices("isDisabled", "is_disabled", "is-disabled"),
        serialization_alias="isDisabled",
        default=False,
    )
    is_deleted: bool = Field(
        description="Whether the component configuration is deleted",
        validation_alias=AliasChoices("isDeleted", "is_deleted", "is-deleted"),
        serialization_alias="isDeleted",
        default=False,
    )


class ComponentWithConfigurations(BaseModel):
    """
    Grouping of a Keboola Component and its associated configurations.
    """

    component: ReducedComponent = Field(description="The Keboola component.")
    configurations: List[ReducedComponentConfiguration] = Field(
        description="The list of component configurations for the given component."
    )


class Component(ReducedComponent):
    """
    Detailed information about a Keboola Component, containing all the relevant details.
    """

    long_description: Optional[str] = Field(
        description="The long description of the component",
        default=None,
        validation_alias=AliasChoices("longDescription", "long_description", "long-description"),
        serialization_alias="longDescription",
    )
    categories: List[str] = Field(description="The categories of the component", default=[])
    version: int = Field(description="The version of the component")
    configuration_schema: Optional[Dict[str, Any]] = Field(
        description=(
            "The configuration schema of the component, detailing the structure and requirements of the "
            "configuration."
        ),
        validation_alias=AliasChoices(
            "configurationSchema", "configuration_schema", "configuration-schema"
        ),
        serialization_alias="configurationSchema",
        default=None,
    )
    configuration_description: Optional[str] = Field(
        description="The configuration description of the component",
        validation_alias=AliasChoices(
            "configurationDescription", "configuration_description", "configuration-description"
        ),
        serialization_alias="configurationDescription",
        default=None,
    )
    empty_configuration: Optional[Dict[str, Any]] = Field(
        description="The empty configuration of the component",
        validation_alias=AliasChoices(
            "emptyConfiguration", "empty_configuration", "empty-configuration"
        ),
        serialization_alias="emptyConfiguration",
        default=None,
    )


class ComponentConfiguration(ReducedComponentConfiguration):
    """
    Detailed information about a Keboola Component Configuration, containing all the relevant details.
    """

    version: int = Field(description="The version of the component configuration")
    configuration: Dict[str, Any] = Field(description="The configuration of the component")
    rows: Optional[List[Dict[str, Any]]] = Field(
        description="The rows of the component configuration", default=None
    )
    configuration_metadata: List[Dict[str, Any]] = Field(
        description="The metadata of the component configuration",
        default=[],
        validation_alias=AliasChoices(
            "metadata", "configuration_metadata", "configurationMetadata", "configuration-metadata"
        ),
        serialization_alias="configurationMetadata",
    )
    component: Optional[Component] = Field(
        description="The Keboola component.",
        validation_alias=AliasChoices("component"),
        serialization_alias="component",
        default=None,
    )
