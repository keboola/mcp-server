from typing import Any, List, Optional, Union

from pydantic import AliasChoices, BaseModel, Field

from keboola_mcp_server.tools.components.model import ComponentConfigurationResponseBase


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
    creator_token: Optional[dict[str, Any]] = Field(
        None,
        description='Token of the creator of the flow configuration',
        validation_alias=AliasChoices('creatorToken', 'creator_token', 'creator-token'),
        serialization_alias='creatorToken',
    )

    @classmethod
    def from_raw_config(cls, raw_config: dict[str, Any]) -> 'FlowConfigurationResponse':
        """
        Create a FlowConfigurationResponse from a raw configuration dictionary.
        This method is particularly useful when the input data does not perfectly align with the
        model's field names, allowing for flexible mapping and default value handling.
        """
        return cls(
            component_id=raw_config['componentId'],
            configuration_id=raw_config['id'],
            configuration_name=raw_config['name'],
            configuration_description=raw_config.get('description'),
            version=raw_config['version'],
            is_disabled=raw_config.get('isDisabled', False),
            is_deleted=raw_config.get('isDeleted', False),
            change_description=raw_config.get('changeDescription'),
            configuration=raw_config['configuration'],
            configuration_metadata=raw_config.get('metadata', []),
            created=raw_config.get('created'),
            creator_token=raw_config.get('creatorToken'),
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
        """
        Creates a ReducedFlow instance from a raw configuration dictionary.
        This method simplifies the process of creating a ReducedFlow object by mapping dictionary keys
        to model fields and calculating phase and task counts from the nested configuration data.
        """
        # Safely access nested 'configuration' dictionary and then 'phases' and 'tasks' lists
        config_data = raw_config.get('configuration', {})
        phases_count = len(config_data.get('phases', []))
        tasks_count = len(config_data.get('tasks', []))

        return cls(
            id=raw_config['id'],
            name=raw_config['name'],
            description=raw_config.get('description', ''),
            created=raw_config.get('created'),
            version=raw_config['version'],
            is_disabled=raw_config.get('isDisabled', False),
            is_deleted=raw_config.get('isDeleted', False),
            phases_count=phases_count,
            tasks_count=tasks_count,
        )
