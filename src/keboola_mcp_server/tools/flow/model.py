from datetime import datetime
from typing import Any, List, Optional, Union

from pydantic import AliasChoices, BaseModel, Field, model_validator

from keboola_mcp_server.client import ORCHESTRATOR_COMPONENT_ID
from keboola_mcp_server.links import Link
from keboola_mcp_server.tools.components.model import ComponentConfigurationResponseBase


class ListFlowsOutput(BaseModel):
    """Output of list_flows tool."""

    flows: List['ReducedFlow'] = Field(description='The retrieved flow configurations.')
    links: List[Link] = Field(
        description='The list of links relevant to the flows.',
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
    links: Optional[list[Link]] = Field(None, description='Links relevant to the flow configuration.')

    @model_validator(mode='before')
    @classmethod
    def _initialize_component_id_to_orchestrator(cls, data: Any) -> Any:
        """Initialize component_id to Orchestrator if not provided."""
        if isinstance(data, dict) and 'component_id' not in data:
            data['component_id'] = ORCHESTRATOR_COMPONENT_ID
        return data


class ReducedFlow(BaseModel):
    """Lightweight flow summary for listing operations - consistent with ReducedComponent naming."""

    id: str = Field(
        description='Configuration ID of the flow',
        validation_alias=AliasChoices('id', 'configuration_id', 'configurationId'),
    )
    name: str = Field(description='Name of the flow')
    description: str = Field(default='', description='Description of the flow')
    created: Optional[str] = Field(None, description='Creation timestamp')
    version: int = Field(default=1, description='Version number of the flow')
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

    @model_validator(mode='before')
    @classmethod
    def _initialize_phases_and_tasks_count(cls, data: Any) -> Any:
        """Initialize phases_count and tasks_count if not provided."""
        if isinstance(data, dict):
            config_data = data.get('configuration', {})
            if 'tasks_count' not in data:
                data['tasks_count'] = len(config_data.get('tasks', []))
            if 'phases_count' not in data:
                data['phases_count'] = len(config_data.get('phases', []))
        return data


class FlowToolResponse(BaseModel):
    flow_id: str = Field(..., description='The id of the flow.', validation_alias=AliasChoices('id', 'flow_id'))
    description: str = Field(..., description='The description of the Flow.')
    timestamp: datetime = Field(
        ...,
        description='The timestamp of the operation.',
        validation_alias=AliasChoices('timestamp', 'created'),
    )
    success: bool = Field(default=True, description='Indicates if the operation succeeded.')
    links: list[Link] = Field(description='The links relevant to the flow.')
