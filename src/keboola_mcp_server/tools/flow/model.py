from datetime import datetime
from typing import Any, Optional

from pydantic import AliasChoices, BaseModel, Field

from keboola_mcp_server.client import FLOW_TYPE
from keboola_mcp_server.links import Link
from keboola_mcp_server.tools.flow.api_models import APIFlowResponse


class ListFlowsOutput(BaseModel):
    """Output of list_flows tool."""
    flows: list['FlowSummary'] = Field(description='The retrieved flow configurations.')
    links: list[Link] = Field(description='The list of links relevant to the flows.')


class FlowPhase(BaseModel):
    """Represents a phase in a flow configuration."""
    id: int | str = Field(description='Unique identifier of the phase')
    name: str = Field(description='Name of the phase', min_length=1)
    description: str = Field(default_factory=str, description='Description of the phase')
    depends_on: list[int | str] = Field(
        default_factory=list,
        description='List of phase IDs this phase depends on',
        validation_alias=AliasChoices('dependsOn', 'depends_on', 'depends-on'),
        serialization_alias='dependsOn',
    )


class FlowTask(BaseModel):
    """Represents a task in a flow configuration."""
    id: int | str = Field(description='Unique identifier of the task')
    name: str = Field(description='Name of the task')
    phase: int | str = Field(description='ID of the phase this task belongs to')
    enabled: Optional[bool] = Field(default=True, description='Whether the task is enabled')
    continue_on_failure: Optional[bool] = Field(
        default=False,
        description='Whether to continue if task fails',
        validation_alias=AliasChoices('continueOnFailure', 'continue_on_failure', 'continue-on-failure'),
        serialization_alias='continueOnFailure',
    )
    task: dict[str, Any] = Field(description='Task configuration containing componentId, configId, etc.')


class FlowConfiguration(BaseModel):
    """Represents a complete flow configuration."""
    phases: list[FlowPhase] = Field(description='List of phases in the flow')
    tasks: list[FlowTask] = Field(description='List of tasks in the flow')


class FlowToolResponse(BaseModel):
    """
    Standard response model for flow tool operations.

    :param flow_id: The id of the flow.
    :param description: The description of the Flow.
    :param timestamp: The timestamp of the operation.
    :param success: Indicates if the operation succeeded.
    :param links: The links relevant to the flow.
    """
    flow_id: str = Field(description='The id of the flow.', validation_alias=AliasChoices('id', 'flow_id'))
    description: str = Field(description='The description of the Flow.')
    timestamp: datetime = Field(
        description='The timestamp of the operation.',
        validation_alias=AliasChoices('timestamp', 'created'),
    )
    success: bool = Field(default=True, description='Indicates if the operation succeeded.')
    links: list[Link] = Field(description='The links relevant to the flow.')


class Flow(BaseModel):
    """Complete flow configuration with all data."""
    component_id: FLOW_TYPE = Field(description='The ID of the component (keboola.orchestrator/keboola.flow)')
    configuration_id: str = Field(description='The ID of this flow configuration')
    name: str = Field(description='The name of the flow configuration')
    description: Optional[str] = Field(default=None, description='The description of the flow configuration')
    version: int = Field(description='The version of the flow configuration')
    is_disabled: bool = Field(default=False, description='Whether the flow configuration is disabled')
    is_deleted: bool = Field(default=False, description='Whether the flow configuration is deleted')
    configuration: FlowConfiguration = Field(description='The flow configuration containing phases and tasks')
    change_description: Optional[str] = Field(default=None, description='The description of the latest changes')
    configuration_metadata: list[dict[str, Any]] = Field(
        default_factory=list,
        description='Flow configuration metadata including MCP tracking')
    created: Optional[str] = Field(None, description='Creation timestamp')
    updated: Optional[str] = Field(None, description='Last update timestamp')
    links: list[Link] = Field(default_factory=list, description='MCP-specific links for UI navigation')

    @classmethod
    def from_api_response(cls, api_config: APIFlowResponse,
                          flow_component_id: FLOW_TYPE,
                          links: Optional[list[Link]] = None) -> 'Flow':
        """
        Create a Flow domain model from an APIFlowResponse.

        :param api_config: The APIFlowResponse instance.
        :param flow_component_id: The component ID of the flow.
        :param links: Optional list of navigation links.
        :return: Flow domain model.
        """
        config = FlowConfiguration(
            phases=[FlowPhase.model_validate(p) for p in api_config.configuration.get('phases', [])],
            tasks=[FlowTask.model_validate(t) for t in api_config.configuration.get('tasks', [])],
        )
        links = links if links else []
        return cls.model_construct(
            component_id=flow_component_id,
            configuration_id=api_config.configuration_id,
            name=api_config.name,
            description=api_config.description,
            version=api_config.version,
            is_disabled=api_config.is_disabled,
            is_deleted=api_config.is_deleted,
            configuration=config,
            change_description=api_config.change_description,
            configuration_metadata=api_config.metadata,
            created=api_config.created,
            updated=api_config.updated,
            links=links
        )


class FlowSummary(BaseModel):
    """Lightweight flow configuration for list operations."""
    component_id: FLOW_TYPE = Field(description='The ID of the component (keboola.orchestrator/keboola.flow)')
    configuration_id: str = Field(description='The ID of this flow configuration')
    name: str = Field(description='The name of the flow configuration')
    description: Optional[str] = Field(default=None, description='The description of the flow configuration')
    version: int = Field(description='The version of the flow configuration')
    is_disabled: bool = Field(default=False, description='Whether the flow configuration is disabled')
    is_deleted: bool = Field(default=False, description='Whether the flow configuration is deleted')
    phases_count: int = Field(description='Number of phases in the flow')
    tasks_count: int = Field(description='Number of tasks in the flow')
    created: Optional[str] = Field(None, description='Creation timestamp')
    updated: Optional[str] = Field(None, description='Last update timestamp')

    @classmethod
    def from_api_response(cls, api_config: APIFlowResponse, flow_component_id: FLOW_TYPE) -> 'FlowSummary':
        """
        Create a FlowSummary domain model from an APIFlowResponse.

        :param api_config: The APIFlowResponse instance.
        :param flow_component_id: The component ID of the flow.
        :return: FlowSummary domain model.
        """
        config = getattr(api_config, 'configuration', {}) or {}
        return cls.model_construct(
            component_id=flow_component_id,
            configuration_id=api_config.configuration_id,
            name=api_config.name,
            description=api_config.description,
            version=api_config.version,
            is_disabled=api_config.is_disabled,
            is_deleted=api_config.is_deleted,
            phases_count=len(config.get('phases', [])),
            tasks_count=len(config.get('tasks', [])),
            created=api_config.created,
            updated=api_config.updated,
        )
