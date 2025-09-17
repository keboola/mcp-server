"""
Flow models for Keboola MCP server.
"""

from datetime import datetime
from typing import Any, Literal, Optional, Union

from pydantic import AliasChoices, BaseModel, Field

from keboola_mcp_server.clients.client import ORCHESTRATOR_COMPONENT_ID, FlowType
from keboola_mcp_server.clients.storage import APIFlowResponse
from keboola_mcp_server.links import Link

# =============================================================================
# RESPONSE MODELS
# =============================================================================


class ListFlowsOutput(BaseModel):
    """Output of list_flows tool."""

    flows: list['FlowSummary'] = Field(description='The retrieved flow configurations.')
    links: list[Link] = Field(description='The list of links relevant to the flows.')


class FlowToolOutput(BaseModel):
    """
    Standard response model for flow tool operations.

    :param configuration_id: The configuration ID of the flow.
    :param component_id: The component ID of the flow.
    :param description: The description of the Flow.
    :param timestamp: The timestamp of the operation.
    :param success: Indicates if the operation succeeded.
    :param links: The links relevant to the flow.
    :param version: The version number of the flow configuration.
    """

    configuration_id: str = Field(description='The configuration ID of the flow.')
    component_id: str = Field(description='The ID of the component.')
    description: str = Field(description='The description of the Flow.')
    version: int = Field(description='The version number of the flow configuration.')
    timestamp: datetime = Field(description='The timestamp of the operation.')
    success: bool = Field(default=True, description='Indicates if the operation succeeded.')
    links: list[Link] = Field(description='The links relevant to the flow.')


# =============================================================================
# LEGACY ORCHESTRATOR FLOW MODELS
# =============================================================================


class FlowPhase(BaseModel):
    """Represents a phase in a legacy flow configuration."""

    id: int | str = Field(description='Unique identifier of the phase')
    name: str = Field(description='Name of the phase', min_length=1)
    description: str = Field(default_factory=str, description='Description of the phase')
    depends_on: list[int | str] = Field(
        default_factory=list,
        description='List of phase IDs this phase depends on',
        validation_alias=AliasChoices('depends_on', 'dependsOn', 'depends-on'),
        serialization_alias='dependsOn',
    )


class FlowTask(BaseModel):
    """Represents a task in a legacy flow configuration."""

    id: int | str = Field(description='Unique identifier of the task')
    name: str = Field(description='Name of the task')
    phase: int | str = Field(description='ID of the phase this task belongs to')
    enabled: Optional[bool] = Field(default=True, description='Whether the task is enabled')
    continue_on_failure: Optional[bool] = Field(
        default=False,
        description='Whether to continue if task fails',
        validation_alias=AliasChoices('continue_on_failure', 'continueOnFailure', 'continue-on-failure'),
        serialization_alias='continueOnFailure',
    )
    task: dict[str, Any] = Field(description='Task configuration containing componentId, configId, etc.')


class FlowConfiguration(BaseModel):
    """Represents a complete legacy flow configuration."""

    phases: list[FlowPhase] = Field(description='List of phases in the flow')
    tasks: list[FlowTask] = Field(description='List of tasks in the flow')


# =============================================================================
# CONDITIONAL FLOW MODELS - RETRY CONFIGURATION
# NOTE: These will be removed in future iterations once we fetch the shcema from AI service
# =============================================================================


class RetryStrategyParams(BaseModel):
    """Retry strategy parameters configuration."""

    max_retries: int = Field(default=3, description='Maximum number of retry attempts', alias='maxRetries')
    delay: int = Field(default=10, description='Delay in seconds between retry attempts')


class RetryOnCondition(BaseModel):
    """Retry condition configuration."""

    type: Literal['errorMessageContains', 'errorMessageExact'] = Field(description='Type of retry condition')
    value: str = Field(description='Value to match for retry condition')


class RetryConfiguration(BaseModel):
    """Retry configuration for tasks and phases."""

    strategy: Literal['linear'] = Field(default='linear', description='Retry strategy')
    strategy_params: RetryStrategyParams = Field(
        default_factory=RetryStrategyParams, description='Strategy parameters', alias='strategyParams'
    )
    retry_on: Optional[list[RetryOnCondition]] = Field(
        default=None, description='Conditions that trigger retry', alias='retryOn'
    )


# =============================================================================
# CONDITIONAL FLOW MODELS - CONDITIONS
# =============================================================================


class TaskCondition(BaseModel):
    """Task-based condition for flow transitions."""

    type: Literal['task'] = Field(description='Condition type')
    task: str = Field(description='ID of the task to evaluate, or "*" when used with phase operators')
    value: Literal[
        'taskId',
        'phaseId',
        'status',
        'job.id',
        'job.componentId',
        'job.configId',
        'job.status',
        'job.result',
        'job.startTime',
        'job.endTime',
        'job.duration',
        'job.result.output.tables',
        'job.result.message',
    ] = Field(description='Property path to retrieve from the task context')


class PhaseCondition(BaseModel):
    """Phase-based condition for flow transitions."""

    type: Literal['phase'] = Field(description='Condition type')
    phase: str = Field(description='ID of the phase to evaluate')
    value: Literal['phaseId', 'status'] = Field(description='Property to retrieve from the phase')


class ConstantCondition(BaseModel):
    """Constant value condition."""

    type: Literal['const', 'constant'] = Field(description='Condition type')
    value: Union[str, int, bool, list] = Field(description='Constant value')


class VariableCondition(BaseModel):
    """Variable-based condition."""

    type: Literal['variable'] = Field(description='Condition type')
    value: str = Field(description='The name of the variable to evaluate')


class OperatorCondition(BaseModel):
    """Operator-based condition with operands."""

    type: Literal['operator'] = Field(description='Condition type')
    operator: Literal['AND', 'OR', 'EQUALS', 'NOT_EQUALS', 'GREATER_THAN', 'LESS_THAN', 'INCLUDES', 'CONTAINS'] = Field(
        description='Operator type'
    )
    operands: list['ConditionObject'] = Field(description='List of operand conditions')


class PhaseOperatorCondition(BaseModel):
    """Phase-specific operator condition."""

    type: Literal['operator'] = Field(description='Condition type')
    operator: Literal['ALL_TASKS_IN_PHASE', 'ANY_TASKS_IN_PHASE'] = Field(description='Phase operator type')
    phase: str = Field(description='ID of the phase to check')
    operands: list['OperatorCondition'] = Field(description='List of operand conditions')


class FunctionCondition(BaseModel):
    """Function-based condition."""

    type: Literal['function'] = Field(description='Condition type')
    function: Literal['COUNT', 'DATE'] = Field(description='Function type')
    operands: list['VariableSourceObject'] = Field(description='List of operand conditions')


class ArrayCondition(BaseModel):
    """Array-based condition."""

    type: Literal['array'] = Field(description='Condition type')
    operands: list['VariableSourceObject'] = Field(description='List of operand conditions')


# Union type for all condition types
ConditionObject = Union[
    TaskCondition,
    PhaseCondition,
    ConstantCondition,
    VariableCondition,
    OperatorCondition,
    PhaseOperatorCondition,
    FunctionCondition,
    ArrayCondition,
]


# =============================================================================
# CONDITIONAL FLOW MODELS - TASK CONFIGURATIONS
# =============================================================================


class JobTaskConfiguration(BaseModel):
    """Job task configuration."""

    type: Literal['job'] = Field(description='Task type')
    component_id: str = Field(description='Component ID', alias='componentId')
    config_id: Optional[str] = Field(default=None, description='Configuration ID', alias='configId')
    config_data: Optional[dict[str, Any]] = Field(default=None, description='Configuration data', alias='configData')
    mode: Literal['run'] = Field(description='Execution mode')
    delay: Optional[Union[str, int]] = Field(default=None, description='Initial delay in seconds')
    retry: Optional[RetryConfiguration] = Field(default=None, description='Retry configuration')


class NotificationRecipient(BaseModel):
    """Notification recipient configuration."""

    channel: Literal['email', 'webhook'] = Field(description='Channel type')
    address: str = Field(description='Recipient address (email or webhook URL)')


class NotificationTaskConfiguration(BaseModel):
    """Notification task configuration."""

    type: Literal['notification'] = Field(description='Task type')
    recipients: list[NotificationRecipient] = Field(description='List of notification recipients', min_length=1)
    title: str = Field(description='Notification title')
    message: Optional[str] = Field(default=None, description='Notification message')


# Variable source object (limited subset of conditions)
VariableSourceObject = Union[
    ConstantCondition, PhaseCondition, TaskCondition, VariableCondition, FunctionCondition, ArrayCondition
]


class VariableTaskConfiguration(BaseModel):
    """Variable task configuration."""

    type: Literal['variable'] = Field(description='Task type')
    name: str = Field(description='Variable name')
    value: Optional[str] = Field(default=None, description='Variable value')
    source: Optional[VariableSourceObject] = Field(default=None, description='Variable source')


TaskConfiguration = Union[JobTaskConfiguration, NotificationTaskConfiguration, VariableTaskConfiguration]


# =============================================================================
# CONDITIONAL FLOW MODELS - CORE STRUCTURES
# =============================================================================


class ConditionalFlowTransition(BaseModel):
    """Transition model with structured conditions."""

    id: str = Field(description='Unique identifier of the transition')
    name: Optional[str] = Field(default=None, description='Optional descriptive name for the transition')
    condition: Optional[ConditionObject] = Field(default=None, description='Structured condition for this transition')
    goto: str | None = Field(description='Target phase ID to transition to, or null to end the flow')


class ConditionalFlowTask(BaseModel):
    """Task model with structured configuration."""

    id: str = Field(description='Unique identifier of the task (must be string)')
    name: str = Field(description='Name of the task')
    phase: str = Field(description='ID of the phase this task belongs to (must be string)')
    enabled: Optional[bool] = Field(default=True, description='Whether the task is enabled')
    task: TaskConfiguration = Field(description='Structured task configuration')


class ConditionalFlowPhase(BaseModel):
    """Phase model with structured retry configuration."""

    id: str = Field(description='Unique identifier of the phase (must be string)')
    name: str = Field(description='Name of the phase', min_length=1)
    description: Optional[str] = Field(default=None, description='Description of the phase')
    retry: Optional[RetryConfiguration] = Field(
        default=None, description='Retry configuration for all tasks in this phase'
    )
    next: Optional[list[ConditionalFlowTransition]] = Field(
        default_factory=list, description='Array of transitions to other phases'
    )


class ConditionalFlowConfiguration(BaseModel):
    """Represents a complete legacy flow configuration."""

    phases: list[ConditionalFlowPhase] = Field(description='List of phases in the flow')
    tasks: list[ConditionalFlowTask] = Field(description='List of tasks in the flow')


# =============================================================================
# DOMAIN MODELS
# =============================================================================


class Flow(BaseModel):
    """Complete flow configuration with all data."""

    component_id: FlowType = Field(description='The ID of the component (keboola.orchestrator/keboola.flow)')
    configuration_id: str = Field(description='The ID of this flow configuration')
    name: str = Field(description='The name of the flow configuration')
    description: Optional[str] = Field(default=None, description='The description of the flow configuration')
    version: int = Field(description='The version of the flow configuration')
    is_disabled: bool = Field(default=False, description='Whether the flow configuration is disabled')
    is_deleted: bool = Field(default=False, description='Whether the flow configuration is deleted')
    configuration: FlowConfiguration | ConditionalFlowConfiguration = Field(
        description='The flow configuration containing phases and tasks'
    )
    change_description: Optional[str] = Field(default=None, description='The description of the latest changes')
    configuration_metadata: list[dict[str, Any]] = Field(
        default_factory=list, description='Flow configuration metadata including MCP tracking'
    )
    created: Optional[str] = Field(None, description='Creation timestamp')
    updated: Optional[str] = Field(None, description='Last update timestamp')
    links: list[Link] = Field(default_factory=list, description='MCP-specific links for UI navigation')

    @classmethod
    def from_api_response(
        cls, api_config: APIFlowResponse, flow_component_id: FlowType, links: Optional[list[Link]] = None
    ) -> 'Flow':
        """
        Create a Flow domain model from an APIFlowResponse.

        :param api_config: The APIFlowResponse instance.
        :param flow_component_id: The component ID of the flow.
        :param links: Optional list of navigation links.
        :return: Flow domain model.
        """
        is_legacy = flow_component_id == ORCHESTRATOR_COMPONENT_ID

        if is_legacy:
            phases = [FlowPhase.model_validate(p) for p in api_config.configuration.get('phases', [])]
            tasks = [FlowTask.model_validate(t) for t in api_config.configuration.get('tasks', [])]
            config = FlowConfiguration(phases=phases, tasks=tasks)
        else:
            phases = [ConditionalFlowPhase.model_validate(p) for p in api_config.configuration.get('phases', [])]
            tasks = [ConditionalFlowTask.model_validate(p) for p in api_config.configuration.get('tasks', [])]
            config = ConditionalFlowConfiguration(phases=phases, tasks=tasks)

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
            links=links or [],
        )


class FlowSummary(BaseModel):
    """Lightweight flow configuration for list operations."""

    component_id: FlowType = Field(description='The ID of the component (keboola.orchestrator/keboola.flow)')
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
    def from_api_response(cls, api_config: APIFlowResponse, flow_component_id: FlowType) -> 'FlowSummary':
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
