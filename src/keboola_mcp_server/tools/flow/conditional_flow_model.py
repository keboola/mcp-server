from typing import List, Literal, Optional, Union

from pydantic import AnyUrl, BaseModel, EmailStr, Field

# === RETRY CONFIGURATION ===


class RetryCondition(BaseModel):
    type: Literal['errorMessageContains', 'errorMessageExact']
    value: str


class RetryStrategyParams(BaseModel):
    max_retries: Optional[int] = Field(default=3, alias='maxRetries')
    delay: Optional[int] = Field(default=10, alias='delay')


class RetryConfiguration(BaseModel):
    retry_on: Optional[List[RetryCondition]] = Field(default=None, alias='retryOn')
    strategy: Optional[Literal['linear']] = Field(default='linear', alias='strategy')
    strategy_params: Optional[RetryStrategyParams] = Field(default=None, alias='strategyParams')


# === CONDITION OBJECTS ===

class ConstantCondition(BaseModel):
    type: Literal['const', 'constant']
    value: Union[str, int, float, bool, list]


class PhaseCondition(BaseModel):
    type: Literal['phase']
    phase: str
    value: str


class TaskCondition(BaseModel):
    type: Literal['task']
    task: str
    value: str


class VariableCondition(BaseModel):
    type: Literal['variable']
    value: str


class OperatorConditionBase(BaseModel):
    type: Literal['operator']
    operator: Literal['AND', 'OR', 'EQUALS', 'NOT_EQUALS', 'GREATER_THAN', 'LESS_THAN', 'INCLUDES', 'CONTAINS']
    operands: List['ConditionObject']


class OperatorPhaseCondition(BaseModel):
    type: Literal['operator']
    operator: Literal['ALL_TASKS_IN_PHASE', 'ANY_TASKS_IN_PHASE']
    phase: str
    operands: List['ConditionObject']


class FunctionCondition(BaseModel):
    type: Literal['function']
    function: Literal['COUNT', 'DATE']
    operands: List['ConditionObject']


class ArrayCondition(BaseModel):
    type: Literal['array']
    operands: List['ConditionObject']


ConditionObject = Union[
    ConstantCondition,
    PhaseCondition,
    TaskCondition,
    VariableCondition,
    OperatorConditionBase,
    OperatorPhaseCondition,
    FunctionCondition,
]

VariableSourceObject = Union[
    ConstantCondition,
    PhaseCondition,
    TaskCondition,
    FunctionCondition,
    ArrayCondition,
]


# === TASK VARIANTS ===

class JobTask(BaseModel):
    type: Literal['job']
    component_id: str = Field(..., alias='componentId')
    config_id: str = Field(..., alias='configId')
    mode: Literal['run']
    delay: Optional[Union[str, float]] = Field(default=None, alias='delay')
    retry: Optional[RetryConfiguration] = Field(default=None, alias='retry')


class EmailChannel(BaseModel):
    type: Literal['email']
    recipients: List[EmailStr]


class WebhookChannel(BaseModel):
    type: Literal['webhook']
    recipients: List[AnyUrl]


ChannelType = Union[EmailChannel, WebhookChannel]


class NotificationTask(BaseModel):
    type: Literal['notification']
    channel: ChannelType
    title: str
    message: Optional[str] = Field(default=None, alias='message')


class VariableTaskWithValue(BaseModel):
    type: Literal['variable']
    name: str
    value: str


class VariableTaskWithSource(BaseModel):
    type: Literal['variable']
    name: str
    source: VariableSourceObject


TaskObject = Union[
    JobTask,
    NotificationTask,
    VariableTaskWithValue,
    VariableTaskWithSource,
]


# === TRANSITIONS & PHASES ===

class PhaseTransition(BaseModel):
    id: str
    name: Optional[str] = Field(default=None, alias='name')
    condition: Optional[ConditionObject] = Field(default=None, alias='condition')
    goto: Optional[str] = Field(default=None, alias='goto')


class Phase(BaseModel):
    id: str
    name: str
    retry: Optional[RetryConfiguration] = Field(default=None, alias='retry')
    next_transitions: Optional[List[PhaseTransition]] = Field(default=None, alias='next')
    description: Optional[str] = Field(default=None, alias='description')


# === TASKS ===

class Task(BaseModel):
    id: str
    name: str
    phase: str
    task: TaskObject
    continue_on_failure: Optional[bool] = Field(default=None, alias='continueOnFailure')
    enabled: Optional[bool] = Field(default=None, alias='enabled')


# === ROOT ===

class ConditionalFlow(BaseModel):
    phases: List[Phase]
    tasks: List[Task]
