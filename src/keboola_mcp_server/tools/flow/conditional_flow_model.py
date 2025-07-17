from typing import List, Literal, Optional, Union

from pydantic import AnyUrl, BaseModel, EmailStr, Field

# === RETRY CONFIGURATION ===


class RetryCondition(BaseModel):
    type: Literal['errorMessageContains', 'errorMessageExact']
    value: str


class RetryStrategyParams(BaseModel):
    maxRetries: Optional[int] = 3
    delay: Optional[int] = 10


class RetryConfiguration(BaseModel):
    retryOn: Optional[List[RetryCondition]] = None
    strategy: Optional[Literal['linear']] = 'linear'
    strategyParams: Optional[RetryStrategyParams] = None


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
    componentId: str
    configId: str
    mode: Literal['run']
    delay: Optional[Union[str, float]] = None
    retry: Optional[RetryConfiguration] = None


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
    message: Optional[str] = None


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
    name: Optional[str]
    condition: Optional['ConditionObject']
    goto: Optional[str]


class Phase(BaseModel):
    id: str
    name: str
    retry: Optional['RetryConfiguration'] = None
    next_transitions: Optional[List[PhaseTransition]] = Field(default=None, alias='next')
    description: Optional[str] = None

    class Config:
        populate_by_name = True  # allows using `next_transitions` when creating programmatically


# === TASKS ===

class Task(BaseModel):
    id: str
    name: str
    phase: str
    task: TaskObject
    continueOnFailure: Optional[bool] = None
    enabled: Optional[bool] = None


# === ROOT ===

class ConditionalFlow(BaseModel):
    phases: List[Phase]
    tasks: List[Task]
