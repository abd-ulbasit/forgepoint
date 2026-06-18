import datetime

from google.protobuf import timestamp_pb2 as _timestamp_pb2
from google.protobuf import duration_pb2 as _duration_pb2
from google.protobuf import struct_pb2 as _struct_pb2
from forgepoint.common.v1 import common_pb2 as _common_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class PipelineType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    PIPELINE_TYPE_UNSPECIFIED: _ClassVar[PipelineType]
    PIPELINE_TYPE_DEPLOYMENT_SAGA: _ClassVar[PipelineType]
    PIPELINE_TYPE_TRAINING_DAG: _ClassVar[PipelineType]
    PIPELINE_TYPE_BATCH_INFERENCE: _ClassVar[PipelineType]

class StepType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    STEP_TYPE_UNSPECIFIED: _ClassVar[StepType]
    STEP_TYPE_VALIDATE: _ClassVar[StepType]
    STEP_TYPE_BUILD: _ClassVar[StepType]
    STEP_TYPE_DEPLOY: _ClassVar[StepType]
    STEP_TYPE_CANARY: _ClassVar[StepType]
    STEP_TYPE_PROMOTE: _ClassVar[StepType]
    STEP_TYPE_TRAIN: _ClassVar[StepType]
    STEP_TYPE_EVALUATE: _ClassVar[StepType]
    STEP_TYPE_REGISTER: _ClassVar[StepType]
    STEP_TYPE_CUSTOM: _ClassVar[StepType]

class ExecutionStatus(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    EXECUTION_STATUS_UNSPECIFIED: _ClassVar[ExecutionStatus]
    EXECUTION_STATUS_PENDING: _ClassVar[ExecutionStatus]
    EXECUTION_STATUS_RUNNING: _ClassVar[ExecutionStatus]
    EXECUTION_STATUS_COMPENSATING: _ClassVar[ExecutionStatus]
    EXECUTION_STATUS_COMPLETED: _ClassVar[ExecutionStatus]
    EXECUTION_STATUS_FAILED: _ClassVar[ExecutionStatus]
    EXECUTION_STATUS_CANCELLED: _ClassVar[ExecutionStatus]

class StepStatus(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    STEP_STATUS_UNSPECIFIED: _ClassVar[StepStatus]
    STEP_STATUS_PENDING: _ClassVar[StepStatus]
    STEP_STATUS_RUNNING: _ClassVar[StepStatus]
    STEP_STATUS_COMPLETED: _ClassVar[StepStatus]
    STEP_STATUS_FAILED: _ClassVar[StepStatus]
    STEP_STATUS_SKIPPED: _ClassVar[StepStatus]
    STEP_STATUS_COMPENSATING: _ClassVar[StepStatus]
    STEP_STATUS_COMPENSATED: _ClassVar[StepStatus]
    STEP_STATUS_COMPENSATION_FAILED: _ClassVar[StepStatus]
PIPELINE_TYPE_UNSPECIFIED: PipelineType
PIPELINE_TYPE_DEPLOYMENT_SAGA: PipelineType
PIPELINE_TYPE_TRAINING_DAG: PipelineType
PIPELINE_TYPE_BATCH_INFERENCE: PipelineType
STEP_TYPE_UNSPECIFIED: StepType
STEP_TYPE_VALIDATE: StepType
STEP_TYPE_BUILD: StepType
STEP_TYPE_DEPLOY: StepType
STEP_TYPE_CANARY: StepType
STEP_TYPE_PROMOTE: StepType
STEP_TYPE_TRAIN: StepType
STEP_TYPE_EVALUATE: StepType
STEP_TYPE_REGISTER: StepType
STEP_TYPE_CUSTOM: StepType
EXECUTION_STATUS_UNSPECIFIED: ExecutionStatus
EXECUTION_STATUS_PENDING: ExecutionStatus
EXECUTION_STATUS_RUNNING: ExecutionStatus
EXECUTION_STATUS_COMPENSATING: ExecutionStatus
EXECUTION_STATUS_COMPLETED: ExecutionStatus
EXECUTION_STATUS_FAILED: ExecutionStatus
EXECUTION_STATUS_CANCELLED: ExecutionStatus
STEP_STATUS_UNSPECIFIED: StepStatus
STEP_STATUS_PENDING: StepStatus
STEP_STATUS_RUNNING: StepStatus
STEP_STATUS_COMPLETED: StepStatus
STEP_STATUS_FAILED: StepStatus
STEP_STATUS_SKIPPED: StepStatus
STEP_STATUS_COMPENSATING: StepStatus
STEP_STATUS_COMPENSATED: StepStatus
STEP_STATUS_COMPENSATION_FAILED: StepStatus

class StepDefinition(_message.Message):
    __slots__ = ("id", "name", "type", "depends_on", "compensation_step_id", "config", "timeout", "max_retries")
    ID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    TYPE_FIELD_NUMBER: _ClassVar[int]
    DEPENDS_ON_FIELD_NUMBER: _ClassVar[int]
    COMPENSATION_STEP_ID_FIELD_NUMBER: _ClassVar[int]
    CONFIG_FIELD_NUMBER: _ClassVar[int]
    TIMEOUT_FIELD_NUMBER: _ClassVar[int]
    MAX_RETRIES_FIELD_NUMBER: _ClassVar[int]
    id: str
    name: str
    type: StepType
    depends_on: _containers.RepeatedScalarFieldContainer[str]
    compensation_step_id: str
    config: _struct_pb2.Struct
    timeout: _duration_pb2.Duration
    max_retries: int
    def __init__(self, id: _Optional[str] = ..., name: _Optional[str] = ..., type: _Optional[_Union[StepType, str]] = ..., depends_on: _Optional[_Iterable[str]] = ..., compensation_step_id: _Optional[str] = ..., config: _Optional[_Union[_struct_pb2.Struct, _Mapping]] = ..., timeout: _Optional[_Union[datetime.timedelta, _duration_pb2.Duration, _Mapping]] = ..., max_retries: _Optional[int] = ...) -> None: ...

class PipelineDefinition(_message.Message):
    __slots__ = ("id", "name", "type", "steps", "created_by", "created_at", "team")
    ID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    TYPE_FIELD_NUMBER: _ClassVar[int]
    STEPS_FIELD_NUMBER: _ClassVar[int]
    CREATED_BY_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_FIELD_NUMBER: _ClassVar[int]
    TEAM_FIELD_NUMBER: _ClassVar[int]
    id: str
    name: str
    type: PipelineType
    steps: _containers.RepeatedCompositeFieldContainer[StepDefinition]
    created_by: str
    created_at: _timestamp_pb2.Timestamp
    team: str
    def __init__(self, id: _Optional[str] = ..., name: _Optional[str] = ..., type: _Optional[_Union[PipelineType, str]] = ..., steps: _Optional[_Iterable[_Union[StepDefinition, _Mapping]]] = ..., created_by: _Optional[str] = ..., created_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., team: _Optional[str] = ...) -> None: ...

class StepExecution(_message.Message):
    __slots__ = ("id", "execution_id", "step_id", "status", "started_at", "completed_at", "output", "error", "attempt")
    ID_FIELD_NUMBER: _ClassVar[int]
    EXECUTION_ID_FIELD_NUMBER: _ClassVar[int]
    STEP_ID_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    STARTED_AT_FIELD_NUMBER: _ClassVar[int]
    COMPLETED_AT_FIELD_NUMBER: _ClassVar[int]
    OUTPUT_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    ATTEMPT_FIELD_NUMBER: _ClassVar[int]
    id: str
    execution_id: str
    step_id: str
    status: StepStatus
    started_at: _timestamp_pb2.Timestamp
    completed_at: _timestamp_pb2.Timestamp
    output: _struct_pb2.Struct
    error: str
    attempt: int
    def __init__(self, id: _Optional[str] = ..., execution_id: _Optional[str] = ..., step_id: _Optional[str] = ..., status: _Optional[_Union[StepStatus, str]] = ..., started_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., completed_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., output: _Optional[_Union[_struct_pb2.Struct, _Mapping]] = ..., error: _Optional[str] = ..., attempt: _Optional[int] = ...) -> None: ...

class Execution(_message.Message):
    __slots__ = ("id", "pipeline_id", "status", "current_step", "step_executions", "triggered_by", "started_at", "completed_at", "input", "error")
    ID_FIELD_NUMBER: _ClassVar[int]
    PIPELINE_ID_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    CURRENT_STEP_FIELD_NUMBER: _ClassVar[int]
    STEP_EXECUTIONS_FIELD_NUMBER: _ClassVar[int]
    TRIGGERED_BY_FIELD_NUMBER: _ClassVar[int]
    STARTED_AT_FIELD_NUMBER: _ClassVar[int]
    COMPLETED_AT_FIELD_NUMBER: _ClassVar[int]
    INPUT_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    id: str
    pipeline_id: str
    status: ExecutionStatus
    current_step: str
    step_executions: _containers.RepeatedCompositeFieldContainer[StepExecution]
    triggered_by: str
    started_at: _timestamp_pb2.Timestamp
    completed_at: _timestamp_pb2.Timestamp
    input: _struct_pb2.Struct
    error: str
    def __init__(self, id: _Optional[str] = ..., pipeline_id: _Optional[str] = ..., status: _Optional[_Union[ExecutionStatus, str]] = ..., current_step: _Optional[str] = ..., step_executions: _Optional[_Iterable[_Union[StepExecution, _Mapping]]] = ..., triggered_by: _Optional[str] = ..., started_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., completed_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., input: _Optional[_Union[_struct_pb2.Struct, _Mapping]] = ..., error: _Optional[str] = ...) -> None: ...

class CreatePipelineRequest(_message.Message):
    __slots__ = ("name", "type", "steps", "idempotency_key")
    NAME_FIELD_NUMBER: _ClassVar[int]
    TYPE_FIELD_NUMBER: _ClassVar[int]
    STEPS_FIELD_NUMBER: _ClassVar[int]
    IDEMPOTENCY_KEY_FIELD_NUMBER: _ClassVar[int]
    name: str
    type: PipelineType
    steps: _containers.RepeatedCompositeFieldContainer[StepDefinition]
    idempotency_key: str
    def __init__(self, name: _Optional[str] = ..., type: _Optional[_Union[PipelineType, str]] = ..., steps: _Optional[_Iterable[_Union[StepDefinition, _Mapping]]] = ..., idempotency_key: _Optional[str] = ...) -> None: ...

class CreatePipelineResponse(_message.Message):
    __slots__ = ("pipeline",)
    PIPELINE_FIELD_NUMBER: _ClassVar[int]
    pipeline: PipelineDefinition
    def __init__(self, pipeline: _Optional[_Union[PipelineDefinition, _Mapping]] = ...) -> None: ...

class TriggerExecutionRequest(_message.Message):
    __slots__ = ("pipeline_id", "input", "idempotency_key")
    PIPELINE_ID_FIELD_NUMBER: _ClassVar[int]
    INPUT_FIELD_NUMBER: _ClassVar[int]
    IDEMPOTENCY_KEY_FIELD_NUMBER: _ClassVar[int]
    pipeline_id: str
    input: _struct_pb2.Struct
    idempotency_key: str
    def __init__(self, pipeline_id: _Optional[str] = ..., input: _Optional[_Union[_struct_pb2.Struct, _Mapping]] = ..., idempotency_key: _Optional[str] = ...) -> None: ...

class TriggerExecutionResponse(_message.Message):
    __slots__ = ("execution",)
    EXECUTION_FIELD_NUMBER: _ClassVar[int]
    execution: Execution
    def __init__(self, execution: _Optional[_Union[Execution, _Mapping]] = ...) -> None: ...

class GetExecutionRequest(_message.Message):
    __slots__ = ("execution_id",)
    EXECUTION_ID_FIELD_NUMBER: _ClassVar[int]
    execution_id: str
    def __init__(self, execution_id: _Optional[str] = ...) -> None: ...

class GetExecutionResponse(_message.Message):
    __slots__ = ("execution",)
    EXECUTION_FIELD_NUMBER: _ClassVar[int]
    execution: Execution
    def __init__(self, execution: _Optional[_Union[Execution, _Mapping]] = ...) -> None: ...

class WatchExecutionRequest(_message.Message):
    __slots__ = ("execution_id", "include_current_state")
    EXECUTION_ID_FIELD_NUMBER: _ClassVar[int]
    INCLUDE_CURRENT_STATE_FIELD_NUMBER: _ClassVar[int]
    execution_id: str
    include_current_state: bool
    def __init__(self, execution_id: _Optional[str] = ..., include_current_state: _Optional[bool] = ...) -> None: ...

class WatchExecutionResponse(_message.Message):
    __slots__ = ("execution", "changed_step", "sequence", "emitted_at")
    EXECUTION_FIELD_NUMBER: _ClassVar[int]
    CHANGED_STEP_FIELD_NUMBER: _ClassVar[int]
    SEQUENCE_FIELD_NUMBER: _ClassVar[int]
    EMITTED_AT_FIELD_NUMBER: _ClassVar[int]
    execution: Execution
    changed_step: StepExecution
    sequence: int
    emitted_at: _timestamp_pb2.Timestamp
    def __init__(self, execution: _Optional[_Union[Execution, _Mapping]] = ..., changed_step: _Optional[_Union[StepExecution, _Mapping]] = ..., sequence: _Optional[int] = ..., emitted_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class CancelExecutionRequest(_message.Message):
    __slots__ = ("execution_id", "reason")
    EXECUTION_ID_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    execution_id: str
    reason: str
    def __init__(self, execution_id: _Optional[str] = ..., reason: _Optional[str] = ...) -> None: ...

class CancelExecutionResponse(_message.Message):
    __slots__ = ("execution",)
    EXECUTION_FIELD_NUMBER: _ClassVar[int]
    execution: Execution
    def __init__(self, execution: _Optional[_Union[Execution, _Mapping]] = ...) -> None: ...

class ListExecutionsRequest(_message.Message):
    __slots__ = ("pipeline_id", "status_filter", "pagination")
    PIPELINE_ID_FIELD_NUMBER: _ClassVar[int]
    STATUS_FILTER_FIELD_NUMBER: _ClassVar[int]
    PAGINATION_FIELD_NUMBER: _ClassVar[int]
    pipeline_id: str
    status_filter: ExecutionStatus
    pagination: _common_pb2.PaginationRequest
    def __init__(self, pipeline_id: _Optional[str] = ..., status_filter: _Optional[_Union[ExecutionStatus, str]] = ..., pagination: _Optional[_Union[_common_pb2.PaginationRequest, _Mapping]] = ...) -> None: ...

class ListExecutionsResponse(_message.Message):
    __slots__ = ("executions", "pagination")
    EXECUTIONS_FIELD_NUMBER: _ClassVar[int]
    PAGINATION_FIELD_NUMBER: _ClassVar[int]
    executions: _containers.RepeatedCompositeFieldContainer[Execution]
    pagination: _common_pb2.PaginationResponse
    def __init__(self, executions: _Optional[_Iterable[_Union[Execution, _Mapping]]] = ..., pagination: _Optional[_Union[_common_pb2.PaginationResponse, _Mapping]] = ...) -> None: ...

class ListPipelinesRequest(_message.Message):
    __slots__ = ("type_filter", "pagination")
    TYPE_FILTER_FIELD_NUMBER: _ClassVar[int]
    PAGINATION_FIELD_NUMBER: _ClassVar[int]
    type_filter: PipelineType
    pagination: _common_pb2.PaginationRequest
    def __init__(self, type_filter: _Optional[_Union[PipelineType, str]] = ..., pagination: _Optional[_Union[_common_pb2.PaginationRequest, _Mapping]] = ...) -> None: ...

class ListPipelinesResponse(_message.Message):
    __slots__ = ("pipelines", "pagination")
    PIPELINES_FIELD_NUMBER: _ClassVar[int]
    PAGINATION_FIELD_NUMBER: _ClassVar[int]
    pipelines: _containers.RepeatedCompositeFieldContainer[PipelineDefinition]
    pagination: _common_pb2.PaginationResponse
    def __init__(self, pipelines: _Optional[_Iterable[_Union[PipelineDefinition, _Mapping]]] = ..., pagination: _Optional[_Union[_common_pb2.PaginationResponse, _Mapping]] = ...) -> None: ...

class GetPipelineRequest(_message.Message):
    __slots__ = ("pipeline_id",)
    PIPELINE_ID_FIELD_NUMBER: _ClassVar[int]
    pipeline_id: str
    def __init__(self, pipeline_id: _Optional[str] = ...) -> None: ...

class GetPipelineResponse(_message.Message):
    __slots__ = ("pipeline",)
    PIPELINE_FIELD_NUMBER: _ClassVar[int]
    pipeline: PipelineDefinition
    def __init__(self, pipeline: _Optional[_Union[PipelineDefinition, _Mapping]] = ...) -> None: ...

class UpdatePipelineRequest(_message.Message):
    __slots__ = ("pipeline_id", "name", "steps")
    PIPELINE_ID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    STEPS_FIELD_NUMBER: _ClassVar[int]
    pipeline_id: str
    name: str
    steps: _containers.RepeatedCompositeFieldContainer[StepDefinition]
    def __init__(self, pipeline_id: _Optional[str] = ..., name: _Optional[str] = ..., steps: _Optional[_Iterable[_Union[StepDefinition, _Mapping]]] = ...) -> None: ...

class UpdatePipelineResponse(_message.Message):
    __slots__ = ("pipeline",)
    PIPELINE_FIELD_NUMBER: _ClassVar[int]
    pipeline: PipelineDefinition
    def __init__(self, pipeline: _Optional[_Union[PipelineDefinition, _Mapping]] = ...) -> None: ...

class DeletePipelineRequest(_message.Message):
    __slots__ = ("pipeline_id",)
    PIPELINE_ID_FIELD_NUMBER: _ClassVar[int]
    pipeline_id: str
    def __init__(self, pipeline_id: _Optional[str] = ...) -> None: ...

class DeletePipelineResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...
