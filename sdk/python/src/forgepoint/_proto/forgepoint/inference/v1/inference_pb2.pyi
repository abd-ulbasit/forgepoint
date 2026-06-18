import datetime

from google.protobuf import timestamp_pb2 as _timestamp_pb2
from google.protobuf import duration_pb2 as _duration_pb2
from forgepoint.common.v1 import common_pb2 as _common_pb2
from forgepoint.events.v1 import events_pb2 as _events_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class DataType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    DATA_TYPE_UNSPECIFIED: _ClassVar[DataType]
    DATA_TYPE_FLOAT32: _ClassVar[DataType]
    DATA_TYPE_FLOAT64: _ClassVar[DataType]
    DATA_TYPE_INT32: _ClassVar[DataType]
    DATA_TYPE_INT64: _ClassVar[DataType]
    DATA_TYPE_STRING: _ClassVar[DataType]
    DATA_TYPE_BOOL: _ClassVar[DataType]

class TargetStatus(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    TARGET_STATUS_UNSPECIFIED: _ClassVar[TargetStatus]
    TARGET_STATUS_ACTIVE: _ClassVar[TargetStatus]
    TARGET_STATUS_DRAINING: _ClassVar[TargetStatus]
    TARGET_STATUS_UNHEALTHY: _ClassVar[TargetStatus]

class CircuitBreakerState(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    CIRCUIT_BREAKER_STATE_UNSPECIFIED: _ClassVar[CircuitBreakerState]
    CIRCUIT_BREAKER_STATE_CLOSED: _ClassVar[CircuitBreakerState]
    CIRCUIT_BREAKER_STATE_OPEN: _ClassVar[CircuitBreakerState]
    CIRCUIT_BREAKER_STATE_HALF_OPEN: _ClassVar[CircuitBreakerState]
DATA_TYPE_UNSPECIFIED: DataType
DATA_TYPE_FLOAT32: DataType
DATA_TYPE_FLOAT64: DataType
DATA_TYPE_INT32: DataType
DATA_TYPE_INT64: DataType
DATA_TYPE_STRING: DataType
DATA_TYPE_BOOL: DataType
TARGET_STATUS_UNSPECIFIED: TargetStatus
TARGET_STATUS_ACTIVE: TargetStatus
TARGET_STATUS_DRAINING: TargetStatus
TARGET_STATUS_UNHEALTHY: TargetStatus
CIRCUIT_BREAKER_STATE_UNSPECIFIED: CircuitBreakerState
CIRCUIT_BREAKER_STATE_CLOSED: CircuitBreakerState
CIRCUIT_BREAKER_STATE_OPEN: CircuitBreakerState
CIRCUIT_BREAKER_STATE_HALF_OPEN: CircuitBreakerState

class TensorData(_message.Message):
    __slots__ = ("shape", "dtype", "data")
    SHAPE_FIELD_NUMBER: _ClassVar[int]
    DTYPE_FIELD_NUMBER: _ClassVar[int]
    DATA_FIELD_NUMBER: _ClassVar[int]
    shape: _containers.RepeatedScalarFieldContainer[int]
    dtype: DataType
    data: bytes
    def __init__(self, shape: _Optional[_Iterable[int]] = ..., dtype: _Optional[_Union[DataType, str]] = ..., data: _Optional[bytes] = ...) -> None: ...

class RouteTarget(_message.Message):
    __slots__ = ("version", "endpoint", "weight_bps", "status")
    VERSION_FIELD_NUMBER: _ClassVar[int]
    ENDPOINT_FIELD_NUMBER: _ClassVar[int]
    WEIGHT_BPS_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    version: str
    endpoint: str
    weight_bps: int
    status: TargetStatus
    def __init__(self, version: _Optional[str] = ..., endpoint: _Optional[str] = ..., weight_bps: _Optional[int] = ..., status: _Optional[_Union[TargetStatus, str]] = ...) -> None: ...

class Route(_message.Message):
    __slots__ = ("model_name", "targets", "updated_at")
    MODEL_NAME_FIELD_NUMBER: _ClassVar[int]
    TARGETS_FIELD_NUMBER: _ClassVar[int]
    UPDATED_AT_FIELD_NUMBER: _ClassVar[int]
    model_name: str
    targets: _containers.RepeatedCompositeFieldContainer[RouteTarget]
    updated_at: _timestamp_pb2.Timestamp
    def __init__(self, model_name: _Optional[str] = ..., targets: _Optional[_Iterable[_Union[RouteTarget, _Mapping]]] = ..., updated_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class CircuitState(_message.Message):
    __slots__ = ("model_name", "version", "state", "consecutive_failures", "last_transition_at")
    MODEL_NAME_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    STATE_FIELD_NUMBER: _ClassVar[int]
    CONSECUTIVE_FAILURES_FIELD_NUMBER: _ClassVar[int]
    LAST_TRANSITION_AT_FIELD_NUMBER: _ClassVar[int]
    model_name: str
    version: str
    state: CircuitBreakerState
    consecutive_failures: int
    last_transition_at: _timestamp_pb2.Timestamp
    def __init__(self, model_name: _Optional[str] = ..., version: _Optional[str] = ..., state: _Optional[_Union[CircuitBreakerState, str]] = ..., consecutive_failures: _Optional[int] = ..., last_transition_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class PredictRequest(_message.Message):
    __slots__ = ("model_name", "inputs", "version_override", "idempotency_key")
    class InputsEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: TensorData
        def __init__(self, key: _Optional[str] = ..., value: _Optional[_Union[TensorData, _Mapping]] = ...) -> None: ...
    MODEL_NAME_FIELD_NUMBER: _ClassVar[int]
    INPUTS_FIELD_NUMBER: _ClassVar[int]
    VERSION_OVERRIDE_FIELD_NUMBER: _ClassVar[int]
    IDEMPOTENCY_KEY_FIELD_NUMBER: _ClassVar[int]
    model_name: str
    inputs: _containers.MessageMap[str, TensorData]
    version_override: str
    idempotency_key: str
    def __init__(self, model_name: _Optional[str] = ..., inputs: _Optional[_Mapping[str, TensorData]] = ..., version_override: _Optional[str] = ..., idempotency_key: _Optional[str] = ...) -> None: ...

class PredictResponse(_message.Message):
    __slots__ = ("outputs", "served_version", "latency", "request_id")
    class OutputsEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: TensorData
        def __init__(self, key: _Optional[str] = ..., value: _Optional[_Union[TensorData, _Mapping]] = ...) -> None: ...
    OUTPUTS_FIELD_NUMBER: _ClassVar[int]
    SERVED_VERSION_FIELD_NUMBER: _ClassVar[int]
    LATENCY_FIELD_NUMBER: _ClassVar[int]
    REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
    outputs: _containers.MessageMap[str, TensorData]
    served_version: str
    latency: _duration_pb2.Duration
    request_id: str
    def __init__(self, outputs: _Optional[_Mapping[str, TensorData]] = ..., served_version: _Optional[str] = ..., latency: _Optional[_Union[datetime.timedelta, _duration_pb2.Duration, _Mapping]] = ..., request_id: _Optional[str] = ...) -> None: ...

class BatchPredictRequest(_message.Message):
    __slots__ = ("model_name", "items", "version_override", "idempotency_key")
    MODEL_NAME_FIELD_NUMBER: _ClassVar[int]
    ITEMS_FIELD_NUMBER: _ClassVar[int]
    VERSION_OVERRIDE_FIELD_NUMBER: _ClassVar[int]
    IDEMPOTENCY_KEY_FIELD_NUMBER: _ClassVar[int]
    model_name: str
    items: _containers.RepeatedCompositeFieldContainer[BatchPredictItem]
    version_override: str
    idempotency_key: str
    def __init__(self, model_name: _Optional[str] = ..., items: _Optional[_Iterable[_Union[BatchPredictItem, _Mapping]]] = ..., version_override: _Optional[str] = ..., idempotency_key: _Optional[str] = ...) -> None: ...

class BatchPredictItem(_message.Message):
    __slots__ = ("item_id", "inputs")
    class InputsEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: TensorData
        def __init__(self, key: _Optional[str] = ..., value: _Optional[_Union[TensorData, _Mapping]] = ...) -> None: ...
    ITEM_ID_FIELD_NUMBER: _ClassVar[int]
    INPUTS_FIELD_NUMBER: _ClassVar[int]
    item_id: str
    inputs: _containers.MessageMap[str, TensorData]
    def __init__(self, item_id: _Optional[str] = ..., inputs: _Optional[_Mapping[str, TensorData]] = ...) -> None: ...

class BatchPredictResponse(_message.Message):
    __slots__ = ("results", "request_id")
    RESULTS_FIELD_NUMBER: _ClassVar[int]
    REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
    results: _containers.RepeatedCompositeFieldContainer[BatchPredictResult]
    request_id: str
    def __init__(self, results: _Optional[_Iterable[_Union[BatchPredictResult, _Mapping]]] = ..., request_id: _Optional[str] = ...) -> None: ...

class BatchPredictResult(_message.Message):
    __slots__ = ("item_id", "outputs", "served_version", "error", "failure_reason")
    class OutputsEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: TensorData
        def __init__(self, key: _Optional[str] = ..., value: _Optional[_Union[TensorData, _Mapping]] = ...) -> None: ...
    ITEM_ID_FIELD_NUMBER: _ClassVar[int]
    OUTPUTS_FIELD_NUMBER: _ClassVar[int]
    SERVED_VERSION_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    FAILURE_REASON_FIELD_NUMBER: _ClassVar[int]
    item_id: str
    outputs: _containers.MessageMap[str, TensorData]
    served_version: str
    error: _common_pb2.ErrorDetail
    failure_reason: _events_pb2.InferenceFailureReason
    def __init__(self, item_id: _Optional[str] = ..., outputs: _Optional[_Mapping[str, TensorData]] = ..., served_version: _Optional[str] = ..., error: _Optional[_Union[_common_pb2.ErrorDetail, _Mapping]] = ..., failure_reason: _Optional[_Union[_events_pb2.InferenceFailureReason, str]] = ...) -> None: ...

class StreamPredictRequest(_message.Message):
    __slots__ = ("model_name", "items", "version_override", "idempotency_key")
    MODEL_NAME_FIELD_NUMBER: _ClassVar[int]
    ITEMS_FIELD_NUMBER: _ClassVar[int]
    VERSION_OVERRIDE_FIELD_NUMBER: _ClassVar[int]
    IDEMPOTENCY_KEY_FIELD_NUMBER: _ClassVar[int]
    model_name: str
    items: _containers.RepeatedCompositeFieldContainer[BatchPredictItem]
    version_override: str
    idempotency_key: str
    def __init__(self, model_name: _Optional[str] = ..., items: _Optional[_Iterable[_Union[BatchPredictItem, _Mapping]]] = ..., version_override: _Optional[str] = ..., idempotency_key: _Optional[str] = ...) -> None: ...

class StreamPredictResponse(_message.Message):
    __slots__ = ("result", "request_id")
    RESULT_FIELD_NUMBER: _ClassVar[int]
    REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
    result: BatchPredictResult
    request_id: str
    def __init__(self, result: _Optional[_Union[BatchPredictResult, _Mapping]] = ..., request_id: _Optional[str] = ...) -> None: ...

class GetRouteRequest(_message.Message):
    __slots__ = ("model_name",)
    MODEL_NAME_FIELD_NUMBER: _ClassVar[int]
    model_name: str
    def __init__(self, model_name: _Optional[str] = ...) -> None: ...

class GetRouteResponse(_message.Message):
    __slots__ = ("route",)
    ROUTE_FIELD_NUMBER: _ClassVar[int]
    route: Route
    def __init__(self, route: _Optional[_Union[Route, _Mapping]] = ...) -> None: ...

class ListRoutesRequest(_message.Message):
    __slots__ = ("pagination",)
    PAGINATION_FIELD_NUMBER: _ClassVar[int]
    pagination: _common_pb2.PaginationRequest
    def __init__(self, pagination: _Optional[_Union[_common_pb2.PaginationRequest, _Mapping]] = ...) -> None: ...

class ListRoutesResponse(_message.Message):
    __slots__ = ("routes", "pagination")
    ROUTES_FIELD_NUMBER: _ClassVar[int]
    PAGINATION_FIELD_NUMBER: _ClassVar[int]
    routes: _containers.RepeatedCompositeFieldContainer[Route]
    pagination: _common_pb2.PaginationResponse
    def __init__(self, routes: _Optional[_Iterable[_Union[Route, _Mapping]]] = ..., pagination: _Optional[_Union[_common_pb2.PaginationResponse, _Mapping]] = ...) -> None: ...

class UpsertRouteRequest(_message.Message):
    __slots__ = ("model_name", "targets", "idempotency_key")
    MODEL_NAME_FIELD_NUMBER: _ClassVar[int]
    TARGETS_FIELD_NUMBER: _ClassVar[int]
    IDEMPOTENCY_KEY_FIELD_NUMBER: _ClassVar[int]
    model_name: str
    targets: _containers.RepeatedCompositeFieldContainer[RouteTarget]
    idempotency_key: str
    def __init__(self, model_name: _Optional[str] = ..., targets: _Optional[_Iterable[_Union[RouteTarget, _Mapping]]] = ..., idempotency_key: _Optional[str] = ...) -> None: ...

class UpsertRouteResponse(_message.Message):
    __slots__ = ("route",)
    ROUTE_FIELD_NUMBER: _ClassVar[int]
    route: Route
    def __init__(self, route: _Optional[_Union[Route, _Mapping]] = ...) -> None: ...

class SetTrafficSplitRequest(_message.Message):
    __slots__ = ("model_name", "weights", "idempotency_key")
    MODEL_NAME_FIELD_NUMBER: _ClassVar[int]
    WEIGHTS_FIELD_NUMBER: _ClassVar[int]
    IDEMPOTENCY_KEY_FIELD_NUMBER: _ClassVar[int]
    model_name: str
    weights: _containers.RepeatedCompositeFieldContainer[TrafficWeight]
    idempotency_key: str
    def __init__(self, model_name: _Optional[str] = ..., weights: _Optional[_Iterable[_Union[TrafficWeight, _Mapping]]] = ..., idempotency_key: _Optional[str] = ...) -> None: ...

class TrafficWeight(_message.Message):
    __slots__ = ("version", "weight_bps")
    VERSION_FIELD_NUMBER: _ClassVar[int]
    WEIGHT_BPS_FIELD_NUMBER: _ClassVar[int]
    version: str
    weight_bps: int
    def __init__(self, version: _Optional[str] = ..., weight_bps: _Optional[int] = ...) -> None: ...

class SetTrafficSplitResponse(_message.Message):
    __slots__ = ("route",)
    ROUTE_FIELD_NUMBER: _ClassVar[int]
    route: Route
    def __init__(self, route: _Optional[_Union[Route, _Mapping]] = ...) -> None: ...

class DeleteRouteRequest(_message.Message):
    __slots__ = ("model_name", "idempotency_key")
    MODEL_NAME_FIELD_NUMBER: _ClassVar[int]
    IDEMPOTENCY_KEY_FIELD_NUMBER: _ClassVar[int]
    model_name: str
    idempotency_key: str
    def __init__(self, model_name: _Optional[str] = ..., idempotency_key: _Optional[str] = ...) -> None: ...

class DeleteRouteResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class GetCircuitStateRequest(_message.Message):
    __slots__ = ("model_name", "version")
    MODEL_NAME_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    model_name: str
    version: str
    def __init__(self, model_name: _Optional[str] = ..., version: _Optional[str] = ...) -> None: ...

class GetCircuitStateResponse(_message.Message):
    __slots__ = ("circuit_state",)
    CIRCUIT_STATE_FIELD_NUMBER: _ClassVar[int]
    circuit_state: CircuitState
    def __init__(self, circuit_state: _Optional[_Union[CircuitState, _Mapping]] = ...) -> None: ...

class ListCircuitStatesRequest(_message.Message):
    __slots__ = ("model_name_filter", "pagination")
    MODEL_NAME_FILTER_FIELD_NUMBER: _ClassVar[int]
    PAGINATION_FIELD_NUMBER: _ClassVar[int]
    model_name_filter: str
    pagination: _common_pb2.PaginationRequest
    def __init__(self, model_name_filter: _Optional[str] = ..., pagination: _Optional[_Union[_common_pb2.PaginationRequest, _Mapping]] = ...) -> None: ...

class ListCircuitStatesResponse(_message.Message):
    __slots__ = ("circuit_states", "pagination")
    CIRCUIT_STATES_FIELD_NUMBER: _ClassVar[int]
    PAGINATION_FIELD_NUMBER: _ClassVar[int]
    circuit_states: _containers.RepeatedCompositeFieldContainer[CircuitState]
    pagination: _common_pb2.PaginationResponse
    def __init__(self, circuit_states: _Optional[_Iterable[_Union[CircuitState, _Mapping]]] = ..., pagination: _Optional[_Union[_common_pb2.PaginationResponse, _Mapping]] = ...) -> None: ...

class TensorSpec(_message.Message):
    __slots__ = ("name", "dtype", "shape")
    NAME_FIELD_NUMBER: _ClassVar[int]
    DTYPE_FIELD_NUMBER: _ClassVar[int]
    SHAPE_FIELD_NUMBER: _ClassVar[int]
    name: str
    dtype: DataType
    shape: _containers.RepeatedScalarFieldContainer[int]
    def __init__(self, name: _Optional[str] = ..., dtype: _Optional[_Union[DataType, str]] = ..., shape: _Optional[_Iterable[int]] = ...) -> None: ...

class VersionInfo(_message.Message):
    __slots__ = ("version", "weight_bps", "is_stable")
    VERSION_FIELD_NUMBER: _ClassVar[int]
    WEIGHT_BPS_FIELD_NUMBER: _ClassVar[int]
    IS_STABLE_FIELD_NUMBER: _ClassVar[int]
    version: str
    weight_bps: int
    is_stable: bool
    def __init__(self, version: _Optional[str] = ..., weight_bps: _Optional[int] = ..., is_stable: _Optional[bool] = ...) -> None: ...

class GetModelInfoRequest(_message.Message):
    __slots__ = ("model_name",)
    MODEL_NAME_FIELD_NUMBER: _ClassVar[int]
    model_name: str
    def __init__(self, model_name: _Optional[str] = ...) -> None: ...

class GetModelInfoResponse(_message.Message):
    __slots__ = ("model_info",)
    MODEL_INFO_FIELD_NUMBER: _ClassVar[int]
    model_info: ModelInfo
    def __init__(self, model_info: _Optional[_Union[ModelInfo, _Mapping]] = ...) -> None: ...

class ModelInfo(_message.Message):
    __slots__ = ("model_name", "is_serving", "inputs", "outputs", "versions", "updated_at")
    MODEL_NAME_FIELD_NUMBER: _ClassVar[int]
    IS_SERVING_FIELD_NUMBER: _ClassVar[int]
    INPUTS_FIELD_NUMBER: _ClassVar[int]
    OUTPUTS_FIELD_NUMBER: _ClassVar[int]
    VERSIONS_FIELD_NUMBER: _ClassVar[int]
    UPDATED_AT_FIELD_NUMBER: _ClassVar[int]
    model_name: str
    is_serving: bool
    inputs: _containers.RepeatedCompositeFieldContainer[TensorSpec]
    outputs: _containers.RepeatedCompositeFieldContainer[TensorSpec]
    versions: _containers.RepeatedCompositeFieldContainer[VersionInfo]
    updated_at: _timestamp_pb2.Timestamp
    def __init__(self, model_name: _Optional[str] = ..., is_serving: _Optional[bool] = ..., inputs: _Optional[_Iterable[_Union[TensorSpec, _Mapping]]] = ..., outputs: _Optional[_Iterable[_Union[TensorSpec, _Mapping]]] = ..., versions: _Optional[_Iterable[_Union[VersionInfo, _Mapping]]] = ..., updated_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...
