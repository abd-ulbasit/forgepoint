import datetime

from google.protobuf import timestamp_pb2 as _timestamp_pb2
from google.protobuf import duration_pb2 as _duration_pb2
from forgepoint.common.v1 import common_pb2 as _common_pb2
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
    DATA_TYPE_BOOL: _ClassVar[DataType]
    DATA_TYPE_STRING: _ClassVar[DataType]

class ModelState(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    MODEL_STATE_UNSPECIFIED: _ClassVar[ModelState]
    MODEL_STATE_DOWNLOADING: _ClassVar[ModelState]
    MODEL_STATE_LOADING: _ClassVar[ModelState]
    MODEL_STATE_READY: _ClassVar[ModelState]
    MODEL_STATE_FAILED: _ClassVar[ModelState]
    MODEL_STATE_UNLOADED: _ClassVar[ModelState]

class HealthStatus(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    HEALTH_STATUS_UNSPECIFIED: _ClassVar[HealthStatus]
    HEALTH_STATUS_SERVING: _ClassVar[HealthStatus]
    HEALTH_STATUS_NOT_SERVING: _ClassVar[HealthStatus]
DATA_TYPE_UNSPECIFIED: DataType
DATA_TYPE_FLOAT32: DataType
DATA_TYPE_FLOAT64: DataType
DATA_TYPE_INT32: DataType
DATA_TYPE_INT64: DataType
DATA_TYPE_BOOL: DataType
DATA_TYPE_STRING: DataType
MODEL_STATE_UNSPECIFIED: ModelState
MODEL_STATE_DOWNLOADING: ModelState
MODEL_STATE_LOADING: ModelState
MODEL_STATE_READY: ModelState
MODEL_STATE_FAILED: ModelState
MODEL_STATE_UNLOADED: ModelState
HEALTH_STATUS_UNSPECIFIED: HealthStatus
HEALTH_STATUS_SERVING: HealthStatus
HEALTH_STATUS_NOT_SERVING: HealthStatus

class TensorData(_message.Message):
    __slots__ = ("shape", "data", "dtype")
    SHAPE_FIELD_NUMBER: _ClassVar[int]
    DATA_FIELD_NUMBER: _ClassVar[int]
    DTYPE_FIELD_NUMBER: _ClassVar[int]
    shape: _containers.RepeatedScalarFieldContainer[int]
    data: bytes
    dtype: DataType
    def __init__(self, shape: _Optional[_Iterable[int]] = ..., data: _Optional[bytes] = ..., dtype: _Optional[_Union[DataType, str]] = ...) -> None: ...

class TensorSpec(_message.Message):
    __slots__ = ("name", "shape", "dtype")
    NAME_FIELD_NUMBER: _ClassVar[int]
    SHAPE_FIELD_NUMBER: _ClassVar[int]
    DTYPE_FIELD_NUMBER: _ClassVar[int]
    name: str
    shape: _containers.RepeatedScalarFieldContainer[int]
    dtype: DataType
    def __init__(self, name: _Optional[str] = ..., shape: _Optional[_Iterable[int]] = ..., dtype: _Optional[_Union[DataType, str]] = ...) -> None: ...

class ModelInfo(_message.Message):
    __slots__ = ("name", "version", "input_schema", "output_schema", "loaded_at", "artifact_digest")
    NAME_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    INPUT_SCHEMA_FIELD_NUMBER: _ClassVar[int]
    OUTPUT_SCHEMA_FIELD_NUMBER: _ClassVar[int]
    LOADED_AT_FIELD_NUMBER: _ClassVar[int]
    ARTIFACT_DIGEST_FIELD_NUMBER: _ClassVar[int]
    name: str
    version: str
    input_schema: _containers.RepeatedCompositeFieldContainer[TensorSpec]
    output_schema: _containers.RepeatedCompositeFieldContainer[TensorSpec]
    loaded_at: _timestamp_pb2.Timestamp
    artifact_digest: str
    def __init__(self, name: _Optional[str] = ..., version: _Optional[str] = ..., input_schema: _Optional[_Iterable[_Union[TensorSpec, _Mapping]]] = ..., output_schema: _Optional[_Iterable[_Union[TensorSpec, _Mapping]]] = ..., loaded_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., artifact_digest: _Optional[str] = ...) -> None: ...

class ServingMetrics(_message.Message):
    __slots__ = ("inflight_requests", "total_requests", "failed_requests", "p50_latency", "p99_latency", "model_memory_bytes")
    INFLIGHT_REQUESTS_FIELD_NUMBER: _ClassVar[int]
    TOTAL_REQUESTS_FIELD_NUMBER: _ClassVar[int]
    FAILED_REQUESTS_FIELD_NUMBER: _ClassVar[int]
    P50_LATENCY_FIELD_NUMBER: _ClassVar[int]
    P99_LATENCY_FIELD_NUMBER: _ClassVar[int]
    MODEL_MEMORY_BYTES_FIELD_NUMBER: _ClassVar[int]
    inflight_requests: int
    total_requests: int
    failed_requests: int
    p50_latency: _duration_pb2.Duration
    p99_latency: _duration_pb2.Duration
    model_memory_bytes: int
    def __init__(self, inflight_requests: _Optional[int] = ..., total_requests: _Optional[int] = ..., failed_requests: _Optional[int] = ..., p50_latency: _Optional[_Union[datetime.timedelta, _duration_pb2.Duration, _Mapping]] = ..., p99_latency: _Optional[_Union[datetime.timedelta, _duration_pb2.Duration, _Mapping]] = ..., model_memory_bytes: _Optional[int] = ...) -> None: ...

class PredictRequest(_message.Message):
    __slots__ = ("model_name", "version", "inputs", "idempotency_key", "correlation_id")
    class InputsEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: TensorData
        def __init__(self, key: _Optional[str] = ..., value: _Optional[_Union[TensorData, _Mapping]] = ...) -> None: ...
    MODEL_NAME_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    INPUTS_FIELD_NUMBER: _ClassVar[int]
    IDEMPOTENCY_KEY_FIELD_NUMBER: _ClassVar[int]
    CORRELATION_ID_FIELD_NUMBER: _ClassVar[int]
    model_name: str
    version: str
    inputs: _containers.MessageMap[str, TensorData]
    idempotency_key: str
    correlation_id: str
    def __init__(self, model_name: _Optional[str] = ..., version: _Optional[str] = ..., inputs: _Optional[_Mapping[str, TensorData]] = ..., idempotency_key: _Optional[str] = ..., correlation_id: _Optional[str] = ...) -> None: ...

class PredictResponse(_message.Message):
    __slots__ = ("outputs", "model_version", "inference_latency", "correlation_id", "from_cache")
    class OutputsEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: TensorData
        def __init__(self, key: _Optional[str] = ..., value: _Optional[_Union[TensorData, _Mapping]] = ...) -> None: ...
    OUTPUTS_FIELD_NUMBER: _ClassVar[int]
    MODEL_VERSION_FIELD_NUMBER: _ClassVar[int]
    INFERENCE_LATENCY_FIELD_NUMBER: _ClassVar[int]
    CORRELATION_ID_FIELD_NUMBER: _ClassVar[int]
    FROM_CACHE_FIELD_NUMBER: _ClassVar[int]
    outputs: _containers.MessageMap[str, TensorData]
    model_version: str
    inference_latency: _duration_pb2.Duration
    correlation_id: str
    from_cache: bool
    def __init__(self, outputs: _Optional[_Mapping[str, TensorData]] = ..., model_version: _Optional[str] = ..., inference_latency: _Optional[_Union[datetime.timedelta, _duration_pb2.Duration, _Mapping]] = ..., correlation_id: _Optional[str] = ..., from_cache: _Optional[bool] = ...) -> None: ...

class StreamPredictRequest(_message.Message):
    __slots__ = ("inputs", "idempotency_key", "correlation_id")
    class InputsEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: TensorData
        def __init__(self, key: _Optional[str] = ..., value: _Optional[_Union[TensorData, _Mapping]] = ...) -> None: ...
    INPUTS_FIELD_NUMBER: _ClassVar[int]
    IDEMPOTENCY_KEY_FIELD_NUMBER: _ClassVar[int]
    CORRELATION_ID_FIELD_NUMBER: _ClassVar[int]
    inputs: _containers.MessageMap[str, TensorData]
    idempotency_key: str
    correlation_id: str
    def __init__(self, inputs: _Optional[_Mapping[str, TensorData]] = ..., idempotency_key: _Optional[str] = ..., correlation_id: _Optional[str] = ...) -> None: ...

class StreamPredictResponse(_message.Message):
    __slots__ = ("outputs", "model_version", "inference_latency", "idempotency_key", "correlation_id")
    class OutputsEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: TensorData
        def __init__(self, key: _Optional[str] = ..., value: _Optional[_Union[TensorData, _Mapping]] = ...) -> None: ...
    OUTPUTS_FIELD_NUMBER: _ClassVar[int]
    MODEL_VERSION_FIELD_NUMBER: _ClassVar[int]
    INFERENCE_LATENCY_FIELD_NUMBER: _ClassVar[int]
    IDEMPOTENCY_KEY_FIELD_NUMBER: _ClassVar[int]
    CORRELATION_ID_FIELD_NUMBER: _ClassVar[int]
    outputs: _containers.MessageMap[str, TensorData]
    model_version: str
    inference_latency: _duration_pb2.Duration
    idempotency_key: str
    correlation_id: str
    def __init__(self, outputs: _Optional[_Mapping[str, TensorData]] = ..., model_version: _Optional[str] = ..., inference_latency: _Optional[_Union[datetime.timedelta, _duration_pb2.Duration, _Mapping]] = ..., idempotency_key: _Optional[str] = ..., correlation_id: _Optional[str] = ...) -> None: ...

class GetModelInfoRequest(_message.Message):
    __slots__ = ("model_name", "version")
    MODEL_NAME_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    model_name: str
    version: str
    def __init__(self, model_name: _Optional[str] = ..., version: _Optional[str] = ...) -> None: ...

class GetModelInfoResponse(_message.Message):
    __slots__ = ("model_info",)
    MODEL_INFO_FIELD_NUMBER: _ClassVar[int]
    model_info: ModelInfo
    def __init__(self, model_info: _Optional[_Union[ModelInfo, _Mapping]] = ...) -> None: ...

class ListLoadedModelsRequest(_message.Message):
    __slots__ = ("pagination", "state_filter")
    PAGINATION_FIELD_NUMBER: _ClassVar[int]
    STATE_FILTER_FIELD_NUMBER: _ClassVar[int]
    pagination: _common_pb2.PaginationRequest
    state_filter: ModelState
    def __init__(self, pagination: _Optional[_Union[_common_pb2.PaginationRequest, _Mapping]] = ..., state_filter: _Optional[_Union[ModelState, str]] = ...) -> None: ...

class ListLoadedModelsResponse(_message.Message):
    __slots__ = ("models", "pagination")
    MODELS_FIELD_NUMBER: _ClassVar[int]
    PAGINATION_FIELD_NUMBER: _ClassVar[int]
    models: _containers.RepeatedCompositeFieldContainer[ModelStatus]
    pagination: _common_pb2.PaginationResponse
    def __init__(self, models: _Optional[_Iterable[_Union[ModelStatus, _Mapping]]] = ..., pagination: _Optional[_Union[_common_pb2.PaginationResponse, _Mapping]] = ...) -> None: ...

class LoadModelRequest(_message.Message):
    __slots__ = ("model_name", "version", "artifact_uri", "expected_digest", "idempotency_key")
    MODEL_NAME_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    ARTIFACT_URI_FIELD_NUMBER: _ClassVar[int]
    EXPECTED_DIGEST_FIELD_NUMBER: _ClassVar[int]
    IDEMPOTENCY_KEY_FIELD_NUMBER: _ClassVar[int]
    model_name: str
    version: str
    artifact_uri: str
    expected_digest: str
    idempotency_key: str
    def __init__(self, model_name: _Optional[str] = ..., version: _Optional[str] = ..., artifact_uri: _Optional[str] = ..., expected_digest: _Optional[str] = ..., idempotency_key: _Optional[str] = ...) -> None: ...

class LoadModelResponse(_message.Message):
    __slots__ = ("status",)
    STATUS_FIELD_NUMBER: _ClassVar[int]
    status: ModelStatus
    def __init__(self, status: _Optional[_Union[ModelStatus, _Mapping]] = ...) -> None: ...

class UnloadModelRequest(_message.Message):
    __slots__ = ("model_name", "version", "reason", "idempotency_key")
    MODEL_NAME_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    IDEMPOTENCY_KEY_FIELD_NUMBER: _ClassVar[int]
    model_name: str
    version: str
    reason: str
    idempotency_key: str
    def __init__(self, model_name: _Optional[str] = ..., version: _Optional[str] = ..., reason: _Optional[str] = ..., idempotency_key: _Optional[str] = ...) -> None: ...

class UnloadModelResponse(_message.Message):
    __slots__ = ("status",)
    STATUS_FIELD_NUMBER: _ClassVar[int]
    status: ModelStatus
    def __init__(self, status: _Optional[_Union[ModelStatus, _Mapping]] = ...) -> None: ...

class ModelStatus(_message.Message):
    __slots__ = ("model_name", "version", "state", "message", "updated_at")
    MODEL_NAME_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    STATE_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    UPDATED_AT_FIELD_NUMBER: _ClassVar[int]
    model_name: str
    version: str
    state: ModelState
    message: str
    updated_at: _timestamp_pb2.Timestamp
    def __init__(self, model_name: _Optional[str] = ..., version: _Optional[str] = ..., state: _Optional[_Union[ModelState, str]] = ..., message: _Optional[str] = ..., updated_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class GetModelStatusRequest(_message.Message):
    __slots__ = ("model_name", "version")
    MODEL_NAME_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    model_name: str
    version: str
    def __init__(self, model_name: _Optional[str] = ..., version: _Optional[str] = ...) -> None: ...

class GetModelStatusResponse(_message.Message):
    __slots__ = ("status",)
    STATUS_FIELD_NUMBER: _ClassVar[int]
    status: ModelStatus
    def __init__(self, status: _Optional[_Union[ModelStatus, _Mapping]] = ...) -> None: ...

class GetServingMetricsRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class GetServingMetricsResponse(_message.Message):
    __slots__ = ("metrics",)
    METRICS_FIELD_NUMBER: _ClassVar[int]
    metrics: ServingMetrics
    def __init__(self, metrics: _Optional[_Union[ServingMetrics, _Mapping]] = ...) -> None: ...

class HealthCheckRequest(_message.Message):
    __slots__ = ("model_name",)
    MODEL_NAME_FIELD_NUMBER: _ClassVar[int]
    model_name: str
    def __init__(self, model_name: _Optional[str] = ...) -> None: ...

class HealthCheckResponse(_message.Message):
    __slots__ = ("status", "model_state", "last_inference_latency")
    STATUS_FIELD_NUMBER: _ClassVar[int]
    MODEL_STATE_FIELD_NUMBER: _ClassVar[int]
    LAST_INFERENCE_LATENCY_FIELD_NUMBER: _ClassVar[int]
    status: HealthStatus
    model_state: ModelState
    last_inference_latency: _duration_pb2.Duration
    def __init__(self, status: _Optional[_Union[HealthStatus, str]] = ..., model_state: _Optional[_Union[ModelState, str]] = ..., last_inference_latency: _Optional[_Union[datetime.timedelta, _duration_pb2.Duration, _Mapping]] = ...) -> None: ...
