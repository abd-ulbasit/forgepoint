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

class ModelStage(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    MODEL_STAGE_UNSPECIFIED: _ClassVar[ModelStage]
    MODEL_STAGE_DEV: _ClassVar[ModelStage]
    MODEL_STAGE_STAGING: _ClassVar[ModelStage]
    MODEL_STAGE_PRODUCTION: _ClassVar[ModelStage]
    MODEL_STAGE_ARCHIVED: _ClassVar[ModelStage]

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

class InferenceFailureReason(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    INFERENCE_FAILURE_REASON_UNSPECIFIED: _ClassVar[InferenceFailureReason]
    INFERENCE_FAILURE_REASON_NO_ROUTE: _ClassVar[InferenceFailureReason]
    INFERENCE_FAILURE_REASON_RATE_LIMITED: _ClassVar[InferenceFailureReason]
    INFERENCE_FAILURE_REASON_BULKHEAD_FULL: _ClassVar[InferenceFailureReason]
    INFERENCE_FAILURE_REASON_CIRCUIT_OPEN: _ClassVar[InferenceFailureReason]
    INFERENCE_FAILURE_REASON_UPSTREAM_ERROR: _ClassVar[InferenceFailureReason]
    INFERENCE_FAILURE_REASON_TIMEOUT: _ClassVar[InferenceFailureReason]
    INFERENCE_FAILURE_REASON_INVALID_INPUT: _ClassVar[InferenceFailureReason]
    INFERENCE_FAILURE_REASON_QUOTA_EXCEEDED: _ClassVar[InferenceFailureReason]

class MeterType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    METER_TYPE_UNSPECIFIED: _ClassVar[MeterType]
    METER_TYPE_INFERENCE_REQUEST: _ClassVar[MeterType]
    METER_TYPE_INFERENCE_TOKENS: _ClassVar[MeterType]
    METER_TYPE_COMPUTE_SECONDS: _ClassVar[MeterType]
    METER_TYPE_STORAGE_BYTES: _ClassVar[MeterType]

class RunStatus(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    RUN_STATUS_UNSPECIFIED: _ClassVar[RunStatus]
    RUN_STATUS_RUNNING: _ClassVar[RunStatus]
    RUN_STATUS_FINISHED: _ClassVar[RunStatus]
    RUN_STATUS_FAILED: _ClassVar[RunStatus]
    RUN_STATUS_KILLED: _ClassVar[RunStatus]

class DriftType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    DRIFT_TYPE_UNSPECIFIED: _ClassVar[DriftType]
    DRIFT_TYPE_DATA: _ClassVar[DriftType]
    DRIFT_TYPE_PREDICTION: _ClassVar[DriftType]
    DRIFT_TYPE_PERFORMANCE: _ClassVar[DriftType]

class DriftMethod(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    DRIFT_METHOD_UNSPECIFIED: _ClassVar[DriftMethod]
    DRIFT_METHOD_PSI: _ClassVar[DriftMethod]
    DRIFT_METHOD_KL: _ClassVar[DriftMethod]
    DRIFT_METHOD_KS: _ClassVar[DriftMethod]

class DriftSeverity(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    DRIFT_SEVERITY_UNSPECIFIED: _ClassVar[DriftSeverity]
    DRIFT_SEVERITY_OK: _ClassVar[DriftSeverity]
    DRIFT_SEVERITY_WARNING: _ClassVar[DriftSeverity]
    DRIFT_SEVERITY_CRITICAL: _ClassVar[DriftSeverity]

class NotificationChannel(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    NOTIFICATION_CHANNEL_UNSPECIFIED: _ClassVar[NotificationChannel]
    NOTIFICATION_CHANNEL_IN_APP: _ClassVar[NotificationChannel]
    NOTIFICATION_CHANNEL_WEBHOOK: _ClassVar[NotificationChannel]
    NOTIFICATION_CHANNEL_SLACK: _ClassVar[NotificationChannel]
    NOTIFICATION_CHANNEL_EMAIL: _ClassVar[NotificationChannel]
MODEL_STAGE_UNSPECIFIED: ModelStage
MODEL_STAGE_DEV: ModelStage
MODEL_STAGE_STAGING: ModelStage
MODEL_STAGE_PRODUCTION: ModelStage
MODEL_STAGE_ARCHIVED: ModelStage
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
INFERENCE_FAILURE_REASON_UNSPECIFIED: InferenceFailureReason
INFERENCE_FAILURE_REASON_NO_ROUTE: InferenceFailureReason
INFERENCE_FAILURE_REASON_RATE_LIMITED: InferenceFailureReason
INFERENCE_FAILURE_REASON_BULKHEAD_FULL: InferenceFailureReason
INFERENCE_FAILURE_REASON_CIRCUIT_OPEN: InferenceFailureReason
INFERENCE_FAILURE_REASON_UPSTREAM_ERROR: InferenceFailureReason
INFERENCE_FAILURE_REASON_TIMEOUT: InferenceFailureReason
INFERENCE_FAILURE_REASON_INVALID_INPUT: InferenceFailureReason
INFERENCE_FAILURE_REASON_QUOTA_EXCEEDED: InferenceFailureReason
METER_TYPE_UNSPECIFIED: MeterType
METER_TYPE_INFERENCE_REQUEST: MeterType
METER_TYPE_INFERENCE_TOKENS: MeterType
METER_TYPE_COMPUTE_SECONDS: MeterType
METER_TYPE_STORAGE_BYTES: MeterType
RUN_STATUS_UNSPECIFIED: RunStatus
RUN_STATUS_RUNNING: RunStatus
RUN_STATUS_FINISHED: RunStatus
RUN_STATUS_FAILED: RunStatus
RUN_STATUS_KILLED: RunStatus
DRIFT_TYPE_UNSPECIFIED: DriftType
DRIFT_TYPE_DATA: DriftType
DRIFT_TYPE_PREDICTION: DriftType
DRIFT_TYPE_PERFORMANCE: DriftType
DRIFT_METHOD_UNSPECIFIED: DriftMethod
DRIFT_METHOD_PSI: DriftMethod
DRIFT_METHOD_KL: DriftMethod
DRIFT_METHOD_KS: DriftMethod
DRIFT_SEVERITY_UNSPECIFIED: DriftSeverity
DRIFT_SEVERITY_OK: DriftSeverity
DRIFT_SEVERITY_WARNING: DriftSeverity
DRIFT_SEVERITY_CRITICAL: DriftSeverity
NOTIFICATION_CHANNEL_UNSPECIFIED: NotificationChannel
NOTIFICATION_CHANNEL_IN_APP: NotificationChannel
NOTIFICATION_CHANNEL_WEBHOOK: NotificationChannel
NOTIFICATION_CHANNEL_SLACK: NotificationChannel
NOTIFICATION_CHANNEL_EMAIL: NotificationChannel

class PredictionSummary(_message.Message):
    __slots__ = ("top_label", "top_score", "output_stats", "output_cardinality")
    class OutputStatsEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: float
        def __init__(self, key: _Optional[str] = ..., value: _Optional[float] = ...) -> None: ...
    TOP_LABEL_FIELD_NUMBER: _ClassVar[int]
    TOP_SCORE_FIELD_NUMBER: _ClassVar[int]
    OUTPUT_STATS_FIELD_NUMBER: _ClassVar[int]
    OUTPUT_CARDINALITY_FIELD_NUMBER: _ClassVar[int]
    top_label: str
    top_score: float
    output_stats: _containers.ScalarMap[str, float]
    output_cardinality: int
    def __init__(self, top_label: _Optional[str] = ..., top_score: _Optional[float] = ..., output_stats: _Optional[_Mapping[str, float]] = ..., output_cardinality: _Optional[int] = ...) -> None: ...

class FeatureSummary(_message.Message):
    __slots__ = ("feature_values", "feature_count")
    class FeatureValuesEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: float
        def __init__(self, key: _Optional[str] = ..., value: _Optional[float] = ...) -> None: ...
    FEATURE_VALUES_FIELD_NUMBER: _ClassVar[int]
    FEATURE_COUNT_FIELD_NUMBER: _ClassVar[int]
    feature_values: _containers.ScalarMap[str, float]
    feature_count: int
    def __init__(self, feature_values: _Optional[_Mapping[str, float]] = ..., feature_count: _Optional[int] = ...) -> None: ...

class DriftMetric(_message.Message):
    __slots__ = ("name", "method", "score", "baseline_value", "current_value", "severity")
    NAME_FIELD_NUMBER: _ClassVar[int]
    METHOD_FIELD_NUMBER: _ClassVar[int]
    SCORE_FIELD_NUMBER: _ClassVar[int]
    BASELINE_VALUE_FIELD_NUMBER: _ClassVar[int]
    CURRENT_VALUE_FIELD_NUMBER: _ClassVar[int]
    SEVERITY_FIELD_NUMBER: _ClassVar[int]
    name: str
    method: DriftMethod
    score: float
    baseline_value: float
    current_value: float
    severity: DriftSeverity
    def __init__(self, name: _Optional[str] = ..., method: _Optional[_Union[DriftMethod, str]] = ..., score: _Optional[float] = ..., baseline_value: _Optional[float] = ..., current_value: _Optional[float] = ..., severity: _Optional[_Union[DriftSeverity, str]] = ...) -> None: ...

class MetricPoint(_message.Message):
    __slots__ = ("key", "value", "step", "timestamp")
    KEY_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    STEP_FIELD_NUMBER: _ClassVar[int]
    TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    key: str
    value: float
    step: int
    timestamp: _timestamp_pb2.Timestamp
    def __init__(self, key: _Optional[str] = ..., value: _Optional[float] = ..., step: _Optional[int] = ..., timestamp: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class ModelRegistered(_message.Message):
    __slots__ = ("model_id", "model_name", "framework", "task_type", "owner_id", "team", "registered_at")
    MODEL_ID_FIELD_NUMBER: _ClassVar[int]
    MODEL_NAME_FIELD_NUMBER: _ClassVar[int]
    FRAMEWORK_FIELD_NUMBER: _ClassVar[int]
    TASK_TYPE_FIELD_NUMBER: _ClassVar[int]
    OWNER_ID_FIELD_NUMBER: _ClassVar[int]
    TEAM_FIELD_NUMBER: _ClassVar[int]
    REGISTERED_AT_FIELD_NUMBER: _ClassVar[int]
    model_id: str
    model_name: str
    framework: str
    task_type: str
    owner_id: str
    team: str
    registered_at: _timestamp_pb2.Timestamp
    def __init__(self, model_id: _Optional[str] = ..., model_name: _Optional[str] = ..., framework: _Optional[str] = ..., task_type: _Optional[str] = ..., owner_id: _Optional[str] = ..., team: _Optional[str] = ..., registered_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class ModelVersionCreated(_message.Message):
    __slots__ = ("model_id", "model_name", "version_id", "version", "created_by", "created_at")
    MODEL_ID_FIELD_NUMBER: _ClassVar[int]
    MODEL_NAME_FIELD_NUMBER: _ClassVar[int]
    VERSION_ID_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    CREATED_BY_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_FIELD_NUMBER: _ClassVar[int]
    model_id: str
    model_name: str
    version_id: str
    version: str
    created_by: str
    created_at: _timestamp_pb2.Timestamp
    def __init__(self, model_id: _Optional[str] = ..., model_name: _Optional[str] = ..., version_id: _Optional[str] = ..., version: _Optional[str] = ..., created_by: _Optional[str] = ..., created_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class ModelVersionReady(_message.Message):
    __slots__ = ("model_id", "model_name", "version_id", "version", "artifact_path", "artifact_digest", "size_bytes", "ready_at")
    MODEL_ID_FIELD_NUMBER: _ClassVar[int]
    MODEL_NAME_FIELD_NUMBER: _ClassVar[int]
    VERSION_ID_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    ARTIFACT_PATH_FIELD_NUMBER: _ClassVar[int]
    ARTIFACT_DIGEST_FIELD_NUMBER: _ClassVar[int]
    SIZE_BYTES_FIELD_NUMBER: _ClassVar[int]
    READY_AT_FIELD_NUMBER: _ClassVar[int]
    model_id: str
    model_name: str
    version_id: str
    version: str
    artifact_path: str
    artifact_digest: str
    size_bytes: int
    ready_at: _timestamp_pb2.Timestamp
    def __init__(self, model_id: _Optional[str] = ..., model_name: _Optional[str] = ..., version_id: _Optional[str] = ..., version: _Optional[str] = ..., artifact_path: _Optional[str] = ..., artifact_digest: _Optional[str] = ..., size_bytes: _Optional[int] = ..., ready_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class ModelPromoted(_message.Message):
    __slots__ = ("model_id", "model_name", "version_id", "version", "from_stage", "to_stage", "demoted_version_id", "demoted_version", "promoted_by", "promoted_at")
    MODEL_ID_FIELD_NUMBER: _ClassVar[int]
    MODEL_NAME_FIELD_NUMBER: _ClassVar[int]
    VERSION_ID_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    FROM_STAGE_FIELD_NUMBER: _ClassVar[int]
    TO_STAGE_FIELD_NUMBER: _ClassVar[int]
    DEMOTED_VERSION_ID_FIELD_NUMBER: _ClassVar[int]
    DEMOTED_VERSION_FIELD_NUMBER: _ClassVar[int]
    PROMOTED_BY_FIELD_NUMBER: _ClassVar[int]
    PROMOTED_AT_FIELD_NUMBER: _ClassVar[int]
    model_id: str
    model_name: str
    version_id: str
    version: str
    from_stage: ModelStage
    to_stage: ModelStage
    demoted_version_id: str
    demoted_version: str
    promoted_by: str
    promoted_at: _timestamp_pb2.Timestamp
    def __init__(self, model_id: _Optional[str] = ..., model_name: _Optional[str] = ..., version_id: _Optional[str] = ..., version: _Optional[str] = ..., from_stage: _Optional[_Union[ModelStage, str]] = ..., to_stage: _Optional[_Union[ModelStage, str]] = ..., demoted_version_id: _Optional[str] = ..., demoted_version: _Optional[str] = ..., promoted_by: _Optional[str] = ..., promoted_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class ModelArchived(_message.Message):
    __slots__ = ("model_id", "model_name", "archived_by", "archived_at")
    MODEL_ID_FIELD_NUMBER: _ClassVar[int]
    MODEL_NAME_FIELD_NUMBER: _ClassVar[int]
    ARCHIVED_BY_FIELD_NUMBER: _ClassVar[int]
    ARCHIVED_AT_FIELD_NUMBER: _ClassVar[int]
    model_id: str
    model_name: str
    archived_by: str
    archived_at: _timestamp_pb2.Timestamp
    def __init__(self, model_id: _Optional[str] = ..., model_name: _Optional[str] = ..., archived_by: _Optional[str] = ..., archived_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class ModelDriftDetected(_message.Message):
    __slots__ = ("model_name", "model_version", "drift_type", "severity", "report_id", "metrics", "window_start", "window_end", "sample_count", "auto_retrain", "retrain_pipeline_id", "retrain_context", "detected_at")
    MODEL_NAME_FIELD_NUMBER: _ClassVar[int]
    MODEL_VERSION_FIELD_NUMBER: _ClassVar[int]
    DRIFT_TYPE_FIELD_NUMBER: _ClassVar[int]
    SEVERITY_FIELD_NUMBER: _ClassVar[int]
    REPORT_ID_FIELD_NUMBER: _ClassVar[int]
    METRICS_FIELD_NUMBER: _ClassVar[int]
    WINDOW_START_FIELD_NUMBER: _ClassVar[int]
    WINDOW_END_FIELD_NUMBER: _ClassVar[int]
    SAMPLE_COUNT_FIELD_NUMBER: _ClassVar[int]
    AUTO_RETRAIN_FIELD_NUMBER: _ClassVar[int]
    RETRAIN_PIPELINE_ID_FIELD_NUMBER: _ClassVar[int]
    RETRAIN_CONTEXT_FIELD_NUMBER: _ClassVar[int]
    DETECTED_AT_FIELD_NUMBER: _ClassVar[int]
    model_name: str
    model_version: str
    drift_type: DriftType
    severity: DriftSeverity
    report_id: str
    metrics: _containers.RepeatedCompositeFieldContainer[DriftMetric]
    window_start: _timestamp_pb2.Timestamp
    window_end: _timestamp_pb2.Timestamp
    sample_count: int
    auto_retrain: bool
    retrain_pipeline_id: str
    retrain_context: _struct_pb2.Struct
    detected_at: _timestamp_pb2.Timestamp
    def __init__(self, model_name: _Optional[str] = ..., model_version: _Optional[str] = ..., drift_type: _Optional[_Union[DriftType, str]] = ..., severity: _Optional[_Union[DriftSeverity, str]] = ..., report_id: _Optional[str] = ..., metrics: _Optional[_Iterable[_Union[DriftMetric, _Mapping]]] = ..., window_start: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., window_end: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., sample_count: _Optional[int] = ..., auto_retrain: _Optional[bool] = ..., retrain_pipeline_id: _Optional[str] = ..., retrain_context: _Optional[_Union[_struct_pb2.Struct, _Mapping]] = ..., detected_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class PipelineStarted(_message.Message):
    __slots__ = ("execution_id", "pipeline_id", "pipeline_type", "triggered_by", "started_at")
    EXECUTION_ID_FIELD_NUMBER: _ClassVar[int]
    PIPELINE_ID_FIELD_NUMBER: _ClassVar[int]
    PIPELINE_TYPE_FIELD_NUMBER: _ClassVar[int]
    TRIGGERED_BY_FIELD_NUMBER: _ClassVar[int]
    STARTED_AT_FIELD_NUMBER: _ClassVar[int]
    execution_id: str
    pipeline_id: str
    pipeline_type: PipelineType
    triggered_by: str
    started_at: _timestamp_pb2.Timestamp
    def __init__(self, execution_id: _Optional[str] = ..., pipeline_id: _Optional[str] = ..., pipeline_type: _Optional[_Union[PipelineType, str]] = ..., triggered_by: _Optional[str] = ..., started_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class StepCompleted(_message.Message):
    __slots__ = ("execution_id", "pipeline_id", "step_id", "step_type", "output", "completed_at")
    EXECUTION_ID_FIELD_NUMBER: _ClassVar[int]
    PIPELINE_ID_FIELD_NUMBER: _ClassVar[int]
    STEP_ID_FIELD_NUMBER: _ClassVar[int]
    STEP_TYPE_FIELD_NUMBER: _ClassVar[int]
    OUTPUT_FIELD_NUMBER: _ClassVar[int]
    COMPLETED_AT_FIELD_NUMBER: _ClassVar[int]
    execution_id: str
    pipeline_id: str
    step_id: str
    step_type: StepType
    output: _struct_pb2.Struct
    completed_at: _timestamp_pb2.Timestamp
    def __init__(self, execution_id: _Optional[str] = ..., pipeline_id: _Optional[str] = ..., step_id: _Optional[str] = ..., step_type: _Optional[_Union[StepType, str]] = ..., output: _Optional[_Union[_struct_pb2.Struct, _Mapping]] = ..., completed_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class StepFailed(_message.Message):
    __slots__ = ("execution_id", "pipeline_id", "step_id", "step_type", "error", "attempts", "failed_at")
    EXECUTION_ID_FIELD_NUMBER: _ClassVar[int]
    PIPELINE_ID_FIELD_NUMBER: _ClassVar[int]
    STEP_ID_FIELD_NUMBER: _ClassVar[int]
    STEP_TYPE_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    ATTEMPTS_FIELD_NUMBER: _ClassVar[int]
    FAILED_AT_FIELD_NUMBER: _ClassVar[int]
    execution_id: str
    pipeline_id: str
    step_id: str
    step_type: StepType
    error: str
    attempts: int
    failed_at: _timestamp_pb2.Timestamp
    def __init__(self, execution_id: _Optional[str] = ..., pipeline_id: _Optional[str] = ..., step_id: _Optional[str] = ..., step_type: _Optional[_Union[StepType, str]] = ..., error: _Optional[str] = ..., attempts: _Optional[int] = ..., failed_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class CompensationTriggered(_message.Message):
    __slots__ = ("execution_id", "pipeline_id", "failed_step_id", "compensating_step_ids", "triggered_at")
    EXECUTION_ID_FIELD_NUMBER: _ClassVar[int]
    PIPELINE_ID_FIELD_NUMBER: _ClassVar[int]
    FAILED_STEP_ID_FIELD_NUMBER: _ClassVar[int]
    COMPENSATING_STEP_IDS_FIELD_NUMBER: _ClassVar[int]
    TRIGGERED_AT_FIELD_NUMBER: _ClassVar[int]
    execution_id: str
    pipeline_id: str
    failed_step_id: str
    compensating_step_ids: _containers.RepeatedScalarFieldContainer[str]
    triggered_at: _timestamp_pb2.Timestamp
    def __init__(self, execution_id: _Optional[str] = ..., pipeline_id: _Optional[str] = ..., failed_step_id: _Optional[str] = ..., compensating_step_ids: _Optional[_Iterable[str]] = ..., triggered_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class PipelineCompleted(_message.Message):
    __slots__ = ("execution_id", "pipeline_id", "pipeline_type", "duration", "completed_at")
    EXECUTION_ID_FIELD_NUMBER: _ClassVar[int]
    PIPELINE_ID_FIELD_NUMBER: _ClassVar[int]
    PIPELINE_TYPE_FIELD_NUMBER: _ClassVar[int]
    DURATION_FIELD_NUMBER: _ClassVar[int]
    COMPLETED_AT_FIELD_NUMBER: _ClassVar[int]
    execution_id: str
    pipeline_id: str
    pipeline_type: PipelineType
    duration: _duration_pb2.Duration
    completed_at: _timestamp_pb2.Timestamp
    def __init__(self, execution_id: _Optional[str] = ..., pipeline_id: _Optional[str] = ..., pipeline_type: _Optional[_Union[PipelineType, str]] = ..., duration: _Optional[_Union[datetime.timedelta, _duration_pb2.Duration, _Mapping]] = ..., completed_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class PipelineFailed(_message.Message):
    __slots__ = ("execution_id", "pipeline_id", "pipeline_type", "failed_step_id", "error", "compensation_failed", "failed_at")
    EXECUTION_ID_FIELD_NUMBER: _ClassVar[int]
    PIPELINE_ID_FIELD_NUMBER: _ClassVar[int]
    PIPELINE_TYPE_FIELD_NUMBER: _ClassVar[int]
    FAILED_STEP_ID_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    COMPENSATION_FAILED_FIELD_NUMBER: _ClassVar[int]
    FAILED_AT_FIELD_NUMBER: _ClassVar[int]
    execution_id: str
    pipeline_id: str
    pipeline_type: PipelineType
    failed_step_id: str
    error: str
    compensation_failed: bool
    failed_at: _timestamp_pb2.Timestamp
    def __init__(self, execution_id: _Optional[str] = ..., pipeline_id: _Optional[str] = ..., pipeline_type: _Optional[_Union[PipelineType, str]] = ..., failed_step_id: _Optional[str] = ..., error: _Optional[str] = ..., compensation_failed: _Optional[bool] = ..., failed_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class ModelDeployed(_message.Message):
    __slots__ = ("model_id", "model_name", "version_id", "version", "endpoint", "weight_bps", "execution_id", "deployed_at")
    MODEL_ID_FIELD_NUMBER: _ClassVar[int]
    MODEL_NAME_FIELD_NUMBER: _ClassVar[int]
    VERSION_ID_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    ENDPOINT_FIELD_NUMBER: _ClassVar[int]
    WEIGHT_BPS_FIELD_NUMBER: _ClassVar[int]
    EXECUTION_ID_FIELD_NUMBER: _ClassVar[int]
    DEPLOYED_AT_FIELD_NUMBER: _ClassVar[int]
    model_id: str
    model_name: str
    version_id: str
    version: str
    endpoint: str
    weight_bps: int
    execution_id: str
    deployed_at: _timestamp_pb2.Timestamp
    def __init__(self, model_id: _Optional[str] = ..., model_name: _Optional[str] = ..., version_id: _Optional[str] = ..., version: _Optional[str] = ..., endpoint: _Optional[str] = ..., weight_bps: _Optional[int] = ..., execution_id: _Optional[str] = ..., deployed_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class ModelUndeployed(_message.Message):
    __slots__ = ("model_id", "model_name", "version_id", "version", "reason", "execution_id", "undeployed_at")
    MODEL_ID_FIELD_NUMBER: _ClassVar[int]
    MODEL_NAME_FIELD_NUMBER: _ClassVar[int]
    VERSION_ID_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    EXECUTION_ID_FIELD_NUMBER: _ClassVar[int]
    UNDEPLOYED_AT_FIELD_NUMBER: _ClassVar[int]
    model_id: str
    model_name: str
    version_id: str
    version: str
    reason: str
    execution_id: str
    undeployed_at: _timestamp_pb2.Timestamp
    def __init__(self, model_id: _Optional[str] = ..., model_name: _Optional[str] = ..., version_id: _Optional[str] = ..., version: _Optional[str] = ..., reason: _Optional[str] = ..., execution_id: _Optional[str] = ..., undeployed_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class InferenceCompleted(_message.Message):
    __slots__ = ("request_id", "model_id", "model_name", "version", "api_key_id", "is_canary", "latency", "latency_ms", "token_count", "prediction_summary", "feature_summary", "completed_at")
    REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
    MODEL_ID_FIELD_NUMBER: _ClassVar[int]
    MODEL_NAME_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    API_KEY_ID_FIELD_NUMBER: _ClassVar[int]
    IS_CANARY_FIELD_NUMBER: _ClassVar[int]
    LATENCY_FIELD_NUMBER: _ClassVar[int]
    LATENCY_MS_FIELD_NUMBER: _ClassVar[int]
    TOKEN_COUNT_FIELD_NUMBER: _ClassVar[int]
    PREDICTION_SUMMARY_FIELD_NUMBER: _ClassVar[int]
    FEATURE_SUMMARY_FIELD_NUMBER: _ClassVar[int]
    COMPLETED_AT_FIELD_NUMBER: _ClassVar[int]
    request_id: str
    model_id: str
    model_name: str
    version: str
    api_key_id: str
    is_canary: bool
    latency: _duration_pb2.Duration
    latency_ms: int
    token_count: int
    prediction_summary: PredictionSummary
    feature_summary: FeatureSummary
    completed_at: _timestamp_pb2.Timestamp
    def __init__(self, request_id: _Optional[str] = ..., model_id: _Optional[str] = ..., model_name: _Optional[str] = ..., version: _Optional[str] = ..., api_key_id: _Optional[str] = ..., is_canary: _Optional[bool] = ..., latency: _Optional[_Union[datetime.timedelta, _duration_pb2.Duration, _Mapping]] = ..., latency_ms: _Optional[int] = ..., token_count: _Optional[int] = ..., prediction_summary: _Optional[_Union[PredictionSummary, _Mapping]] = ..., feature_summary: _Optional[_Union[FeatureSummary, _Mapping]] = ..., completed_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class InferenceFailed(_message.Message):
    __slots__ = ("request_id", "model_id", "model_name", "version", "api_key_id", "reason", "error", "failed_at")
    REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
    MODEL_ID_FIELD_NUMBER: _ClassVar[int]
    MODEL_NAME_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    API_KEY_ID_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    FAILED_AT_FIELD_NUMBER: _ClassVar[int]
    request_id: str
    model_id: str
    model_name: str
    version: str
    api_key_id: str
    reason: InferenceFailureReason
    error: _common_pb2.ErrorDetail
    failed_at: _timestamp_pb2.Timestamp
    def __init__(self, request_id: _Optional[str] = ..., model_id: _Optional[str] = ..., model_name: _Optional[str] = ..., version: _Optional[str] = ..., api_key_id: _Optional[str] = ..., reason: _Optional[_Union[InferenceFailureReason, str]] = ..., error: _Optional[_Union[_common_pb2.ErrorDetail, _Mapping]] = ..., failed_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class FeatureViewDefined(_message.Message):
    __slots__ = ("feature_view_id", "feature_view_name", "schema_version", "owner_team", "defined_at")
    FEATURE_VIEW_ID_FIELD_NUMBER: _ClassVar[int]
    FEATURE_VIEW_NAME_FIELD_NUMBER: _ClassVar[int]
    SCHEMA_VERSION_FIELD_NUMBER: _ClassVar[int]
    OWNER_TEAM_FIELD_NUMBER: _ClassVar[int]
    DEFINED_AT_FIELD_NUMBER: _ClassVar[int]
    feature_view_id: str
    feature_view_name: str
    schema_version: int
    owner_team: str
    defined_at: _timestamp_pb2.Timestamp
    def __init__(self, feature_view_id: _Optional[str] = ..., feature_view_name: _Optional[str] = ..., schema_version: _Optional[int] = ..., owner_team: _Optional[str] = ..., defined_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class FeaturesWritten(_message.Message):
    __slots__ = ("feature_view_id", "feature_view_name", "entity_ids", "written_count", "written_through_version", "written_at")
    FEATURE_VIEW_ID_FIELD_NUMBER: _ClassVar[int]
    FEATURE_VIEW_NAME_FIELD_NUMBER: _ClassVar[int]
    ENTITY_IDS_FIELD_NUMBER: _ClassVar[int]
    WRITTEN_COUNT_FIELD_NUMBER: _ClassVar[int]
    WRITTEN_THROUGH_VERSION_FIELD_NUMBER: _ClassVar[int]
    WRITTEN_AT_FIELD_NUMBER: _ClassVar[int]
    feature_view_id: str
    feature_view_name: str
    entity_ids: _containers.RepeatedScalarFieldContainer[str]
    written_count: int
    written_through_version: int
    written_at: _timestamp_pb2.Timestamp
    def __init__(self, feature_view_id: _Optional[str] = ..., feature_view_name: _Optional[str] = ..., entity_ids: _Optional[_Iterable[str]] = ..., written_count: _Optional[int] = ..., written_through_version: _Optional[int] = ..., written_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class UsageRecorded(_message.Message):
    __slots__ = ("record_id", "team", "rate_plan_id", "meter_type", "quantity", "cost_micros", "currency_code", "model_id", "model_version", "source_request_id", "occurred_at")
    RECORD_ID_FIELD_NUMBER: _ClassVar[int]
    TEAM_FIELD_NUMBER: _ClassVar[int]
    RATE_PLAN_ID_FIELD_NUMBER: _ClassVar[int]
    METER_TYPE_FIELD_NUMBER: _ClassVar[int]
    QUANTITY_FIELD_NUMBER: _ClassVar[int]
    COST_MICROS_FIELD_NUMBER: _ClassVar[int]
    CURRENCY_CODE_FIELD_NUMBER: _ClassVar[int]
    MODEL_ID_FIELD_NUMBER: _ClassVar[int]
    MODEL_VERSION_FIELD_NUMBER: _ClassVar[int]
    SOURCE_REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
    OCCURRED_AT_FIELD_NUMBER: _ClassVar[int]
    record_id: str
    team: str
    rate_plan_id: str
    meter_type: MeterType
    quantity: int
    cost_micros: int
    currency_code: str
    model_id: str
    model_version: str
    source_request_id: str
    occurred_at: _timestamp_pb2.Timestamp
    def __init__(self, record_id: _Optional[str] = ..., team: _Optional[str] = ..., rate_plan_id: _Optional[str] = ..., meter_type: _Optional[_Union[MeterType, str]] = ..., quantity: _Optional[int] = ..., cost_micros: _Optional[int] = ..., currency_code: _Optional[str] = ..., model_id: _Optional[str] = ..., model_version: _Optional[str] = ..., source_request_id: _Optional[str] = ..., occurred_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class QuotaExceeded(_message.Message):
    __slots__ = ("team", "rate_plan_id", "meter_type", "quota_limit", "current_usage", "occurred_at")
    TEAM_FIELD_NUMBER: _ClassVar[int]
    RATE_PLAN_ID_FIELD_NUMBER: _ClassVar[int]
    METER_TYPE_FIELD_NUMBER: _ClassVar[int]
    QUOTA_LIMIT_FIELD_NUMBER: _ClassVar[int]
    CURRENT_USAGE_FIELD_NUMBER: _ClassVar[int]
    OCCURRED_AT_FIELD_NUMBER: _ClassVar[int]
    team: str
    rate_plan_id: str
    meter_type: MeterType
    quota_limit: int
    current_usage: int
    occurred_at: _timestamp_pb2.Timestamp
    def __init__(self, team: _Optional[str] = ..., rate_plan_id: _Optional[str] = ..., meter_type: _Optional[_Union[MeterType, str]] = ..., quota_limit: _Optional[int] = ..., current_usage: _Optional[int] = ..., occurred_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class InvoiceGenerated(_message.Message):
    __slots__ = ("invoice_id", "invoice_number", "team", "rate_plan_id", "period_start", "period_end", "total_micros", "currency_code", "finalized_at")
    INVOICE_ID_FIELD_NUMBER: _ClassVar[int]
    INVOICE_NUMBER_FIELD_NUMBER: _ClassVar[int]
    TEAM_FIELD_NUMBER: _ClassVar[int]
    RATE_PLAN_ID_FIELD_NUMBER: _ClassVar[int]
    PERIOD_START_FIELD_NUMBER: _ClassVar[int]
    PERIOD_END_FIELD_NUMBER: _ClassVar[int]
    TOTAL_MICROS_FIELD_NUMBER: _ClassVar[int]
    CURRENCY_CODE_FIELD_NUMBER: _ClassVar[int]
    FINALIZED_AT_FIELD_NUMBER: _ClassVar[int]
    invoice_id: str
    invoice_number: str
    team: str
    rate_plan_id: str
    period_start: _timestamp_pb2.Timestamp
    period_end: _timestamp_pb2.Timestamp
    total_micros: int
    currency_code: str
    finalized_at: _timestamp_pb2.Timestamp
    def __init__(self, invoice_id: _Optional[str] = ..., invoice_number: _Optional[str] = ..., team: _Optional[str] = ..., rate_plan_id: _Optional[str] = ..., period_start: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., period_end: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., total_micros: _Optional[int] = ..., currency_code: _Optional[str] = ..., finalized_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class RunCreated(_message.Message):
    __slots__ = ("run_id", "experiment_id", "model_version_id", "display_name", "started_at")
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    EXPERIMENT_ID_FIELD_NUMBER: _ClassVar[int]
    MODEL_VERSION_ID_FIELD_NUMBER: _ClassVar[int]
    DISPLAY_NAME_FIELD_NUMBER: _ClassVar[int]
    STARTED_AT_FIELD_NUMBER: _ClassVar[int]
    run_id: str
    experiment_id: str
    model_version_id: str
    display_name: str
    started_at: _timestamp_pb2.Timestamp
    def __init__(self, run_id: _Optional[str] = ..., experiment_id: _Optional[str] = ..., model_version_id: _Optional[str] = ..., display_name: _Optional[str] = ..., started_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class RunFinished(_message.Message):
    __slots__ = ("run_id", "experiment_id", "model_version_id", "status", "final_metrics", "ended_at")
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    EXPERIMENT_ID_FIELD_NUMBER: _ClassVar[int]
    MODEL_VERSION_ID_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    FINAL_METRICS_FIELD_NUMBER: _ClassVar[int]
    ENDED_AT_FIELD_NUMBER: _ClassVar[int]
    run_id: str
    experiment_id: str
    model_version_id: str
    status: RunStatus
    final_metrics: _containers.RepeatedCompositeFieldContainer[MetricPoint]
    ended_at: _timestamp_pb2.Timestamp
    def __init__(self, run_id: _Optional[str] = ..., experiment_id: _Optional[str] = ..., model_version_id: _Optional[str] = ..., status: _Optional[_Union[RunStatus, str]] = ..., final_metrics: _Optional[_Iterable[_Union[MetricPoint, _Mapping]]] = ..., ended_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class NotificationDelivered(_message.Message):
    __slots__ = ("notification_id", "recipient_user_id", "channel", "event_type", "delivered_at")
    NOTIFICATION_ID_FIELD_NUMBER: _ClassVar[int]
    RECIPIENT_USER_ID_FIELD_NUMBER: _ClassVar[int]
    CHANNEL_FIELD_NUMBER: _ClassVar[int]
    EVENT_TYPE_FIELD_NUMBER: _ClassVar[int]
    DELIVERED_AT_FIELD_NUMBER: _ClassVar[int]
    notification_id: str
    recipient_user_id: str
    channel: NotificationChannel
    event_type: str
    delivered_at: _timestamp_pb2.Timestamp
    def __init__(self, notification_id: _Optional[str] = ..., recipient_user_id: _Optional[str] = ..., channel: _Optional[_Union[NotificationChannel, str]] = ..., event_type: _Optional[str] = ..., delivered_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class NotificationFailed(_message.Message):
    __slots__ = ("notification_id", "recipient_user_id", "channel", "event_type", "attempts", "error_message", "failed_at")
    NOTIFICATION_ID_FIELD_NUMBER: _ClassVar[int]
    RECIPIENT_USER_ID_FIELD_NUMBER: _ClassVar[int]
    CHANNEL_FIELD_NUMBER: _ClassVar[int]
    EVENT_TYPE_FIELD_NUMBER: _ClassVar[int]
    ATTEMPTS_FIELD_NUMBER: _ClassVar[int]
    ERROR_MESSAGE_FIELD_NUMBER: _ClassVar[int]
    FAILED_AT_FIELD_NUMBER: _ClassVar[int]
    notification_id: str
    recipient_user_id: str
    channel: NotificationChannel
    event_type: str
    attempts: int
    error_message: str
    failed_at: _timestamp_pb2.Timestamp
    def __init__(self, notification_id: _Optional[str] = ..., recipient_user_id: _Optional[str] = ..., channel: _Optional[_Union[NotificationChannel, str]] = ..., event_type: _Optional[str] = ..., attempts: _Optional[int] = ..., error_message: _Optional[str] = ..., failed_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class UserCreated(_message.Message):
    __slots__ = ("user_id", "email", "name", "team", "role", "created_at")
    USER_ID_FIELD_NUMBER: _ClassVar[int]
    EMAIL_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    TEAM_FIELD_NUMBER: _ClassVar[int]
    ROLE_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_FIELD_NUMBER: _ClassVar[int]
    user_id: str
    email: str
    name: str
    team: str
    role: str
    created_at: _timestamp_pb2.Timestamp
    def __init__(self, user_id: _Optional[str] = ..., email: _Optional[str] = ..., name: _Optional[str] = ..., team: _Optional[str] = ..., role: _Optional[str] = ..., created_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class ApiKeyRotated(_message.Message):
    __slots__ = ("key_id", "user_id", "key_prefix", "scopes", "replaced_key_id", "rotated_at")
    KEY_ID_FIELD_NUMBER: _ClassVar[int]
    USER_ID_FIELD_NUMBER: _ClassVar[int]
    KEY_PREFIX_FIELD_NUMBER: _ClassVar[int]
    SCOPES_FIELD_NUMBER: _ClassVar[int]
    REPLACED_KEY_ID_FIELD_NUMBER: _ClassVar[int]
    ROTATED_AT_FIELD_NUMBER: _ClassVar[int]
    key_id: str
    user_id: str
    key_prefix: str
    scopes: _containers.RepeatedScalarFieldContainer[str]
    replaced_key_id: str
    rotated_at: _timestamp_pb2.Timestamp
    def __init__(self, key_id: _Optional[str] = ..., user_id: _Optional[str] = ..., key_prefix: _Optional[str] = ..., scopes: _Optional[_Iterable[str]] = ..., replaced_key_id: _Optional[str] = ..., rotated_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...
