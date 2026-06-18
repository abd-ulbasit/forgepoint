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

class MonitorState(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    MONITOR_STATE_UNSPECIFIED: _ClassVar[MonitorState]
    MONITOR_STATE_PENDING_BASELINE: _ClassVar[MonitorState]
    MONITOR_STATE_WARMING_UP: _ClassVar[MonitorState]
    MONITOR_STATE_ACTIVE: _ClassVar[MonitorState]
    MONITOR_STATE_PAUSED: _ClassVar[MonitorState]
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
MONITOR_STATE_UNSPECIFIED: MonitorState
MONITOR_STATE_PENDING_BASELINE: MonitorState
MONITOR_STATE_WARMING_UP: MonitorState
MONITOR_STATE_ACTIVE: MonitorState
MONITOR_STATE_PAUSED: MonitorState

class ThresholdConfig(_message.Message):
    __slots__ = ("drift_type", "method", "warn_score", "critical_score")
    DRIFT_TYPE_FIELD_NUMBER: _ClassVar[int]
    METHOD_FIELD_NUMBER: _ClassVar[int]
    WARN_SCORE_FIELD_NUMBER: _ClassVar[int]
    CRITICAL_SCORE_FIELD_NUMBER: _ClassVar[int]
    drift_type: DriftType
    method: DriftMethod
    warn_score: float
    critical_score: float
    def __init__(self, drift_type: _Optional[_Union[DriftType, str]] = ..., method: _Optional[_Union[DriftMethod, str]] = ..., warn_score: _Optional[float] = ..., critical_score: _Optional[float] = ...) -> None: ...

class Monitor(_message.Message):
    __slots__ = ("id", "model_name", "owner_team", "window_duration", "window_size", "min_samples", "thresholds", "auto_retrain", "retrain_pipeline_id", "state", "baseline_version", "baseline_captured_at", "created_at", "updated_at")
    ID_FIELD_NUMBER: _ClassVar[int]
    MODEL_NAME_FIELD_NUMBER: _ClassVar[int]
    OWNER_TEAM_FIELD_NUMBER: _ClassVar[int]
    WINDOW_DURATION_FIELD_NUMBER: _ClassVar[int]
    WINDOW_SIZE_FIELD_NUMBER: _ClassVar[int]
    MIN_SAMPLES_FIELD_NUMBER: _ClassVar[int]
    THRESHOLDS_FIELD_NUMBER: _ClassVar[int]
    AUTO_RETRAIN_FIELD_NUMBER: _ClassVar[int]
    RETRAIN_PIPELINE_ID_FIELD_NUMBER: _ClassVar[int]
    STATE_FIELD_NUMBER: _ClassVar[int]
    BASELINE_VERSION_FIELD_NUMBER: _ClassVar[int]
    BASELINE_CAPTURED_AT_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_FIELD_NUMBER: _ClassVar[int]
    UPDATED_AT_FIELD_NUMBER: _ClassVar[int]
    id: str
    model_name: str
    owner_team: str
    window_duration: _duration_pb2.Duration
    window_size: int
    min_samples: int
    thresholds: _containers.RepeatedCompositeFieldContainer[ThresholdConfig]
    auto_retrain: bool
    retrain_pipeline_id: str
    state: MonitorState
    baseline_version: str
    baseline_captured_at: _timestamp_pb2.Timestamp
    created_at: _timestamp_pb2.Timestamp
    updated_at: _timestamp_pb2.Timestamp
    def __init__(self, id: _Optional[str] = ..., model_name: _Optional[str] = ..., owner_team: _Optional[str] = ..., window_duration: _Optional[_Union[datetime.timedelta, _duration_pb2.Duration, _Mapping]] = ..., window_size: _Optional[int] = ..., min_samples: _Optional[int] = ..., thresholds: _Optional[_Iterable[_Union[ThresholdConfig, _Mapping]]] = ..., auto_retrain: _Optional[bool] = ..., retrain_pipeline_id: _Optional[str] = ..., state: _Optional[_Union[MonitorState, str]] = ..., baseline_version: _Optional[str] = ..., baseline_captured_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., created_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., updated_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

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

class DriftReport(_message.Message):
    __slots__ = ("id", "monitor_id", "model_name", "model_version", "drift_type", "severity", "metrics", "window_id", "sample_count", "window_start", "window_end", "created_at")
    ID_FIELD_NUMBER: _ClassVar[int]
    MONITOR_ID_FIELD_NUMBER: _ClassVar[int]
    MODEL_NAME_FIELD_NUMBER: _ClassVar[int]
    MODEL_VERSION_FIELD_NUMBER: _ClassVar[int]
    DRIFT_TYPE_FIELD_NUMBER: _ClassVar[int]
    SEVERITY_FIELD_NUMBER: _ClassVar[int]
    METRICS_FIELD_NUMBER: _ClassVar[int]
    WINDOW_ID_FIELD_NUMBER: _ClassVar[int]
    SAMPLE_COUNT_FIELD_NUMBER: _ClassVar[int]
    WINDOW_START_FIELD_NUMBER: _ClassVar[int]
    WINDOW_END_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_FIELD_NUMBER: _ClassVar[int]
    id: str
    monitor_id: str
    model_name: str
    model_version: str
    drift_type: DriftType
    severity: DriftSeverity
    metrics: _containers.RepeatedCompositeFieldContainer[DriftMetric]
    window_id: str
    sample_count: int
    window_start: _timestamp_pb2.Timestamp
    window_end: _timestamp_pb2.Timestamp
    created_at: _timestamp_pb2.Timestamp
    def __init__(self, id: _Optional[str] = ..., monitor_id: _Optional[str] = ..., model_name: _Optional[str] = ..., model_version: _Optional[str] = ..., drift_type: _Optional[_Union[DriftType, str]] = ..., severity: _Optional[_Union[DriftSeverity, str]] = ..., metrics: _Optional[_Iterable[_Union[DriftMetric, _Mapping]]] = ..., window_id: _Optional[str] = ..., sample_count: _Optional[int] = ..., window_start: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., window_end: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., created_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class MonitorStatus(_message.Message):
    __slots__ = ("monitor", "state", "current_window_samples", "latest_report", "last_event_at", "drift_events_total")
    MONITOR_FIELD_NUMBER: _ClassVar[int]
    STATE_FIELD_NUMBER: _ClassVar[int]
    CURRENT_WINDOW_SAMPLES_FIELD_NUMBER: _ClassVar[int]
    LATEST_REPORT_FIELD_NUMBER: _ClassVar[int]
    LAST_EVENT_AT_FIELD_NUMBER: _ClassVar[int]
    DRIFT_EVENTS_TOTAL_FIELD_NUMBER: _ClassVar[int]
    monitor: Monitor
    state: MonitorState
    current_window_samples: int
    latest_report: DriftReport
    last_event_at: _timestamp_pb2.Timestamp
    drift_events_total: int
    def __init__(self, monitor: _Optional[_Union[Monitor, _Mapping]] = ..., state: _Optional[_Union[MonitorState, str]] = ..., current_window_samples: _Optional[int] = ..., latest_report: _Optional[_Union[DriftReport, _Mapping]] = ..., last_event_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., drift_events_total: _Optional[int] = ...) -> None: ...

class ModelHealth(_message.Message):
    __slots__ = ("model_name", "model_version", "overall_severity", "severity_by_type", "state", "latest_report_id", "last_event_at", "drift_events_total")
    class SeverityByTypeEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: int
        value: DriftSeverity
        def __init__(self, key: _Optional[int] = ..., value: _Optional[_Union[DriftSeverity, str]] = ...) -> None: ...
    MODEL_NAME_FIELD_NUMBER: _ClassVar[int]
    MODEL_VERSION_FIELD_NUMBER: _ClassVar[int]
    OVERALL_SEVERITY_FIELD_NUMBER: _ClassVar[int]
    SEVERITY_BY_TYPE_FIELD_NUMBER: _ClassVar[int]
    STATE_FIELD_NUMBER: _ClassVar[int]
    LATEST_REPORT_ID_FIELD_NUMBER: _ClassVar[int]
    LAST_EVENT_AT_FIELD_NUMBER: _ClassVar[int]
    DRIFT_EVENTS_TOTAL_FIELD_NUMBER: _ClassVar[int]
    model_name: str
    model_version: str
    overall_severity: DriftSeverity
    severity_by_type: _containers.ScalarMap[int, DriftSeverity]
    state: MonitorState
    latest_report_id: str
    last_event_at: _timestamp_pb2.Timestamp
    drift_events_total: int
    def __init__(self, model_name: _Optional[str] = ..., model_version: _Optional[str] = ..., overall_severity: _Optional[_Union[DriftSeverity, str]] = ..., severity_by_type: _Optional[_Mapping[int, DriftSeverity]] = ..., state: _Optional[_Union[MonitorState, str]] = ..., latest_report_id: _Optional[str] = ..., last_event_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., drift_events_total: _Optional[int] = ...) -> None: ...

class GroundTruthLabel(_message.Message):
    __slots__ = ("request_id", "actual_label", "observed_at")
    REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
    ACTUAL_LABEL_FIELD_NUMBER: _ClassVar[int]
    OBSERVED_AT_FIELD_NUMBER: _ClassVar[int]
    request_id: str
    actual_label: str
    observed_at: _timestamp_pb2.Timestamp
    def __init__(self, request_id: _Optional[str] = ..., actual_label: _Optional[str] = ..., observed_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class ConfigureMonitorRequest(_message.Message):
    __slots__ = ("model_name", "window_duration", "window_size", "min_samples", "thresholds", "auto_retrain", "retrain_pipeline_id", "enabled", "idempotency_key")
    MODEL_NAME_FIELD_NUMBER: _ClassVar[int]
    WINDOW_DURATION_FIELD_NUMBER: _ClassVar[int]
    WINDOW_SIZE_FIELD_NUMBER: _ClassVar[int]
    MIN_SAMPLES_FIELD_NUMBER: _ClassVar[int]
    THRESHOLDS_FIELD_NUMBER: _ClassVar[int]
    AUTO_RETRAIN_FIELD_NUMBER: _ClassVar[int]
    RETRAIN_PIPELINE_ID_FIELD_NUMBER: _ClassVar[int]
    ENABLED_FIELD_NUMBER: _ClassVar[int]
    IDEMPOTENCY_KEY_FIELD_NUMBER: _ClassVar[int]
    model_name: str
    window_duration: _duration_pb2.Duration
    window_size: int
    min_samples: int
    thresholds: _containers.RepeatedCompositeFieldContainer[ThresholdConfig]
    auto_retrain: bool
    retrain_pipeline_id: str
    enabled: bool
    idempotency_key: str
    def __init__(self, model_name: _Optional[str] = ..., window_duration: _Optional[_Union[datetime.timedelta, _duration_pb2.Duration, _Mapping]] = ..., window_size: _Optional[int] = ..., min_samples: _Optional[int] = ..., thresholds: _Optional[_Iterable[_Union[ThresholdConfig, _Mapping]]] = ..., auto_retrain: _Optional[bool] = ..., retrain_pipeline_id: _Optional[str] = ..., enabled: _Optional[bool] = ..., idempotency_key: _Optional[str] = ...) -> None: ...

class ConfigureMonitorResponse(_message.Message):
    __slots__ = ("monitor", "created")
    MONITOR_FIELD_NUMBER: _ClassVar[int]
    CREATED_FIELD_NUMBER: _ClassVar[int]
    monitor: Monitor
    created: bool
    def __init__(self, monitor: _Optional[_Union[Monitor, _Mapping]] = ..., created: _Optional[bool] = ...) -> None: ...

class GetMonitorStatusRequest(_message.Message):
    __slots__ = ("model_name",)
    MODEL_NAME_FIELD_NUMBER: _ClassVar[int]
    model_name: str
    def __init__(self, model_name: _Optional[str] = ...) -> None: ...

class GetMonitorStatusResponse(_message.Message):
    __slots__ = ("status",)
    STATUS_FIELD_NUMBER: _ClassVar[int]
    status: MonitorStatus
    def __init__(self, status: _Optional[_Union[MonitorStatus, _Mapping]] = ...) -> None: ...

class GetModelHealthRequest(_message.Message):
    __slots__ = ("model_name",)
    MODEL_NAME_FIELD_NUMBER: _ClassVar[int]
    model_name: str
    def __init__(self, model_name: _Optional[str] = ...) -> None: ...

class GetModelHealthResponse(_message.Message):
    __slots__ = ("health",)
    HEALTH_FIELD_NUMBER: _ClassVar[int]
    health: ModelHealth
    def __init__(self, health: _Optional[_Union[ModelHealth, _Mapping]] = ...) -> None: ...

class ListMonitorsRequest(_message.Message):
    __slots__ = ("min_severity", "state", "pagination")
    MIN_SEVERITY_FIELD_NUMBER: _ClassVar[int]
    STATE_FIELD_NUMBER: _ClassVar[int]
    PAGINATION_FIELD_NUMBER: _ClassVar[int]
    min_severity: DriftSeverity
    state: MonitorState
    pagination: _common_pb2.PaginationRequest
    def __init__(self, min_severity: _Optional[_Union[DriftSeverity, str]] = ..., state: _Optional[_Union[MonitorState, str]] = ..., pagination: _Optional[_Union[_common_pb2.PaginationRequest, _Mapping]] = ...) -> None: ...

class ListMonitorsResponse(_message.Message):
    __slots__ = ("entries", "pagination")
    class Entry(_message.Message):
        __slots__ = ("monitor", "health")
        MONITOR_FIELD_NUMBER: _ClassVar[int]
        HEALTH_FIELD_NUMBER: _ClassVar[int]
        monitor: Monitor
        health: ModelHealth
        def __init__(self, monitor: _Optional[_Union[Monitor, _Mapping]] = ..., health: _Optional[_Union[ModelHealth, _Mapping]] = ...) -> None: ...
    ENTRIES_FIELD_NUMBER: _ClassVar[int]
    PAGINATION_FIELD_NUMBER: _ClassVar[int]
    entries: _containers.RepeatedCompositeFieldContainer[ListMonitorsResponse.Entry]
    pagination: _common_pb2.PaginationResponse
    def __init__(self, entries: _Optional[_Iterable[_Union[ListMonitorsResponse.Entry, _Mapping]]] = ..., pagination: _Optional[_Union[_common_pb2.PaginationResponse, _Mapping]] = ...) -> None: ...

class DeleteMonitorRequest(_message.Message):
    __slots__ = ("model_name", "purge_reports", "idempotency_key")
    MODEL_NAME_FIELD_NUMBER: _ClassVar[int]
    PURGE_REPORTS_FIELD_NUMBER: _ClassVar[int]
    IDEMPOTENCY_KEY_FIELD_NUMBER: _ClassVar[int]
    model_name: str
    purge_reports: bool
    idempotency_key: str
    def __init__(self, model_name: _Optional[str] = ..., purge_reports: _Optional[bool] = ..., idempotency_key: _Optional[str] = ...) -> None: ...

class DeleteMonitorResponse(_message.Message):
    __slots__ = ("purged_report_count",)
    PURGED_REPORT_COUNT_FIELD_NUMBER: _ClassVar[int]
    purged_report_count: int
    def __init__(self, purged_report_count: _Optional[int] = ...) -> None: ...

class ResetBaselineRequest(_message.Message):
    __slots__ = ("model_name", "baseline_version", "idempotency_key")
    MODEL_NAME_FIELD_NUMBER: _ClassVar[int]
    BASELINE_VERSION_FIELD_NUMBER: _ClassVar[int]
    IDEMPOTENCY_KEY_FIELD_NUMBER: _ClassVar[int]
    model_name: str
    baseline_version: str
    idempotency_key: str
    def __init__(self, model_name: _Optional[str] = ..., baseline_version: _Optional[str] = ..., idempotency_key: _Optional[str] = ...) -> None: ...

class ResetBaselineResponse(_message.Message):
    __slots__ = ("monitor",)
    MONITOR_FIELD_NUMBER: _ClassVar[int]
    monitor: Monitor
    def __init__(self, monitor: _Optional[_Union[Monitor, _Mapping]] = ...) -> None: ...

class GetDriftReportRequest(_message.Message):
    __slots__ = ("report_id",)
    REPORT_ID_FIELD_NUMBER: _ClassVar[int]
    report_id: str
    def __init__(self, report_id: _Optional[str] = ...) -> None: ...

class GetDriftReportResponse(_message.Message):
    __slots__ = ("report",)
    REPORT_FIELD_NUMBER: _ClassVar[int]
    report: DriftReport
    def __init__(self, report: _Optional[_Union[DriftReport, _Mapping]] = ...) -> None: ...

class ListDriftReportsRequest(_message.Message):
    __slots__ = ("model_name", "min_severity", "since", "until", "pagination")
    MODEL_NAME_FIELD_NUMBER: _ClassVar[int]
    MIN_SEVERITY_FIELD_NUMBER: _ClassVar[int]
    SINCE_FIELD_NUMBER: _ClassVar[int]
    UNTIL_FIELD_NUMBER: _ClassVar[int]
    PAGINATION_FIELD_NUMBER: _ClassVar[int]
    model_name: str
    min_severity: DriftSeverity
    since: _timestamp_pb2.Timestamp
    until: _timestamp_pb2.Timestamp
    pagination: _common_pb2.PaginationRequest
    def __init__(self, model_name: _Optional[str] = ..., min_severity: _Optional[_Union[DriftSeverity, str]] = ..., since: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., until: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., pagination: _Optional[_Union[_common_pb2.PaginationRequest, _Mapping]] = ...) -> None: ...

class ListDriftReportsResponse(_message.Message):
    __slots__ = ("reports", "pagination")
    REPORTS_FIELD_NUMBER: _ClassVar[int]
    PAGINATION_FIELD_NUMBER: _ClassVar[int]
    reports: _containers.RepeatedCompositeFieldContainer[DriftReport]
    pagination: _common_pb2.PaginationResponse
    def __init__(self, reports: _Optional[_Iterable[_Union[DriftReport, _Mapping]]] = ..., pagination: _Optional[_Union[_common_pb2.PaginationResponse, _Mapping]] = ...) -> None: ...

class SubmitGroundTruthRequest(_message.Message):
    __slots__ = ("model_name", "labels", "idempotency_key")
    MODEL_NAME_FIELD_NUMBER: _ClassVar[int]
    LABELS_FIELD_NUMBER: _ClassVar[int]
    IDEMPOTENCY_KEY_FIELD_NUMBER: _ClassVar[int]
    model_name: str
    labels: _containers.RepeatedCompositeFieldContainer[GroundTruthLabel]
    idempotency_key: str
    def __init__(self, model_name: _Optional[str] = ..., labels: _Optional[_Iterable[_Union[GroundTruthLabel, _Mapping]]] = ..., idempotency_key: _Optional[str] = ...) -> None: ...

class SubmitGroundTruthResponse(_message.Message):
    __slots__ = ("accepted", "unmatched_request_ids")
    ACCEPTED_FIELD_NUMBER: _ClassVar[int]
    UNMATCHED_REQUEST_IDS_FIELD_NUMBER: _ClassVar[int]
    accepted: int
    unmatched_request_ids: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, accepted: _Optional[int] = ..., unmatched_request_ids: _Optional[_Iterable[str]] = ...) -> None: ...

class StreamDriftEventsRequest(_message.Message):
    __slots__ = ("model_name", "min_severity")
    MODEL_NAME_FIELD_NUMBER: _ClassVar[int]
    MIN_SEVERITY_FIELD_NUMBER: _ClassVar[int]
    model_name: str
    min_severity: DriftSeverity
    def __init__(self, model_name: _Optional[str] = ..., min_severity: _Optional[_Union[DriftSeverity, str]] = ...) -> None: ...

class StreamDriftEventsResponse(_message.Message):
    __slots__ = ("report",)
    REPORT_FIELD_NUMBER: _ClassVar[int]
    report: DriftReport
    def __init__(self, report: _Optional[_Union[DriftReport, _Mapping]] = ...) -> None: ...
