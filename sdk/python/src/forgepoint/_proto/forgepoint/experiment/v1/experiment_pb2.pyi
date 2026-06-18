import datetime

from google.protobuf import timestamp_pb2 as _timestamp_pb2
from google.protobuf import struct_pb2 as _struct_pb2
from forgepoint.common.v1 import common_pb2 as _common_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class RunStatus(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    RUN_STATUS_UNSPECIFIED: _ClassVar[RunStatus]
    RUN_STATUS_RUNNING: _ClassVar[RunStatus]
    RUN_STATUS_FINISHED: _ClassVar[RunStatus]
    RUN_STATUS_FAILED: _ClassVar[RunStatus]
    RUN_STATUS_KILLED: _ClassVar[RunStatus]

class RunSource(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    RUN_SOURCE_UNSPECIFIED: _ClassVar[RunSource]
    RUN_SOURCE_API: _ClassVar[RunSource]
    RUN_SOURCE_EVENT: _ClassVar[RunSource]
RUN_STATUS_UNSPECIFIED: RunStatus
RUN_STATUS_RUNNING: RunStatus
RUN_STATUS_FINISHED: RunStatus
RUN_STATUS_FAILED: RunStatus
RUN_STATUS_KILLED: RunStatus
RUN_SOURCE_UNSPECIFIED: RunSource
RUN_SOURCE_API: RunSource
RUN_SOURCE_EVENT: RunSource

class Experiment(_message.Message):
    __slots__ = ("id", "name", "description", "tags", "owner_id", "team", "created_at", "updated_at", "archived_at")
    class TagsEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    ID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    TAGS_FIELD_NUMBER: _ClassVar[int]
    OWNER_ID_FIELD_NUMBER: _ClassVar[int]
    TEAM_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_FIELD_NUMBER: _ClassVar[int]
    UPDATED_AT_FIELD_NUMBER: _ClassVar[int]
    ARCHIVED_AT_FIELD_NUMBER: _ClassVar[int]
    id: str
    name: str
    description: str
    tags: _containers.ScalarMap[str, str]
    owner_id: str
    team: str
    created_at: _timestamp_pb2.Timestamp
    updated_at: _timestamp_pb2.Timestamp
    archived_at: _timestamp_pb2.Timestamp
    def __init__(self, id: _Optional[str] = ..., name: _Optional[str] = ..., description: _Optional[str] = ..., tags: _Optional[_Mapping[str, str]] = ..., owner_id: _Optional[str] = ..., team: _Optional[str] = ..., created_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., updated_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., archived_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class Run(_message.Message):
    __slots__ = ("id", "experiment_id", "display_name", "status", "source", "model_version_id", "owner_id", "params", "final_metrics", "started_at", "ended_at", "artifacts")
    ID_FIELD_NUMBER: _ClassVar[int]
    EXPERIMENT_ID_FIELD_NUMBER: _ClassVar[int]
    DISPLAY_NAME_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    SOURCE_FIELD_NUMBER: _ClassVar[int]
    MODEL_VERSION_ID_FIELD_NUMBER: _ClassVar[int]
    OWNER_ID_FIELD_NUMBER: _ClassVar[int]
    PARAMS_FIELD_NUMBER: _ClassVar[int]
    FINAL_METRICS_FIELD_NUMBER: _ClassVar[int]
    STARTED_AT_FIELD_NUMBER: _ClassVar[int]
    ENDED_AT_FIELD_NUMBER: _ClassVar[int]
    ARTIFACTS_FIELD_NUMBER: _ClassVar[int]
    id: str
    experiment_id: str
    display_name: str
    status: RunStatus
    source: RunSource
    model_version_id: str
    owner_id: str
    params: _containers.RepeatedCompositeFieldContainer[Param]
    final_metrics: _containers.RepeatedCompositeFieldContainer[MetricPoint]
    started_at: _timestamp_pb2.Timestamp
    ended_at: _timestamp_pb2.Timestamp
    artifacts: _struct_pb2.Struct
    def __init__(self, id: _Optional[str] = ..., experiment_id: _Optional[str] = ..., display_name: _Optional[str] = ..., status: _Optional[_Union[RunStatus, str]] = ..., source: _Optional[_Union[RunSource, str]] = ..., model_version_id: _Optional[str] = ..., owner_id: _Optional[str] = ..., params: _Optional[_Iterable[_Union[Param, _Mapping]]] = ..., final_metrics: _Optional[_Iterable[_Union[MetricPoint, _Mapping]]] = ..., started_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., ended_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., artifacts: _Optional[_Union[_struct_pb2.Struct, _Mapping]] = ...) -> None: ...

class Param(_message.Message):
    __slots__ = ("key", "value")
    KEY_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    key: str
    value: str
    def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...

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

class MetricSeries(_message.Message):
    __slots__ = ("key", "points")
    KEY_FIELD_NUMBER: _ClassVar[int]
    POINTS_FIELD_NUMBER: _ClassVar[int]
    key: str
    points: _containers.RepeatedCompositeFieldContainer[MetricPoint]
    def __init__(self, key: _Optional[str] = ..., points: _Optional[_Iterable[_Union[MetricPoint, _Mapping]]] = ...) -> None: ...

class CreateExperimentRequest(_message.Message):
    __slots__ = ("name", "description", "tags")
    class TagsEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    NAME_FIELD_NUMBER: _ClassVar[int]
    DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    TAGS_FIELD_NUMBER: _ClassVar[int]
    name: str
    description: str
    tags: _containers.ScalarMap[str, str]
    def __init__(self, name: _Optional[str] = ..., description: _Optional[str] = ..., tags: _Optional[_Mapping[str, str]] = ...) -> None: ...

class CreateExperimentResponse(_message.Message):
    __slots__ = ("experiment",)
    EXPERIMENT_FIELD_NUMBER: _ClassVar[int]
    experiment: Experiment
    def __init__(self, experiment: _Optional[_Union[Experiment, _Mapping]] = ...) -> None: ...

class GetExperimentRequest(_message.Message):
    __slots__ = ("id",)
    ID_FIELD_NUMBER: _ClassVar[int]
    id: str
    def __init__(self, id: _Optional[str] = ...) -> None: ...

class GetExperimentResponse(_message.Message):
    __slots__ = ("experiment",)
    EXPERIMENT_FIELD_NUMBER: _ClassVar[int]
    experiment: Experiment
    def __init__(self, experiment: _Optional[_Union[Experiment, _Mapping]] = ...) -> None: ...

class ListExperimentsRequest(_message.Message):
    __slots__ = ("team_filter", "include_archived", "pagination")
    TEAM_FILTER_FIELD_NUMBER: _ClassVar[int]
    INCLUDE_ARCHIVED_FIELD_NUMBER: _ClassVar[int]
    PAGINATION_FIELD_NUMBER: _ClassVar[int]
    team_filter: str
    include_archived: bool
    pagination: _common_pb2.PaginationRequest
    def __init__(self, team_filter: _Optional[str] = ..., include_archived: _Optional[bool] = ..., pagination: _Optional[_Union[_common_pb2.PaginationRequest, _Mapping]] = ...) -> None: ...

class ListExperimentsResponse(_message.Message):
    __slots__ = ("experiments", "pagination")
    EXPERIMENTS_FIELD_NUMBER: _ClassVar[int]
    PAGINATION_FIELD_NUMBER: _ClassVar[int]
    experiments: _containers.RepeatedCompositeFieldContainer[Experiment]
    pagination: _common_pb2.PaginationResponse
    def __init__(self, experiments: _Optional[_Iterable[_Union[Experiment, _Mapping]]] = ..., pagination: _Optional[_Union[_common_pb2.PaginationResponse, _Mapping]] = ...) -> None: ...

class UpdateExperimentRequest(_message.Message):
    __slots__ = ("id", "name", "description", "tags", "update_fields")
    class TagsEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    ID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    TAGS_FIELD_NUMBER: _ClassVar[int]
    UPDATE_FIELDS_FIELD_NUMBER: _ClassVar[int]
    id: str
    name: str
    description: str
    tags: _containers.ScalarMap[str, str]
    update_fields: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, id: _Optional[str] = ..., name: _Optional[str] = ..., description: _Optional[str] = ..., tags: _Optional[_Mapping[str, str]] = ..., update_fields: _Optional[_Iterable[str]] = ...) -> None: ...

class UpdateExperimentResponse(_message.Message):
    __slots__ = ("experiment",)
    EXPERIMENT_FIELD_NUMBER: _ClassVar[int]
    experiment: Experiment
    def __init__(self, experiment: _Optional[_Union[Experiment, _Mapping]] = ...) -> None: ...

class ArchiveExperimentRequest(_message.Message):
    __slots__ = ("id",)
    ID_FIELD_NUMBER: _ClassVar[int]
    id: str
    def __init__(self, id: _Optional[str] = ...) -> None: ...

class ArchiveExperimentResponse(_message.Message):
    __slots__ = ("experiment",)
    EXPERIMENT_FIELD_NUMBER: _ClassVar[int]
    experiment: Experiment
    def __init__(self, experiment: _Optional[_Union[Experiment, _Mapping]] = ...) -> None: ...

class StartRunRequest(_message.Message):
    __slots__ = ("experiment_id", "display_name", "model_version_id", "params", "idempotency_key")
    EXPERIMENT_ID_FIELD_NUMBER: _ClassVar[int]
    DISPLAY_NAME_FIELD_NUMBER: _ClassVar[int]
    MODEL_VERSION_ID_FIELD_NUMBER: _ClassVar[int]
    PARAMS_FIELD_NUMBER: _ClassVar[int]
    IDEMPOTENCY_KEY_FIELD_NUMBER: _ClassVar[int]
    experiment_id: str
    display_name: str
    model_version_id: str
    params: _containers.RepeatedCompositeFieldContainer[Param]
    idempotency_key: str
    def __init__(self, experiment_id: _Optional[str] = ..., display_name: _Optional[str] = ..., model_version_id: _Optional[str] = ..., params: _Optional[_Iterable[_Union[Param, _Mapping]]] = ..., idempotency_key: _Optional[str] = ...) -> None: ...

class StartRunResponse(_message.Message):
    __slots__ = ("run",)
    RUN_FIELD_NUMBER: _ClassVar[int]
    run: Run
    def __init__(self, run: _Optional[_Union[Run, _Mapping]] = ...) -> None: ...

class GetRunRequest(_message.Message):
    __slots__ = ("id",)
    ID_FIELD_NUMBER: _ClassVar[int]
    id: str
    def __init__(self, id: _Optional[str] = ...) -> None: ...

class GetRunResponse(_message.Message):
    __slots__ = ("run",)
    RUN_FIELD_NUMBER: _ClassVar[int]
    run: Run
    def __init__(self, run: _Optional[_Union[Run, _Mapping]] = ...) -> None: ...

class ListRunsRequest(_message.Message):
    __slots__ = ("experiment_id", "status_filter", "pagination")
    EXPERIMENT_ID_FIELD_NUMBER: _ClassVar[int]
    STATUS_FILTER_FIELD_NUMBER: _ClassVar[int]
    PAGINATION_FIELD_NUMBER: _ClassVar[int]
    experiment_id: str
    status_filter: RunStatus
    pagination: _common_pb2.PaginationRequest
    def __init__(self, experiment_id: _Optional[str] = ..., status_filter: _Optional[_Union[RunStatus, str]] = ..., pagination: _Optional[_Union[_common_pb2.PaginationRequest, _Mapping]] = ...) -> None: ...

class ListRunsResponse(_message.Message):
    __slots__ = ("runs", "pagination")
    RUNS_FIELD_NUMBER: _ClassVar[int]
    PAGINATION_FIELD_NUMBER: _ClassVar[int]
    runs: _containers.RepeatedCompositeFieldContainer[Run]
    pagination: _common_pb2.PaginationResponse
    def __init__(self, runs: _Optional[_Iterable[_Union[Run, _Mapping]]] = ..., pagination: _Optional[_Union[_common_pb2.PaginationResponse, _Mapping]] = ...) -> None: ...

class DeleteRunRequest(_message.Message):
    __slots__ = ("run_id", "idempotency_key")
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    IDEMPOTENCY_KEY_FIELD_NUMBER: _ClassVar[int]
    run_id: str
    idempotency_key: str
    def __init__(self, run_id: _Optional[str] = ..., idempotency_key: _Optional[str] = ...) -> None: ...

class DeleteRunResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class UpdateRunStatusRequest(_message.Message):
    __slots__ = ("run_id", "status")
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    run_id: str
    status: RunStatus
    def __init__(self, run_id: _Optional[str] = ..., status: _Optional[_Union[RunStatus, str]] = ...) -> None: ...

class UpdateRunStatusResponse(_message.Message):
    __slots__ = ("run",)
    RUN_FIELD_NUMBER: _ClassVar[int]
    run: Run
    def __init__(self, run: _Optional[_Union[Run, _Mapping]] = ...) -> None: ...

class LogMetricsRequest(_message.Message):
    __slots__ = ("run_id", "points", "idempotency_key")
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    POINTS_FIELD_NUMBER: _ClassVar[int]
    IDEMPOTENCY_KEY_FIELD_NUMBER: _ClassVar[int]
    run_id: str
    points: _containers.RepeatedCompositeFieldContainer[MetricPoint]
    idempotency_key: str
    def __init__(self, run_id: _Optional[str] = ..., points: _Optional[_Iterable[_Union[MetricPoint, _Mapping]]] = ..., idempotency_key: _Optional[str] = ...) -> None: ...

class LogMetricsResponse(_message.Message):
    __slots__ = ("accepted_count",)
    ACCEPTED_COUNT_FIELD_NUMBER: _ClassVar[int]
    accepted_count: int
    def __init__(self, accepted_count: _Optional[int] = ...) -> None: ...

class LogParamsRequest(_message.Message):
    __slots__ = ("run_id", "params")
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    PARAMS_FIELD_NUMBER: _ClassVar[int]
    run_id: str
    params: _containers.RepeatedCompositeFieldContainer[Param]
    def __init__(self, run_id: _Optional[str] = ..., params: _Optional[_Iterable[_Union[Param, _Mapping]]] = ...) -> None: ...

class LogParamsResponse(_message.Message):
    __slots__ = ("accepted_count",)
    ACCEPTED_COUNT_FIELD_NUMBER: _ClassVar[int]
    accepted_count: int
    def __init__(self, accepted_count: _Optional[int] = ...) -> None: ...

class CompareRunsRequest(_message.Message):
    __slots__ = ("run_ids", "metric_keys")
    RUN_IDS_FIELD_NUMBER: _ClassVar[int]
    METRIC_KEYS_FIELD_NUMBER: _ClassVar[int]
    run_ids: _containers.RepeatedScalarFieldContainer[str]
    metric_keys: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, run_ids: _Optional[_Iterable[str]] = ..., metric_keys: _Optional[_Iterable[str]] = ...) -> None: ...

class RunComparison(_message.Message):
    __slots__ = ("run", "series")
    RUN_FIELD_NUMBER: _ClassVar[int]
    SERIES_FIELD_NUMBER: _ClassVar[int]
    run: Run
    series: _containers.RepeatedCompositeFieldContainer[MetricSeries]
    def __init__(self, run: _Optional[_Union[Run, _Mapping]] = ..., series: _Optional[_Iterable[_Union[MetricSeries, _Mapping]]] = ...) -> None: ...

class CompareRunsResponse(_message.Message):
    __slots__ = ("comparisons",)
    COMPARISONS_FIELD_NUMBER: _ClassVar[int]
    comparisons: _containers.RepeatedCompositeFieldContainer[RunComparison]
    def __init__(self, comparisons: _Optional[_Iterable[_Union[RunComparison, _Mapping]]] = ...) -> None: ...

class GetMetricHistoryRequest(_message.Message):
    __slots__ = ("run_id", "metric_keys", "min_step", "max_step", "pagination")
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    METRIC_KEYS_FIELD_NUMBER: _ClassVar[int]
    MIN_STEP_FIELD_NUMBER: _ClassVar[int]
    MAX_STEP_FIELD_NUMBER: _ClassVar[int]
    PAGINATION_FIELD_NUMBER: _ClassVar[int]
    run_id: str
    metric_keys: _containers.RepeatedScalarFieldContainer[str]
    min_step: int
    max_step: int
    pagination: _common_pb2.PaginationRequest
    def __init__(self, run_id: _Optional[str] = ..., metric_keys: _Optional[_Iterable[str]] = ..., min_step: _Optional[int] = ..., max_step: _Optional[int] = ..., pagination: _Optional[_Union[_common_pb2.PaginationRequest, _Mapping]] = ...) -> None: ...

class GetMetricHistoryResponse(_message.Message):
    __slots__ = ("series", "pagination")
    SERIES_FIELD_NUMBER: _ClassVar[int]
    PAGINATION_FIELD_NUMBER: _ClassVar[int]
    series: _containers.RepeatedCompositeFieldContainer[MetricSeries]
    pagination: _common_pb2.PaginationResponse
    def __init__(self, series: _Optional[_Iterable[_Union[MetricSeries, _Mapping]]] = ..., pagination: _Optional[_Union[_common_pb2.PaginationResponse, _Mapping]] = ...) -> None: ...

class SetRunArtifactsRequest(_message.Message):
    __slots__ = ("run_id", "artifacts", "idempotency_key")
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    ARTIFACTS_FIELD_NUMBER: _ClassVar[int]
    IDEMPOTENCY_KEY_FIELD_NUMBER: _ClassVar[int]
    run_id: str
    artifacts: _struct_pb2.Struct
    idempotency_key: str
    def __init__(self, run_id: _Optional[str] = ..., artifacts: _Optional[_Union[_struct_pb2.Struct, _Mapping]] = ..., idempotency_key: _Optional[str] = ...) -> None: ...

class SetRunArtifactsResponse(_message.Message):
    __slots__ = ("run",)
    RUN_FIELD_NUMBER: _ClassVar[int]
    run: Run
    def __init__(self, run: _Optional[_Union[Run, _Mapping]] = ...) -> None: ...
