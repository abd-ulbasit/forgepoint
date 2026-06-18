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

class FeatureValueType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    FEATURE_VALUE_TYPE_UNSPECIFIED: _ClassVar[FeatureValueType]
    FEATURE_VALUE_TYPE_INT64: _ClassVar[FeatureValueType]
    FEATURE_VALUE_TYPE_DOUBLE: _ClassVar[FeatureValueType]
    FEATURE_VALUE_TYPE_STRING: _ClassVar[FeatureValueType]
    FEATURE_VALUE_TYPE_BOOL: _ClassVar[FeatureValueType]
    FEATURE_VALUE_TYPE_TIMESTAMP: _ClassVar[FeatureValueType]
    FEATURE_VALUE_TYPE_DOUBLE_LIST: _ClassVar[FeatureValueType]
    FEATURE_VALUE_TYPE_STRUCT: _ClassVar[FeatureValueType]

class RebuildTarget(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    REBUILD_TARGET_UNSPECIFIED: _ClassVar[RebuildTarget]
    REBUILD_TARGET_ALL: _ClassVar[RebuildTarget]
    REBUILD_TARGET_ONLINE: _ClassVar[RebuildTarget]
    REBUILD_TARGET_OFFLINE: _ClassVar[RebuildTarget]
FEATURE_VALUE_TYPE_UNSPECIFIED: FeatureValueType
FEATURE_VALUE_TYPE_INT64: FeatureValueType
FEATURE_VALUE_TYPE_DOUBLE: FeatureValueType
FEATURE_VALUE_TYPE_STRING: FeatureValueType
FEATURE_VALUE_TYPE_BOOL: FeatureValueType
FEATURE_VALUE_TYPE_TIMESTAMP: FeatureValueType
FEATURE_VALUE_TYPE_DOUBLE_LIST: FeatureValueType
FEATURE_VALUE_TYPE_STRUCT: FeatureValueType
REBUILD_TARGET_UNSPECIFIED: RebuildTarget
REBUILD_TARGET_ALL: RebuildTarget
REBUILD_TARGET_ONLINE: RebuildTarget
REBUILD_TARGET_OFFLINE: RebuildTarget

class Entity(_message.Message):
    __slots__ = ("name", "join_key", "description")
    NAME_FIELD_NUMBER: _ClassVar[int]
    JOIN_KEY_FIELD_NUMBER: _ClassVar[int]
    DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    name: str
    join_key: str
    description: str
    def __init__(self, name: _Optional[str] = ..., join_key: _Optional[str] = ..., description: _Optional[str] = ...) -> None: ...

class FeatureSpec(_message.Message):
    __slots__ = ("name", "value_type", "description", "dimension")
    NAME_FIELD_NUMBER: _ClassVar[int]
    VALUE_TYPE_FIELD_NUMBER: _ClassVar[int]
    DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    DIMENSION_FIELD_NUMBER: _ClassVar[int]
    name: str
    value_type: FeatureValueType
    description: str
    dimension: int
    def __init__(self, name: _Optional[str] = ..., value_type: _Optional[_Union[FeatureValueType, str]] = ..., description: _Optional[str] = ..., dimension: _Optional[int] = ...) -> None: ...

class FeatureView(_message.Message):
    __slots__ = ("id", "name", "description", "entity", "features", "schema_version", "owner_user_id", "owner_team", "created_at", "updated_at", "deleted_at")
    ID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    ENTITY_FIELD_NUMBER: _ClassVar[int]
    FEATURES_FIELD_NUMBER: _ClassVar[int]
    SCHEMA_VERSION_FIELD_NUMBER: _ClassVar[int]
    OWNER_USER_ID_FIELD_NUMBER: _ClassVar[int]
    OWNER_TEAM_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_FIELD_NUMBER: _ClassVar[int]
    UPDATED_AT_FIELD_NUMBER: _ClassVar[int]
    DELETED_AT_FIELD_NUMBER: _ClassVar[int]
    id: str
    name: str
    description: str
    entity: Entity
    features: _containers.RepeatedCompositeFieldContainer[FeatureSpec]
    schema_version: int
    owner_user_id: str
    owner_team: str
    created_at: _timestamp_pb2.Timestamp
    updated_at: _timestamp_pb2.Timestamp
    deleted_at: _timestamp_pb2.Timestamp
    def __init__(self, id: _Optional[str] = ..., name: _Optional[str] = ..., description: _Optional[str] = ..., entity: _Optional[_Union[Entity, _Mapping]] = ..., features: _Optional[_Iterable[_Union[FeatureSpec, _Mapping]]] = ..., schema_version: _Optional[int] = ..., owner_user_id: _Optional[str] = ..., owner_team: _Optional[str] = ..., created_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., updated_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., deleted_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class FeatureValues(_message.Message):
    __slots__ = ("entity_id", "values", "event_time")
    class ValuesEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: _struct_pb2.Value
        def __init__(self, key: _Optional[str] = ..., value: _Optional[_Union[_struct_pb2.Value, _Mapping]] = ...) -> None: ...
    ENTITY_ID_FIELD_NUMBER: _ClassVar[int]
    VALUES_FIELD_NUMBER: _ClassVar[int]
    EVENT_TIME_FIELD_NUMBER: _ClassVar[int]
    entity_id: str
    values: _containers.MessageMap[str, _struct_pb2.Value]
    event_time: _timestamp_pb2.Timestamp
    def __init__(self, entity_id: _Optional[str] = ..., values: _Optional[_Mapping[str, _struct_pb2.Value]] = ..., event_time: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class FeatureVector(_message.Message):
    __slots__ = ("entity_id", "values", "as_of_version", "event_time", "schema_version")
    class ValuesEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: _struct_pb2.Value
        def __init__(self, key: _Optional[str] = ..., value: _Optional[_Union[_struct_pb2.Value, _Mapping]] = ...) -> None: ...
    ENTITY_ID_FIELD_NUMBER: _ClassVar[int]
    VALUES_FIELD_NUMBER: _ClassVar[int]
    AS_OF_VERSION_FIELD_NUMBER: _ClassVar[int]
    EVENT_TIME_FIELD_NUMBER: _ClassVar[int]
    SCHEMA_VERSION_FIELD_NUMBER: _ClassVar[int]
    entity_id: str
    values: _containers.MessageMap[str, _struct_pb2.Value]
    as_of_version: int
    event_time: _timestamp_pb2.Timestamp
    schema_version: int
    def __init__(self, entity_id: _Optional[str] = ..., values: _Optional[_Mapping[str, _struct_pb2.Value]] = ..., as_of_version: _Optional[int] = ..., event_time: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., schema_version: _Optional[int] = ...) -> None: ...

class DefineFeatureViewRequest(_message.Message):
    __slots__ = ("name", "description", "entity", "features", "idempotency_key")
    NAME_FIELD_NUMBER: _ClassVar[int]
    DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    ENTITY_FIELD_NUMBER: _ClassVar[int]
    FEATURES_FIELD_NUMBER: _ClassVar[int]
    IDEMPOTENCY_KEY_FIELD_NUMBER: _ClassVar[int]
    name: str
    description: str
    entity: Entity
    features: _containers.RepeatedCompositeFieldContainer[FeatureSpec]
    idempotency_key: str
    def __init__(self, name: _Optional[str] = ..., description: _Optional[str] = ..., entity: _Optional[_Union[Entity, _Mapping]] = ..., features: _Optional[_Iterable[_Union[FeatureSpec, _Mapping]]] = ..., idempotency_key: _Optional[str] = ...) -> None: ...

class DefineFeatureViewResponse(_message.Message):
    __slots__ = ("feature_view",)
    FEATURE_VIEW_FIELD_NUMBER: _ClassVar[int]
    feature_view: FeatureView
    def __init__(self, feature_view: _Optional[_Union[FeatureView, _Mapping]] = ...) -> None: ...

class GetFeatureViewRequest(_message.Message):
    __slots__ = ("feature_view_id", "name")
    FEATURE_VIEW_ID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    feature_view_id: str
    name: str
    def __init__(self, feature_view_id: _Optional[str] = ..., name: _Optional[str] = ...) -> None: ...

class GetFeatureViewResponse(_message.Message):
    __slots__ = ("feature_view",)
    FEATURE_VIEW_FIELD_NUMBER: _ClassVar[int]
    feature_view: FeatureView
    def __init__(self, feature_view: _Optional[_Union[FeatureView, _Mapping]] = ...) -> None: ...

class ListFeatureViewsRequest(_message.Message):
    __slots__ = ("name_filter", "pagination")
    NAME_FILTER_FIELD_NUMBER: _ClassVar[int]
    PAGINATION_FIELD_NUMBER: _ClassVar[int]
    name_filter: str
    pagination: _common_pb2.PaginationRequest
    def __init__(self, name_filter: _Optional[str] = ..., pagination: _Optional[_Union[_common_pb2.PaginationRequest, _Mapping]] = ...) -> None: ...

class ListFeatureViewsResponse(_message.Message):
    __slots__ = ("feature_views", "pagination")
    FEATURE_VIEWS_FIELD_NUMBER: _ClassVar[int]
    PAGINATION_FIELD_NUMBER: _ClassVar[int]
    feature_views: _containers.RepeatedCompositeFieldContainer[FeatureView]
    pagination: _common_pb2.PaginationResponse
    def __init__(self, feature_views: _Optional[_Iterable[_Union[FeatureView, _Mapping]]] = ..., pagination: _Optional[_Union[_common_pb2.PaginationResponse, _Mapping]] = ...) -> None: ...

class WriteFeaturesRequest(_message.Message):
    __slots__ = ("feature_view_id", "features", "idempotency_key")
    FEATURE_VIEW_ID_FIELD_NUMBER: _ClassVar[int]
    FEATURES_FIELD_NUMBER: _ClassVar[int]
    IDEMPOTENCY_KEY_FIELD_NUMBER: _ClassVar[int]
    feature_view_id: str
    features: _containers.RepeatedCompositeFieldContainer[FeatureValues]
    idempotency_key: str
    def __init__(self, feature_view_id: _Optional[str] = ..., features: _Optional[_Iterable[_Union[FeatureValues, _Mapping]]] = ..., idempotency_key: _Optional[str] = ...) -> None: ...

class WriteFeaturesResponse(_message.Message):
    __slots__ = ("written_count", "written_through_version")
    WRITTEN_COUNT_FIELD_NUMBER: _ClassVar[int]
    WRITTEN_THROUGH_VERSION_FIELD_NUMBER: _ClassVar[int]
    written_count: int
    written_through_version: int
    def __init__(self, written_count: _Optional[int] = ..., written_through_version: _Optional[int] = ...) -> None: ...

class GetOnlineFeaturesRequest(_message.Message):
    __slots__ = ("feature_view_id", "entity_ids", "feature_names")
    FEATURE_VIEW_ID_FIELD_NUMBER: _ClassVar[int]
    ENTITY_IDS_FIELD_NUMBER: _ClassVar[int]
    FEATURE_NAMES_FIELD_NUMBER: _ClassVar[int]
    feature_view_id: str
    entity_ids: _containers.RepeatedScalarFieldContainer[str]
    feature_names: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, feature_view_id: _Optional[str] = ..., entity_ids: _Optional[_Iterable[str]] = ..., feature_names: _Optional[_Iterable[str]] = ...) -> None: ...

class GetOnlineFeaturesResponse(_message.Message):
    __slots__ = ("vectors", "missing_entity_ids")
    VECTORS_FIELD_NUMBER: _ClassVar[int]
    MISSING_ENTITY_IDS_FIELD_NUMBER: _ClassVar[int]
    vectors: _containers.RepeatedCompositeFieldContainer[FeatureVector]
    missing_entity_ids: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, vectors: _Optional[_Iterable[_Union[FeatureVector, _Mapping]]] = ..., missing_entity_ids: _Optional[_Iterable[str]] = ...) -> None: ...

class GetHistoricalFeaturesRequest(_message.Message):
    __slots__ = ("feature_view_id", "entity_ids", "as_of", "feature_names", "pagination")
    FEATURE_VIEW_ID_FIELD_NUMBER: _ClassVar[int]
    ENTITY_IDS_FIELD_NUMBER: _ClassVar[int]
    AS_OF_FIELD_NUMBER: _ClassVar[int]
    FEATURE_NAMES_FIELD_NUMBER: _ClassVar[int]
    PAGINATION_FIELD_NUMBER: _ClassVar[int]
    feature_view_id: str
    entity_ids: _containers.RepeatedScalarFieldContainer[str]
    as_of: _timestamp_pb2.Timestamp
    feature_names: _containers.RepeatedScalarFieldContainer[str]
    pagination: _common_pb2.PaginationRequest
    def __init__(self, feature_view_id: _Optional[str] = ..., entity_ids: _Optional[_Iterable[str]] = ..., as_of: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., feature_names: _Optional[_Iterable[str]] = ..., pagination: _Optional[_Union[_common_pb2.PaginationRequest, _Mapping]] = ...) -> None: ...

class GetHistoricalFeaturesResponse(_message.Message):
    __slots__ = ("vectors", "missing_entity_ids", "pagination")
    VECTORS_FIELD_NUMBER: _ClassVar[int]
    MISSING_ENTITY_IDS_FIELD_NUMBER: _ClassVar[int]
    PAGINATION_FIELD_NUMBER: _ClassVar[int]
    vectors: _containers.RepeatedCompositeFieldContainer[FeatureVector]
    missing_entity_ids: _containers.RepeatedScalarFieldContainer[str]
    pagination: _common_pb2.PaginationResponse
    def __init__(self, vectors: _Optional[_Iterable[_Union[FeatureVector, _Mapping]]] = ..., missing_entity_ids: _Optional[_Iterable[str]] = ..., pagination: _Optional[_Union[_common_pb2.PaginationResponse, _Mapping]] = ...) -> None: ...

class DeleteFeatureViewRequest(_message.Message):
    __slots__ = ("feature_view_id", "idempotency_key")
    FEATURE_VIEW_ID_FIELD_NUMBER: _ClassVar[int]
    IDEMPOTENCY_KEY_FIELD_NUMBER: _ClassVar[int]
    feature_view_id: str
    idempotency_key: str
    def __init__(self, feature_view_id: _Optional[str] = ..., idempotency_key: _Optional[str] = ...) -> None: ...

class DeleteFeatureViewResponse(_message.Message):
    __slots__ = ("feature_view",)
    FEATURE_VIEW_FIELD_NUMBER: _ClassVar[int]
    feature_view: FeatureView
    def __init__(self, feature_view: _Optional[_Union[FeatureView, _Mapping]] = ...) -> None: ...

class RebuildViewsRequest(_message.Message):
    __slots__ = ("feature_view_id", "target")
    FEATURE_VIEW_ID_FIELD_NUMBER: _ClassVar[int]
    TARGET_FIELD_NUMBER: _ClassVar[int]
    feature_view_id: str
    target: RebuildTarget
    def __init__(self, feature_view_id: _Optional[str] = ..., target: _Optional[_Union[RebuildTarget, str]] = ...) -> None: ...

class RebuildViewsResponse(_message.Message):
    __slots__ = ("events_replayed", "total_events", "current_feature_view_id", "done")
    EVENTS_REPLAYED_FIELD_NUMBER: _ClassVar[int]
    TOTAL_EVENTS_FIELD_NUMBER: _ClassVar[int]
    CURRENT_FEATURE_VIEW_ID_FIELD_NUMBER: _ClassVar[int]
    DONE_FIELD_NUMBER: _ClassVar[int]
    events_replayed: int
    total_events: int
    current_feature_view_id: str
    done: bool
    def __init__(self, events_replayed: _Optional[int] = ..., total_events: _Optional[int] = ..., current_feature_view_id: _Optional[str] = ..., done: _Optional[bool] = ...) -> None: ...
