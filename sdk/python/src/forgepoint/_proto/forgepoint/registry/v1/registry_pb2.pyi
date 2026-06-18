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

class ModelStage(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    MODEL_STAGE_UNSPECIFIED: _ClassVar[ModelStage]
    MODEL_STAGE_DEV: _ClassVar[ModelStage]
    MODEL_STAGE_STAGING: _ClassVar[ModelStage]
    MODEL_STAGE_PRODUCTION: _ClassVar[ModelStage]
    MODEL_STAGE_ARCHIVED: _ClassVar[ModelStage]

class VersionStatus(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    VERSION_STATUS_UNSPECIFIED: _ClassVar[VersionStatus]
    VERSION_STATUS_PENDING_UPLOAD: _ClassVar[VersionStatus]
    VERSION_STATUS_READY: _ClassVar[VersionStatus]
    VERSION_STATUS_FAILED: _ClassVar[VersionStatus]
MODEL_STAGE_UNSPECIFIED: ModelStage
MODEL_STAGE_DEV: ModelStage
MODEL_STAGE_STAGING: ModelStage
MODEL_STAGE_PRODUCTION: ModelStage
MODEL_STAGE_ARCHIVED: ModelStage
VERSION_STATUS_UNSPECIFIED: VersionStatus
VERSION_STATUS_PENDING_UPLOAD: VersionStatus
VERSION_STATUS_READY: VersionStatus
VERSION_STATUS_FAILED: VersionStatus

class Model(_message.Message):
    __slots__ = ("id", "name", "description", "owner_id", "team", "framework", "task_type", "tags", "production_version", "latest_version", "created_at", "updated_at", "archived_at")
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
    OWNER_ID_FIELD_NUMBER: _ClassVar[int]
    TEAM_FIELD_NUMBER: _ClassVar[int]
    FRAMEWORK_FIELD_NUMBER: _ClassVar[int]
    TASK_TYPE_FIELD_NUMBER: _ClassVar[int]
    TAGS_FIELD_NUMBER: _ClassVar[int]
    PRODUCTION_VERSION_FIELD_NUMBER: _ClassVar[int]
    LATEST_VERSION_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_FIELD_NUMBER: _ClassVar[int]
    UPDATED_AT_FIELD_NUMBER: _ClassVar[int]
    ARCHIVED_AT_FIELD_NUMBER: _ClassVar[int]
    id: str
    name: str
    description: str
    owner_id: str
    team: str
    framework: str
    task_type: str
    tags: _containers.ScalarMap[str, str]
    production_version: str
    latest_version: str
    created_at: _timestamp_pb2.Timestamp
    updated_at: _timestamp_pb2.Timestamp
    archived_at: _timestamp_pb2.Timestamp
    def __init__(self, id: _Optional[str] = ..., name: _Optional[str] = ..., description: _Optional[str] = ..., owner_id: _Optional[str] = ..., team: _Optional[str] = ..., framework: _Optional[str] = ..., task_type: _Optional[str] = ..., tags: _Optional[_Mapping[str, str]] = ..., production_version: _Optional[str] = ..., latest_version: _Optional[str] = ..., created_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., updated_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., archived_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class ModelVersion(_message.Message):
    __slots__ = ("id", "model_id", "version", "description", "artifact_path", "artifact_digest", "size_bytes", "metrics", "stage", "status", "created_by", "created_at")
    ID_FIELD_NUMBER: _ClassVar[int]
    MODEL_ID_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    ARTIFACT_PATH_FIELD_NUMBER: _ClassVar[int]
    ARTIFACT_DIGEST_FIELD_NUMBER: _ClassVar[int]
    SIZE_BYTES_FIELD_NUMBER: _ClassVar[int]
    METRICS_FIELD_NUMBER: _ClassVar[int]
    STAGE_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    CREATED_BY_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_FIELD_NUMBER: _ClassVar[int]
    id: str
    model_id: str
    version: str
    description: str
    artifact_path: str
    artifact_digest: str
    size_bytes: int
    metrics: _struct_pb2.Struct
    stage: ModelStage
    status: VersionStatus
    created_by: str
    created_at: _timestamp_pb2.Timestamp
    def __init__(self, id: _Optional[str] = ..., model_id: _Optional[str] = ..., version: _Optional[str] = ..., description: _Optional[str] = ..., artifact_path: _Optional[str] = ..., artifact_digest: _Optional[str] = ..., size_bytes: _Optional[int] = ..., metrics: _Optional[_Union[_struct_pb2.Struct, _Mapping]] = ..., stage: _Optional[_Union[ModelStage, str]] = ..., status: _Optional[_Union[VersionStatus, str]] = ..., created_by: _Optional[str] = ..., created_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class RegisterModelRequest(_message.Message):
    __slots__ = ("name", "description", "framework", "task_type", "tags", "idempotency_key")
    class TagsEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    NAME_FIELD_NUMBER: _ClassVar[int]
    DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    FRAMEWORK_FIELD_NUMBER: _ClassVar[int]
    TASK_TYPE_FIELD_NUMBER: _ClassVar[int]
    TAGS_FIELD_NUMBER: _ClassVar[int]
    IDEMPOTENCY_KEY_FIELD_NUMBER: _ClassVar[int]
    name: str
    description: str
    framework: str
    task_type: str
    tags: _containers.ScalarMap[str, str]
    idempotency_key: str
    def __init__(self, name: _Optional[str] = ..., description: _Optional[str] = ..., framework: _Optional[str] = ..., task_type: _Optional[str] = ..., tags: _Optional[_Mapping[str, str]] = ..., idempotency_key: _Optional[str] = ...) -> None: ...

class RegisterModelResponse(_message.Message):
    __slots__ = ("model",)
    MODEL_FIELD_NUMBER: _ClassVar[int]
    model: Model
    def __init__(self, model: _Optional[_Union[Model, _Mapping]] = ...) -> None: ...

class UpdateModelRequest(_message.Message):
    __slots__ = ("id", "description", "update_description", "tags", "replace_tags", "idempotency_key")
    class TagsEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    ID_FIELD_NUMBER: _ClassVar[int]
    DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    UPDATE_DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    TAGS_FIELD_NUMBER: _ClassVar[int]
    REPLACE_TAGS_FIELD_NUMBER: _ClassVar[int]
    IDEMPOTENCY_KEY_FIELD_NUMBER: _ClassVar[int]
    id: str
    description: str
    update_description: bool
    tags: _containers.ScalarMap[str, str]
    replace_tags: bool
    idempotency_key: str
    def __init__(self, id: _Optional[str] = ..., description: _Optional[str] = ..., update_description: _Optional[bool] = ..., tags: _Optional[_Mapping[str, str]] = ..., replace_tags: _Optional[bool] = ..., idempotency_key: _Optional[str] = ...) -> None: ...

class UpdateModelResponse(_message.Message):
    __slots__ = ("model",)
    MODEL_FIELD_NUMBER: _ClassVar[int]
    model: Model
    def __init__(self, model: _Optional[_Union[Model, _Mapping]] = ...) -> None: ...

class GetModelRequest(_message.Message):
    __slots__ = ("id", "name")
    ID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    id: str
    name: str
    def __init__(self, id: _Optional[str] = ..., name: _Optional[str] = ...) -> None: ...

class GetModelResponse(_message.Message):
    __slots__ = ("model",)
    MODEL_FIELD_NUMBER: _ClassVar[int]
    model: Model
    def __init__(self, model: _Optional[_Union[Model, _Mapping]] = ...) -> None: ...

class ListModelsRequest(_message.Message):
    __slots__ = ("task_type_filter", "framework_filter", "include_archived", "pagination")
    TASK_TYPE_FILTER_FIELD_NUMBER: _ClassVar[int]
    FRAMEWORK_FILTER_FIELD_NUMBER: _ClassVar[int]
    INCLUDE_ARCHIVED_FIELD_NUMBER: _ClassVar[int]
    PAGINATION_FIELD_NUMBER: _ClassVar[int]
    task_type_filter: str
    framework_filter: str
    include_archived: bool
    pagination: _common_pb2.PaginationRequest
    def __init__(self, task_type_filter: _Optional[str] = ..., framework_filter: _Optional[str] = ..., include_archived: _Optional[bool] = ..., pagination: _Optional[_Union[_common_pb2.PaginationRequest, _Mapping]] = ...) -> None: ...

class ListModelsResponse(_message.Message):
    __slots__ = ("models", "pagination")
    MODELS_FIELD_NUMBER: _ClassVar[int]
    PAGINATION_FIELD_NUMBER: _ClassVar[int]
    models: _containers.RepeatedCompositeFieldContainer[Model]
    pagination: _common_pb2.PaginationResponse
    def __init__(self, models: _Optional[_Iterable[_Union[Model, _Mapping]]] = ..., pagination: _Optional[_Union[_common_pb2.PaginationResponse, _Mapping]] = ...) -> None: ...

class SearchByTagRequest(_message.Message):
    __slots__ = ("key", "value", "pagination")
    KEY_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    PAGINATION_FIELD_NUMBER: _ClassVar[int]
    key: str
    value: str
    pagination: _common_pb2.PaginationRequest
    def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ..., pagination: _Optional[_Union[_common_pb2.PaginationRequest, _Mapping]] = ...) -> None: ...

class SearchByTagResponse(_message.Message):
    __slots__ = ("models", "pagination")
    MODELS_FIELD_NUMBER: _ClassVar[int]
    PAGINATION_FIELD_NUMBER: _ClassVar[int]
    models: _containers.RepeatedCompositeFieldContainer[Model]
    pagination: _common_pb2.PaginationResponse
    def __init__(self, models: _Optional[_Iterable[_Union[Model, _Mapping]]] = ..., pagination: _Optional[_Union[_common_pb2.PaginationResponse, _Mapping]] = ...) -> None: ...

class CreateVersionRequest(_message.Message):
    __slots__ = ("model_id", "version", "description", "metrics", "idempotency_key", "upload_url_ttl_seconds")
    MODEL_ID_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    METRICS_FIELD_NUMBER: _ClassVar[int]
    IDEMPOTENCY_KEY_FIELD_NUMBER: _ClassVar[int]
    UPLOAD_URL_TTL_SECONDS_FIELD_NUMBER: _ClassVar[int]
    model_id: str
    version: str
    description: str
    metrics: _struct_pb2.Struct
    idempotency_key: str
    upload_url_ttl_seconds: int
    def __init__(self, model_id: _Optional[str] = ..., version: _Optional[str] = ..., description: _Optional[str] = ..., metrics: _Optional[_Union[_struct_pb2.Struct, _Mapping]] = ..., idempotency_key: _Optional[str] = ..., upload_url_ttl_seconds: _Optional[int] = ...) -> None: ...

class CreateVersionResponse(_message.Message):
    __slots__ = ("version", "upload_url", "upload_url_expires_at")
    VERSION_FIELD_NUMBER: _ClassVar[int]
    UPLOAD_URL_FIELD_NUMBER: _ClassVar[int]
    UPLOAD_URL_EXPIRES_AT_FIELD_NUMBER: _ClassVar[int]
    version: ModelVersion
    upload_url: str
    upload_url_expires_at: _timestamp_pb2.Timestamp
    def __init__(self, version: _Optional[_Union[ModelVersion, _Mapping]] = ..., upload_url: _Optional[str] = ..., upload_url_expires_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class GetUploadURLRequest(_message.Message):
    __slots__ = ("version_id", "ttl_seconds")
    VERSION_ID_FIELD_NUMBER: _ClassVar[int]
    TTL_SECONDS_FIELD_NUMBER: _ClassVar[int]
    version_id: str
    ttl_seconds: int
    def __init__(self, version_id: _Optional[str] = ..., ttl_seconds: _Optional[int] = ...) -> None: ...

class GetUploadURLResponse(_message.Message):
    __slots__ = ("upload_url", "upload_url_expires_at")
    UPLOAD_URL_FIELD_NUMBER: _ClassVar[int]
    UPLOAD_URL_EXPIRES_AT_FIELD_NUMBER: _ClassVar[int]
    upload_url: str
    upload_url_expires_at: _timestamp_pb2.Timestamp
    def __init__(self, upload_url: _Optional[str] = ..., upload_url_expires_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class ConfirmVersionUploadRequest(_message.Message):
    __slots__ = ("version_id", "expected_digest", "idempotency_key")
    VERSION_ID_FIELD_NUMBER: _ClassVar[int]
    EXPECTED_DIGEST_FIELD_NUMBER: _ClassVar[int]
    IDEMPOTENCY_KEY_FIELD_NUMBER: _ClassVar[int]
    version_id: str
    expected_digest: str
    idempotency_key: str
    def __init__(self, version_id: _Optional[str] = ..., expected_digest: _Optional[str] = ..., idempotency_key: _Optional[str] = ...) -> None: ...

class ConfirmVersionUploadResponse(_message.Message):
    __slots__ = ("version",)
    VERSION_FIELD_NUMBER: _ClassVar[int]
    version: ModelVersion
    def __init__(self, version: _Optional[_Union[ModelVersion, _Mapping]] = ...) -> None: ...

class GetVersionRequest(_message.Message):
    __slots__ = ("id", "model_id", "version")
    ID_FIELD_NUMBER: _ClassVar[int]
    MODEL_ID_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    id: str
    model_id: str
    version: str
    def __init__(self, id: _Optional[str] = ..., model_id: _Optional[str] = ..., version: _Optional[str] = ...) -> None: ...

class GetVersionResponse(_message.Message):
    __slots__ = ("version",)
    VERSION_FIELD_NUMBER: _ClassVar[int]
    version: ModelVersion
    def __init__(self, version: _Optional[_Union[ModelVersion, _Mapping]] = ...) -> None: ...

class ListVersionsRequest(_message.Message):
    __slots__ = ("model_id", "stage_filter", "pagination")
    MODEL_ID_FIELD_NUMBER: _ClassVar[int]
    STAGE_FILTER_FIELD_NUMBER: _ClassVar[int]
    PAGINATION_FIELD_NUMBER: _ClassVar[int]
    model_id: str
    stage_filter: ModelStage
    pagination: _common_pb2.PaginationRequest
    def __init__(self, model_id: _Optional[str] = ..., stage_filter: _Optional[_Union[ModelStage, str]] = ..., pagination: _Optional[_Union[_common_pb2.PaginationRequest, _Mapping]] = ...) -> None: ...

class ListVersionsResponse(_message.Message):
    __slots__ = ("versions", "pagination")
    VERSIONS_FIELD_NUMBER: _ClassVar[int]
    PAGINATION_FIELD_NUMBER: _ClassVar[int]
    versions: _containers.RepeatedCompositeFieldContainer[ModelVersion]
    pagination: _common_pb2.PaginationResponse
    def __init__(self, versions: _Optional[_Iterable[_Union[ModelVersion, _Mapping]]] = ..., pagination: _Optional[_Union[_common_pb2.PaginationResponse, _Mapping]] = ...) -> None: ...

class PromoteVersionRequest(_message.Message):
    __slots__ = ("version_id", "target_stage", "idempotency_key")
    VERSION_ID_FIELD_NUMBER: _ClassVar[int]
    TARGET_STAGE_FIELD_NUMBER: _ClassVar[int]
    IDEMPOTENCY_KEY_FIELD_NUMBER: _ClassVar[int]
    version_id: str
    target_stage: ModelStage
    idempotency_key: str
    def __init__(self, version_id: _Optional[str] = ..., target_stage: _Optional[_Union[ModelStage, str]] = ..., idempotency_key: _Optional[str] = ...) -> None: ...

class PromoteVersionResponse(_message.Message):
    __slots__ = ("version", "demoted_version")
    VERSION_FIELD_NUMBER: _ClassVar[int]
    DEMOTED_VERSION_FIELD_NUMBER: _ClassVar[int]
    version: ModelVersion
    demoted_version: ModelVersion
    def __init__(self, version: _Optional[_Union[ModelVersion, _Mapping]] = ..., demoted_version: _Optional[_Union[ModelVersion, _Mapping]] = ...) -> None: ...

class DeleteModelRequest(_message.Message):
    __slots__ = ("id", "idempotency_key")
    ID_FIELD_NUMBER: _ClassVar[int]
    IDEMPOTENCY_KEY_FIELD_NUMBER: _ClassVar[int]
    id: str
    idempotency_key: str
    def __init__(self, id: _Optional[str] = ..., idempotency_key: _Optional[str] = ...) -> None: ...

class DeleteModelResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class GetDownloadURLRequest(_message.Message):
    __slots__ = ("version_id", "ttl_seconds")
    VERSION_ID_FIELD_NUMBER: _ClassVar[int]
    TTL_SECONDS_FIELD_NUMBER: _ClassVar[int]
    version_id: str
    ttl_seconds: int
    def __init__(self, version_id: _Optional[str] = ..., ttl_seconds: _Optional[int] = ...) -> None: ...

class GetDownloadURLResponse(_message.Message):
    __slots__ = ("download_url", "expires_at", "artifact_digest")
    DOWNLOAD_URL_FIELD_NUMBER: _ClassVar[int]
    EXPIRES_AT_FIELD_NUMBER: _ClassVar[int]
    ARTIFACT_DIGEST_FIELD_NUMBER: _ClassVar[int]
    download_url: str
    expires_at: _timestamp_pb2.Timestamp
    artifact_digest: str
    def __init__(self, download_url: _Optional[str] = ..., expires_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., artifact_digest: _Optional[str] = ...) -> None: ...
