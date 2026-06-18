import datetime

from google.protobuf import timestamp_pb2 as _timestamp_pb2
from forgepoint.common.v1 import common_pb2 as _common_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class NotificationChannel(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    NOTIFICATION_CHANNEL_UNSPECIFIED: _ClassVar[NotificationChannel]
    NOTIFICATION_CHANNEL_IN_APP: _ClassVar[NotificationChannel]
    NOTIFICATION_CHANNEL_WEBHOOK: _ClassVar[NotificationChannel]
    NOTIFICATION_CHANNEL_SLACK: _ClassVar[NotificationChannel]
    NOTIFICATION_CHANNEL_EMAIL: _ClassVar[NotificationChannel]

class NotificationSeverity(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    NOTIFICATION_SEVERITY_UNSPECIFIED: _ClassVar[NotificationSeverity]
    NOTIFICATION_SEVERITY_INFO: _ClassVar[NotificationSeverity]
    NOTIFICATION_SEVERITY_WARNING: _ClassVar[NotificationSeverity]
    NOTIFICATION_SEVERITY_ERROR: _ClassVar[NotificationSeverity]
    NOTIFICATION_SEVERITY_CRITICAL: _ClassVar[NotificationSeverity]

class DeliveryStatus(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    DELIVERY_STATUS_UNSPECIFIED: _ClassVar[DeliveryStatus]
    DELIVERY_STATUS_PENDING: _ClassVar[DeliveryStatus]
    DELIVERY_STATUS_RETRYING: _ClassVar[DeliveryStatus]
    DELIVERY_STATUS_DELIVERED: _ClassVar[DeliveryStatus]
    DELIVERY_STATUS_FAILED: _ClassVar[DeliveryStatus]
    DELIVERY_STATUS_SUPPRESSED: _ClassVar[DeliveryStatus]
NOTIFICATION_CHANNEL_UNSPECIFIED: NotificationChannel
NOTIFICATION_CHANNEL_IN_APP: NotificationChannel
NOTIFICATION_CHANNEL_WEBHOOK: NotificationChannel
NOTIFICATION_CHANNEL_SLACK: NotificationChannel
NOTIFICATION_CHANNEL_EMAIL: NotificationChannel
NOTIFICATION_SEVERITY_UNSPECIFIED: NotificationSeverity
NOTIFICATION_SEVERITY_INFO: NotificationSeverity
NOTIFICATION_SEVERITY_WARNING: NotificationSeverity
NOTIFICATION_SEVERITY_ERROR: NotificationSeverity
NOTIFICATION_SEVERITY_CRITICAL: NotificationSeverity
DELIVERY_STATUS_UNSPECIFIED: DeliveryStatus
DELIVERY_STATUS_PENDING: DeliveryStatus
DELIVERY_STATUS_RETRYING: DeliveryStatus
DELIVERY_STATUS_DELIVERED: DeliveryStatus
DELIVERY_STATUS_FAILED: DeliveryStatus
DELIVERY_STATUS_SUPPRESSED: DeliveryStatus

class Notification(_message.Message):
    __slots__ = ("id", "recipient_user_id", "title", "body", "severity", "channels", "read", "event_id", "event_type", "source_service", "created_at", "read_at")
    ID_FIELD_NUMBER: _ClassVar[int]
    RECIPIENT_USER_ID_FIELD_NUMBER: _ClassVar[int]
    TITLE_FIELD_NUMBER: _ClassVar[int]
    BODY_FIELD_NUMBER: _ClassVar[int]
    SEVERITY_FIELD_NUMBER: _ClassVar[int]
    CHANNELS_FIELD_NUMBER: _ClassVar[int]
    READ_FIELD_NUMBER: _ClassVar[int]
    EVENT_ID_FIELD_NUMBER: _ClassVar[int]
    EVENT_TYPE_FIELD_NUMBER: _ClassVar[int]
    SOURCE_SERVICE_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_FIELD_NUMBER: _ClassVar[int]
    READ_AT_FIELD_NUMBER: _ClassVar[int]
    id: str
    recipient_user_id: str
    title: str
    body: str
    severity: NotificationSeverity
    channels: _containers.RepeatedScalarFieldContainer[NotificationChannel]
    read: bool
    event_id: str
    event_type: str
    source_service: str
    created_at: _timestamp_pb2.Timestamp
    read_at: _timestamp_pb2.Timestamp
    def __init__(self, id: _Optional[str] = ..., recipient_user_id: _Optional[str] = ..., title: _Optional[str] = ..., body: _Optional[str] = ..., severity: _Optional[_Union[NotificationSeverity, str]] = ..., channels: _Optional[_Iterable[_Union[NotificationChannel, str]]] = ..., read: _Optional[bool] = ..., event_id: _Optional[str] = ..., event_type: _Optional[str] = ..., source_service: _Optional[str] = ..., created_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., read_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class DeliveryAttempt(_message.Message):
    __slots__ = ("channel", "status", "attempt", "response_code", "error_message", "attempted_at")
    CHANNEL_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    ATTEMPT_FIELD_NUMBER: _ClassVar[int]
    RESPONSE_CODE_FIELD_NUMBER: _ClassVar[int]
    ERROR_MESSAGE_FIELD_NUMBER: _ClassVar[int]
    ATTEMPTED_AT_FIELD_NUMBER: _ClassVar[int]
    channel: NotificationChannel
    status: DeliveryStatus
    attempt: int
    response_code: int
    error_message: str
    attempted_at: _timestamp_pb2.Timestamp
    def __init__(self, channel: _Optional[_Union[NotificationChannel, str]] = ..., status: _Optional[_Union[DeliveryStatus, str]] = ..., attempt: _Optional[int] = ..., response_code: _Optional[int] = ..., error_message: _Optional[str] = ..., attempted_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class ChannelPreference(_message.Message):
    __slots__ = ("channel", "enabled", "min_severity", "target")
    CHANNEL_FIELD_NUMBER: _ClassVar[int]
    ENABLED_FIELD_NUMBER: _ClassVar[int]
    MIN_SEVERITY_FIELD_NUMBER: _ClassVar[int]
    TARGET_FIELD_NUMBER: _ClassVar[int]
    channel: NotificationChannel
    enabled: bool
    min_severity: NotificationSeverity
    target: str
    def __init__(self, channel: _Optional[_Union[NotificationChannel, str]] = ..., enabled: _Optional[bool] = ..., min_severity: _Optional[_Union[NotificationSeverity, str]] = ..., target: _Optional[str] = ...) -> None: ...

class NotificationPreferences(_message.Message):
    __slots__ = ("user_id", "channels", "muted_event_patterns", "updated_at")
    USER_ID_FIELD_NUMBER: _ClassVar[int]
    CHANNELS_FIELD_NUMBER: _ClassVar[int]
    MUTED_EVENT_PATTERNS_FIELD_NUMBER: _ClassVar[int]
    UPDATED_AT_FIELD_NUMBER: _ClassVar[int]
    user_id: str
    channels: _containers.RepeatedCompositeFieldContainer[ChannelPreference]
    muted_event_patterns: _containers.RepeatedScalarFieldContainer[str]
    updated_at: _timestamp_pb2.Timestamp
    def __init__(self, user_id: _Optional[str] = ..., channels: _Optional[_Iterable[_Union[ChannelPreference, _Mapping]]] = ..., muted_event_patterns: _Optional[_Iterable[str]] = ..., updated_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class ListNotificationsRequest(_message.Message):
    __slots__ = ("unread_only", "min_severity", "event_type_filter", "pagination")
    UNREAD_ONLY_FIELD_NUMBER: _ClassVar[int]
    MIN_SEVERITY_FIELD_NUMBER: _ClassVar[int]
    EVENT_TYPE_FILTER_FIELD_NUMBER: _ClassVar[int]
    PAGINATION_FIELD_NUMBER: _ClassVar[int]
    unread_only: bool
    min_severity: NotificationSeverity
    event_type_filter: str
    pagination: _common_pb2.PaginationRequest
    def __init__(self, unread_only: _Optional[bool] = ..., min_severity: _Optional[_Union[NotificationSeverity, str]] = ..., event_type_filter: _Optional[str] = ..., pagination: _Optional[_Union[_common_pb2.PaginationRequest, _Mapping]] = ...) -> None: ...

class ListNotificationsResponse(_message.Message):
    __slots__ = ("notifications", "pagination", "unread_count")
    NOTIFICATIONS_FIELD_NUMBER: _ClassVar[int]
    PAGINATION_FIELD_NUMBER: _ClassVar[int]
    UNREAD_COUNT_FIELD_NUMBER: _ClassVar[int]
    notifications: _containers.RepeatedCompositeFieldContainer[Notification]
    pagination: _common_pb2.PaginationResponse
    unread_count: int
    def __init__(self, notifications: _Optional[_Iterable[_Union[Notification, _Mapping]]] = ..., pagination: _Optional[_Union[_common_pb2.PaginationResponse, _Mapping]] = ..., unread_count: _Optional[int] = ...) -> None: ...

class GetNotificationRequest(_message.Message):
    __slots__ = ("id",)
    ID_FIELD_NUMBER: _ClassVar[int]
    id: str
    def __init__(self, id: _Optional[str] = ...) -> None: ...

class GetNotificationResponse(_message.Message):
    __slots__ = ("notification", "delivery_attempts")
    NOTIFICATION_FIELD_NUMBER: _ClassVar[int]
    DELIVERY_ATTEMPTS_FIELD_NUMBER: _ClassVar[int]
    notification: Notification
    delivery_attempts: _containers.RepeatedCompositeFieldContainer[DeliveryAttempt]
    def __init__(self, notification: _Optional[_Union[Notification, _Mapping]] = ..., delivery_attempts: _Optional[_Iterable[_Union[DeliveryAttempt, _Mapping]]] = ...) -> None: ...

class MarkReadRequest(_message.Message):
    __slots__ = ("ids", "mark_all")
    IDS_FIELD_NUMBER: _ClassVar[int]
    MARK_ALL_FIELD_NUMBER: _ClassVar[int]
    ids: _containers.RepeatedScalarFieldContainer[str]
    mark_all: bool
    def __init__(self, ids: _Optional[_Iterable[str]] = ..., mark_all: _Optional[bool] = ...) -> None: ...

class MarkReadResponse(_message.Message):
    __slots__ = ("marked_count", "unread_count")
    MARKED_COUNT_FIELD_NUMBER: _ClassVar[int]
    UNREAD_COUNT_FIELD_NUMBER: _ClassVar[int]
    marked_count: int
    unread_count: int
    def __init__(self, marked_count: _Optional[int] = ..., unread_count: _Optional[int] = ...) -> None: ...

class GetPreferencesRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class GetPreferencesResponse(_message.Message):
    __slots__ = ("preferences",)
    PREFERENCES_FIELD_NUMBER: _ClassVar[int]
    preferences: NotificationPreferences
    def __init__(self, preferences: _Optional[_Union[NotificationPreferences, _Mapping]] = ...) -> None: ...

class UpdatePreferencesRequest(_message.Message):
    __slots__ = ("channels", "muted_event_patterns", "idempotency_key")
    CHANNELS_FIELD_NUMBER: _ClassVar[int]
    MUTED_EVENT_PATTERNS_FIELD_NUMBER: _ClassVar[int]
    IDEMPOTENCY_KEY_FIELD_NUMBER: _ClassVar[int]
    channels: _containers.RepeatedCompositeFieldContainer[ChannelPreference]
    muted_event_patterns: _containers.RepeatedScalarFieldContainer[str]
    idempotency_key: str
    def __init__(self, channels: _Optional[_Iterable[_Union[ChannelPreference, _Mapping]]] = ..., muted_event_patterns: _Optional[_Iterable[str]] = ..., idempotency_key: _Optional[str] = ...) -> None: ...

class UpdatePreferencesResponse(_message.Message):
    __slots__ = ("preferences",)
    PREFERENCES_FIELD_NUMBER: _ClassVar[int]
    preferences: NotificationPreferences
    def __init__(self, preferences: _Optional[_Union[NotificationPreferences, _Mapping]] = ...) -> None: ...

class ListDeliveryAttemptsRequest(_message.Message):
    __slots__ = ("channel", "status", "notification_id", "pagination")
    CHANNEL_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    NOTIFICATION_ID_FIELD_NUMBER: _ClassVar[int]
    PAGINATION_FIELD_NUMBER: _ClassVar[int]
    channel: NotificationChannel
    status: DeliveryStatus
    notification_id: str
    pagination: _common_pb2.PaginationRequest
    def __init__(self, channel: _Optional[_Union[NotificationChannel, str]] = ..., status: _Optional[_Union[DeliveryStatus, str]] = ..., notification_id: _Optional[str] = ..., pagination: _Optional[_Union[_common_pb2.PaginationRequest, _Mapping]] = ...) -> None: ...

class ListDeliveryAttemptsResponse(_message.Message):
    __slots__ = ("attempts", "notification_ids", "pagination")
    ATTEMPTS_FIELD_NUMBER: _ClassVar[int]
    NOTIFICATION_IDS_FIELD_NUMBER: _ClassVar[int]
    PAGINATION_FIELD_NUMBER: _ClassVar[int]
    attempts: _containers.RepeatedCompositeFieldContainer[DeliveryAttempt]
    notification_ids: _containers.RepeatedScalarFieldContainer[str]
    pagination: _common_pb2.PaginationResponse
    def __init__(self, attempts: _Optional[_Iterable[_Union[DeliveryAttempt, _Mapping]]] = ..., notification_ids: _Optional[_Iterable[str]] = ..., pagination: _Optional[_Union[_common_pb2.PaginationResponse, _Mapping]] = ...) -> None: ...

class TestChannelRequest(_message.Message):
    __slots__ = ("channel", "idempotency_key")
    CHANNEL_FIELD_NUMBER: _ClassVar[int]
    IDEMPOTENCY_KEY_FIELD_NUMBER: _ClassVar[int]
    channel: NotificationChannel
    idempotency_key: str
    def __init__(self, channel: _Optional[_Union[NotificationChannel, str]] = ..., idempotency_key: _Optional[str] = ...) -> None: ...

class TestChannelResponse(_message.Message):
    __slots__ = ("status", "response_code", "error_message")
    STATUS_FIELD_NUMBER: _ClassVar[int]
    RESPONSE_CODE_FIELD_NUMBER: _ClassVar[int]
    ERROR_MESSAGE_FIELD_NUMBER: _ClassVar[int]
    status: DeliveryStatus
    response_code: int
    error_message: str
    def __init__(self, status: _Optional[_Union[DeliveryStatus, str]] = ..., response_code: _Optional[int] = ..., error_message: _Optional[str] = ...) -> None: ...
