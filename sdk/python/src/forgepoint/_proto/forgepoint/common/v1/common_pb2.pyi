import datetime

from google.protobuf import timestamp_pb2 as _timestamp_pb2
from google.protobuf import any_pb2 as _any_pb2
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class EventEnvelope(_message.Message):
    __slots__ = ("id", "type", "source", "timestamp", "correlation_id", "data")
    ID_FIELD_NUMBER: _ClassVar[int]
    TYPE_FIELD_NUMBER: _ClassVar[int]
    SOURCE_FIELD_NUMBER: _ClassVar[int]
    TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    CORRELATION_ID_FIELD_NUMBER: _ClassVar[int]
    DATA_FIELD_NUMBER: _ClassVar[int]
    id: str
    type: str
    source: str
    timestamp: _timestamp_pb2.Timestamp
    correlation_id: str
    data: _any_pb2.Any
    def __init__(self, id: _Optional[str] = ..., type: _Optional[str] = ..., source: _Optional[str] = ..., timestamp: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., correlation_id: _Optional[str] = ..., data: _Optional[_Union[_any_pb2.Any, _Mapping]] = ...) -> None: ...

class PaginationRequest(_message.Message):
    __slots__ = ("page_size", "page_token")
    PAGE_SIZE_FIELD_NUMBER: _ClassVar[int]
    PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    page_size: int
    page_token: str
    def __init__(self, page_size: _Optional[int] = ..., page_token: _Optional[str] = ...) -> None: ...

class PaginationResponse(_message.Message):
    __slots__ = ("next_page_token", "total_count")
    NEXT_PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    TOTAL_COUNT_FIELD_NUMBER: _ClassVar[int]
    next_page_token: str
    total_count: int
    def __init__(self, next_page_token: _Optional[str] = ..., total_count: _Optional[int] = ...) -> None: ...

class ErrorDetail(_message.Message):
    __slots__ = ("code", "message", "field")
    CODE_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    FIELD_FIELD_NUMBER: _ClassVar[int]
    code: str
    message: str
    field: str
    def __init__(self, code: _Optional[str] = ..., message: _Optional[str] = ..., field: _Optional[str] = ...) -> None: ...
