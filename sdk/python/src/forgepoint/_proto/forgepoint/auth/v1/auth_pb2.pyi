import datetime

from google.protobuf import timestamp_pb2 as _timestamp_pb2
from forgepoint.common.v1 import common_pb2 as _common_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class User(_message.Message):
    __slots__ = ("id", "email", "name", "team", "role", "created_at")
    ID_FIELD_NUMBER: _ClassVar[int]
    EMAIL_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    TEAM_FIELD_NUMBER: _ClassVar[int]
    ROLE_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_FIELD_NUMBER: _ClassVar[int]
    id: str
    email: str
    name: str
    team: str
    role: str
    created_at: _timestamp_pb2.Timestamp
    def __init__(self, id: _Optional[str] = ..., email: _Optional[str] = ..., name: _Optional[str] = ..., team: _Optional[str] = ..., role: _Optional[str] = ..., created_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class APIKey(_message.Message):
    __slots__ = ("id", "key_prefix", "user_id", "scopes", "expires_at", "created_at")
    ID_FIELD_NUMBER: _ClassVar[int]
    KEY_PREFIX_FIELD_NUMBER: _ClassVar[int]
    USER_ID_FIELD_NUMBER: _ClassVar[int]
    SCOPES_FIELD_NUMBER: _ClassVar[int]
    EXPIRES_AT_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_FIELD_NUMBER: _ClassVar[int]
    id: str
    key_prefix: str
    user_id: str
    scopes: _containers.RepeatedScalarFieldContainer[str]
    expires_at: _timestamp_pb2.Timestamp
    created_at: _timestamp_pb2.Timestamp
    def __init__(self, id: _Optional[str] = ..., key_prefix: _Optional[str] = ..., user_id: _Optional[str] = ..., scopes: _Optional[_Iterable[str]] = ..., expires_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., created_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class Role(_message.Message):
    __slots__ = ("id", "name", "permissions")
    ID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    PERMISSIONS_FIELD_NUMBER: _ClassVar[int]
    id: str
    name: str
    permissions: _containers.RepeatedCompositeFieldContainer[Permission]
    def __init__(self, id: _Optional[str] = ..., name: _Optional[str] = ..., permissions: _Optional[_Iterable[_Union[Permission, _Mapping]]] = ...) -> None: ...

class Permission(_message.Message):
    __slots__ = ("resource", "action")
    RESOURCE_FIELD_NUMBER: _ClassVar[int]
    ACTION_FIELD_NUMBER: _ClassVar[int]
    resource: str
    action: str
    def __init__(self, resource: _Optional[str] = ..., action: _Optional[str] = ...) -> None: ...

class TokenClaims(_message.Message):
    __slots__ = ("user_id", "email", "team", "role", "scopes", "exp", "iat")
    USER_ID_FIELD_NUMBER: _ClassVar[int]
    EMAIL_FIELD_NUMBER: _ClassVar[int]
    TEAM_FIELD_NUMBER: _ClassVar[int]
    ROLE_FIELD_NUMBER: _ClassVar[int]
    SCOPES_FIELD_NUMBER: _ClassVar[int]
    EXP_FIELD_NUMBER: _ClassVar[int]
    IAT_FIELD_NUMBER: _ClassVar[int]
    user_id: str
    email: str
    team: str
    role: str
    scopes: _containers.RepeatedScalarFieldContainer[str]
    exp: _timestamp_pb2.Timestamp
    iat: _timestamp_pb2.Timestamp
    def __init__(self, user_id: _Optional[str] = ..., email: _Optional[str] = ..., team: _Optional[str] = ..., role: _Optional[str] = ..., scopes: _Optional[_Iterable[str]] = ..., exp: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., iat: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class LoginRequest(_message.Message):
    __slots__ = ("email", "password")
    EMAIL_FIELD_NUMBER: _ClassVar[int]
    PASSWORD_FIELD_NUMBER: _ClassVar[int]
    email: str
    password: str
    def __init__(self, email: _Optional[str] = ..., password: _Optional[str] = ...) -> None: ...

class LoginResponse(_message.Message):
    __slots__ = ("access_token", "expires_at", "user")
    ACCESS_TOKEN_FIELD_NUMBER: _ClassVar[int]
    EXPIRES_AT_FIELD_NUMBER: _ClassVar[int]
    USER_FIELD_NUMBER: _ClassVar[int]
    access_token: str
    expires_at: _timestamp_pb2.Timestamp
    user: User
    def __init__(self, access_token: _Optional[str] = ..., expires_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., user: _Optional[_Union[User, _Mapping]] = ...) -> None: ...

class CreateUserRequest(_message.Message):
    __slots__ = ("email", "name", "password", "team")
    EMAIL_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    PASSWORD_FIELD_NUMBER: _ClassVar[int]
    TEAM_FIELD_NUMBER: _ClassVar[int]
    email: str
    name: str
    password: str
    team: str
    def __init__(self, email: _Optional[str] = ..., name: _Optional[str] = ..., password: _Optional[str] = ..., team: _Optional[str] = ...) -> None: ...

class CreateUserResponse(_message.Message):
    __slots__ = ("user",)
    USER_FIELD_NUMBER: _ClassVar[int]
    user: User
    def __init__(self, user: _Optional[_Union[User, _Mapping]] = ...) -> None: ...

class CreateAPIKeyRequest(_message.Message):
    __slots__ = ("user_id", "scopes", "expires_at")
    USER_ID_FIELD_NUMBER: _ClassVar[int]
    SCOPES_FIELD_NUMBER: _ClassVar[int]
    EXPIRES_AT_FIELD_NUMBER: _ClassVar[int]
    user_id: str
    scopes: _containers.RepeatedScalarFieldContainer[str]
    expires_at: _timestamp_pb2.Timestamp
    def __init__(self, user_id: _Optional[str] = ..., scopes: _Optional[_Iterable[str]] = ..., expires_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class CreateAPIKeyResponse(_message.Message):
    __slots__ = ("raw_key", "api_key")
    RAW_KEY_FIELD_NUMBER: _ClassVar[int]
    API_KEY_FIELD_NUMBER: _ClassVar[int]
    raw_key: str
    api_key: APIKey
    def __init__(self, raw_key: _Optional[str] = ..., api_key: _Optional[_Union[APIKey, _Mapping]] = ...) -> None: ...

class RevokeAPIKeyRequest(_message.Message):
    __slots__ = ("key_id",)
    KEY_ID_FIELD_NUMBER: _ClassVar[int]
    key_id: str
    def __init__(self, key_id: _Optional[str] = ...) -> None: ...

class RevokeAPIKeyResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class ValidateTokenRequest(_message.Message):
    __slots__ = ("token",)
    TOKEN_FIELD_NUMBER: _ClassVar[int]
    token: str
    def __init__(self, token: _Optional[str] = ...) -> None: ...

class ValidateTokenResponse(_message.Message):
    __slots__ = ("claims",)
    CLAIMS_FIELD_NUMBER: _ClassVar[int]
    claims: TokenClaims
    def __init__(self, claims: _Optional[_Union[TokenClaims, _Mapping]] = ...) -> None: ...

class CheckPermissionRequest(_message.Message):
    __slots__ = ("user_id", "resource", "action")
    USER_ID_FIELD_NUMBER: _ClassVar[int]
    RESOURCE_FIELD_NUMBER: _ClassVar[int]
    ACTION_FIELD_NUMBER: _ClassVar[int]
    user_id: str
    resource: str
    action: str
    def __init__(self, user_id: _Optional[str] = ..., resource: _Optional[str] = ..., action: _Optional[str] = ...) -> None: ...

class CheckPermissionResponse(_message.Message):
    __slots__ = ("allowed", "reason")
    ALLOWED_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    allowed: bool
    reason: str
    def __init__(self, allowed: _Optional[bool] = ..., reason: _Optional[str] = ...) -> None: ...

class AssignRoleRequest(_message.Message):
    __slots__ = ("user_id", "role_name")
    USER_ID_FIELD_NUMBER: _ClassVar[int]
    ROLE_NAME_FIELD_NUMBER: _ClassVar[int]
    user_id: str
    role_name: str
    def __init__(self, user_id: _Optional[str] = ..., role_name: _Optional[str] = ...) -> None: ...

class AssignRoleResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class ListUsersRequest(_message.Message):
    __slots__ = ("team_filter", "pagination")
    TEAM_FILTER_FIELD_NUMBER: _ClassVar[int]
    PAGINATION_FIELD_NUMBER: _ClassVar[int]
    team_filter: str
    pagination: _common_pb2.PaginationRequest
    def __init__(self, team_filter: _Optional[str] = ..., pagination: _Optional[_Union[_common_pb2.PaginationRequest, _Mapping]] = ...) -> None: ...

class ListUsersResponse(_message.Message):
    __slots__ = ("users", "pagination")
    USERS_FIELD_NUMBER: _ClassVar[int]
    PAGINATION_FIELD_NUMBER: _ClassVar[int]
    users: _containers.RepeatedCompositeFieldContainer[User]
    pagination: _common_pb2.PaginationResponse
    def __init__(self, users: _Optional[_Iterable[_Union[User, _Mapping]]] = ..., pagination: _Optional[_Union[_common_pb2.PaginationResponse, _Mapping]] = ...) -> None: ...
