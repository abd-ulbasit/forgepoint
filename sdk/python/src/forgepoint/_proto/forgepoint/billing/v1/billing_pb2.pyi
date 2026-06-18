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

class MeterType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    METER_TYPE_UNSPECIFIED: _ClassVar[MeterType]
    METER_TYPE_INFERENCE_REQUEST: _ClassVar[MeterType]
    METER_TYPE_INFERENCE_TOKENS: _ClassVar[MeterType]
    METER_TYPE_COMPUTE_SECONDS: _ClassVar[MeterType]
    METER_TYPE_STORAGE_BYTES: _ClassVar[MeterType]

class InvoiceStatus(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    INVOICE_STATUS_UNSPECIFIED: _ClassVar[InvoiceStatus]
    INVOICE_STATUS_DRAFT: _ClassVar[InvoiceStatus]
    INVOICE_STATUS_FINALIZED: _ClassVar[InvoiceStatus]
    INVOICE_STATUS_PAID: _ClassVar[InvoiceStatus]
    INVOICE_STATUS_OVERDUE: _ClassVar[InvoiceStatus]
    INVOICE_STATUS_VOID: _ClassVar[InvoiceStatus]
METER_TYPE_UNSPECIFIED: MeterType
METER_TYPE_INFERENCE_REQUEST: MeterType
METER_TYPE_INFERENCE_TOKENS: MeterType
METER_TYPE_COMPUTE_SECONDS: MeterType
METER_TYPE_STORAGE_BYTES: MeterType
INVOICE_STATUS_UNSPECIFIED: InvoiceStatus
INVOICE_STATUS_DRAFT: InvoiceStatus
INVOICE_STATUS_FINALIZED: InvoiceStatus
INVOICE_STATUS_PAID: InvoiceStatus
INVOICE_STATUS_OVERDUE: InvoiceStatus
INVOICE_STATUS_VOID: InvoiceStatus

class Money(_message.Message):
    __slots__ = ("amount_micros", "currency_code")
    AMOUNT_MICROS_FIELD_NUMBER: _ClassVar[int]
    CURRENCY_CODE_FIELD_NUMBER: _ClassVar[int]
    amount_micros: int
    currency_code: str
    def __init__(self, amount_micros: _Optional[int] = ..., currency_code: _Optional[str] = ...) -> None: ...

class RatePlan(_message.Message):
    __slots__ = ("id", "name", "unit_prices", "included_quantities", "quota_limits", "created_at")
    class UnitPricesEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: Money
        def __init__(self, key: _Optional[str] = ..., value: _Optional[_Union[Money, _Mapping]] = ...) -> None: ...
    class IncludedQuantitiesEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: int
        def __init__(self, key: _Optional[str] = ..., value: _Optional[int] = ...) -> None: ...
    class QuotaLimitsEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: int
        def __init__(self, key: _Optional[str] = ..., value: _Optional[int] = ...) -> None: ...
    ID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    UNIT_PRICES_FIELD_NUMBER: _ClassVar[int]
    INCLUDED_QUANTITIES_FIELD_NUMBER: _ClassVar[int]
    QUOTA_LIMITS_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_FIELD_NUMBER: _ClassVar[int]
    id: str
    name: str
    unit_prices: _containers.MessageMap[str, Money]
    included_quantities: _containers.ScalarMap[str, int]
    quota_limits: _containers.ScalarMap[str, int]
    created_at: _timestamp_pb2.Timestamp
    def __init__(self, id: _Optional[str] = ..., name: _Optional[str] = ..., unit_prices: _Optional[_Mapping[str, Money]] = ..., included_quantities: _Optional[_Mapping[str, int]] = ..., quota_limits: _Optional[_Mapping[str, int]] = ..., created_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class UsageRecord(_message.Message):
    __slots__ = ("id", "team", "rate_plan_id", "meter_type", "quantity", "cost", "model_id", "model_version", "source_request_id", "occurred_at")
    ID_FIELD_NUMBER: _ClassVar[int]
    TEAM_FIELD_NUMBER: _ClassVar[int]
    RATE_PLAN_ID_FIELD_NUMBER: _ClassVar[int]
    METER_TYPE_FIELD_NUMBER: _ClassVar[int]
    QUANTITY_FIELD_NUMBER: _ClassVar[int]
    COST_FIELD_NUMBER: _ClassVar[int]
    MODEL_ID_FIELD_NUMBER: _ClassVar[int]
    MODEL_VERSION_FIELD_NUMBER: _ClassVar[int]
    SOURCE_REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
    OCCURRED_AT_FIELD_NUMBER: _ClassVar[int]
    id: str
    team: str
    rate_plan_id: str
    meter_type: MeterType
    quantity: int
    cost: Money
    model_id: str
    model_version: str
    source_request_id: str
    occurred_at: _timestamp_pb2.Timestamp
    def __init__(self, id: _Optional[str] = ..., team: _Optional[str] = ..., rate_plan_id: _Optional[str] = ..., meter_type: _Optional[_Union[MeterType, str]] = ..., quantity: _Optional[int] = ..., cost: _Optional[_Union[Money, _Mapping]] = ..., model_id: _Optional[str] = ..., model_version: _Optional[str] = ..., source_request_id: _Optional[str] = ..., occurred_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class UsageSummary(_message.Message):
    __slots__ = ("team", "period_start", "period_end", "by_meter", "total_cost")
    class ByMeterEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: MeterUsage
        def __init__(self, key: _Optional[str] = ..., value: _Optional[_Union[MeterUsage, _Mapping]] = ...) -> None: ...
    TEAM_FIELD_NUMBER: _ClassVar[int]
    PERIOD_START_FIELD_NUMBER: _ClassVar[int]
    PERIOD_END_FIELD_NUMBER: _ClassVar[int]
    BY_METER_FIELD_NUMBER: _ClassVar[int]
    TOTAL_COST_FIELD_NUMBER: _ClassVar[int]
    team: str
    period_start: _timestamp_pb2.Timestamp
    period_end: _timestamp_pb2.Timestamp
    by_meter: _containers.MessageMap[str, MeterUsage]
    total_cost: Money
    def __init__(self, team: _Optional[str] = ..., period_start: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., period_end: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., by_meter: _Optional[_Mapping[str, MeterUsage]] = ..., total_cost: _Optional[_Union[Money, _Mapping]] = ...) -> None: ...

class MeterUsage(_message.Message):
    __slots__ = ("meter_type", "total_quantity", "total_cost")
    METER_TYPE_FIELD_NUMBER: _ClassVar[int]
    TOTAL_QUANTITY_FIELD_NUMBER: _ClassVar[int]
    TOTAL_COST_FIELD_NUMBER: _ClassVar[int]
    meter_type: MeterType
    total_quantity: int
    total_cost: Money
    def __init__(self, meter_type: _Optional[_Union[MeterType, str]] = ..., total_quantity: _Optional[int] = ..., total_cost: _Optional[_Union[Money, _Mapping]] = ...) -> None: ...

class Invoice(_message.Message):
    __slots__ = ("id", "invoice_number", "team", "rate_plan_id", "status", "period_start", "period_end", "line_items", "total", "created_at", "finalized_at", "due_at")
    ID_FIELD_NUMBER: _ClassVar[int]
    INVOICE_NUMBER_FIELD_NUMBER: _ClassVar[int]
    TEAM_FIELD_NUMBER: _ClassVar[int]
    RATE_PLAN_ID_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    PERIOD_START_FIELD_NUMBER: _ClassVar[int]
    PERIOD_END_FIELD_NUMBER: _ClassVar[int]
    LINE_ITEMS_FIELD_NUMBER: _ClassVar[int]
    TOTAL_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_FIELD_NUMBER: _ClassVar[int]
    FINALIZED_AT_FIELD_NUMBER: _ClassVar[int]
    DUE_AT_FIELD_NUMBER: _ClassVar[int]
    id: str
    invoice_number: str
    team: str
    rate_plan_id: str
    status: InvoiceStatus
    period_start: _timestamp_pb2.Timestamp
    period_end: _timestamp_pb2.Timestamp
    line_items: _containers.RepeatedCompositeFieldContainer[InvoiceLineItem]
    total: Money
    created_at: _timestamp_pb2.Timestamp
    finalized_at: _timestamp_pb2.Timestamp
    due_at: _timestamp_pb2.Timestamp
    def __init__(self, id: _Optional[str] = ..., invoice_number: _Optional[str] = ..., team: _Optional[str] = ..., rate_plan_id: _Optional[str] = ..., status: _Optional[_Union[InvoiceStatus, str]] = ..., period_start: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., period_end: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., line_items: _Optional[_Iterable[_Union[InvoiceLineItem, _Mapping]]] = ..., total: _Optional[_Union[Money, _Mapping]] = ..., created_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., finalized_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., due_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class InvoiceLineItem(_message.Message):
    __slots__ = ("meter_type", "description", "quantity", "unit_price", "amount")
    METER_TYPE_FIELD_NUMBER: _ClassVar[int]
    DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    QUANTITY_FIELD_NUMBER: _ClassVar[int]
    UNIT_PRICE_FIELD_NUMBER: _ClassVar[int]
    AMOUNT_FIELD_NUMBER: _ClassVar[int]
    meter_type: MeterType
    description: str
    quantity: int
    unit_price: Money
    amount: Money
    def __init__(self, meter_type: _Optional[_Union[MeterType, str]] = ..., description: _Optional[str] = ..., quantity: _Optional[int] = ..., unit_price: _Optional[_Union[Money, _Mapping]] = ..., amount: _Optional[_Union[Money, _Mapping]] = ...) -> None: ...

class RecordUsageRequest(_message.Message):
    __slots__ = ("meter_type", "quantity", "model_id", "model_version", "source_request_id", "occurred_at", "idempotency_key")
    METER_TYPE_FIELD_NUMBER: _ClassVar[int]
    QUANTITY_FIELD_NUMBER: _ClassVar[int]
    MODEL_ID_FIELD_NUMBER: _ClassVar[int]
    MODEL_VERSION_FIELD_NUMBER: _ClassVar[int]
    SOURCE_REQUEST_ID_FIELD_NUMBER: _ClassVar[int]
    OCCURRED_AT_FIELD_NUMBER: _ClassVar[int]
    IDEMPOTENCY_KEY_FIELD_NUMBER: _ClassVar[int]
    meter_type: MeterType
    quantity: int
    model_id: str
    model_version: str
    source_request_id: str
    occurred_at: _timestamp_pb2.Timestamp
    idempotency_key: str
    def __init__(self, meter_type: _Optional[_Union[MeterType, str]] = ..., quantity: _Optional[int] = ..., model_id: _Optional[str] = ..., model_version: _Optional[str] = ..., source_request_id: _Optional[str] = ..., occurred_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., idempotency_key: _Optional[str] = ...) -> None: ...

class RecordUsageResponse(_message.Message):
    __slots__ = ("record", "deduplicated")
    RECORD_FIELD_NUMBER: _ClassVar[int]
    DEDUPLICATED_FIELD_NUMBER: _ClassVar[int]
    record: UsageRecord
    deduplicated: bool
    def __init__(self, record: _Optional[_Union[UsageRecord, _Mapping]] = ..., deduplicated: _Optional[bool] = ...) -> None: ...

class GetUsageRequest(_message.Message):
    __slots__ = ("team", "period_start", "period_end", "meter_type_filter", "pagination")
    TEAM_FIELD_NUMBER: _ClassVar[int]
    PERIOD_START_FIELD_NUMBER: _ClassVar[int]
    PERIOD_END_FIELD_NUMBER: _ClassVar[int]
    METER_TYPE_FILTER_FIELD_NUMBER: _ClassVar[int]
    PAGINATION_FIELD_NUMBER: _ClassVar[int]
    team: str
    period_start: _timestamp_pb2.Timestamp
    period_end: _timestamp_pb2.Timestamp
    meter_type_filter: MeterType
    pagination: _common_pb2.PaginationRequest
    def __init__(self, team: _Optional[str] = ..., period_start: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., period_end: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., meter_type_filter: _Optional[_Union[MeterType, str]] = ..., pagination: _Optional[_Union[_common_pb2.PaginationRequest, _Mapping]] = ...) -> None: ...

class GetUsageResponse(_message.Message):
    __slots__ = ("summaries", "grand_total", "pagination")
    SUMMARIES_FIELD_NUMBER: _ClassVar[int]
    GRAND_TOTAL_FIELD_NUMBER: _ClassVar[int]
    PAGINATION_FIELD_NUMBER: _ClassVar[int]
    summaries: _containers.RepeatedCompositeFieldContainer[UsageSummary]
    grand_total: Money
    pagination: _common_pb2.PaginationResponse
    def __init__(self, summaries: _Optional[_Iterable[_Union[UsageSummary, _Mapping]]] = ..., grand_total: _Optional[_Union[Money, _Mapping]] = ..., pagination: _Optional[_Union[_common_pb2.PaginationResponse, _Mapping]] = ...) -> None: ...

class GetInvoiceRequest(_message.Message):
    __slots__ = ("invoice_id",)
    INVOICE_ID_FIELD_NUMBER: _ClassVar[int]
    invoice_id: str
    def __init__(self, invoice_id: _Optional[str] = ...) -> None: ...

class GetInvoiceResponse(_message.Message):
    __slots__ = ("invoice",)
    INVOICE_FIELD_NUMBER: _ClassVar[int]
    invoice: Invoice
    def __init__(self, invoice: _Optional[_Union[Invoice, _Mapping]] = ...) -> None: ...

class ListInvoicesRequest(_message.Message):
    __slots__ = ("team", "status_filter", "pagination")
    TEAM_FIELD_NUMBER: _ClassVar[int]
    STATUS_FILTER_FIELD_NUMBER: _ClassVar[int]
    PAGINATION_FIELD_NUMBER: _ClassVar[int]
    team: str
    status_filter: InvoiceStatus
    pagination: _common_pb2.PaginationRequest
    def __init__(self, team: _Optional[str] = ..., status_filter: _Optional[_Union[InvoiceStatus, str]] = ..., pagination: _Optional[_Union[_common_pb2.PaginationRequest, _Mapping]] = ...) -> None: ...

class ListInvoicesResponse(_message.Message):
    __slots__ = ("invoices", "pagination")
    INVOICES_FIELD_NUMBER: _ClassVar[int]
    PAGINATION_FIELD_NUMBER: _ClassVar[int]
    invoices: _containers.RepeatedCompositeFieldContainer[Invoice]
    pagination: _common_pb2.PaginationResponse
    def __init__(self, invoices: _Optional[_Iterable[_Union[Invoice, _Mapping]]] = ..., pagination: _Optional[_Union[_common_pb2.PaginationResponse, _Mapping]] = ...) -> None: ...

class CreateRatePlanRequest(_message.Message):
    __slots__ = ("name", "unit_prices", "included_quantities", "quota_limits", "idempotency_key")
    class UnitPricesEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: Money
        def __init__(self, key: _Optional[str] = ..., value: _Optional[_Union[Money, _Mapping]] = ...) -> None: ...
    class IncludedQuantitiesEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: int
        def __init__(self, key: _Optional[str] = ..., value: _Optional[int] = ...) -> None: ...
    class QuotaLimitsEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: int
        def __init__(self, key: _Optional[str] = ..., value: _Optional[int] = ...) -> None: ...
    NAME_FIELD_NUMBER: _ClassVar[int]
    UNIT_PRICES_FIELD_NUMBER: _ClassVar[int]
    INCLUDED_QUANTITIES_FIELD_NUMBER: _ClassVar[int]
    QUOTA_LIMITS_FIELD_NUMBER: _ClassVar[int]
    IDEMPOTENCY_KEY_FIELD_NUMBER: _ClassVar[int]
    name: str
    unit_prices: _containers.MessageMap[str, Money]
    included_quantities: _containers.ScalarMap[str, int]
    quota_limits: _containers.ScalarMap[str, int]
    idempotency_key: str
    def __init__(self, name: _Optional[str] = ..., unit_prices: _Optional[_Mapping[str, Money]] = ..., included_quantities: _Optional[_Mapping[str, int]] = ..., quota_limits: _Optional[_Mapping[str, int]] = ..., idempotency_key: _Optional[str] = ...) -> None: ...

class CreateRatePlanResponse(_message.Message):
    __slots__ = ("rate_plan", "deduplicated")
    RATE_PLAN_FIELD_NUMBER: _ClassVar[int]
    DEDUPLICATED_FIELD_NUMBER: _ClassVar[int]
    rate_plan: RatePlan
    deduplicated: bool
    def __init__(self, rate_plan: _Optional[_Union[RatePlan, _Mapping]] = ..., deduplicated: _Optional[bool] = ...) -> None: ...

class GetRatePlanRequest(_message.Message):
    __slots__ = ("rate_plan_id",)
    RATE_PLAN_ID_FIELD_NUMBER: _ClassVar[int]
    rate_plan_id: str
    def __init__(self, rate_plan_id: _Optional[str] = ...) -> None: ...

class GetRatePlanResponse(_message.Message):
    __slots__ = ("rate_plan",)
    RATE_PLAN_FIELD_NUMBER: _ClassVar[int]
    rate_plan: RatePlan
    def __init__(self, rate_plan: _Optional[_Union[RatePlan, _Mapping]] = ...) -> None: ...

class CheckQuotaRequest(_message.Message):
    __slots__ = ("team", "meter_type")
    TEAM_FIELD_NUMBER: _ClassVar[int]
    METER_TYPE_FIELD_NUMBER: _ClassVar[int]
    team: str
    meter_type: MeterType
    def __init__(self, team: _Optional[str] = ..., meter_type: _Optional[_Union[MeterType, str]] = ...) -> None: ...

class CheckQuotaResponse(_message.Message):
    __slots__ = ("team", "rate_plan_id", "meter_type", "quota_limit", "current_usage", "remaining", "exceeded")
    TEAM_FIELD_NUMBER: _ClassVar[int]
    RATE_PLAN_ID_FIELD_NUMBER: _ClassVar[int]
    METER_TYPE_FIELD_NUMBER: _ClassVar[int]
    QUOTA_LIMIT_FIELD_NUMBER: _ClassVar[int]
    CURRENT_USAGE_FIELD_NUMBER: _ClassVar[int]
    REMAINING_FIELD_NUMBER: _ClassVar[int]
    EXCEEDED_FIELD_NUMBER: _ClassVar[int]
    team: str
    rate_plan_id: str
    meter_type: MeterType
    quota_limit: int
    current_usage: int
    remaining: int
    exceeded: bool
    def __init__(self, team: _Optional[str] = ..., rate_plan_id: _Optional[str] = ..., meter_type: _Optional[_Union[MeterType, str]] = ..., quota_limit: _Optional[int] = ..., current_usage: _Optional[int] = ..., remaining: _Optional[int] = ..., exceeded: _Optional[bool] = ...) -> None: ...
