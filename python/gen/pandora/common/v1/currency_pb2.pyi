from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class CurrencyKind(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    CURRENCY_KIND_UNSPECIFIED: _ClassVar[CurrencyKind]
    CURRENCY_KIND_GOLD: _ClassVar[CurrencyKind]
    CURRENCY_KIND_DIAMOND: _ClassVar[CurrencyKind]
    CURRENCY_KIND_HONOR: _ClassVar[CurrencyKind]
CURRENCY_KIND_UNSPECIFIED: CurrencyKind
CURRENCY_KIND_GOLD: CurrencyKind
CURRENCY_KIND_DIAMOND: CurrencyKind
CURRENCY_KIND_HONOR: CurrencyKind

class CurrencyAmount(_message.Message):
    __slots__ = ("kind", "amount")
    KIND_FIELD_NUMBER: _ClassVar[int]
    AMOUNT_FIELD_NUMBER: _ClassVar[int]
    kind: CurrencyKind
    amount: int
    def __init__(self, kind: _Optional[_Union[CurrencyKind, str]] = ..., amount: _Optional[int] = ...) -> None: ...

class CurrencyBalancesStorageRecord(_message.Message):
    __slots__ = ("balances",)
    BALANCES_FIELD_NUMBER: _ClassVar[int]
    balances: _containers.RepeatedCompositeFieldContainer[CurrencyAmount]
    def __init__(self, balances: _Optional[_Iterable[_Union[CurrencyAmount, _Mapping]]] = ...) -> None: ...
