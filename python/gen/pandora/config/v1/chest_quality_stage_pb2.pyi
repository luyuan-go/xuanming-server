from pandora.config.v1 import excel_pb2 as _excel_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class ChestQualityStageRow(_message.Message):
    __slots__ = ("id", "rule_set_id", "quality", "display_name", "total_unlock_seconds", "progress_bar_length_cm", "progress_bar_width_pixels", "color_r", "color_g", "color_b", "color_a", "opened_effect_duration_multiplier")
    ID_FIELD_NUMBER: _ClassVar[int]
    RULE_SET_ID_FIELD_NUMBER: _ClassVar[int]
    QUALITY_FIELD_NUMBER: _ClassVar[int]
    DISPLAY_NAME_FIELD_NUMBER: _ClassVar[int]
    TOTAL_UNLOCK_SECONDS_FIELD_NUMBER: _ClassVar[int]
    PROGRESS_BAR_LENGTH_CM_FIELD_NUMBER: _ClassVar[int]
    PROGRESS_BAR_WIDTH_PIXELS_FIELD_NUMBER: _ClassVar[int]
    COLOR_R_FIELD_NUMBER: _ClassVar[int]
    COLOR_G_FIELD_NUMBER: _ClassVar[int]
    COLOR_B_FIELD_NUMBER: _ClassVar[int]
    COLOR_A_FIELD_NUMBER: _ClassVar[int]
    OPENED_EFFECT_DURATION_MULTIPLIER_FIELD_NUMBER: _ClassVar[int]
    id: int
    rule_set_id: str
    quality: int
    display_name: str
    total_unlock_seconds: float
    progress_bar_length_cm: float
    progress_bar_width_pixels: float
    color_r: float
    color_g: float
    color_b: float
    color_a: float
    opened_effect_duration_multiplier: float
    def __init__(self, id: _Optional[int] = ..., rule_set_id: _Optional[str] = ..., quality: _Optional[int] = ..., display_name: _Optional[str] = ..., total_unlock_seconds: _Optional[float] = ..., progress_bar_length_cm: _Optional[float] = ..., progress_bar_width_pixels: _Optional[float] = ..., color_r: _Optional[float] = ..., color_g: _Optional[float] = ..., color_b: _Optional[float] = ..., color_a: _Optional[float] = ..., opened_effect_duration_multiplier: _Optional[float] = ...) -> None: ...

class ChestQualityStageTableData(_message.Message):
    __slots__ = ("rows",)
    ROWS_FIELD_NUMBER: _ClassVar[int]
    rows: _containers.RepeatedCompositeFieldContainer[ChestQualityStageRow]
    def __init__(self, rows: _Optional[_Iterable[_Union[ChestQualityStageRow, _Mapping]]] = ...) -> None: ...
