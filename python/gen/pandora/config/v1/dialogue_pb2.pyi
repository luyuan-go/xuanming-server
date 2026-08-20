from pandora.config.v1 import excel_pb2 as _excel_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class DialogueRow(_message.Message):
    __slots__ = ("id", "npc_id", "speaker", "text", "is_start", "option1_text", "option1_next", "option2_text", "option2_next", "option3_text", "option3_next")
    ID_FIELD_NUMBER: _ClassVar[int]
    NPC_ID_FIELD_NUMBER: _ClassVar[int]
    SPEAKER_FIELD_NUMBER: _ClassVar[int]
    TEXT_FIELD_NUMBER: _ClassVar[int]
    IS_START_FIELD_NUMBER: _ClassVar[int]
    OPTION1_TEXT_FIELD_NUMBER: _ClassVar[int]
    OPTION1_NEXT_FIELD_NUMBER: _ClassVar[int]
    OPTION2_TEXT_FIELD_NUMBER: _ClassVar[int]
    OPTION2_NEXT_FIELD_NUMBER: _ClassVar[int]
    OPTION3_TEXT_FIELD_NUMBER: _ClassVar[int]
    OPTION3_NEXT_FIELD_NUMBER: _ClassVar[int]
    id: int
    npc_id: int
    speaker: str
    text: str
    is_start: bool
    option1_text: str
    option1_next: int
    option2_text: str
    option2_next: int
    option3_text: str
    option3_next: int
    def __init__(self, id: _Optional[int] = ..., npc_id: _Optional[int] = ..., speaker: _Optional[str] = ..., text: _Optional[str] = ..., is_start: bool = ..., option1_text: _Optional[str] = ..., option1_next: _Optional[int] = ..., option2_text: _Optional[str] = ..., option2_next: _Optional[int] = ..., option3_text: _Optional[str] = ..., option3_next: _Optional[int] = ...) -> None: ...

class DialogueTableData(_message.Message):
    __slots__ = ("rows",)
    ROWS_FIELD_NUMBER: _ClassVar[int]
    rows: _containers.RepeatedCompositeFieldContainer[DialogueRow]
    def __init__(self, rows: _Optional[_Iterable[_Union[DialogueRow, _Mapping]]] = ...) -> None: ...
