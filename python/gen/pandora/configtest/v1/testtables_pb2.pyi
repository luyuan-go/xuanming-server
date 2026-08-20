from pandora.config.v1 import excel_pb2 as _excel_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class TestSceneRow(_message.Message):
    __slots__ = ("id", "scene_name")
    ID_FIELD_NUMBER: _ClassVar[int]
    SCENE_NAME_FIELD_NUMBER: _ClassVar[int]
    id: int
    scene_name: str
    def __init__(self, id: _Optional[int] = ..., scene_name: _Optional[str] = ...) -> None: ...

class TestSceneTableData(_message.Message):
    __slots__ = ("rows",)
    ROWS_FIELD_NUMBER: _ClassVar[int]
    rows: _containers.RepeatedCompositeFieldContainer[TestSceneRow]
    def __init__(self, rows: _Optional[_Iterable[_Union[TestSceneRow, _Mapping]]] = ...) -> None: ...

class TestDungeonRow(_message.Message):
    __slots__ = ("id", "scene_id", "difficulty", "code")
    ID_FIELD_NUMBER: _ClassVar[int]
    SCENE_ID_FIELD_NUMBER: _ClassVar[int]
    DIFFICULTY_FIELD_NUMBER: _ClassVar[int]
    CODE_FIELD_NUMBER: _ClassVar[int]
    id: int
    scene_id: int
    difficulty: int
    code: str
    def __init__(self, id: _Optional[int] = ..., scene_id: _Optional[int] = ..., difficulty: _Optional[int] = ..., code: _Optional[str] = ...) -> None: ...

class TestDungeonTableData(_message.Message):
    __slots__ = ("rows",)
    ROWS_FIELD_NUMBER: _ClassVar[int]
    rows: _containers.RepeatedCompositeFieldContainer[TestDungeonRow]
    def __init__(self, rows: _Optional[_Iterable[_Union[TestDungeonRow, _Mapping]]] = ...) -> None: ...
