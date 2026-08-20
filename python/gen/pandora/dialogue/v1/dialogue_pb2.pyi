from pandora.common.v1 import errcode_pb2 as _errcode_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class DialogueOption(_message.Message):
    __slots__ = ("option_id", "text", "visible")
    OPTION_ID_FIELD_NUMBER: _ClassVar[int]
    TEXT_FIELD_NUMBER: _ClassVar[int]
    VISIBLE_FIELD_NUMBER: _ClassVar[int]
    option_id: str
    text: str
    visible: bool
    def __init__(self, option_id: _Optional[str] = ..., text: _Optional[str] = ..., visible: bool = ...) -> None: ...

class DialogueState(_message.Message):
    __slots__ = ("dialogue_id", "npc_id", "node_id", "speaker", "text", "options", "ended")
    DIALOGUE_ID_FIELD_NUMBER: _ClassVar[int]
    NPC_ID_FIELD_NUMBER: _ClassVar[int]
    NODE_ID_FIELD_NUMBER: _ClassVar[int]
    SPEAKER_FIELD_NUMBER: _ClassVar[int]
    TEXT_FIELD_NUMBER: _ClassVar[int]
    OPTIONS_FIELD_NUMBER: _ClassVar[int]
    ENDED_FIELD_NUMBER: _ClassVar[int]
    dialogue_id: int
    npc_id: int
    node_id: str
    speaker: str
    text: str
    options: _containers.RepeatedCompositeFieldContainer[DialogueOption]
    ended: bool
    def __init__(self, dialogue_id: _Optional[int] = ..., npc_id: _Optional[int] = ..., node_id: _Optional[str] = ..., speaker: _Optional[str] = ..., text: _Optional[str] = ..., options: _Optional[_Iterable[_Union[DialogueOption, _Mapping]]] = ..., ended: bool = ...) -> None: ...

class StartDialogueRequest(_message.Message):
    __slots__ = ("npc_id",)
    NPC_ID_FIELD_NUMBER: _ClassVar[int]
    npc_id: int
    def __init__(self, npc_id: _Optional[int] = ...) -> None: ...

class StartDialogueResponse(_message.Message):
    __slots__ = ("code", "state")
    CODE_FIELD_NUMBER: _ClassVar[int]
    STATE_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    state: DialogueState
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ..., state: _Optional[_Union[DialogueState, _Mapping]] = ...) -> None: ...

class ChooseOptionRequest(_message.Message):
    __slots__ = ("dialogue_id", "option_id")
    DIALOGUE_ID_FIELD_NUMBER: _ClassVar[int]
    OPTION_ID_FIELD_NUMBER: _ClassVar[int]
    dialogue_id: int
    option_id: str
    def __init__(self, dialogue_id: _Optional[int] = ..., option_id: _Optional[str] = ...) -> None: ...

class ChooseOptionResponse(_message.Message):
    __slots__ = ("code", "state")
    CODE_FIELD_NUMBER: _ClassVar[int]
    STATE_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    state: DialogueState
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ..., state: _Optional[_Union[DialogueState, _Mapping]] = ...) -> None: ...

class EndDialogueRequest(_message.Message):
    __slots__ = ("dialogue_id",)
    DIALOGUE_ID_FIELD_NUMBER: _ClassVar[int]
    dialogue_id: int
    def __init__(self, dialogue_id: _Optional[int] = ...) -> None: ...

class EndDialogueResponse(_message.Message):
    __slots__ = ("code",)
    CODE_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ...) -> None: ...
