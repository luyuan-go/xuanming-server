from pandora.common.v1 import errcode_pb2 as _errcode_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class LocationState(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    LOCATION_STATE_UNSPECIFIED: _ClassVar[LocationState]
    LOCATION_STATE_OFFLINE: _ClassVar[LocationState]
    LOCATION_STATE_LOGIN_PENDING: _ClassVar[LocationState]
    LOCATION_STATE_HUB: _ClassVar[LocationState]
    LOCATION_STATE_MATCHING: _ClassVar[LocationState]
    LOCATION_STATE_BATTLE: _ClassVar[LocationState]

class PlacementRoute(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    PLACEMENT_ROUTE_UNSPECIFIED: _ClassVar[PlacementRoute]
    PLACEMENT_ROUTE_HUB: _ClassVar[PlacementRoute]
    PLACEMENT_ROUTE_BATTLE: _ClassVar[PlacementRoute]

class PlacementTransitionState(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    PLACEMENT_TRANSITION_STATE_UNSPECIFIED: _ClassVar[PlacementTransitionState]
    PLACEMENT_TRANSITION_STATE_STABLE: _ClassVar[PlacementTransitionState]
    PLACEMENT_TRANSITION_STATE_PENDING: _ClassVar[PlacementTransitionState]

class PlacementProofType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    PLACEMENT_PROOF_TYPE_UNSPECIFIED: _ClassVar[PlacementProofType]
    PLACEMENT_PROOF_TYPE_MATCH_TERMINAL: _ClassVar[PlacementProofType]
    PLACEMENT_PROOF_TYPE_PLAYER_LEAVE: _ClassVar[PlacementProofType]
    PLACEMENT_PROOF_TYPE_ACCOUNT_BOOTSTRAP: _ClassVar[PlacementProofType]
    PLACEMENT_PROOF_TYPE_MATCH_START: _ClassVar[PlacementProofType]
    PLACEMENT_PROOF_TYPE_HUB_TRANSFER: _ClassVar[PlacementProofType]

class PlacementTargetUnavailableReason(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    PLACEMENT_TARGET_UNAVAILABLE_REASON_UNSPECIFIED: _ClassVar[PlacementTargetUnavailableReason]
    PLACEMENT_TARGET_UNAVAILABLE_REASON_INSTANCE_TERMINATED: _ClassVar[PlacementTargetUnavailableReason]
    PLACEMENT_TARGET_UNAVAILABLE_REASON_RESERVATION_EXPIRED_UNUSED: _ClassVar[PlacementTargetUnavailableReason]
    PLACEMENT_TARGET_UNAVAILABLE_REASON_ALLOCATION_REVOKED: _ClassVar[PlacementTargetUnavailableReason]

class PlacementSourceDepartureProofType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    PLACEMENT_SOURCE_DEPARTURE_PROOF_TYPE_UNSPECIFIED: _ClassVar[PlacementSourceDepartureProofType]
    PLACEMENT_SOURCE_DEPARTURE_PROOF_TYPE_HUB_DEPARTURE: _ClassVar[PlacementSourceDepartureProofType]
    PLACEMENT_SOURCE_DEPARTURE_PROOF_TYPE_BATTLE_DEPARTURE: _ClassVar[PlacementSourceDepartureProofType]

class PresenceStatus(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    PRESENCE_STATUS_UNSPECIFIED: _ClassVar[PresenceStatus]
    PRESENCE_STATUS_OFFLINE: _ClassVar[PresenceStatus]
    PRESENCE_STATUS_ONLINE: _ClassVar[PresenceStatus]
    PRESENCE_STATUS_IN_GAME: _ClassVar[PresenceStatus]
LOCATION_STATE_UNSPECIFIED: LocationState
LOCATION_STATE_OFFLINE: LocationState
LOCATION_STATE_LOGIN_PENDING: LocationState
LOCATION_STATE_HUB: LocationState
LOCATION_STATE_MATCHING: LocationState
LOCATION_STATE_BATTLE: LocationState
PLACEMENT_ROUTE_UNSPECIFIED: PlacementRoute
PLACEMENT_ROUTE_HUB: PlacementRoute
PLACEMENT_ROUTE_BATTLE: PlacementRoute
PLACEMENT_TRANSITION_STATE_UNSPECIFIED: PlacementTransitionState
PLACEMENT_TRANSITION_STATE_STABLE: PlacementTransitionState
PLACEMENT_TRANSITION_STATE_PENDING: PlacementTransitionState
PLACEMENT_PROOF_TYPE_UNSPECIFIED: PlacementProofType
PLACEMENT_PROOF_TYPE_MATCH_TERMINAL: PlacementProofType
PLACEMENT_PROOF_TYPE_PLAYER_LEAVE: PlacementProofType
PLACEMENT_PROOF_TYPE_ACCOUNT_BOOTSTRAP: PlacementProofType
PLACEMENT_PROOF_TYPE_MATCH_START: PlacementProofType
PLACEMENT_PROOF_TYPE_HUB_TRANSFER: PlacementProofType
PLACEMENT_TARGET_UNAVAILABLE_REASON_UNSPECIFIED: PlacementTargetUnavailableReason
PLACEMENT_TARGET_UNAVAILABLE_REASON_INSTANCE_TERMINATED: PlacementTargetUnavailableReason
PLACEMENT_TARGET_UNAVAILABLE_REASON_RESERVATION_EXPIRED_UNUSED: PlacementTargetUnavailableReason
PLACEMENT_TARGET_UNAVAILABLE_REASON_ALLOCATION_REVOKED: PlacementTargetUnavailableReason
PLACEMENT_SOURCE_DEPARTURE_PROOF_TYPE_UNSPECIFIED: PlacementSourceDepartureProofType
PLACEMENT_SOURCE_DEPARTURE_PROOF_TYPE_HUB_DEPARTURE: PlacementSourceDepartureProofType
PLACEMENT_SOURCE_DEPARTURE_PROOF_TYPE_BATTLE_DEPARTURE: PlacementSourceDepartureProofType
PRESENCE_STATUS_UNSPECIFIED: PresenceStatus
PRESENCE_STATUS_OFFLINE: PresenceStatus
PRESENCE_STATUS_ONLINE: PresenceStatus
PRESENCE_STATUS_IN_GAME: PresenceStatus

class Location(_message.Message):
    __slots__ = ("state", "hub_pod", "shard_id", "match_id", "battle_pod", "updated_at_ms")
    STATE_FIELD_NUMBER: _ClassVar[int]
    HUB_POD_FIELD_NUMBER: _ClassVar[int]
    SHARD_ID_FIELD_NUMBER: _ClassVar[int]
    MATCH_ID_FIELD_NUMBER: _ClassVar[int]
    BATTLE_POD_FIELD_NUMBER: _ClassVar[int]
    UPDATED_AT_MS_FIELD_NUMBER: _ClassVar[int]
    state: LocationState
    hub_pod: str
    shard_id: int
    match_id: int
    battle_pod: str
    updated_at_ms: int
    def __init__(self, state: _Optional[_Union[LocationState, str]] = ..., hub_pod: _Optional[str] = ..., shard_id: _Optional[int] = ..., match_id: _Optional[int] = ..., battle_pod: _Optional[str] = ..., updated_at_ms: _Optional[int] = ...) -> None: ...

class HubPresenceFence(_message.Message):
    __slots__ = ("assignment_id", "admission_id", "admission_seq")
    ASSIGNMENT_ID_FIELD_NUMBER: _ClassVar[int]
    ADMISSION_ID_FIELD_NUMBER: _ClassVar[int]
    ADMISSION_SEQ_FIELD_NUMBER: _ClassVar[int]
    assignment_id: str
    admission_id: str
    admission_seq: int
    def __init__(self, assignment_id: _Optional[str] = ..., admission_id: _Optional[str] = ..., admission_seq: _Optional[int] = ...) -> None: ...

class SetLocationRequest(_message.Message):
    __slots__ = ("player_id", "location", "hub_presence_fence")
    PLAYER_ID_FIELD_NUMBER: _ClassVar[int]
    LOCATION_FIELD_NUMBER: _ClassVar[int]
    HUB_PRESENCE_FENCE_FIELD_NUMBER: _ClassVar[int]
    player_id: int
    location: Location
    hub_presence_fence: HubPresenceFence
    def __init__(self, player_id: _Optional[int] = ..., location: _Optional[_Union[Location, _Mapping]] = ..., hub_presence_fence: _Optional[_Union[HubPresenceFence, _Mapping]] = ...) -> None: ...

class SetLocationResponse(_message.Message):
    __slots__ = ("code",)
    CODE_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ...) -> None: ...

class GetLocationRequest(_message.Message):
    __slots__ = ("player_id",)
    PLAYER_ID_FIELD_NUMBER: _ClassVar[int]
    player_id: int
    def __init__(self, player_id: _Optional[int] = ...) -> None: ...

class GetLocationResponse(_message.Message):
    __slots__ = ("code", "location")
    CODE_FIELD_NUMBER: _ClassVar[int]
    LOCATION_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    location: Location
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ..., location: _Optional[_Union[Location, _Mapping]] = ...) -> None: ...

class BatchGetLocationRequest(_message.Message):
    __slots__ = ("player_ids",)
    PLAYER_IDS_FIELD_NUMBER: _ClassVar[int]
    player_ids: _containers.RepeatedScalarFieldContainer[int]
    def __init__(self, player_ids: _Optional[_Iterable[int]] = ...) -> None: ...

class BatchGetLocationResponse(_message.Message):
    __slots__ = ("code", "locations")
    class LocationsEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: int
        value: Location
        def __init__(self, key: _Optional[int] = ..., value: _Optional[_Union[Location, _Mapping]] = ...) -> None: ...
    CODE_FIELD_NUMBER: _ClassVar[int]
    LOCATIONS_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    locations: _containers.MessageMap[int, Location]
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ..., locations: _Optional[_Mapping[int, Location]] = ...) -> None: ...

class ClearLocationRequest(_message.Message):
    __slots__ = ("player_id",)
    PLAYER_ID_FIELD_NUMBER: _ClassVar[int]
    player_id: int
    def __init__(self, player_id: _Optional[int] = ...) -> None: ...

class ClearLocationResponse(_message.Message):
    __slots__ = ("code",)
    CODE_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ...) -> None: ...

class RefreshHubLocationsRequest(_message.Message):
    __slots__ = ("hub_pod", "player_ids")
    HUB_POD_FIELD_NUMBER: _ClassVar[int]
    PLAYER_IDS_FIELD_NUMBER: _ClassVar[int]
    hub_pod: str
    player_ids: _containers.RepeatedScalarFieldContainer[int]
    def __init__(self, hub_pod: _Optional[str] = ..., player_ids: _Optional[_Iterable[int]] = ...) -> None: ...

class RefreshHubLocationsResponse(_message.Message):
    __slots__ = ("code", "refreshed")
    CODE_FIELD_NUMBER: _ClassVar[int]
    REFRESHED_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    refreshed: int
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ..., refreshed: _Optional[int] = ...) -> None: ...

class ReportDisconnectRequest(_message.Message):
    __slots__ = ("hub_pod", "player_id", "hub_presence_fence")
    HUB_POD_FIELD_NUMBER: _ClassVar[int]
    PLAYER_ID_FIELD_NUMBER: _ClassVar[int]
    HUB_PRESENCE_FENCE_FIELD_NUMBER: _ClassVar[int]
    hub_pod: str
    player_id: int
    hub_presence_fence: HubPresenceFence
    def __init__(self, hub_pod: _Optional[str] = ..., player_id: _Optional[int] = ..., hub_presence_fence: _Optional[_Union[HubPresenceFence, _Mapping]] = ...) -> None: ...

class ReportDisconnectResponse(_message.Message):
    __slots__ = ("code", "shrunk")
    CODE_FIELD_NUMBER: _ClassVar[int]
    SHRUNK_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    shrunk: bool
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ..., shrunk: bool = ...) -> None: ...

class BatchGetLastSeenRequest(_message.Message):
    __slots__ = ("player_ids",)
    PLAYER_IDS_FIELD_NUMBER: _ClassVar[int]
    player_ids: _containers.RepeatedScalarFieldContainer[int]
    def __init__(self, player_ids: _Optional[_Iterable[int]] = ...) -> None: ...

class BatchGetLastSeenResponse(_message.Message):
    __slots__ = ("code", "last_seen_ms")
    class LastSeenMsEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: int
        value: int
        def __init__(self, key: _Optional[int] = ..., value: _Optional[int] = ...) -> None: ...
    CODE_FIELD_NUMBER: _ClassVar[int]
    LAST_SEEN_MS_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    last_seen_ms: _containers.ScalarMap[int, int]
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ..., last_seen_ms: _Optional[_Mapping[int, int]] = ...) -> None: ...

class PlayerLeftHubEvent(_message.Message):
    __slots__ = ("player_id", "left_at_ms", "hub_pod")
    PLAYER_ID_FIELD_NUMBER: _ClassVar[int]
    LEFT_AT_MS_FIELD_NUMBER: _ClassVar[int]
    HUB_POD_FIELD_NUMBER: _ClassVar[int]
    player_id: int
    left_at_ms: int
    hub_pod: str
    def __init__(self, player_id: _Optional[int] = ..., left_at_ms: _Optional[int] = ..., hub_pod: _Optional[str] = ...) -> None: ...

class PlayerPlacementStorageRecord(_message.Message):
    __slots__ = ("player_id", "current_route", "target_route", "transition_state", "version", "operation_id", "match_id", "source_match_id", "ds_pod_name", "ds_instance_uid", "hub_assignment_id", "updated_at_ms", "lease_deadline_ms", "proof_type", "proof_id", "admission_id", "target_match_id", "ds_instance_epoch", "allocation_id", "release_track", "retarget_count", "last_retarget_proof_id", "last_retarget_reason", "source_placement_version", "source_operation_id", "source_ds_pod_name", "source_ds_instance_uid", "source_ds_instance_epoch", "source_hub_assignment_id", "source_allocation_id", "source_release_track", "source_departure_confirmed", "source_departure_proof_type", "source_departure_proof_id", "last_source_departure_proof_type", "last_source_departure_proof_id", "last_source_departure_placement_version", "last_source_departure_operation_id")
    PLAYER_ID_FIELD_NUMBER: _ClassVar[int]
    CURRENT_ROUTE_FIELD_NUMBER: _ClassVar[int]
    TARGET_ROUTE_FIELD_NUMBER: _ClassVar[int]
    TRANSITION_STATE_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    OPERATION_ID_FIELD_NUMBER: _ClassVar[int]
    MATCH_ID_FIELD_NUMBER: _ClassVar[int]
    SOURCE_MATCH_ID_FIELD_NUMBER: _ClassVar[int]
    DS_POD_NAME_FIELD_NUMBER: _ClassVar[int]
    DS_INSTANCE_UID_FIELD_NUMBER: _ClassVar[int]
    HUB_ASSIGNMENT_ID_FIELD_NUMBER: _ClassVar[int]
    UPDATED_AT_MS_FIELD_NUMBER: _ClassVar[int]
    LEASE_DEADLINE_MS_FIELD_NUMBER: _ClassVar[int]
    PROOF_TYPE_FIELD_NUMBER: _ClassVar[int]
    PROOF_ID_FIELD_NUMBER: _ClassVar[int]
    ADMISSION_ID_FIELD_NUMBER: _ClassVar[int]
    TARGET_MATCH_ID_FIELD_NUMBER: _ClassVar[int]
    DS_INSTANCE_EPOCH_FIELD_NUMBER: _ClassVar[int]
    ALLOCATION_ID_FIELD_NUMBER: _ClassVar[int]
    RELEASE_TRACK_FIELD_NUMBER: _ClassVar[int]
    RETARGET_COUNT_FIELD_NUMBER: _ClassVar[int]
    LAST_RETARGET_PROOF_ID_FIELD_NUMBER: _ClassVar[int]
    LAST_RETARGET_REASON_FIELD_NUMBER: _ClassVar[int]
    SOURCE_PLACEMENT_VERSION_FIELD_NUMBER: _ClassVar[int]
    SOURCE_OPERATION_ID_FIELD_NUMBER: _ClassVar[int]
    SOURCE_DS_POD_NAME_FIELD_NUMBER: _ClassVar[int]
    SOURCE_DS_INSTANCE_UID_FIELD_NUMBER: _ClassVar[int]
    SOURCE_DS_INSTANCE_EPOCH_FIELD_NUMBER: _ClassVar[int]
    SOURCE_HUB_ASSIGNMENT_ID_FIELD_NUMBER: _ClassVar[int]
    SOURCE_ALLOCATION_ID_FIELD_NUMBER: _ClassVar[int]
    SOURCE_RELEASE_TRACK_FIELD_NUMBER: _ClassVar[int]
    SOURCE_DEPARTURE_CONFIRMED_FIELD_NUMBER: _ClassVar[int]
    SOURCE_DEPARTURE_PROOF_TYPE_FIELD_NUMBER: _ClassVar[int]
    SOURCE_DEPARTURE_PROOF_ID_FIELD_NUMBER: _ClassVar[int]
    LAST_SOURCE_DEPARTURE_PROOF_TYPE_FIELD_NUMBER: _ClassVar[int]
    LAST_SOURCE_DEPARTURE_PROOF_ID_FIELD_NUMBER: _ClassVar[int]
    LAST_SOURCE_DEPARTURE_PLACEMENT_VERSION_FIELD_NUMBER: _ClassVar[int]
    LAST_SOURCE_DEPARTURE_OPERATION_ID_FIELD_NUMBER: _ClassVar[int]
    player_id: int
    current_route: PlacementRoute
    target_route: PlacementRoute
    transition_state: PlacementTransitionState
    version: int
    operation_id: str
    match_id: int
    source_match_id: int
    ds_pod_name: str
    ds_instance_uid: str
    hub_assignment_id: str
    updated_at_ms: int
    lease_deadline_ms: int
    proof_type: PlacementProofType
    proof_id: str
    admission_id: str
    target_match_id: int
    ds_instance_epoch: int
    allocation_id: str
    release_track: str
    retarget_count: int
    last_retarget_proof_id: str
    last_retarget_reason: PlacementTargetUnavailableReason
    source_placement_version: int
    source_operation_id: str
    source_ds_pod_name: str
    source_ds_instance_uid: str
    source_ds_instance_epoch: int
    source_hub_assignment_id: str
    source_allocation_id: str
    source_release_track: str
    source_departure_confirmed: bool
    source_departure_proof_type: PlacementSourceDepartureProofType
    source_departure_proof_id: str
    last_source_departure_proof_type: PlacementSourceDepartureProofType
    last_source_departure_proof_id: str
    last_source_departure_placement_version: int
    last_source_departure_operation_id: str
    def __init__(self, player_id: _Optional[int] = ..., current_route: _Optional[_Union[PlacementRoute, str]] = ..., target_route: _Optional[_Union[PlacementRoute, str]] = ..., transition_state: _Optional[_Union[PlacementTransitionState, str]] = ..., version: _Optional[int] = ..., operation_id: _Optional[str] = ..., match_id: _Optional[int] = ..., source_match_id: _Optional[int] = ..., ds_pod_name: _Optional[str] = ..., ds_instance_uid: _Optional[str] = ..., hub_assignment_id: _Optional[str] = ..., updated_at_ms: _Optional[int] = ..., lease_deadline_ms: _Optional[int] = ..., proof_type: _Optional[_Union[PlacementProofType, str]] = ..., proof_id: _Optional[str] = ..., admission_id: _Optional[str] = ..., target_match_id: _Optional[int] = ..., ds_instance_epoch: _Optional[int] = ..., allocation_id: _Optional[str] = ..., release_track: _Optional[str] = ..., retarget_count: _Optional[int] = ..., last_retarget_proof_id: _Optional[str] = ..., last_retarget_reason: _Optional[_Union[PlacementTargetUnavailableReason, str]] = ..., source_placement_version: _Optional[int] = ..., source_operation_id: _Optional[str] = ..., source_ds_pod_name: _Optional[str] = ..., source_ds_instance_uid: _Optional[str] = ..., source_ds_instance_epoch: _Optional[int] = ..., source_hub_assignment_id: _Optional[str] = ..., source_allocation_id: _Optional[str] = ..., source_release_track: _Optional[str] = ..., source_departure_confirmed: bool = ..., source_departure_proof_type: _Optional[_Union[PlacementSourceDepartureProofType, str]] = ..., source_departure_proof_id: _Optional[str] = ..., last_source_departure_proof_type: _Optional[_Union[PlacementSourceDepartureProofType, str]] = ..., last_source_departure_proof_id: _Optional[str] = ..., last_source_departure_placement_version: _Optional[int] = ..., last_source_departure_operation_id: _Optional[str] = ...) -> None: ...

class GetPlacementRequest(_message.Message):
    __slots__ = ("player_id",)
    PLAYER_ID_FIELD_NUMBER: _ClassVar[int]
    player_id: int
    def __init__(self, player_id: _Optional[int] = ...) -> None: ...

class GetPlacementResponse(_message.Message):
    __slots__ = ("code", "found", "placement")
    CODE_FIELD_NUMBER: _ClassVar[int]
    FOUND_FIELD_NUMBER: _ClassVar[int]
    PLACEMENT_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    found: bool
    placement: PlayerPlacementStorageRecord
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ..., found: bool = ..., placement: _Optional[_Union[PlayerPlacementStorageRecord, _Mapping]] = ...) -> None: ...

class BeginPlacementTransitionRequest(_message.Message):
    __slots__ = ("player_id", "expected_version", "target_route", "operation_id", "source_match_id", "proof_type", "proof_id", "lease_deadline_ms", "target_match_id", "proof_signature")
    PLAYER_ID_FIELD_NUMBER: _ClassVar[int]
    EXPECTED_VERSION_FIELD_NUMBER: _ClassVar[int]
    TARGET_ROUTE_FIELD_NUMBER: _ClassVar[int]
    OPERATION_ID_FIELD_NUMBER: _ClassVar[int]
    SOURCE_MATCH_ID_FIELD_NUMBER: _ClassVar[int]
    PROOF_TYPE_FIELD_NUMBER: _ClassVar[int]
    PROOF_ID_FIELD_NUMBER: _ClassVar[int]
    LEASE_DEADLINE_MS_FIELD_NUMBER: _ClassVar[int]
    TARGET_MATCH_ID_FIELD_NUMBER: _ClassVar[int]
    PROOF_SIGNATURE_FIELD_NUMBER: _ClassVar[int]
    player_id: int
    expected_version: int
    target_route: PlacementRoute
    operation_id: str
    source_match_id: int
    proof_type: PlacementProofType
    proof_id: str
    lease_deadline_ms: int
    target_match_id: int
    proof_signature: str
    def __init__(self, player_id: _Optional[int] = ..., expected_version: _Optional[int] = ..., target_route: _Optional[_Union[PlacementRoute, str]] = ..., operation_id: _Optional[str] = ..., source_match_id: _Optional[int] = ..., proof_type: _Optional[_Union[PlacementProofType, str]] = ..., proof_id: _Optional[str] = ..., lease_deadline_ms: _Optional[int] = ..., target_match_id: _Optional[int] = ..., proof_signature: _Optional[str] = ...) -> None: ...

class BeginPlacementTransitionResponse(_message.Message):
    __slots__ = ("code", "placement")
    CODE_FIELD_NUMBER: _ClassVar[int]
    PLACEMENT_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    placement: PlayerPlacementStorageRecord
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ..., placement: _Optional[_Union[PlayerPlacementStorageRecord, _Mapping]] = ...) -> None: ...

class BindPlacementTargetRequest(_message.Message):
    __slots__ = ("player_id", "placement_version", "operation_id", "target_route", "ds_pod_name", "ds_instance_uid", "hub_assignment_id", "target_match_id", "ds_instance_epoch", "allocation_id", "release_track", "lease_deadline_ms")
    PLAYER_ID_FIELD_NUMBER: _ClassVar[int]
    PLACEMENT_VERSION_FIELD_NUMBER: _ClassVar[int]
    OPERATION_ID_FIELD_NUMBER: _ClassVar[int]
    TARGET_ROUTE_FIELD_NUMBER: _ClassVar[int]
    DS_POD_NAME_FIELD_NUMBER: _ClassVar[int]
    DS_INSTANCE_UID_FIELD_NUMBER: _ClassVar[int]
    HUB_ASSIGNMENT_ID_FIELD_NUMBER: _ClassVar[int]
    TARGET_MATCH_ID_FIELD_NUMBER: _ClassVar[int]
    DS_INSTANCE_EPOCH_FIELD_NUMBER: _ClassVar[int]
    ALLOCATION_ID_FIELD_NUMBER: _ClassVar[int]
    RELEASE_TRACK_FIELD_NUMBER: _ClassVar[int]
    LEASE_DEADLINE_MS_FIELD_NUMBER: _ClassVar[int]
    player_id: int
    placement_version: int
    operation_id: str
    target_route: PlacementRoute
    ds_pod_name: str
    ds_instance_uid: str
    hub_assignment_id: str
    target_match_id: int
    ds_instance_epoch: int
    allocation_id: str
    release_track: str
    lease_deadline_ms: int
    def __init__(self, player_id: _Optional[int] = ..., placement_version: _Optional[int] = ..., operation_id: _Optional[str] = ..., target_route: _Optional[_Union[PlacementRoute, str]] = ..., ds_pod_name: _Optional[str] = ..., ds_instance_uid: _Optional[str] = ..., hub_assignment_id: _Optional[str] = ..., target_match_id: _Optional[int] = ..., ds_instance_epoch: _Optional[int] = ..., allocation_id: _Optional[str] = ..., release_track: _Optional[str] = ..., lease_deadline_ms: _Optional[int] = ...) -> None: ...

class BindPlacementTargetResponse(_message.Message):
    __slots__ = ("code", "placement")
    CODE_FIELD_NUMBER: _ClassVar[int]
    PLACEMENT_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    placement: PlayerPlacementStorageRecord
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ..., placement: _Optional[_Union[PlayerPlacementStorageRecord, _Mapping]] = ...) -> None: ...

class PlacementTargetIdentity(_message.Message):
    __slots__ = ("ds_pod_name", "ds_instance_uid", "hub_assignment_id", "ds_instance_epoch", "allocation_id", "release_track")
    DS_POD_NAME_FIELD_NUMBER: _ClassVar[int]
    DS_INSTANCE_UID_FIELD_NUMBER: _ClassVar[int]
    HUB_ASSIGNMENT_ID_FIELD_NUMBER: _ClassVar[int]
    DS_INSTANCE_EPOCH_FIELD_NUMBER: _ClassVar[int]
    ALLOCATION_ID_FIELD_NUMBER: _ClassVar[int]
    RELEASE_TRACK_FIELD_NUMBER: _ClassVar[int]
    ds_pod_name: str
    ds_instance_uid: str
    hub_assignment_id: str
    ds_instance_epoch: int
    allocation_id: str
    release_track: str
    def __init__(self, ds_pod_name: _Optional[str] = ..., ds_instance_uid: _Optional[str] = ..., hub_assignment_id: _Optional[str] = ..., ds_instance_epoch: _Optional[int] = ..., allocation_id: _Optional[str] = ..., release_track: _Optional[str] = ...) -> None: ...

class RetargetPlacementTargetRequest(_message.Message):
    __slots__ = ("player_id", "placement_version", "operation_id", "target_route", "target_match_id", "expected_target", "replacement_version", "replacement_operation_id", "replacement_target", "proof_type", "reason", "proof_id", "proof_signature", "lease_deadline_ms")
    PLAYER_ID_FIELD_NUMBER: _ClassVar[int]
    PLACEMENT_VERSION_FIELD_NUMBER: _ClassVar[int]
    OPERATION_ID_FIELD_NUMBER: _ClassVar[int]
    TARGET_ROUTE_FIELD_NUMBER: _ClassVar[int]
    TARGET_MATCH_ID_FIELD_NUMBER: _ClassVar[int]
    EXPECTED_TARGET_FIELD_NUMBER: _ClassVar[int]
    REPLACEMENT_VERSION_FIELD_NUMBER: _ClassVar[int]
    REPLACEMENT_OPERATION_ID_FIELD_NUMBER: _ClassVar[int]
    REPLACEMENT_TARGET_FIELD_NUMBER: _ClassVar[int]
    PROOF_TYPE_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    PROOF_ID_FIELD_NUMBER: _ClassVar[int]
    PROOF_SIGNATURE_FIELD_NUMBER: _ClassVar[int]
    LEASE_DEADLINE_MS_FIELD_NUMBER: _ClassVar[int]
    player_id: int
    placement_version: int
    operation_id: str
    target_route: PlacementRoute
    target_match_id: int
    expected_target: PlacementTargetIdentity
    replacement_version: int
    replacement_operation_id: str
    replacement_target: PlacementTargetIdentity
    proof_type: PlacementProofType
    reason: PlacementTargetUnavailableReason
    proof_id: str
    proof_signature: str
    lease_deadline_ms: int
    def __init__(self, player_id: _Optional[int] = ..., placement_version: _Optional[int] = ..., operation_id: _Optional[str] = ..., target_route: _Optional[_Union[PlacementRoute, str]] = ..., target_match_id: _Optional[int] = ..., expected_target: _Optional[_Union[PlacementTargetIdentity, _Mapping]] = ..., replacement_version: _Optional[int] = ..., replacement_operation_id: _Optional[str] = ..., replacement_target: _Optional[_Union[PlacementTargetIdentity, _Mapping]] = ..., proof_type: _Optional[_Union[PlacementProofType, str]] = ..., reason: _Optional[_Union[PlacementTargetUnavailableReason, str]] = ..., proof_id: _Optional[str] = ..., proof_signature: _Optional[str] = ..., lease_deadline_ms: _Optional[int] = ...) -> None: ...

class RetargetPlacementTargetResponse(_message.Message):
    __slots__ = ("code", "placement")
    CODE_FIELD_NUMBER: _ClassVar[int]
    PLACEMENT_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    placement: PlayerPlacementStorageRecord
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ..., placement: _Optional[_Union[PlayerPlacementStorageRecord, _Mapping]] = ...) -> None: ...

class ConfirmPlacementSourceDepartureRequest(_message.Message):
    __slots__ = ("player_id", "placement_version", "operation_id", "target_route", "target_match_id", "source_placement_version", "source_operation_id", "source_route", "source_match_id", "source_target", "proof_type", "proof_id", "proof_signature")
    PLAYER_ID_FIELD_NUMBER: _ClassVar[int]
    PLACEMENT_VERSION_FIELD_NUMBER: _ClassVar[int]
    OPERATION_ID_FIELD_NUMBER: _ClassVar[int]
    TARGET_ROUTE_FIELD_NUMBER: _ClassVar[int]
    TARGET_MATCH_ID_FIELD_NUMBER: _ClassVar[int]
    SOURCE_PLACEMENT_VERSION_FIELD_NUMBER: _ClassVar[int]
    SOURCE_OPERATION_ID_FIELD_NUMBER: _ClassVar[int]
    SOURCE_ROUTE_FIELD_NUMBER: _ClassVar[int]
    SOURCE_MATCH_ID_FIELD_NUMBER: _ClassVar[int]
    SOURCE_TARGET_FIELD_NUMBER: _ClassVar[int]
    PROOF_TYPE_FIELD_NUMBER: _ClassVar[int]
    PROOF_ID_FIELD_NUMBER: _ClassVar[int]
    PROOF_SIGNATURE_FIELD_NUMBER: _ClassVar[int]
    player_id: int
    placement_version: int
    operation_id: str
    target_route: PlacementRoute
    target_match_id: int
    source_placement_version: int
    source_operation_id: str
    source_route: PlacementRoute
    source_match_id: int
    source_target: PlacementTargetIdentity
    proof_type: PlacementSourceDepartureProofType
    proof_id: str
    proof_signature: str
    def __init__(self, player_id: _Optional[int] = ..., placement_version: _Optional[int] = ..., operation_id: _Optional[str] = ..., target_route: _Optional[_Union[PlacementRoute, str]] = ..., target_match_id: _Optional[int] = ..., source_placement_version: _Optional[int] = ..., source_operation_id: _Optional[str] = ..., source_route: _Optional[_Union[PlacementRoute, str]] = ..., source_match_id: _Optional[int] = ..., source_target: _Optional[_Union[PlacementTargetIdentity, _Mapping]] = ..., proof_type: _Optional[_Union[PlacementSourceDepartureProofType, str]] = ..., proof_id: _Optional[str] = ..., proof_signature: _Optional[str] = ...) -> None: ...

class ConfirmPlacementSourceDepartureResponse(_message.Message):
    __slots__ = ("code", "confirmed", "placement")
    CODE_FIELD_NUMBER: _ClassVar[int]
    CONFIRMED_FIELD_NUMBER: _ClassVar[int]
    PLACEMENT_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    confirmed: bool
    placement: PlayerPlacementStorageRecord
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ..., confirmed: bool = ..., placement: _Optional[_Union[PlayerPlacementStorageRecord, _Mapping]] = ...) -> None: ...

class CommitPlacementAdmissionRequest(_message.Message):
    __slots__ = ("player_id", "placement_version", "operation_id", "target_route", "ds_pod_name", "ds_instance_uid", "hub_assignment_id", "admission_id", "target_match_id", "ds_instance_epoch", "allocation_id", "release_track")
    PLAYER_ID_FIELD_NUMBER: _ClassVar[int]
    PLACEMENT_VERSION_FIELD_NUMBER: _ClassVar[int]
    OPERATION_ID_FIELD_NUMBER: _ClassVar[int]
    TARGET_ROUTE_FIELD_NUMBER: _ClassVar[int]
    DS_POD_NAME_FIELD_NUMBER: _ClassVar[int]
    DS_INSTANCE_UID_FIELD_NUMBER: _ClassVar[int]
    HUB_ASSIGNMENT_ID_FIELD_NUMBER: _ClassVar[int]
    ADMISSION_ID_FIELD_NUMBER: _ClassVar[int]
    TARGET_MATCH_ID_FIELD_NUMBER: _ClassVar[int]
    DS_INSTANCE_EPOCH_FIELD_NUMBER: _ClassVar[int]
    ALLOCATION_ID_FIELD_NUMBER: _ClassVar[int]
    RELEASE_TRACK_FIELD_NUMBER: _ClassVar[int]
    player_id: int
    placement_version: int
    operation_id: str
    target_route: PlacementRoute
    ds_pod_name: str
    ds_instance_uid: str
    hub_assignment_id: str
    admission_id: str
    target_match_id: int
    ds_instance_epoch: int
    allocation_id: str
    release_track: str
    def __init__(self, player_id: _Optional[int] = ..., placement_version: _Optional[int] = ..., operation_id: _Optional[str] = ..., target_route: _Optional[_Union[PlacementRoute, str]] = ..., ds_pod_name: _Optional[str] = ..., ds_instance_uid: _Optional[str] = ..., hub_assignment_id: _Optional[str] = ..., admission_id: _Optional[str] = ..., target_match_id: _Optional[int] = ..., ds_instance_epoch: _Optional[int] = ..., allocation_id: _Optional[str] = ..., release_track: _Optional[str] = ...) -> None: ...

class CommitPlacementAdmissionResponse(_message.Message):
    __slots__ = ("code", "committed", "placement")
    CODE_FIELD_NUMBER: _ClassVar[int]
    COMMITTED_FIELD_NUMBER: _ClassVar[int]
    PLACEMENT_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    committed: bool
    placement: PlayerPlacementStorageRecord
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ..., committed: bool = ..., placement: _Optional[_Union[PlayerPlacementStorageRecord, _Mapping]] = ...) -> None: ...

class BootstrapPlacementRequest(_message.Message):
    __slots__ = ("player_id", "operation_id", "proof_id", "proof_signature", "lease_deadline_ms")
    PLAYER_ID_FIELD_NUMBER: _ClassVar[int]
    OPERATION_ID_FIELD_NUMBER: _ClassVar[int]
    PROOF_ID_FIELD_NUMBER: _ClassVar[int]
    PROOF_SIGNATURE_FIELD_NUMBER: _ClassVar[int]
    LEASE_DEADLINE_MS_FIELD_NUMBER: _ClassVar[int]
    player_id: int
    operation_id: str
    proof_id: str
    proof_signature: str
    lease_deadline_ms: int
    def __init__(self, player_id: _Optional[int] = ..., operation_id: _Optional[str] = ..., proof_id: _Optional[str] = ..., proof_signature: _Optional[str] = ..., lease_deadline_ms: _Optional[int] = ...) -> None: ...

class BootstrapPlacementResponse(_message.Message):
    __slots__ = ("code", "placement")
    CODE_FIELD_NUMBER: _ClassVar[int]
    PLACEMENT_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    placement: PlayerPlacementStorageRecord
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ..., placement: _Optional[_Union[PlayerPlacementStorageRecord, _Mapping]] = ...) -> None: ...

class SubscribePresenceRequest(_message.Message):
    __slots__ = ("subscriber_id", "watched_player_ids")
    SUBSCRIBER_ID_FIELD_NUMBER: _ClassVar[int]
    WATCHED_PLAYER_IDS_FIELD_NUMBER: _ClassVar[int]
    subscriber_id: int
    watched_player_ids: _containers.RepeatedScalarFieldContainer[int]
    def __init__(self, subscriber_id: _Optional[int] = ..., watched_player_ids: _Optional[_Iterable[int]] = ...) -> None: ...

class SubscribePresenceResponse(_message.Message):
    __slots__ = ("code",)
    CODE_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ...) -> None: ...

class UnsubscribePresenceRequest(_message.Message):
    __slots__ = ("subscriber_id",)
    SUBSCRIBER_ID_FIELD_NUMBER: _ClassVar[int]
    subscriber_id: int
    def __init__(self, subscriber_id: _Optional[int] = ...) -> None: ...

class UnsubscribePresenceResponse(_message.Message):
    __slots__ = ("code",)
    CODE_FIELD_NUMBER: _ClassVar[int]
    code: _errcode_pb2.ErrCode
    def __init__(self, code: _Optional[_Union[_errcode_pb2.ErrCode, str]] = ...) -> None: ...

class PresenceChange(_message.Message):
    __slots__ = ("player_id", "status", "ts_ms")
    PLAYER_ID_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    TS_MS_FIELD_NUMBER: _ClassVar[int]
    player_id: int
    status: PresenceStatus
    ts_ms: int
    def __init__(self, player_id: _Optional[int] = ..., status: _Optional[_Union[PresenceStatus, str]] = ..., ts_ms: _Optional[int] = ...) -> None: ...

class PresenceBatchEvent(_message.Message):
    __slots__ = ("changes",)
    CHANGES_FIELD_NUMBER: _ClassVar[int]
    changes: _containers.RepeatedCompositeFieldContainer[PresenceChange]
    def __init__(self, changes: _Optional[_Iterable[_Union[PresenceChange, _Mapping]]] = ...) -> None: ...
