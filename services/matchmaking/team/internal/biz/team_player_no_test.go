package biz

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"google.golang.org/protobuf/proto"

	teamv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/team/v1"
	"github.com/luyuancpp/pandora/services/matchmaking/team/internal/conf"
)

type playerNoResolverFake struct {
	values map[uint64]uint64
	err    error
	calls  [][]uint64
}

func (f *playerNoResolverFake) ResolvePlayerNos(_ context.Context, playerIDs []uint64) (map[uint64]uint64, error) {
	f.calls = append(f.calls, append([]uint64(nil), playerIDs...))
	if f.err != nil {
		return nil, f.err
	}
	return f.values, nil
}

type playerNoPayloadPusher struct {
	payload []byte
}

func (p *playerNoPayloadPusher) PushTeamUpdate(_ context.Context, _ uint64, _ []uint64, payload []byte) (int, error) {
	p.payload = append([]byte(nil), payload...)
	return 1, nil
}

func (p *playerNoPayloadPusher) PushTeamEvent(context.Context, uint64, []uint64, []byte, uint32) (int, error) {
	return 0, nil
}

func playerNoTestRecord() *teamv1.TeamStorageRecord {
	return &teamv1.TeamStorageRecord{
		TeamId:    101,
		CaptainId: 11,
		Members: []*teamv1.TeamMemberStorageRecord{
			{PlayerId: 11},
			{PlayerId: 22},
		},
	}
}

func TestTeamToProtoResolvesEveryMemberPlayerNoInOneBatch(t *testing.T) {
	resolver := &playerNoResolverFake{values: map[uint64]uint64{11: 100011, 22: 100022}}
	uc := NewTeamUsecase(nil, nil, conf.TeamConf{})
	uc.SetPlayerNoResolver(resolver)

	got, err := uc.TeamToProto(context.Background(), playerNoTestRecord())
	if err != nil {
		t.Fatalf("TeamToProto: %v", err)
	}

	if len(resolver.calls) != 1 {
		t.Fatalf("ResolvePlayerNos calls = %d, want exactly one batch", len(resolver.calls))
	}
	if want := []uint64{11, 22}; !reflect.DeepEqual(resolver.calls[0], want) {
		t.Fatalf("ResolvePlayerNos player_ids = %v, want %v", resolver.calls[0], want)
	}
	if got.GetMembers()[0].GetPlayerNo() != 100011 || got.GetMembers()[1].GetPlayerNo() != 100022 {
		t.Fatalf("player_no projection = [%d %d], want [100011 100022]",
			got.GetMembers()[0].GetPlayerNo(), got.GetMembers()[1].GetPlayerNo())
	}
}

func TestTeamToProtoPlayerNoFailureIsFailSoft(t *testing.T) {
	resolver := &playerNoResolverFake{err: errors.New("login unavailable")}
	uc := NewTeamUsecase(nil, nil, conf.TeamConf{})
	uc.SetPlayerNoResolver(resolver)

	got, err := uc.TeamToProto(context.Background(), playerNoTestRecord())
	if err != nil {
		t.Fatalf("TeamToProto: %v", err)
	}

	if got == nil || len(got.GetMembers()) != 2 {
		t.Fatalf("display dependency failure blocked team snapshot: %#v", got)
	}
	for _, member := range got.GetMembers() {
		if member.GetPlayerNo() != 0 {
			t.Fatalf("fail-soft player_no = %d, want 0", member.GetPlayerNo())
		}
	}
}

func TestTeamUpdatePushCarriesResolvedPlayerNos(t *testing.T) {
	resolver := &playerNoResolverFake{values: map[uint64]uint64{11: 100011, 22: 100022}}
	pusher := &playerNoPayloadPusher{}
	uc := NewTeamUsecase(nil, pusher, conf.TeamConf{})
	uc.SetPlayerNoResolver(resolver)

	uc.pushUpdate(context.Background(), 11, []uint64{22}, playerNoTestRecord(),
		teamv1.TeamUpdateReason_TEAM_UPDATE_REASON_MEMBER_JOINED, 0)

	var event teamv1.TeamUpdateEvent
	if err := proto.Unmarshal(pusher.payload, &event); err != nil {
		t.Fatalf("unmarshal TeamUpdateEvent: %v", err)
	}
	if got := event.GetTeam().GetMembers()[1].GetPlayerNo(); got != 100022 {
		t.Fatalf("pushed member player_no = %d, want 100022", got)
	}
	if len(resolver.calls) != 1 {
		t.Fatalf("push ResolvePlayerNos calls = %d, want one batch", len(resolver.calls))
	}
}
