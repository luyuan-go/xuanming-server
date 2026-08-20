package biz

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	teamv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/team/v1"
	"github.com/luyuancpp/pandora/services/matchmaking/team/internal/conf"
)

type playerNameResolverFake struct {
	values map[uint64]string
	err    error
	calls  [][]uint64
}

func (f *playerNameResolverFake) ResolvePlayerNames(_ context.Context, playerIDs []uint64) (map[uint64]string, error) {
	f.calls = append(f.calls, append([]uint64(nil), playerIDs...))
	if f.err != nil {
		return nil, f.err
	}
	return f.values, nil
}

type playerNameResolverFunc func(context.Context, []uint64) (map[uint64]string, error)

func (f playerNameResolverFunc) ResolvePlayerNames(ctx context.Context, ids []uint64) (map[uint64]string, error) {
	return f(ctx, ids)
}

type playerNoResolverFunc func(context.Context, []uint64) (map[uint64]uint64, error)

func (f playerNoResolverFunc) ResolvePlayerNos(ctx context.Context, ids []uint64) (map[uint64]uint64, error) {
	return f(ctx, ids)
}

func TestTeamToProtoUsesPlayerAuthorityNameAndNeverStorageNickname(t *testing.T) {
	record := playerNoTestRecord()
	record.Members[0].Nickname = "forged-storage-alice"
	record.Members[1].Nickname = "stale-storage-bob"
	nameResolver := &playerNameResolverFake{values: map[uint64]string{11: "Alice"}}
	noResolver := &playerNoResolverFake{values: map[uint64]uint64{11: 100011, 22: 100022}}
	uc := NewTeamUsecase(nil, nil, conf.TeamConf{})
	uc.SetPlayerNameResolver(nameResolver)
	uc.SetPlayerNoResolver(noResolver)

	got, err := uc.TeamToProto(context.Background(), record)
	if err != nil {
		t.Fatalf("TeamToProto: %v", err)
	}

	if len(nameResolver.calls) != 1 || !reflect.DeepEqual(nameResolver.calls[0], []uint64{11, 22}) {
		t.Fatalf("ResolvePlayerNames batches=%v, want one [11 22]", nameResolver.calls)
	}
	if got.GetMembers()[0].GetNickname() != "Alice" {
		t.Fatalf("member 11 nickname=%q, want player authority Alice", got.GetMembers()[0].GetNickname())
	}
	if got.GetMembers()[1].GetNickname() != "" {
		t.Fatalf("missing authority name leaked storage nickname %q", got.GetMembers()[1].GetNickname())
	}
	if got.GetMembers()[0].GetPlayerId() != 11 || got.GetMembers()[1].GetPlayerId() != 22 ||
		got.GetMembers()[0].GetPlayerNo() != 100011 || got.GetMembers()[1].GetPlayerNo() != 100022 {
		t.Fatalf("name projection changed identity/player_no: members=%+v", got.GetMembers())
	}
}

func TestTeamToProtoPlayerNameFailureIsEmptyAndKeepsPlayerNo(t *testing.T) {
	record := playerNoTestRecord()
	record.Members[0].Nickname = "must-not-leak"
	nameResolver := &playerNameResolverFake{err: errors.New("player unavailable")}
	noResolver := &playerNoResolverFake{values: map[uint64]uint64{11: 100011, 22: 100022}}
	uc := NewTeamUsecase(nil, nil, conf.TeamConf{})
	uc.SetPlayerNameResolver(nameResolver)
	uc.SetPlayerNoResolver(noResolver)

	got, err := uc.TeamToProto(context.Background(), record)
	if err != nil {
		t.Fatalf("TeamToProto: %v", err)
	}

	for i, member := range got.GetMembers() {
		if member.GetNickname() != "" {
			t.Fatalf("member %d nickname leaked %q", i, member.GetNickname())
		}
	}
	if got.GetMembers()[0].GetPlayerNo() != 100011 || got.GetMembers()[1].GetPlayerNo() != 100022 {
		t.Fatalf("name failure changed player_no: members=%+v", got.GetMembers())
	}
}

func TestTeamToProtoResolvesNameAndPlayerNoConcurrently(t *testing.T) {
	started := make(chan string, 2)
	release := make(chan struct{})
	released := false
	defer func() {
		if !released {
			close(release)
		}
	}()
	uc := NewTeamUsecase(nil, nil, conf.TeamConf{})
	uc.SetPlayerNameResolver(playerNameResolverFunc(func(_ context.Context, _ []uint64) (map[uint64]string, error) {
		started <- "name"
		<-release
		return map[uint64]string{11: "Alice"}, nil
	}))
	uc.SetPlayerNoResolver(playerNoResolverFunc(func(_ context.Context, _ []uint64) (map[uint64]uint64, error) {
		started <- "number"
		<-release
		return map[uint64]uint64{11: 100011}, nil
	}))
	type projectionResult struct {
		team *teamv1.Team
		err  error
	}
	done := make(chan projectionResult, 1)
	go func() {
		team, err := uc.TeamToProto(context.Background(), playerNoTestRecord())
		done <- projectionResult{team: team, err: err}
	}()

	seen := map[string]bool{}
	for len(seen) < 2 {
		select {
		case dependency := <-started:
			seen[dependency] = true
		case <-time.After(time.Second):
			t.Fatalf("display resolvers did not start concurrently; started=%v", seen)
		}
	}
	close(release)
	released = true
	select {
	case got := <-done:
		if got.err != nil {
			t.Fatalf("TeamToProto: %v", got.err)
		}
		if got.team.GetMembers()[0].GetNickname() != "Alice" || got.team.GetMembers()[0].GetPlayerNo() != 100011 {
			t.Fatalf("projection=%+v", got.team.GetMembers()[0])
		}
	case <-time.After(time.Second):
		t.Fatal("TeamToProto did not complete after both dependencies released")
	}
}

func TestTeamUpdatePushCarriesResolvedPlayerNames(t *testing.T) {
	resolver := &playerNameResolverFake{values: map[uint64]string{11: "Alice", 22: "Bob"}}
	pusher := &playerNoPayloadPusher{}
	uc := NewTeamUsecase(nil, pusher, conf.TeamConf{})
	uc.SetPlayerNameResolver(resolver)

	uc.pushUpdate(context.Background(), 11, []uint64{22}, playerNoTestRecord(),
		teamv1.TeamUpdateReason_TEAM_UPDATE_REASON_MEMBER_JOINED, 0)

	var event teamv1.TeamUpdateEvent
	if err := proto.Unmarshal(pusher.payload, &event); err != nil {
		t.Fatalf("unmarshal TeamUpdateEvent: %v", err)
	}
	if got := event.GetTeam().GetMembers()[1].GetNickname(); got != "Bob" {
		t.Fatalf("pushed member nickname=%q, want Bob", got)
	}
	if len(resolver.calls) != 1 {
		t.Fatalf("push ResolvePlayerNames calls=%d, want one batch", len(resolver.calls))
	}
}
