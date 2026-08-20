package biz

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/luyuancpp/pandora/services/matchmaking/team/internal/conf"
	"github.com/luyuancpp/pandora/services/matchmaking/team/internal/data"
)

func TestTeamApplicationsToProtoResolvesDisplayFieldsInOneBatch(t *testing.T) {
	nameResolver := &playerNameResolverFake{values: map[uint64]string{11: "Alice"}}
	noResolver := &playerNoResolverFake{values: map[uint64]uint64{11: 100011, 22: 100022}}
	uc := NewTeamUsecase(nil, nil, conf.TeamConf{})
	uc.SetPlayerNameResolver(nameResolver)
	uc.SetPlayerNoResolver(noResolver)

	records := []*data.ApplicationRecord{
		{PlayerID: 11, ExpiresAtMs: 101},
		{PlayerID: 22, ExpiresAtMs: 202},
		{PlayerID: 11, ExpiresAtMs: 303},
	}
	got, err := uc.TeamApplicationsToProto(context.Background(), 7001, records)
	if err != nil {
		t.Fatalf("TeamApplicationsToProto: %v", err)
	}

	wantBatch := []uint64{11, 22}
	if len(nameResolver.calls) != 1 || !reflect.DeepEqual(nameResolver.calls[0], wantBatch) {
		t.Fatalf("ResolvePlayerNames batches=%v, want one %v", nameResolver.calls, wantBatch)
	}
	if len(noResolver.calls) != 1 || !reflect.DeepEqual(noResolver.calls[0], wantBatch) {
		t.Fatalf("ResolvePlayerNos batches=%v, want one %v", noResolver.calls, wantBatch)
	}
	if len(got) != 3 {
		t.Fatalf("applications len=%d, want 3", len(got))
	}
	if got[0].GetPlayerId() != 11 || got[0].GetNickname() != "Alice" || got[0].GetPlayerNo() != 100011 || got[0].GetExpiresAtMs() != 101 {
		t.Fatalf("application[0]=%+v", got[0])
	}
	if got[1].GetPlayerId() != 22 || got[1].GetNickname() != "" || got[1].GetPlayerNo() != 100022 || got[1].GetExpiresAtMs() != 202 {
		t.Fatalf("application[1]=%+v", got[1])
	}
	if got[2].GetPlayerId() != 11 || got[2].GetNickname() != "Alice" || got[2].GetPlayerNo() != 100011 || got[2].GetExpiresAtMs() != 303 {
		t.Fatalf("application order/identity changed: application[2]=%+v", got[2])
	}
}

func TestTeamApplicationsToProtoDisplayFailuresAreIndependentAndFailSoft(t *testing.T) {
	records := []*data.ApplicationRecord{{PlayerID: 11, ExpiresAtMs: 101}}

	t.Run("name failure keeps player number", func(t *testing.T) {
		uc := NewTeamUsecase(nil, nil, conf.TeamConf{})
		uc.SetPlayerNameResolver(&playerNameResolverFake{err: errors.New("player unavailable")})
		uc.SetPlayerNoResolver(&playerNoResolverFake{values: map[uint64]uint64{11: 100011}})

		got, err := uc.TeamApplicationsToProto(context.Background(), 7001, records)
		if err != nil {
			t.Fatalf("TeamApplicationsToProto: %v", err)
		}
		if len(got) != 1 || got[0].GetNickname() != "" || got[0].GetPlayerNo() != 100011 || got[0].GetPlayerId() != 11 {
			t.Fatalf("projection=%+v", got)
		}
	})

	t.Run("number failure keeps player name", func(t *testing.T) {
		uc := NewTeamUsecase(nil, nil, conf.TeamConf{})
		uc.SetPlayerNameResolver(&playerNameResolverFake{values: map[uint64]string{11: "Alice"}})
		uc.SetPlayerNoResolver(&playerNoResolverFake{err: errors.New("login unavailable")})

		got, err := uc.TeamApplicationsToProto(context.Background(), 7001, records)
		if err != nil {
			t.Fatalf("TeamApplicationsToProto: %v", err)
		}
		if len(got) != 1 || got[0].GetNickname() != "Alice" || got[0].GetPlayerNo() != 0 || got[0].GetPlayerId() != 11 {
			t.Fatalf("projection=%+v", got)
		}
	})
}

func TestTeamApplicationsToProtoResolvesNameAndNumberConcurrently(t *testing.T) {
	started := make(chan string, 2)
	release := make(chan struct{})
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

	done := make(chan struct{})
	go func() {
		_, _ = uc.TeamApplicationsToProto(context.Background(), 7001, []*data.ApplicationRecord{{PlayerID: 11}})
		close(done)
	}()
	seen := map[string]bool{}
	for len(seen) < 2 {
		select {
		case dependency := <-started:
			seen[dependency] = true
		case <-time.After(time.Second):
			close(release)
			t.Fatalf("display resolvers did not start concurrently; started=%v", seen)
		}
	}
	close(release)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("TeamApplicationsToProto did not complete after dependencies released")
	}
}

func TestTeamApplicationsToProtoEmptyListSkipsResolvers(t *testing.T) {
	nameResolver := &playerNameResolverFake{}
	noResolver := &playerNoResolverFake{}
	uc := NewTeamUsecase(nil, nil, conf.TeamConf{})
	uc.SetPlayerNameResolver(nameResolver)
	uc.SetPlayerNoResolver(noResolver)

	got, err := uc.TeamApplicationsToProto(context.Background(), 7001, nil)
	if err != nil {
		t.Fatalf("TeamApplicationsToProto: %v", err)
	}
	if len(got) != 0 || len(nameResolver.calls) != 0 || len(noResolver.calls) != 0 {
		t.Fatalf("empty projection=%v name_calls=%v no_calls=%v", got, nameResolver.calls, noResolver.calls)
	}
}

func TestTeamApplicationsToProtoPropagatesParentCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	uc := NewTeamUsecase(nil, nil, conf.TeamConf{})
	uc.SetPlayerNameResolver(playerNameResolverFunc(func(callCtx context.Context, _ []uint64) (map[uint64]string, error) {
		cancel()
		<-callCtx.Done()
		return nil, callCtx.Err()
	}))

	applications, err := uc.TeamApplicationsToProto(ctx, 7001, []*data.ApplicationRecord{{PlayerID: 11}})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("TeamApplicationsToProto error=%v, want context.Canceled", err)
	}
	if applications != nil {
		t.Fatalf("applications=%v, want nil on cancellation", applications)
	}
}
