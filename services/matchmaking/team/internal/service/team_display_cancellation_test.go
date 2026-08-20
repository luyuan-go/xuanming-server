package service

import (
	"context"
	"errors"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	plog "github.com/luyuancpp/pandora/pkg/log"
	teamv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/team/v1"
	"github.com/luyuancpp/pandora/services/matchmaking/team/internal/biz"
	"github.com/luyuancpp/pandora/services/matchmaking/team/internal/conf"
	"github.com/luyuancpp/pandora/services/matchmaking/team/internal/data"
)

type cancelingPlayerNameResolver struct {
	cancel context.CancelFunc
}

func (r cancelingPlayerNameResolver) ResolvePlayerNames(
	ctx context.Context,
	_ []uint64,
) (map[uint64]string, error) {
	r.cancel()
	<-ctx.Done()
	return nil, ctx.Err()
}

func TestListOpenTeamsPropagatesDisplayProjectionCancellationAsTransportError(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis: %v", err)
	}
	defer mr.Close()
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer func() { _ = rdb.Close() }()

	var cfg conf.Config
	cfg.Defaults()
	uc := biz.NewTeamUsecase(data.NewRedisTeamRepo(rdb), nil, cfg.Team)
	if _, err := uc.CreateTeam(context.Background(), 9001, 7001); err != nil {
		t.Fatalf("CreateTeam: %v", err)
	}

	requestCtx, cancel := context.WithCancel(context.Background())
	uc.SetPlayerNameResolver(cancelingPlayerNameResolver{cancel: cancel})
	svc := NewTeamService(uc, nil, nil)
	requestCtx = context.WithValue(requestCtx, plog.CtxKeyPlayerID, uint64(8001))

	resp, err := svc.ListOpenTeams(requestCtx, &teamv1.ListOpenTeamsRequest{Limit: 10})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("ListOpenTeams error=%v, want context.Canceled", err)
	}
	if resp != nil {
		t.Fatalf("response=%v, want nil on transport cancellation", resp)
	}
}
