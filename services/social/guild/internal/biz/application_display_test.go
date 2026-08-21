package biz

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/luyuancpp/pandora/services/social/guild/internal/data"
)

type guildFakePlayerNameResolver struct {
	mu      sync.Mutex
	names   map[uint64]string
	batches [][]uint64
}

func (r *guildFakePlayerNameResolver) ResolvePlayerNames(_ context.Context, playerIDs []uint64) (map[uint64]string, error) {
	r.mu.Lock()
	r.batches = append(r.batches, append([]uint64(nil), playerIDs...))
	r.mu.Unlock()
	if len(playerIDs) > 32 {
		return nil, fmt.Errorf("oversized name batch: %d", len(playerIDs))
	}
	result := make(map[uint64]string, len(playerIDs))
	for _, playerID := range playerIDs {
		if nickname, ok := r.names[playerID]; ok {
			result[playerID] = nickname
		}
	}
	return result, nil
}

type guildFakePlayerNoResolver struct {
	mu      sync.Mutex
	numbers map[uint64]uint64
	batches [][]uint64
}

func (r *guildFakePlayerNoResolver) ResolvePlayerNos(_ context.Context, playerIDs []uint64) (map[uint64]uint64, error) {
	r.mu.Lock()
	r.batches = append(r.batches, append([]uint64(nil), playerIDs...))
	r.mu.Unlock()
	if len(playerIDs) > 32 {
		return nil, fmt.Errorf("oversized player_no batch: %d", len(playerIDs))
	}
	result := make(map[uint64]uint64, len(playerIDs))
	for _, playerID := range playerIDs {
		if playerNo, ok := r.numbers[playerID]; ok {
			result[playerID] = playerNo
		}
	}
	return result, nil
}

func TestListJoinRequestsProjectsAuthoritativeDisplayFields(t *testing.T) {
	repo := newBoundaryGuildRepo()
	repo.members[7] = &data.GuildMemberRow{
		PlayerID: 7,
		GuildID:  100,
		Role:     data.GuildRoleOfficer,
	}
	repo.pendingRows = []data.GuildJoinRequestRow{
		{RequestID: 901, GuildID: 100, PlayerID: 11},
		{RequestID: 902, GuildID: 100, PlayerID: 22},
	}

	uc := newBoundaryGuildUsecase(repo)
	uc.SetPlayerNameResolver(&guildFakePlayerNameResolver{names: map[uint64]string{11: "Alice"}})
	uc.SetPlayerNoResolver(&guildFakePlayerNoResolver{numbers: map[uint64]uint64{11: 10011, 22: 10022}})

	requests, _, err := uc.ListJoinRequests(context.Background(), 7, 0, 10)
	if err != nil {
		t.Fatalf("ListJoinRequests: %v", err)
	}
	byPlayerID := make(map[uint64]struct {
		nickname string
		playerNo uint64
	}, len(requests))
	for _, request := range requests {
		byPlayerID[request.GetFromPlayerId()] = struct {
			nickname string
			playerNo uint64
		}{request.GetFromNickname(), request.GetFromPlayerNo()}
	}
	if got := byPlayerID[11]; got.nickname != "Alice" || got.playerNo != 10011 {
		t.Fatalf("player 11 projection = %+v, want nickname Alice / player_no 10011", got)
	}
	if got := byPlayerID[22]; got.nickname != "" || got.playerNo != 10022 {
		t.Fatalf("player 22 projection = %+v, want empty nickname / player_no 10022", got)
	}
}

func TestListJoinRequestsDeduplicatesAndChunksDisplayLookups(t *testing.T) {
	repo := newBoundaryGuildRepo()
	repo.members[7] = &data.GuildMemberRow{
		PlayerID: 7,
		GuildID:  100,
		Role:     data.GuildRoleOfficer,
	}
	for playerID := uint64(1); playerID <= 34; playerID++ {
		repo.pendingRows = append(repo.pendingRows, data.GuildJoinRequestRow{
			RequestID: 1000 + playerID,
			GuildID:   100,
			PlayerID:  playerID,
		})
	}
	// Defensive duplicate: one applicant must still be looked up only once.
	repo.pendingRows = append(repo.pendingRows, data.GuildJoinRequestRow{
		RequestID: 2000,
		GuildID:   100,
		PlayerID:  1,
	})

	names := make(map[uint64]string, 34)
	numbers := make(map[uint64]uint64, 34)
	for playerID := uint64(1); playerID <= 34; playerID++ {
		names[playerID] = fmt.Sprintf("player-%d", playerID)
		numbers[playerID] = 10000 + playerID
	}
	nameResolver := &guildFakePlayerNameResolver{names: names}
	noResolver := &guildFakePlayerNoResolver{numbers: numbers}
	uc := newBoundaryGuildUsecase(repo)
	uc.SetPlayerNameResolver(nameResolver)
	uc.SetPlayerNoResolver(noResolver)

	requests, _, err := uc.ListJoinRequests(context.Background(), 7, 0, 100)
	if err != nil {
		t.Fatalf("ListJoinRequests: %v", err)
	}
	for _, request := range requests {
		playerID := request.GetFromPlayerId()
		if request.GetFromNickname() != names[playerID] || request.GetFromPlayerNo() != numbers[playerID] {
			t.Fatalf("player %d projection = (%q,%d), want (%q,%d)", playerID,
				request.GetFromNickname(), request.GetFromPlayerNo(), names[playerID], numbers[playerID])
		}
	}
	assertGuildResolverBatches(t, "name", nameResolver.batches, 34)
	assertGuildResolverBatches(t, "player_no", noResolver.batches, 34)
}

func assertGuildResolverBatches(t *testing.T, label string, batches [][]uint64, wantUnique int) {
	t.Helper()
	seen := make(map[uint64]int, wantUnique)
	for _, batch := range batches {
		if len(batch) == 0 || len(batch) > 32 {
			t.Fatalf("%s batch size = %d, want 1..32", label, len(batch))
		}
		for _, playerID := range batch {
			seen[playerID]++
		}
	}
	if len(seen) != wantUnique {
		t.Fatalf("%s unique ids = %d, want %d (batches=%v)", label, len(seen), wantUnique, batches)
	}
	for playerID, count := range seen {
		if count != 1 {
			t.Fatalf("%s player %d resolved %d times, want once", label, playerID, count)
		}
	}
}
