package biz

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/luyuancpp/pandora/services/social/friend/internal/data"
)

type fakePlayerNameResolver struct {
	mu      sync.Mutex
	names   map[uint64]string
	batches [][]uint64
	err     error
}

func (r *fakePlayerNameResolver) ResolvePlayerNames(_ context.Context, playerIDs []uint64) (map[uint64]string, error) {
	r.mu.Lock()
	r.batches = append(r.batches, append([]uint64(nil), playerIDs...))
	r.mu.Unlock()
	if len(playerIDs) > 32 {
		return nil, fmt.Errorf("oversized name batch: %d", len(playerIDs))
	}
	if r.err != nil {
		return nil, r.err
	}
	result := make(map[uint64]string, len(playerIDs))
	for _, playerID := range playerIDs {
		if nickname, ok := r.names[playerID]; ok {
			result[playerID] = nickname
		}
	}
	return result, nil
}

type fakePlayerNoResolver struct {
	mu      sync.Mutex
	numbers map[uint64]uint64
	batches [][]uint64
	err     error
}

func (r *fakePlayerNoResolver) ResolvePlayerNos(_ context.Context, playerIDs []uint64) (map[uint64]uint64, error) {
	r.mu.Lock()
	r.batches = append(r.batches, append([]uint64(nil), playerIDs...))
	r.mu.Unlock()
	if len(playerIDs) > 32 {
		return nil, fmt.Errorf("oversized player_no batch: %d", len(playerIDs))
	}
	if r.err != nil {
		return nil, r.err
	}
	result := make(map[uint64]uint64, len(playerIDs))
	for _, playerID := range playerIDs {
		if playerNo, ok := r.numbers[playerID]; ok {
			result[playerID] = playerNo
		}
	}
	return result, nil
}

func TestListFriendRequestsDeduplicatesAndChunksDisplayLookups(t *testing.T) {
	repo := newFakeRepo()
	for i := uint64(1); i <= 34; i++ {
		repo.requests[1000+i] = &data.FriendRequestRow{
			RequestID:   1000 + i,
			RequesterID: i,
			TargetID:    99,
			Status:      requestStatusPending,
		}
	}
	// 防御性覆盖重复 requester：投影输入必须稳定去重，不能放大权威服务请求。
	repo.requests[2000] = &data.FriendRequestRow{
		RequestID:   2000,
		RequesterID: 1,
		TargetID:    99,
		Status:      requestStatusPending,
	}

	names := make(map[uint64]string, 34)
	numbers := make(map[uint64]uint64, 34)
	for i := uint64(1); i <= 34; i++ {
		names[i] = fmt.Sprintf("player-%d", i)
		numbers[i] = 10000 + i
	}
	nameResolver := &fakePlayerNameResolver{names: names}
	noResolver := &fakePlayerNoResolver{numbers: numbers}
	uc := newUC(repo, nil, nil)
	uc.SetPlayerNameResolver(nameResolver)
	uc.SetPlayerNoResolver(noResolver)

	requests, err := uc.ListFriendRequests(context.Background(), 99)
	if err != nil {
		t.Fatalf("ListFriendRequests: %v", err)
	}
	for _, request := range requests {
		playerID := request.GetFromPlayerId()
		if request.GetFromNickname() != names[playerID] || request.GetFromPlayerNo() != numbers[playerID] {
			t.Fatalf("player %d projection = (%q,%d), want (%q,%d)", playerID,
				request.GetFromNickname(), request.GetFromPlayerNo(), names[playerID], numbers[playerID])
		}
	}
	assertResolverBatches(t, "name", nameResolver.batches, 34)
	assertResolverBatches(t, "player_no", noResolver.batches, 34)
}

func assertResolverBatches(t *testing.T, label string, batches [][]uint64, wantUnique int) {
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

func TestListFriendRequestsProjectsAuthoritativeDisplayFields(t *testing.T) {
	repo := newFakeRepo()
	repo.requests[901] = &data.FriendRequestRow{
		RequestID:   901,
		RequesterID: 11,
		TargetID:    99,
		Status:      requestStatusPending,
	}
	repo.requests[902] = &data.FriendRequestRow{
		RequestID:   902,
		RequesterID: 22,
		TargetID:    99,
		Status:      requestStatusPending,
	}

	uc := newUC(repo, nil, nil)
	uc.SetPlayerNameResolver(&fakePlayerNameResolver{names: map[uint64]string{11: "Alice"}})
	uc.SetPlayerNoResolver(&fakePlayerNoResolver{numbers: map[uint64]uint64{11: 10011, 22: 10022}})

	requests, err := uc.ListFriendRequests(context.Background(), 99)
	if err != nil {
		t.Fatalf("ListFriendRequests: %v", err)
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
