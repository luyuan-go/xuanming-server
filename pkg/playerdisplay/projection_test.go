package playerdisplay

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

type concurrencyProbe struct {
	mu        sync.Mutex
	active    int
	maxActive int
	batches   [][]uint64
	started   chan struct{}
	release   <-chan struct{}
}

func (p *concurrencyProbe) enter(ctx context.Context, playerIDs []uint64) error {
	p.mu.Lock()
	p.active++
	if p.active > p.maxActive {
		p.maxActive = p.active
	}
	p.batches = append(p.batches, append([]uint64(nil), playerIDs...))
	p.mu.Unlock()
	select {
	case p.started <- struct{}{}:
	default:
	}
	select {
	case <-ctx.Done():
		p.leave()
		return ctx.Err()
	case <-p.release:
		p.leave()
		return nil
	}
}

func (p *concurrencyProbe) leave() {
	p.mu.Lock()
	p.active--
	p.mu.Unlock()
}

func (p *concurrencyProbe) ResolvePlayerNames(ctx context.Context, playerIDs []uint64) (map[uint64]string, error) {
	if err := p.enter(ctx, playerIDs); err != nil {
		return nil, err
	}
	result := make(map[uint64]string, len(playerIDs))
	for _, playerID := range playerIDs {
		result[playerID] = "name"
	}
	return result, nil
}

func (p *concurrencyProbe) ResolvePlayerNos(ctx context.Context, playerIDs []uint64) (map[uint64]uint64, error) {
	if err := p.enter(ctx, playerIDs); err != nil {
		return nil, err
	}
	result := make(map[uint64]uint64, len(playerIDs))
	for _, playerID := range playerIDs {
		result[playerID] = playerID + 10000
	}
	return result, nil
}

func TestResolveBoundsConcurrencyAcrossIndependentDependencies(t *testing.T) {
	release := make(chan struct{})
	probe := &concurrencyProbe{started: make(chan struct{}, 8), release: release}
	playerIDs := make([]uint64, 65)
	for i := range playerIDs {
		playerIDs[i] = uint64(i + 1)
	}
	type outcome struct {
		projection Projection
		err        error
	}
	done := make(chan outcome, 1)
	go func() {
		projection, err := Resolve(context.Background(), playerIDs, probe, probe)
		done <- outcome{projection: projection, err: err}
	}()

	for i := 0; i < MaxConcurrency; i++ {
		select {
		case <-probe.started:
		case <-time.After(time.Second):
			t.Fatalf("only %d resolver calls started concurrently", i)
		}
	}
	probe.mu.Lock()
	if probe.maxActive != MaxConcurrency {
		t.Fatalf("max concurrent authority calls = %d, want %d", probe.maxActive, MaxConcurrency)
	}
	probe.mu.Unlock()
	close(release)

	result := <-done
	if result.err != nil {
		t.Fatalf("Resolve: %v", result.err)
	}
	if len(result.projection.Names) != 65 || len(result.projection.Numbers) != 65 {
		t.Fatalf("projection sizes = (%d,%d), want (65,65)",
			len(result.projection.Names), len(result.projection.Numbers))
	}
	probe.mu.Lock()
	defer probe.mu.Unlock()
	if len(probe.batches) != 6 {
		t.Fatalf("authority calls = %d, want 6", len(probe.batches))
	}
	for _, batch := range probe.batches {
		if len(batch) == 0 || len(batch) > 32 {
			t.Fatalf("batch size = %d, want 1..32", len(batch))
		}
	}
}

type failingNameResolver struct{ err error }

func (r failingNameResolver) ResolvePlayerNames(context.Context, []uint64) (map[uint64]string, error) {
	return nil, r.err
}

type fixedNoResolver struct{}

func (fixedNoResolver) ResolvePlayerNos(_ context.Context, playerIDs []uint64) (map[uint64]uint64, error) {
	result := make(map[uint64]uint64, len(playerIDs))
	for _, playerID := range playerIDs {
		result[playerID] = playerID + 100
	}
	return result, nil
}

type fixedNameResolver struct{}

func (fixedNameResolver) ResolvePlayerNames(_ context.Context, playerIDs []uint64) (map[uint64]string, error) {
	result := make(map[uint64]string, len(playerIDs))
	for _, playerID := range playerIDs {
		result[playerID] = "player"
	}
	return result, nil
}

type failingNoResolver struct{ err error }

func (r failingNoResolver) ResolvePlayerNos(context.Context, []uint64) (map[uint64]uint64, error) {
	return nil, r.err
}

func TestResolveFailsSoftPerDependency(t *testing.T) {
	wantErr := errors.New("player authority unavailable")
	projection, err := Resolve(context.Background(), []uint64{7, 8}, failingNameResolver{wantErr}, fixedNoResolver{})
	if err != nil {
		t.Fatalf("Resolve hard error: %v", err)
	}
	if len(projection.Names) != 0 || projection.Numbers[7] != 107 || projection.Numbers[8] != 108 {
		t.Fatalf("projection = %+v, want number dependency preserved", projection)
	}
	if len(projection.Failures) != 1 || projection.Failures[0].Dependency != DependencyPlayerName ||
		!errors.Is(projection.Failures[0].Err, wantErr) {
		t.Fatalf("failures = %+v, want one player_name failure", projection.Failures)
	}
}

func TestResolveKeepsNamesWhenPlayerNoDependencyFails(t *testing.T) {
	wantErr := errors.New("login authority unavailable")
	projection, err := Resolve(context.Background(), []uint64{7, 8}, fixedNameResolver{}, failingNoResolver{wantErr})
	if err != nil {
		t.Fatalf("Resolve hard error: %v", err)
	}
	if projection.Names[7] != "player" || projection.Names[8] != "player" || len(projection.Numbers) != 0 {
		t.Fatalf("projection = %+v, want name dependency preserved", projection)
	}
	if len(projection.Failures) != 1 || projection.Failures[0].Dependency != DependencyPlayerNo ||
		!errors.Is(projection.Failures[0].Err, wantErr) {
		t.Fatalf("failures = %+v, want one player_no failure", projection.Failures)
	}
}

func TestResolvePropagatesParentCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	probe := &concurrencyProbe{started: make(chan struct{}, 2), release: make(chan struct{})}
	done := make(chan error, 1)
	go func() {
		_, err := Resolve(ctx, []uint64{1}, probe, probe)
		done <- err
	}()
	select {
	case <-probe.started:
	case <-time.After(time.Second):
		t.Fatal("resolver call did not start")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Resolve error = %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Resolve did not propagate cancellation")
	}
}
