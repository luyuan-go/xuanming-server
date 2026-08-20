package main

import (
	"context"
	"reflect"
	"testing"
)

type mainPlayerNoBatchReaderFake struct {
	calls [][]uint64
}

func (f *mainPlayerNoBatchReaderFake) ResolvePlayerNos(_ context.Context, playerIDs []uint64) (map[uint64]uint64, error) {
	f.calls = append(f.calls, append([]uint64(nil), playerIDs...))
	return map[uint64]uint64{11: 900011}, nil
}

func TestNewPlayerNoResolveUsecaseDoesNotDependOnTeamAuthConfig(t *testing.T) {
	reader := &mainPlayerNoBatchReaderFake{}
	uc, err := newPlayerNoResolveUsecase(reader)
	if err != nil {
		t.Fatal(err)
	}
	got, err := uc.ResolvePlayerNos(context.Background(), []uint64{11})
	if err != nil || got[11] != 900011 {
		t.Fatalf("resolved=%v err=%v", got, err)
	}
	if !reflect.DeepEqual(reader.calls, [][]uint64{{11}}) {
		t.Fatalf("calls=%v", reader.calls)
	}
}

func TestNewPlayerNoResolveUsecaseRejectsRepositoryWithoutBatchAuthority(t *testing.T) {
	if _, err := newPlayerNoResolveUsecase(struct{}{}); err == nil {
		t.Fatal("repository without PlayerNoBatchReader must fail fast")
	}
}
