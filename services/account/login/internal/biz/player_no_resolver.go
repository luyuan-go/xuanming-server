package biz

import (
	"context"

	"github.com/luyuancpp/pandora/pkg/errcode"
)

// PlayerNoBatchReader 是 login 账号权威对角色展示编号的一次批量读取端口。
// player_no 只作为结果值，输入和 map key 永远是 player_id。
type PlayerNoBatchReader interface {
	ResolvePlayerNos(ctx context.Context, playerIDs []uint64) (map[uint64]uint64, error)
}

// PlayerNoResolveUsecase 为内部服务提供有界批量角色编号解析。
type PlayerNoResolveUsecase struct {
	reader PlayerNoBatchReader
}

func NewPlayerNoResolveUsecase(reader PlayerNoBatchReader) *PlayerNoResolveUsecase {
	return &PlayerNoResolveUsecase{reader: reader}
}

func (u *PlayerNoResolveUsecase) ResolvePlayerNos(ctx context.Context, playerIDs []uint64) (map[uint64]uint64, error) {
	if u == nil || u.reader == nil {
		return nil, errcode.New(errcode.ErrUnavailable, "player_no authority unavailable")
	}
	if len(playerIDs) == 0 {
		return nil, errcode.New(errcode.ErrInvalidArg, "player_ids required")
	}
	return u.reader.ResolvePlayerNos(ctx, playerIDs)
}
