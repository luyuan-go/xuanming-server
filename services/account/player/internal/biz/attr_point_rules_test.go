package biz

import (
	"context"
	"testing"

	"github.com/luyuancpp/pandora/pkg/errcode"
	"github.com/luyuancpp/pandora/services/account/player/internal/data"
)

// fakeAttrPointRules 直接给一份属性点键集合,免去为一张配置表拉起整个 Store。
type fakeAttrPointRules struct {
	keys map[string]struct{}
}

func (f fakeAttrPointRules) AllocatableKeys() map[string]struct{} { return f.keys }

// TestAllocateAttributePoints_RejectsUnknownKey 钉住写入边界:
// 属性加点效果表里没有的键在 DS 上换不出任何战斗数值,加了点等于把点数永久沉没,
// 而且从协议上看完全成功 —— 必须在写入时就拒,不能等玩家发现"加了没反应"。
func TestAllocateAttributePoints_RejectsUnknownKey(t *testing.T) {
	repo := newFakeRepo()
	uc := newUC(repo)
	uc.attrPointRules = fakeAttrPointRules{keys: map[string]struct{}{"str": {}, "con": {}}}
	if _, err := uc.GrantAttributePoints(context.Background(), 100, 10, "g1"); err != nil {
		t.Fatalf("grant err: %v", err)
	}

	// "agi" 不在表里(表用的是 con/dex/men 这套键)。
	_, err := uc.AllocateAttributePoints(context.Background(), 100, []data.AttrAllocation{
		{Key: "str", Points: 1},
		{Key: "agi", Points: 1},
	})
	if errcode.As(err) != errcode.ErrInvalidArg {
		t.Fatalf("未知属性点键应被拒为 ErrInvalidArg, got %v", err)
	}

	// 零写入:同一请求里合法的那条也不能落地(整条请求原子拒)。
	attrs, unspent, gerr := uc.GetAttributes(context.Background(), 100)
	if gerr != nil {
		t.Fatalf("get attrs err: %v", gerr)
	}
	if unspent != 10 || len(attrs) != 0 {
		t.Fatalf("被拒的请求必须零写入, unspent=%d attrs=%d", unspent, len(attrs))
	}

	// 表里有的键照常放行。
	if _, err := uc.AllocateAttributePoints(context.Background(), 100,
		[]data.AttrAllocation{{Key: "con", Points: 2}}); err != nil {
		t.Fatalf("表内属性点键应放行: %v", err)
	}
}

// TestAllocateAttributePoints_TableAbsentStaysLenient 钉住过渡态:
// 属性加点效果表尚未发布(整批缺表)时不收紧写入,否则老客户端连点都加不了。
// 这与 talent_effect 缺表不拒是同一处置。
func TestAllocateAttributePoints_TableAbsentStaysLenient(t *testing.T) {
	repo := newFakeRepo()
	uc := newUC(repo)
	if _, err := uc.GrantAttributePoints(context.Background(), 100, 10, "g1"); err != nil {
		t.Fatalf("grant err: %v", err)
	}

	// attrPointRules 未接线(nil)。
	if _, err := uc.AllocateAttributePoints(context.Background(), 100,
		[]data.AttrAllocation{{Key: "agi", Points: 1}}); err != nil {
		t.Fatalf("表未接线时应保持宽松口径: %v", err)
	}

	// 接线了但表是空的(未发布),同样不收紧。
	uc.attrPointRules = fakeAttrPointRules{keys: nil}
	if _, err := uc.AllocateAttributePoints(context.Background(), 100,
		[]data.AttrAllocation{{Key: "vit", Points: 1}}); err != nil {
		t.Fatalf("表为空时应保持宽松口径: %v", err)
	}
}
