package configtable

import (
	"testing"

	"google.golang.org/protobuf/proto"

	configpb "github.com/luyuancpp/pandora/proto/gen/go/pandora/config/v1"
)

func TestValidateEquipmentAffixRowBounds(t *testing.T) {
	valid := &configpb.EquipmentAffixRow{
		Id: 1, PoolId: 1, AttrCount: 2, AttrId: 3,
		Weight: 40, MinValue: 1, MaxValue: 10,
	}
	if err := validateEquipmentAffixRow(valid); err != nil {
		t.Fatalf("valid row rejected: %v", err)
	}
	tests := []struct {
		name string
		edit func(*configpb.EquipmentAffixRow)
	}{
		{"zero pool", func(r *configpb.EquipmentAffixRow) { r.PoolId = 0 }},
		{"zero count", func(r *configpb.EquipmentAffixRow) { r.AttrCount = 0 }},
		{"too many attrs", func(r *configpb.EquipmentAffixRow) { r.AttrCount = 9 }},
		{"zero attr", func(r *configpb.EquipmentAffixRow) { r.AttrId = 0 }},
		{"zero weight", func(r *configpb.EquipmentAffixRow) { r.Weight = 0 }},
		{"reversed range", func(r *configpb.EquipmentAffixRow) { r.MinValue, r.MaxValue = 2, 1 }},
		{"non-positive value", func(r *configpb.EquipmentAffixRow) { r.MinValue = 0 }},
		{"value too large", func(r *configpb.EquipmentAffixRow) { r.MaxValue = 1_000_000_001 }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// 必须 proto.Clone,不能 `row := *valid`:值拷贝会把 MessageState 里的
			// sync.Mutex / sizeCache 一起复制(CLAUDE.md §5.10;go vet copylocks 会报)。
			row := proto.Clone(valid).(*configpb.EquipmentAffixRow)
			tt.edit(row)
			if err := validateEquipmentAffixRow(row); err == nil {
				t.Fatal("invalid row accepted")
			}
		})
	}
}
