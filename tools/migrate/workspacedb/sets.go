package workspacedb

// canonicalMigrationSets 是中心 workspace 必须具有的完整 migration set 集合。
// 顺序稳定，供 provisioner 生成确定性的目标清单；调用方只能取得副本，不能修改权威集合。
var canonicalMigrationSets = [...]string{
	"pandora_account",
	"pandora_auction",
	"pandora_bag",
	"pandora_battle",
	"pandora_leaderboard",
	"pandora_mission",
	"pandora_owner",
	"pandora_player",
	"pandora_social",
	"pandora_trade",
}

// CanonicalMigrationSets 返回中心 workspace 的完整 migration set 集合副本。
func CanonicalMigrationSets() []string {
	return append([]string(nil), canonicalMigrationSets[:]...)
}
