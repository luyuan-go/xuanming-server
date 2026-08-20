package main

// migration_set_coverage_test.go — 「后来新增的库没有迁移集」的机械门禁。
//
// 存在的理由(2026-08-18,INC-20260818-003 的次生故障):owner 新版把
// `owner_record.hub_source_revision` 写进 SELECT,启动期 fail-fast 拒启;而 pandora_owner
// **根本没有迁移集**,expand DDL 只以注释形式写在 deploy/mysql-init/15-owner-tables.sql 里。
// dev_migrate.ps1 第 0 步重放 mysql-init 全是 `CREATE TABLE IF NOT EXISTS`,对**加列**是
// 空操作;第 1 步只跑 migrations/<库名>/。两步都不覆盖 = 存量库永远拿不到新列,
// 一键启动直接 `[FAIL] owner 启动后立即退出 (exit 1)`,生产上则是新镜像整个不可用。
//
// 这类漏洞的共性是「建库脚本里有、迁移集里没有」,而它只在**下一次加列**时才爆,
// 且爆的时候现象在别的服务身上。本文件把方向单向钉死:
//
//	deploy/mysql-init 里出现的每个库,都必须有 migrations/<库名>/ 迁移集。
//
// 反过来不成立 —— 迁移集可以服务多个物理分片库(pandora_auction 的 auction_00/01)。
//
// 与 cmd/dbcheck/registry_test.go 同一思路:用仓库里的建库/建表事实反向核对登记,不连库。

import (
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/luyuancpp/pandora/tools/migrate/workspacedb"
)

const freshInitSQLDir = "../../deploy/mysql-init"

var (
	freshInitUseDatabase    = regexp.MustCompile("(?im)^\\s*USE\\s+`?([a-z0-9_]+)`?\\s*;")
	freshInitCreateDatabase = regexp.MustCompile("(?i)CREATE\\s+DATABASE\\s+(?:IF\\s+NOT\\s+EXISTS\\s+)?`?([a-z0-9_]+)`?")
	freshInitCreateTable    = regexp.MustCompile("(?i)CREATE\\s+TABLE\\s+(?:IF\\s+NOT\\s+EXISTS\\s+)?`([a-z0-9_]+)`")
)

// migrationSetExemptDatabases 是**故意**不建迁移集的库。每一条都必须写清为什么,
// 并由 TestMigrationSetExemptionsStayEmpty 反向校验豁免理由仍然成立 ——
// 否则这张表会退化成一张没人维护的永久后门(同 expand_only_contract_test.go 的 allowlist 纪律)。
var migrationSetExemptDatabases = map[string]string{
	"pandora_ops": "运营日志 / 封禁 / 客诉的预留库,建库脚本只 CREATE DATABASE + GRANT,当前零表," +
		"没有任何 schema 可迁移。落第一张表时必须同时建 migrations/pandora_ops/000001_baseline。",
}

// TestEveryFreshInitDatabaseHasMigrationSet 是主门禁。
func TestEveryFreshInitDatabaseHasMigrationSet(t *testing.T) {
	databases := collectFreshInitDatabases(t)
	if len(databases) < 8 {
		t.Fatalf("只从 %s 解析出 %d 个库,解析八成坏了", freshInitSQLDir, len(databases))
	}

	sets := collectMigrationSets(t)
	missing := make([]string, 0)
	for _, database := range databases {
		if _, ok := sets[database]; ok {
			continue
		}
		if _, exempt := migrationSetExemptDatabases[database]; exempt {
			continue
		}
		missing = append(missing, database)
	}
	if len(missing) == 0 {
		return
	}
	sort.Strings(missing)
	t.Fatalf("这些库在 deploy/mysql-init 里建了,却没有 tools/migrate/migrations/<库名>/ 迁移集:\n  %s\n"+
		"后果:mysql-init 只在数据卷首次创建时执行,且全是 CREATE TABLE IF NOT EXISTS —— 对**加列**\n"+
		"是空操作;没有迁移集就等于该库的列改动在任何存量库上永远不会自动进库,新镜像上线即崩\n"+
		"(2026-08-18 owner 的 hub_source_revision 就是这么炸的)。\n"+
		"修法:建 migrations/<库名>/000001_baseline.{up,down}.sql(从 mysql-init 那份生成,全 IF NOT EXISTS),\n"+
		"以后的结构变更一律新增更高版本。确实无 schema 的库登记进 migrationSetExemptDatabases 并写明理由。",
		strings.Join(missing, "\n  "))
}

// TestCanonicalWorkspaceSetsMatchEmbeddedMigrations 防止中心 provisioner 的十库清单与
// pandora-migrate 真正编进二进制的 migration set 漂移。
func TestCanonicalWorkspaceSetsMatchEmbeddedMigrations(t *testing.T) {
	embedded := collectMigrationSets(t)
	canonical := workspacedb.CanonicalMigrationSets()
	if len(canonical) != len(embedded) {
		t.Fatalf("canonical/embedded migration sets=%v/%v", canonical, embedded)
	}
	seen := make(map[string]bool, len(canonical))
	for _, migrationSet := range canonical {
		if seen[migrationSet] {
			t.Fatalf("canonical migration set 重复: %s", migrationSet)
		}
		seen[migrationSet] = true
		if !embedded[migrationSet] {
			t.Fatalf("canonical migration set %s 不在内嵌 migrations 目录", migrationSet)
		}
	}
}

// TestMigrationSetExemptionsStayEmpty 反向门禁:被豁免的库必须**确实**还没有表。
// 有人给 pandora_ops 加了第一张表却没建迁移集时,这里立刻红。
func TestMigrationSetExemptionsStayEmpty(t *testing.T) {
	tablesByDatabase := collectFreshInitTablesByDatabase(t)
	for database, reason := range migrationSetExemptDatabases {
		if reason == "" {
			t.Errorf("%s 的豁免没写理由", database)
		}
		if tables := tablesByDatabase[database]; len(tables) > 0 {
			sort.Strings(tables)
			t.Errorf("%s 已经有表 %v,豁免理由(零表)不再成立:必须建 migrations/%s/000001_baseline",
				database, tables, database)
		}
	}
}

// TestEveryMigrationSetHasBaseline:每个迁移集的第一版都必须是 000001_baseline,且
// up/down 成对。golang-migrate 缺 down 文件时 Down/Steps(-1) 直接失败,dev 回滚会卡住。
func TestEveryMigrationSetHasBaseline(t *testing.T) {
	for set := range collectMigrationSets(t) {
		entries, err := fs.ReadDir(migrationsFS, "migrations/"+set)
		if err != nil {
			t.Fatalf("读取迁移集 %s: %v", set, err)
		}
		ups := make(map[string]bool)
		downs := make(map[string]bool)
		for _, entry := range entries {
			name := entry.Name()
			switch {
			case strings.HasSuffix(name, ".up.sql"):
				ups[strings.TrimSuffix(name, ".up.sql")] = true
			case strings.HasSuffix(name, ".down.sql"):
				downs[strings.TrimSuffix(name, ".down.sql")] = true
			default:
				t.Errorf("迁移集 %s 里有非 .up.sql/.down.sql 文件: %s", set, name)
			}
		}
		if !ups["000001_baseline"] {
			t.Errorf("迁移集 %s 缺 000001_baseline.up.sql", set)
		}
		for name := range ups {
			if !downs[name] {
				t.Errorf("迁移集 %s 的 %s 缺配套 down 文件", set, name)
			}
		}
		for name := range downs {
			if !ups[name] {
				t.Errorf("迁移集 %s 的 %s 有 down 却没有 up", set, name)
			}
		}
	}
}

// collectMigrationSets 返回嵌入 FS 里的全部迁移集名(= 目标库名)。
func collectMigrationSets(t *testing.T) map[string]bool {
	t.Helper()
	entries, err := fs.ReadDir(migrationsFS, "migrations")
	if err != nil {
		t.Fatalf("读取 migrations 目录: %v", err)
	}
	sets := make(map[string]bool, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			sets[entry.Name()] = true
		}
	}
	if len(sets) == 0 {
		t.Fatal("嵌入的 migrations 目录为空,embed 八成坏了")
	}
	return sets
}

// collectFreshInitDatabases 返回 deploy/mysql-init 声明过的全部库(CREATE DATABASE 与 USE 并集)。
func collectFreshInitDatabases(t *testing.T) []string {
	t.Helper()
	seen := make(map[string]bool)
	forEachFreshInitFile(t, func(_, content string) {
		for _, pattern := range []*regexp.Regexp{freshInitCreateDatabase, freshInitUseDatabase} {
			for _, match := range pattern.FindAllStringSubmatch(content, -1) {
				seen[match[1]] = true
			}
		}
	})
	databases := make([]string, 0, len(seen))
	for database := range seen {
		databases = append(databases, database)
	}
	sort.Strings(databases)
	return databases
}

// collectFreshInitTablesByDatabase 按 `USE` 归属把建表语句分到各库名下。
func collectFreshInitTablesByDatabase(t *testing.T) map[string][]string {
	t.Helper()
	tables := make(map[string][]string)
	forEachFreshInitFile(t, func(_, content string) {
		current := ""
		for _, line := range strings.Split(content, "\n") {
			if match := freshInitUseDatabase.FindStringSubmatch(line); match != nil {
				current = match[1]
				continue
			}
			if match := freshInitCreateTable.FindStringSubmatch(line); match != nil && current != "" {
				tables[current] = append(tables[current], match[1])
			}
		}
	})
	return tables
}

func forEachFreshInitFile(t *testing.T, visit func(name, content string)) {
	t.Helper()
	entries, err := os.ReadDir(freshInitSQLDir)
	if err != nil {
		t.Fatalf("读取 %s: %v", freshInitSQLDir, err)
	}
	scanned := 0
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".sql") {
			continue
		}
		raw, err := os.ReadFile(filepath.Join(freshInitSQLDir, entry.Name()))
		if err != nil {
			t.Fatalf("读取 %s: %v", entry.Name(), err)
		}
		scanned++
		visit(path.Base(entry.Name()), stripLineComments(string(raw)))
	}
	if scanned == 0 {
		t.Fatalf("%s 下没有 .sql 文件", freshInitSQLDir)
	}
}
