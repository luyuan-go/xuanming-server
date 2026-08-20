// Package workspacedb 定义中心开发 MySQL 中 workspace 物理库名的唯一格式。
//
// 本包只消费中心 provisioner 已分配的稳定 workspace ID；不读取用户名、MachineGuid、SID，
// 也不创建数据库或接触管理员凭据。
package workspacedb

import (
	"fmt"
	"regexp"
	"strings"
)

const (
	workspaceIDLength          = 26
	maximumMySQLIdentifierSize = 64
	workspaceDatabaseSeparator = "_w_"
)

var (
	// workspace ID 是中心 provisioner 分配的 128-bit opaque ID，使用 26 位小写
	// Crockford Base32。i/l/o/u 被排除，避免人工抄写时产生歧义。
	workspaceIDPattern  = regexp.MustCompile(`^[0-7][0-9a-hjkmnp-tv-z]{25}$`)
	migrationSetPattern = regexp.MustCompile(`^[a-z][a-z0-9_]{0,63}$`)
)

// ValidateWorkspaceID 验证中心 provisioner 分配的稳定 workspace ID。
func ValidateWorkspaceID(workspaceID string) error {
	if len(workspaceID) != workspaceIDLength || !workspaceIDPattern.MatchString(workspaceID) {
		return fmt.Errorf("workspace_id=%q 非法，必须是 128-bit canonical 的 26 位小写 Crockford Base32（首位 0..7）", workspaceID)
	}
	return nil
}

// PhysicalDatabaseName 把 canonical migration set 映射为 workspace 专属物理库名。
// 调用方仍须从迁移器内嵌清单确认 migrationSet 确实存在；本包只负责格式与 MySQL 长度约束。
func PhysicalDatabaseName(migrationSet, workspaceID string) (string, error) {
	if !migrationSetPattern.MatchString(migrationSet) {
		return "", fmt.Errorf("migration_set=%q 非法", migrationSet)
	}
	if err := ValidateWorkspaceID(workspaceID); err != nil {
		return "", err
	}
	database := migrationSet + workspaceDatabaseSeparator + workspaceID
	if len(database) > maximumMySQLIdentifierSize {
		return "", fmt.Errorf("workspace 物理库名长度=%d 超过 MySQL 上限 %d", len(database), maximumMySQLIdentifierSize)
	}
	return database, nil
}

// ParsePhysicalDatabaseName 仅解析 PhysicalDatabaseName 生成的精确 workspace 库名。
// 普通 canonical 库、历史分片名和形似但不合法的 _w_ 名都会返回 ok=false。
func ParsePhysicalDatabaseName(migrationSet, database string) (workspaceID string, ok bool) {
	if !migrationSetPattern.MatchString(migrationSet) || len(database) > maximumMySQLIdentifierSize {
		return "", false
	}
	prefix := migrationSet + workspaceDatabaseSeparator
	if len(database) != len(prefix)+workspaceIDLength || !strings.HasPrefix(database, prefix) {
		return "", false
	}
	workspaceID = database[len(prefix):]
	if ValidateWorkspaceID(workspaceID) != nil {
		return "", false
	}
	return workspaceID, true
}
