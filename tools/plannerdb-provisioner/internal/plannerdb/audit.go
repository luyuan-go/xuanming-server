package plannerdb

import (
	"context"
	"errors"
	"fmt"
	"time"
)

type AuditOptions struct {
	StuckAfter    time.Duration
	InactiveAfter time.Duration
	EntryLimit    int
}

type AuditCounts struct {
	TotalWorkspaces          uint64 `json:"total_workspaces"`
	StaleInProgress          uint64 `json:"stale_in_progress"`
	MigrationFailed          uint64 `json:"migration_failed"`
	InactiveWorkspaces       uint64 `json:"inactive_workspaces"`
	AttentionWorkspaces      uint64 `json:"attention_workspaces"`
	TotalTokenHashes         uint64 `json:"total_token_hashes"`
	ExpiredUnusedTokenHashes uint64 `json:"expired_unused_token_hashes"`
	ExpiredConsumedHashes    uint64 `json:"expired_consumed_token_hashes"`
}

type AuditWorkspace struct {
	WorkspaceID string    `json:"workspace_id"`
	DisplayName string    `json:"display_name"`
	State       State     `json:"state"`
	FailureCode string    `json:"failure_code,omitempty"`
	UpdatedAt   time.Time `json:"updated_at"`
	LastSeenAt  time.Time `json:"last_seen_at"`
	Stuck       bool      `json:"stuck"`
	Inactive    bool      `json:"inactive"`
}

type AuditReport struct {
	SchemaVersion        int              `json:"schema_version"`
	GeneratedAt          time.Time        `json:"generated_at"`
	StuckAfterSeconds    int64            `json:"stuck_after_seconds"`
	InactiveAfterSeconds int64            `json:"inactive_after_seconds"`
	EntryLimit           int              `json:"entry_limit"`
	EntriesTruncated     bool             `json:"entries_truncated"`
	Counts               AuditCounts      `json:"counts"`
	Workspaces           []AuditWorkspace `json:"workspaces"`
}

// HasBlockingFindings 只把“需要中心维护者处理”的项设为门禁失败。inactive workspace
// 仅报告，绝不自动删除，也不会因为策划暂时离线把日常 audit 永久打红。
func (report AuditReport) HasBlockingFindings() bool {
	return report.Counts.StaleInProgress != 0 || report.Counts.MigrationFailed != 0 ||
		report.Counts.ExpiredUnusedTokenHashes != 0 || report.Counts.ExpiredConsumedHashes != 0
}

// Audit 只读扫描 registry。它不删除 token、workspace、账号、数据库或 schema。
func (b *MySQLBackend) Audit(ctx context.Context, options AuditOptions) (AuditReport, error) {
	if b == nil || b.db == nil {
		return AuditReport{}, errors.New("planner registry backend 未初始化")
	}
	const maximumAge = 10 * 365 * 24 * time.Hour
	if options.StuckAfter < time.Minute || options.StuckAfter > maximumAge ||
		options.InactiveAfter < time.Minute || options.InactiveAfter > maximumAge {
		return AuditReport{}, errors.New("audit stuck/inactive 时限必须在 1 分钟到 10 年之间")
	}
	if options.EntryLimit < 1 || options.EntryLimit > 10000 {
		return AuditReport{}, errors.New("audit entry limit 必须在 1..10000")
	}
	now := time.Now().UTC()
	stuckBefore := now.Add(-options.StuckAfter)
	inactiveBefore := now.Add(-options.InactiveAfter)
	consumedRetentionBefore := now.Add(-30 * 24 * time.Hour)
	report := AuditReport{
		SchemaVersion: schemaVersion, GeneratedAt: now,
		StuckAfterSeconds:    int64(options.StuckAfter / time.Second),
		InactiveAfterSeconds: int64(options.InactiveAfter / time.Second),
		EntryLimit:           options.EntryLimit, Workspaces: make([]AuditWorkspace, 0),
	}

	workspaceCountsQuery := `SELECT
COUNT(*),
COALESCE(SUM(state IN ('PROVISIONING','MIGRATING') AND updated_at < ?), 0),
COALESCE(SUM(state = 'MIGRATION_FAILED'), 0),
COALESCE(SUM(last_seen_at < ?), 0),
COALESCE(SUM((state IN ('PROVISIONING','MIGRATING') AND updated_at < ?)
          OR state = 'MIGRATION_FAILED' OR last_seen_at < ?), 0)
FROM ` + b.registryTable("planner_workspaces")
	if err := b.db.QueryRowContext(ctx, workspaceCountsQuery,
		stuckBefore, inactiveBefore, stuckBefore, inactiveBefore).Scan(
		&report.Counts.TotalWorkspaces, &report.Counts.StaleInProgress,
		&report.Counts.MigrationFailed, &report.Counts.InactiveWorkspaces,
		&report.Counts.AttentionWorkspaces,
	); err != nil {
		return AuditReport{}, errors.New("统计 planner workspace audit 数据失败")
	}

	tokenCountsQuery := `SELECT
COUNT(*),
COALESCE(SUM(consumed_at IS NULL AND expires_at < ?), 0),
COALESCE(SUM(consumed_at IS NOT NULL AND consumed_at < ?), 0)
FROM ` + b.registryTable("planner_enrollment_tokens")
	if err := b.db.QueryRowContext(ctx, tokenCountsQuery, now, consumedRetentionBefore).Scan(
		&report.Counts.TotalTokenHashes, &report.Counts.ExpiredUnusedTokenHashes,
		&report.Counts.ExpiredConsumedHashes,
	); err != nil {
		return AuditReport{}, errors.New("统计 enrollment token audit 数据失败")
	}

	rows, err := b.db.QueryContext(ctx, `SELECT workspace_id, display_name, state, failure_code,
updated_at, last_seen_at
FROM `+b.registryTable("planner_workspaces")+`
WHERE (state IN ('PROVISIONING','MIGRATING') AND updated_at < ?)
   OR state = 'MIGRATION_FAILED'
   OR last_seen_at < ?
ORDER BY
  CASE WHEN state = 'MIGRATION_FAILED' THEN 0
       WHEN state IN ('PROVISIONING','MIGRATING') AND updated_at < ? THEN 1
       ELSE 2 END,
  updated_at, workspace_id
LIMIT ?`, stuckBefore, inactiveBefore, stuckBefore, options.EntryLimit)
	if err != nil {
		return AuditReport{}, errors.New("读取 planner workspace audit 明细失败")
	}
	defer rows.Close()
	for rows.Next() {
		var item AuditWorkspace
		if err := rows.Scan(&item.WorkspaceID, &item.DisplayName, &item.State, &item.FailureCode,
			&item.UpdatedAt, &item.LastSeenAt); err != nil {
			return AuditReport{}, errors.New("解析 planner workspace audit 明细失败")
		}
		if !validState(item.State) {
			return AuditReport{}, fmt.Errorf("planner workspace %s 含非法状态", item.WorkspaceID)
		}
		item.Stuck = item.State == StateMigrationFailed ||
			(item.State != StateReady && item.UpdatedAt.Before(stuckBefore))
		item.Inactive = item.LastSeenAt.Before(inactiveBefore)
		report.Workspaces = append(report.Workspaces, item)
	}
	if err := rows.Err(); err != nil {
		return AuditReport{}, errors.New("遍历 planner workspace audit 明细失败")
	}
	report.EntriesTruncated = report.Counts.AttentionWorkspaces > uint64(len(report.Workspaces))
	return report, nil
}
