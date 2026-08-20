package plannerdb

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"io"
	"time"

	"github.com/luyuancpp/pandora/tools/migrate/workspacedb"
)

func issueEnrollmentToken(ctx context.Context, registry Registry, random io.Reader, now time.Time, ttl time.Duration) (string, error) {
	return issueToken(ctx, registry, random, now, ttl, TokenPurposeEnroll, "")
}

func issueRecoveryToken(ctx context.Context, registry Registry, random io.Reader, now time.Time, ttl time.Duration, workspaceID string) (string, error) {
	if err := workspacedb.ValidateWorkspaceID(workspaceID); err != nil {
		return "", errors.New("recovery workspace ID 非法")
	}
	return issueToken(ctx, registry, random, now, ttl, TokenPurposeRecovery, workspaceID)
}

func issueToken(ctx context.Context, registry Registry, random io.Reader, now time.Time, ttl time.Duration, purpose TokenPurpose, targetWorkspaceID string) (string, error) {
	if registry == nil || random == nil {
		return "", errors.New("registry 与安全随机源不能为空")
	}
	if ttl < time.Minute || ttl > 24*time.Hour {
		return "", errors.New("enrollment token TTL 必须在 1 分钟到 24 小时之间")
	}
	raw := make([]byte, 32)
	if _, err := io.ReadFull(random, raw); err != nil {
		return "", err
	}
	token := base64.RawURLEncoding.EncodeToString(raw)
	hash := sha256.Sum256([]byte(token))
	if err := registry.IssueTokenGrant(ctx, TokenGrant{
		TokenHash: hash, Purpose: purpose, TargetWorkspaceID: targetWorkspaceID,
		ExpiresAt: now.UTC().Add(ttl),
	}); err != nil {
		return "", err
	}
	return token, nil
}
