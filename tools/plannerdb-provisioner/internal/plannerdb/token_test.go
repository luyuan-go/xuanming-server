package plannerdb

import (
	"bytes"
	"context"
	"crypto/sha256"
	"testing"
	"time"
)

type tokenRegistry struct {
	grant TokenGrant
	calls int
}

func (r *tokenRegistry) ClaimEnrollment(context.Context, EnrollmentClaim) (WorkspaceRecord, error) {
	panic("not used")
}
func (r *tokenRegistry) WithWorkspaceLock(context.Context, string, time.Duration, func(context.Context) error) (bool, error) {
	panic("not used")
}
func (r *tokenRegistry) GetWorkspace(context.Context, string) (WorkspaceRecord, error) {
	panic("not used")
}
func (r *tokenRegistry) BeginProvisionAttempt(context.Context, string) (WorkspaceRecord, error) {
	panic("not used")
}
func (r *tokenRegistry) TransitionAttemptState(context.Context, string, uint64, []State, State, string, AttemptStatus) error {
	panic("not used")
}
func (r *tokenRegistry) TransitionState(context.Context, string, []State, State, string) error {
	panic("not used")
}
func (r *tokenRegistry) IssueTokenGrant(_ context.Context, grant TokenGrant) error {
	r.grant = grant
	r.calls++
	return nil
}

func TestIssueTokenReturnsSecretOnceAndPersistsOnlyHash(t *testing.T) {
	now := time.Date(2026, 8, 20, 12, 0, 0, 0, time.UTC)
	registry := &tokenRegistry{}
	token, err := issueEnrollmentToken(context.Background(), registry, bytes.NewReader(bytes.Repeat([]byte{0x21}, 32)), now, 15*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if token != "ISEhISEhISEhISEhISEhISEhISEhISEhISEhISEhISE" {
		t.Fatalf("token=%q", token)
	}
	if registry.calls != 1 || registry.grant.TokenHash != sha256.Sum256([]byte(token)) {
		t.Fatalf("stored hash/calls=%x/%d", registry.grant.TokenHash, registry.calls)
	}
	if registry.grant.Purpose != TokenPurposeEnroll || registry.grant.TargetWorkspaceID != "" {
		t.Fatalf("enroll grant=%+v", registry.grant)
	}
	if !registry.grant.ExpiresAt.Equal(now.Add(15 * time.Minute)) {
		t.Fatalf("expires_at=%s", registry.grant.ExpiresAt)
	}
}

func TestIssueRecoveryTokenBindsExactWorkspaceAndStoresOnlyHash(t *testing.T) {
	now := time.Date(2026, 8, 20, 12, 0, 0, 0, time.UTC)
	registry := &tokenRegistry{}
	workspaceID := "01arz3ndektsv4rrffq69g5fav"
	token, err := issueRecoveryToken(context.Background(), registry,
		bytes.NewReader(bytes.Repeat([]byte{0x22}, 32)), now, 10*time.Minute, workspaceID)
	if err != nil {
		t.Fatal(err)
	}
	if registry.grant.TokenHash != sha256.Sum256([]byte(token)) ||
		registry.grant.Purpose != TokenPurposeRecovery || registry.grant.TargetWorkspaceID != workspaceID {
		t.Fatalf("recovery grant=%+v", registry.grant)
	}
}
