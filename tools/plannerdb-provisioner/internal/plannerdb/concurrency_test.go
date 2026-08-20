package plannerdb

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/luyuancpp/pandora/tools/migrate/workspacedb"
)

type concurrentRegistry struct {
	mu        sync.Mutex
	tokens    map[[32]byte][32]byte
	purposes  map[[32]byte]TokenPurpose
	targets   map[[32]byte]string
	records   map[[32]byte]WorkspaceRecord
	workspace map[string][32]byte
	locks     map[string]*sync.Mutex
}

func newConcurrentRegistry() *concurrentRegistry {
	return &concurrentRegistry{
		tokens: make(map[[32]byte][32]byte), records: make(map[[32]byte]WorkspaceRecord),
		purposes: make(map[[32]byte]TokenPurpose), targets: make(map[[32]byte]string),
		workspace: make(map[string][32]byte), locks: make(map[string]*sync.Mutex),
	}
}

func (r *concurrentRegistry) ClaimEnrollment(_ context.Context, claim EnrollmentClaim) (WorkspaceRecord, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	consumed, ok := r.tokens[claim.TokenHash]
	if !ok {
		return WorkspaceRecord{}, ErrInvalidEnrollmentToken
	}
	if consumed != ([32]byte{}) && consumed != claim.DeviceDigest {
		return WorkspaceRecord{}, ErrInvalidEnrollmentToken
	}
	if r.purposes[claim.TokenHash] == TokenPurposeRecovery {
		target := r.targets[claim.TokenHash]
		if target == "" || claim.ExpectedWorkspaceID != target {
			return WorkspaceRecord{}, ErrInvalidEnrollmentToken
		}
		digest, ok := r.workspace[target]
		if !ok || digest != claim.DeviceDigest {
			return WorkspaceRecord{}, ErrInvalidEnrollmentToken
		}
		r.tokens[claim.TokenHash] = claim.DeviceDigest
		return r.records[digest], nil
	}
	if existing, ok := r.records[claim.DeviceDigest]; ok {
		if consumed == ([32]byte{}) {
			return WorkspaceRecord{}, ErrDeviceAlreadyEnrolled
		}
		return existing, nil
	}
	if other, collision := r.workspace[claim.Candidate.WorkspaceID]; collision && other != claim.DeviceDigest {
		return WorkspaceRecord{}, ErrWorkspaceCollision
	}
	r.tokens[claim.TokenHash] = claim.DeviceDigest
	r.records[claim.DeviceDigest] = claim.Candidate
	r.workspace[claim.Candidate.WorkspaceID] = claim.DeviceDigest
	return claim.Candidate, nil
}

func (r *concurrentRegistry) WithWorkspaceLock(ctx context.Context, workspaceID string, _ time.Duration, fn func(context.Context) error) (bool, error) {
	r.mu.Lock()
	lock := r.locks[workspaceID]
	if lock == nil {
		lock = &sync.Mutex{}
		r.locks[workspaceID] = lock
	}
	r.mu.Unlock()
	lock.Lock()
	defer lock.Unlock()
	return true, fn(ctx)
}

func (r *concurrentRegistry) GetWorkspace(_ context.Context, workspaceID string) (WorkspaceRecord, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	digest, ok := r.workspace[workspaceID]
	if !ok {
		return WorkspaceRecord{}, errors.New("not found")
	}
	return r.records[digest], nil
}

func (r *concurrentRegistry) BeginProvisionAttempt(_ context.Context, workspaceID string) (WorkspaceRecord, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	digest, ok := r.workspace[workspaceID]
	if !ok {
		return WorkspaceRecord{}, errors.New("not found")
	}
	record := r.records[digest]
	if record.AttemptStatus == AttemptStatusRunning {
		return record, ErrAttemptOutcomeUnknown
	}
	if record.State != StateProvisioning || record.AttemptStatus != AttemptStatusIdle {
		return record, errors.New("attempt cannot start")
	}
	record.OperationEpoch++
	record.AttemptStatus = AttemptStatusRunning
	r.records[digest] = record
	return record, nil
}

func (r *concurrentRegistry) TransitionAttemptState(_ context.Context, workspaceID string, epoch uint64, from []State, to State, failure string, status AttemptStatus) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	digest, ok := r.workspace[workspaceID]
	if !ok {
		return errors.New("not found")
	}
	record := r.records[digest]
	allowed := false
	for _, state := range from {
		allowed = allowed || record.State == state
	}
	if !allowed || record.OperationEpoch != epoch {
		return errors.New("fence mismatch")
	}
	record.State = to
	record.FailureCode = failure
	record.AttemptStatus = status
	r.records[digest] = record
	return nil
}

func (r *concurrentRegistry) TransitionState(_ context.Context, workspaceID string, _ []State, to State, failure string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	digest, ok := r.workspace[workspaceID]
	if !ok {
		return errors.New("not found")
	}
	record := r.records[digest]
	record.State = to
	record.FailureCode = failure
	record.AttemptStatus = terminalAttemptStatus(to)
	r.records[digest] = record
	return nil
}

func (r *concurrentRegistry) IssueTokenGrant(_ context.Context, grant TokenGrant) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if grant.Purpose == TokenPurposeRecovery {
		if _, ok := r.workspace[grant.TargetWorkspaceID]; !ok {
			return errors.New("target not found")
		}
	}
	r.tokens[grant.TokenHash] = [32]byte{}
	r.purposes[grant.TokenHash] = grant.Purpose
	r.targets[grant.TokenHash] = grant.TargetWorkspaceID
	return nil
}

type noOpResources struct{}

func (noOpResources) EnsureWorkspace(context.Context, WorkspaceAccess) error { return nil }

type noOpMigrator struct{}

func (noOpMigrator) MigrateWorkspace(context.Context, WorkspaceAccess) error { return nil }

func TestOneThousandConcurrentRegistrationsHaveNoWorkspaceCollision(t *testing.T) {
	registry := newConcurrentRegistry()
	service, err := newService(serviceConfig{
		Registry: registry, Resources: noOpResources{}, Migrator: noOpMigrator{},
		Endpoint:  Endpoint{Host: "pandora-dev-db.intra", Port: 3306, TLSServerName: "pandora-dev-db.intra"},
		MasterKey: make([]byte, 32), Random: rand.Reader, ProvisionTimeout: time.Minute,
		MaxConcurrentProvisions: 32,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = service.close(5 * time.Second) }()
	const registrations = 1000
	requests := make([]enrollRequest, registrations)
	for index := range requests {
		rawToken := make([]byte, 32)
		binary.BigEndian.PutUint64(rawToken[24:], uint64(index+1))
		token := base64.RawURLEncoding.EncodeToString(rawToken)
		tokenHash := sha256.Sum256([]byte(token))
		if err := registry.IssueTokenGrant(context.Background(), TokenGrant{
			TokenHash: tokenHash, Purpose: TokenPurposeEnroll, ExpiresAt: time.Now().Add(time.Hour),
		}); err != nil {
			t.Fatal(err)
		}
		device := sha256.Sum256([]byte(fmt.Sprintf("device-%04d", index)))
		requests[index] = enrollRequest{
			SchemaVersion: 1, EnrollmentToken: token,
			DeviceDigest: fmt.Sprintf("sha256:%x", device), DisplayName: fmt.Sprintf("PC%04d\\planner", index),
		}
	}
	var wait sync.WaitGroup
	errorsSeen := make(chan error, registrations)
	for _, request := range requests {
		request := request
		wait.Add(1)
		go func() {
			defer wait.Done()
			_, err := service.enroll(context.Background(), request)
			if err != nil && !errors.Is(err, ErrProvisionCapacity) {
				errorsSeen <- err
			}
		}()
	}
	wait.Wait()
	close(errorsSeen)
	for err := range errorsSeen {
		t.Fatalf("registration error: %v", err)
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if len(registry.records) != registrations || len(registry.workspace) != registrations {
		t.Fatalf("records/workspaces=%d/%d", len(registry.records), len(registry.workspace))
	}
	for workspaceID := range registry.workspace {
		if err := workspacedb.ValidateWorkspaceID(workspaceID); err != nil {
			t.Fatal(err)
		}
	}
}

func TestNewTokenCannotClaimExistingDeviceIdentity(t *testing.T) {
	registry := newConcurrentRegistry()
	service, err := newService(serviceConfig{
		Registry: registry, Resources: noOpResources{}, Migrator: noOpMigrator{},
		Endpoint:  Endpoint{Host: "pandora-dev-db.intra", Port: 3306, TLSServerName: "pandora-dev-db.intra"},
		MasterKey: make([]byte, 32), Random: rand.Reader, ProvisionTimeout: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = service.close(time.Second) }()

	firstRaw := make([]byte, 32)
	firstRaw[31] = 1
	firstToken := base64.RawURLEncoding.EncodeToString(firstRaw)
	secondRaw := make([]byte, 32)
	secondRaw[31] = 2
	secondToken := base64.RawURLEncoding.EncodeToString(secondRaw)
	firstHash := sha256.Sum256([]byte(firstToken))
	secondHash := sha256.Sum256([]byte(secondToken))
	_ = registry.IssueTokenGrant(context.Background(), TokenGrant{TokenHash: firstHash, Purpose: TokenPurposeEnroll, ExpiresAt: time.Now().Add(time.Hour)})
	_ = registry.IssueTokenGrant(context.Background(), TokenGrant{TokenHash: secondHash, Purpose: TokenPurposeEnroll, ExpiresAt: time.Now().Add(time.Hour)})
	device := sha256.Sum256([]byte("cloned-device"))
	request := enrollRequest{
		SchemaVersion: 1, EnrollmentToken: firstToken,
		DeviceDigest: fmt.Sprintf("sha256:%x", device), DisplayName: "PC01\\planner",
	}
	first, err := service.enroll(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	request.EnrollmentToken = secondToken
	request.DisplayName = "CLONE\\planner"
	if _, err := service.enroll(context.Background(), request); !errors.Is(err, ErrDeviceAlreadyEnrolled) {
		t.Fatalf("new token + old digest error=%v", err)
	}
	registry.mu.Lock()
	secondConsumed := registry.tokens[secondHash]
	registry.mu.Unlock()
	if secondConsumed != ([32]byte{}) {
		t.Fatalf("rejected token was consumed: %x", secondConsumed)
	}

	request.EnrollmentToken = firstToken
	request.DisplayName = "PC01\\planner"
	retry, err := service.enroll(context.Background(), request)
	if err != nil || retry.WorkspaceID != first.WorkspaceID {
		t.Fatalf("same token retry=%+v err=%v", retry, err)
	}
	otherDevice := sha256.Sum256([]byte("different-device"))
	request.DeviceDigest = fmt.Sprintf("sha256:%x", otherDevice)
	if _, err := service.enroll(context.Background(), request); !errors.Is(err, ErrInvalidEnrollmentToken) {
		t.Fatalf("same token + other digest error=%v", err)
	}
}

func TestRecoveryTokenRestoresOnlyExactWorkspaceAndDevice(t *testing.T) {
	registry := newConcurrentRegistry()
	service, err := newService(serviceConfig{
		Registry: registry, Resources: noOpResources{}, Migrator: noOpMigrator{},
		Endpoint:  Endpoint{Host: "pandora-dev-db.intra", Port: 3306, TLSServerName: "pandora-dev-db.intra"},
		MasterKey: make([]byte, 32), Random: rand.Reader, ProvisionTimeout: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = service.close(time.Second) }()

	enrollRaw := make([]byte, 32)
	enrollRaw[31] = 3
	enrollToken := base64.RawURLEncoding.EncodeToString(enrollRaw)
	enrollHash := sha256.Sum256([]byte(enrollToken))
	_ = registry.IssueTokenGrant(context.Background(), TokenGrant{
		TokenHash: enrollHash, Purpose: TokenPurposeEnroll, ExpiresAt: time.Now().Add(time.Hour),
	})
	device := sha256.Sum256([]byte("recoverable-device"))
	request := enrollRequest{
		SchemaVersion: 1, EnrollmentToken: enrollToken,
		DeviceDigest: fmt.Sprintf("sha256:%x", device), DisplayName: "PC01\\planner",
	}
	first, err := service.enroll(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}

	recoveryRaw := make([]byte, 32)
	recoveryRaw[31] = 4
	recoveryToken := base64.RawURLEncoding.EncodeToString(recoveryRaw)
	recoveryHash := sha256.Sum256([]byte(recoveryToken))
	if err := registry.IssueTokenGrant(context.Background(), TokenGrant{
		TokenHash: recoveryHash, Purpose: TokenPurposeRecovery,
		TargetWorkspaceID: first.WorkspaceID, ExpiresAt: time.Now().Add(time.Hour),
	}); err != nil {
		t.Fatal(err)
	}
	request.EnrollmentToken = recoveryToken
	request.ExpectedWorkspaceID = first.WorkspaceID
	recovered, err := service.enroll(context.Background(), request)
	if err != nil || recovered.WorkspaceID != first.WorkspaceID {
		t.Fatalf("recovery=%+v err=%v", recovered, err)
	}
	retry, err := service.enroll(context.Background(), request)
	if err != nil || retry.WorkspaceID != first.WorkspaceID {
		t.Fatalf("recovery retry=%+v err=%v", retry, err)
	}
	request.ExpectedWorkspaceID = "02arz3ndektsv4rrffq69g5fav"
	if _, err := service.enroll(context.Background(), request); !errors.Is(err, ErrInvalidEnrollmentToken) {
		t.Fatalf("wrong expected workspace error=%v", err)
	}
	request.ExpectedWorkspaceID = first.WorkspaceID
	otherDevice := sha256.Sum256([]byte("cloned-recovery-device"))
	request.DeviceDigest = fmt.Sprintf("sha256:%x", otherDevice)
	if _, err := service.enroll(context.Background(), request); !errors.Is(err, ErrInvalidEnrollmentToken) {
		t.Fatalf("wrong recovery device error=%v", err)
	}
	registry.mu.Lock()
	recordCount := len(registry.records)
	registry.mu.Unlock()
	if recordCount != 1 {
		t.Fatalf("recovery created a new workspace: records=%d", recordCount)
	}
}
