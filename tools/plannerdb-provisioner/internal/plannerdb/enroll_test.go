package plannerdb

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

const testEnrollmentToken = "ERERERERERERERERERERERERERERERERERERERERERE"

type memoryRegistry struct {
	mu                         sync.Mutex
	workspaceMu                sync.Mutex
	tokenHash                  [32]byte
	tokenDevice                [32]byte
	consumed                   bool
	workspace                  WorkspaceRecord
	states                     []State
	claimErr                   error
	failNextTerminalTransition bool
}

type observedWorkspaceLockRegistry struct {
	*memoryRegistry
	attempts chan struct{}
}

func (r *observedWorkspaceLockRegistry) WithWorkspaceLock(ctx context.Context, _ string, _ time.Duration, fn func(context.Context) error) (bool, error) {
	r.attempts <- struct{}{}
	r.workspaceMu.Lock()
	defer r.workspaceMu.Unlock()
	return true, fn(ctx)
}

type blockingClaimRegistry struct {
	mu      sync.Mutex
	calls   int
	started chan struct{}
	release chan struct{}
}

func (r *blockingClaimRegistry) ClaimEnrollment(ctx context.Context, _ EnrollmentClaim) (WorkspaceRecord, error) {
	r.mu.Lock()
	r.calls++
	r.mu.Unlock()
	r.started <- struct{}{}
	select {
	case <-r.release:
		return WorkspaceRecord{}, ErrInvalidEnrollmentToken
	case <-ctx.Done():
		return WorkspaceRecord{}, ctx.Err()
	}
}

func (r *blockingClaimRegistry) WithWorkspaceLock(context.Context, string, time.Duration, func(context.Context) error) (bool, error) {
	panic("not used")
}
func (r *blockingClaimRegistry) GetWorkspace(context.Context, string) (WorkspaceRecord, error) {
	panic("not used")
}
func (r *blockingClaimRegistry) BeginProvisionAttempt(context.Context, string) (WorkspaceRecord, error) {
	panic("not used")
}
func (r *blockingClaimRegistry) TransitionAttemptState(context.Context, string, uint64, []State, State, string, AttemptStatus) error {
	panic("not used")
}
func (r *blockingClaimRegistry) TransitionState(context.Context, string, []State, State, string) error {
	panic("not used")
}
func (r *blockingClaimRegistry) IssueTokenGrant(context.Context, TokenGrant) error { panic("not used") }

func (r *blockingClaimRegistry) callCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.calls
}

func (r *memoryRegistry) ClaimEnrollment(_ context.Context, claim EnrollmentClaim) (WorkspaceRecord, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.claimErr != nil {
		return WorkspaceRecord{}, r.claimErr
	}
	if claim.TokenHash != r.tokenHash {
		return WorkspaceRecord{}, ErrInvalidEnrollmentToken
	}
	if r.consumed && r.tokenDevice != claim.DeviceDigest {
		return WorkspaceRecord{}, ErrInvalidEnrollmentToken
	}
	if !r.consumed {
		r.consumed = true
		r.tokenDevice = claim.DeviceDigest
	}
	if r.workspace.WorkspaceID == "" {
		r.workspace = claim.Candidate
	}
	r.workspace.DisplayName = claim.DisplayName
	return r.workspace, nil
}

func TestEnrollDistinguishesInvalidInputFromInternalFailure(t *testing.T) {
	registry := &memoryRegistry{tokenHash: sha256.Sum256([]byte(testEnrollmentToken)), claimErr: errors.New("database unavailable")}
	service, err := newService(serviceConfig{
		Registry: registry, Resources: &recordingResources{}, Migrator: &recordingMigrator{},
		Endpoint:  Endpoint{Host: "pandora-dev-db.intra", Port: 3306, TLSServerName: "pandora-dev-db.intra"},
		MasterKey: bytes.Repeat([]byte{0x42}, 32), Random: bytes.NewReader(bytes.Repeat([]byte{0x11}, 4096)),
		ProvisionTimeout: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = service.close(time.Second) }()
	server := httptest.NewTLSServer(newHTTPHandler(service))
	defer server.Close()
	validBody := []byte(`{"schema_version":1,"enrollment_token":"` + testEnrollmentToken + `","device_digest":"sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef","display_name":"PC02\\planner"}`)
	response, err := server.Client().Post(server.URL+"/v1/enroll", "application/json", bytes.NewReader(validBody))
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	if response.StatusCode != http.StatusInternalServerError {
		t.Fatalf("internal status=%d", response.StatusCode)
	}

	invalidBody := []byte(`{"schema_version":2,"enrollment_token":"bad","device_digest":"bad","display_name":""}`)
	response, err = server.Client().Post(server.URL+"/v1/enroll", "application/json", bytes.NewReader(invalidBody))
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusBadRequest {
		t.Fatalf("invalid status=%d", response.StatusCode)
	}
}

func TestEnrollCapacityRejectsBeforeClaimOrTokenConsumption(t *testing.T) {
	registry := &blockingClaimRegistry{started: make(chan struct{}, 2), release: make(chan struct{})}
	service, err := newService(serviceConfig{
		Registry: registry, Resources: &recordingResources{}, Migrator: &recordingMigrator{},
		Endpoint:  Endpoint{Host: "pandora-dev-db.intra", Port: 3306, TLSServerName: "pandora-dev-db.intra"},
		MasterKey: bytes.Repeat([]byte{0x42}, 32), Random: bytes.NewReader(bytes.Repeat([]byte{0x11}, 8192)),
		ProvisionTimeout: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = service.close(time.Second) }()
	handler := newHTTPHandlerWithLimits(service, 2, time.Second)
	body := []byte(`{"schema_version":1,"enrollment_token":"` + testEnrollmentToken + `","device_digest":"sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef","display_name":"PC02\\planner"}`)

	var wait sync.WaitGroup
	for range 2 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			recorder := httptest.NewRecorder()
			request := httptest.NewRequest(http.MethodPost, "https://provisioner.test/v1/enroll", bytes.NewReader(body))
			request.Header.Set("Content-Type", "application/json")
			handler.ServeHTTP(recorder, request)
		}()
	}
	for range 2 {
		select {
		case <-registry.started:
		case <-time.After(time.Second):
			t.Fatal("blocking claims did not start")
		}
	}
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "https://provisioner.test/v1/enroll", bytes.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusServiceUnavailable || recorder.Header().Get("Retry-After") != "2" {
		t.Fatalf("capacity status=%d retry=%q body=%s", recorder.Code, recorder.Header().Get("Retry-After"), recorder.Body.String())
	}
	if registry.callCount() != 2 {
		t.Fatalf("capacity request reached registry: claims=%d", registry.callCount())
	}
	close(registry.release)
	wait.Wait()
}

func TestEnrollRequestDeadlineReleasesCapacitySlot(t *testing.T) {
	registry := &blockingClaimRegistry{started: make(chan struct{}, 2), release: make(chan struct{})}
	service, err := newService(serviceConfig{
		Registry: registry, Resources: &recordingResources{}, Migrator: &recordingMigrator{},
		Endpoint:  Endpoint{Host: "pandora-dev-db.intra", Port: 3306, TLSServerName: "pandora-dev-db.intra"},
		MasterKey: bytes.Repeat([]byte{0x42}, 32), Random: bytes.NewReader(bytes.Repeat([]byte{0x11}, 8192)),
		ProvisionTimeout: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = service.close(time.Second) }()
	handler := newHTTPHandlerWithLimits(service, 1, 30*time.Millisecond)
	body := []byte(`{"schema_version":1,"enrollment_token":"` + testEnrollmentToken + `","device_digest":"sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef","display_name":"PC02\\planner"}`)
	for attempt := 0; attempt < 2; attempt++ {
		recorder := httptest.NewRecorder()
		request := httptest.NewRequest(http.MethodPost, "https://provisioner.test/v1/enroll", bytes.NewReader(body))
		request.Header.Set("Content-Type", "application/json")
		handler.ServeHTTP(recorder, request)
		if recorder.Code != http.StatusServiceUnavailable || !strings.Contains(recorder.Body.String(), "ENROLL_TIMEOUT") {
			t.Fatalf("attempt=%d status=%d body=%s", attempt, recorder.Code, recorder.Body.String())
		}
	}
	if registry.callCount() != 2 {
		t.Fatalf("deadline did not release slot: claims=%d", registry.callCount())
	}
}

func TestDeviceAlreadyEnrolledResponseIsConflictWithoutCredential(t *testing.T) {
	registry := &memoryRegistry{
		tokenHash: sha256.Sum256([]byte(testEnrollmentToken)),
		claimErr:  ErrDeviceAlreadyEnrolled,
	}
	service, err := newService(serviceConfig{
		Registry: registry, Resources: &recordingResources{}, Migrator: &recordingMigrator{},
		Endpoint:  Endpoint{Host: "pandora-dev-db.intra", Port: 3306, TLSServerName: "pandora-dev-db.intra"},
		MasterKey: bytes.Repeat([]byte{0x42}, 32), Random: bytes.NewReader(bytes.Repeat([]byte{0x11}, 4096)),
		ProvisionTimeout: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = service.close(time.Second) }()
	server := httptest.NewTLSServer(newHTTPHandler(service))
	defer server.Close()
	body := []byte(`{"schema_version":1,"enrollment_token":"` + testEnrollmentToken + `","device_digest":"sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef","display_name":"CLONE\\planner"}`)
	response, err := server.Client().Post(server.URL+"/v1/enroll", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	var payload bytes.Buffer
	_, _ = payload.ReadFrom(response.Body)
	if response.StatusCode != http.StatusConflict || !strings.Contains(payload.String(), `"code":"DEVICE_ALREADY_ENROLLED"`) {
		t.Fatalf("status=%d body=%s", response.StatusCode, payload.String())
	}
	if strings.Contains(payload.String(), "credential") || strings.Contains(payload.String(), testEnrollmentToken) {
		t.Fatalf("identity conflict leaked enrollment material: %s", payload.String())
	}
}

func TestMigrationFailureErrorMappingIsTerminalWithoutRetry(t *testing.T) {
	recorder := httptest.NewRecorder()
	writeEnrollError(recorder, ErrMigrationFailed)
	if recorder.Code != http.StatusConflict {
		t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	if got := recorder.Header().Get("Retry-After"); got != "" {
		t.Fatalf("terminal migration failure advertised automatic retry: %q", got)
	}
	if !strings.Contains(recorder.Body.String(), `"code":"MIGRATION_FAILED"`) ||
		strings.Contains(recorder.Body.String(), "同一 token 重试") {
		t.Fatalf("terminal migration body=%s", recorder.Body.String())
	}
}

func TestEnrollLoadShedsBeforeStartingAnotherWorkspaceProvision(t *testing.T) {
	registry := &memoryRegistry{tokenHash: sha256.Sum256([]byte(testEnrollmentToken))}
	resources := &recordingResources{}
	service, err := newService(serviceConfig{
		Registry: registry, Resources: resources, Migrator: &recordingMigrator{},
		Endpoint:  Endpoint{Host: "pandora-dev-db.intra", Port: 3306, TLSServerName: "pandora-dev-db.intra"},
		MasterKey: bytes.Repeat([]byte{0x42}, 32), Random: bytes.NewReader(bytes.Repeat([]byte{0x11}, 4096)),
		ProvisionTimeout: time.Minute, MaxConcurrentProvisions: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = service.close(time.Second) }()
	service.provisionSlots <- struct{}{}
	defer func() { <-service.provisionSlots }()
	server := httptest.NewTLSServer(newHTTPHandler(service))
	defer server.Close()
	body := []byte(`{"schema_version":1,"enrollment_token":"` + testEnrollmentToken + `","device_digest":"sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef","display_name":"PC02\\planner"}`)
	response, err := server.Client().Post(server.URL+"/v1/enroll", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusAccepted || response.Header.Get("Retry-After") != "2" {
		t.Fatalf("status=%d retry-after=%q", response.StatusCode, response.Header.Get("Retry-After"))
	}
	var pending enrollResponse
	if err := json.NewDecoder(response.Body).Decode(&pending); err != nil {
		t.Fatal(err)
	}
	if pending.State != StateProvisioning || pending.Credential != nil || pending.RetryAfterMS != 2000 {
		t.Fatalf("pending response=%+v", pending)
	}
	if resources.callCount() != 0 {
		t.Fatalf("resource provisioning started despite saturation: %d", resources.callCount())
	}
}

func (r *memoryRegistry) WithWorkspaceLock(ctx context.Context, _ string, _ time.Duration, fn func(context.Context) error) (bool, error) {
	r.workspaceMu.Lock()
	defer r.workspaceMu.Unlock()
	return true, fn(ctx)
}

func (r *memoryRegistry) GetWorkspace(_ context.Context, _ string) (WorkspaceRecord, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.workspace, nil
}

func (r *memoryRegistry) BeginProvisionAttempt(_ context.Context, _ string) (WorkspaceRecord, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.workspace.AttemptStatus == AttemptStatusRunning {
		return r.workspace, ErrAttemptOutcomeUnknown
	}
	if r.workspace.State != StateProvisioning || r.workspace.AttemptStatus != AttemptStatusIdle {
		return r.workspace, errors.New("attempt cannot start")
	}
	r.workspace.OperationEpoch++
	r.workspace.AttemptStatus = AttemptStatusRunning
	r.states = append(r.states, StateProvisioning)
	return r.workspace, nil
}

func (r *memoryRegistry) TransitionAttemptState(_ context.Context, _ string, epoch uint64, from []State, to State, failureCode string, status AttemptStatus) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if to == StateMigrationFailed && r.failNextTerminalTransition {
		r.failNextTerminalTransition = false
		return errors.New("forced terminal CAS failure")
	}
	allowed := false
	for _, state := range from {
		allowed = allowed || r.workspace.State == state
	}
	if !allowed || r.workspace.OperationEpoch != epoch {
		return errors.New("fence mismatch")
	}
	r.workspace.State = to
	r.workspace.FailureCode = failureCode
	r.workspace.AttemptStatus = status
	r.states = append(r.states, to)
	return nil
}

func (r *memoryRegistry) TransitionState(_ context.Context, _ string, _ []State, to State, failureCode string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.workspace.State = to
	r.workspace.FailureCode = failureCode
	r.workspace.AttemptStatus = terminalAttemptStatus(to)
	r.states = append(r.states, to)
	return nil
}

func (r *memoryRegistry) snapshot() WorkspaceRecord {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.workspace
}

func (r *memoryRegistry) stateHistory() []State {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]State(nil), r.states...)
}

func (r *memoryRegistry) IssueTokenGrant(context.Context, TokenGrant) error {
	return errors.New("not used")
}

type recordingResources struct {
	mu    sync.Mutex
	calls int
	err   error
}

func (r *recordingResources) EnsureWorkspace(_ context.Context, _ WorkspaceAccess) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls++
	return r.err
}

func (r *recordingResources) callCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.calls
}

type recordingMigrator struct {
	mu             sync.Mutex
	calls          int
	err            error
	leakCredential bool
}

type recordingLogger struct {
	mu   sync.Mutex
	text string
}

func (l *recordingLogger) Printf(format string, values ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.text += fmt.Sprintf(format, values...)
}

func (l *recordingLogger) contents() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.text
}

func (m *recordingMigrator) MigrateWorkspace(_ context.Context, access WorkspaceAccess) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls++
	if m.leakCredential {
		return errors.New("driver accidentally included " + access.MigrationPassword)
	}
	return m.err
}

func (m *recordingMigrator) setError(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.err = err
	m.leakCredential = false
}

func (m *recordingMigrator) callCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.calls
}

func TestMigrationFailureNeverLeaksCredentialsAndRetryConverges(t *testing.T) {
	tokenHash := sha256.Sum256([]byte(testEnrollmentToken))
	registry := &memoryRegistry{tokenHash: tokenHash}
	resources := &recordingResources{}
	migrator := &recordingMigrator{}
	logger := &recordingLogger{}
	service, err := newService(serviceConfig{
		Registry: registry, Resources: resources, Migrator: migrator,
		Endpoint:  Endpoint{Host: "pandora-dev-db.intra", Port: 3306, TLSServerName: "pandora-dev-db.intra"},
		MasterKey: bytes.Repeat([]byte{0x42}, 32), Random: bytes.NewReader(bytes.Repeat([]byte{0x11}, 8192)),
		ProvisionTimeout: time.Minute, Logger: logger,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = service.close(time.Second) }()
	httpsServer := httptest.NewTLSServer(newHTTPHandler(service))
	defer httpsServer.Close()
	body := []byte(`{"schema_version":1,"enrollment_token":"` + testEnrollmentToken + `","device_digest":"sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef","display_name":"PC02\\planner"}`)

	// 迁移 adapter 故意把明文密码塞进错误，验证后台任务日志仍不泄漏。
	migrator.leakCredential = true
	response, err := httpsServer.Client().Post(httpsServer.URL+"/v1/enroll", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	var pending enrollResponse
	if err := json.NewDecoder(response.Body).Decode(&pending); err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	if response.StatusCode != http.StatusAccepted || pending.Credential != nil {
		t.Fatalf("initial response status=%d body=%+v", response.StatusCode, pending)
	}
	waitForRegistryState(t, registry, StateMigrationFailed)
	record := registry.snapshot()
	if record.WorkspaceID == "" {
		t.Fatal("workspace was not persisted")
	}
	access, err := service.workspaceAccess(record)
	if err != nil {
		t.Fatal(err)
	}
	leakedPassword := access.MigrationPassword
	pendingJSON, _ := json.Marshal(pending)
	if strings.Contains(string(pendingJSON), leakedPassword) || strings.Contains(logger.contents(), leakedPassword) {
		t.Fatalf("credential leaked: body=%s log=%s", pendingJSON, logger.contents())
	}

	failedCalls := migrator.callCount()
	response, err = httpsServer.Client().Post(httpsServer.URL+"/v1/enroll", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	var terminal enrollResponse
	if err := json.NewDecoder(response.Body).Decode(&terminal); err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	if response.StatusCode != http.StatusConflict || terminal.State != StateMigrationFailed ||
		terminal.RetryAfterMS != 0 || terminal.Credential != nil {
		t.Fatalf("terminal migration response status=%d body=%+v", response.StatusCode, terminal)
	}
	time.Sleep(20 * time.Millisecond)
	if migrator.callCount() != failedCalls {
		t.Fatalf("MIGRATION_FAILED was automatically retried: before=%d after=%d", failedCalls, migrator.callCount())
	}
	// 代表管理命令的显式 CAS；只有重置回 PROVISIONING 后 enroll 才可启动新 worker。
	if err := registry.TransitionState(context.Background(), record.WorkspaceID,
		[]State{StateMigrationFailed}, StateProvisioning, ""); err != nil {
		t.Fatal(err)
	}
	migrator.setError(nil)
	ready := pollUntilReady(t, httpsServer.Client(), httpsServer.URL+"/v1/enroll", body)
	if ready.WorkspaceID != record.WorkspaceID || ready.Credential == nil || ready.Credential.Password != access.AppPassword {
		t.Fatalf("retry did not converge: %+v", ready)
	}
	if migrator.callCount() < 2 {
		t.Fatalf("migration calls=%d, failure was not retried", migrator.callCount())
	}
}

func TestSecondReplicaCannotRetryMigrationFailedWithoutManagementCAS(t *testing.T) {
	registry := &observedWorkspaceLockRegistry{
		memoryRegistry: &memoryRegistry{},
		attempts:       make(chan struct{}, 2),
	}
	firstResources := &recordingResources{}
	firstMigrator := &blockingMigrator{
		started: make(chan struct{}),
		release: make(chan struct{}),
		err:     errors.New("forced migration failure"),
	}
	secondResources := &recordingResources{}
	secondMigrator := &recordingMigrator{}
	config := serviceConfig{
		Registry: registry, Endpoint: Endpoint{
			Host: "pandora-dev-db.intra", Port: 3306, TLSServerName: "pandora-dev-db.intra",
		},
		MasterKey:        bytes.Repeat([]byte{0x42}, 32),
		Random:           bytes.NewReader(bytes.Repeat([]byte{0x33}, 16384)),
		ProvisionTimeout: time.Minute,
	}
	firstConfig := config
	firstConfig.Resources = firstResources
	firstConfig.Migrator = firstMigrator
	firstService, err := newService(firstConfig)
	if err != nil {
		t.Fatal(err)
	}
	secondConfig := config
	secondConfig.Resources = secondResources
	secondConfig.Migrator = secondMigrator
	secondService, err := newService(secondConfig)
	if err != nil {
		t.Fatal(err)
	}
	var releaseOnce sync.Once
	t.Cleanup(func() {
		releaseOnce.Do(func() { close(firstMigrator.release) })
		_ = firstService.close(time.Second)
		_ = secondService.close(time.Second)
	})

	digest := sha256.Sum256([]byte("two-replica-terminal-race"))
	record, err := firstService.newWorkspaceCandidate(digest, "PC02\\planner")
	if err != nil {
		t.Fatal(err)
	}
	registry.workspace = record
	if !firstService.tryStartProvision(record.WorkspaceID) {
		t.Fatal("first replica did not start worker")
	}
	select {
	case <-registry.attempts:
	case <-time.After(time.Second):
		t.Fatal("first replica did not attempt workspace lock")
	}
	select {
	case <-firstMigrator.started:
	case <-time.After(time.Second):
		t.Fatal("first replica did not enter migration")
	}
	if !secondService.tryStartProvision(record.WorkspaceID) {
		t.Fatal("second replica did not queue worker")
	}
	select {
	case <-registry.attempts:
		// 第二副本已经排在同一 workspace lock 后面。
	case <-time.After(time.Second):
		t.Fatal("second replica did not attempt workspace lock")
	}
	releaseOnce.Do(func() { close(firstMigrator.release) })
	if err := firstService.close(time.Second); err != nil {
		t.Fatal(err)
	}
	if err := secondService.close(time.Second); err != nil {
		t.Fatal(err)
	}

	if got := registry.snapshot().State; got != StateMigrationFailed {
		t.Fatalf("second replica changed terminal state to %s", got)
	}
	if got := firstResources.callCount(); got != 1 {
		t.Fatalf("first replica EnsureWorkspace calls=%d", got)
	}
	if got := secondResources.callCount(); got != 0 {
		t.Fatalf("second replica retried EnsureWorkspace: calls=%d", got)
	}
	if got := secondMigrator.callCount(); got != 0 {
		t.Fatalf("second replica retried migration: calls=%d", got)
	}
}

func TestFailedTerminalCASLeavesPersistentFenceAndNextWorkerOnlyConfirmsFailure(t *testing.T) {
	registry := &memoryRegistry{failNextTerminalTransition: true}
	firstResources := &recordingResources{}
	firstMigrator := &recordingMigrator{err: errors.New("forced migration failure")}
	firstService := newTestService(t, registry, firstResources, firstMigrator, 0x51)
	secondResources := &recordingResources{}
	secondMigrator := &recordingMigrator{}
	secondService := newTestService(t, registry, secondResources, secondMigrator, 0x52)
	t.Cleanup(func() {
		_ = firstService.close(time.Second)
		_ = secondService.close(time.Second)
	})

	digest := sha256.Sum256([]byte("terminal-cas-unknown"))
	record, err := firstService.newWorkspaceCandidate(digest, "PC03\\planner")
	if err != nil {
		t.Fatal(err)
	}
	registry.workspace = record
	if !firstService.tryStartProvision(record.WorkspaceID) {
		t.Fatal("first worker did not start")
	}
	waitForAttemptStatus(t, registry, AttemptStatusRunning)
	if err := firstService.close(time.Second); err != nil {
		t.Fatal(err)
	}
	unknown := registry.snapshot()
	if unknown.State != StateMigrating || unknown.AttemptStatus != AttemptStatusRunning || unknown.OperationEpoch == 0 {
		t.Fatalf("failed terminal CAS lost persistent fence: %+v", unknown)
	}

	if !secondService.tryStartProvision(record.WorkspaceID) {
		t.Fatal("second worker did not start")
	}
	if err := secondService.close(time.Second); err != nil {
		t.Fatal(err)
	}
	terminal := registry.snapshot()
	if terminal.State != StateMigrationFailed || terminal.AttemptStatus != AttemptStatusFailed {
		t.Fatalf("next lock owner did not confirm terminal state: %+v", terminal)
	}
	if secondResources.callCount() != 0 || secondMigrator.callCount() != 0 {
		t.Fatalf("fenced unknown attempt reran DDL: ensure=%d migrate=%d", secondResources.callCount(), secondMigrator.callCount())
	}
}

func newTestService(t *testing.T, registry Registry, resources ResourceManager, migrator MigrationRunner, randomByte byte) *service {
	t.Helper()
	result, err := newService(serviceConfig{
		Registry: registry, Resources: resources, Migrator: migrator,
		Endpoint:  Endpoint{Host: "pandora-dev-db.intra", Port: 3306, TLSServerName: "pandora-dev-db.intra"},
		MasterKey: bytes.Repeat([]byte{0x42}, 32), Random: bytes.NewReader(bytes.Repeat([]byte{randomByte}, 16384)),
		ProvisionTimeout: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func waitForAttemptStatus(t *testing.T, registry *memoryRegistry, status AttemptStatus) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if registry.snapshot().AttemptStatus == status {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("attempt status did not become %s: %+v", status, registry.snapshot())
}

type blockingMigrator struct {
	started chan struct{}
	release chan struct{}
	once    sync.Once
	mu      sync.Mutex
	calls   int
	err     error
}

func (m *blockingMigrator) MigrateWorkspace(ctx context.Context, _ WorkspaceAccess) error {
	m.mu.Lock()
	m.calls++
	m.mu.Unlock()
	m.once.Do(func() { close(m.started) })
	select {
	case <-m.release:
		return m.err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func TestEnrollReturnsAcceptedImmediatelyThenSameTokenPollsToReady(t *testing.T) {
	tokenHash := sha256.Sum256([]byte(testEnrollmentToken))
	registry := &memoryRegistry{tokenHash: tokenHash}
	resources := &recordingResources{}
	migrator := &blockingMigrator{started: make(chan struct{}), release: make(chan struct{})}
	service, err := newService(serviceConfig{
		Registry:  registry,
		Resources: resources,
		Migrator:  migrator,
		Endpoint: Endpoint{
			Host:          "pandora-dev-db.intra",
			Port:          3306,
			TLSServerName: "pandora-dev-db.intra",
		},
		MasterKey:        bytes.Repeat([]byte{0x42}, 32),
		Random:           bytes.NewReader(bytes.Repeat([]byte{0x11}, 4096)),
		ProvisionTimeout: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = service.close(time.Second) }()

	httpsServer := httptest.NewTLSServer(newHTTPHandler(service))
	defer httpsServer.Close()
	client := httpsServer.Client()
	client.Timeout = 250 * time.Millisecond
	body := []byte(`{"schema_version":1,"enrollment_token":"` + testEnrollmentToken + `","device_digest":"sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef","display_name":"PC02\\planner"}`)
	response, err := client.Post(httpsServer.URL+"/v1/enroll", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("enroll 被后台迁移阻塞：%v", err)
	}
	if response.StatusCode != http.StatusAccepted || response.Header.Get("Retry-After") != "2" {
		t.Fatalf("status=%d", response.StatusCode)
	}
	var first enrollResponse
	if err := json.NewDecoder(response.Body).Decode(&first); err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	if first.SchemaVersion != 1 || first.State != StateProvisioning || first.RetryAfterMS != 2000 || first.Credential != nil {
		t.Fatalf("pending response=%+v", first)
	}
	if first.WorkspaceID == "" || first.Endpoint.Host != "pandora-dev-db.intra" || len(first.Databases) != 10 {
		t.Fatalf("pending identity/endpoint/databases=%+v", first)
	}
	select {
	case <-migrator.started:
	case <-time.After(time.Second):
		t.Fatal("background migration did not start")
	}

	response, err = client.Post(httpsServer.URL+"/v1/enroll", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	var second enrollResponse
	if err := json.NewDecoder(response.Body).Decode(&second); err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	if response.StatusCode != http.StatusAccepted || second.WorkspaceID != first.WorkspaceID || second.Credential != nil {
		t.Fatalf("in-flight retry status=%d response=%+v", response.StatusCode, second)
	}

	close(migrator.release)
	client.Timeout = time.Second
	got := pollUntilReady(t, client, httpsServer.URL+"/v1/enroll", body)
	if got.WorkspaceID != first.WorkspaceID || got.Credential == nil || got.Credential.Username == "" || got.Credential.Password == "" {
		t.Fatalf("READY response missing identity/credential: %+v", got)
	}
	if got.Credential.Version != 1 || got.RetryAfterMS != 0 {
		t.Fatalf("credential/retry=%+v/%d", got.Credential, got.RetryAfterMS)
	}
	if resources.callCount() != 1 {
		t.Fatalf("resources=%d", resources.callCount())
	}
	wantStates := []State{StateProvisioning, StateMigrating, StateReady}
	states := registry.stateHistory()
	if len(states) != len(wantStates) {
		t.Fatalf("states=%v", states)
	}
	for i := range wantStates {
		if states[i] != wantStates[i] {
			t.Fatalf("states=%v", states)
		}
	}
}

func waitForRegistryState(t *testing.T, registry *memoryRegistry, want State) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if registry.snapshot().State == want {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("workspace state=%s, want=%s", registry.snapshot().State, want)
}

func pollUntilReady(t *testing.T, client *http.Client, url string, body []byte) enrollResponse {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		response, err := client.Post(url, "application/json", bytes.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		var result enrollResponse
		if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
			_ = response.Body.Close()
			t.Fatal(err)
		}
		_ = response.Body.Close()
		switch response.StatusCode {
		case http.StatusOK:
			if result.State != StateReady || result.Credential == nil {
				t.Fatalf("invalid READY response=%+v", result)
			}
			return result
		case http.StatusAccepted:
			if result.Credential != nil || result.RetryAfterMS == 0 {
				t.Fatalf("invalid pending response=%+v", result)
			}
		default:
			t.Fatalf("poll status=%d response=%+v", response.StatusCode, result)
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("workspace did not become READY")
	return enrollResponse{}
}
