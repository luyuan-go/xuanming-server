package plannerdb

import (
	"context"
	"crypto/cipher"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"regexp"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/luyuancpp/pandora/tools/migrate/workspacedb"
)

var dnsNamePattern = regexp.MustCompile(`^[a-z0-9](?:[a-z0-9.-]{0,251}[a-z0-9])?$`)

type service struct {
	registry          Registry
	resources         ResourceManager
	migrator          MigrationRunner
	endpoint          Endpoint
	gcm               cipher.AEAD
	random            io.Reader
	provisionTimeout  time.Duration
	workspaceLockWait time.Duration
	logger            safeLogger
	provisionSlots    chan struct{}
	cryptoMu          sync.Mutex
	lifecycleCtx      context.Context
	cancelLifecycle   context.CancelFunc
	workerMu          sync.Mutex
	activeWorkspaces  map[string]struct{}
	workers           sync.WaitGroup
	closing           bool
	closeOnce         sync.Once
	workersDone       chan struct{}
}

func newService(config serviceConfig) (*service, error) {
	if config.Registry == nil || config.Resources == nil || config.Migrator == nil {
		return nil, errors.New("registry/resources/migrator 必须全部提供")
	}
	if err := validateEndpoint(config.Endpoint); err != nil {
		return nil, err
	}
	gcm, err := newGCM(config.MasterKey)
	if err != nil {
		return nil, err
	}
	if config.Random == nil {
		return nil, errors.New("安全随机源不能为空")
	}
	if config.ProvisionTimeout <= 0 {
		return nil, errors.New("provision timeout 必须大于 0")
	}
	if config.WorkspaceLockWait <= 0 {
		config.WorkspaceLockWait = 5 * time.Second
	}
	if config.MaxConcurrentProvisions <= 0 {
		config.MaxConcurrentProvisions = 2
	}
	if config.MaxConcurrentProvisions > 32 {
		return nil, errors.New("并发 workspace provisioning 上限不能超过 32")
	}
	lifecycleCtx, cancelLifecycle := context.WithCancel(context.Background())
	return &service{
		registry:          config.Registry,
		resources:         config.Resources,
		migrator:          config.Migrator,
		endpoint:          config.Endpoint,
		gcm:               gcm,
		random:            config.Random,
		provisionTimeout:  config.ProvisionTimeout,
		workspaceLockWait: config.WorkspaceLockWait,
		logger:            config.Logger,
		provisionSlots:    make(chan struct{}, config.MaxConcurrentProvisions),
		lifecycleCtx:      lifecycleCtx,
		cancelLifecycle:   cancelLifecycle,
		activeWorkspaces:  make(map[string]struct{}),
		workersDone:       make(chan struct{}),
	}, nil
}

func validateEndpoint(endpoint Endpoint) error {
	if endpoint.Port == 0 {
		return errors.New("MySQL endpoint port 必须是 1..65535")
	}
	if endpoint.Host != strings.ToLower(strings.TrimSpace(endpoint.Host)) ||
		!dnsNamePattern.MatchString(endpoint.Host) || net.ParseIP(endpoint.Host) != nil || strings.Contains(endpoint.Host, "..") {
		return fmt.Errorf("MySQL endpoint host=%q 必须是小写 DNS 名，不能是裸 IP", endpoint.Host)
	}
	if endpoint.TLSServerName != endpoint.Host {
		return errors.New("MySQL endpoint host 必须与 tls_server_name 完全一致")
	}
	return nil
}

func parseDeviceDigest(value string) ([32]byte, error) {
	var digest [32]byte
	const prefix = "sha256:"
	if !strings.HasPrefix(value, prefix) || len(value) != len(prefix)+64 || value != strings.ToLower(value) {
		return digest, errors.New("device_digest 必须是 sha256:<64 lowercase hex>")
	}
	raw, err := hex.DecodeString(value[len(prefix):])
	if err != nil || len(raw) != len(digest) {
		return digest, errors.New("device_digest 必须是 sha256:<64 lowercase hex>")
	}
	copy(digest[:], raw)
	return digest, nil
}

func validateDisplayName(value string) error {
	if value == "" || len(value) > 255 || strings.TrimSpace(value) != value {
		return errors.New("display_name 必须是 1..255 字节且不能含首尾空白")
	}
	for _, r := range value {
		if unicode.IsControl(r) {
			return errors.New("display_name 不能含控制字符")
		}
	}
	return nil
}

func parseEnrollmentToken(value string) ([32]byte, error) {
	var hash [32]byte
	raw, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil || len(raw) != 32 || base64.RawURLEncoding.EncodeToString(raw) != value {
		return hash, errors.New("enrollment_token 格式非法")
	}
	return sha256.Sum256([]byte(value)), nil
}

func (s *service) enroll(ctx context.Context, request enrollRequest) (enrollResponse, error) {
	if request.SchemaVersion != schemaVersion {
		return enrollResponse{}, fmt.Errorf("%w: schema_version 必须是 1", ErrInvalidRequest)
	}
	tokenHash, err := parseEnrollmentToken(request.EnrollmentToken)
	if err != nil {
		return enrollResponse{}, fmt.Errorf("%w: %v", ErrInvalidRequest, err)
	}
	deviceDigest, err := parseDeviceDigest(request.DeviceDigest)
	if err != nil {
		return enrollResponse{}, fmt.Errorf("%w: %v", ErrInvalidRequest, err)
	}
	if err := validateDisplayName(request.DisplayName); err != nil {
		return enrollResponse{}, fmt.Errorf("%w: %v", ErrInvalidRequest, err)
	}
	if request.ExpectedWorkspaceID != "" {
		if err := workspacedb.ValidateWorkspaceID(request.ExpectedWorkspaceID); err != nil {
			return enrollResponse{}, fmt.Errorf("%w: expected_workspace_id 非法", ErrInvalidRequest)
		}
	}

	var record WorkspaceRecord
	for attempt := 0; attempt < 3; attempt++ {
		candidate, candidateErr := s.newWorkspaceCandidate(deviceDigest, request.DisplayName)
		if candidateErr != nil {
			return enrollResponse{}, candidateErr
		}
		record, err = s.registry.ClaimEnrollment(ctx, EnrollmentClaim{
			TokenHash: tokenHash, DeviceDigest: deviceDigest, DisplayName: request.DisplayName,
			ExpectedWorkspaceID: request.ExpectedWorkspaceID, Candidate: candidate,
		})
		if !errors.Is(err, ErrWorkspaceCollision) {
			break
		}
	}
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return enrollResponse{}, ctxErr
		}
		return enrollResponse{}, err
	}
	if record.State == StateReady {
		return s.readyResponse(record)
	}
	if record.State == StateMigrationFailed {
		return s.nonReadyResponse(record, false)
	}

	// 只把已持久化的 workspace ID 交给服务生命周期 worker；请求 ctx/token/
	// transport metadata 都不进入后台任务。容量已满时也不消费第二次 token，后续轮询再启动。
	s.tryStartProvision(record.WorkspaceID)
	return s.nonReadyResponse(record, true)
}

func (s *service) tryStartProvision(workspaceID string) bool {
	s.workerMu.Lock()
	defer s.workerMu.Unlock()
	if s.closing {
		return false
	}
	if _, active := s.activeWorkspaces[workspaceID]; active {
		return true
	}
	select {
	case s.provisionSlots <- struct{}{}:
	default:
		return false
	}
	s.activeWorkspaces[workspaceID] = struct{}{}
	s.workers.Add(1)
	go s.provisionWorkspace(workspaceID)
	return true
}

func (s *service) provisionWorkspace(workspaceID string) {
	defer func() {
		<-s.provisionSlots
		s.workerMu.Lock()
		delete(s.activeWorkspaces, workspaceID)
		s.workerMu.Unlock()
		s.workers.Done()
	}()

	// 这是 service lifecycle 派生的干净 context，同时有单 workspace 硬 deadline。
	provisionCtx, cancel := context.WithTimeout(s.lifecycleCtx, s.provisionTimeout)
	defer cancel()
	locked, err := s.registry.WithWorkspaceLock(provisionCtx, workspaceID, s.workspaceLockWait, func(lockCtx context.Context) error {
		current, loadErr := s.registry.GetWorkspace(lockCtx, workspaceID)
		if loadErr != nil {
			return loadErr
		}
		switch current.State {
		case StateReady, StateMigrationFailed:
			// MIGRATION_FAILED 是管理终态。即使另一个副本已经排队并在失败后才
			// 取得 workspace lock，也不能绕过 retry-workspace 的显式 CAS。
			return nil
		case StateProvisioning, StateMigrating:
		default:
			return fmt.Errorf("workspace=%s state=%q 非法", current.WorkspaceID, current.State)
		}
		if current.AttemptStatus == AttemptStatusRunning {
			// 能取得 advisory lock 证明旧 owner 已不再持锁；RUNNING 是持久化的
			// unknown-outcome fence，只能确认终态，绝不自动重放 DDL。
			return s.confirmAttemptFailed(current.WorkspaceID, current.OperationEpoch,
				"PREVIOUS_ATTEMPT_OUTCOME_UNKNOWN", nil)
		}
		current, beginErr := s.registry.BeginProvisionAttempt(lockCtx, current.WorkspaceID)
		if beginErr != nil {
			if errors.Is(beginErr, ErrAttemptOutcomeUnknown) && current.OperationEpoch != 0 {
				return s.confirmAttemptFailed(current.WorkspaceID, current.OperationEpoch,
					"PREVIOUS_ATTEMPT_OUTCOME_UNKNOWN", beginErr)
			}
			return beginErr
		}
		access, accessErr := s.workspaceAccess(current)
		if accessErr != nil {
			return s.confirmAttemptFailed(current.WorkspaceID, current.OperationEpoch,
				"CREDENTIAL_DECRYPT_FAILED", accessErr)
		}
		if ensureErr := s.resources.EnsureWorkspace(lockCtx, access); ensureErr != nil {
			s.logFailure(current.WorkspaceID, "RESOURCE_PROVISION_FAILED")
			return s.confirmAttemptFailed(current.WorkspaceID, current.OperationEpoch,
				"RESOURCE_PROVISION_FAILED", errors.Join(ErrProvisionFailed, ensureErr))
		}
		if transitionErr := s.registry.TransitionAttemptState(lockCtx, current.WorkspaceID, current.OperationEpoch,
			[]State{StateProvisioning}, StateMigrating, "", AttemptStatusRunning); transitionErr != nil {
			return transitionErr
		}
		if migrationErr := s.migrator.MigrateWorkspace(lockCtx, access); migrationErr != nil {
			s.logFailure(current.WorkspaceID, "SCHEMA_MIGRATION_FAILED")
			failureCode := "SCHEMA_MIGRATION_FAILED"
			if errors.Is(context.Cause(lockCtx), ErrWorkspaceLockLost) {
				failureCode = "WORKSPACE_LOCK_LOST"
			}
			return s.confirmAttemptFailed(current.WorkspaceID, current.OperationEpoch,
				failureCode, errors.Join(ErrMigrationFailed, migrationErr))
		}
		if transitionErr := s.registry.TransitionAttemptState(lockCtx, current.WorkspaceID, current.OperationEpoch,
			[]State{StateMigrating}, StateReady, "", AttemptStatusSucceeded); transitionErr != nil {
			return s.confirmAttemptFailed(current.WorkspaceID, current.OperationEpoch,
				"READY_COMMIT_OUTCOME_UNKNOWN", transitionErr)
		}
		return nil
	})
	if err != nil {
		s.logFailure(workspaceID, "WORKSPACE_PROVISION_ATTEMPT_FAILED")
		return
	}
	if !locked {
		return
	}
}

func (s *service) confirmAttemptFailed(workspaceID string, operationEpoch uint64, failureCode string, cause error) error {
	// 与 HTTP/worker/lock ctx 解耦；即使请求取消、GET_LOCK 丢失或 shutdown，仍给
	// fenced 终态一次独立且有界的确认机会。失败不能吞，RUNNING fence 留给下个锁 owner。
	confirmCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err := s.registry.TransitionAttemptState(confirmCtx, workspaceID, operationEpoch,
		[]State{StateProvisioning, StateMigrating}, StateMigrationFailed, failureCode, AttemptStatusFailed)
	if err != nil {
		s.logFailure(workspaceID, "TERMINAL_STATE_CONFIRM_FAILED")
		return errors.Join(cause, errors.New("确认 workspace 终态失败"), err)
	}
	return cause
}

func (s *service) logFailure(workspaceID, code string) {
	if s.logger != nil {
		// 故意不记 adapter error 文本：无法证明第三方 driver 不会把 DSN/密码编进 error。
		s.logger.Printf("[plannerdb-provisioner] workspace=%s code=%s", workspaceID, code)
	}
}

func (s *service) nonReadyResponse(record WorkspaceRecord, retry bool) (enrollResponse, error) {
	if !validState(record.State) || record.State == StateReady {
		return enrollResponse{}, fmt.Errorf("workspace pending state=%q 非法", record.State)
	}
	databases, err := s.databaseMap(record.WorkspaceID)
	if err != nil {
		return enrollResponse{}, err
	}
	response := enrollResponse{
		SchemaVersion: schemaVersion, WorkspaceID: record.WorkspaceID, State: record.State,
		Endpoint: s.endpoint, Databases: databases,
	}
	if retry {
		response.RetryAfterMS = 2000
	}
	return response, nil
}

func (s *service) close(timeout time.Duration) error {
	if s == nil {
		return nil
	}
	if timeout <= 0 {
		return errors.New("service shutdown timeout 必须大于 0")
	}
	s.closeOnce.Do(func() {
		s.workerMu.Lock()
		s.closing = true
		s.cancelLifecycle()
		s.workerMu.Unlock()
		go func() {
			s.workers.Wait()
			close(s.workersDone)
		}()
	})
	select {
	case <-s.workersDone:
		return nil
	case <-time.After(timeout):
		return errors.New("plannerdb provision worker 未在关闭时限内退出")
	}
}

func (s *service) newWorkspaceCandidate(deviceDigest [32]byte, displayName string) (WorkspaceRecord, error) {
	s.cryptoMu.Lock()
	defer s.cryptoMu.Unlock()
	workspaceID, err := generateWorkspaceID(s.random)
	if err != nil {
		return WorkspaceRecord{}, err
	}
	if err := workspacedb.ValidateWorkspaceID(workspaceID); err != nil {
		return WorkspaceRecord{}, err
	}
	appPassword, err := generatePassword(s.random)
	if err != nil {
		return WorkspaceRecord{}, err
	}
	migrationPassword, err := generatePassword(s.random)
	if err != nil {
		return WorkspaceRecord{}, err
	}
	const version uint64 = 1
	appSecret, err := sealSecret(s.gcm, s.random, workspaceID, "app", version, appPassword)
	if err != nil {
		return WorkspaceRecord{}, err
	}
	migrationSecret, err := sealSecret(s.gcm, s.random, workspaceID, "migration", version, migrationPassword)
	if err != nil {
		return WorkspaceRecord{}, err
	}
	return WorkspaceRecord{
		WorkspaceID: workspaceID, DeviceDigest: deviceDigest, DisplayName: displayName,
		State: StateProvisioning, AttemptStatus: AttemptStatusIdle, AppUsername: "p_app_" + workspaceID,
		AppSecret: appSecret, MigrationUsername: "p_mig_" + workspaceID,
		MigrationSecret: migrationSecret, CredentialVersion: version,
	}, nil
}

func (s *service) databaseMap(workspaceID string) (map[string]string, error) {
	migrationSets := workspacedb.CanonicalMigrationSets()
	databases := make(map[string]string, len(migrationSets))
	for _, migrationSet := range migrationSets {
		database, err := workspacedb.PhysicalDatabaseName(migrationSet, workspaceID)
		if err != nil {
			return nil, err
		}
		databases[migrationSet] = database
	}
	return databases, nil
}

func (s *service) workspaceAccess(record WorkspaceRecord) (WorkspaceAccess, error) {
	s.cryptoMu.Lock()
	defer s.cryptoMu.Unlock()
	appPassword, err := openSecret(s.gcm, record.WorkspaceID, "app", record.CredentialVersion, record.AppSecret)
	if err != nil {
		return WorkspaceAccess{}, err
	}
	migrationPassword, err := openSecret(s.gcm, record.WorkspaceID, "migration", record.CredentialVersion, record.MigrationSecret)
	if err != nil {
		return WorkspaceAccess{}, err
	}
	databases, err := s.databaseMap(record.WorkspaceID)
	if err != nil {
		return WorkspaceAccess{}, err
	}
	return WorkspaceAccess{
		WorkspaceID: record.WorkspaceID, AppUsername: record.AppUsername, AppPassword: appPassword,
		MigrationUsername: record.MigrationUsername, MigrationPassword: migrationPassword,
		CredentialVersion: record.CredentialVersion, Endpoint: s.endpoint, Databases: databases,
	}, nil
}

func (s *service) readyResponse(record WorkspaceRecord) (enrollResponse, error) {
	if record.State != StateReady {
		return enrollResponse{}, fmt.Errorf("workspace state=%s，尚未 READY", record.State)
	}
	access, err := s.workspaceAccess(record)
	if err != nil {
		return enrollResponse{}, err
	}
	return enrollResponse{
		SchemaVersion: schemaVersion, WorkspaceID: record.WorkspaceID, State: StateReady,
		Endpoint: s.endpoint, Databases: access.Databases,
		Credential: &credentialResponse{Username: access.AppUsername, Password: access.AppPassword, Version: access.CredentialVersion},
	}, nil
}
