package plannerdb

import (
	"context"
	"errors"
	"io"
	"time"
)

const schemaVersion = 1

type State string

type AttemptStatus string

const (
	StateProvisioning    State = "PROVISIONING"
	StateMigrating       State = "MIGRATING"
	StateReady           State = "READY"
	StateMigrationFailed State = "MIGRATION_FAILED"

	AttemptStatusIdle      AttemptStatus = "IDLE"
	AttemptStatusRunning   AttemptStatus = "RUNNING"
	AttemptStatusSucceeded AttemptStatus = "SUCCEEDED"
	AttemptStatusFailed    AttemptStatus = "FAILED"
)

var (
	ErrInvalidRequest         = errors.New("enrollment 请求参数非法")
	ErrInvalidEnrollmentToken = errors.New("enrollment token 无效或已被其他设备使用")
	ErrDeviceAlreadyEnrolled  = errors.New("device identity 已绑定其他 enrollment token")
	ErrWorkspaceNotFailed     = errors.New("workspace 不处于 MIGRATION_FAILED")
	ErrWorkspaceCollision     = errors.New("workspace ID 碰撞")
	ErrProvisionInProgress    = errors.New("workspace 正由另一请求初始化")
	ErrProvisionCapacity      = errors.New("中心 provisioner 当前已达并发上限")
	ErrProvisionFailed        = errors.New("workspace 资源初始化失败")
	ErrMigrationFailed        = errors.New("workspace schema 迁移失败")
	ErrAttemptOutcomeUnknown  = errors.New("上一次 workspace attempt 结果未知，必须先落终态")
	ErrWorkspaceLockLost      = errors.New("workspace advisory lock 已丢失")
)

type Endpoint struct {
	Host          string `json:"host"`
	Port          uint16 `json:"port"`
	TLSServerName string `json:"tls_server_name"`
}

type SecretBox struct {
	Nonce      []byte
	Ciphertext []byte
}

type WorkspaceRecord struct {
	WorkspaceID       string
	DeviceDigest      [32]byte
	DisplayName       string
	State             State
	AppUsername       string
	AppSecret         SecretBox
	MigrationUsername string
	MigrationSecret   SecretBox
	CredentialVersion uint64
	FailureCode       string
	OperationEpoch    uint64
	AttemptStatus     AttemptStatus
}

type EnrollmentClaim struct {
	TokenHash           [32]byte
	DeviceDigest        [32]byte
	DisplayName         string
	ExpectedWorkspaceID string
	Candidate           WorkspaceRecord
}

type TokenPurpose string

const (
	TokenPurposeEnroll   TokenPurpose = "ENROLL"
	TokenPurposeRecovery TokenPurpose = "RECOVERY"
)

type TokenGrant struct {
	TokenHash         [32]byte
	Purpose           TokenPurpose
	TargetWorkspaceID string
	ExpiresAt         time.Time
}

type WorkspaceAccess struct {
	WorkspaceID       string
	AppUsername       string
	AppPassword       string
	MigrationUsername string
	MigrationPassword string
	CredentialVersion uint64
	Endpoint          Endpoint
	Databases         map[string]string
}

type Registry interface {
	ClaimEnrollment(context.Context, EnrollmentClaim) (WorkspaceRecord, error)
	WithWorkspaceLock(context.Context, string, time.Duration, func(context.Context) error) (bool, error)
	GetWorkspace(context.Context, string) (WorkspaceRecord, error)
	BeginProvisionAttempt(context.Context, string) (WorkspaceRecord, error)
	TransitionAttemptState(context.Context, string, uint64, []State, State, string, AttemptStatus) error
	TransitionState(context.Context, string, []State, State, string) error
	IssueTokenGrant(context.Context, TokenGrant) error
}

type ResourceManager interface {
	EnsureWorkspace(context.Context, WorkspaceAccess) error
}

type MigrationRunner interface {
	MigrateWorkspace(context.Context, WorkspaceAccess) error
}

type safeLogger interface {
	Printf(string, ...any)
}

type serviceConfig struct {
	Registry                Registry
	Resources               ResourceManager
	Migrator                MigrationRunner
	Endpoint                Endpoint
	MasterKey               []byte
	Random                  io.Reader
	ProvisionTimeout        time.Duration
	WorkspaceLockWait       time.Duration
	Logger                  safeLogger
	MaxConcurrentProvisions int
}

type enrollRequest struct {
	SchemaVersion       int    `json:"schema_version"`
	EnrollmentToken     string `json:"enrollment_token"`
	DeviceDigest        string `json:"device_digest"`
	DisplayName         string `json:"display_name"`
	ExpectedWorkspaceID string `json:"expected_workspace_id,omitempty"`
}

type credentialResponse struct {
	Username string `json:"username"`
	Password string `json:"password"`
	Version  uint64 `json:"version"`
}

type enrollResponse struct {
	SchemaVersion int                 `json:"schema_version"`
	WorkspaceID   string              `json:"workspace_id"`
	State         State               `json:"state"`
	Endpoint      Endpoint            `json:"endpoint"`
	Databases     map[string]string   `json:"databases"`
	RetryAfterMS  uint32              `json:"retry_after_ms,omitempty"`
	Credential    *credentialResponse `json:"credential,omitempty"`
}
