package plannerdb

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"time"
)

const maxEnrollBodyBytes = 16 << 10

const (
	defaultMaxConcurrentEnrollRequests = 64
	defaultEnrollRequestTimeout        = 10 * time.Second
)

type errorEnvelope struct {
	SchemaVersion int           `json:"schema_version"`
	Error         errorResponse `json:"error"`
}

type errorResponse struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func newHTTPHandler(service *service) http.Handler {
	return newHTTPHandlerWithLimits(service, defaultMaxConcurrentEnrollRequests, defaultEnrollRequestTimeout)
}

func newHTTPHandlerWithLimits(service *service, maximumConcurrent int, requestTimeout time.Duration) http.Handler {
	if service == nil || maximumConcurrent <= 0 || requestTimeout <= 0 {
		panic("plannerdb HTTP handler 配置非法")
	}
	requestSlots := make(chan struct{}, maximumConcurrent)
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/enroll", func(writer http.ResponseWriter, request *http.Request) {
		select {
		case requestSlots <- struct{}{}:
			defer func() { <-requestSlots }()
		default:
			writer.Header().Set("Retry-After", "2")
			writeAPIError(writer, http.StatusServiceUnavailable, "ENROLL_CAPACITY", "中心 enrollment 并发已满，请稍后重试")
			return
		}
		requestCtx, cancel := context.WithTimeout(request.Context(), requestTimeout)
		defer cancel()
		request = request.WithContext(requestCtx)
		if request.TLS == nil {
			writeAPIError(writer, http.StatusUpgradeRequired, "HTTPS_REQUIRED", "该接口只接受 HTTPS")
			return
		}
		if request.Method != http.MethodPost {
			writer.Header().Set("Allow", http.MethodPost)
			writeAPIError(writer, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "只允许 POST")
			return
		}
		mediaType, _, err := mime.ParseMediaType(request.Header.Get("Content-Type"))
		if err != nil || mediaType != "application/json" {
			writeAPIError(writer, http.StatusUnsupportedMediaType, "JSON_REQUIRED", "Content-Type 必须是 application/json")
			return
		}
		request.Body = http.MaxBytesReader(writer, request.Body, maxEnrollBodyBytes)
		decoder := json.NewDecoder(request.Body)
		decoder.DisallowUnknownFields()
		var input enrollRequest
		if err := decoder.Decode(&input); err != nil {
			writeAPIError(writer, http.StatusBadRequest, "INVALID_REQUEST", "请求 JSON 非法")
			return
		}
		if err := ensureJSONEOF(decoder); err != nil {
			writeAPIError(writer, http.StatusBadRequest, "INVALID_REQUEST", "请求 JSON 只能包含一个对象")
			return
		}
		result, err := service.enroll(request.Context(), input)
		if err != nil {
			writeEnrollError(writer, err)
			return
		}
		writer.Header().Set("Content-Type", "application/json; charset=utf-8")
		writer.Header().Set("Cache-Control", "no-store")
		writer.Header().Set("X-Content-Type-Options", "nosniff")
		status := http.StatusOK
		switch result.State {
		case StateMigrationFailed:
			status = http.StatusConflict
		case StateProvisioning, StateMigrating:
			status = http.StatusAccepted
			writer.Header().Set("Retry-After", "2")
		}
		writer.WriteHeader(status)
		_ = json.NewEncoder(writer).Encode(result)
	})
	return mux
}

func ensureJSONEOF(decoder *json.Decoder) error {
	var extra any
	err := decoder.Decode(&extra)
	if errors.Is(err, io.EOF) {
		return nil
	}
	if err == nil {
		return errors.New("存在多余 JSON")
	}
	return err
}

func writeEnrollError(writer http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled):
		writer.Header().Set("Retry-After", "2")
		writeAPIError(writer, http.StatusServiceUnavailable, "ENROLL_TIMEOUT", "enrollment 请求超时，请用同一 token 重试")
	case errors.Is(err, ErrInvalidRequest):
		writeAPIError(writer, http.StatusBadRequest, "INVALID_REQUEST", "enrollment 请求参数非法")
	case errors.Is(err, ErrInvalidEnrollmentToken):
		writeAPIError(writer, http.StatusUnauthorized, "INVALID_ENROLLMENT_TOKEN", "enrollment token 无效、过期或已被使用")
	case errors.Is(err, ErrDeviceAlreadyEnrolled):
		writeAPIError(writer, http.StatusConflict, "DEVICE_ALREADY_ENROLLED", "该设备身份已登记，禁止用新 token 自动认领；请联系管理员恢复")
	case errors.Is(err, ErrProvisionInProgress):
		writer.Header().Set("Retry-After", "5")
		writeAPIError(writer, http.StatusConflict, "PROVISION_IN_PROGRESS", "workspace 正在初始化，请稍后用同一 token 重试")
	case errors.Is(err, ErrProvisionCapacity):
		writer.Header().Set("Retry-After", "10")
		writeAPIError(writer, http.StatusServiceUnavailable, "PROVISION_CAPACITY", "中心 provisioner 当前繁忙，请用同一 token 稍后重试")
	case errors.Is(err, ErrMigrationFailed):
		// 防御性映射：正常异步路径会由持久化 state 返回带 workspace 的 409；
		// 即使未来 adapter 直接上抛，也绝不能暗示客户端自动重跑 DDL。
		writeAPIError(writer, http.StatusConflict, "MIGRATION_FAILED", "workspace schema 迁移失败，请联系管理员显式恢复")
	case errors.Is(err, ErrProvisionFailed):
		writer.Header().Set("Retry-After", "10")
		writeAPIError(writer, http.StatusServiceUnavailable, "PROVISION_FAILED", "workspace 资源初始化失败，请稍后重试")
	default:
		writer.Header().Set("Retry-After", "10")
		writeAPIError(writer, http.StatusInternalServerError, "INTERNAL_ERROR", "中心 provisioner 暂时无法完成请求")
	}
}

func writeAPIError(writer http.ResponseWriter, status int, code, message string) {
	writer.Header().Set("Content-Type", "application/json; charset=utf-8")
	writer.Header().Set("Cache-Control", "no-store")
	writer.Header().Set("X-Content-Type-Options", "nosniff")
	writer.WriteHeader(status)
	_ = json.NewEncoder(writer).Encode(errorEnvelope{
		SchemaVersion: schemaVersion,
		Error:         errorResponse{Code: code, Message: message},
	})
}
