// Package internalrpcauth authenticates narrowly scoped east-west unary RPCs.
//
// It is intentionally separate from player JWT, DS callback and placement
// proof trust domains. A signature binds the caller identity, exact gRPC
// method, request subject, timestamp and random nonce. The verifier consumes
// the nonce in a shared replay store before allowing the handler to run.
package internalrpcauth

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"
	"unicode"

	"google.golang.org/grpc/metadata"
)

const (
	CallerMetadataKey    = "x-pandora-service-caller"
	AudienceMetadataKey  = "x-pandora-service-audience"
	TimestampMetadataKey = "x-pandora-service-timestamp-ms"
	NonceMetadataKey     = "x-pandora-service-nonce"
	SignatureMetadataKey = "x-pandora-service-signature"
	PayloadMetadataKey   = "x-pandora-service-payload-sha256"

	protocolVersion        = "pandora-internal-rpc-v1"
	payloadProtocolVersion = "pandora-internal-rpc-payload-v1"
	nonceBytes             = 24
)

var (
	ErrUnauthorized = errors.New("internal RPC authentication failed")
	ErrReplay       = errors.New("internal RPC authentication replayed")
	ErrUnavailable  = errors.New("internal RPC replay authority unavailable")
)

// ReplayStore atomically consumes a signed nonce. Implementations must be
// shared by all replicas of the protected service; process-local caches are
// not sufficient for replay protection behind a load balancer.
type ReplayStore interface {
	Consume(context.Context, string, time.Duration) (bool, error)
}

type Signer struct {
	secret   []byte
	caller   string
	audience string
	now      func() time.Time
	random   io.Reader
}

// NewSigner constructs a caller signer. Secret must be a distinct, random
// service-auth key with at least 32 bytes of entropy.
func NewSigner(secret, caller, audience string) (*Signer, error) {
	if err := validateSecretAndIdentities(secret, caller, audience); err != nil {
		return nil, err
	}
	return &Signer{secret: []byte(secret), caller: caller, audience: audience, now: time.Now, random: rand.Reader}, nil
}

// SignContext replaces this protocol's outgoing metadata with one fresh,
// request-bound credential. It never appends duplicate values.
func (s *Signer) SignContext(ctx context.Context, fullMethod string, subject uint64) (context.Context, error) {
	return s.signContext(ctx, fullMethod, subject, "")
}

// SignContextWithPayload additionally binds an application-defined canonical
// payload to the service identity. Callers must pass exactly the same bytes to
// Verifier.VerifyWithPayload; this is intended for destructive exact-identity
// control-plane RPCs whose request body must not be mutable independently of
// the authenticated method/subject.
func (s *Signer) SignContextWithPayload(
	ctx context.Context,
	fullMethod string,
	subject uint64,
	payload []byte,
) (context.Context, error) {
	if len(payload) == 0 {
		return nil, ErrUnauthorized
	}
	digest := sha256.Sum256(payload)
	return s.signContext(ctx, fullMethod, subject, base64.RawURLEncoding.EncodeToString(digest[:]))
}

func (s *Signer) signContext(ctx context.Context, fullMethod string, subject uint64, payloadDigest string) (context.Context, error) {
	if s == nil || len(s.secret) < 32 || s.caller == "" || s.audience == "" || fullMethod == "" || subject == 0 {
		return nil, ErrUnauthorized
	}
	nonceRaw := make([]byte, nonceBytes)
	if _, err := io.ReadFull(s.random, nonceRaw); err != nil {
		return nil, fmt.Errorf("generate internal RPC nonce: %w", err)
	}
	timestamp := s.now().UTC().UnixMilli()
	nonce := base64.RawURLEncoding.EncodeToString(nonceRaw)
	message := canonical(s.caller, s.audience, fullMethod, subject, timestamp, nonce)
	if payloadDigest != "" {
		message = canonicalPayload(s.caller, s.audience, fullMethod, subject, timestamp, nonce, payloadDigest)
	}
	signature := sign(s.secret, message)

	md, _ := metadata.FromOutgoingContext(ctx)
	md = md.Copy()
	// Never forward a player credential into a system-only hop. Matchmaker's
	// independent player-JWT guard would reject it, but stripping here avoids a
	// confused-deputy failure when callers reuse a context with outgoing data.
	md.Delete("authorization")
	md.Delete("x-pandora-player-id")
	md.Set(CallerMetadataKey, s.caller)
	md.Set(AudienceMetadataKey, s.audience)
	md.Set(TimestampMetadataKey, strconv.FormatInt(timestamp, 10))
	md.Set(NonceMetadataKey, nonce)
	md.Set(SignatureMetadataKey, signature)
	if payloadDigest == "" {
		md.Delete(PayloadMetadataKey)
	} else {
		md.Set(PayloadMetadataKey, payloadDigest)
	}
	return metadata.NewOutgoingContext(ctx, md), nil
}

type Verifier struct {
	secret           []byte
	expectedCaller   string
	expectedAudience string
	maxClockSkew     time.Duration
	replayTTL        time.Duration
	replays          ReplayStore
	now              func() time.Time
}

// NewVerifier constructs a fail-closed verifier. A shared replay store is
// mandatory; without it a valid credential could be replayed against another
// replica during the clock-skew window.
func NewVerifier(secret, expectedCaller, expectedAudience string, maxClockSkew time.Duration, replays ReplayStore) (*Verifier, error) {
	if err := validateSecretAndIdentities(secret, expectedCaller, expectedAudience); err != nil {
		return nil, err
	}
	if maxClockSkew <= 0 || maxClockSkew > 5*time.Minute {
		return nil, errors.New("internal RPC max clock skew must be within (0,5m]")
	}
	if replays == nil {
		return nil, errors.New("internal RPC shared replay store is required")
	}
	return &Verifier{
		secret:           []byte(secret),
		expectedCaller:   expectedCaller,
		expectedAudience: expectedAudience,
		maxClockSkew:     maxClockSkew,
		// A credential timestamp may be maxClockSkew in the future and remain
		// acceptable until maxClockSkew in the past. Keep the nonce throughout
		// that entire interval, plus one second for millisecond boundaries.
		replayTTL: 2*maxClockSkew + time.Second,
		replays:   replays,
		now:       time.Now,
	}, nil
}

// Caller reports the single caller identity this verifier accepts.
func (v *Verifier) Caller() string {
	if v == nil {
		return ""
	}
	return v.expectedCaller
}

// Verify validates and consumes a request credential before protected code is
// allowed to read state or mint a downstream credential.
func (v *Verifier) Verify(ctx context.Context, fullMethod string, subject uint64) error {
	return v.verify(ctx, fullMethod, subject, "")
}

// MultiCallerVerifier accepts one protected method from several caller
// services, each holding an **independent** key. It exists because collapsing
// two callers onto one shared secret would merge their trust domains: whoever
// holds the key can impersonate the other caller.
//
// Dispatching on the (unauthenticated) caller header is safe because the caller
// identity is signed into the canonical message and each Verifier re-compares
// it against its own expectedCaller. A forged header only routes the request to
// a key that cannot produce a matching signature, so it fails closed. Nonces are
// already namespaced by caller in the replay store, so no two verifiers can
// consume each other's nonce.
type MultiCallerVerifier struct {
	byCaller map[string]*Verifier
}

// NewMultiCallerVerifier indexes verifiers by their accepted caller identity.
// Duplicate callers are rejected: silently keeping one of two keys for the same
// identity would make a rotation look applied when it is not.
func NewMultiCallerVerifier(verifiers ...*Verifier) (*MultiCallerVerifier, error) {
	byCaller := make(map[string]*Verifier, len(verifiers))
	for _, v := range verifiers {
		if v == nil {
			return nil, errors.New("internal RPC multi-caller verifier: nil verifier")
		}
		caller := v.Caller()
		if caller == "" {
			return nil, errors.New("internal RPC multi-caller verifier: empty caller identity")
		}
		if _, dup := byCaller[caller]; dup {
			return nil, fmt.Errorf("internal RPC multi-caller verifier: duplicate caller %q", caller)
		}
		byCaller[caller] = v
	}
	if len(byCaller) == 0 {
		return nil, errors.New("internal RPC multi-caller verifier: at least one verifier is required")
	}
	return &MultiCallerVerifier{byCaller: byCaller}, nil
}

// Verify routes to the verifier registered for the request's caller identity.
// An unknown caller is rejected without consuming any nonce.
func (m *MultiCallerVerifier) Verify(ctx context.Context, fullMethod string, subject uint64) error {
	if m == nil || len(m.byCaller) == 0 {
		return ErrUnauthorized
	}
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ErrUnauthorized
	}
	caller, ok := single(md, CallerMetadataKey)
	if !ok {
		return ErrUnauthorized
	}
	v, ok := m.byCaller[caller]
	if !ok {
		return ErrUnauthorized
	}
	return v.Verify(ctx, fullMethod, subject)
}

// VerifyWithPayload routes to the independent verifier registered for the
// signed caller, then verifies the exact canonical request payload. Unknown
// callers and malformed caller metadata are rejected before any replay nonce
// is consumed.
func (m *MultiCallerVerifier) VerifyWithPayload(
	ctx context.Context,
	fullMethod string,
	subject uint64,
	payload []byte,
) error {
	if m == nil || len(m.byCaller) == 0 {
		return ErrUnauthorized
	}
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ErrUnauthorized
	}
	caller, ok := single(md, CallerMetadataKey)
	if !ok {
		return ErrUnauthorized
	}
	v, ok := m.byCaller[caller]
	if !ok {
		return ErrUnauthorized
	}
	return v.VerifyWithPayload(ctx, fullMethod, subject, payload)
}

// VerifyWithPayload verifies the request-bound service credential and consumes
// its nonce only after the canonical payload digest also matches.
func (v *Verifier) VerifyWithPayload(
	ctx context.Context,
	fullMethod string,
	subject uint64,
	payload []byte,
) error {
	if len(payload) == 0 {
		return ErrUnauthorized
	}
	digest := sha256.Sum256(payload)
	return v.verify(ctx, fullMethod, subject, base64.RawURLEncoding.EncodeToString(digest[:]))
}

func (v *Verifier) verify(ctx context.Context, fullMethod string, subject uint64, expectedPayloadDigest string) error {
	if v == nil || len(v.secret) < 32 || v.replays == nil || fullMethod == "" || subject == 0 {
		return ErrUnauthorized
	}
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ErrUnauthorized
	}
	caller, ok := single(md, CallerMetadataKey)
	if !ok || caller != v.expectedCaller {
		return ErrUnauthorized
	}
	audience, ok := single(md, AudienceMetadataKey)
	if !ok || audience != v.expectedAudience {
		return ErrUnauthorized
	}
	tsRaw, ok := single(md, TimestampMetadataKey)
	if !ok {
		return ErrUnauthorized
	}
	timestamp, err := strconv.ParseInt(tsRaw, 10, 64)
	if err != nil || timestamp <= 0 {
		return ErrUnauthorized
	}
	nonce, ok := single(md, NonceMetadataKey)
	if !ok || !validNonce(nonce) {
		return ErrUnauthorized
	}
	signature, ok := single(md, SignatureMetadataKey)
	if !ok {
		return ErrUnauthorized
	}
	provided, err := base64.RawURLEncoding.DecodeString(signature)
	if err != nil || len(provided) != sha256.Size {
		return ErrUnauthorized
	}
	message := canonical(caller, audience, fullMethod, subject, timestamp, nonce)
	if expectedPayloadDigest != "" {
		payloadDigest, ok := single(md, PayloadMetadataKey)
		if !ok || subtle.ConstantTimeCompare([]byte(payloadDigest), []byte(expectedPayloadDigest)) != 1 {
			return ErrUnauthorized
		}
		message = canonicalPayload(caller, audience, fullMethod, subject, timestamp, nonce, payloadDigest)
	} else if values := md.Get(PayloadMetadataKey); len(values) != 0 {
		// Do not let a payload-bound credential silently downgrade through Verify.
		return ErrUnauthorized
	}
	expected, _ := base64.RawURLEncoding.DecodeString(sign(v.secret, message))
	if subtle.ConstantTimeCompare(provided, expected) != 1 {
		return ErrUnauthorized
	}

	delta := v.now().UTC().Sub(time.UnixMilli(timestamp))
	if delta > v.maxClockSkew || delta < -v.maxClockSkew {
		return ErrUnauthorized
	}
	consumed, err := v.replays.Consume(ctx, caller+":"+nonce, v.replayTTL)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrUnavailable, err)
	}
	if !consumed {
		return ErrReplay
	}
	return nil
}

func validateSecretAndIdentities(secret, caller, audience string) error {
	if err := ValidateSecret(secret); err != nil {
		return err
	}
	if err := ValidateIdentity(caller); err != nil {
		return fmt.Errorf("internal RPC caller identity is invalid: %w", err)
	}
	if err := ValidateIdentity(audience); err != nil {
		return fmt.Errorf("internal RPC audience identity is invalid: %w", err)
	}
	return nil
}

func ValidateIdentity(identity string) error {
	if strings.TrimSpace(identity) == "" || identity != strings.TrimSpace(identity) ||
		strings.ContainsAny(identity, "\r\n\t") || len(identity) > 128 {
		return errors.New("identity must be a non-empty, trimmed value no longer than 128 bytes")
	}
	return nil
}

// ValidateSecret applies the configuration-independent secret policy without
// returning or logging any part of the key.
func ValidateSecret(secret string) error {
	if len([]byte(secret)) < 32 {
		return errors.New("internal RPC auth secret must be at least 32 bytes")
	}
	for _, r := range secret {
		if unicode.IsControl(r) {
			return errors.New("internal RPC auth secret contains control characters")
		}
	}
	return nil
}

func single(md metadata.MD, key string) (string, bool) {
	values := md.Get(key)
	return func() (string, bool) {
		if len(values) != 1 || values[0] == "" {
			return "", false
		}
		return values[0], true
	}()
}

func validNonce(encoded string) bool {
	raw, err := base64.RawURLEncoding.DecodeString(encoded)
	return err == nil && len(raw) == nonceBytes
}

func canonical(caller, audience, method string, subject uint64, timestamp int64, nonce string) string {
	return strings.Join([]string{
		protocolVersion,
		caller,
		audience,
		method,
		strconv.FormatUint(subject, 10),
		strconv.FormatInt(timestamp, 10),
		nonce,
	}, "\n")
}

func canonicalPayload(
	caller, audience, method string,
	subject uint64,
	timestamp int64,
	nonce, payloadDigest string,
) string {
	return strings.Join([]string{
		payloadProtocolVersion,
		caller,
		audience,
		method,
		strconv.FormatUint(subject, 10),
		strconv.FormatInt(timestamp, 10),
		nonce,
		payloadDigest,
	}, "\n")
}

func sign(secret []byte, message string) string {
	mac := hmac.New(sha256.New, secret)
	_, _ = mac.Write([]byte(message))
	return base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}
