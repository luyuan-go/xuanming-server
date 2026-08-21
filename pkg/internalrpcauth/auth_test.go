package internalrpcauth

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"google.golang.org/grpc/metadata"
)

const (
	testSecret   = "internal-rpc-auth-test-secret-0123456789abcdef"
	testMethod   = "/pandora.match.v1.MatchService/ResolvePlayerMatchContext"
	testAudience = "matchmaker:5v5_ranked"
)

type memoryReplayStore struct {
	seen map[string]struct{}
	err  error
}

func (s *memoryReplayStore) Consume(_ context.Context, key string, _ time.Duration) (bool, error) {
	if s.err != nil {
		return false, s.err
	}
	if _, ok := s.seen[key]; ok {
		return false, nil
	}
	s.seen[key] = struct{}{}
	return true, nil
}

func signedIncoming(t *testing.T, signer *Signer, method string, subject uint64) context.Context {
	t.Helper()
	out, err := signer.SignContext(context.Background(), method, subject)
	if err != nil {
		t.Fatalf("SignContext: %v", err)
	}
	md, ok := metadata.FromOutgoingContext(out)
	if !ok {
		t.Fatal("signed context has no outgoing metadata")
	}
	return metadata.NewIncomingContext(context.Background(), md.Copy())
}

func signedIncomingWithPayload(t *testing.T, signer *Signer, method string, subject uint64, payload []byte) context.Context {
	t.Helper()
	out, err := signer.SignContextWithPayload(context.Background(), method, subject, payload)
	if err != nil {
		t.Fatalf("SignContextWithPayload: %v", err)
	}
	md, ok := metadata.FromOutgoingContext(out)
	if !ok {
		t.Fatal("payload-signed context has no outgoing metadata")
	}
	return metadata.NewIncomingContext(context.Background(), md.Copy())
}

func newAuthPair(t *testing.T) (*Signer, *Verifier, *memoryReplayStore) {
	t.Helper()
	store := &memoryReplayStore{seen: map[string]struct{}{}}
	signer, err := NewSigner(testSecret, "login", testAudience)
	if err != nil {
		t.Fatal(err)
	}
	verifier, err := NewVerifier(testSecret, "login", testAudience, 30*time.Second, store)
	if err != nil {
		t.Fatal(err)
	}
	fixed := time.Unix(1_800_000_000, 123_000_000).UTC()
	signer.now = func() time.Time { return fixed }
	signer.random = bytes.NewReader(bytes.Repeat([]byte{0x5a}, nonceBytes))
	verifier.now = func() time.Time { return fixed }
	return signer, verifier, store
}

func TestVerifyConsumesNonceAndRejectsReplay(t *testing.T) {
	signer, verifier, _ := newAuthPair(t)
	ctx := signedIncoming(t, signer, testMethod, 42)
	if err := verifier.Verify(ctx, testMethod, 42); err != nil {
		t.Fatalf("first Verify: %v", err)
	}
	if err := verifier.Verify(ctx, testMethod, 42); !errors.Is(err, ErrReplay) {
		t.Fatalf("replay error=%v, want ErrReplay", err)
	}
}

func TestVerifyBindsMethodAndSubjectBeforeReplayWrite(t *testing.T) {
	for name, verify := range map[string]func(*Verifier, context.Context) error{
		"different method":  func(v *Verifier, ctx context.Context) error { return v.Verify(ctx, "/other.Service/Read", 42) },
		"different subject": func(v *Verifier, ctx context.Context) error { return v.Verify(ctx, testMethod, 43) },
	} {
		t.Run(name, func(t *testing.T) {
			signer, verifier, store := newAuthPair(t)
			ctx := signedIncoming(t, signer, testMethod, 42)
			if err := verify(verifier, ctx); !errors.Is(err, ErrUnauthorized) {
				t.Fatalf("error=%v, want ErrUnauthorized", err)
			}
			if len(store.seen) != 0 {
				t.Fatalf("invalid signature consumed replay nonce: %d", len(store.seen))
			}
		})
	}
}

func TestVerifyBindsTargetAudience(t *testing.T) {
	signer, _, store := newAuthPair(t)
	other, err := NewVerifier(testSecret, "login", "matchmaker:pve_coop", 30*time.Second, store)
	if err != nil {
		t.Fatal(err)
	}
	ctx := signedIncoming(t, signer, testMethod, 42)
	if err := other.Verify(ctx, testMethod, 42); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("cross-audience error=%v, want ErrUnauthorized", err)
	}
	if len(store.seen) != 0 {
		t.Fatal("cross-audience request consumed replay nonce")
	}
}

func TestVerifyRejectsDuplicateMetadataAndStaleCredential(t *testing.T) {
	signer, verifier, store := newAuthPair(t)
	ctx := signedIncoming(t, signer, testMethod, 42)
	md, _ := metadata.FromIncomingContext(ctx)
	md = md.Copy()
	md.Append(CallerMetadataKey, "login")
	if err := verifier.Verify(metadata.NewIncomingContext(context.Background(), md), testMethod, 42); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("duplicate metadata error=%v", err)
	}
	if len(store.seen) != 0 {
		t.Fatal("duplicate metadata reached replay store")
	}

	signer, verifier, _ = newAuthPair(t)
	ctx = signedIncoming(t, signer, testMethod, 42)
	verifier.now = func() time.Time { return signer.now().Add(31 * time.Second) }
	if err := verifier.Verify(ctx, testMethod, 42); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("stale credential error=%v", err)
	}
}

func TestVerifyFailsClosedWhenReplayAuthorityFails(t *testing.T) {
	signer, verifier, store := newAuthPair(t)
	store.err = errors.New("redis unavailable")
	ctx := signedIncoming(t, signer, testMethod, 42)
	if err := verifier.Verify(ctx, testMethod, 42); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("error=%v, want ErrUnavailable", err)
	}
}

func TestSignerDoesNotForwardPlayerCredential(t *testing.T) {
	signer, _, _ := newAuthPair(t)
	base := metadata.NewOutgoingContext(context.Background(), metadata.Pairs(
		"authorization", "Bearer player-token", "x-pandora-player-id", "42"))
	signed, err := signer.SignContext(base, testMethod, 42)
	if err != nil {
		t.Fatal(err)
	}
	md, _ := metadata.FromOutgoingContext(signed)
	if len(md.Get("authorization")) != 0 || len(md.Get("x-pandora-player-id")) != 0 {
		t.Fatal("player credential leaked into internal RPC metadata")
	}
}

func TestVerifyWithPayloadBindsCanonicalBodyBeforeReplayWrite(t *testing.T) {
	signer, verifier, store := newAuthPair(t)
	payload := []byte("pandora-battle-allocation-abort-v1\n42\nop-a\npod-a\nuid-a\n1\nalloc-a\nstable")
	ctx := signedIncomingWithPayload(t, signer, testMethod, 42, payload)

	mutated := append([]byte(nil), payload...)
	mutated[len(mutated)-1] = 'y'
	if err := verifier.VerifyWithPayload(ctx, testMethod, 42, mutated); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("mutated payload error=%v, want ErrUnauthorized", err)
	}
	if len(store.seen) != 0 {
		t.Fatal("mutated payload consumed replay nonce")
	}
	if err := verifier.VerifyWithPayload(ctx, testMethod, 42, payload); err != nil {
		t.Fatalf("exact payload VerifyWithPayload: %v", err)
	}
	if err := verifier.VerifyWithPayload(ctx, testMethod, 42, payload); !errors.Is(err, ErrReplay) {
		t.Fatalf("payload replay error=%v, want ErrReplay", err)
	}
}

func TestPayloadCredentialCannotDowngradeToPlainVerify(t *testing.T) {
	signer, verifier, store := newAuthPair(t)
	ctx := signedIncomingWithPayload(t, signer, testMethod, 42, []byte("canonical-body"))
	if err := verifier.Verify(ctx, testMethod, 42); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("downgrade error=%v, want ErrUnauthorized", err)
	}
	if len(store.seen) != 0 {
		t.Fatal("downgrade attempt consumed replay nonce")
	}
}

func TestPlainCredentialCannotUpgradeToPayloadVerify(t *testing.T) {
	signer, verifier, store := newAuthPair(t)
	ctx := signedIncoming(t, signer, testMethod, 42)
	if err := verifier.VerifyWithPayload(ctx, testMethod, 42, []byte("canonical-body")); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("upgrade error=%v, want ErrUnauthorized", err)
	}
	if len(store.seen) != 0 {
		t.Fatal("upgrade attempt consumed replay nonce")
	}
}

// --- MultiCallerVerifier: one protected method, several callers, independent keys ---

const teamTestSecret = "internal-rpc-auth-test-secret-team-9876543210fedcba"

// newMultiCallerFixture wires the production shape: Login and Team both call the
// same method, each with its own key, sharing one replay store.
func newMultiCallerFixture(t *testing.T) (loginSigner, teamSigner *Signer, mv *MultiCallerVerifier, store *memoryReplayStore) {
	t.Helper()
	store = &memoryReplayStore{seen: map[string]struct{}{}}
	fixed := time.Unix(1_800_000_000, 123_000_000).UTC()

	newSigner := func(secret, caller string) *Signer {
		s, err := NewSigner(secret, caller, testAudience)
		if err != nil {
			t.Fatal(err)
		}
		s.now = func() time.Time { return fixed }
		s.random = bytes.NewReader(bytes.Repeat([]byte{0x5a}, nonceBytes))
		return s
	}
	newVerifier := func(secret, caller string) *Verifier {
		v, err := NewVerifier(secret, caller, testAudience, 30*time.Second, store)
		if err != nil {
			t.Fatal(err)
		}
		v.now = func() time.Time { return fixed }
		return v
	}

	loginSigner = newSigner(testSecret, "login")
	teamSigner = newSigner(teamTestSecret, "team")
	mv, err := NewMultiCallerVerifier(newVerifier(testSecret, "login"), newVerifier(teamTestSecret, "team"))
	if err != nil {
		t.Fatal(err)
	}
	return loginSigner, teamSigner, mv, store
}

// Regression for the team browse/join outage: team's credential must be accepted
// on the same method as login's. Before the fix team sent no credential at all
// and every ListOpenTeams candidate was pruned, so the recruiting list was
// permanently empty and the fail-closed join gate always rejected.
func TestMultiCallerVerifierAcceptsEachCallerWithItsOwnKey(t *testing.T) {
	loginSigner, teamSigner, mv, _ := newMultiCallerFixture(t)

	if err := mv.Verify(signedIncoming(t, loginSigner, testMethod, 42), testMethod, 42); err != nil {
		t.Fatalf("login Verify: %v", err)
	}
	if err := mv.Verify(signedIncoming(t, teamSigner, testMethod, 42), testMethod, 42); err != nil {
		t.Fatalf("team Verify: %v", err)
	}
}

func TestMultiCallerVerifierBindsPayloadForEachIndependentCaller(t *testing.T) {
	loginSigner, teamSigner, mv, store := newMultiCallerFixture(t)
	payload := []byte("canonical-display-request")

	loginCtx := signedIncomingWithPayload(t, loginSigner, testMethod, 42, payload)
	if err := mv.VerifyWithPayload(loginCtx, testMethod, 42, []byte("tampered")); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("tampered payload error=%v, want ErrUnauthorized", err)
	}
	if len(store.seen) != 0 {
		t.Fatal("tampered multi-caller payload consumed replay nonce")
	}
	if err := mv.VerifyWithPayload(loginCtx, testMethod, 42, payload); err != nil {
		t.Fatalf("login VerifyWithPayload: %v", err)
	}

	teamCtx := signedIncomingWithPayload(t, teamSigner, testMethod, 42, payload)
	if err := mv.VerifyWithPayload(teamCtx, testMethod, 42, payload); err != nil {
		t.Fatalf("team VerifyWithPayload: %v", err)
	}
}

// Nonces are namespaced by caller, so two callers reusing the same nonce bytes
// must not consume each other's entry — but each caller still cannot replay.
func TestMultiCallerVerifierIsolatesReplayNoncesPerCaller(t *testing.T) {
	loginSigner, teamSigner, mv, _ := newMultiCallerFixture(t)

	loginCtx := signedIncoming(t, loginSigner, testMethod, 42)
	teamCtx := signedIncoming(t, teamSigner, testMethod, 42)
	if err := mv.Verify(loginCtx, testMethod, 42); err != nil {
		t.Fatalf("login Verify: %v", err)
	}
	// Same fixed nonce bytes, different caller: must still be accepted.
	if err := mv.Verify(teamCtx, testMethod, 42); err != nil {
		t.Fatalf("team Verify after login consumed same nonce: %v", err)
	}
	if err := mv.Verify(teamCtx, testMethod, 42); !errors.Is(err, ErrReplay) {
		t.Fatalf("team replay error=%v, want ErrReplay", err)
	}
}

// The whole point of separate keys: holding one caller's secret must not let you
// speak as the other. Dispatching on the caller header is only safe because the
// identity is signed into the canonical message.
func TestMultiCallerVerifierRejectsCrossCallerImpersonation(t *testing.T) {
	_, _, mv, store := newMultiCallerFixture(t)
	fixed := time.Unix(1_800_000_000, 123_000_000).UTC()

	// Login's key, but claiming to be "team".
	forged, err := NewSigner(testSecret, "team", testAudience)
	if err != nil {
		t.Fatal(err)
	}
	forged.now = func() time.Time { return fixed }
	forged.random = bytes.NewReader(bytes.Repeat([]byte{0x5a}, nonceBytes))

	if err := mv.Verify(signedIncoming(t, forged, testMethod, 42), testMethod, 42); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("impersonation error=%v, want ErrUnauthorized", err)
	}
	if len(store.seen) != 0 {
		t.Fatalf("rejected impersonation consumed %d nonce(s), want 0", len(store.seen))
	}
}

// An unregistered caller is rejected without touching the replay store, so an
// unknown identity cannot burn nonce entries.
func TestMultiCallerVerifierRejectsUnknownCallerWithoutConsumingNonce(t *testing.T) {
	_, _, mv, store := newMultiCallerFixture(t)
	fixed := time.Unix(1_800_000_000, 123_000_000).UTC()

	stranger, err := NewSigner(teamTestSecret, "battle-result", testAudience)
	if err != nil {
		t.Fatal(err)
	}
	stranger.now = func() time.Time { return fixed }
	stranger.random = bytes.NewReader(bytes.Repeat([]byte{0x5a}, nonceBytes))

	if err := mv.Verify(signedIncoming(t, stranger, testMethod, 42), testMethod, 42); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("unknown caller error=%v, want ErrUnauthorized", err)
	}
	if len(store.seen) != 0 {
		t.Fatalf("unknown caller consumed %d nonce(s), want 0", len(store.seen))
	}
}

// A request with no credential at all is exactly what team sent before the fix.
func TestMultiCallerVerifierRejectsUnsignedRequest(t *testing.T) {
	_, _, mv, _ := newMultiCallerFixture(t)
	if err := mv.Verify(context.Background(), testMethod, 42); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("unsigned error=%v, want ErrUnauthorized", err)
	}
}

func TestNewMultiCallerVerifierRejectsDuplicateCaller(t *testing.T) {
	store := &memoryReplayStore{seen: map[string]struct{}{}}
	first, err := NewVerifier(testSecret, "login", testAudience, 30*time.Second, store)
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewVerifier(teamTestSecret, "login", testAudience, 30*time.Second, store)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := NewMultiCallerVerifier(first, second); err == nil {
		t.Fatal("duplicate caller accepted, want error")
	}
}
