package audit

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/abd-ulbasit/forgepoint/pkg/grpcutil"
	"github.com/abd-ulbasit/forgepoint/pkg/testutil"
)

// ============================================================================
// FAKE SINK — records what the interceptor captured, with an optional error
// ============================================================================

// fakeSink captures records for assertions. err, when non-nil, simulates a sink
// failure to prove the interceptor never breaks the RPC on a sink error.
type fakeSink struct {
	mu      sync.Mutex
	records []Record
	err     error
}

func (f *fakeSink) Record(_ context.Context, rec Record) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.records = append(f.records, rec)
	return f.err
}

func (f *fakeSink) captured() []Record {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]Record, len(f.records))
	copy(out, f.records)
	return out
}

// ctxWithClaims builds a context carrying claims the way grpcutil's auth
// interceptor would — by invoking that interceptor with a mock validator and
// capturing the context it hands the inner handler. This exercises the REAL
// grpcutil claims plumbing rather than reaching into its private context key.
func ctxWithClaims(t *testing.T, claims *grpcutil.Claims) context.Context {
	t.Helper()
	var captured context.Context
	inner := func(ctx context.Context, _ any) (any, error) {
		captured = ctx
		return nil, nil
	}
	authInt := grpcutil.AuthUnaryInterceptor(&staticValidator{claims: claims})
	md := metadata.Pairs("authorization", "Bearer token")
	ctx := metadata.NewIncomingContext(context.Background(), md)
	if _, err := authInt(ctx, nil, &grpc.UnaryServerInfo{FullMethod: "/x/Y"}, inner); err != nil {
		t.Fatalf("auth interceptor unexpectedly failed building claims context: %v", err)
	}
	return captured
}

// staticValidator is a grpcutil.TokenValidator that always returns the same claims.
type staticValidator struct{ claims *grpcutil.Claims }

func (s *staticValidator) Validate(context.Context, string) (*grpcutil.Claims, error) {
	return s.claims, nil
}

// ============================================================================
// THE DECISION MATRIX — the heart of the interceptor
// ============================================================================

// TestUnaryInterceptor_DecisionMatrix drives every (method, handler-result) →
// (audited?, decision) combination the interceptor must get right.
func TestUnaryInterceptor_DecisionMatrix(t *testing.T) {
	const mutating = "/forgepoint.auth.v1.AuthService/CreateUser"
	const read = "/forgepoint.auth.v1.AuthService/GetUser"

	cases := []struct {
		name         string
		method       string
		handlerErr   error
		wantAudited  bool
		wantDecision Decision
		wantCode     string
	}{
		{"allow on mutation is audited", mutating, nil, true, DecisionAllow, "OK"},
		{"allow on read is NOT audited", read, nil, false, "", ""},
		{"deny (PermissionDenied) on mutation audited", mutating, status.Error(codes.PermissionDenied, "nope"), true, DecisionDeny, "PermissionDenied"},
		{"deny (Unauthenticated) on a READ is STILL audited", read, status.Error(codes.Unauthenticated, "no token"), true, DecisionDeny, "Unauthenticated"},
		{"error on mutation is audited as ERROR", mutating, status.Error(codes.Internal, "boom"), true, DecisionError, "Internal"},
		{"error on read is NOT audited", read, status.Error(codes.NotFound, "missing"), false, "", ""},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sink := &fakeSink{}
			interceptor := UnaryServerInterceptor(sink, Options{Source: "auth"})

			handler := func(ctx context.Context, req any) (any, error) {
				return "resp", tc.handlerErr
			}

			resp, err := interceptor(context.Background(), "req",
				&grpc.UnaryServerInfo{FullMethod: tc.method}, handler)

			// The interceptor MUST pass the handler's result through unchanged.
			if resp != "resp" {
				t.Errorf("resp = %v, want passthrough 'resp'", resp)
			}
			if !errors.Is(err, tc.handlerErr) && err != tc.handlerErr {
				t.Errorf("err = %v, want passthrough %v", err, tc.handlerErr)
			}

			got := sink.captured()
			if tc.wantAudited {
				if len(got) != 1 {
					t.Fatalf("expected 1 audit record, got %d", len(got))
				}
				if got[0].Decision != tc.wantDecision {
					t.Errorf("decision = %q, want %q", got[0].Decision, tc.wantDecision)
				}
				if got[0].GRPCCode != tc.wantCode {
					t.Errorf("grpc code = %q, want %q", got[0].GRPCCode, tc.wantCode)
				}
				if got[0].Action != tc.method {
					t.Errorf("action = %q, want %q", got[0].Action, tc.method)
				}
				if got[0].SourceService != "auth" {
					t.Errorf("source = %q, want auth", got[0].SourceService)
				}
				if got[0].OccurredAt.IsZero() {
					t.Error("OccurredAt not set")
				}
			} else if len(got) != 0 {
				t.Fatalf("expected NO audit record, got %d: %+v", len(got), got)
			}
		})
	}
}

// TestUnaryInterceptor_AllowRecordsAuthenticatedActor proves that for an allowed
// call with claims in context (populated by the auth interceptor upstream), the
// Record's Actor is the authenticated identity — NOT anonymous.
func TestUnaryInterceptor_AllowRecordsAuthenticatedActor(t *testing.T) {
	sink := &fakeSink{}
	interceptor := UnaryServerInterceptor(sink, Options{Source: "auth"})

	ctx := ctxWithClaims(t, &grpcutil.Claims{
		UserID: "user-123", Email: "ada@fp.io", Team: "platform", Role: "admin",
	})

	_, err := interceptor(ctx, "req",
		&grpc.UnaryServerInfo{FullMethod: "/forgepoint.auth.v1.AuthService/AssignRole"},
		func(ctx context.Context, req any) (any, error) { return "ok", nil })
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	got := sink.captured()
	if len(got) != 1 {
		t.Fatalf("expected 1 record, got %d", len(got))
	}
	a := got[0].Actor
	if a.UserID != "user-123" || a.Email != "ada@fp.io" || a.Team != "platform" || a.Role != "admin" {
		t.Errorf("actor = %+v, want the authenticated identity", a)
	}
}

// TestUnaryInterceptor_AnonymousDeniedCaptured proves the most important case: an
// unauthenticated, DENIED call (no claims in context) is STILL captured, with the
// anonymous actor sentinel. This is the failed-access signal an audit log exists for.
func TestUnaryInterceptor_AnonymousDeniedCaptured(t *testing.T) {
	sink := &fakeSink{}
	interceptor := UnaryServerInterceptor(sink, Options{Source: "auth"})

	// No claims in context; handler returns Unauthenticated (as the auth
	// interceptor would when short-circuiting an unauthenticated privileged call).
	_, err := interceptor(context.Background(), "req",
		&grpc.UnaryServerInfo{FullMethod: "/forgepoint.auth.v1.AuthService/ListUsers"},
		func(ctx context.Context, req any) (any, error) {
			return nil, status.Error(codes.Unauthenticated, "missing authorization header")
		})
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("expected Unauthenticated passthrough, got %v", err)
	}

	got := sink.captured()
	if len(got) != 1 {
		t.Fatalf("expected the denied call to be audited, got %d records", len(got))
	}
	if got[0].Actor.UserID != AnonymousActor {
		t.Errorf("actor = %q, want %q for an unauthenticated call", got[0].Actor.UserID, AnonymousActor)
	}
	if got[0].Decision != DecisionDeny {
		t.Errorf("decision = %q, want DENY", got[0].Decision)
	}
}

// TestUnaryInterceptor_FailingSinkNeverBreaksRPC proves a sink error is swallowed:
// the RPC's response and error are returned untouched even though Record failed.
func TestUnaryInterceptor_FailingSinkNeverBreaksRPC(t *testing.T) {
	sink := &fakeSink{err: errors.New("nats is down")}
	interceptor := UnaryServerInterceptor(sink, Options{Source: "auth"})

	// Success path: handler returns ok, sink fails — RPC must still see ok/nil.
	resp, err := interceptor(context.Background(), "req",
		&grpc.UnaryServerInfo{FullMethod: "/forgepoint.auth.v1.AuthService/CreateUser"},
		func(ctx context.Context, req any) (any, error) { return "the-real-response", nil })
	if err != nil {
		t.Fatalf("sink failure leaked into RPC error: %v", err)
	}
	if resp != "the-real-response" {
		t.Errorf("resp = %v, want the real response despite sink failure", resp)
	}

	// Error path: handler returns an error, sink fails — the ORIGINAL handler error
	// must be returned, not the sink's error.
	handlerErr := status.Error(codes.PermissionDenied, "denied")
	_, err = interceptor(context.Background(), "req",
		&grpc.UnaryServerInfo{FullMethod: "/forgepoint.auth.v1.AuthService/AssignRole"},
		func(ctx context.Context, req any) (any, error) { return nil, handlerErr })
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("expected the handler's PermissionDenied, got %v (sink error must not leak)", err)
	}
}

// TestUnaryInterceptor_SanitizesError proves Err carries only the status MESSAGE,
// never the verbose err.Error() form, and is empty on success.
func TestUnaryInterceptor_SanitizesError(t *testing.T) {
	sink := &fakeSink{}
	interceptor := UnaryServerInterceptor(sink, Options{Source: "auth"})

	_, _ = interceptor(context.Background(), "req",
		&grpc.UnaryServerInfo{FullMethod: "/forgepoint.auth.v1.AuthService/CreateUser"},
		func(ctx context.Context, req any) (any, error) {
			return nil, status.Error(codes.AlreadyExists, "email already exists")
		})

	got := sink.captured()
	if len(got) != 1 {
		t.Fatalf("expected 1 record, got %d", len(got))
	}
	if got[0].Err != "email already exists" {
		t.Errorf("err = %q, want the sanitized status message", got[0].Err)
	}
	if strings.Contains(got[0].Err, "rpc error") {
		t.Errorf("err contains the verbose 'rpc error' form: %q", got[0].Err)
	}
}

// ============================================================================
// THE SECRET-LEAK TEST — a password must NEVER reach a Record
// ============================================================================

// createUserRequest mimics the SHAPE of a CreateUser request: it carries a
// plaintext password. The audit interceptor must never copy any request field
// into the Record, so this secret cannot appear anywhere in the captured record.
type createUserRequest struct {
	Email    string
	Password string // SECRET — must never be audited
}

// TestUnaryInterceptor_NeverCapturesPassword is the security regression guard. It
// runs a CreateUser-shaped call whose request contains a password and asserts the
// secret appears in NO field of the captured Record — even with a (misbehaving)
// ResourceExtractor, the audit record's fields are checked exhaustively.
func TestUnaryInterceptor_NeverCapturesPassword(t *testing.T) {
	const secret = "S3cr3t-Passw0rd!"
	sink := &fakeSink{}

	// Even supply a ResourceExtractor that reads the email (a legitimate id) to
	// prove the SAFE extractor path also never touches the password.
	interceptor := UnaryServerInterceptor(sink, Options{
		Source: "auth",
		ResourceExtractor: func(_ string, req any) string {
			if r, ok := req.(*createUserRequest); ok {
				return r.Email // identifier only — never the password
			}
			return ""
		},
	})

	req := &createUserRequest{Email: "ada@fp.io", Password: secret}
	_, err := interceptor(context.Background(), req,
		&grpc.UnaryServerInfo{FullMethod: "/forgepoint.auth.v1.AuthService/CreateUser"},
		func(ctx context.Context, req any) (any, error) { return "ok", nil })
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	got := sink.captured()
	if len(got) != 1 {
		t.Fatalf("expected 1 record, got %d", len(got))
	}
	rec := got[0]

	// Exhaustively check every string field of the record for the secret.
	fields := []string{
		rec.Actor.UserID, rec.Actor.Email, rec.Actor.Team, rec.Actor.Role,
		rec.Action, rec.Resource, string(rec.Decision), rec.GRPCCode, rec.Err,
		rec.CorrelationID, rec.SourceService,
	}
	for _, f := range fields {
		if strings.Contains(f, secret) {
			t.Fatalf("password leaked into audit record field: %q", f)
		}
	}
	// And the canonical (hashed) encoding must not contain it either.
	if strings.Contains(string(canonicalJSON(rec)), secret) {
		t.Fatal("password leaked into the canonical (hashed) record encoding")
	}
	// The safe resource extraction DID capture the email identifier.
	if rec.Resource != "ada@fp.io" {
		t.Errorf("resource = %q, want the email identifier", rec.Resource)
	}
}

// ============================================================================
// END-TO-END via bufconn — the FULL two-interceptor chain around auth
// ============================================================================
//
// This proves the CHAIN the auth service wires:
//
//	DENY-CAPTURE(outer) → auth → AUDIT(inner) → handler
//
// Consequences this test pins down:
//
//   - AUTHENTICATED ALLOW: auth injects claims into the context it passes inward,
//     so the inner audit interceptor reads the REAL authenticated actor. This is
//     why the inner interceptor is wired AFTER the validator ("claims are
//     populated"). The outer interceptor stays silent (the marker says the inner
//     one handled it).
//
//   - HANDLER-LEVEL DENIAL (e.g. requireAdmin → PermissionDenied): the denial
//     happens INSIDE the inner interceptor's wrapped scope, so the inner one
//     captures it as a DENY with the authenticated actor — EXACTLY ONCE (the outer
//     interceptor sees the marker set and stays silent).
//
//   - AUTH-INTERCEPTOR TOKEN REJECTION (missing/forged token): auth short-circuits
//     BEFORE the inner interceptor runs, so the inner one never fires. The OUTER
//     DENY-capture interceptor (wrapping auth) records the denial with the
//     anonymous actor. This is the completeness-gap fix — the most valuable audit
//     signal (an anonymous probe of a privileged method) is now captured, not
//     dropped onto the logging interceptor. TestEndToEnd_NoToken_DeniedCaptured
//     below pins exactly this.
//
// We register a hand-rolled gRPC service (no generated proto needed) so pkg/audit
// stays free of a gen/go dependency.

// testServiceDesc describes a one-method gRPC service "audit.test.T/CreateThing".
// Request and response are *emptypb.Empty — a real proto.Message — so the gRPC
// codec can marshal them over bufconn WITHOUT pulling a generated service into
// pkg/audit. handlerErr lets a test make the business handler itself DENY (the
// requireAdmin authZ path): because audit is OUTER of the handler, it captures
// that denial with the caller's claims.
func testServiceDesc(t *testing.T, handlerErr error) grpc.ServiceDesc {
	t.Helper()
	return grpc.ServiceDesc{
		ServiceName: "audit.test.T",
		HandlerType: (*any)(nil),
		Methods: []grpc.MethodDesc{
			{
				MethodName: "CreateThing", // a mutating verb → audited on success
				Handler: func(_ any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
					req := new(emptypb.Empty)
					if err := dec(req); err != nil {
						return nil, err
					}
					business := func(ctx context.Context, _ any) (any, error) {
						if handlerErr != nil {
							return nil, handlerErr
						}
						return new(emptypb.Empty), nil
					}
					info := &grpc.UnaryServerInfo{FullMethod: "/audit.test.T/CreateThing"}
					if interceptor == nil {
						return business(ctx, req)
					}
					return interceptor(ctx, req, info, business)
				},
			},
		},
		Streams:  []grpc.StreamDesc{},
		Metadata: "audit_test",
	}
}

func TestEndToEnd_AuthThenAudit_Chained(t *testing.T) {
	validClaims := &grpcutil.Claims{UserID: "user-7", Email: "e@fp.io", Role: "admin"}

	cases := []struct {
		name         string
		handlerErr   error // nil → ALLOW; an error → the handler (requireAdmin) denies
		wantRPCErr   codes.Code
		wantActor    string
		wantDecision Decision
	}{
		// Authenticated, handler succeeds: audit (inner of auth) reads the real actor.
		{"authenticated allow", nil, codes.OK, "user-7", DecisionAllow},
		// Authenticated, handler denies (the requireAdmin authZ path): the denial
		// occurs INSIDE audit's wrapped scope, so audit captures it as DENY with the
		// caller's authenticated identity — proving DENIED access is recorded.
		{"authenticated handler-denied is audited", status.Error(codes.PermissionDenied, "admin only"), codes.PermissionDenied, "user-7", DecisionDeny},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sink := &fakeSink{}

			validator := &staticValidator{claims: validClaims}

			// THE CHAIN under test, exactly as the auth service wires it:
			//   deny-capture (outer) → auth → audit (inner) → handler.
			denyInt := DenyUnaryInterceptor(sink, Options{Source: "auth"})
			authInt := grpcutil.AuthUnaryInterceptor(validator)
			auditInt := UnaryServerInterceptor(sink, Options{Source: "auth"})

			desc := testServiceDesc(t, tc.handlerErr)
			conn := testutil.NewTestGRPCServer(t, func(s *grpc.Server) {
				s.RegisterService(&desc, struct{}{})
			}, grpc.ChainUnaryInterceptor(denyInt, authInt, auditInt))

			// Always authenticated (a valid token); the denial, when present, comes
			// from the handler, not the auth interceptor.
			ctx := metadata.AppendToOutgoingContext(context.Background(), "authorization", "Bearer good")

			resp := new(emptypb.Empty)
			err := conn.Invoke(ctx, "/audit.test.T/CreateThing", new(emptypb.Empty), resp)
			if status.Code(err) != tc.wantRPCErr {
				t.Fatalf("rpc code = %v, want %v (err=%v)", status.Code(err), tc.wantRPCErr, err)
			}

			got := sink.captured()
			if len(got) != 1 {
				t.Fatalf("expected exactly 1 audit record, got %d", len(got))
			}
			if got[0].Actor.UserID != tc.wantActor {
				t.Errorf("actor = %q, want %q", got[0].Actor.UserID, tc.wantActor)
			}
			if got[0].Decision != tc.wantDecision {
				t.Errorf("decision = %q, want %q", got[0].Decision, tc.wantDecision)
			}
			if got[0].Action != "/audit.test.T/CreateThing" {
				t.Errorf("action = %q", got[0].Action)
			}
		})
	}
}

// TestEndToEnd_NoToken_DeniedCaptured is the completeness-gap regression guard: a
// call to a privileged method with NO token must produce EXACTLY ONE audit record,
// a DENY with Actor=anonymous, captured by the OUTER deny-interceptor (the auth
// interceptor rejected the request before the inner interceptor could run).
//
// Before the fix this produced ZERO audit rows (auth short-circuited inner of the
// only audit interceptor), so the most common attack shape — an anonymous probe of
// AssignRole/CreateUser — left no trace. This test fails on a regression of that.
func TestEndToEnd_NoToken_DeniedCaptured(t *testing.T) {
	sink := &fakeSink{}

	// A validator that would succeed IF a token were presented — but we send NO
	// authorization metadata, so the auth interceptor rejects with Unauthenticated
	// BEFORE the inner audit interceptor (and before any claims exist).
	validator := &staticValidator{claims: &grpcutil.Claims{UserID: "would-be-admin", Role: "admin"}}

	denyInt := DenyUnaryInterceptor(sink, Options{Source: "auth"})
	authInt := grpcutil.AuthUnaryInterceptor(validator)
	auditInt := UnaryServerInterceptor(sink, Options{Source: "auth"})

	// handlerErr is irrelevant: the handler is never reached (auth denies first).
	desc := testServiceDesc(t, nil)
	conn := testutil.NewTestGRPCServer(t, func(s *grpc.Server) {
		s.RegisterService(&desc, struct{}{})
	}, grpc.ChainUnaryInterceptor(denyInt, authInt, auditInt))

	// NO authorization metadata → the auth interceptor short-circuits.
	resp := new(emptypb.Empty)
	err := conn.Invoke(context.Background(), "/audit.test.T/CreateThing", new(emptypb.Empty), resp)
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("rpc code = %v, want Unauthenticated (auth must reject the no-token call)", status.Code(err))
	}

	got := sink.captured()
	if len(got) != 1 {
		t.Fatalf("expected EXACTLY 1 audit record for the no-token denial, got %d: %+v", len(got), got)
	}
	if got[0].Decision != DecisionDeny {
		t.Errorf("decision = %q, want DENY", got[0].Decision)
	}
	if got[0].Actor.UserID != AnonymousActor {
		t.Errorf("actor = %q, want %q (auth rejected before claims existed)", got[0].Actor.UserID, AnonymousActor)
	}
	if got[0].GRPCCode != codes.Unauthenticated.String() {
		t.Errorf("grpc code = %q, want Unauthenticated", got[0].GRPCCode)
	}
	if got[0].Action != "/audit.test.T/CreateThing" {
		t.Errorf("action = %q, want the privileged method", got[0].Action)
	}
}
