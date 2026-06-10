package grpcutil_test

import (
	"context"
	"errors"
	"testing"

	"github.com/abd-ulbasit/forgepoint/pkg/grpcutil"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// stubValidator lets each test control what Validate returns.
type stubValidator struct {
	claims *grpcutil.Claims
	err    error
}

func (s stubValidator) Validate(context.Context, string) (*grpcutil.Claims, error) {
	return s.claims, s.err
}

func ctxWithBearer(token string) context.Context {
	return metadata.NewIncomingContext(
		context.Background(),
		metadata.Pairs("authorization", "Bearer "+token),
	)
}

// A validator error that is already a gRPC status (e.g. the auth backend is
// down → Internal) must be PRESERVED, not flattened to Unauthenticated.
// Otherwise an auth-service outage masquerades as every client having bad creds.
func TestAuthUnary_PreservesValidatorStatusCode(t *testing.T) {
	interceptor := grpcutil.AuthUnaryInterceptor(
		stubValidator{err: status.Error(codes.Internal, "auth backend down")},
	)

	_, err := interceptor(ctxWithBearer("tok"), nil,
		&grpc.UnaryServerInfo{FullMethod: "/svc/Method"},
		func(context.Context, any) (any, error) { return nil, nil },
	)

	if status.Code(err) != codes.Internal {
		t.Fatalf("code = %s, want Internal (status must pass through)", status.Code(err))
	}
}

// A plain (non-status) validator error is the genuine "bad token" case and
// should become Unauthenticated.
func TestAuthUnary_WrapsPlainErrorAsUnauthenticated(t *testing.T) {
	interceptor := grpcutil.AuthUnaryInterceptor(
		stubValidator{err: errors.New("signature mismatch")},
	)

	_, err := interceptor(ctxWithBearer("tok"), nil,
		&grpc.UnaryServerInfo{FullMethod: "/svc/Method"},
		func(context.Context, any) (any, error) { return nil, nil },
	)

	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("code = %s, want Unauthenticated", status.Code(err))
	}
}

// fakeServerStream is a minimal grpc.ServerStream whose Context() is settable,
// so we can drive the stream auth interceptor.
type fakeServerStream struct {
	ctx context.Context
}

func (f *fakeServerStream) SetHeader(metadata.MD) error  { return nil }
func (f *fakeServerStream) SendHeader(metadata.MD) error { return nil }
func (f *fakeServerStream) SetTrailer(metadata.MD)       {}
func (f *fakeServerStream) Context() context.Context     { return f.ctx }
func (f *fakeServerStream) SendMsg(any) error            { return nil }
func (f *fakeServerStream) RecvMsg(any) error            { return nil }

// Streaming RPCs must be authenticated too — the whole point of the fix. A
// missing token must be rejected before the handler runs.
func TestAuthStream_RejectsMissingToken(t *testing.T) {
	interceptor := grpcutil.AuthStreamInterceptor(stubValidator{})

	handlerCalled := false
	err := interceptor(nil, &fakeServerStream{ctx: context.Background()},
		&grpc.StreamServerInfo{FullMethod: "/svc/Watch"},
		func(any, grpc.ServerStream) error { handlerCalled = true; return nil },
	)

	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("code = %s, want Unauthenticated", status.Code(err))
	}
	if handlerCalled {
		t.Fatal("handler ran despite failed auth")
	}
}

// On success, claims must be injected into the stream's context so streaming
// handlers can read them via ClaimsFromContext (the ServerStream wrapper trick).
func TestAuthStream_InjectsClaimsIntoStreamContext(t *testing.T) {
	want := &grpcutil.Claims{UserID: "u1", Role: "admin"}
	interceptor := grpcutil.AuthStreamInterceptor(stubValidator{claims: want})

	var got *grpcutil.Claims
	err := interceptor(nil, &fakeServerStream{ctx: ctxWithBearer("tok")},
		&grpc.StreamServerInfo{FullMethod: "/svc/Watch"},
		func(_ any, ss grpc.ServerStream) error {
			got, _ = grpcutil.ClaimsFromContext(ss.Context())
			return nil
		},
	)
	if err != nil {
		t.Fatalf("interceptor returned error: %v", err)
	}
	if got != want {
		t.Fatalf("claims in stream context = %+v, want %+v", got, want)
	}
}

// Skipped methods bypass auth on the stream path (e.g. health/reflection).
func TestAuthStream_SkipMethodBypassesAuth(t *testing.T) {
	interceptor := grpcutil.AuthStreamInterceptor(
		stubValidator{err: errors.New("should not be called")},
		grpcutil.WithSkipMethods("/svc/Public"),
	)

	called := false
	err := interceptor(nil, &fakeServerStream{ctx: context.Background()},
		&grpc.StreamServerInfo{FullMethod: "/svc/Public"},
		func(any, grpc.ServerStream) error { called = true; return nil },
	)
	if err != nil {
		t.Fatalf("skipped method returned error: %v", err)
	}
	if !called {
		t.Fatal("handler not called for skipped method")
	}
}
