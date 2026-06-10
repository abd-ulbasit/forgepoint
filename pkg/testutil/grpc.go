package testutil

import (
	"context"
	"net"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

// ============================================================================
// IN-PROCESS gRPC TEST SERVER
// ============================================================================
//
// WHY bufconn instead of real network:
//   gRPC tests need a server and client, but using real TCP ports creates
//   problems:
//   - Port conflicts between parallel tests
//   - Slower due to TCP handshake, serialization, kernel context switches
//   - Flaky in CI (port exhaustion, firewall rules)
//
//   bufconn solves all of this by using an in-memory buffer as the transport.
//   The gRPC client and server think they're talking over a network, but
//   data never leaves the process. ~100x faster than real TCP.
//
// HOW IT WORKS:
//   1. bufconn.Listen(bufSize) creates an in-memory listener
//   2. grpc.Server listens on the bufconn listener
//   3. grpc.DialContext connects via a custom dialer that uses bufconn
//   4. Client ↔ Server communication is entirely in-memory
//   5. Full gRPC semantics preserved (metadata, interceptors, streaming)
//
//   ┌─────────────┐    bufconn (in-memory)    ┌─────────────┐
//   │ gRPC Client  │ ◀──────────────────────▶ │ gRPC Server  │
//   │ (test code)  │    no real network        │ (handler)    │
//   └─────────────┘                            └─────────────┘
//
// ALTERNATIVES CONSIDERED:
//   - Real TCP: Slow, port conflicts, flaky → rejected
//   - httptest.Server: Doesn't support gRPC (HTTP/2 + proto) → rejected
//   - Mock clients: Miss interceptor bugs, proto serialization → rejected
//   - bufconn: Fast, reliable, full gRPC stack → chosen
//
// HOW GOOGLE/UBER DO IT:
//   - Google: bufconn (it's their library, part of grpc-go)
//   - Uber: bufconn for unit tests, real server for integration
//   - Netflix: Similar pattern with in-process HTTP for REST services
// ============================================================================

const bufSize = 1024 * 1024 // 1MB buffer — sufficient for test payloads

// NewTestGRPCServer creates an in-process gRPC server using bufconn.
// The register function should register service handlers on the server.
// Returns a client connection ready to use with generated gRPC clients.
//
// Both server and connection are automatically cleaned up when the test finishes.
//
// Usage:
//
//	conn := testutil.NewTestGRPCServer(t, func(s *grpc.Server) {
//	    authv1.RegisterAuthServiceServer(s, myHandler)
//	})
//	client := authv1.NewAuthServiceClient(conn)
//	resp, err := client.Login(ctx, &authv1.LoginRequest{...})
//
// Pass serverOpts to install the interceptor chain under test, e.g.:
//
//	conn := testutil.NewTestGRPCServer(t, register,
//	    grpc.ChainUnaryInterceptor(grpcutil.AuthUnaryInterceptor(validator)))
func NewTestGRPCServer(t *testing.T, register func(s *grpc.Server), serverOpts ...grpc.ServerOption) *grpc.ClientConn {
	t.Helper()

	lis := bufconn.Listen(bufSize)

	server := grpc.NewServer(serverOpts...)
	register(server)

	// Start serving in a background goroutine, sending the final Serve error
	// back over a buffered channel.
	//
	// WHY a channel and not t.Logf here: t.Logf/t.Errorf MUST NOT be called
	// from a goroutine that can outlive the test function — doing so panics
	// ("Log in goroutine after Test has completed"). The Serve goroutine is
	// exactly such a goroutine. So we hand the error back to the test goroutine
	// and inspect it inside t.Cleanup, where calling t.* is safe.
	serveErrCh := make(chan error, 1)
	go func() {
		serveErrCh <- server.Serve(lis)
	}()

	// Create client connection through the bufconn dialer.
	conn, err := grpc.NewClient(
		"passthrough:///bufconn",
		grpc.WithContextDialer(func(ctx context.Context, addr string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("failed to create bufconn client: %v", err)
	}

	t.Cleanup(func() {
		_ = conn.Close()
		// GracefulStop unblocks Serve, which then returns nil. Any non-nil
		// error is a real test failure (e.g., listener died mid-test).
		server.GracefulStop()
		if err := <-serveErrCh; err != nil {
			t.Errorf("bufconn server.Serve returned: %v", err)
		}
	})

	return conn
}
