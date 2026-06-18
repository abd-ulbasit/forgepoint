package clientfactory

import (
	"context"
	"net"
	"strings"
	"testing"
	"time"

	authv1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/auth/v1"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/test/bufconn"
)

// recordingAuthServer is a stub AuthService that captures the incoming metadata
// of each call so the test can assert the CLI forwarded the bearer token. It
// embeds the Unimplemented base so unused RPCs return Unimplemented for free.
type recordingAuthServer struct {
	authv1.UnimplementedAuthServiceServer
	gotMD metadata.MD
}

func (s *recordingAuthServer) Login(ctx context.Context, _ *authv1.LoginRequest) (*authv1.LoginResponse, error) {
	s.gotMD, _ = metadata.FromIncomingContext(ctx)
	return &authv1.LoginResponse{AccessToken: "issued"}, nil
}

// dialBufconn builds a *grpc.ClientConn to an in-memory server, wiring in the
// SAME token DialOptions production uses (via TokenDialOptions). This is the key
// to testing the real interceptor end-to-end rather than a mock of it.
func dialBufconn(t *testing.T, lis *bufconn.Listener, token string) *grpc.ClientConn {
	t.Helper()
	opts := []grpc.DialOption{
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}
	opts = append(opts, TokenDialOptions(token)...)

	conn, err := grpc.NewClient("passthrough:///bufconn", opts...)
	if err != nil {
		t.Fatalf("dial bufconn: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	return conn
}

// startServer spins up the recording server on a bufconn listener.
func startServer(t *testing.T) (*bufconn.Listener, *recordingAuthServer) {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	rec := &recordingAuthServer{}
	authv1.RegisterAuthServiceServer(srv, rec)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	return lis, rec
}

// TestTokenForwardedAsBearerMetadata is THE auth-mechanism test: it confirms a
// unary call carries "authorization: Bearer <token>" so the server's auth
// interceptor can validate it. If this regresses, every authenticated command
// silently breaks.
func TestTokenForwardedAsBearerMetadata(t *testing.T) {
	lis, rec := startServer(t)
	conn := dialBufconn(t, lis, "my-jwt-123")

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	if _, err := NewAuth(conn).Login(ctx, &authv1.LoginRequest{Email: "a@b.co"}); err != nil {
		t.Fatalf("Login: %v", err)
	}

	got := rec.gotMD.Get("authorization")
	if len(got) != 1 {
		t.Fatalf("authorization metadata = %v, want exactly one value", got)
	}
	if got[0] != "Bearer my-jwt-123" {
		t.Errorf("authorization = %q, want %q", got[0], "Bearer my-jwt-123")
	}
}

// TestNoTokenSendsNoAuthHeader confirms the login bootstrap path sends NO
// authorization header (there is no token yet).
func TestNoTokenSendsNoAuthHeader(t *testing.T) {
	lis, rec := startServer(t)
	conn := dialBufconn(t, lis, "") // empty token

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	if _, err := NewAuth(conn).Login(ctx, &authv1.LoginRequest{Email: "a@b.co"}); err != nil {
		t.Fatalf("Login: %v", err)
	}
	if got := rec.gotMD.Get("authorization"); len(got) != 0 {
		t.Errorf("expected no authorization header, got %v", got)
	}
}

// TestTokenDialOptionsEmpty: empty token yields zero options (no interceptors).
func TestTokenDialOptionsEmpty(t *testing.T) {
	if opts := TokenDialOptions(""); len(opts) != 0 {
		t.Errorf("TokenDialOptions(\"\") returned %d opts, want 0", len(opts))
	}
	if opts := TokenDialOptions("x"); len(opts) != 2 {
		t.Errorf("TokenDialOptions(token) returned %d opts, want 2 (unary+stream)", len(opts))
	}
}

// ----------------------------------------------------------------------------
// Fix 2: loopback guard — refuse cleartext credentials to non-loopback hosts
// ----------------------------------------------------------------------------

// TestIsLoopback exercises the helper directly for all recognised loopback
// forms and a representative set of non-loopback values.
func TestIsLoopback(t *testing.T) {
	cases := []struct {
		host string
		want bool
	}{
		// Loopback — must be accepted in plaintext mode.
		{"localhost", true},
		{"127.0.0.1", true},
		{"127.0.0.2", true},    // whole 127.0.0.0/8 range
		{"127.255.255.255", true},
		{"::1", true},          // IPv6 loopback
		// Non-loopback — must be refused in plaintext mode.
		{"example.com", false},
		{"gateway.internal", false},
		{"10.0.0.1", false},    // RFC1918 private, but not loopback
		{"192.168.1.1", false},
		{"0.0.0.0", false},     // unspecified, not loopback
		{"::ffff:1.2.3.4", false}, // IPv4-mapped IPv6, not loopback
	}
	for _, c := range cases {
		t.Run(c.host, func(t *testing.T) {
			if got := isLoopback(c.host); got != c.want {
				t.Errorf("isLoopback(%q) = %v, want %v", c.host, got, c.want)
			}
		})
	}
}

// TestDialLoopbackAccepted verifies that Dial succeeds (returns a ClientConn
// without error) for loopback addresses without --tls. We use a bufconn listener
// for localhost and verify the conn is non-nil. We do NOT actually send an RPC
// here — grpc.NewClient is lazy, so the error surface for these tests is the
// loopback guard itself, not a real dial.
func TestDialLoopbackAccepted(t *testing.T) {
	loopbackAddrs := []string{
		"localhost:50051",
		"127.0.0.1:50051",
		"[::1]:50051",
	}
	for _, addr := range loopbackAddrs {
		t.Run(addr, func(t *testing.T) {
			conn, err := Dial(addr, "tok", false /* useTLS=false */)
			if err != nil {
				t.Errorf("Dial(%q, ..., false): unexpected error: %v", addr, err)
				return
			}
			conn.Close()
		})
	}
}

// TestDialNonLoopbackRefusedWithoutTLS is the security-critical case: Dial must
// return a non-nil error for any non-loopback address when useTLS is false,
// preventing the bearer JWT from ever being sent in cleartext to a remote host.
func TestDialNonLoopbackRefusedWithoutTLS(t *testing.T) {
	nonLoopbackAddrs := []string{
		"example.com:443",
		"gateway.example.com:443",
		"10.0.0.1:8080",
		"192.168.1.1:8080",
	}
	for _, addr := range nonLoopbackAddrs {
		t.Run(addr, func(t *testing.T) {
			conn, err := Dial(addr, "tok", false /* useTLS=false */)
			if err == nil {
				conn.Close()
				t.Errorf("Dial(%q, ..., false): expected cleartext refusal error, got nil", addr)
				return
			}
			// The error must mention the address and give actionable guidance.
			if !strings.Contains(err.Error(), "cleartext") {
				t.Errorf("Dial(%q) error %q should contain 'cleartext'", addr, err.Error())
			}
			if !strings.Contains(err.Error(), "--tls") {
				t.Errorf("Dial(%q) error %q should mention '--tls'", addr, err.Error())
			}
		})
	}
}

// TestDialNonLoopbackAllowedWithTLS: with --tls, any host is permitted (the
// connection is encrypted, so forwarding a bearer token is safe).
func TestDialNonLoopbackAllowedWithTLS(t *testing.T) {
	// grpc.NewClient with TLS credentials succeeds even if nothing is listening;
	// the connection is lazy and the loopback guard must NOT fire for useTLS=true.
	// We use a clearly non-local addr to ensure the guard path is not taken.
	conn, err := Dial("example.com:443", "tok", true /* useTLS=true */)
	if err != nil {
		t.Errorf("Dial(example.com:443, ..., true): unexpected error: %v", err)
		return
	}
	conn.Close()
}
