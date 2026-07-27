// Package clientfactory dials Forgepoint gRPC services, attaches the stored
// bearer token to every outgoing call, and hands back the generated client
// stubs. It is the one place that knows how the CLI authenticates to the mesh.
//
// ============================================================================
// WHY A CLI IS A THIN gRPC CLIENT (the mental model)
// ============================================================================
//
// `fp` holds NO business logic. Every command is: dial the right service →
// attach the user's token → call one RPC → format the response. The servers
// own validation, authorization, persistence, and orchestration. This is the
// whole point of an API-first platform: the proto contract is the product, and
// the CLI, the web BFF, and the Python SDK are interchangeable thin clients
// over the SAME gRPC surface. If a rule isn't enforced by the server, the CLI
// must not pretend to enforce it.
//
// ============================================================================
// TOKEN FORWARDING — how the CLI authenticates each call
// ============================================================================
//
// After `fp login`, the JWT lives in ~/.forgepoint/token. For every OTHER
// command we must present it to the service so the server's auth interceptor
// (which calls AuthService.ValidateToken) accepts the request. gRPC carries
// per-call credentials in METADATA — the HTTP/2 header bag. The convention,
// shared with HTTP, is:
//
//	authorization: Bearer <jwt>
//
// We inject that header with a CLIENT-SIDE UNARY + STREAM INTERCEPTOR that calls
// metadata.AppendToOutgoingContext before the RPC leaves. Doing it in an
// interceptor (rather than per-call) guarantees EVERY call on the connection is
// authenticated and that no command can forget to add the header.
//
//	┌──────────┐  authorization: Bearer <jwt>   ┌──────────────────────┐
//	│  fp CLI  │ ─────────(metadata)──────────▶ │  Service gRPC server  │
//	└──────────┘                                 │  auth interceptor:    │
//	                                             │   ValidateToken(jwt)  │
//	                                             │   CheckPermission(..) │
//	                                             └──────────────────────┘
//
// NOTE: the server reads this with metadata.FromIncomingContext and
// the "authorization" key. metadata keys are lowercased by gRPC, so the server
// looks up "authorization" regardless of how we cased it here. We send it
// lowercase to match.
//
// ============================================================================
// TRANSPORT SECURITY
// ============================================================================
//
// For the dev/port-forward workflow the connection is plaintext (insecure
// transport): traffic flows over a localhost tunnel that kubectl already
// secured end-to-end to the cluster. For a PROD endpoint you would dial with
// credentials.NewTLS(...) and, ideally, send the bearer token only over TLS so
// the credential is never on the wire in clear text. That switch is a single
// DialOption; see useTLS below.
// ============================================================================
package clientfactory

import (
	"context"
	"fmt"
	"net"

	authv1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/auth/v1"
	billingv1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/billing/v1"
	experimentv1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/experiment/v1"
	monitorv1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/monitor/v1"
	pipelinev1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/pipeline/v1"
	registryv1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/registry/v1"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

// authHeader is the metadata key carrying the bearer token. gRPC lowercases
// metadata keys on the wire, so this must be lowercase to match what the
// server's metadata.FromIncomingContext lookup expects.
const authHeader = "authorization"

// ============================================================================
// CLIENT INTERFACES
// ============================================================================
//
// WHY redeclare small interfaces instead of using the generated *ServiceClient
// types directly: TESTABILITY. The generated interfaces are large (every RPC of
// the service). Commands only need a handful of methods, and tests want to mock
// just those — without a real connection. By depending on these narrow,
// CLI-local interfaces (Interface Segregation Principle), each command's test
// can supply a tiny fake that records the context it was called with (to assert
// the token was forwarded) and returns canned responses.
//
// The generated *ServiceClient types satisfy these by structural typing, so the
// real clients drop in with no adapter.
// ============================================================================

// AuthClient is the slice of AuthService the CLI uses (just Login today).
type AuthClient interface {
	Login(ctx context.Context, in *authv1.LoginRequest, opts ...grpc.CallOption) (*authv1.LoginResponse, error)
}

// RegistryClient is the slice of RegistryService the CLI uses.
type RegistryClient interface {
	RegisterModel(ctx context.Context, in *registryv1.RegisterModelRequest, opts ...grpc.CallOption) (*registryv1.RegisterModelResponse, error)
	GetModel(ctx context.Context, in *registryv1.GetModelRequest, opts ...grpc.CallOption) (*registryv1.GetModelResponse, error)
	ListModels(ctx context.Context, in *registryv1.ListModelsRequest, opts ...grpc.CallOption) (*registryv1.ListModelsResponse, error)
}

// PipelineClient is the slice of PipelineOrchestratorService the CLI uses.
// WatchExecution returns a server-streaming client we consume update-by-update.
type PipelineClient interface {
	ListPipelines(ctx context.Context, in *pipelinev1.ListPipelinesRequest, opts ...grpc.CallOption) (*pipelinev1.ListPipelinesResponse, error)
	TriggerExecution(ctx context.Context, in *pipelinev1.TriggerExecutionRequest, opts ...grpc.CallOption) (*pipelinev1.TriggerExecutionResponse, error)
	GetExecution(ctx context.Context, in *pipelinev1.GetExecutionRequest, opts ...grpc.CallOption) (*pipelinev1.GetExecutionResponse, error)
	ListExecutions(ctx context.Context, in *pipelinev1.ListExecutionsRequest, opts ...grpc.CallOption) (*pipelinev1.ListExecutionsResponse, error)
	WatchExecution(ctx context.Context, in *pipelinev1.WatchExecutionRequest, opts ...grpc.CallOption) (grpc.ServerStreamingClient[pipelinev1.WatchExecutionResponse], error)
}

// ExperimentClient is the slice of ExperimentTrackerService the CLI uses.
type ExperimentClient interface {
	GetRun(ctx context.Context, in *experimentv1.GetRunRequest, opts ...grpc.CallOption) (*experimentv1.GetRunResponse, error)
	ListRuns(ctx context.Context, in *experimentv1.ListRunsRequest, opts ...grpc.CallOption) (*experimentv1.ListRunsResponse, error)
}

// MonitorClient is the slice of MonitorService the CLI uses.
type MonitorClient interface {
	ListMonitors(ctx context.Context, in *monitorv1.ListMonitorsRequest, opts ...grpc.CallOption) (*monitorv1.ListMonitorsResponse, error)
	ListDriftReports(ctx context.Context, in *monitorv1.ListDriftReportsRequest, opts ...grpc.CallOption) (*monitorv1.ListDriftReportsResponse, error)
}

// BillingClient is the slice of BillingService the CLI uses.
type BillingClient interface {
	GetUsage(ctx context.Context, in *billingv1.GetUsageRequest, opts ...grpc.CallOption) (*billingv1.GetUsageResponse, error)
}

// ============================================================================
// DIALING
// ============================================================================

// isLoopback reports whether host refers to a loopback interface.
//
// We treat three forms as loopback:
//   - the literal string "localhost" (DNS resolves this to 127.0.0.1/::1, but we
//     accept it without a round-trip to the resolver so the check is offline and
//     fast).
//   - any IP in 127.0.0.0/8 (IPv4 loopback range per RFC 5735).
//   - the IPv6 loopback address ::1.
//
// The stdlib net.IP.IsLoopback() covers the IP cases; we add the "localhost"
// literal so the most common dev invocation ("--addr localhost:8080") works
// without a DNS lookup.
//
// WHY this function exists: net.SplitHostPort gives us just the hostname, which
// may be a bare IP or a hostname; net.ParseIP handles both numeric forms, and
// IsLoopback() classifies them correctly.
func isLoopback(host string) bool {
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

// Dial opens a gRPC connection to addr, wiring in the token-forwarding
// interceptors. token may be empty for the one call that must NOT require auth:
// `fp login` (the Login RPC mints the token, so there's nothing to send yet).
//
// The caller owns the returned *grpc.ClientConn and must Close it. For a CLI we
// dial fresh per invocation and close on exit — connection pooling/reuse buys a
// long-running server nothing here, and a per-call connection keeps the code
// dead simple (no shared state, no leak risk across commands).
//
// ============================================================================
// CLEARTEXT CREDENTIAL GUARD
// ============================================================================
//
// When useTLS is false (the default dev/port-forward mode) the connection is
// plaintext. That's fine while the endpoint is localhost (the tunnel kubectl
// port-forward creates already provides an encrypted channel to the cluster).
// But if a user accidentally points --addr at a remote host without --tls, the
// bearer JWT would travel over an unencrypted connection — a credential-in-clear
// vulnerability. We detect this and refuse: parse the host with net.SplitHostPort
// and check isLoopback. Non-loopback + !useTLS → hard error before any dial.
//
// NOTE: this is defence-in-depth at the CLI layer. The server also
// enforces TLS (mutual or one-way) in production, but client-side refusal means
// the secret never leaves the machine in clear text even if the server were
// misconfigured.
// ============================================================================
func Dial(addr, token string, useTLS bool) (*grpc.ClientConn, error) {
	// Guard: refuse to forward credentials over a plaintext connection to a
	// non-loopback host. We parse the host out of addr (which may be host:port or
	// just host) using net.SplitHostPort; if there is no port SplitHostPort will
	// error, in which case we treat the whole string as the host.
	if !useTLS {
		host, _, err := net.SplitHostPort(addr)
		if err != nil {
			// No port in addr — treat the whole value as the host.
			host = addr
		}
		if !isLoopback(host) {
			return nil, fmt.Errorf(
				"refusing to send credentials in cleartext to %s: pass --tls (or use a localhost port-forward)",
				addr,
			)
		}
	}

	var transport grpc.DialOption
	if useTLS {
		// Production: real TLS. We omit a custom config so it uses the system
		// cert pool — sufficient for services behind a properly-certificated
		// gateway/ingress.
		transport = grpc.WithTransportCredentials(credentials.NewTLS(nil))
	} else {
		// Dev/port-forward: plaintext over the localhost tunnel.
		transport = grpc.WithTransportCredentials(insecure.NewCredentials())
	}

	tokenOpts := TokenDialOptions(token)
	opts := make([]grpc.DialOption, 0, 1+len(tokenOpts))
	opts = append(opts, transport)
	opts = append(opts, tokenOpts...)

	// grpc.NewClient is the modern, non-deprecated constructor (replaces
	// grpc.Dial/DialContext). It creates the ClientConn lazily; the first RPC
	// triggers the actual connect, so dial errors surface as Unavailable on the
	// call — which our cmderr layer already maps to a friendly message.
	return grpc.NewClient(addr, opts...)
}

// TokenDialOptions returns the DialOptions that attach the bearer token to
// every unary and streaming call. Exported so tests can build a bufconn-backed
// connection that still goes through the REAL token-forwarding interceptors —
// i.e. the test exercises the exact mechanism production uses, not a stand-in.
// An empty token yields no options (the unauthenticated `fp login` bootstrap).
func TokenDialOptions(token string) []grpc.DialOption {
	if token == "" {
		return nil
	}
	return []grpc.DialOption{
		grpc.WithUnaryInterceptor(unaryTokenInterceptor(token)),
		grpc.WithStreamInterceptor(streamTokenInterceptor(token)),
	}
}

// unaryTokenInterceptor returns a client interceptor that appends the bearer
// token to the outgoing metadata of every unary RPC.
//
// This is the exact mechanism the task calls out: metadata.AppendToOutgoingContext
// adds "authorization: Bearer <token>" to the context, and gRPC serializes
// outgoing-context metadata into HTTP/2 headers for the request.
func unaryTokenInterceptor(token string) grpc.UnaryClientInterceptor {
	bearer := "Bearer " + token
	return func(
		ctx context.Context,
		method string,
		req, reply any,
		cc *grpc.ClientConn,
		invoker grpc.UnaryInvoker,
		opts ...grpc.CallOption,
	) error {
		ctx = metadata.AppendToOutgoingContext(ctx, authHeader, bearer)
		return invoker(ctx, method, req, reply, cc, opts...)
	}
}

// streamTokenInterceptor does the same for streaming RPCs (e.g. WatchExecution).
// Streams need their own interceptor type — a unary interceptor never fires for
// a stream — so without this, `fp pipelines watch` would go out UNAUTHENTICATED.
// This is a classic gotcha.
func streamTokenInterceptor(token string) grpc.StreamClientInterceptor {
	bearer := "Bearer " + token
	return func(
		ctx context.Context,
		desc *grpc.StreamDesc,
		cc *grpc.ClientConn,
		method string,
		streamer grpc.Streamer,
		opts ...grpc.CallOption,
	) (grpc.ClientStream, error) {
		ctx = metadata.AppendToOutgoingContext(ctx, authHeader, bearer)
		return streamer(ctx, desc, cc, method, opts...)
	}
}

// ============================================================================
// TYPED CONSTRUCTORS
// ============================================================================
//
// Thin wrappers over the generated NewXxxServiceClient so commands don't import
// the generated packages directly and so the return types are the narrow CLI
// interfaces (keeping commands decoupled from the full generated surface).

// NewAuth wraps a conn as an AuthClient.
func NewAuth(cc grpc.ClientConnInterface) AuthClient {
	return authv1.NewAuthServiceClient(cc)
}

// NewRegistry wraps a conn as a RegistryClient.
func NewRegistry(cc grpc.ClientConnInterface) RegistryClient {
	return registryv1.NewRegistryServiceClient(cc)
}

// NewPipeline wraps a conn as a PipelineClient.
func NewPipeline(cc grpc.ClientConnInterface) PipelineClient {
	return pipelinev1.NewPipelineOrchestratorServiceClient(cc)
}

// NewExperiment wraps a conn as an ExperimentClient.
func NewExperiment(cc grpc.ClientConnInterface) ExperimentClient {
	return experimentv1.NewExperimentTrackerServiceClient(cc)
}

// NewMonitor wraps a conn as a MonitorClient.
func NewMonitor(cc grpc.ClientConnInterface) MonitorClient {
	return monitorv1.NewMonitorServiceClient(cc)
}

// NewBilling wraps a conn as a BillingClient.
func NewBilling(cc grpc.ClientConnInterface) BillingClient {
	return billingv1.NewBillingServiceClient(cc)
}
