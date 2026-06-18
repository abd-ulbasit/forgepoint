// Package clients owns the BFF's outbound gRPC connections to the platform's
// domain services.
//
// ============================================================================
// WHY A DEDICATED CLIENTS LAYER (connection reuse, not per-request dials)
// ============================================================================
//
// A gRPC ClientConn is a long-lived, concurrency-safe object that multiplexes
// MANY RPCs over a SINGLE HTTP/2 connection (HTTP/2 streams). Dialing a fresh
// connection per HTTP request would be a classic performance bug: every browser
// call would pay a TCP + HTTP/2 handshake, defeating the multiplexing that makes
// gRPC fast, and would leak file descriptors under load. So we dial ONCE at
// startup and share the resulting typed stubs for the process's lifetime.
//
// We use grpc.NewClient (the modern replacement for the deprecated grpc.Dial):
// it returns immediately with an IDLE connection and connects lazily on the
// first RPC (or eagerly if you call conn.Connect()). It does NOT block on a
// reachable server, which is exactly right for a BFF — a downstream being down
// at boot must NOT stop the BFF from starting (the dashboard degrades that tile,
// the rest of the UI still works). This is the partial-failure-tolerant posture
// the ADR calls for.
//
// TRANSPORT: insecure (h2c, plaintext HTTP/2). The BFF talks to services over
// the in-cluster pod network, where mTLS is the mesh's job (Istio sidecars,
// Phase 15), NOT the application's. Doing TLS here too would be redundant and
// would fight the mesh. In a meshless deployment you'd swap WithTransport
// credentials for real TLS — that's a config concern, isolated to this file.
//
// The struct exposes the generated *ServiceClient stubs directly. The BFF has
// ZERO business logic (ADR guardrail), so there is nothing to wrap them in —
// handlers call stubs, map proto<->JSON, and forward the caller's token. The
// only abstraction we add is small per-resource interfaces (see handlers) so the
// handlers are unit-testable against mocks without a live cluster.
// ============================================================================
package clients

import (
	"context"
	"fmt"
	"log/slog"

	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"

	aiv1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/ai/v1"
	authv1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/auth/v1"
	billingv1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/billing/v1"
	experimentv1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/experiment/v1"
	monitorv1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/monitor/v1"
	notificationv1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/notification/v1"
	pipelinev1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/pipeline/v1"
	registryv1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/registry/v1"
)

// Addresses holds the dial target for every downstream service. All are
// CONFIGURABLE (env, see cmd/server config) with in-cluster defaults of the
// form fp-<svc>.fp-system.svc.cluster.local:9090. model-serving lives in the
// fp-models namespace; the others in fp-system. The BFF itself does not call
// model-serving directly today (inference goes through the inference-gateway),
// so it is intentionally absent here — adding a dead dial would mean a health
// check guarding nothing real (same discipline as auth not wiring Redis).
type Addresses struct {
	Auth         string
	Registry     string
	Pipeline     string
	Experiment   string
	Monitor      string
	Billing      string
	Notification string
	// AIGateway is the LLM entry point (M7). Defaults to
	// fp-ai-gateway.fp-system.svc.cluster.local:9090. It is dialed like every
	// other downstream — same insecure in-cluster transport, same lazy connect —
	// because the BFF relays its ChatCompletion server-stream to the browser as
	// SSE (the chat playground) and proxies its ListProviders/GetUsage RPCs.
	AIGateway string
}

// Clients bundles the live gRPC connections and the typed stubs the handlers
// use. Each stub is a thin, concurrency-safe view over its shared ClientConn.
type Clients struct {
	// conns is kept ONLY so Close can drain every connection on shutdown.
	// Handlers never touch it — they use the typed stubs below.
	conns []*grpc.ClientConn

	// authConn/registryConn are kept by reference so the readiness probe can
	// inspect their connectivity state (the BFF's "can I do my core job" floor).
	authConn     *grpc.ClientConn
	registryConn *grpc.ClientConn

	Auth         authv1.AuthServiceClient
	Registry     registryv1.RegistryServiceClient
	Pipeline     pipelinev1.PipelineOrchestratorServiceClient
	Experiment   experimentv1.ExperimentTrackerServiceClient
	Monitor      monitorv1.MonitorServiceClient
	Billing      billingv1.BillingServiceClient
	Notification notificationv1.NotificationServiceClient
	AIGateway    aiv1.AIGatewayServiceClient
}

// Dial constructs one ClientConn per service and wires the typed stubs.
//
// It returns an error only if a target string is structurally invalid (an empty
// address) — NOT if a server is unreachable, because grpc.NewClient is lazy and
// a downstream being down at boot must not block BFF startup. On any error, the
// connections already opened are closed so we never leak a half-built set.
func Dial(logger *slog.Logger, addrs Addresses) (*Clients, error) {
	c := &Clients{}

	// dial is a closure so the seven near-identical dials read as a list, and so
	// a failure can roll back every connection opened so far via c.Close().
	dial := func(name, target string) (*grpc.ClientConn, error) {
		if target == "" {
			return nil, fmt.Errorf("clients: empty address for service %q", name)
		}
		conn, err := grpc.NewClient(
			target,
			// insecure: plaintext HTTP/2 in-cluster; the mesh owns mTLS (see header).
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		if err != nil {
			return nil, fmt.Errorf("clients: dial %s (%s): %w", name, target, err)
		}
		c.conns = append(c.conns, conn)
		logger.Info("bff gRPC client dialed",
			slog.String("service", name),
			slog.String("target", target),
		)
		return conn, nil
	}

	authConn, err := dial("auth", addrs.Auth)
	if err != nil {
		_ = c.Close()
		return nil, err
	}
	registryConn, err := dial("registry", addrs.Registry)
	if err != nil {
		_ = c.Close()
		return nil, err
	}
	pipelineConn, err := dial("pipeline", addrs.Pipeline)
	if err != nil {
		_ = c.Close()
		return nil, err
	}
	experimentConn, err := dial("experiment", addrs.Experiment)
	if err != nil {
		_ = c.Close()
		return nil, err
	}
	monitorConn, err := dial("monitor", addrs.Monitor)
	if err != nil {
		_ = c.Close()
		return nil, err
	}
	billingConn, err := dial("billing", addrs.Billing)
	if err != nil {
		_ = c.Close()
		return nil, err
	}
	notificationConn, err := dial("notification", addrs.Notification)
	if err != nil {
		_ = c.Close()
		return nil, err
	}
	aiGatewayConn, err := dial("ai-gateway", addrs.AIGateway)
	if err != nil {
		_ = c.Close()
		return nil, err
	}

	c.authConn = authConn
	c.registryConn = registryConn

	c.Auth = authv1.NewAuthServiceClient(authConn)
	c.Registry = registryv1.NewRegistryServiceClient(registryConn)
	c.Pipeline = pipelinev1.NewPipelineOrchestratorServiceClient(pipelineConn)
	c.Experiment = experimentv1.NewExperimentTrackerServiceClient(experimentConn)
	c.Monitor = monitorv1.NewMonitorServiceClient(monitorConn)
	c.Billing = billingv1.NewBillingServiceClient(billingConn)
	c.Notification = notificationv1.NewNotificationServiceClient(notificationConn)
	c.AIGateway = aiv1.NewAIGatewayServiceClient(aiGatewayConn)

	return c, nil
}

// AuthReachable / RegistryReachable are readiness checks for the BFF's two
// critical upstreams. They use the gRPC ClientConn's connectivity state rather
// than firing a real RPC: a probe should be cheap and side-effect-free, and we
// have no "ping" RPC to call without forging a request. Because connections are
// LAZY (IDLE until first use), we nudge the conn with Connect() so an
// otherwise-idle BFF reports honestly instead of forever "not ready".
//
// We treat Ready and Connecting as healthy-enough (Connecting means the dial is
// in progress, which is normal right after boot); TransientFailure/Shutdown are
// reported as not-ready so K8s pulls the pod until the upstream recovers. The
// passed ctx is the health package's bounded per-check context.
func (c *Clients) AuthReachable(ctx context.Context) error {
	return connReachable(ctx, "auth", c.authConn)
}

// RegistryReachable mirrors AuthReachable for the registry conn.
func (c *Clients) RegistryReachable(ctx context.Context) error {
	return connReachable(ctx, "registry", c.registryConn)
}

func connReachable(_ context.Context, name string, conn *grpc.ClientConn) error {
	if conn == nil {
		return fmt.Errorf("%s connection not initialized", name)
	}
	// Kick a lazy/idle conn toward connecting so readiness reflects reality.
	conn.Connect()
	switch s := conn.GetState(); s {
	case connectivity.Ready, connectivity.Connecting, connectivity.Idle:
		// Idle is acceptable: the conn is healthy but unused; Connect() above moves
		// it forward, and the first real RPC will establish it. Reporting ready
		// here avoids flapping a freshly-booted, traffic-less BFF out of service.
		return nil
	default: // TransientFailure, Shutdown
		return fmt.Errorf("%s connection not reachable (state: %s)", name, s)
	}
}

// Close drains every gRPC connection. Called from the composition root's
// deferred shutdown so in-flight RPCs the HTTP server already drained do not
// race a closing connection. conn.Close() is idempotent and safe on a
// never-connected (lazy) conn, so partial-construction rollback is safe too.
func (c *Clients) Close() error {
	var firstErr error
	for _, conn := range c.conns {
		if err := conn.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}
