// Package router assembles the BFF's HTTP surface: it constructs every handler
// from the gRPC stubs and wires the Go 1.22 net/http.ServeMux with method+path
// pattern routing, splitting PUBLIC (login, health) from PROTECTED (everything
// else, guarded by the token-forwarding auth middleware), then wraps the whole
// thing in the cross-cutting middleware chain.
//
// ============================================================================
// WHY STDLIB net/http ServeMux (no framework)
// ============================================================================
//
// Go 1.22's ServeMux gained METHOD + WILDCARD patterns ("GET /api/v1/models/{id}")
// and r.PathValue("id") — the exact ergonomics that used to require chi/gorilla.
// For a BFF whose routes are a flat, well-known list, the stdlib mux is now
// sufficient, so we take ZERO router dependency (the task's constraint and good
// supply-chain hygiene: fewer deps = smaller attack surface). Method-specific
// patterns also give us 405 Method Not Allowed for free.
//
// THE PUBLIC/PROTECTED SPLIT (interview point): rather than check auth inside
// each handler (easy to forget one = an auth bypass), we register protected
// routes on a SEPARATE mux wrapped ONCE in RequireAuth, and mount it under the
// root mux. Auth is then structural — a new protected route is automatically
// guarded because it lives on the protected mux. Login and health are the only
// routes on the public mux. This "secure by construction" layout is the same
// reason grpcutil uses a default-deny interceptor with an explicit skip list.
// ============================================================================
package router

import (
	"log/slog"
	"net/http"

	"github.com/abd-ulbasit/forgepoint/services/bff/internal/clients"
	"github.com/abd-ulbasit/forgepoint/services/bff/internal/handlers"
	"github.com/abd-ulbasit/forgepoint/services/bff/internal/httpx"
)

// Config carries everything the router needs to build the handler tree.
type Config struct {
	Clients        *clients.Clients
	Logger         *slog.Logger
	AllowedOrigins []string
}

// New builds the fully-wired http.Handler (mux + middleware) for the BFF.
func New(cfg Config) http.Handler {
	cl := cfg.Clients
	log := cfg.Logger

	// Construct handlers, each given only the segmented stub interface it needs
	// (the concrete *ServiceClient satisfies the small interface structurally).
	authH := handlers.NewAuthHandler(cl.Auth, log)
	modelsH := handlers.NewModelsHandler(cl.Registry, log)
	pipelinesH := handlers.NewPipelinesHandler(cl.Pipeline, log)
	experimentsH := handlers.NewExperimentsHandler(cl.Experiment, log)
	monitorsH := handlers.NewMonitorsHandler(cl.Monitor, log)
	billingH := handlers.NewBillingHandler(cl.Billing, log)
	notificationsH := handlers.NewNotificationsHandler(cl.Notification, log)
	dashboardH := handlers.NewDashboardHandler(cl.Registry, cl.Pipeline, cl.Monitor, cl.Billing, log)

	// ---- PROTECTED routes (require a forwarded bearer token) -----------------
	protected := http.NewServeMux()

	// Models
	protected.HandleFunc("GET /api/v1/models", modelsH.List)
	protected.HandleFunc("POST /api/v1/models", modelsH.Register)
	protected.HandleFunc("GET /api/v1/models/{id}", modelsH.Get)
	protected.HandleFunc("GET /api/v1/models/{id}/versions", modelsH.Versions)

	// Pipelines + executions
	protected.HandleFunc("GET /api/v1/pipelines", pipelinesH.List)
	protected.HandleFunc("POST /api/v1/pipelines", pipelinesH.Create)
	protected.HandleFunc("POST /api/v1/pipelines/{id}/start", pipelinesH.Start)
	protected.HandleFunc("GET /api/v1/executions/{id}", pipelinesH.GetExecution)
	protected.HandleFunc("GET /api/v1/executions/{id}/watch", pipelinesH.Watch)

	// Experiments (runs)
	protected.HandleFunc("GET /api/v1/runs", experimentsH.ListRuns)
	protected.HandleFunc("GET /api/v1/runs/{id}", experimentsH.GetRun)

	// Monitors + drift
	protected.HandleFunc("GET /api/v1/monitors", monitorsH.List)
	protected.HandleFunc("GET /api/v1/drift-reports", monitorsH.DriftReports)

	// Billing
	protected.HandleFunc("GET /api/v1/usage", billingH.Usage)

	// Notifications
	protected.HandleFunc("GET /api/v1/notifications", notificationsH.List)

	// Aggregation
	protected.HandleFunc("GET /api/v1/dashboard", dashboardH.Dashboard)

	// Guard the ENTIRE protected mux with RequireAuth ONCE. Every route above is
	// now token-gated by construction.
	protectedWithAuth := httpx.RequireAuth(protected)

	// ---- ROOT mux: public routes + mount the protected sub-tree --------------
	root := http.NewServeMux()

	// PUBLIC: login (mints/obtains the token — cannot itself require one).
	root.HandleFunc("POST /api/v1/login", authH.Login)

	// Mount the protected sub-tree under /api/v1/. The login route above is
	// registered as a MORE SPECIFIC pattern than "/api/v1/", and Go 1.22's mux
	// prefers the most specific match, so POST /api/v1/login keeps hitting the
	// public handler while every other /api/v1/* path falls through to the
	// auth-guarded mux.
	root.Handle("/api/v1/", protectedWithAuth)

	// ---- Cross-cutting middleware (OUTERMOST first) --------------------------
	// recover (catch panics anywhere) → log → CORS (answer preflight, stamp
	// headers) → BodyLimit (cap all request bodies at 1 MiB).
	//
	// WHY BodyLimit is INNERMOST of the chain (last to wrap, first middleware
	// the handler sees): the cap must apply to the actual body read, which
	// happens inside handlers. Placing it after CORS/Logging means the logging
	// and CORS headers are still set even if the body is oversized. Auth is NOT
	// here — it is applied only to the protected sub-tree above, so
	// login/preflight stay reachable without a token. Crucially, BodyLimit still
	// covers the public POST /api/v1/login endpoint because it wraps the root
	// mux which contains that route.
	const oneMiB = 1 << 20 // 1 048 576 bytes
	chain := httpx.Chain(
		httpx.Recover(log),
		httpx.Logging(log),
		httpx.CORS(cfg.AllowedOrigins),
		httpx.BodyLimit(oneMiB),
	)
	return chain(root)
}
