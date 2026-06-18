package commands

import (
	"bytes"
	"context"
	"net"
	"strings"
	"testing"
	"time"

	authv1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/auth/v1"
	billingv1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/billing/v1"
	experimentv1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/experiment/v1"
	monitorv1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/monitor/v1"
	pipelinev1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/pipeline/v1"
	registryv1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/registry/v1"

	"github.com/abd-ulbasit/forgepoint/cli/internal/clientfactory"
	"github.com/abd-ulbasit/forgepoint/cli/internal/cmderr"
	"github.com/abd-ulbasit/forgepoint/cli/internal/config"
	"github.com/abd-ulbasit/forgepoint/cli/internal/token"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

// ============================================================================
// TEST HARNESS
// ============================================================================
//
// We stand up an in-memory gRPC server that implements just the RPCs the CLI
// calls, and build an App whose Dialer connects to it through the REAL token
// DialOptions (clientfactory.TokenDialOptions). That means these tests exercise
// the genuine path: dispatch → requireToken → dial-with-token → RPC → render.
// Each stub records the metadata it received so we can assert token forwarding.
// ============================================================================

// fakeBackend implements all the service servers the CLI talks to and records
// the authorization metadata of the last call to any of them.
type fakeBackend struct {
	authv1.UnimplementedAuthServiceServer
	registryv1.UnimplementedRegistryServiceServer
	pipelinev1.UnimplementedPipelineOrchestratorServiceServer
	experimentv1.UnimplementedExperimentTrackerServiceServer
	monitorv1.UnimplementedMonitorServiceServer
	billingv1.UnimplementedBillingServiceServer

	lastAuthHeader string
	// forceErr, when set, makes the next call return this status code.
	forceErr codes.Code
}

func (f *fakeBackend) capture(ctx context.Context) {
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		if vals := md.Get("authorization"); len(vals) > 0 {
			f.lastAuthHeader = vals[0]
		}
	}
}

func (f *fakeBackend) maybeErr() error {
	if f.forceErr != codes.OK {
		c := f.forceErr
		f.forceErr = codes.OK
		return status.Error(c, "forced")
	}
	return nil
}

func (f *fakeBackend) Login(ctx context.Context, _ *authv1.LoginRequest) (*authv1.LoginResponse, error) {
	f.capture(ctx)
	if err := f.maybeErr(); err != nil {
		return nil, err
	}
	return &authv1.LoginResponse{
		AccessToken: "fresh-jwt",
		User:        &authv1.User{Name: "Ada"},
	}, nil
}

func (f *fakeBackend) ListModels(ctx context.Context, _ *registryv1.ListModelsRequest) (*registryv1.ListModelsResponse, error) {
	f.capture(ctx)
	if err := f.maybeErr(); err != nil {
		return nil, err
	}
	return &registryv1.ListModelsResponse{
		Models: []*registryv1.Model{
			{Id: "m1", Name: "fraud", Framework: "pytorch", TaskType: "classification"},
		},
	}, nil
}

func (f *fakeBackend) RegisterModel(ctx context.Context, req *registryv1.RegisterModelRequest) (*registryv1.RegisterModelResponse, error) {
	f.capture(ctx)
	if err := f.maybeErr(); err != nil {
		return nil, err
	}
	return &registryv1.RegisterModelResponse{Model: &registryv1.Model{Id: "new-id", Name: req.GetName()}}, nil
}

func (f *fakeBackend) ListPipelines(ctx context.Context, _ *pipelinev1.ListPipelinesRequest) (*pipelinev1.ListPipelinesResponse, error) {
	f.capture(ctx)
	if err := f.maybeErr(); err != nil {
		return nil, err
	}
	return &pipelinev1.ListPipelinesResponse{
		Pipelines: []*pipelinev1.PipelineDefinition{
			{Id: "p1", Name: "deploy", Type: pipelinev1.PipelineType_PIPELINE_TYPE_DEPLOYMENT_SAGA},
		},
	}, nil
}

func (f *fakeBackend) GetExecution(ctx context.Context, _ *pipelinev1.GetExecutionRequest) (*pipelinev1.GetExecutionResponse, error) {
	f.capture(ctx)
	if err := f.maybeErr(); err != nil {
		return nil, err
	}
	return &pipelinev1.GetExecutionResponse{Execution: &pipelinev1.Execution{
		Id: "e1", PipelineId: "p1", Status: pipelinev1.ExecutionStatus_EXECUTION_STATUS_RUNNING,
	}}, nil
}

// WatchExecution streams two updates then a terminal one and closes — exercising
// the CLI's stream-consume loop and terminal-state early exit.
func (f *fakeBackend) WatchExecution(_ *pipelinev1.WatchExecutionRequest, stream grpc.ServerStreamingServer[pipelinev1.WatchExecutionResponse]) error {
	f.capture(stream.Context())
	updates := []pipelinev1.ExecutionStatus{
		pipelinev1.ExecutionStatus_EXECUTION_STATUS_RUNNING,
		pipelinev1.ExecutionStatus_EXECUTION_STATUS_COMPLETED,
	}
	for i, st := range updates {
		if err := stream.Send(&pipelinev1.WatchExecutionResponse{
			Sequence:  uint64(i + 1),
			Execution: &pipelinev1.Execution{Id: "e1", Status: st},
		}); err != nil {
			return err
		}
	}
	return nil
}

func (f *fakeBackend) ListRuns(ctx context.Context, _ *experimentv1.ListRunsRequest) (*experimentv1.ListRunsResponse, error) {
	f.capture(ctx)
	if err := f.maybeErr(); err != nil {
		return nil, err
	}
	return &experimentv1.ListRunsResponse{
		Runs: []*experimentv1.Run{
			{Id: "r1", DisplayName: "baseline", Status: experimentv1.RunStatus_RUN_STATUS_FINISHED},
		},
	}, nil
}

func (f *fakeBackend) ListMonitors(ctx context.Context, _ *monitorv1.ListMonitorsRequest) (*monitorv1.ListMonitorsResponse, error) {
	f.capture(ctx)
	if err := f.maybeErr(); err != nil {
		return nil, err
	}
	return &monitorv1.ListMonitorsResponse{
		Entries: []*monitorv1.ListMonitorsResponse_Entry{
			{
				Monitor: &monitorv1.Monitor{Id: "mon1", ModelName: "fraud", State: monitorv1.MonitorState_MONITOR_STATE_ACTIVE},
				Health:  &monitorv1.ModelHealth{OverallSeverity: monitorv1.DriftSeverity_DRIFT_SEVERITY_OK},
			},
		},
	}, nil
}

func (f *fakeBackend) ListDriftReports(ctx context.Context, _ *monitorv1.ListDriftReportsRequest) (*monitorv1.ListDriftReportsResponse, error) {
	f.capture(ctx)
	if err := f.maybeErr(); err != nil {
		return nil, err
	}
	return &monitorv1.ListDriftReportsResponse{
		Reports: []*monitorv1.DriftReport{
			{Id: "d1", ModelName: "fraud", Severity: monitorv1.DriftSeverity_DRIFT_SEVERITY_CRITICAL},
		},
	}, nil
}

func (f *fakeBackend) GetUsage(ctx context.Context, _ *billingv1.GetUsageRequest) (*billingv1.GetUsageResponse, error) {
	f.capture(ctx)
	if err := f.maybeErr(); err != nil {
		return nil, err
	}
	return &billingv1.GetUsageResponse{
		Summaries: []*billingv1.UsageSummary{
			{Team: "ml", ByMeter: map[string]*billingv1.MeterUsage{
				"INFERENCE_REQUEST": {TotalQuantity: 100, TotalCost: &billingv1.Money{AmountMicros: 5_000_000, CurrencyCode: "USD"}},
			}},
		},
		GrandTotal: &billingv1.Money{AmountMicros: 5_000_000, CurrencyCode: "USD"},
	}, nil
}

// harness bundles the App, the backend stub, and output buffers for assertions.
type harness struct {
	app     *App
	backend *fakeBackend
	stdout  *bytes.Buffer
	stderr  *bytes.Buffer
}

// newHarness builds the in-memory server + an App pointed at it. loggedIn
// controls whether a valid token is pre-seeded (to test the auth-required path).
func newHarness(t *testing.T, loggedIn bool) *harness {
	t.Helper()

	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	backend := &fakeBackend{}
	authv1.RegisterAuthServiceServer(srv, backend)
	registryv1.RegisterRegistryServiceServer(srv, backend)
	pipelinev1.RegisterPipelineOrchestratorServiceServer(srv, backend)
	experimentv1.RegisterExperimentTrackerServiceServer(srv, backend)
	monitorv1.RegisterMonitorServiceServer(srv, backend)
	billingv1.RegisterBillingServiceServer(srv, backend)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	// Token store in a temp dir.
	tokens := token.New(t.TempDir())
	if loggedIn {
		if err := tokens.Save(token.Stored{
			AccessToken: "stored-jwt",
			ExpiresAt:   time.Now().Add(time.Hour),
		}); err != nil {
			t.Fatalf("seed token: %v", err)
		}
	}

	resolver, err := config.NewResolver(config.Options{Env: func(string) string { return "" }})
	if err != nil {
		t.Fatalf("resolver: %v", err)
	}

	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}
	app := NewApp(stdout, stderr, strings.NewReader(""), resolver, tokens)

	// Override the Dialer to reach the bufconn server, threading the token
	// through the REAL TokenDialOptions so token forwarding is genuinely tested.
	app.Dial = func(addr, tok string, useTLS bool) (*grpc.ClientConn, error) {
		opts := []grpc.DialOption{
			grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
				return lis.DialContext(ctx)
			}),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		}
		opts = append(opts, clientfactory.TokenDialOptions(tok)...)
		return grpc.NewClient("passthrough:///bufconn", opts...)
	}

	return &harness{app: app, backend: backend, stdout: stdout, stderr: stderr}
}

// run executes the CLI with the given args and returns the error.
func (h *harness) run(args ...string) error { return h.app.Run(args) }

// ----------------------------------------------------------------------------
// Auth-required gating
// ----------------------------------------------------------------------------

// TestCommandsRequireLogin: every authenticated command must fail with the auth
// exit code and a "fp login" hint when no token is present.
func TestCommandsRequireLogin(t *testing.T) {
	cmds := [][]string{
		{"models", "list"},
		{"pipelines", "list"},
		{"runs", "list"},
		{"monitors", "list"},
		{"usage", "--team", "ml"},
	}
	for _, c := range cmds {
		t.Run(strings.Join(c, " "), func(t *testing.T) {
			h := newHarness(t, false) // NOT logged in
			err := h.run(c...)
			if err == nil {
				t.Fatal("expected auth error, got nil")
			}
			if cmderr.ExitCode(err) != cmderr.ExitAuth {
				t.Errorf("exit = %d, want ExitAuth(%d): %v", cmderr.ExitCode(err), cmderr.ExitAuth, err)
			}
			if !strings.Contains(err.Error(), "fp login") {
				t.Errorf("error %q should mention 'fp login'", err.Error())
			}
		})
	}
}

// TestExpiredTokenRequiresRelogin: a present-but-expired token is rejected
// offline with the auth exit code.
func TestExpiredTokenRequiresRelogin(t *testing.T) {
	h := newHarness(t, false)
	if err := h.app.Tokens.Save(token.Stored{AccessToken: "old", ExpiresAt: time.Now().Add(-time.Hour)}); err != nil {
		t.Fatal(err)
	}
	err := h.run("models", "list")
	if cmderr.ExitCode(err) != cmderr.ExitAuth {
		t.Errorf("exit = %d, want ExitAuth: %v", cmderr.ExitCode(err), err)
	}
	if !strings.Contains(err.Error(), "expired") {
		t.Errorf("error %q should mention expiry", err.Error())
	}
}

// ----------------------------------------------------------------------------
// Token forwarding through real commands
// ----------------------------------------------------------------------------

// TestAuthenticatedCommandsForwardToken asserts the stored token rides along as
// "Bearer <token>" on the actual RPC for representative commands across services.
func TestAuthenticatedCommandsForwardToken(t *testing.T) {
	cases := []struct {
		name string
		args []string
	}{
		{"models list", []string{"models", "list"}},
		{"pipelines list", []string{"pipelines", "list"}},
		{"runs list", []string{"runs", "list"}},
		{"monitors list", []string{"monitors", "list"}},
		{"monitors drift", []string{"monitors", "drift"}},
		{"usage", []string{"usage", "--team", "ml"}},
		{"pipelines status", []string{"pipelines", "status", "e1"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			h := newHarness(t, true)
			if err := h.run(c.args...); err != nil {
				t.Fatalf("run %v: %v", c.args, err)
			}
			if h.backend.lastAuthHeader != "Bearer stored-jwt" {
				t.Errorf("forwarded auth = %q, want %q", h.backend.lastAuthHeader, "Bearer stored-jwt")
			}
		})
	}
}

// TestWatchForwardsTokenOnStream verifies the STREAM interceptor attaches the
// token to a server-streaming call — the easy-to-miss case.
func TestWatchForwardsTokenOnStream(t *testing.T) {
	h := newHarness(t, true)
	if err := h.run("pipelines", "watch", "e1"); err != nil {
		t.Fatalf("watch: %v", err)
	}
	if h.backend.lastAuthHeader != "Bearer stored-jwt" {
		t.Errorf("stream auth = %q, want Bearer stored-jwt", h.backend.lastAuthHeader)
	}
	// The stream emitted a RUNNING then COMPLETED update; the loop should have
	// printed both sequence numbers and stopped on the terminal state.
	out := h.stdout.String()
	if !strings.Contains(out, "seq 1") || !strings.Contains(out, "seq 2") {
		t.Errorf("watch output missing updates:\n%s", out)
	}
	if !strings.Contains(out, "COMPLETED") {
		t.Errorf("watch output missing terminal status:\n%s", out)
	}
}

// ----------------------------------------------------------------------------
// Output formatting
// ----------------------------------------------------------------------------

// TestModelsListTableOutput checks the human table renders the model row.
func TestModelsListTableOutput(t *testing.T) {
	h := newHarness(t, true)
	if err := h.run("models", "list"); err != nil {
		t.Fatalf("run: %v", err)
	}
	out := h.stdout.String()
	for _, want := range []string{"ID", "NAME", "fraud", "pytorch"} {
		if !strings.Contains(out, want) {
			t.Errorf("output missing %q:\n%s", want, out)
		}
	}
}

// TestJSONFlagProducesJSON: --json before the command switches the output mode.
func TestJSONFlagProducesJSON(t *testing.T) {
	h := newHarness(t, true)
	if err := h.run("--json", "models", "list"); err != nil {
		t.Fatalf("run: %v", err)
	}
	out := strings.TrimSpace(h.stdout.String())
	if !strings.HasPrefix(out, "[") {
		t.Errorf("expected JSON array, got:\n%s", out)
	}
	if !strings.Contains(out, `"NAME": "fraud"`) {
		t.Errorf("JSON missing model name:\n%s", out)
	}
}

// ----------------------------------------------------------------------------
// Login flow
// ----------------------------------------------------------------------------

// TestLoginStoresToken: a successful login persists the returned JWT and does
// NOT forward any pre-existing token (the bootstrap call is unauthenticated).
func TestLoginStoresToken(t *testing.T) {
	h := newHarness(t, false)
	if err := h.run("login", "--email", "ada@co", "--password", "secret"); err != nil {
		t.Fatalf("login: %v", err)
	}
	// Login must NOT send an authorization header.
	if h.backend.lastAuthHeader != "" {
		t.Errorf("login forwarded a token (%q); it must be unauthenticated", h.backend.lastAuthHeader)
	}
	// The fresh token must be persisted.
	st, err := h.app.Tokens.Load()
	if err != nil {
		t.Fatalf("load token after login: %v", err)
	}
	if st.AccessToken != "fresh-jwt" {
		t.Errorf("stored token = %q, want fresh-jwt", st.AccessToken)
	}
	if !strings.Contains(h.stdout.String(), "Logged in as Ada") {
		t.Errorf("login output: %s", h.stdout.String())
	}
}

// TestLoginRequiresEmail: missing --email is a usage error.
func TestLoginRequiresEmail(t *testing.T) {
	h := newHarness(t, false)
	err := h.run("login", "--password", "x")
	if cmderr.ExitCode(err) != cmderr.ExitUsage {
		t.Errorf("exit = %d, want ExitUsage: %v", cmderr.ExitCode(err), err)
	}
}

// TestLoginBadCredentials: Unauthenticated from the server becomes a login-
// specific message, not the generic "run fp login".
func TestLoginBadCredentials(t *testing.T) {
	h := newHarness(t, false)
	h.backend.forceErr = codes.Unauthenticated
	err := h.run("login", "--email", "a@b.co", "--password", "wrong")
	if cmderr.ExitCode(err) != cmderr.ExitAuth {
		t.Errorf("exit = %d, want ExitAuth: %v", cmderr.ExitCode(err), err)
	}
	if !strings.Contains(err.Error(), "invalid email or password") {
		t.Errorf("error %q should be credential-specific", err.Error())
	}
}

// ----------------------------------------------------------------------------
// gRPC error → exit code propagation through a command
// ----------------------------------------------------------------------------

// TestServerErrorMapsToExitCode: a PermissionDenied from the server surfaces as
// the forbidden exit code at the command boundary.
func TestServerErrorMapsToExitCode(t *testing.T) {
	h := newHarness(t, true)
	h.backend.forceErr = codes.PermissionDenied
	err := h.run("models", "list")
	if cmderr.ExitCode(err) != cmderr.ExitForbidden {
		t.Errorf("exit = %d, want ExitForbidden: %v", cmderr.ExitCode(err), err)
	}
}

// ----------------------------------------------------------------------------
// Dispatch
// ----------------------------------------------------------------------------

// TestDispatchErrors covers unknown command / subcommand / no-args paths.
func TestDispatchErrors(t *testing.T) {
	cases := []struct {
		name string
		args []string
	}{
		{"unknown command", []string{"frobnicate"}},
		{"no args", nil},
		{"unknown subcommand", []string{"models", "frobnicate"}},
		{"missing subcommand", []string{"models"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			h := newHarness(t, true)
			err := h.run(c.args...)
			if cmderr.ExitCode(err) != cmderr.ExitUsage {
				t.Errorf("exit = %d, want ExitUsage: %v", cmderr.ExitCode(err), err)
			}
		})
	}
}

// ----------------------------------------------------------------------------
// Fix 1: flag.ErrHelp (-h / --help) must exit 0, not 2
// ----------------------------------------------------------------------------

// TestHelpFlagExitsZero verifies that -h/--help at the top level and on a
// subcommand return nil (exit 0) rather than ExitUsage (exit 2).
//
// Convention: kubectl, gh, and aws all exit 0 on explicit help. A user asking
// for help should never see a non-zero exit code in their shell.
func TestHelpFlagExitsZero(t *testing.T) {
	cases := []struct {
		name string
		args []string
	}{
		// Top-level help via flag: flag.ContinueOnError returns flag.ErrHelp,
		// which our guard maps to nil.
		{"top-level --help", []string{"--help"}},
		{"top-level -h", []string{"-h"}},
		// Subcommand help: the subcommand's FlagSet also returns flag.ErrHelp.
		{"models list -h", []string{"models", "list", "-h"}},
		{"models list --help", []string{"models", "list", "--help"}},
		{"pipelines list -h", []string{"pipelines", "list", "-h"}},
		{"runs list -h", []string{"runs", "list", "-h"}},
		{"monitors list -h", []string{"monitors", "list", "-h"}},
		{"usage -h", []string{"usage", "-h"}},
		{"version -h", []string{"version", "-h"}},
		{"login -h", []string{"login", "-h"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			// loggedIn=true so we never hit auth errors before the flag parse.
			h := newHarness(t, true)
			err := h.run(c.args...)
			if err != nil {
				t.Errorf("args %v: want nil (exit 0), got %v (exit %d)",
					c.args, err, cmderr.ExitCode(err))
			}
		})
	}
}

// TestBareHelpCommandExitsZero covers `fp help` (the bare word, not a flag),
// which the dispatch switch handles separately.
func TestBareHelpCommandExitsZero(t *testing.T) {
	h := newHarness(t, false)
	if err := h.run("help"); err != nil {
		t.Errorf("fp help: want nil, got %v", err)
	}
}

// TestVersionNeedsNoAuth: version works without a token and prints the version.
func TestVersionNeedsNoAuth(t *testing.T) {
	h := newHarness(t, false)
	if err := h.run("version"); err != nil {
		t.Fatalf("version: %v", err)
	}
	if !strings.Contains(h.stdout.String(), "fp version") {
		t.Errorf("version output: %s", h.stdout.String())
	}
}

// TestRegisterModelRequiresName: register without --name is a usage error.
func TestRegisterModelRequiresName(t *testing.T) {
	h := newHarness(t, true)
	err := h.run("models", "register")
	if cmderr.ExitCode(err) != cmderr.ExitUsage {
		t.Errorf("exit = %d, want ExitUsage: %v", cmderr.ExitCode(err), err)
	}
}

// TestRegisterModelSucceeds drives the write path end to end.
func TestRegisterModelSucceeds(t *testing.T) {
	h := newHarness(t, true)
	if err := h.run("models", "register", "--name", "churn"); err != nil {
		t.Fatalf("register: %v", err)
	}
	if h.backend.lastAuthHeader != "Bearer stored-jwt" {
		t.Errorf("register forwarded auth = %q", h.backend.lastAuthHeader)
	}
	if !strings.Contains(h.stdout.String(), "churn") {
		t.Errorf("register output: %s", h.stdout.String())
	}
}
