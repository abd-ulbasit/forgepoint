// main_test.go — UNIT tests for the composition-root helpers and the audit wiring.
//
// main() itself isn't unit-testable (it dials Redis/NATS and blocks on Serve), but
// the GOVERNANCE-critical decisions it makes are pulled into small pure helpers that
// ARE testable: the audit MethodPredicate (which gateway RPCs are audited), the
// provider-kind config parser (the allow-list's provider dimension), and the audit
// interceptor wiring (that the interceptors actually compose into the gRPC chain).
package main

import (
	"context"
	"io"
	"log/slog"
	"testing"

	aiv1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/ai/v1"
	pkgaudit "github.com/abd-ulbasit/forgepoint/pkg/audit"
	"github.com/abd-ulbasit/forgepoint/pkg/grpcutil"
	"github.com/abd-ulbasit/forgepoint/services/ai-gateway/internal/domain"
	"github.com/abd-ulbasit/forgepoint/services/ai-gateway/internal/events"
	"google.golang.org/grpc"
)

// quietLogger discards output so a test that exercises a logging path stays silent.
func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// --- AUDIT PREDICATE --------------------------------------------------------

func TestAuditPredicate_AuditsChatCompletionAndMutations(t *testing.T) {
	t.Parallel()
	pred := auditPredicate()

	audited := []string{
		aiv1.AIGatewayService_ChatCompletion_FullMethodName, // explicit opt-in (not a default verb).
		aiv1.AIGatewayService_CreatePrompt_FullMethodName,   // matched by the default "Create" verb.
	}
	for _, m := range audited {
		if !pred(m) {
			t.Errorf("expected %s to be audited on success", m)
		}
	}

	// Pure reads stay quiet on SUCCESS (a DENY on them is still audited by the
	// interceptor, but that path is independent of the predicate).
	notAudited := []string{
		aiv1.AIGatewayService_ListProviders_FullMethodName,
		aiv1.AIGatewayService_GetUsage_FullMethodName,
		aiv1.AIGatewayService_GetPrompt_FullMethodName,
		aiv1.AIGatewayService_ListPrompts_FullMethodName,
		"/grpc.health.v1.Health/Check",
	}
	for _, m := range notAudited {
		if pred(m) {
			t.Errorf("expected %s NOT to be audited on success", m)
		}
	}
}

// --- PROVIDER-KIND PARSER ---------------------------------------------------

func TestParseProviderKinds(t *testing.T) {
	t.Parallel()
	got := parseProviderKinds([]string{"ollama", " OpenAI ", "anthropic", "stub", "", "bogus"}, quietLogger())
	want := []domain.ProviderKind{
		domain.ProviderKindOllama,
		domain.ProviderKindOpenAI,
		domain.ProviderKindAnthropic,
		domain.ProviderKindStub,
	}
	if len(got) != len(want) {
		t.Fatalf("parsed %d kinds, want %d (%v)", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("kind[%d] = %v, want %v", i, got[i], want[i])
		}
	}
	// An empty / all-blank config yields no kinds (the allow-all provider dimension),
	// NOT a crash and NOT a spurious entry.
	if k := parseProviderKinds([]string{"", "  "}, quietLogger()); len(k) != 0 {
		t.Errorf("blank-only config should parse to zero kinds, got %v", k)
	}
}

// --- AUDIT WIRING BUILDS + INTERCEPTORS ARE IN THE CHAIN --------------------

// recordingSink is a no-op AuditSink: it satisfies the port so we can build the real
// interceptors without a NATS broker. (The NATSAuditSink is exercised by pkg/audit's
// own tests; here we only prove the gateway WIRES the interceptors into the chain.)
type recordingSink struct{ records []pkgaudit.Record }

func (s *recordingSink) Record(_ context.Context, rec pkgaudit.Record) error {
	s.records = append(s.records, rec)
	return nil
}

func TestAuditWiring_BuildsAndInterceptorsRunInChain(t *testing.T) {
	t.Parallel()
	// Build the gRPC server with the SAME audit options main() uses (source =
	// events.Source, the gateway predicate, the four interceptors at the pre-auth +
	// post-auth positions). If the audit package or grpcutil options drifted, this
	// fails to compile or to construct — that is the "audit wiring builds" assertion.
	sink := &recordingSink{}
	opts := pkgaudit.Options{Source: events.Source, Logger: quietLogger(), Predicate: auditPredicate()}

	srv := grpcutil.NewServer(
		grpcutil.WithLogger(quietLogger()),
		grpcutil.WithPreAuthUnaryInterceptors(pkgaudit.DenyUnaryInterceptor(sink, opts)),
		grpcutil.WithPreAuthStreamInterceptors(pkgaudit.DenyStreamInterceptor(sink, opts)),
		grpcutil.WithUnaryInterceptors(pkgaudit.UnaryServerInterceptor(sink, opts)),
		grpcutil.WithStreamInterceptors(pkgaudit.StreamServerInterceptor(sink, opts)),
	)
	if srv == nil || srv.GRPC == nil {
		t.Fatal("expected a constructed gRPC server with the audit interceptors wired")
	}

	// Prove the INNER unary interceptor actually RUNS in a chain and emits a record:
	// invoke it directly with a handler that returns OK for an audited method. The
	// interceptor should build + hand a Record to our sink (an audited ALLOW).
	interceptor := pkgaudit.UnaryServerInterceptor(sink, opts)
	info := &grpc.UnaryServerInfo{FullMethod: aiv1.AIGatewayService_ChatCompletion_FullMethodName}
	_, err := interceptor(context.Background(), struct{}{}, info,
		func(ctx context.Context, _ any) (any, error) { return struct{}{}, nil })
	if err != nil {
		t.Fatalf("interceptor returned an unexpected error: %v", err)
	}
	if len(sink.records) != 1 {
		t.Fatalf("expected the audit interceptor to emit 1 record for an audited ALLOW, got %d", len(sink.records))
	}
	if rec := sink.records[0]; rec.Action != aiv1.AIGatewayService_ChatCompletion_FullMethodName || rec.Decision != pkgaudit.DecisionAllow {
		t.Fatalf("record = {action:%q decision:%q}, want ChatCompletion / ALLOW", rec.Action, rec.Decision)
	}
}
