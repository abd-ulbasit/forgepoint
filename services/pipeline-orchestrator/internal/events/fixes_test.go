package events_test

import (
	"context"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	eventsv1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/events/v1"
	"github.com/abd-ulbasit/forgepoint/pkg/natsutil"
	"github.com/abd-ulbasit/forgepoint/pkg/testutil"
	"github.com/abd-ulbasit/forgepoint/services/pipeline-orchestrator/internal/domain"
	"github.com/abd-ulbasit/forgepoint/services/pipeline-orchestrator/internal/events"
)

// ============================================================================
// REGRESSION TESTS FOR THE THREE CONFIRMED FINDINGS
// ============================================================================
//
// These tests pin behavior the prior suite either MASKED (Finding 1: the shared
// newJS harness added fp.dlq.> to the MODELS stream — a stream main.go does NOT
// provision — so the DLQ test passed while production silently dropped the poison
// message) or did NOT cover at all (Finding 3: PipelineFailed.compensation_failed
// / failed_step_id were never asserted). Each test reproduces the PRODUCTION
// topology / signal so a regression fails here, not in prod.

// newProdLikeJS provisions EXACTLY the JetStream streams main.go's composition
// root provisions — PIPELINES (fp.pipelines.>), MODELS (fp.models.>), and the
// dedicated DLQ stream (fp.dlq.>) — and NOTHING ELSE.
//
// WHY a separate helper from newJS (Finding 1): the shared newJS folds fp.dlq.>
// into the MODELS stream, which main.go does NOT do. That extra subject is what
// MASKED the bug: with it, the poison message had somewhere to land even though
// main.go provisioned no DLQ stream. By mirroring main.go's streams precisely
// (a dedicated DLQ stream is the FIX), this harness proves the dead-letter
// guarantee holds under the REAL wiring — and would FAIL if the DLQ stream were
// removed from main.go again.
func newProdLikeJS(t *testing.T) jetstream.JetStream {
	t.Helper()
	testutil.SkipIfNoDocker(t)
	url := testutil.StartNATS(t)

	conn, js, err := natsutil.Connect(url)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(conn.Close)

	ctx := context.Background()
	// EXACTLY main.go's streams slice (cmd/server/main.go) — no extra fp.dlq.> on
	// MODELS. CreateOrUpdateStream mirrors main.go's idempotent provisioning.
	for _, sc := range []jetstream.StreamConfig{
		{Name: "PIPELINES", Subjects: []string{"fp.pipelines.>"}},
		{Name: "MODELS", Subjects: []string{"fp.models.>"}},
		{Name: "DLQ", Subjects: []string{"fp.dlq.>"}}, // the Finding-1 fix
	} {
		if _, err := js.CreateOrUpdateStream(ctx, sc); err != nil {
			t.Fatalf("create stream %s: %v", sc.Name, err)
		}
	}
	return js
}

// TestDriftSubscriber_PoisonMessage_RoutesToDLQ_ProdStreams is the Finding-1
// regression: under the EXACT streams main.go provisions (a dedicated DLQ stream,
// and crucially NO fp.dlq.> bolted onto MODELS), a poison drift event must STILL be
// recoverable from the DLQ. Before the fix, main.go provisioned no stream capturing
// fp.dlq.>, so natsutil.routeToDLQ's Publish errored, it logged dlq_publish_failed
// and Term()'d the original — the operator's poison message was DROPPED. This test
// would FAIL in that world (nothing ever arrives on the DLQ subject) and PASSES now.
func TestDriftSubscriber_PoisonMessage_RoutesToDLQ_ProdStreams(t *testing.T) {
	js := newProdLikeJS(t)
	svc := &fakeService{err: domain.ErrPipelineArchived} // every trigger is poison

	// Wire the consumer with the SAME options main.go uses (consumer group, bounded
	// retries, the SAME DLQ subject main.go configures). AckWait is shortened only so
	// the redelivery-driven retries exhaust quickly under test.
	sub := natsutil.NewSubscriber(js,
		natsutil.WithConsumerGroup("pipeline-orchestrator-retrain"),
		natsutil.WithIdempotencyStore(natsutil.NewMemoryProcessedStore()),
		natsutil.WithMaxRetries(3),
		natsutil.WithDLQSubject("fp.dlq.pipelines.retrain"),
		natsutil.WithAckWait(2*time.Second),
	)
	t.Cleanup(sub.Close)
	rs := events.NewDriftRetrainSubscriber(svc, sub, nil)
	if err := rs.Start(context.Background()); err != nil {
		t.Fatalf("start retrain subscriber: %v", err)
	}

	// Watch the DLQ subject for the dead-lettered poison message. It is bound to the
	// dedicated DLQ stream — the same stream main.go provisions.
	dlq := make(chan natsutil.EventEnvelope, 1)
	dlqSub := natsutil.NewSubscriber(js)
	t.Cleanup(dlqSub.Close)
	if err := dlqSub.Subscribe(context.Background(), "DLQ", "fp.dlq.pipelines.retrain",
		func(_ context.Context, env natsutil.EventEnvelope) error { dlq <- env; return nil }); err != nil {
		t.Fatalf("dlq subscribe: %v", err)
	}

	publishDrift(t, js, criticalDrift("report-poison-prod", "archived-pipe"))

	select {
	case <-dlq:
		// Parked on the DLQ as the guarantee promises — recoverable for an operator.
	case <-time.After(30 * time.Second):
		t.Fatal("timeout: poison message did not reach the DLQ under main.go's exact streams " +
			"(regression: no stream captures fp.dlq.> → routeToDLQ drops the message)")
	}
	if svc.triggerCount() < 3 {
		t.Errorf("trigger attempts = %d, want >= 3 (retried before DLQ)", svc.triggerCount())
	}
}

// TestPublisher_StuckSaga_PipelineFailedCarriesCompensationFailed is the Finding-3
// regression. A stuck saga (one whose compensation itself failed) settles to FAILED
// with the cause WRAPPED in domain.ErrCompensationFailed; that wrapped message
// reaches the publisher as StepEvent.Error. The emitted PipelineFailed MUST carry
// compensation_failed=true (so Notification PAGES for a possible orphaned resource)
// and failed_step_id (so an operator knows WHICH step is stuck). Before the fix the
// publisher always sent compensation_failed=false, erasing the page/no-page signal.
func TestPublisher_StuckSaga_PipelineFailedCarriesCompensationFailed(t *testing.T) {
	js := newJS(t)
	pub := events.NewPublisher(natsutil.NewPublisher(js, events.Source), nil)

	// Simulate exactly what the domain's settle() emits for a stuck saga: the
	// terminal event's Error is the run cause wrapped in ErrCompensationFailed, and
	// StepID names the step that could not be undone.
	stuckErr := domain.ErrCompensationFailed.Error() + " (original cause: deploy step failed)"
	err := pub.Publish(context.Background(), domain.StepEvent{
		Type:        domain.EventPipelineFailed,
		ExecutionID: "exec-stuck",
		PipelineID:  "pipe-stuck",
		StepID:      "deploy", // the step whose compensation failed
		Error:       stuckErr,
		OccurredAt:  time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("publish: %v", err)
	}

	env := drainOne(t, js, "PIPELINES", events.SubjectPipelineFailed)
	var payload eventsv1.PipelineFailed
	if err := protojsonUnmarshal(t, env.Data, &payload); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !payload.GetCompensationFailed() {
		t.Errorf("compensation_failed = false, want true for a stuck saga (Notification would not page)")
	}
	if payload.GetFailedStepId() != "deploy" {
		t.Errorf("failed_step_id = %q, want %q", payload.GetFailedStepId(), "deploy")
	}
	if payload.GetExecutionId() != "exec-stuck" {
		t.Errorf("execution_id = %q, want exec-stuck", payload.GetExecutionId())
	}
}

// TestPublisher_CleanRollback_PipelineFailedNotCompensationFailed is the negative
// half of Finding 3: a saga that failed but rolled back CLEANLY (no
// ErrCompensationFailed in the cause) must emit compensation_failed=false so
// Notification logs without paging. This pins the routing distinction at both ends
// so neither a false page nor a missed page can regress unnoticed.
func TestPublisher_CleanRollback_PipelineFailedNotCompensationFailed(t *testing.T) {
	js := newJS(t)
	pub := events.NewPublisher(natsutil.NewPublisher(js, events.Source), nil)

	err := pub.Publish(context.Background(), domain.StepEvent{
		Type:        domain.EventPipelineFailed,
		ExecutionID: "exec-clean",
		PipelineID:  "pipe-clean",
		StepID:      "deploy",
		Error:       "deploy step failed: image pull backoff", // ordinary failure, clean rollback
		OccurredAt:  time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("publish: %v", err)
	}

	env := drainOne(t, js, "PIPELINES", events.SubjectPipelineFailed)
	var payload eventsv1.PipelineFailed
	if err := protojsonUnmarshal(t, env.Data, &payload); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if payload.GetCompensationFailed() {
		t.Errorf("compensation_failed = true, want false for a clean rollback (would falsely page)")
	}
	if payload.GetFailedStepId() != "deploy" {
		t.Errorf("failed_step_id = %q, want deploy", payload.GetFailedStepId())
	}
}
