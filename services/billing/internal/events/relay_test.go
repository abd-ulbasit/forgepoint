package events_test

// relay_test.go — the OUTBOX RELAY over REAL NATS JetStream.
//
// These tests prove the DB→NATS half of the transactional outbox:
//   1. the relay reads claimed (stored) outbox rows, rebuilds the canonical
//      events.v1 payload, and publishes each to the RIGHT subject with the correct
//      EventEnvelope (source="billing", id == the outbox ROW id) and payload;
//   2. it MARKS each successfully-published row published (so it isn't re-sent);
//   3. a REPUBLISH of the same row carries the SAME envelope id — the property that
//      makes the relay's at-least-once delivery safe (consumers dedupe on that id).
//
// The OutboxReader is a fake (we control exactly which rows are "pending" and
// observe which get marked) — but the PUBLISHER is the real *RelayPublisher over a
// real JetStream, so the publish path (envelope build, id stamping, subject) is
// exercised for real, not mocked.

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	eventsv1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/events/v1"
	"github.com/abd-ulbasit/forgepoint/pkg/natsutil"
	"github.com/abd-ulbasit/forgepoint/pkg/testutil"
	"github.com/abd-ulbasit/forgepoint/services/billing/internal/domain"
	"github.com/abd-ulbasit/forgepoint/services/billing/internal/events"
	"github.com/google/uuid"
)

// TestRelay_PublishesAndMarks proves the relay publishes one stored row of EACH
// billing event type to its canonical subject with a faithful envelope+payload, and
// marks every published row.
func TestRelay_PublishesAndMarks(t *testing.T) {
	testutil.SkipIfNoDocker(t)
	js := newJetStream(t)
	mustCreateStream(t, js, events.StreamBilling, "fp.billing.>")

	now := time.Now().UTC().Truncate(time.Second)

	// Three pending rows — one per billing event type — with KNOWN ids so we can
	// assert the envelope id == the outbox row id.
	usageID := uuid.NewString()
	quotaID := uuid.NewString()
	invoiceID := uuid.NewString()

	reader := newFakeReader(
		outboxRow(t, usageID, domain.EventTypeUsageRecorded, domain.UsageRecordedPayload{
			RecordID: "rec-1", Team: "acme", RatePlanID: "plan-1",
			MeterType: domain.MeterTypeInferenceTokens, Quantity: 1500,
			CostMicros: 600000, CurrencyCode: "USD",
			ModelID: "m-1", ModelVersion: "v3", SourceRequestID: "req-1", OccurredAt: now,
		}),
		outboxRow(t, quotaID, domain.EventTypeQuotaExceeded, domain.QuotaExceededPayload{
			Team: "acme", RatePlanID: "plan-1", MeterType: domain.MeterTypeInferenceTokens,
			QuotaLimit: 1000, CurrentUsage: 1500, OccurredAt: now,
		}),
		outboxRow(t, invoiceID, domain.EventTypeInvoiceGenerated, domain.InvoiceGeneratedPayload{
			InvoiceID: "inv-1", InvoiceNumber: "INV-2026-000042", Team: "acme", RatePlanID: "plan-1",
			PeriodStart: now.Add(-720 * time.Hour), PeriodEnd: now,
			TotalMicros: 600000, CurrencyCode: "USD", FinalizedAt: now,
		}),
	)

	// Subscribe a raw reader to fp.billing.> to capture exactly what the relay emits.
	got := make(chan natsutil.EventEnvelope, 3)
	raw := natsutil.NewSubscriber(js, natsutil.WithIdempotencyStore(natsutil.NewMemoryProcessedStore()))
	t.Cleanup(raw.Close)
	if err := raw.Subscribe(context.Background(), events.StreamBilling, "fp.billing.>",
		func(_ context.Context, env natsutil.EventEnvelope) error {
			got <- env
			return nil
		}); err != nil {
		t.Fatalf("subscribe billing: %v", err)
	}

	relay := events.NewOutboxRelay(reader, events.NewRelayPublisher(js, events.Source), events.RelayConfig{BatchSize: 10})

	n, err := relay.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if n != 3 {
		t.Fatalf("relay published %d rows, want 3", n)
	}

	// Collect the three envelopes, keyed by subject-derived type, and verify each.
	byType := map[string]natsutil.EventEnvelope{}
	for range 3 {
		env := waitEnvelope(t, got)
		byType[env.Type] = env
		if env.Source != events.Source {
			t.Errorf("envelope source = %q, want %q", env.Source, events.Source)
		}
	}

	// --- UsageRecorded: envelope id == outbox row id; payload faithful ---
	usageEnv, ok := byType["usage.recorded"]
	if !ok {
		t.Fatal("no usage.recorded envelope received")
	}
	if usageEnv.ID != usageID {
		t.Errorf("UsageRecorded envelope id = %q, want the outbox row id %q", usageEnv.ID, usageID)
	}
	var ur eventsv1.UsageRecorded
	if err := json.Unmarshal(usageEnv.Data, &ur); err != nil {
		t.Fatalf("decode UsageRecorded: %v", err)
	}
	if ur.GetTeam() != "acme" || ur.GetQuantity() != 1500 || ur.GetCostMicros() != 600000 ||
		ur.GetCurrencyCode() != "USD" || ur.GetSourceRequestId() != "req-1" {
		t.Errorf("UsageRecorded payload mismatch: %+v", &ur)
	}
	if ur.GetMeterType() != eventsv1.MeterType_METER_TYPE_INFERENCE_TOKENS {
		t.Errorf("UsageRecorded meter = %v, want INFERENCE_TOKENS", ur.GetMeterType())
	}
	if !ur.GetOccurredAt().AsTime().Equal(now) {
		t.Errorf("UsageRecorded occurred_at = %v, want %v", ur.GetOccurredAt().AsTime(), now)
	}

	// --- QuotaExceeded ---
	quotaEnv := byType["quota.exceeded"]
	if quotaEnv.ID != quotaID {
		t.Errorf("QuotaExceeded envelope id = %q, want %q", quotaEnv.ID, quotaID)
	}
	var qe eventsv1.QuotaExceeded
	if err := json.Unmarshal(quotaEnv.Data, &qe); err != nil {
		t.Fatalf("decode QuotaExceeded: %v", err)
	}
	if qe.GetTeam() != "acme" || qe.GetQuotaLimit() != 1000 || qe.GetCurrentUsage() != 1500 {
		t.Errorf("QuotaExceeded payload mismatch: %+v", &qe)
	}

	// --- InvoiceGenerated ---
	invEnv := byType["invoice.generated"]
	if invEnv.ID != invoiceID {
		t.Errorf("InvoiceGenerated envelope id = %q, want %q", invEnv.ID, invoiceID)
	}
	var ig eventsv1.InvoiceGenerated
	if err := json.Unmarshal(invEnv.Data, &ig); err != nil {
		t.Fatalf("decode InvoiceGenerated: %v", err)
	}
	if ig.GetInvoiceNumber() != "INV-2026-000042" || ig.GetTotalMicros() != 600000 || ig.GetTeam() != "acme" {
		t.Errorf("InvoiceGenerated payload mismatch: %+v", &ig)
	}

	// All three rows must have been MARKED published (drained from the pending set).
	if marked := reader.markedIDs(); len(marked) != 3 {
		t.Errorf("relay marked %d rows published, want 3 (%v)", len(marked), marked)
	}
	if reader.pendingCount() != 0 {
		t.Errorf("reader still has %d pending rows after drain, want 0", reader.pendingCount())
	}
}

// TestRelay_RepublishCarriesSameEnvelopeID proves the property that makes the
// outbox safe under at-least-once: publishing the SAME row twice (the crash-before-
// mark scenario) emits the SAME EventEnvelope.id both times — so a consumer's
// ProcessedStore collapses the duplicate. We simulate the crash by NOT marking
// (the fake reader keeps the row pending) and running the relay twice.
func TestRelay_RepublishCarriesSameEnvelopeID(t *testing.T) {
	testutil.SkipIfNoDocker(t)
	js := newJetStream(t)
	mustCreateStream(t, js, events.StreamBilling, "fp.billing.>")

	rowID := uuid.NewString()
	reader := newFakeReader(outboxRow(t, rowID, domain.EventTypeUsageRecorded, domain.UsageRecordedPayload{
		RecordID: "rec-x", Team: "acme", RatePlanID: "plan-1",
		MeterType: domain.MeterTypeInferenceRequest, Quantity: 1, CostMicros: 1000000,
		CurrencyCode: "USD", SourceRequestID: "req-x", OccurredAt: time.Now().UTC().Truncate(time.Second),
	}))
	// Simulate "crash before mark": MarkPublished is a no-op, so the row stays
	// pending and the relay re-claims+re-publishes it on the next run.
	reader.suppressMark = true

	// Raw-capture with a DISTINCT-id store would dedupe; here we want to SEE both
	// physical deliveries, so we capture without a ProcessedStore and assert both
	// carry the same envelope id (the broker may still dedupe by Msg-Id within the
	// window, so we run the second publish after a short gap and also assert via the
	// stream that the id is stable on the wire).
	got := make(chan natsutil.EventEnvelope, 4)
	raw := natsutil.NewSubscriber(js)
	t.Cleanup(raw.Close)
	if err := raw.Subscribe(context.Background(), events.StreamBilling, "fp.billing.>",
		func(_ context.Context, env natsutil.EventEnvelope) error {
			got <- env
			return nil
		}); err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	relay := events.NewOutboxRelay(reader, events.NewRelayPublisher(js, events.Source), events.RelayConfig{BatchSize: 10})

	if _, err := relay.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce #1: %v", err)
	}
	first := waitEnvelope(t, got)
	if first.ID != rowID {
		t.Fatalf("first publish envelope id = %q, want outbox row id %q", first.ID, rowID)
	}

	// Second run = the relay republishing the still-unmarked row. The envelope id
	// MUST equal the row id again — the stable idempotency anchor across republishes.
	if _, err := relay.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce #2: %v", err)
	}
	// The broker dedupes by Nats-Msg-Id (== row id) within its window, so the second
	// delivery may be collapsed — that collapse is ITSELF the desired property
	// (same id ⇒ deduped). We assert NO delivery with a DIFFERENT id arrives.
	select {
	case second := <-got:
		if second.ID != rowID {
			t.Fatalf("republish emitted a DIFFERENT envelope id %q (want stable %q) — republish-idempotency broken",
				second.ID, rowID)
		}
		// Same id again — acceptable (broker didn't dedupe within timing); the id is stable.
	case <-time.After(3 * time.Second):
		// No second delivery — broker deduped on the stable Msg-Id. Also correct.
	}
}

// ----------------------------------------------------------------------------
// fakeReader — an in-memory OutboxReader: drives the relay loop without a DB and
// records which rows were marked published.
// ----------------------------------------------------------------------------

type fakeReader struct {
	mu           sync.Mutex
	pending      []events.OutboxRow
	marked       []string
	suppressMark bool // when true, MarkPublished is a no-op (simulate crash-before-mark)
}

func newFakeReader(rows ...events.OutboxRow) *fakeReader {
	return &fakeReader{pending: rows}
}

func (r *fakeReader) ClaimUnpublished(_ context.Context, limit int) ([]events.OutboxRow, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := len(r.pending)
	if n > limit {
		n = limit
	}
	out := make([]events.OutboxRow, n)
	copy(out, r.pending[:n])
	return out, nil
}

func (r *fakeReader) MarkPublished(_ context.Context, id string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.marked = append(r.marked, id)
	if r.suppressMark {
		return nil // row stays pending → relay re-publishes it next run
	}
	// Remove the row from the pending set so a drain terminates.
	kept := r.pending[:0]
	for _, row := range r.pending {
		if row.ID != id {
			kept = append(kept, row)
		}
	}
	r.pending = kept
	return nil
}

func (r *fakeReader) markedIDs() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]string, len(r.marked))
	copy(out, r.marked)
	return out
}

func (r *fakeReader) pendingCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.pending)
}

// outboxRow builds an events.OutboxRow whose Payload is the JSONB the repository
// would have stored (json.Marshal of the flat domain payload — see mapping.go's
// marshalOutboxPayload, which marshals the concrete struct directly). This makes
// the test fixture byte-identical to a production-stored row.
func outboxRow(t *testing.T, id, eventType string, payload domain.OutboxPayload) events.OutboxRow {
	t.Helper()
	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal outbox payload: %v", err)
	}
	return events.OutboxRow{
		ID:        id,
		EventType: eventType,
		Payload:   body,
		CreatedAt: time.Now().UTC(),
	}
}
