package natsutil

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// validSpanContext builds a non-recording but valid remote span context for
// propagation tests (no SDK/exporter needed).
func validSpanContext() trace.SpanContext {
	return trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    trace.TraceID{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10},
		SpanID:     trace.SpanID{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08},
		TraceFlags: trace.FlagsSampled,
		Remote:     true,
	})
}

// The producer's trace context must survive being carried in the envelope and
// re-extracted on the consumer side — otherwise distributed traces break at
// every async hop.
func TestTraceContextRoundTrip(t *testing.T) {
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{}, propagation.Baggage{},
	))

	sc := validSpanContext()
	producerCtx := trace.ContextWithSpanContext(context.Background(), sc)

	env := NewEnvelope("registered", "registry", nil)
	injectTraceContext(producerCtx, &env)
	if len(env.TraceContext) == 0 {
		t.Fatal("injectTraceContext wrote nothing into the envelope")
	}

	consumerCtx := extractTraceContext(context.Background(), env)
	got := trace.SpanContextFromContext(consumerCtx)
	if got.TraceID() != sc.TraceID() {
		t.Fatalf("extracted TraceID = %s, want %s", got.TraceID(), sc.TraceID())
	}
	if got.SpanID() != sc.SpanID() {
		t.Fatalf("extracted SpanID = %s, want %s", got.SpanID(), sc.SpanID())
	}
}

func TestCorrelationIDForPublish_Priority(t *testing.T) {
	// 1. An explicit correlation id on the context wins (continue the chain).
	ctx := ContextWithCorrelationID(context.Background(), "explicit-id")
	if got := correlationIDForPublish(ctx); got != "explicit-id" {
		t.Fatalf("explicit: got %q, want explicit-id", got)
	}

	// 2. Otherwise fall back to the active trace id.
	sc := validSpanContext()
	traceCtx := trace.ContextWithSpanContext(context.Background(), sc)
	if got := correlationIDForPublish(traceCtx); got != sc.TraceID().String() {
		t.Fatalf("trace fallback: got %q, want %q", got, sc.TraceID().String())
	}

	// 3. With neither, a fresh non-empty id is generated.
	if got := correlationIDForPublish(context.Background()); got == "" {
		t.Fatal("expected a generated correlation id, got empty string")
	}
}

func TestMemoryProcessedStore(t *testing.T) {
	store := NewMemoryProcessedStore()
	ctx := context.Background()

	seen, err := store.IsProcessed(ctx, "evt-1")
	if err != nil {
		t.Fatalf("IsProcessed: %v", err)
	}
	if seen {
		t.Fatal("fresh store reported evt-1 as already processed")
	}

	if err := store.MarkProcessed(ctx, "evt-1"); err != nil {
		t.Fatalf("MarkProcessed: %v", err)
	}

	seen, err = store.IsProcessed(ctx, "evt-1")
	if err != nil {
		t.Fatalf("IsProcessed after mark: %v", err)
	}
	if !seen {
		t.Fatal("evt-1 not reported processed after MarkProcessed")
	}
}
