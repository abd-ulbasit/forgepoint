package events_test

// harness_test.go — shared scaffolding for the billing events adapter tests.
//
// Every test here runs against a REAL NATS JetStream server started by
// pkg/testutil.StartNATS on the remote Docker engine — never a mock. NATS behavior
// (durable consumers, at-least-once redelivery, consumer-side dedup, DLQ routing,
// envelope round-trip) is stateful and subtle; mocking it would hide the very
// behavior these adapters exist to get right. testutil.SkipIfNoDocker skips cleanly
// when no engine is reachable so the rest of the suite still runs.
//
// Each test provisions its OWN streams on a fresh server (no cross-test state).

import (
	"context"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/abd-ulbasit/forgepoint/pkg/natsutil"
	"github.com/abd-ulbasit/forgepoint/pkg/testutil"
)

// newJetStream starts a real NATS (JetStream) container and returns a connected
// JetStream context. The connection is closed via t.Cleanup.
func newJetStream(t *testing.T) jetstream.JetStream {
	t.Helper()
	url := testutil.StartNATS(t) // SkipIfNoDocker is called inside
	conn, js, err := natsutil.Connect(url)
	if err != nil {
		t.Fatalf("connect NATS: %v", err)
	}
	t.Cleanup(conn.Close)
	return js
}

// mustCreateStream binds the given subject filters to a stream. Both the relay
// (publishes into BILLING) and the consumer (reads from INFERENCE) need their
// streams to pre-exist; the DLQ subject also needs a stream or DLQ publishes vanish.
func mustCreateStream(t *testing.T, js jetstream.JetStream, name string, subjects ...string) {
	t.Helper()
	_, err := js.CreateStream(context.Background(), jetstream.StreamConfig{
		Name:     name,
		Subjects: subjects,
	})
	if err != nil {
		t.Fatalf("create stream %s: %v", name, err)
	}
}

// waitEnvelope reads one envelope from ch or fails the test on timeout. 15s is
// generous for a container round-trip while still bounding a hung test.
func waitEnvelope(t *testing.T, ch <-chan natsutil.EventEnvelope) natsutil.EventEnvelope {
	t.Helper()
	select {
	case env := <-ch:
		return env
	case <-time.After(15 * time.Second):
		t.Fatal("timeout waiting for event")
		return natsutil.EventEnvelope{}
	}
}
