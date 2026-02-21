package natsutil

import (
	"fmt"
	"log/slog"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// ============================================================================
// NATS CONNECTION
// ============================================================================
//
// WHY JetStream over core NATS:
//   Core NATS is fire-and-forget: if no subscriber is listening when a message
//   is published, it's lost. JetStream adds persistence:
//   - Messages are stored in streams (on disk or memory)
//   - Consumers can replay from any point (at-least-once delivery)
//   - Acknowledgment: consumers ACK/NAK messages (retry on failure)
//   - Deduplication: using message ID (our envelope ID)
//
//   This makes NATS JetStream comparable to Kafka for event-driven systems,
//   but with much simpler operations (single binary, no ZooKeeper/KRaft).
//
// NATS vs KAFKA:
//   ┌───────────────┬─────────────────────────┬──────────────────────────┐
//   │ Feature        │ NATS JetStream           │ Kafka                    │
//   ├───────────────┼─────────────────────────┼──────────────────────────┤
//   │ Operations     │ Single binary, zero deps │ JVM, ZooKeeper/KRaft     │
//   │ Latency        │ Sub-millisecond          │ Low-millisecond          │
//   │ Throughput     │ ~1M msg/s                │ ~1M msg/s (partitioned)  │
//   │ Consumer model │ Push + Pull              │ Pull only                │
//   │ Ordering       │ Per-stream               │ Per-partition             │
//   │ Exactly-once   │ Via dedup (msg ID)       │ Built-in (transactions)  │
//   │ Ecosystem      │ Growing                  │ Massive                  │
//   └───────────────┴─────────────────────────┴──────────────────────────┘
//
//   We chose NATS because:
//   1. Simpler to run locally and in CI (just `nats-server -js`)
//   2. Built-in request/reply (used for sync service calls if needed)
//   3. Sufficient throughput for ML platform workloads (~1K events/sec)
//   4. Go-native (NATS is written in Go, client is excellent)
//
// HOW NETFLIX/UBER DO IT:
//   - Netflix: Kafka (massive scale, legacy choice)
//   - Uber: Kafka → migrating some to NATS for lower-latency use cases
//   - Synadia (NATS company): Uses NATS for their own platform
//   - Kubernetes: Uses NATS in some distributions (k3s)
// ============================================================================

// Connect establishes a connection to NATS and returns both the raw
// connection and a JetStream context.
//
// The raw connection (nats.Conn) is needed for:
//   - Connection lifecycle management (Close, Drain)
//   - Connection status callbacks (disconnect, reconnect)
//
// The JetStream context is needed for:
//   - Creating streams and consumers
//   - Publishing to streams
//   - Subscribing with persistence
//
// Both are returned because they serve different purposes and both are
// needed by services.
func Connect(url string, opts ...nats.Option) (*nats.Conn, jetstream.JetStream, error) {
	// Default options for production robustness.
	defaultOpts := []nats.Option{
		// Reconnect automatically if connection drops.
		// MaxReconnects=-1 means retry forever (K8s will restart the pod
		// if health checks fail, but we want to survive transient network blips).
		nats.MaxReconnects(-1),

		// Log reconnection events for debugging.
		nats.DisconnectErrHandler(func(_ *nats.Conn, err error) {
			if err != nil {
				slog.Warn("NATS disconnected", slog.String("error", err.Error()))
			}
		}),
		nats.ReconnectHandler(func(nc *nats.Conn) {
			slog.Info("NATS reconnected", slog.String("url", nc.ConnectedUrl()))
		}),
	}

	allOpts := append(defaultOpts, opts...)

	conn, err := nats.Connect(url, allOpts...)
	if err != nil {
		return nil, nil, fmt.Errorf("natsutil: connect to %s: %w", url, err)
	}

	js, err := jetstream.New(conn)
	if err != nil {
		conn.Close()
		return nil, nil, fmt.Errorf("natsutil: create jetstream context: %w", err)
	}

	return conn, js, nil
}
