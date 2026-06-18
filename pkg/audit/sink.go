package audit

import (
	"context"

	"github.com/abd-ulbasit/forgepoint/pkg/natsutil"
)

// ============================================================================
// AUDIT SINK — the seam between CAPTURE and PERSIST
// ============================================================================
//
// The AuditInterceptor builds a Record and hands it to an AuditSink. The sink is
// an interface so the interceptor (which runs in EVERY service) is decoupled from
// HOW records are durably stored (which ONE service — auth — owns). This is the
// Dependency Inversion that makes the capture/persist split work: the hot-path
// capture side depends only on this one-method abstraction.
// ============================================================================

// AuditSink receives audit records from the interceptor. Implementations must be
// safe for concurrent use (the interceptor calls Record from many RPC goroutines).
//
// CONTRACT: Record MUST NOT block the caller for long, and the interceptor treats
// any error as non-fatal (audit is best-effort on the request hot path — see
// AuditInterceptor). A sink that needs durability should hand off asynchronously
// (the NATS sink below publishes, which is fast and itself buffered/retried by
// JetStream) rather than doing synchronous Postgres writes inline with the RPC.
type AuditSink interface {
	Record(ctx context.Context, rec Record) error
}

// SubjectRecorded is the canonical subject every service publishes audit records
// to. One subject, one consumer (in auth) — the choreography fan-in. The
// "fp.audit.>" tree is owned by the auth service, which provisions the AUDIT
// stream and binds its consumer to THIS subject. Exported so the auth-side
// consumer wiring references the SAME constant the producer publishes to (no
// stringly-typed drift between emit and persist).
const SubjectRecorded = "fp.audit.recorded"

// auditEnvelopeType is the EventEnvelope.Type derived for audit records. natsutil
// derives the type from the subject by stripping "fp.{service}." — for
// "fp.audit.recorded" that yields "recorded". We keep this constant as the
// documented expectation; the publisher computes it from the subject.
const auditEnvelopeType = "recorded"

// NATSAuditSink publishes audit records to NATS JetStream on fp.audit.recorded.
//
// WHY publish instead of writing Postgres directly from the interceptor:
//   - DECOUPLING: a service emitting audit events should not need a connection to
//     the audit database (auth owns it; database-per-service forbids cross-service
//     DB access). It only needs the bus it already has.
//   - DURABILITY + BACKPRESSURE: JetStream persists the record the instant it is
//     published; the single auth-side consumer then appends to the hash chain at
//     its own pace. A burst of audited RPCs doesn't contend on the audit table.
//   - SAME SHAPE AS THE PLATFORM: this is exactly how notification's choreography
//     works (many producers publish, one reactor consumes). Audit is one more
//     fan-in feed.
//
// The record rides inside the standard natsutil EventEnvelope (id/type/source/
// timestamp/correlation_id/data), JSON-encoded into data — consistent with every
// other Forgepoint event and with ADR 0004 (JSON on the bus, schema decoupled
// from the API proto).
type NATSAuditSink struct {
	pub *natsutil.Publisher
}

// Compile-time assertion that NATSAuditSink satisfies the sink port. If the
// interface changes, the build breaks here, not at a wiring site.
var _ AuditSink = (*NATSAuditSink)(nil)

// NewNATSAuditSink builds a sink over an existing natsutil.Publisher. The caller
// (each service's main.go) already constructs a Publisher for its own events; the
// audit sink reuses that same publisher so audit shares the service's NATS
// connection, dedup window, and trace-propagation policy.
func NewNATSAuditSink(pub *natsutil.Publisher) *NATSAuditSink {
	return &NATSAuditSink{pub: pub}
}

// Record publishes the audit record onto fp.audit.recorded. The Publisher wraps
// it in an EventEnvelope (stamping the producing service as Source, generating
// the dedup id, and propagating the correlation id + trace context from ctx), so
// the auth-side consumer can dedup on envelope.id and continue the trace.
//
// The returned error is surfaced to the interceptor, which logs-and-continues —
// a failed audit publish must NEVER fail the underlying RPC (best-effort on the
// hot path). Durability of the record then rests on JetStream once published.
func (s *NATSAuditSink) Record(ctx context.Context, rec Record) error {
	return s.pub.Publish(ctx, SubjectRecorded, rec)
}
