// Package domain is the innermost ring of the Clean Architecture onion for the
// Model Monitor service. It holds the canonical business types and the drift
// MATH — the streaming-aggregation + closed-loop-control pattern that closes the
// platform's `serve → monitor → retrain` lifecycle.
//
// ============================================================================
// CLEAN ARCHITECTURE — DOMAIN LAYER (zero framework imports)
// ============================================================================
//
// This package imports ONLY the standard library + github.com/google/uuid. No
// gRPC, no NATS, no SQL driver, no generated proto code. That purity is what
// makes the drift statistics unit-testable in microseconds with `-race` and
// what lets the same logic run identically whether driven by a NATS consumer, a
// gRPC handler, or a test harness. The handler layer (adapter) converts proto ↔
// these domain types; the repository/event adapters implement the PORTS this
// package defines (see ports.go).
//
//	┌─────────────────────────────────────────────────────────────┐
//	│  cmd/server/main.go        (composition root — wires all)   │
//	│    ↓                                                        │
//	│  internal/handler          (proto ↔ domain)                │
//	│    ↓                                                        │
//	│  internal/domain  ◄──── repository/postgres, events/nats   │
//	│  (THIS PACKAGE)              (implement the domain PORTS)   │
//	│  stdlib + uuid only                                        │
//	└─────────────────────────────────────────────────────────────┘
//
// ============================================================================
// THE PATTERN, IN ONE PARAGRAPH (interview framing)
// ============================================================================
//
// Streaming Aggregation: the monitor never stores raw inference requests. It
// folds each InferenceCompleted event into a per-model SLIDING WINDOW that holds
// only compact distributional summaries (per-feature histograms, prediction
// counts, a rolling confusion of ground-truth vs prediction). Periodically it
// compares the current window's distribution against the training-time BASELINE
// using a drift statistic (PSI / KL / KS) — exactly how Flink / Kafka Streams do
// windowed aggregation, except the "aggregate" is a drift score, not a SUM.
//
// Closed-Loop Control: when a window's score crosses a configured CRITICAL
// threshold, a POLICY fires a control-plane action — emit ModelDriftDetected and
// (if auto_retrain is on) ask the Pipeline Orchestrator to re-run training. A
// COOLDOWN debounces the policy so one sustained drift cannot retrain-storm. This
// is sense → decide → act, the same shape as a Kubernetes reconcile loop or a
// thermostat.
package domain

import (
	"time"
)

// ============================================================================
// ENUMS — mirror the proto enums by VALUE so the handler conversion is a checked
// switch, not an unchecked numeric cast (a future divergence is caught at the
// boundary). We deliberately re-declare them as pure Go types so the domain has
// NO dependency on the generated proto package.
// ============================================================================

// DriftType is the kind of distributional shift a metric/report measures. The
// three signals are independent and have different latency characteristics:
// data & prediction drift are LEADING indicators (computable the instant an
// inference event arrives); performance decay is the LAGGING confirmation (needs
// delayed ground-truth labels).
type DriftType int

const (
	DriftTypeUnspecified DriftType = iota // 0 — surfaces serialization bugs
	DriftTypeData                         // 1 — input feature distribution shifted
	DriftTypePrediction                   // 2 — output distribution shifted
	DriftTypePerformance                  // 3 — measured accuracy/F1 dropped
)

// DriftMethod is the statistical test behind a score. It travels WITH every
// score because thresholds are method-specific: PSI=0.3 ("significant") and a
// KS distance of 0.3 mean very different things. See the per-method math in
// drift.go.
type DriftMethod int

const (
	DriftMethodUnspecified DriftMethod = iota // 0
	DriftMethodPSI                            // 1 — Population Stability Index (tabular default)
	DriftMethodKL                             // 2 — Kullback–Leibler divergence
	DriftMethodKS                             // 3 — Kolmogorov–Smirnov two-sample
)

// DriftSeverity is the severity ladder the control loop branches on. WARNING
// means "alert a human, do NOT auto-retrain"; CRITICAL means "act now". The
// policy maps a numeric score against the monitor's two thresholds to one rung —
// consumers branch on the rung rather than re-deriving it from raw scores
// (mirrors Prometheus/Alertmanager severity labels). SERVER-AUTHORITATIVE:
// computed by the monitor, never supplied by a client.
type DriftSeverity int

const (
	DriftSeverityUnspecified DriftSeverity = iota // 0
	DriftSeverityOK                               // 1 — within limits, no action
	DriftSeverityWarning                          // 2 — crossed warn, not critical
	DriftSeverityCritical                         // 3 — crossed critical → fires the loop
)

// MonitorState is the monitor's lifecycle. Drift math is meaningless until we
// have a baseline AND enough samples in the window (comparing 3 requests to a
// 50k-row baseline produces noise, not signal). The state lets the monitor say
// "I'm collecting, ask me later" instead of emitting garbage scores.
// SERVER-AUTHORITATIVE: the monitor advances its own state as baselines load and
// windows fill; a client can PAUSE/RESUME (enabled flag) but cannot fake ACTIVE.
type MonitorState int

const (
	MonitorStateUnspecified     MonitorState = iota // 0
	MonitorStatePendingBaseline                     // 1 — configured, awaiting baseline
	MonitorStateWarmingUp                           // 2 — baseline present, window < min_samples
	MonitorStateActive                              // 3 — scoring + able to fire the loop
	MonitorStatePaused                              // 4 — operator-paused (windowed, not scored)
)

// ============================================================================
// CONFIGURATION AGGREGATES
// ============================================================================

// ThresholdConfig governs ONE drift type's severity mapping. WHY per-type, not
// one global number: data drift, prediction drift, and performance decay live on
// different scales (a PSI of 0.2 vs a 5-point accuracy drop). WarnScore must be
// ≤ CriticalScore — enforced on write (Validate below) so the severity ladder is
// monotone.
type ThresholdConfig struct {
	DriftType     DriftType
	Method        DriftMethod
	WarnScore     float64
	CriticalScore float64
}

// Severity maps a computed score to a rung on the ladder. This is the single
// place the score→severity decision lives, so DriftMetric, DriftReport, and
// ModelHealth all agree. Higher score = more drift for every method we use (PSI,
// KL, KS, and the performance DROP magnitude are all "bigger is worse").
func (t ThresholdConfig) Severity(score float64) DriftSeverity {
	switch {
	case score >= t.CriticalScore:
		return DriftSeverityCritical
	case score >= t.WarnScore:
		return DriftSeverityWarning
	default:
		return DriftSeverityOK
	}
}

// Monitor is the central config aggregate: one Monitor governs one model's drift
// detection — its window shape, thresholds, baseline reference, and auto-retrain
// switch.
//
// SECURITY — SERVER-AUTHORITATIVE fields (NOT client-writable on configure):
// ID, OwnerTeam, State, BaselineVersion, BaselineCapturedAt, CreatedAt,
// UpdatedAt. A client that could set OwnerTeam could create monitors against
// another tenant's models (cross-tenant write); one that could pick BaselineVersion
// could point at a baseline that conveniently matches current traffic to suppress
// a real drift signal. The handler maps only the operator-owned fields from
// ConfigureMonitorRequest (mass-assignment guard).
type Monitor struct {
	ID        string // UUID v4, server-assigned, immutable
	ModelName string // stable name from the Model Registry
	OwnerTeam string // SERVER-set from auth claims, never a request field

	// Window shape. A window closes when EITHER bound is hit (whichever first):
	// the time bound keeps low-traffic models from waiting forever; the count
	// bound keeps high-traffic models from accumulating unbounded memory.
	WindowDuration time.Duration // time bound; 0 = unbounded by time
	WindowSize     int           // count bound; 0 = unbounded by count
	MinSamples     int           // refuse to SCORE below this (stay WARMING_UP)

	Thresholds []ThresholdConfig // per-drift-type; an unconfigured type is not scored

	// The closed-loop switch — the heart of the pattern. When true and a CRITICAL
	// breach fires, the monitor calls the Pipeline Orchestrator. Defaults false:
	// self-healing must be opted into (an accidental retrain storm is expensive).
	AutoRetrain bool
	// The training pipeline to trigger (opaque Orchestrator id). REQUIRED when
	// AutoRetrain=true. Opaque on purpose: Model Monitor does not import the
	// pipeline proto — the loop is closed via the event bus / a gRPC client port,
	// not a compile-time dependency.
	RetrainPipelineID string

	State              MonitorState // SERVER-set lifecycle
	BaselineVersion    string       // SERVER-resolved from Registry/Experiment-Tracker
	BaselineCapturedAt time.Time    // when the baseline distribution was captured
	CreatedAt          time.Time
	UpdatedAt          time.Time
}

// ThresholdFor returns the configured threshold for a drift type, or false if
// that type is not configured (and therefore not scored — silence, not a zero
// verdict). Used by the scorer to decide which signals to compute and how to map
// their scores.
func (m Monitor) ThresholdFor(dt DriftType) (ThresholdConfig, bool) {
	for _, t := range m.Thresholds {
		if t.DriftType == dt {
			return t, true
		}
	}
	return ThresholdConfig{}, false
}

// WindowBound is true when this monitor bounds windows by count (WindowSize > 0).
// Used by the window to decide whether a count-based close can ever fire.
func (m Monitor) BoundsByCount() bool { return m.WindowSize > 0 }

// ============================================================================
// DRIFT RESULTS
// ============================================================================

// DriftMetric is ONE feature's / output's / performance-metric's measurement.
// WHY per-thing (not one number per report): drift is rarely uniform — usually
// ONE feature moved (a sensor recalibrated, an upstream join changed units)
// while the rest are stable. "income PSI=0.41, everything else <0.05" tells an
// engineer exactly where to look; a single model-level number throws that away.
type DriftMetric struct {
	Name          string        // feature name / output class / metric name ("accuracy")
	Method        DriftMethod   // the test behind Score
	Score         float64       // SERVER-computed; higher = more drift
	BaselineValue float64       // baseline summary (e.g. baseline mean) for "was 5.1, now 7.8"
	CurrentValue  float64       // current-window summary, paired with BaselineValue
	Severity      DriftSeverity // this metric's rung; report severity = max across metrics
}

// DriftReport is the durable verdict for a single closed window. It backs the
// paginated history, the UI time series, and the audit trail ("why did the model
// retrain at 03:14?"). The canonical ModelDriftDetected event carries a FLAT copy
// of these fields + the report id (events are decoupled from domain types).
//
// IDEMPOTENCY — WindowID is the natural key: each closed window has a stable id,
// and a report is persisted idempotently on it. If the stream redelivers events
// or the scorer runs twice for one window, we get ONE report, not duplicates —
// and exactly one event fires. Same idempotency-key discipline as every consumer
// on the platform.
type DriftReport struct {
	ID        string // UUID v4, server-assigned
	MonitorID string
	// OwnerTeam is the tenant that owns the monitor that produced this report.
	// SERVER-AUTHORITATIVE: copied from the Monitor at score time (see scoreWindow),
	// never client-supplied. It is the TENANCY KEY for every report read/purge —
	// because monitors are keyed by (owner_team, model_name), model_name alone is
	// not unique, so reports must carry and be filtered by owner_team to prevent
	// cross-tenant reads (IDOR on GetDriftReport) and cross-tenant purges.
	OwnerTeam    string
	ModelName    string
	ModelVersion string // captured from the inference events (vital under canary split)

	DriftType DriftType     // the dominant (breach-triggering) type — the headline
	Severity  DriftSeverity // max across Metrics — what the policy/consumers branch on
	Metrics   []DriftMetric // the actionable per-thing breakdown

	WindowID    string // the IDEMPOTENCY KEY for persistence + event emission
	SampleCount int    // how many samples were in the scored window (statistical confidence)
	WindowStart time.Time
	WindowEnd   time.Time
	CreatedAt   time.Time
}

// IsActionable is true when the report's severity warrants surfacing it (anything
// above OK). An OK report may still be persisted for the time series, but it does
// not alert or fire the loop.
func (r DriftReport) IsActionable() bool { return r.Severity >= DriftSeverityWarning }

// FiresRetrain reports whether THIS report is severe enough to arm the auto-
// retrain action. The decision is "CRITICAL" — WARNING deliberately only alerts
// a human (the leading-indicator zone). The cooldown/auto_retrain gating lives in
// the policy (see RetrainPolicy); this is purely the severity predicate.
func (r DriftReport) FiresRetrain() bool { return r.Severity == DriftSeverityCritical }

// ============================================================================
// OBSERVABILITY VIEWS (live status + at-a-glance health)
// ============================================================================

// MonitorStatus is the live "what's happening right now" view, distinct from the
// Monitor config ("what you set"). It exposes the in-flight window's fill level
// and the freshest report — the operator drill-in.
type MonitorStatus struct {
	Monitor              Monitor
	State                MonitorState
	CurrentWindowSamples int          // fill of the still-open window vs MinSamples
	LatestReport         *DriftReport // nil until the first window closes
	LastEventAt          time.Time    // stale ⇒ no traffic (its own kind of alert)
	DriftEventsTotal     int64        // lifetime count of CRITICAL reports
}

// ModelHealth is the at-a-glance, one-line verdict a dashboard tile / `fp monitor
// <model>` consumes — simpler than MonitorStatus. OverallSeverity is the MAX
// across all active drift signals in the most recent scored window.
// SERVER-computed; a client cannot assert its model "healthy".
type ModelHealth struct {
	ModelName        string
	ModelVersion     string                      // live judged version; empty if no traffic yet
	OverallSeverity  DriftSeverity               // max across signals — the tile color
	SeverityByType   map[DriftType]DriftSeverity // per-type lights (3-light panel)
	State            MonitorState                // distinguishes "healthy" from "not scoring yet"
	LatestReportID   string                      // deep-link handle for "see why"
	LastEventAt      time.Time
	DriftEventsTotal int64
}

// ============================================================================
// GROUND TRUTH (delayed labels → performance decay)
// ============================================================================

// GroundTruthLabel is a delayed true outcome for a past prediction, joined by
// RequestID. WHY RequestID as the key: the Inference Gateway stamps a server-
// authoritative request_id on every prediction and carries it on the
// InferenceCompleted event. That id ties "a prediction we saw" to "the outcome
// you now know" WITHOUT shipping PII or raw features — the PII-free trust
// boundary. ActualLabel is a LABEL, capped in length, never a free-form PII blob.
type GroundTruthLabel struct {
	RequestID   string    // join key — must match an observed prediction; NOT a user id
	ActualLabel string    // observed outcome ("fraud"/"not_fraud"); length-capped upstream
	ObservedAt  time.Time // when the truth became known (NOT when predicted)
}

// MaxLabelBytes caps a ground-truth label's length. WHY a cap: a label is a class
// name, not a payload — an unbounded label is both a memory footgun and a PII
// smuggling vector. Enforced where labels enter the domain (RecordGroundTruth).
const MaxLabelBytes = 256

// MaxGroundTruthBatch caps a single SubmitGroundTruth batch. Mirrors the proto
// contract cap: it bounds the transactional write and protects the server from an
// unbounded single request. Callers with more labels page through multiple calls.
const MaxGroundTruthBatch = 1000

// MaxWindowSize caps a monitor's count-based window. WHY a hard cap: the live
// window is held in Redis; an unbounded or absurd WindowSize would let a single
// monitor pin gigabytes of RAM. int comfortably covers this with no overflow risk
// in window-fill arithmetic.
const MaxWindowSize = 1_000_000

// MaxListPageSize is the platform-wide page-size cap for paginated reads. A larger
// client request is capped here (DoS / accidental-firehose guard), never honored.
const MaxListPageSize = 100

// DefaultListPageSize is used when a list request omits a page size.
const DefaultListPageSize = 20
