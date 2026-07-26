// ports.go — the PORTS (interfaces) the Model Monitor domain depends on. The
// domain is the CONSUMER of persistence, the event bus, the orchestrator, and the
// baseline source, so — per Hexagonal Architecture — it OWNS the interfaces and
// the adapters (Postgres / Redis / NATS / a gRPC client) implement them.
//
// ============================================================================
// WHY PORTS LIVE HERE AND NOT IN repository/ (the import cycle)
// ============================================================================
//
// If these interfaces lived in `internal/repository` they would reference domain
// types (Monitor, DriftReport), and the domain's service impl (which lives here)
// would reference the interfaces — so domain → repository → domain, an import
// CYCLE Go rejects at build time. "The consumer owns the port" breaks the cycle:
// the domain declares what it needs; the adapter imports the domain and conforms.
// One inward arrow, no cycle. (See services/auth/internal/domain/ports.go — same
// lesson, learned the hard way.)
//
//	domain (defines ports + service + models)         ← stdlib + uuid only
//	   ▲           ▲            ▲              ▲
//	   │ impl      │ impl       │ impl         │ impl
//	postgres    redis       nats publisher   orchestrator gRPC client
//	(reports)   (windows)   (drift events)   (closes the loop)
//
// ============================================================================
package domain

import (
	"context"
	"errors"
	"time"
)

// ============================================================================
// STORAGE SENTINEL (translated by the service into business errors)
// ============================================================================

// ErrRepoNotFound is the STORAGE sentinel a repository port returns when a row is
// absent. The service translates it into the right BUSINESS error
// (ErrMonitorNotFound / ErrReportNotFound). WHY distinct: "no row" is a storage
// fact; "this model isn't monitored" is the business fact the handler maps to a
// gRPC code. The service is the single translation point.
var ErrRepoNotFound = errors.New("repository: not found")

// ListOptions carries cursor-based pagination for the list ports. Cursor (not
// LIMIT/OFFSET) is stable under concurrent inserts — new reports arriving while a
// client pages don't shift the offset and cause skips/dupes. PageSize 0 ⇒ the
// service applies DefaultListPageSize; the service also CAPS it at MaxListPageSize.
type ListOptions struct {
	PageSize  int
	PageToken string
}

// ReportFilter narrows a drift-history query. Zero-valued fields mean "no filter
// on that dimension" (Since/Until zero = unbounded on that side;
// MinSeverity == DriftSeverityUnspecified = all severities).
//
// TENANCY: OwnerTeam is REQUIRED and set by the SERVICE from the caller's auth
// claims — never from a client field. WHY it must be here and not just ModelName:
// monitors are keyed by (owner_team, model_name), so model names are NOT globally
// unique — team-a/"fraud" and team-b/"fraud" are different models. A history query
// keyed by model name alone would return BOTH teams' reports (cross-tenant read).
// The adapter MUST filter on (owner_team, model_name) together, mirroring how the
// monitor write model is keyed.
type ReportFilter struct {
	OwnerTeam   string // REQUIRED, service-set from auth claims (tenant scope)
	ModelName   string // OPTIONAL filter: set = one model; empty = all of the team's models
	MinSeverity DriftSeverity
	Since       time.Time
	Until       time.Time
}

// FleetFilter narrows the fleet (ListMonitors) view. OwnerTeam is set by the
// SERVICE from the caller's auth claims — never from a client field — so the
// fleet read is always team-scoped (no cross-tenant read). MinSeverity/State are
// optional client filters.
type FleetFilter struct {
	OwnerTeam   string
	MinSeverity DriftSeverity
	State       MonitorState
}

// ============================================================================
// PERSISTENCE PORTS (Postgres adapter implements these)
// ============================================================================

// MonitorRepository persists monitor CONFIG (the write model: monitors table).
type MonitorRepository interface {
	// Upsert inserts or updates a monitor, keyed by (OwnerTeam, ModelName) — one
	// monitor per model within a team. Returns the stored monitor (with server
	// fields) and `created` true on insert. The service sets all server-authoritative
	// fields before calling; the adapter just persists.
	Upsert(ctx context.Context, m Monitor) (stored Monitor, created bool, err error)
	// GetByModel returns a team's monitor for a model, or ErrRepoNotFound.
	GetByModel(ctx context.Context, ownerTeam, modelName string) (Monitor, error)
	// GetByID returns a monitor by its server-assigned id, or ErrRepoNotFound. Used
	// on the DATA plane: the events adapter resolves the monitor id from the event,
	// and the scorer loads the config by id (team already established at resolution).
	GetByID(ctx context.Context, id string) (Monitor, error)
	// Delete soft-deletes the monitor config (drift history is retained unless the
	// caller purges separately). A second delete of an already-deleted monitor is a
	// safe no-op (returns nil), so a retried DeleteMonitor is idempotent.
	Delete(ctx context.Context, ownerTeam, modelName string) error
	// List returns a page of the team's monitors with optional severity/state
	// filters, plus a next-page cursor.
	List(ctx context.Context, f FleetFilter, opts ListOptions) (monitors []Monitor, nextToken string, err error)
}

// DriftReportRepository persists drift REPORTS (the durable verdict history).
//
// TENANCY (the cross-tenant lesson): EVERY read/purge here is scoped by
// owner_team, NOT by model_name alone. Monitors are keyed by (owner_team,
// model_name) — so the same model name can belong to two different teams
// (team-a/"fraud" ≠ team-b/"fraud"). Keying reports by model name alone would let
// team-a read team-b's "fraud" history (LatestByModel/List) or DESTROY it
// (PurgeByModel on a purge-delete). The owner_team is stored on each report (set
// server-side from the monitor at score time) and is part of the lookup key.
// GetByID is also team-scoped so a leaked/guessed report id (these ids travel in
// events, retrain requests, deep links, and logs — they are NOT secret) cannot be
// used to read another tenant's report (IDOR). The adapter SQL must include
// `WHERE owner_team = $team` on every one of these.
type DriftReportRepository interface {
	// Save persists a report IDEMPOTENTLY on its WindowID. If a report already
	// exists for that window (stream redelivery / double scoring), Save returns the
	// EXISTING report and `inserted` false — so exactly one report exists and the
	// caller knows whether to emit the event (emit only on a true insert). This is
	// the persistence half of exactly-once-in-effect. The report carries OwnerTeam
	// (server-set from the monitor), which the adapter persists for tenant scoping.
	Save(ctx context.Context, r DriftReport) (stored DriftReport, inserted bool, err error)
	// GetByID returns a report by id SCOPED to ownerTeam, or ErrRepoNotFound. WHY
	// team-scoped: report ids are not secret (they ride on events, retrain requests,
	// deep links, and logs), so id alone is NOT an authorization control. A mismatch
	// returns ErrRepoNotFound — INDISTINGUISHABLE from "no such id" — so a caller
	// cannot probe which ids exist in other tenants (no enumeration oracle).
	GetByID(ctx context.Context, ownerTeam, id string) (DriftReport, error)
	// List returns a page of a (team, model)'s reports (newest window_end first)
	// under the filter, plus a next-page cursor. f.OwnerTeam (service-set) and
	// f.ModelName together form the scope.
	List(ctx context.Context, f ReportFilter, opts ListOptions) (reports []DriftReport, nextToken string, err error)
	// LatestByModel returns the most recent report for a team's model (for
	// ModelHealth / MonitorStatus), or ErrRepoNotFound if none yet. Scoped by
	// (ownerTeam, modelName) so a same-named model in another team can't bleed its
	// latest verdict into this team's health tile.
	LatestByModel(ctx context.Context, ownerTeam, modelName string) (DriftReport, error)
	// PurgeByModel hard-deletes a team's reports for a model (GDPR / test cleanup)
	// and returns how many were removed. Used by DeleteMonitor(purge_reports=true).
	// Scoped by (ownerTeam, modelName): deleting team-a's "fraud" monitor with purge
	// MUST NOT touch team-b's "fraud" history (cross-tenant data destruction).
	PurgeByModel(ctx context.Context, ownerTeam, modelName string) (purged int, err error)
}

// ============================================================================
// LIVE WINDOW PORT (Redis adapter implements this)
// ============================================================================

// WindowStore holds the in-flight per-model windows (the live Redis state). The
// FOLD logic lives on the Window domain type; this port is just durable
// load/save/close around it. WHY a port (not direct Redis): the streaming-
// aggregation loop is unit-tested against an in-memory implementation of this
// interface — no Redis required to verify windowing correctness.
type WindowStore interface {
	// LoadOrOpen returns the current open window for a monitor, opening a fresh one
	// (NewWindow) if none exists. `openedAt` seeds a new window's OpenedAt.
	LoadOrOpen(ctx context.Context, monitorID, modelName string, openedAt time.Time) (*Window, error)
	// Save persists the window state after a fold (Add). Called on every consumed
	// event; must be cheap.
	Save(ctx context.Context, w *Window) error
	// Close atomically retires the current window (so the next LoadOrOpen opens a
	// fresh one). Called after a closed window is scored. Idempotent: closing an
	// already-closed window is a no-op.
	Close(ctx context.Context, monitorID, windowID string) error
	// CurrentSampleCount returns the fill of the open window (for MonitorStatus)
	// without loading the whole summary. 0 if no open window.
	CurrentSampleCount(ctx context.Context, monitorID string) (int, error)
}

// ============================================================================
// GROUND-TRUTH PORT (Postgres/Redis adapter implements this)
// ============================================================================

// GroundTruthStore records delayed labels and the predictions they join to, for
// rolling performance-decay math. WHY a dedicated store: predictions and their
// truths arrive at very different times (minutes/days apart), so they cannot live
// in a single window — they need their own retention-bounded store keyed by
// request_id.
type GroundTruthStore interface {
	// RecordPrediction stores (request_id → predicted label) when an inference is
	// observed, so a later label can be joined. Bounded by a retention window.
	RecordPrediction(ctx context.Context, modelName, requestID, predicted string, at time.Time) error
	// RecordLabel stores a delayed true outcome for a request_id IF that request_id
	// was previously observed for this model. Returns matched=false for an unknown
	// or aged-out id (the caller reports it back as unmatched, never silently
	// trusting a fabricated id). Idempotent per request_id (re-recording the same
	// label does not double-count).
	RecordLabel(ctx context.Context, modelName, requestID, actual string, observedAt time.Time) (matched bool, err error)
	// RecentPairs returns the matched (predicted, actual) pairs within the rolling
	// performance window for a model — the input to Accuracy/PerformanceDrop.
	RecentPairs(ctx context.Context, modelName string, window time.Duration) ([]LabelPair, error)
}

// ============================================================================
// BASELINE PORT (Registry / Experiment-Tracker gRPC client implements this)
// ============================================================================

// Baseline is a model version's training-time distribution — the comparison
// reference for drift. WHY the monitor RESOLVES this itself (server-authoritative)
// rather than accepting it from a client: a client that could upload a baseline
// could hand-craft one that matches current traffic and suppress a real drift
// signal. The baseline is fetched from Registry/Experiment-Tracker by version.
type Baseline struct {
	ModelName  string
	Version    string
	CapturedAt time.Time

	// FeatureEdges gives the bin layout per feature (the edges captured at training
	// time). The current window's feature values are binned onto THESE edges so the
	// two histograms align. nil for a feature the baseline didn't capture.
	FeatureEdges map[string][]float64
	// FeatureBaselines is the baseline histogram per feature (training-time bin
	// counts), aligned to FeatureEdges. The "base" side of PSI/KL/KS.
	FeatureBaselines map[string]Histogram

	// PredictionOrder is the canonical class order for prediction drift, and
	// PredictionBaseline is the training-time class distribution over that order.
	PredictionOrder    []string
	PredictionBaseline Histogram

	// Accuracy is the registered baseline accuracy — the reference for performance
	// decay (PerformanceDrop = baseline − current).
	Accuracy float64
}

// BaselineProvider resolves a model version's training-time baseline. The adapter
// is a gRPC client to Registry/Experiment-Tracker. Returns ErrBaselineUnavailable
// (surfaced as-is) when the version has no captured distribution.
type BaselineProvider interface {
	// GetBaseline resolves the baseline for an explicit version. Empty version =
	// the current PRODUCTION version (the "refresh to latest" case).
	GetBaseline(ctx context.Context, modelName, version string) (Baseline, error)
}

// ============================================================================
// CONTROL-LOOP PORTS (the loop-closers)
// ============================================================================

// DriftPublisher emits the ModelDriftDetected event to the bus (NATS). WHY a port,
// not a direct NATS dependency: the domain decides WHAT/ WHEN to publish; the
// adapter owns the envelope, the subject (fp.models.drift.detected), and trace
// propagation. The domain passes a transport-neutral DriftEvent.
type DriftPublisher interface {
	// PublishDrift emits one drift event. Called only on a TRUE report insert
	// (idempotency) so the event fires exactly once per window.
	PublishDrift(ctx context.Context, e DriftEvent) error
}

// DriftEvent is the transport-neutral payload the domain hands the publisher. The
// NATS adapter maps it to the canonical events.ModelDriftDetected (flattening the
// report + the control fields) — the domain does NOT import the events proto. The
// FAT-BUT-FLAT shape lets the orchestrator close the loop with NO callback into
// this service.
type DriftEvent struct {
	Report            DriftReport
	AutoRetrain       bool   // from the monitor — tells the orchestrator whether to act
	RetrainPipelineID string // the pipeline to run; opaque
	OwnerTeam         string // for tenant-scoped routing/audit on the consumer side
}

// Orchestrator is the gRPC-client port to the Pipeline Orchestrator — THE
// LOOP-CLOSER. When a CRITICAL breach fires and auto_retrain is on, the policy
// calls TriggerRetrain, which re-runs the model's training pipeline. The new
// version is canaried and promoted, and the serve→monitor→retrain loop closes.
//
// SECURITY (SSRF-adjacent): the pipeline id is OPAQUE and operator-configured at
// ConfigureMonitor time, never derived from event data — so a poisoned inference
// event cannot redirect the retrain to an attacker-chosen target. The adapter
// calls a FIXED orchestrator endpoint (config), not a URL from the event.
type Orchestrator interface {
	// TriggerRetrain starts the model's retrain pipeline, passing the drift report
	// id as input/correlation. Returns the orchestrator's execution id (for audit /
	// the cooldown record). Idempotency on the orchestrator side is keyed by the
	// (pipeline_id, report_id) the caller supplies.
	TriggerRetrain(ctx context.Context, in RetrainRequest) (executionID string, err error)
}

// RetrainRequest is the transport-neutral input to a retrain trigger.
type RetrainRequest struct {
	PipelineID   string // opaque, operator-configured (NOT from event data)
	ModelName    string
	ModelVersion string
	ReportID     string // correlation + orchestrator-side idempotency key
	Reason       string // human-readable ("data drift PSI=0.41 on income"), for audit
}

// RetrainGate guards the closed loop against retrain STORMS (the debounce/cooldown
// half of the control loop). WHY essential: a sustained drift can breach CRITICAL
// on EVERY window close (every few seconds for a busy model). Without a cooldown
// the monitor would fire a retrain pipeline on every breach — flooding the
// orchestrator, burning compute, and thrashing the served model. The gate records
// the last trigger time per model and refuses a new trigger inside the cooldown.
//
// This is the SAME idea as Alertmanager's `repeat_interval`, a circuit breaker's
// open state, or a thermostat's anti-short-cycle delay.
type RetrainGate interface {
	// LastTriggered returns when this model's retrain pipeline was last fired (zero
	// time if never). The policy compares it against the cooldown.
	LastTriggered(ctx context.Context, modelName string) (time.Time, error)
	// MarkTriggered records a retrain trigger at `at`, starting a fresh cooldown.
	// Should be atomic with the trigger decision (set-if-absent within cooldown) so
	// two concurrent breaches can't both fire; the adapter implements that with a
	// Redis SET NX PX or a Postgres conditional update.
	MarkTriggered(ctx context.Context, modelName string, at time.Time) error
}
