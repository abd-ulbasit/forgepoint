// monitor_service.go — the MonitorService interface: the primary PORT the handler
// (and the NATS consumer) reach the business logic through.
//
// ============================================================================
// THE INTERFACE AS A PORT (Clean / Hexagonal Architecture)
// ============================================================================
//
// The domain OWNS this interface; the handler DEPENDS on it; the concrete impl
// (monitorService in monitor_service_impl.go) lives one ring in. Dependency
// inversion: the handler holds a MonitorService and never knows whether it's
// backed by Postgres+Redis+NATS or by in-memory test fakes. Handler tests inject a
// mock; the streaming consumer injects the real one.
//
// TWO PLANES, ONE SERVICE (this is the key mental model):
//
//	CONTROL/OBSERVABILITY PLANE (gRPC RPCs — the handler calls these):
//	  ConfigureMonitor, DeleteMonitor, ResetBaseline,
//	  GetModelHealth, GetMonitorStatus, ListMonitors,
//	  GetDriftReport, ListDriftReports, SubmitGroundTruth
//	  → configure the loop, read what it found, feed it delayed truth.
//
//	DATA PLANE (event-driven — the NATS consumer calls these):
//	  ObserveInference  (fold an InferenceCompleted into the window)
//	  ResetBaselineFromPromotion (a ModelPromoted re-pins the baseline)
//	  → the streaming-aggregation + closed-loop heartbeat. NOT RPCs.
//
// The most important behavior on the platform — windowing the inference stream,
// scoring, and firing the retrain — runs on the DATA plane (ObserveInference),
// driven by NATS, not by a client call. The gRPC surface is the control panel
// over that loop.
package domain

import (
	"context"
	"time"
)

// ============================================================================
// INPUT TYPES — explicit structs so the handler converts proto → these (never
// passing proto into the domain), and so adding a field later is backward-
// compatible (no positional-arg churn). Each carries ONLY the operator-owned
// surface; server-authoritative fields are set by the service, not accepted here.
// ============================================================================

// ConfigureMonitorInput is the writable surface of ConfigureMonitor. It
// deliberately omits id/owner_team/state/baseline_*/timestamps — the mass-
// assignment guard. OwnerTeam is passed SEPARATELY (from auth claims) into the
// service method, NOT as a field here, so it can never arrive on the wire.
type ConfigureMonitorInput struct {
	ModelName         string
	WindowDuration    time.Duration
	WindowSize        int
	MinSamples        int
	Thresholds        []ThresholdConfig
	AutoRetrain       bool
	RetrainPipelineID string
	Enabled           bool // false ⇒ PAUSED (windowed, not scored); true ⇒ scoring
	IdempotencyKey    string
}

// SubmitGroundTruthInput backfills delayed labels (batched). The service caps the
// batch at MaxGroundTruthBatch and only accepts labels for request_ids it has
// actually observed (anti-fabrication — see the impl).
type SubmitGroundTruthInput struct {
	ModelName      string
	Labels         []GroundTruthLabel
	IdempotencyKey string
}

// SubmitGroundTruthResult reports how a label batch was applied — NOT a bare ack.
// Ground truth is fuzzy (some ids won't match an observed prediction, some aged
// out), and the caller needs to know what stuck so it can retry/alert.
type SubmitGroundTruthResult struct {
	Accepted            int
	UnmatchedRequestIDs []string
}

// ============================================================================
// THE SERVICE INTERFACE
// ============================================================================

// MonitorService is the primary domain interface for drift monitoring + the
// closed loop. Every method takes a context (deadline/trace propagation) and,
// where authorization is tenant-scoped, an explicit `ownerTeam` derived by the
// handler from the caller's auth claims — NEVER a client-supplied field.
type MonitorService interface {
	// -----------------------------------------------------------------------
	// CONTROL PLANE
	// -----------------------------------------------------------------------

	// ConfigureMonitor upserts a model's monitor (one per model per team). The
	// service sets all server-authoritative fields (id on insert, owner_team from
	// the passed claims, state, baseline_* by resolving the baseline, timestamps).
	// Validates: window bounds (size in [0, MaxWindowSize]; 0 <= min_samples <=
	// size when size>0; at least one of duration/size set so windows can close),
	// threshold monotonicity (warn <= critical), and auto_retrain ⇒ pipeline set.
	// Returns the stored monitor and whether it was newly created.
	ConfigureMonitor(ctx context.Context, ownerTeam string, in ConfigureMonitorInput) (monitor Monitor, created bool, err error)

	// DeleteMonitor stops monitoring a model (soft-delete by default). If
	// purgeReports, ALSO hard-deletes the drift history and returns how many were
	// purged. Idempotent: deleting an absent monitor is a successful no-op.
	DeleteMonitor(ctx context.Context, ownerTeam, modelName string, purgeReports bool) (purgedReports int, err error)

	// ResetBaseline re-pins the drift baseline to a model version's training
	// distribution (operator escape hatch; auto-reset happens on ModelPromoted).
	// Empty version = the current production version. The baseline is SERVER-
	// resolved from the named version (a client names a version, never a distribution).
	ResetBaseline(ctx context.Context, ownerTeam, modelName, version string) (Monitor, error)

	// GetModelHealth returns the at-a-glance verdict (overall severity + per-type
	// lights + state) for one model. The one-line "is this model healthy?" read.
	GetModelHealth(ctx context.Context, ownerTeam, modelName string) (ModelHealth, error)

	// GetMonitorStatus returns the live status (lifecycle, current-window fill,
	// latest report, lifetime drift count) — the operator drill-in (reads the live
	// window).
	GetMonitorStatus(ctx context.Context, ownerTeam, modelName string) (MonitorStatus, error)

	// ListMonitors returns the caller team's monitored-model fleet (config + current
	// health per row), paginated, with optional severity/state filters. Always
	// team-scoped by the passed ownerTeam.
	ListMonitors(ctx context.Context, ownerTeam string, minSeverity DriftSeverity, state MonitorState, opts ListOptions) (entries []FleetEntry, nextToken string, err error)

	// GetDriftReport fetches one report by id, SCOPED to ownerTeam. The auth
	// interceptor only proves the caller is SOME authenticated team, not that they
	// own THIS report — and report ids are NOT secret (they ride on the
	// ModelDriftDetected event, RetrainRequest.ReportID, deep-link URLs, and logs).
	// So id alone cannot be the authz control (that would be IDOR). The service
	// passes the claim-derived ownerTeam to a team-scoped lookup; a report owned by
	// another team returns ErrReportNotFound (indistinguishable from "no such id" —
	// no enumeration oracle, and not a distinguishable "forbidden").
	GetDriftReport(ctx context.Context, ownerTeam, reportID string) (DriftReport, error)

	// ListDriftReports returns a team's drift history (newest first), paginated, with
	// optional severity, time-range, AND model_name filters. ownerTeam is service-set
	// from auth claims and is the MANDATORY scope. model_name is OPTIONAL: when set it
	// narrows to one model (model names are not unique across teams, so owner_team must
	// scope first — otherwise a same-named model in another team would leak); when EMPTY
	// it returns the most-recent reports across ALL of this team's models (the fleet /
	// dashboard "recent drift" view). Tenancy is never relaxed — empty model_name means
	// "all of MY models", never an unscoped read.
	ListDriftReports(ctx context.Context, ownerTeam string, f ReportFilter, opts ListOptions) (reports []DriftReport, nextToken string, err error)

	// SubmitGroundTruth backfills delayed true outcomes (batched) for performance
	// decay. Only accepts labels for observed request_ids; reports unmatched ones.
	SubmitGroundTruth(ctx context.Context, ownerTeam string, in SubmitGroundTruthInput) (SubmitGroundTruthResult, error)

	// -----------------------------------------------------------------------
	// DATA PLANE (event-driven — the NATS consumer calls these, not the handler)
	// -----------------------------------------------------------------------

	// ObserveInference folds ONE inference observation into the model's window —
	// the streaming-aggregation heartbeat. On window close it SCORES the window
	// against the baseline, persists a report (idempotent on window id), and runs
	// the closed-loop policy (emit ModelDriftDetected; if CRITICAL + auto_retrain +
	// cooldown-elapsed, trigger the retrain pipeline). Returns the report produced
	// when a window closed and scored, or nil when the event was merely folded in.
	//
	// WHY one method does fold+close+score+act: it keeps the loop atomic per event
	// from the consumer's view — the consumer just hands over an observation and
	// acks; everything downstream (including idempotency) is the service's job.
	ObserveInference(ctx context.Context, obs InferenceObservation) (*DriftReport, error)

	// ResetBaselineFromPromotion re-pins a model's baseline to the newly promoted
	// production version. Driven by the ModelPromoted event (to_stage=PRODUCTION).
	// WHY: without it, every new deploy reads as drift vs a stale baseline. The
	// events adapter resolves the model's owning team (the event carries no auth
	// claims) and passes it here, so the re-pin reuses the same team-scoped path as
	// the manual ResetBaseline. A no-monitor model is a silent no-op (not an error
	// on an event path — many promoted models simply aren't monitored).
	ResetBaselineFromPromotion(ctx context.Context, ownerTeam, modelName, newProdVersion string) error
}

// FleetEntry pairs a monitor with its current health for the fleet view — so the
// UI renders the whole table from ONE call (no N+1 GetModelHealth per row).
type FleetEntry struct {
	Monitor Monitor
	Health  ModelHealth
}
