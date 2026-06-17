// Package domain is the innermost ring of the Clean Architecture onion for the
// Experiment Tracker service. It holds the canonical business types —
// Experiment, Run, Param, MetricPoint, MetricSeries — and the pure business
// rules over them (the run-status state machine, batch dedup keys, final-metric
// projection). It has ZERO framework imports: only the standard library and
// github.com/google/uuid. The handler converts proto↔domain; the repository
// converts domain↔SQL rows; the domain itself never knows either exists.
//
// ============================================================================
// PATTERN — Event-Driven (Async Batch Ingestion): where the domain fits in
// ============================================================================
//
// This service's defining trait is HOW metrics get ingested. The DOMAIN models
// the *business operations* behind both ingestion paths, independent of the
// transport that triggers them:
//
//   - SYNC path:  a training job's gRPC LogMetrics(batch) lands here as
//     ExperimentService.LogMetrics — a slice of MetricPoint, validated, deduped,
//     timestamp-stamped, and persisted in one transactional write.
//   - ASYNC path: the NATS batch consumer (events layer, a LATER phase) decodes
//     platform events into the SAME domain MetricPoints and calls the SAME
//     LogMetrics. The buffer/flush/back-pressure machinery lives in the events
//     adapter; the per-batch *business* logic (validate, dedup, project finals)
//     lives HERE so both paths share one implementation and one set of tests.
//
// The lesson the comments keep returning to: the domain doesn't care whether a
// batch came from a gRPC call or a NATS flush. It validates and dedups a slice
// of points. That transport-independence is precisely why the pattern's hard
// part (batching/back-pressure) can be added in the events layer WITHOUT
// touching the rules tested here.
//
// INTERVIEW FRAMING:
//
//	Q: "Where does the back-pressure live in your event-driven service?"
//	A: In the events adapter (the consumer NAKs when its buffer is full). The
//	   domain just exposes a batch-shaped LogMetrics; the adapter decides WHEN
//	   to flush a batch into it. Separation of mechanism (transport/buffering)
//	   from policy (validation/dedup) — the domain owns policy only.
package domain

import (
	"sort"
	"time"
)

// ============================================================================
// RUN STATUS — the lifecycle state machine
// ============================================================================
//
// WHY a typed string enum (not an int, not a bare string): a run has a small,
// known lifecycle and downstream code switches on it ("show failed runs"). A
// typed constant set is self-documenting and prevents typos. We use a string
// (not an iota int) so the value is human-readable in logs and DB rows and
// stable across schema changes — the DB column stores "RUNNING", not "1".
//
// STATE MACHINE (RUNNING is the ONLY non-terminal state):
//
//	          ┌──────────────► FINISHED   (job completed; metrics final)
//	          │
//	RUNNING ──┼──────────────► FAILED     (job crashed / OOM / bad data)
//	          │
//	          └──────────────► KILLED      (cancelled by user/orchestrator)
//
// Status is SERVER-AUTHORITATIVE: a run is born RUNNING (StartRun) and only the
// server moves it to a terminal state (UpdateRunStatus, or async on a
// PipelineFailed event). A client never sets status on creation — that would be
// a mass-assignment hole (a client could declare its run "FINISHED" with forged
// metrics). The legal-transition rule is encoded in CanTransitionTo below so the
// invariant lives in ONE testable place, not scattered across handlers.
type RunStatus string

const (
	// RunStatusUnspecified is the zero value. A run is never legitimately in this
	// state; treat it as a programming/serialization bug (it mirrors the proto's
	// RUN_STATUS_UNSPECIFIED = 0 required zero value).
	RunStatusUnspecified RunStatus = ""

	// RunStatusRunning is the only non-terminal state. New metrics are accepted
	// only while a run is RUNNING.
	RunStatusRunning RunStatus = "RUNNING"

	// RunStatusFinished means the run completed successfully; metrics are final.
	RunStatusFinished RunStatus = "FINISHED"

	// RunStatusFailed means the run errored out; metrics are partial but final.
	RunStatusFailed RunStatus = "FAILED"

	// RunStatusKilled means the run was cancelled by a user or the orchestrator.
	RunStatusKilled RunStatus = "KILLED"
)

// IsTerminal reports whether a status is an end state (no further metrics
// accepted). RUNNING is the only non-terminal state, so this is the negation of
// "== RunStatusRunning" — but a named predicate reads better at call sites
// ("if run.Status.IsTerminal()") and documents intent.
func (s RunStatus) IsTerminal() bool {
	return s == RunStatusFinished || s == RunStatusFailed || s == RunStatusKilled
}

// CanTransitionTo reports whether moving from the receiver status to `target`
// is a legal edge in the state machine. This is the SINGLE source of truth for
// the lifecycle rules — UpdateRunStatus/FinishRun call it instead of re-deriving
// the rules inline, so the invariant is tested once and can't drift between
// call sites.
//
// RULES:
//   - Only a RUNNING run may transition (terminal states are absorbing — you
//     cannot "un-finish" a run; its metrics have already been reported as final
//     and may be referenced by model lineage in the Registry).
//   - The target must itself be a terminal state (FINISHED/FAILED/KILLED). You
//     never transition TO RunStatusRunning (a run is born RUNNING via StartRun;
//     it is never re-opened) and never TO Unspecified.
func (s RunStatus) CanTransitionTo(target RunStatus) bool {
	if s != RunStatusRunning {
		return false // terminal (or unspecified) states are absorbing
	}
	return target.IsTerminal()
}

// ============================================================================
// RUN SOURCE — which ingestion path created the run
// ============================================================================
//
// WHY record it: a run may be opened by a first-party training job (SYNC,
// StartRun) OR materialized by the async NATS consumer from a platform event
// (e.g. observing fp.models.version.created and opening a run to attach
// production metrics). Recording the source makes the dual-ingestion pattern
// VISIBLE in the data — invaluable when debugging "why does this run have no
// params?" (answer: it came from an event, not a training job).
type RunSource string

const (
	// RunSourceUnspecified is the zero value (mirrors RUN_SOURCE_UNSPECIFIED).
	RunSourceUnspecified RunSource = ""

	// RunSourceAPI: created via the sync gRPC StartRun by a first-party job/SDK.
	RunSourceAPI RunSource = "API"

	// RunSourceEvent: materialized by the async NATS batch consumer from a
	// platform event. Set by the events adapter, never by a client.
	RunSourceEvent RunSource = "EVENT"
)

// ============================================================================
// EXPERIMENT
// ============================================================================
//
// An Experiment is a NAMED GROUPING of runs that share a goal (e.g.
// "fraud-detector-v3 sweep"). You compare runs WITHIN an experiment — the MLflow
// "experiment → runs" hierarchy.
//
// SECURITY: OwnerID and Team are SERVER-AUTHORITATIVE. They are set from the
// authenticated caller's claims (injected by the auth interceptor), NEVER taken
// from the create request — otherwise a caller could create an experiment
// "owned" by someone else (mass-assignment). ArchivedAt is server-set on
// ArchiveExperiment; an unset (nil) value means active.
type Experiment struct {
	ID          string
	Name        string            // unique per team
	Description string            // free text
	Tags        map[string]string // organizational labels (NOT metrics)
	OwnerID     string            // SERVER-set from auth claims
	Team        string            // SERVER-set from auth claims (tenancy boundary)
	CreatedAt   time.Time         // SERVER-set, immutable
	UpdatedAt   time.Time         // SERVER-set on every write
	ArchivedAt  *time.Time        // nil = active; non-nil = soft-deleted
}

// IsArchived is a pure predicate used by list filtering and to reject mutations
// on archived experiments. A nil ArchivedAt means active.
func (e Experiment) IsArchived() bool { return e.ArchivedAt != nil }

// ============================================================================
// PARAM — an INPUT to a run (a hyperparameter / config value)
// ============================================================================
//
// A Param is set BEFORE/at run start (learning_rate=0.01, optimizer="adam").
// Contrast a MetricPoint, an OUTPUT measured DURING the run.
//
// WHY Value is a string (not a typed scalar): params are heterogeneous (ints,
// floats, bools, strings) used for DISPLAY, GROUPING, and EQUALITY — never math.
// One string field keeps the schema trivial and stable as new param kinds
// appear. MLflow makes the same call. Params are write-once per key within a run
// (see LogParams) so a run's configuration can't silently change mid-flight.
type Param struct {
	Key   string
	Value string
}

// ============================================================================
// METRIC POINT — one sample of a metric time-series
// ============================================================================
//
// A metric in ML is a TIME-SERIES, not a single number: "loss" is a curve over
// training steps. Each point pins (Key, Value, Step, Timestamp):
//   - Key:       which metric ("loss", "accuracy", "val_auc")
//   - Value:     the measured number at this point (float64 — ML metrics are reals)
//   - Step:      the monotonic training step/epoch index — the SEMANTIC X axis
//     the user reasons about ("loss at epoch 10"). Client-supplied: the
//     training job owns its step counter.
//   - Timestamp: wall-clock time — the PHYSICAL axis for retention/partitioning.
//     SERVER-stamped on ingest (see StampMetricTimestamps) so a client can't
//     backdate a point into an old partition or the future.
//
// WHY both Step and Timestamp: Step is what charts plot against; Timestamp is
// what the RANGE-partitioned metrics table uses as its partition key (so old
// partitions can be dropped cheaply) and what time-window queries filter on.
type MetricPoint struct {
	Key       string
	Value     float64
	Step      int64
	Timestamp time.Time // SERVER-set on ingest; not trusted from the client
}

// metricKey is the DEDUP IDENTITY of a metric point WITHIN A RUN: the same
// (Key, Step) pair logged twice is the SAME logical sample — re-logging "loss at
// step 100" must not append a second, conflicting point that corrupts the curve.
//
// WHY (Key, Step) and NOT (Key, Step, Value): if we keyed on value too, a retry
// that re-sent "loss@100 = 0.5" AND a buggy double-log of "loss@100 = 0.6" would
// both be kept (different values → different keys) — exactly the corruption we
// want to prevent. Keying on (Key, Step) means the FIRST value for a (key, step)
// wins within a batch and a re-log is a no-op. This is the in-memory half of the
// idempotency guarantee; the DB half is a UNIQUE (run_id, key, step) index that
// the repository's INSERT ... ON CONFLICT DO NOTHING leans on (added in the repo
// phase). Defining the key here keeps the dedup rule testable without a DB.
type metricKey struct {
	key  string
	step int64
}

// DedupMetricPoints returns the input points with intra-batch duplicates
// removed: for each (Key, Step), the FIRST occurrence wins and later ones are
// dropped. Order of the surviving points is preserved (stable), so a chart's
// step ordering from the producer is respected.
//
// WHY "first wins" (not "last wins"): on a retry the client re-sends the SAME
// batch; first-wins makes the operation idempotent (re-running yields the same
// surviving set). Within a single legitimate flush a producer should never emit
// two values for one (key, step); if it does, treating the first as canonical is
// a defensible, deterministic choice (and the DB's ON CONFLICT DO NOTHING agrees:
// the already-stored row wins).
//
// This is a PURE function (no clock, no I/O) — the core of the batch-ingestion
// pattern's correctness, and trivially unit-testable. The async consumer and the
// sync RPC both run their batches through it, so dedup behaves identically on
// both ingestion paths.
func DedupMetricPoints(points []MetricPoint) []MetricPoint {
	seen := make(map[metricKey]struct{}, len(points))
	out := make([]MetricPoint, 0, len(points))
	for _, p := range points {
		k := metricKey{key: p.Key, step: p.Step}
		if _, dup := seen[k]; dup {
			continue // a point for this (key, step) already accepted in this batch
		}
		seen[k] = struct{}{}
		out = append(out, p)
	}
	return out
}

// StampMetricTimestamps sets every point's Timestamp to `now`, overwriting any
// client-supplied value. WHY overwrite unconditionally: Timestamp is the
// partition key and the retention axis — it MUST be server-authoritative so a
// client can't backdate metrics into a dropped partition or push them into the
// future. The training job owns Step (the semantic axis); the platform owns
// Timestamp (the physical axis). Returning a fresh slice keeps the function
// free of surprising in-place mutation for callers that still hold the input.
func StampMetricTimestamps(points []MetricPoint, now time.Time) []MetricPoint {
	out := make([]MetricPoint, len(points))
	for i, p := range points {
		p.Timestamp = now
		out[i] = p
	}
	return out
}

// ============================================================================
// METRIC SERIES — the grouped/columnar view of one metric on one run
// ============================================================================
//
// The shape a chart wants ("draw the loss curve"). CompareRuns and
// GetMetricHistory return these so a client can overlay the same metric across
// runs without re-grouping flat points. Points are ordered by Step then
// Timestamp (see GroupIntoSeries).
type MetricSeries struct {
	Key    string
	Points []MetricPoint
}

// GroupIntoSeries folds a flat slice of MetricPoints into per-key MetricSeries,
// each sorted by (Step, Timestamp). This is the chart-ready projection
// GetMetricHistory/CompareRuns hand back. Pure function — no I/O — so the
// grouping/ordering is unit-testable independent of storage.
//
// WHY sort by Step then Timestamp: Step is the semantic X axis a chart plots
// against; Timestamp is the tiebreaker when two points share a step (rare, e.g.
// a re-stamped re-log that slipped past dedup across batches). Deterministic
// ordering makes the curve render the same every time and makes test assertions
// stable.
func GroupIntoSeries(points []MetricPoint) []MetricSeries {
	byKey := make(map[string][]MetricPoint)
	// Preserve first-seen key order so output is deterministic without depending
	// on Go's randomized map iteration.
	var order []string
	for _, p := range points {
		if _, ok := byKey[p.Key]; !ok {
			order = append(order, p.Key)
		}
		byKey[p.Key] = append(byKey[p.Key], p)
	}

	series := make([]MetricSeries, 0, len(order))
	for _, key := range order {
		pts := byKey[key]
		sort.SliceStable(pts, func(i, j int) bool {
			if pts[i].Step != pts[j].Step {
				return pts[i].Step < pts[j].Step
			}
			return pts[i].Timestamp.Before(pts[j].Timestamp)
		})
		series = append(series, MetricSeries{Key: key, Points: pts})
	}
	return series
}

// ComputeFinalMetrics projects a flat metric history into the denormalized
// "headline number per key" that ListRuns and the comparison UI render without
// reading the whole series. For each metric key it returns the point at the
// HIGHEST step (the latest sample on the semantic axis), ties broken by the
// latest Timestamp.
//
// WHY "latest by step" rather than "best": "best" is metric-specific (higher is
// better for accuracy, lower for loss) and the service can't know each metric's
// polarity without extra config. The LAST value on the curve is the unambiguous,
// universally-meaningful summary ("final accuracy = 0.94"). A future enhancement
// could let a user mark a key's polarity to surface a best-so-far; the proto
// comment calls final_metrics "final/best", and "final" is the safe default we
// implement. This is the CQRS-flavored "projection": a read-optimized snapshot
// of the write-side series, computed when the run finishes.
//
// Returned points carry the winning sample's Value/Step/Timestamp. Output is
// sorted by Key for deterministic comparison/serialization.
func ComputeFinalMetrics(history []MetricPoint) []MetricPoint {
	best := make(map[string]MetricPoint)
	for _, p := range history {
		cur, ok := best[p.Key]
		if !ok {
			best[p.Key] = p
			continue
		}
		// Latest by step; tie-break by latest timestamp. Strictly-greater checks
		// keep the FIRST point among exact (step,timestamp) ties — deterministic.
		if p.Step > cur.Step || (p.Step == cur.Step && p.Timestamp.After(cur.Timestamp)) {
			best[p.Key] = p
		}
	}

	out := make([]MetricPoint, 0, len(best))
	for _, p := range best {
		out = append(out, p)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out
}

// ============================================================================
// RUN — one execution within an experiment (the unit you compare)
// ============================================================================
//
// A Run is ONE training job / one set of hyperparameters / one resulting metric
// curve. LINK TO MODEL VERSIONS: ModelVersionID ties the run that PRODUCED a
// model to that model's artifact in the Registry — stored as an OPAQUE string,
// NOT by importing the registry proto. Services are decoupled; the contract
// between them is the ID value plus async events, not a compile-time dependency
// (the same reasoning behind "database per service").
//
// FinalMetrics is the denormalized headline snapshot (see ComputeFinalMetrics) —
// a read-optimization so ListRuns/leaderboards don't read the full series.
//
// SECURITY: ID, Status, Source, OwnerID, StartedAt, EndedAt, and FinalMetrics
// are all SERVER-AUTHORITATIVE — none are accepted from a client on
// StartRun/LogMetrics. A client supplies only ExperimentID, an optional label,
// the target model version, params, and metric points.
type Run struct {
	ID             string
	ExperimentID   string
	DisplayName    string
	Status         RunStatus
	Source         RunSource
	ModelVersionID string // opaque Registry id; empty if the run yields no model
	OwnerID        string // SERVER-set from auth claims
	Params         []Param
	FinalMetrics   []MetricPoint  // SERVER-computed denormalized headline metrics
	StartedAt      time.Time      // SERVER-set at StartRun, immutable
	EndedAt        *time.Time     // SERVER-set at terminal transition; nil while RUNNING
	Artifacts      map[string]any // free-form JSON-shaped side-artifacts (SetRunArtifacts); nil if none
}
