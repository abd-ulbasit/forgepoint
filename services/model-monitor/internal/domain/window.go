// window.go — the SLIDING WINDOW: the streaming-aggregation half of the pattern.
//
// ============================================================================
// WHAT A WINDOW IS AND WHY
// ============================================================================
//
// The monitor consumes an unbounded stream of InferenceCompleted events. It can
// NOT keep every event — that's gigabytes per popular model per day, and it would
// leak raw inputs. Instead it folds each event into a bounded, COMPACT running
// summary: per-feature histograms, prediction-class counts, and the set of
// request_ids seen (so delayed ground truth can be joined later). When the window
// "closes" (a time OR count bound is hit), the monitor scores it against the
// baseline and starts a fresh window.
//
// This is exactly Flink / Kafka Streams TUMBLING-window aggregation: events →
// incremental fold → emit-on-close → reset. We use a TUMBLING window (each event
// belongs to exactly one window) rather than a true overlapping sliding window
// because (a) it's simpler to reason about idempotency — one WindowID per closed
// window — and (b) overlapping windows re-score the same events repeatedly, which
// for drift adds cost without adding much signal. The proto/design call it a
// "sliding window"; tumbling is the pragmatic realization and the term used
// loosely across the industry (Arize/WhyLabs batch by time bucket too).
//
// CLOSE CONDITION — EITHER bound (whichever first):
//   - COUNT: SampleCount >= WindowSize  (caps memory for high-traffic models)
//   - TIME:  now - OpenedAt >= WindowDuration  (forces progress for low-traffic)
//
// A monitor may set one or both. At least one must be set or the window never
// closes (validated at configure time, not here).
//
// ============================================================================
// WHY THIS LIVES IN THE DOMAIN (not in Redis code)
// ============================================================================
//
// The PRODUCTION window lives in Redis (durable across restarts, shared if the
// consumer scales out). But the FOLD LOGIC — how an event mutates the summary,
// when a window closes — is pure business logic with no I/O, so it lives here and
// is unit-tested without Redis. The Redis adapter (later phase) is a thin
// load/save around this struct: load the summary, call Add, save it back, and on
// close call Snapshot. Keeping the math here keeps the windowing correctness
// tests fast and the Redis code trivial.
package domain

import (
	"time"

	"github.com/google/uuid"
)

// InferenceObservation is the slice of an InferenceCompleted event the window
// actually needs. The NATS consumer adapter maps the proto event to this — the
// domain never sees the proto type. WHY a narrow struct (not the whole event):
// the window only needs the distributional signal + the join key; carrying the
// full event into the domain would couple it to the event schema and waste memory.
//
// MONITOR BINDING (MonitorID / OwnerTeam): an InferenceCompleted event carries NO
// tenancy and NO monitor reference. The EVENTS ADAPTER resolves model → owning
// team → monitor (from the registry / the monitors table) BEFORE handing the
// observation to the domain, and stamps the resolved MonitorID + OwnerTeam here.
// WHY resolve in the adapter, not the domain: the domain must not fabricate a
// tenant from untrusted event data (a cross-tenant footgun) — it receives an
// already-authorized binding. An observation with an empty MonitorID is for a
// model with no monitor and is simply ignored.
type InferenceObservation struct {
	MonitorID    string             // resolved by the events adapter; empty ⇒ no monitor (ignore)
	OwnerTeam    string             // resolved by the events adapter (tenancy fact)
	RequestID    string             // join key for delayed ground truth (NOT PII)
	ModelName    string             // the model that served
	ModelVersion string             // the version that actually served (canary-aware)
	Features     map[string]float64 // per-feature scalar summary (data-drift input)
	PredictedTop string             // top predicted class/label (prediction-drift input)
	ObservedAt   time.Time          // event time (drives the time-based close)
}

// Window is the in-flight per-model aggregation state. It holds ONLY summaries —
// never raw events. It is the unit the Redis adapter persists between events.
type Window struct {
	ID        string    // stable WindowID, assigned at open; the report idempotency key
	MonitorID string    // owning monitor
	ModelName string    // for the eventual report
	OpenedAt  time.Time // for the time-based close

	// ModelVersion is the version this window is scoring. WHY pin one version: a
	// report must be tied to the EXACT served version (vital under canary
	// splitting). The window adopts the version of its FIRST observation and, by
	// policy, a different version starts a NEW window — mixing two versions'
	// traffic in one drift score would be meaningless. (The split is enforced by
	// the caller, which opens a fresh window on version change; Window records the
	// version it committed to.)
	ModelVersion string

	SampleCount int // observations folded in so far (the count-based close input)

	// featureValues accumulates the raw per-feature values seen this window, keyed
	// by feature name. At close we BIN these against the baseline's edges to form
	// the current histogram. WHY keep raw values rather than pre-binning on the
	// fly: the baseline edges are not known until score time (baseline may be
	// re-pinned mid-window via ModelPromoted/ResetBaseline), so binning is deferred
	// to Snapshot. For very high cardinality the Redis adapter may switch to a
	// reservoir sample; the domain models the exact-values case for clarity/tests.
	featureValues map[string][]float64

	// predictionCounts is the per-class tally for prediction drift — a true
	// streaming aggregate (a counter, not a value list), so it stays O(#classes)
	// regardless of traffic.
	predictionCounts map[string]float64

	// requestIDs records the join keys observed this window so a later
	// SubmitGroundTruth can match delayed labels back. Stored as a set (the value
	// is unused). The Redis adapter caps/expires these per the retention policy;
	// the domain models the set for join correctness.
	requestIDs map[string]struct{}
}

// NewWindow opens a fresh window for a monitor. The WindowID is a UUID — the
// stable idempotency key that makes report persistence and event emission
// exactly-once-in-effect even under stream redelivery.
func NewWindow(monitorID, modelName string, openedAt time.Time) *Window {
	return &Window{
		ID:               uuid.NewString(),
		MonitorID:        monitorID,
		ModelName:        modelName,
		OpenedAt:         openedAt,
		featureValues:    make(map[string][]float64),
		predictionCounts: make(map[string]float64),
		requestIDs:       make(map[string]struct{}),
	}
}

// Add folds one observation into the window (the streaming aggregation step). It
// is idempotent on RequestID WITHIN a window: re-adding an already-seen request_id
// is a no-op, so a redelivered InferenceCompleted does not double-count and skew
// the very drift score it feeds. (Cross-window idempotency is handled by the
// WindowID-keyed report.)
//
// Returns true if the observation was newly folded in, false if it was a
// duplicate (already-seen request_id) and ignored.
func (w *Window) Add(obs InferenceObservation) bool {
	if obs.RequestID != "" {
		if _, seen := w.requestIDs[obs.RequestID]; seen {
			return false // duplicate within this window — ignore (idempotent fold)
		}
		w.requestIDs[obs.RequestID] = struct{}{}
	}
	// First observation pins the window's scored version (see ModelVersion note).
	if w.SampleCount == 0 && w.ModelVersion == "" {
		w.ModelVersion = obs.ModelVersion
	}
	for name, val := range obs.Features {
		w.featureValues[name] = append(w.featureValues[name], val)
	}
	if obs.PredictedTop != "" {
		w.predictionCounts[obs.PredictedTop]++
	}
	w.SampleCount++
	return true
}

// IsClosed reports whether the window has hit a close bound given the monitor's
// config and the current time. EITHER bound closes it (whichever first):
//   - count: SampleCount >= WindowSize (only when WindowSize > 0)
//   - time:  now - OpenedAt >= WindowDuration (only when WindowDuration > 0)
//
// WHY pass the monitor + now rather than store bounds on the window: the bounds
// are config that can change (ConfigureMonitor), and `now` must be injected for
// deterministic tests (no time.Now() in the domain math). This keeps the close
// decision a pure function of (window state, config, clock).
func (w *Window) IsClosed(m Monitor, now time.Time) bool {
	if m.WindowSize > 0 && w.SampleCount >= m.WindowSize {
		return true
	}
	if m.WindowDuration > 0 && !w.OpenedAt.IsZero() && now.Sub(w.OpenedAt) >= m.WindowDuration {
		return true
	}
	return false
}

// HasEnoughSamples reports whether the window has reached the monitor's
// MinSamples floor — i.e. whether it is statistically sound to SCORE. Below this
// the monitor stays WARMING_UP and skips scoring (a drift score over a near-empty
// window is noise). A MinSamples of 0 means "score whenever closed".
func (w *Window) HasEnoughSamples(m Monitor) bool {
	return w.SampleCount >= m.MinSamples
}

// PredictionHistogram projects this window's prediction tally onto the baseline's
// class order, producing a histogram aligned for PSI/KL against the baseline
// prediction distribution. See NewCategoricalHistogram for why a fixed order.
func (w *Window) PredictionHistogram(baselineOrder []string) Histogram {
	return NewCategoricalHistogram(baselineOrder, w.predictionCounts)
}

// FeatureHistogram projects one feature's accumulated window values onto the
// baseline's edges for that feature, producing a histogram aligned for PSI/KL/KS.
// Returns (histogram, true) if the feature was observed this window, else (_, false)
// so the scorer can skip a feature with no current data rather than scoring an
// empty histogram as drift.
func (w *Window) FeatureHistogram(feature string, baselineEdges []float64) (Histogram, bool) {
	vals, ok := w.featureValues[feature]
	if !ok || len(vals) == 0 {
		return Histogram{}, false
	}
	return NewNumericHistogram(baselineEdges, vals), true
}

// RequestIDs returns the join keys observed this window (a copy, so callers can't
// mutate the window's internal set). Used to match delayed ground-truth labels.
func (w *Window) RequestIDs() []string {
	out := make([]string, 0, len(w.requestIDs))
	for id := range w.requestIDs {
		out = append(out, id)
	}
	return out
}

// Features returns the names of features observed this window — the set the
// scorer iterates to compute per-feature data drift. A copy of the keys.
func (w *Window) Features() []string {
	out := make([]string, 0, len(w.featureValues))
	for name := range w.featureValues {
		out = append(out, name)
	}
	return out
}
