package events

import (
	"time"

	eventsv1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/events/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// ============================================================================
// WIRE → DOMAIN FIELD EXTRACTION (consume side)
// ============================================================================
//
// These helpers translate the events.v1 summary sub-messages into the narrow
// shape the domain's InferenceObservation wants. The monitor folds only the
// distributional SIGNAL (per-feature scalars, the top predicted label) — never raw
// tensors or PII (the gateway already strips those; the summaries ARE the signal).
// Kept here, beside the codec, so the wire shape lives in one place.

// featuresFromSummary extracts the per-feature scalar map the window folds for
// DATA-drift. It returns the FeatureSummary.feature_values map directly (already a
// map[string]float64 in the generated type). nil summary ⇒ nil map (the domain
// treats an absent summary as "no data-drift signal this call").
//
// We deliberately COPY the map rather than alias the proto's internal map: the
// decoded proto value is local to the handler and discarded, but copying makes the
// observation self-contained (no shared mutable state with a value that may be
// reused/reset by a pooled decoder) — cheap for the small summary maps the gateway
// sends.
func featuresFromSummary(fs *eventsv1.FeatureSummary) map[string]float64 {
	if fs == nil {
		return nil
	}
	vals := fs.GetFeatureValues()
	if len(vals) == 0 {
		return nil
	}
	out := make(map[string]float64, len(vals))
	for k, v := range vals {
		out[k] = v
	}
	return out
}

// predictedTop extracts the top predicted label for PREDICTION-drift. nil summary
// or empty label ⇒ "" (the domain treats an empty PredictedTop as "no prediction
// signal", so it is not counted in the per-class tally).
func predictedTop(ps *eventsv1.PredictionSummary) string {
	if ps == nil {
		return ""
	}
	return ps.GetTopLabel()
}

// timeOrNow converts an event's producer timestamp to a time.Time, falling back to
// the current wall clock when the field is absent/zero. WHY a fallback (not the
// proto epoch): the observation's ObservedAt drives the time-based window close; a
// zero/1970 value would make a window look instantly stale and close prematurely.
// A producer always stamps completed_at/failed_at, so the fallback is a safety net
// for a malformed event, not a normal path.
func timeOrNow(ts *timestamppb.Timestamp) time.Time {
	if ts == nil {
		return time.Now().UTC()
	}
	t := ts.AsTime()
	if t.IsZero() {
		return time.Now().UTC()
	}
	return t
}
