// fold_test.go — TDD specification for the EVENT-SOURCING FOLD (the pattern core).
//
// These tests pin the REFERENCE SEMANTICS of the materialized-view projections:
//   - ProjectLatest  (online "latest value now")
//   - ProjectAsOf    (offline "point-in-time / as-of T")
//   - foldView is exercised indirectly via the service tests (it is unexported).
//
// They assert REAL OUTCOMES — that the math/ordering is correct and that the fold
// is a PURE, REPLAYABLE function of the log — not mock interactions. This is the
// part an interviewer probes hardest ("prove your projection is deterministic and
// point-in-time correct"), so the tests double as the proof.
//
// In-package test (package domain) deliberately: the fold functions are part of
// the domain's public surface and have no port dependencies, so there is no
// import cycle to dodge here. The service tests use an EXTERNAL package where the
// cycle concern applies.
package domain

import (
	"reflect"
	"testing"
	"time"
)

// at is a tiny helper for readable event_times in tests: at(1) = a fixed base +1h.
func at(hours int) time.Time {
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	return base.Add(time.Duration(hours) * time.Hour)
}

// valueEvent builds a FeatureEventValuesWritten for one entity with one DOUBLE
// feature "x", at a given version + event_time. Keeps the table tests terse.
func valueEvent(version int64, entityID string, x float64, eventTime time.Time) FeatureEvent {
	return FeatureEvent{
		Type:          FeatureEventValuesWritten,
		FeatureViewID: "view-1",
		Version:       version,
		EntityID:      entityID,
		EventTime:     eventTime,
		Values:        map[string]FeatureValue{"x": {Kind: FeatureTypeDouble, Double: x}},
	}
}

// ============================================================================
// ProjectLatest — online "latest value now" (folds by VERSION)
// ============================================================================

func TestProjectLatest_PicksHighestVersionPerEntity(t *testing.T) {
	// Two writes for u1 (v1 then v3) and one for u2 (v2). Latest must be the
	// HIGHEST-version event per entity: u1→v3 (value 30), u2→v2 (value 20).
	// Deliberately pass the events OUT of version order to prove the fold sorts.
	events := []FeatureEvent{
		valueEvent(3, "u1", 30, at(3)),
		valueEvent(1, "u1", 10, at(1)),
		valueEvent(2, "u2", 20, at(2)),
	}

	got := ProjectLatest(events, 7)

	if len(got) != 2 {
		t.Fatalf("expected 2 entities, got %d", len(got))
	}
	if v := got["u1"]; v.Values["x"].Double != 30 || v.AsOfVersion != 3 {
		t.Errorf("u1: want value=30 asOfVersion=3, got value=%v version=%d", v.Values["x"].Double, v.AsOfVersion)
	}
	if v := got["u2"]; v.Values["x"].Double != 20 || v.AsOfVersion != 2 {
		t.Errorf("u2: want value=20 asOfVersion=2, got value=%v version=%d", v.Values["x"].Double, v.AsOfVersion)
	}
	// The schema_version provenance must be stamped onto every projected vector.
	if got["u1"].SchemaVersion != 7 {
		t.Errorf("u1: schemaVersion not propagated, got %d", got["u1"].SchemaVersion)
	}
}

func TestProjectLatest_IgnoresNonValueEvents(t *testing.T) {
	// Definition/deletion events carry no entity values and must NOT appear in the
	// online projection. Only the single value event should produce a vector.
	events := []FeatureEvent{
		{Type: FeatureEventViewDefined, Version: 1, ViewDef: &ViewDefinition{ID: "view-1"}},
		valueEvent(2, "u1", 5, at(2)),
		{Type: FeatureEventViewDeleted, Version: 3, EventTime: at(3)},
	}
	got := ProjectLatest(events, 1)
	if len(got) != 1 {
		t.Fatalf("expected only the value event to project, got %d entities", len(got))
	}
	if _, ok := got["u1"]; !ok {
		t.Error("u1 missing from projection")
	}
}

// ============================================================================
// ProjectLatest is a PURE FOLD — replayable, no input mutation
// ============================================================================

func TestProjectLatest_IsPureFold_Replayable(t *testing.T) {
	// REPLAYABILITY is THE event-sourcing guarantee: same events ⇒ same view, and
	// the fold must not mutate its input (RebuildViews replays the same slice
	// repeatedly). We (1) snapshot the input, (2) fold twice, (3) assert the two
	// results are identical AND the input slice is untouched (no reordering).
	events := []FeatureEvent{
		valueEvent(3, "u1", 30, at(3)),
		valueEvent(1, "u1", 10, at(1)),
		valueEvent(2, "u2", 20, at(2)),
	}
	snapshot := make([]FeatureEvent, len(events))
	copy(snapshot, events)

	first := ProjectLatest(events, 1)
	second := ProjectLatest(events, 1)

	if !reflect.DeepEqual(first, second) {
		t.Errorf("fold is not deterministic: first=%v second=%v", first, second)
	}
	if !reflect.DeepEqual(events, snapshot) {
		t.Errorf("fold mutated its input slice (reordered events); input must be immutable")
	}
}

// ============================================================================
// ProjectAsOf — point-in-time "as-of T" (folds by EVENT_TIME, version tie-break)
// ============================================================================

func TestProjectAsOf_ReconstructsValueAtTimestamp(t *testing.T) {
	// u1 has values at t1=10, t2=20, t3=30. A point-in-time read as-of t2 must see
	// 20 (the latest event with event_time ≤ t2) — NOT 30 (the future). This is the
	// label-leakage guard: future feature values are invisible to a past snapshot.
	events := []FeatureEvent{
		valueEvent(1, "u1", 10, at(1)),
		valueEvent(2, "u1", 20, at(2)),
		valueEvent(3, "u1", 30, at(3)),
	}

	got := ProjectAsOf(events, at(2), 1)

	if len(got) != 1 {
		t.Fatalf("expected 1 entity, got %d", len(got))
	}
	if v := got["u1"]; v.Values["x"].Double != 20 || v.AsOfVersion != 2 {
		t.Errorf("as-of t2: want value=20 version=2, got value=%v version=%d", v.Values["x"].Double, v.AsOfVersion)
	}
}

func TestProjectAsOf_EntityNotYetExisting_IsMissing(t *testing.T) {
	// u2's first event is at t3. A read as-of t1 must NOT include u2 (it did not
	// exist yet). Returning it would be temporal corruption.
	events := []FeatureEvent{
		valueEvent(1, "u1", 10, at(1)),
		valueEvent(3, "u2", 99, at(3)),
	}
	got := ProjectAsOf(events, at(1), 1)
	if _, ok := got["u2"]; ok {
		t.Error("u2 should be absent as-of t1 (it did not exist yet)")
	}
	if _, ok := got["u1"]; !ok {
		t.Error("u1 should be present as-of t1")
	}
}

func TestProjectAsOf_EventTimeTie_HigherVersionWins(t *testing.T) {
	// Two events for u1 at the SAME event_time (a correction backfilled at the same
	// instant). The tie-break must be deterministic: the higher VERSION (written
	// later → more recent fact) wins. v2's value (20) must beat v1's (10).
	events := []FeatureEvent{
		valueEvent(1, "u1", 10, at(5)),
		valueEvent(2, "u1", 20, at(5)),
	}
	got := ProjectAsOf(events, at(5), 1)
	if v := got["u1"]; v.Values["x"].Double != 20 || v.AsOfVersion != 2 {
		t.Errorf("event_time tie: want higher-version value=20 version=2, got value=%v version=%d", v.Values["x"].Double, v.AsOfVersion)
	}
}

func TestProjectAsOf_BoundaryIsInclusive(t *testing.T) {
	// as_of is INCLUSIVE (event_time ≤ asOf). An event exactly AT asOf must be
	// visible — an off-by-one here would silently drop the most recent valid value.
	events := []FeatureEvent{valueEvent(1, "u1", 42, at(2))}
	got := ProjectAsOf(events, at(2), 1)
	if v, ok := got["u1"]; !ok || v.Values["x"].Double != 42 {
		t.Errorf("event exactly at as_of must be included, got %#v ok=%v", got, ok)
	}
}

// TestProjectLatest_vs_AsOf_DifferOnBackfill is the interview-critical contrast:
// "latest" follows WRITE order (version); "as-of" follows EVENT TIME. A backfill
// with an OLD event_time but a NEW version is the latest online value, yet a
// point-in-time read before that event_time must not see it.
func TestProjectLatest_vs_AsOf_DifferOnBackfill(t *testing.T) {
	events := []FeatureEvent{
		valueEvent(1, "u1", 10, at(5)), // written first, event_time t5
		valueEvent(2, "u1", 99, at(1)), // backfill: written LATER (v2), event_time t1 (old)
	}

	// ONLINE: the backfill is the most recently WRITTEN fact ⇒ it is "latest".
	latest := ProjectLatest(events, 1)
	if latest["u1"].Values["x"].Double != 99 {
		t.Errorf("online latest should reflect the most recently written value (99), got %v", latest["u1"].Values["x"].Double)
	}

	// POINT-IN-TIME as-of t3: only the t1 backfill (99) is ≤ t3; the t5 event is the
	// future. So as-of t3 sees 99 too — but for a DIFFERENT reason (event_time), and
	// as-of t0 (before everything) sees NOTHING.
	asOfT3 := ProjectAsOf(events, at(3), 1)
	if asOfT3["u1"].Values["x"].Double != 99 {
		t.Errorf("as-of t3: only the t1 event is ≤ t3, want 99, got %v", asOfT3["u1"].Values["x"].Double)
	}
	asOfT0 := ProjectAsOf(events, at(0), 1)
	if _, ok := asOfT0["u1"]; ok {
		t.Error("as-of t0: no event has event_time ≤ t0, entity must be absent")
	}
}
