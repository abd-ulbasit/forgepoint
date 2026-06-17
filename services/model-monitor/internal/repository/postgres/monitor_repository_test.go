// monitor_repository_test.go — integration tests for the MonitorRepository adapter
// against a REAL Postgres. Every test verifies an actual storage behavior (the
// partial-unique key, soft delete, JSONB round-trip, team scoping, keyset
// pagination), not a mock interaction.
package postgres

import (
	"errors"
	"testing"
	"time"

	"github.com/abd-ulbasit/forgepoint/services/model-monitor/internal/domain"
)

// TestMonitor_Upsert_InsertThenUpdate verifies the created flag flips correctly and
// that DO UPDATE overwrites mutable fields while preserving created_at — the core
// upsert mechanic (xmax-derived created, EXCLUDED re-assert).
func TestMonitor_Upsert_InsertThenUpdate(t *testing.T) {
	s, ctx := newTestStore(t)
	repo := s.Monitors()

	m := newMonitor("team-a", "fraud")
	stored, created := mustSaveMonitor(t, ctx, repo, m)
	if !created {
		t.Fatal("first upsert: expected created=true")
	}
	if stored.ID != m.ID {
		t.Fatalf("stored id = %q, want %q", stored.ID, m.ID)
	}

	// Re-configure the SAME (team, model): change window + thresholds + auto-retrain.
	// The id stays the same (the upsert targets the existing live row via the
	// partial-unique key, not m.ID).
	m2 := m
	m2.ID = newID() // a NEW id the client proposes — must be IGNORED on the update path
	m2.WindowSize = 2000
	m2.AutoRetrain = true
	m2.RetrainPipelineID = "pipe-retrain-fraud"
	m2.Thresholds = []domain.ThresholdConfig{{
		DriftType: domain.DriftTypeData, Method: domain.DriftMethodKS,
		WarnScore: 0.2, CriticalScore: 0.5,
	}}
	m2.UpdatedAt = tnow().Add(time.Minute)

	updated, created2 := mustSaveMonitor(t, ctx, repo, m2)
	if created2 {
		t.Fatal("second upsert of same (team,model): expected created=false")
	}
	// The row keeps its ORIGINAL id (the conflict matched on (team,model), the
	// proposed new id is discarded — the existing PK wins).
	if updated.ID != m.ID {
		t.Fatalf("update kept id %q, want original %q", updated.ID, m.ID)
	}
	if updated.WindowSize != 2000 || !updated.AutoRetrain || updated.RetrainPipelineID != "pipe-retrain-fraud" {
		t.Fatalf("update did not overwrite mutable fields: %+v", updated)
	}
	if len(updated.Thresholds) != 1 || updated.Thresholds[0].Method != domain.DriftMethodKS {
		t.Fatalf("thresholds not overwritten: %+v", updated.Thresholds)
	}
	// created_at preserved from the original insert; updated_at advanced.
	if !updated.CreatedAt.Equal(m.CreatedAt) {
		t.Fatalf("created_at changed on update: got %v want %v", updated.CreatedAt, m.CreatedAt)
	}
	if !updated.UpdatedAt.After(m.UpdatedAt) {
		t.Fatalf("updated_at not advanced: got %v", updated.UpdatedAt)
	}
}

// TestMonitor_Upsert_JSONBRoundTrip pins the thresholds JSONB (de)serialization: a
// monitor with multiple per-type thresholds round-trips field-for-field, and a
// monitor with NO thresholds comes back with a nil slice (not an empty non-nil one).
func TestMonitor_Upsert_JSONBRoundTrip(t *testing.T) {
	s, ctx := newTestStore(t)
	repo := s.Monitors()

	m := newMonitor("team-a", "rich") // newMonitor seeds two thresholds
	mustSaveMonitor(t, ctx, repo, m)

	got, err := repo.GetByModel(ctx, "team-a", "rich")
	if err != nil {
		t.Fatalf("get by model: %v", err)
	}
	if len(got.Thresholds) != 2 {
		t.Fatalf("threshold count = %d, want 2", len(got.Thresholds))
	}
	// Compare the two thresholds exactly (order preserved by the JSON array).
	for i := range m.Thresholds {
		if got.Thresholds[i] != m.Thresholds[i] {
			t.Fatalf("threshold[%d] = %+v, want %+v", i, got.Thresholds[i], m.Thresholds[i])
		}
	}

	// Baseline timestamp round-trips through the nullable column at micro precision.
	if !got.BaselineCapturedAt.Equal(m.BaselineCapturedAt) {
		t.Fatalf("baseline_captured_at = %v, want %v", got.BaselineCapturedAt, m.BaselineCapturedAt)
	}

	// A monitor with NO thresholds and NO baseline ⇒ nil slice + zero time back.
	empty := newMonitor("team-a", "empty")
	empty.Thresholds = nil
	empty.BaselineCapturedAt = time.Time{}
	mustSaveMonitor(t, ctx, repo, empty)
	gotEmpty, err := repo.GetByModel(ctx, "team-a", "empty")
	if err != nil {
		t.Fatalf("get empty: %v", err)
	}
	if gotEmpty.Thresholds != nil {
		t.Fatalf("expected nil thresholds, got %+v", gotEmpty.Thresholds)
	}
	if !gotEmpty.BaselineCapturedAt.IsZero() {
		t.Fatalf("expected zero baseline time, got %v", gotEmpty.BaselineCapturedAt)
	}
}

// TestMonitor_GetByModel_NotFound asserts an absent (team, model) returns the storage
// sentinel ErrRepoNotFound (not a raw pgx.ErrNoRows leak).
func TestMonitor_GetByModel_NotFound(t *testing.T) {
	s, ctx := newTestStore(t)
	if _, err := s.Monitors().GetByModel(ctx, "team-a", "ghost"); !errors.Is(err, domain.ErrRepoNotFound) {
		t.Fatalf("GetByModel on absent = %v, want ErrRepoNotFound", err)
	}
}

// TestMonitor_GetByModel_TeamScoped is the cross-tenant lesson: two teams own a model
// with the SAME name; each GetByModel returns ONLY its own monitor.
func TestMonitor_GetByModel_TeamScoped(t *testing.T) {
	s, ctx := newTestStore(t)
	repo := s.Monitors()

	a, _ := mustSaveMonitor(t, ctx, repo, newMonitor("team-a", "fraud"))
	b, _ := mustSaveMonitor(t, ctx, repo, newMonitor("team-b", "fraud")) // same name, other team

	gotA, err := repo.GetByModel(ctx, "team-a", "fraud")
	if err != nil {
		t.Fatalf("get team-a: %v", err)
	}
	gotB, err := repo.GetByModel(ctx, "team-b", "fraud")
	if err != nil {
		t.Fatalf("get team-b: %v", err)
	}
	if gotA.ID != a.ID || gotB.ID != b.ID {
		t.Fatalf("team scoping crossed: a=%q(want %q) b=%q(want %q)", gotA.ID, a.ID, gotB.ID, b.ID)
	}
	if gotA.ID == gotB.ID {
		t.Fatal("two teams' same-named models resolved to the SAME monitor")
	}
}

// TestMonitor_GetByID resolves on the data plane and returns ErrRepoNotFound for an
// unknown id.
func TestMonitor_GetByID(t *testing.T) {
	s, ctx := newTestStore(t)
	repo := s.Monitors()
	m, _ := mustSaveMonitor(t, ctx, repo, newMonitor("team-a", "fraud"))

	got, err := repo.GetByID(ctx, m.ID)
	if err != nil {
		t.Fatalf("get by id: %v", err)
	}
	if got.ModelName != "fraud" || got.OwnerTeam != "team-a" {
		t.Fatalf("get by id returned wrong row: %+v", got)
	}
	if _, err := repo.GetByID(ctx, newID()); !errors.Is(err, domain.ErrRepoNotFound) {
		t.Fatalf("GetByID(unknown) = %v, want ErrRepoNotFound", err)
	}
}

// TestMonitor_Delete_IdempotentSoftDelete verifies: delete hides the row from reads;
// a SECOND delete (and a delete of a never-existed monitor) is a safe no-op returning
// nil; and the (team, model) name is FREED for a fresh re-configure after delete.
func TestMonitor_Delete_IdempotentAndReconfigurable(t *testing.T) {
	s, ctx := newTestStore(t)
	repo := s.Monitors()

	orig, _ := mustSaveMonitor(t, ctx, repo, newMonitor("team-a", "fraud"))

	if err := repo.Delete(ctx, "team-a", "fraud"); err != nil {
		t.Fatalf("first delete: %v", err)
	}
	// Soft-deleted ⇒ invisible to reads.
	if _, err := repo.GetByModel(ctx, "team-a", "fraud"); !errors.Is(err, domain.ErrRepoNotFound) {
		t.Fatalf("after delete GetByModel = %v, want ErrRepoNotFound", err)
	}
	// Second delete: idempotent no-op (nil), not an error.
	if err := repo.Delete(ctx, "team-a", "fraud"); err != nil {
		t.Fatalf("second delete should be a no-op, got %v", err)
	}
	// Delete of a never-existed monitor: also nil.
	if err := repo.Delete(ctx, "team-a", "nonexistent"); err != nil {
		t.Fatalf("delete of nonexistent should be nil, got %v", err)
	}

	// Re-configure the same (team, model): the partial-unique index excluded the
	// tombstone, so this is a fresh INSERT with a brand-new id (created=true).
	reconf := newMonitor("team-a", "fraud")
	stored, created := mustSaveMonitor(t, ctx, repo, reconf)
	if !created {
		t.Fatal("re-configure after delete: expected created=true (fresh insert)")
	}
	if stored.ID == orig.ID {
		t.Fatal("re-configure reused the tombstoned monitor's id; expected a new row")
	}
	// Exactly one LIVE monitor for (team-a, fraud) now.
	got, err := repo.GetByModel(ctx, "team-a", "fraud")
	if err != nil {
		t.Fatalf("get after reconfigure: %v", err)
	}
	if got.ID != stored.ID {
		t.Fatalf("live monitor id = %q, want the reconfigured %q", got.ID, stored.ID)
	}
}

// TestMonitor_List_PaginationAndTenancy seeds two teams' monitors and verifies: the
// list is team-scoped (team-a never sees team-b), newest-first, and pages exactly via
// the cursor with no skips/dupes.
func TestMonitor_List_PaginationAndTenancy(t *testing.T) {
	s, ctx := newTestStore(t)
	repo := s.Monitors()

	base := tnow().Add(-time.Hour)
	// 5 monitors for team-a with increasing created_at (so newest-first order is
	// deterministic), 2 decoys for team-b.
	wantIDs := make([]string, 0, 5)
	for i := 0; i < 5; i++ {
		m := newMonitor("team-a", "model-"+string(rune('a'+i)))
		m.CreatedAt = base.Add(time.Duration(i) * time.Minute)
		m.UpdatedAt = m.CreatedAt
		stored, _ := mustSaveMonitor(t, ctx, repo, m)
		wantIDs = append(wantIDs, stored.ID)
	}
	// Reverse: newest-first means the last-created is first.
	for i, j := 0, len(wantIDs)-1; i < j; i, j = i+1, j-1 {
		wantIDs[i], wantIDs[j] = wantIDs[j], wantIDs[i]
	}
	mustSaveMonitor(t, ctx, repo, newMonitor("team-b", "decoy-1"))
	mustSaveMonitor(t, ctx, repo, newMonitor("team-b", "decoy-2"))

	// Page through team-a's fleet 2 at a time.
	var got []string
	token := ""
	for {
		page, next, err := repo.List(ctx, domain.FleetFilter{OwnerTeam: "team-a"}, domain.ListOptions{PageSize: 2, PageToken: token})
		if err != nil {
			t.Fatalf("list: %v", err)
		}
		for _, m := range page {
			if m.OwnerTeam != "team-a" {
				t.Fatalf("cross-tenant leak: got team %q in team-a list", m.OwnerTeam)
			}
			got = append(got, m.ID)
		}
		if next == "" {
			break
		}
		token = next
	}
	if len(got) != 5 {
		t.Fatalf("paged %d monitors, want 5 (team-b decoys must be excluded)", len(got))
	}
	for i := range wantIDs {
		if got[i] != wantIDs[i] {
			t.Fatalf("page order mismatch at %d: got %q want %q (full got=%v want=%v)", i, got[i], wantIDs[i], got, wantIDs)
		}
	}
}

// TestMonitor_List_Filters verifies the state filter and the MinSeverity correlated-
// subquery filter (only monitors whose latest report is >= the severity).
func TestMonitor_List_Filters(t *testing.T) {
	s, ctx := newTestStore(t)
	mons := s.Monitors()
	reps := s.Reports()

	// Two ACTIVE monitors and one PAUSED, all team-a.
	active1, _ := mustSaveMonitor(t, ctx, mons, withState(newMonitor("team-a", "m-active-1"), domain.MonitorStateActive))
	_, _ = mustSaveMonitor(t, ctx, mons, withState(newMonitor("team-a", "m-active-2"), domain.MonitorStateActive))
	paused, _ := mustSaveMonitor(t, ctx, mons, withState(newMonitor("team-a", "m-paused"), domain.MonitorStatePaused))

	// State filter: PAUSED only ⇒ exactly the paused one.
	page, _, err := mons.List(ctx, domain.FleetFilter{OwnerTeam: "team-a", State: domain.MonitorStatePaused}, domain.ListOptions{PageSize: 50})
	if err != nil {
		t.Fatalf("list paused: %v", err)
	}
	if len(page) != 1 || page[0].ID != paused.ID {
		t.Fatalf("state filter returned %d rows; want just the paused monitor", len(page))
	}

	// Give active1 a CRITICAL latest report; active2 gets only an OK report.
	critReport := newReport(active1) // newReport is CRITICAL
	mustSaveReport(t, ctx, reps, critReport)

	// MinSeverity=CRITICAL over ACTIVE monitors ⇒ only active1 (it has a CRITICAL
	// latest report); active2 has none, paused excluded by state.
	page, _, err = mons.List(ctx, domain.FleetFilter{
		OwnerTeam:   "team-a",
		State:       domain.MonitorStateActive,
		MinSeverity: domain.DriftSeverityCritical,
	}, domain.ListOptions{PageSize: 50})
	if err != nil {
		t.Fatalf("list crit: %v", err)
	}
	if len(page) != 1 || page[0].ID != active1.ID {
		t.Fatalf("MinSeverity filter returned %d rows; want just the monitor with a CRITICAL latest report", len(page))
	}
}

func withState(m domain.Monitor, st domain.MonitorState) domain.Monitor {
	m.State = st
	return m
}
