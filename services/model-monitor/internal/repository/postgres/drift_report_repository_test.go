// drift_report_repository_test.go — integration tests for the DriftReportRepository
// adapter against a REAL Postgres. These verify the two defining mechanics — the
// window_id idempotency conflict (exactly-once persistence) and the (owner_team,
// model_name) tenancy on every read/purge — plus pagination, filters, and the FK.
package postgres

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/abd-ulbasit/forgepoint/services/model-monitor/internal/domain"
)

// seedMonitor persists a valid monitor (reports FK-reference it) and returns it.
func seedMonitor(t *testing.T, ctx context.Context, s *Store, team, model string) domain.Monitor {
	t.Helper()
	m, _ := mustSaveMonitor(t, ctx, s.Monitors(), newMonitor(team, model))
	return m
}

// TestReport_Save_IdempotentOnWindowID is the exactly-once-in-effect test: saving two
// reports with the SAME window_id yields ONE row; the second Save returns inserted=
// false and the ORIGINAL row (not the second payload), so a redelivered/double-scored
// window never duplicates and the caller knows not to re-emit the event.
func TestReport_Save_IdempotentOnWindowID(t *testing.T) {
	s, ctx := newTestStore(t)
	reps := s.Reports()
	m := seedMonitor(t, ctx, s, "team-a", "fraud")

	first := newReport(m)
	stored1, inserted1 := mustSaveReport(t, ctx, reps, first)
	if !inserted1 {
		t.Fatal("first save: expected inserted=true")
	}

	// A redelivery: SAME window_id, but a DIFFERENT id/severity/metrics payload (as a
	// double-score might produce). The adapter must return the EXISTING row unchanged.
	second := newReport(m)
	second.WindowID = first.WindowID // collide on the idempotency key
	second.Severity = domain.DriftSeverityWarning
	second.Metrics = nil
	stored2, inserted2 := mustSaveReport(t, ctx, reps, second)
	if inserted2 {
		t.Fatal("second save (same window_id): expected inserted=false")
	}
	if stored2.ID != stored1.ID {
		t.Fatalf("conflict returned id %q, want the original %q", stored2.ID, stored1.ID)
	}
	if stored2.Severity != domain.DriftSeverityCritical {
		t.Fatalf("conflict overwrote the row: severity=%v, want the original CRITICAL", stored2.Severity)
	}
	if len(stored2.Metrics) != 2 {
		t.Fatalf("conflict lost the original metrics: got %d, want 2", len(stored2.Metrics))
	}

	// Exactly one row exists for that window_id.
	var count int
	if err := s.Pool().QueryRow(ctx, `SELECT count(*) FROM drift_reports WHERE window_id = $1`, first.WindowID).Scan(&count); err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 1 {
		t.Fatalf("rows for window_id = %d, want exactly 1", count)
	}
}

// TestReport_Save_JSONBRoundTrip pins the metrics JSONB (de)serialization, then reads
// the report back via GetByID to prove the stored bytes reconstruct field-for-field.
func TestReport_Save_JSONBRoundTrip(t *testing.T) {
	s, ctx := newTestStore(t)
	reps := s.Reports()
	m := seedMonitor(t, ctx, s, "team-a", "fraud")

	rep := newReport(m)
	mustSaveReport(t, ctx, reps, rep)

	got, err := reps.GetByID(ctx, "team-a", rep.ID)
	if err != nil {
		t.Fatalf("get by id: %v", err)
	}
	if len(got.Metrics) != 2 {
		t.Fatalf("metric count = %d, want 2", len(got.Metrics))
	}
	for i := range rep.Metrics {
		if got.Metrics[i] != rep.Metrics[i] {
			t.Fatalf("metric[%d] = %+v, want %+v", i, got.Metrics[i], rep.Metrics[i])
		}
	}
	if got.DriftType != rep.DriftType || got.Severity != rep.Severity || got.SampleCount != rep.SampleCount {
		t.Fatalf("scalar fields mismatch: %+v vs %+v", got, rep)
	}
	if !got.WindowStart.Equal(rep.WindowStart) || !got.WindowEnd.Equal(rep.WindowEnd) {
		t.Fatalf("window times mismatch: got [%v,%v] want [%v,%v]", got.WindowStart, got.WindowEnd, rep.WindowStart, rep.WindowEnd)
	}
}

// TestReport_Save_FKToMissingMonitor maps a foreign_key_violation (report under a
// monitor_id that doesn't exist) to ErrRepoNotFound rather than leaking a raw DB error.
func TestReport_Save_FKToMissingMonitor(t *testing.T) {
	s, ctx := newTestStore(t)
	reps := s.Reports()

	orphan := newReport(domain.Monitor{ID: newID(), OwnerTeam: "team-a", ModelName: "ghost"})
	_, _, err := reps.Save(ctx, orphan)
	if !errors.Is(err, domain.ErrRepoNotFound) {
		t.Fatalf("save with dangling monitor_id = %v, want ErrRepoNotFound", err)
	}
}

// TestReport_GetByID_TenancyIDOR is the anti-IDOR test: team-b cannot read team-a's
// report even with the exact id; the mismatch is indistinguishable from "no such id".
func TestReport_GetByID_TenancyIDOR(t *testing.T) {
	s, ctx := newTestStore(t)
	reps := s.Reports()
	m := seedMonitor(t, ctx, s, "team-a", "fraud")

	rep := newReport(m)
	mustSaveReport(t, ctx, reps, rep)

	// Owner reads fine.
	if _, err := reps.GetByID(ctx, "team-a", rep.ID); err != nil {
		t.Fatalf("owner GetByID: %v", err)
	}
	// Another tenant with the EXACT id ⇒ ErrRepoNotFound (no enumeration oracle).
	if _, err := reps.GetByID(ctx, "team-b", rep.ID); !errors.Is(err, domain.ErrRepoNotFound) {
		t.Fatalf("cross-tenant GetByID = %v, want ErrRepoNotFound", err)
	}
}

// TestReport_LatestByModel returns the newest-by-window_end report and is team-scoped.
func TestReport_LatestByModel(t *testing.T) {
	s, ctx := newTestStore(t)
	reps := s.Reports()
	m := seedMonitor(t, ctx, s, "team-a", "fraud")

	// No reports yet ⇒ ErrRepoNotFound.
	if _, err := reps.LatestByModel(ctx, "team-a", "fraud"); !errors.Is(err, domain.ErrRepoNotFound) {
		t.Fatalf("LatestByModel with no reports = %v, want ErrRepoNotFound", err)
	}

	base := tnow().Add(-time.Hour)
	var newestID string
	for i := 0; i < 4; i++ {
		rep := newReport(m)
		rep.WindowEnd = base.Add(time.Duration(i) * time.Minute)
		rep.WindowStart = rep.WindowEnd.Add(-time.Minute)
		stored, _ := mustSaveReport(t, ctx, reps, rep)
		newestID = stored.ID // last iteration has the greatest window_end
	}
	got, err := reps.LatestByModel(ctx, "team-a", "fraud")
	if err != nil {
		t.Fatalf("latest: %v", err)
	}
	if got.ID != newestID {
		t.Fatalf("latest id = %q, want %q (greatest window_end)", got.ID, newestID)
	}

	// A same-named model in another team must NOT bleed into team-a's latest.
	mB := seedMonitor(t, ctx, s, "team-b", "fraud")
	repB := newReport(mB)
	repB.WindowEnd = base.Add(time.Hour) // strictly newer than any team-a report
	repB.WindowStart = repB.WindowEnd.Add(-time.Minute)
	mustSaveReport(t, ctx, reps, repB)

	gotA, err := reps.LatestByModel(ctx, "team-a", "fraud")
	if err != nil {
		t.Fatalf("latest team-a after team-b insert: %v", err)
	}
	if gotA.ID != newestID {
		t.Fatalf("team-b's newer report bled into team-a's latest: got %q", gotA.ID)
	}
}

// TestReport_List_PaginationAndTenancy pages a (team, model)'s reports newest-first
// with exact cursor boundaries and verifies another team's same-named reports are
// excluded.
func TestReport_List_PaginationAndTenancy(t *testing.T) {
	s, ctx := newTestStore(t)
	reps := s.Reports()
	m := seedMonitor(t, ctx, s, "team-a", "fraud")
	mB := seedMonitor(t, ctx, s, "team-b", "fraud")

	base := tnow().Add(-time.Hour)
	wantIDs := make([]string, 0, 6)
	for i := 0; i < 6; i++ {
		rep := newReport(m)
		rep.WindowEnd = base.Add(time.Duration(i) * time.Minute)
		rep.WindowStart = rep.WindowEnd.Add(-time.Minute)
		stored, _ := mustSaveReport(t, ctx, reps, rep)
		wantIDs = append(wantIDs, stored.ID)
	}
	// newest-first.
	for i, j := 0, len(wantIDs)-1; i < j; i, j = i+1, j-1 {
		wantIDs[i], wantIDs[j] = wantIDs[j], wantIDs[i]
	}
	// team-b decoys (same model name) — must never appear in team-a's history.
	for i := 0; i < 3; i++ {
		mustSaveReport(t, ctx, reps, newReport(mB))
	}

	var got []string
	token := ""
	for {
		page, next, err := reps.List(ctx, domain.ReportFilter{OwnerTeam: "team-a", ModelName: "fraud"}, domain.ListOptions{PageSize: 4, PageToken: token})
		if err != nil {
			t.Fatalf("list: %v", err)
		}
		for _, r := range page {
			if r.OwnerTeam != "team-a" {
				t.Fatalf("cross-tenant leak in report list: team %q", r.OwnerTeam)
			}
			got = append(got, r.ID)
		}
		if next == "" {
			break
		}
		token = next
	}
	if len(got) != 6 {
		t.Fatalf("paged %d reports, want 6 (team-b excluded)", len(got))
	}
	for i := range wantIDs {
		if got[i] != wantIDs[i] {
			t.Fatalf("order mismatch at %d: got %q want %q", i, got[i], wantIDs[i])
		}
	}
}

// TestReport_List_SeverityAndTimeFilters verifies MinSeverity (>=) and the Since/Until
// window_end bounds, all as bound parameters.
func TestReport_List_SeverityAndTimeFilters(t *testing.T) {
	s, ctx := newTestStore(t)
	reps := s.Reports()
	m := seedMonitor(t, ctx, s, "team-a", "fraud")

	base := tnow().Add(-time.Hour)
	// Mix severities across time: OK, WARNING, CRITICAL at t0, t1, t2.
	sev := []domain.DriftSeverity{domain.DriftSeverityOK, domain.DriftSeverityWarning, domain.DriftSeverityCritical}
	ends := make([]time.Time, 3)
	for i, sv := range sev {
		rep := newReport(m)
		rep.Severity = sv
		rep.WindowEnd = base.Add(time.Duration(i) * time.Minute)
		rep.WindowStart = rep.WindowEnd.Add(-time.Minute)
		mustSaveReport(t, ctx, reps, rep)
		ends[i] = rep.WindowEnd
	}

	// MinSeverity=WARNING ⇒ the WARNING + CRITICAL rows (2).
	page, _, err := reps.List(ctx, domain.ReportFilter{
		OwnerTeam: "team-a", ModelName: "fraud", MinSeverity: domain.DriftSeverityWarning,
	}, domain.ListOptions{PageSize: 50})
	if err != nil {
		t.Fatalf("list min-severity: %v", err)
	}
	if len(page) != 2 {
		t.Fatalf("MinSeverity=WARNING returned %d, want 2", len(page))
	}
	for _, r := range page {
		if r.Severity < domain.DriftSeverityWarning {
			t.Fatalf("severity filter leaked an OK row: %+v", r)
		}
	}

	// Time window [t1, t2] ⇒ the WARNING + CRITICAL rows (window_end in range).
	page, _, err = reps.List(ctx, domain.ReportFilter{
		OwnerTeam: "team-a", ModelName: "fraud",
		Since: ends[1], Until: ends[2],
	}, domain.ListOptions{PageSize: 50})
	if err != nil {
		t.Fatalf("list time window: %v", err)
	}
	if len(page) != 2 {
		t.Fatalf("time window [t1,t2] returned %d, want 2", len(page))
	}
}

// TestReport_PurgeByModel hard-deletes a team's reports for a model, returns the exact
// count, and leaves another team's same-named history untouched (cross-tenant safety).
func TestReport_PurgeByModel(t *testing.T) {
	s, ctx := newTestStore(t)
	reps := s.Reports()
	mA := seedMonitor(t, ctx, s, "team-a", "fraud")
	mB := seedMonitor(t, ctx, s, "team-b", "fraud")

	for i := 0; i < 3; i++ {
		mustSaveReport(t, ctx, reps, newReport(mA))
	}
	for i := 0; i < 2; i++ {
		mustSaveReport(t, ctx, reps, newReport(mB))
	}

	purged, err := reps.PurgeByModel(ctx, "team-a", "fraud")
	if err != nil {
		t.Fatalf("purge: %v", err)
	}
	if purged != 3 {
		t.Fatalf("purged %d, want 3", purged)
	}
	// team-a's history is gone.
	if _, err := reps.LatestByModel(ctx, "team-a", "fraud"); !errors.Is(err, domain.ErrRepoNotFound) {
		t.Fatalf("after purge team-a has reports: %v", err)
	}
	// team-b's same-named history survives.
	var countB int
	if err := s.Pool().QueryRow(ctx, `SELECT count(*) FROM drift_reports WHERE owner_team = 'team-b'`).Scan(&countB); err != nil {
		t.Fatalf("count team-b: %v", err)
	}
	if countB != 2 {
		t.Fatalf("team-b report count = %d after team-a purge, want 2 (untouched)", countB)
	}

	// Purge of a model with no reports ⇒ 0, nil.
	purged, err = reps.PurgeByModel(ctx, "team-a", "nonexistent")
	if err != nil {
		t.Fatalf("purge nonexistent: %v", err)
	}
	if purged != 0 {
		t.Fatalf("purge nonexistent purged %d, want 0", purged)
	}
}
