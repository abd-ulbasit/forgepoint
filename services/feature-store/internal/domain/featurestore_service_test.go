// featurestore_service_test.go — TDD specification for the event-sourced
// FeatureStoreService implementation.
//
// ============================================================================
// EXTERNAL TEST PACKAGE (domain_test) — and an IN-MEMORY EVENT LOG, not a stub
// ============================================================================
//
// These tests live in package domain_test (the external test package) so they
// exercise ONLY the exported surface real callers (the handler) depend on. The
// mock ports here are not assertion puppets — the EventLog mock is a real, tiny,
// in-memory append-only log with monotonic versions and idempotency dedup, and
// the view stores are real in-memory maps. That lets the tests assert REAL
// event-sourcing behavior end to end:
//   - DefineFeatureView/WriteFeatures actually APPEND immutable events,
//   - GetOnline/GetHistorical actually PROJECT (fold) those events,
//   - the SAME schema_version/owner/version provenance flows through,
//   - idempotency actually dedups a replayed command,
//   - RebuildViews actually replays the log to regenerate the views.
//
// Written BEFORE featurestore_service_impl.go exists (TDD): they describe the
// contract; the impl makes them pass.
package domain_test

import (
	"context"
	"errors"
	"sort"
	"testing"
	"time"

	"github.com/abd-ulbasit/forgepoint/services/feature-store/internal/domain"
)

// ----------------------------------------------------------------------------
// Deterministic Clock + IDGenerator (the injected impure edges)
// ----------------------------------------------------------------------------

// fixedClock returns a controllable time so tests can assert exact
// server-assigned timestamps (created_at, appended_at) instead of "something
// near now".
type fixedClock struct{ t time.Time }

func (c *fixedClock) Now() time.Time { return c.t }

// seqIDGen mints predictable ids ("id-1", "id-2", …) so tests can assert the
// exact server-assigned view/event ids. Production wires uuid.NewString.
type seqIDGen struct{ n int }

func (g *seqIDGen) NewID() string {
	g.n++
	return "id-" + itoa(g.n)
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b []byte
	for n > 0 {
		b = append([]byte{byte('0' + n%10)}, b...)
		n /= 10
	}
	return string(b)
}

// at builds a readable event_time: at(1) = a fixed base + 1h. Defined here too
// (the external test package can't see fold_test.go's in-package helper).
func at(hours int) time.Time {
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	return base.Add(time.Duration(hours) * time.Hour)
}

// ----------------------------------------------------------------------------
// memEventLog — a real in-memory append-only log (the WRITE MODEL).
//
// It enforces the two contracts the service relies on: monotonic gap-free
// versions assigned at append, and idempotency dedup by key. It NEVER mutates or
// deletes a stored event (append-only), so the tests genuinely test event sourcing.
// ----------------------------------------------------------------------------

type memEventLog struct {
	events    []domain.FeatureEvent // the immutable log, in append order
	nextVer   int64
	byIdemKey map[string]idemRecord // idempotency dedup
	nameToID  map[string]string     // "team\x00name" → viewID (team-scoped resolve)
}

type idemRecord struct {
	appended       []domain.FeatureEvent
	writtenThrough int64
}

func newMemEventLog() *memEventLog {
	return &memEventLog{
		byIdemKey: make(map[string]idemRecord),
		nameToID:  make(map[string]string),
	}
}

func (l *memEventLog) Append(_ context.Context, idemKey string, events []domain.FeatureEvent) ([]domain.FeatureEvent, int64, bool, error) {
	// Idempotency: a repeat with the same key returns the ORIGINAL result (the
	// exactly-once-EFFECT guarantee under at-least-once delivery).
	if idemKey != "" {
		if rec, ok := l.byIdemKey[idemKey]; ok {
			return rec.appended, rec.writtenThrough, true, nil
		}
	}
	appended := make([]domain.FeatureEvent, len(events))
	for i, e := range events {
		l.nextVer++
		e.Version = l.nextVer
		if e.ID == "" {
			e.ID = "ev-" + itoa(int(l.nextVer))
		}
		l.events = append(l.events, e)
		appended[i] = e
		// Maintain the team-scoped name index on definition events.
		if e.Type == domain.FeatureEventViewDefined && e.ViewDef != nil {
			l.nameToID[e.ViewDef.OwnerTeam+"\x00"+e.ViewDef.Name] = e.ViewDef.ID
		}
	}
	through := l.nextVer
	if idemKey != "" {
		l.byIdemKey[idemKey] = idemRecord{appended: appended, writtenThrough: through}
	}
	return appended, through, false, nil
}

func (l *memEventLog) Load(_ context.Context, viewID string) ([]domain.FeatureEvent, error) {
	var out []domain.FeatureEvent
	for _, e := range l.events {
		if e.FeatureViewID == viewID {
			out = append(out, e)
		}
	}
	if len(out) == 0 {
		return nil, domain.ErrEventLogNotFound
	}
	return out, nil
}

func (l *memEventLog) LoadView(_ context.Context, viewID string) ([]domain.FeatureEvent, error) {
	var out []domain.FeatureEvent
	for _, e := range l.events {
		if e.FeatureViewID != viewID {
			continue
		}
		if e.Type == domain.FeatureEventViewDefined || e.Type == domain.FeatureEventViewDeleted {
			out = append(out, e)
		}
	}
	if len(out) == 0 {
		return nil, domain.ErrEventLogNotFound
	}
	return out, nil
}

func (l *memEventLog) ResolveViewIDByName(_ context.Context, team, name string) (string, error) {
	id, ok := l.nameToID[team+"\x00"+name]
	if !ok {
		return "", domain.ErrEventLogNotFound
	}
	return id, nil
}

func (l *memEventLog) ListViewIDs(_ context.Context, team, _ string, _ domain.ListOptions) ([]string, string, error) {
	var ids []string
	seen := map[string]bool{}
	for _, e := range l.events {
		if e.Type == domain.FeatureEventViewDefined && e.ViewDef != nil && e.ViewDef.OwnerTeam == team && !seen[e.ViewDef.ID] {
			ids = append(ids, e.ViewDef.ID)
			seen[e.ViewDef.ID] = true
		}
	}
	sort.Strings(ids)
	return ids, "", nil
}

// ----------------------------------------------------------------------------
// memOnlineStore / memOfflineStore — in-memory read models.
// ----------------------------------------------------------------------------

type memOnlineStore struct {
	data   map[string]map[string]domain.FeatureVector // viewID → entityID → vector
	purged map[string]bool
}

func newMemOnlineStore() *memOnlineStore {
	return &memOnlineStore{data: map[string]map[string]domain.FeatureVector{}, purged: map[string]bool{}}
}

func (s *memOnlineStore) GetLatest(_ context.Context, viewID string, ids []string) (map[string]domain.FeatureVector, error) {
	out := map[string]domain.FeatureVector{}
	for _, id := range ids {
		if v, ok := s.data[viewID][id]; ok {
			out[id] = v
		}
	}
	return out, nil
}

func (s *memOnlineStore) Put(_ context.Context, viewID string, vectors map[string]domain.FeatureVector) error {
	if s.data[viewID] == nil {
		s.data[viewID] = map[string]domain.FeatureVector{}
	}
	for id, v := range vectors {
		s.data[viewID][id] = v
	}
	return nil
}

func (s *memOnlineStore) Purge(_ context.Context, viewID string) error {
	delete(s.data, viewID)
	s.purged[viewID] = true
	return nil
}

type memOfflineStore struct {
	rebuilt map[string]map[string]domain.FeatureVector
}

func newMemOfflineStore() *memOfflineStore {
	return &memOfflineStore{rebuilt: map[string]map[string]domain.FeatureVector{}}
}

// GetAsOf delegates to the domain fold over whatever the service hands it; but the
// service's GetHistoricalFeatures loads from the log and folds itself, so this is
// used only if the impl routes through the offline port. We keep a simple
// implementation that the impl can use: it returns the stored rebuilt projection
// filtered to the requested entities. For the as-of read the impl is expected to
// fold the log directly (point-in-time), so this returns empty by default.
func (s *memOfflineStore) GetAsOf(_ context.Context, viewID string, ids []string, _ time.Time, _ domain.ListOptions) (map[string]domain.FeatureVector, string, error) {
	out := map[string]domain.FeatureVector{}
	for _, id := range ids {
		if v, ok := s.rebuilt[viewID][id]; ok {
			out[id] = v
		}
	}
	return out, "", nil
}

func (s *memOfflineStore) Rebuild(_ context.Context, viewID string, vectors map[string]domain.FeatureVector) error {
	s.rebuilt[viewID] = vectors
	return nil
}

// Compile-time proof the mocks satisfy the ports (catches interface skew at build).
var (
	_ domain.EventLog         = (*memEventLog)(nil)
	_ domain.OnlineViewStore  = (*memOnlineStore)(nil)
	_ domain.OfflineViewStore = (*memOfflineStore)(nil)
	_ domain.Clock            = (*fixedClock)(nil)
	_ domain.IDGenerator      = (*seqIDGen)(nil)
)

// ----------------------------------------------------------------------------
// Test harness
// ----------------------------------------------------------------------------

type harness struct {
	svc     domain.FeatureStoreService
	log     *memEventLog
	online  *memOnlineStore
	offline *memOfflineStore
	clock   *fixedClock
}

func newHarness() *harness {
	log := newMemEventLog()
	online := newMemOnlineStore()
	offline := newMemOfflineStore()
	clock := &fixedClock{t: time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)}
	svc := domain.NewFeatureStoreService(log, online, offline, clock, &seqIDGen{})
	return &harness{svc: svc, log: log, online: online, offline: offline, clock: clock}
}

var alice = domain.Principal{UserID: "alice", Team: "platform", Scopes: []string{"features:admin"}}

func sampleDefineInput() domain.DefineFeatureViewInput {
	return domain.DefineFeatureViewInput{
		Name:        "user_credit",
		Description: "credit features",
		Entity:      domain.Entity{Name: "user", JoinKey: "user_id"},
		Features: []domain.FeatureSpec{
			{Name: "score", ValueType: domain.FeatureTypeDouble},
			{Name: "tier", ValueType: domain.FeatureTypeString},
		},
		IdempotencyKey: "def-key-1",
	}
}

func mustDefine(t *testing.T, h *harness) domain.FeatureView {
	t.Helper()
	v, err := h.svc.DefineFeatureView(context.Background(), alice, sampleDefineInput())
	if err != nil {
		t.Fatalf("DefineFeatureView: %v", err)
	}
	return v
}

// ============================================================================
// DefineFeatureView — append a definition event; SERVER assigns authority
// ============================================================================

func TestDefineFeatureView_AppendsEventAndAssignsServerFields(t *testing.T) {
	h := newHarness()
	v, err := h.svc.DefineFeatureView(context.Background(), alice, sampleDefineInput())
	if err != nil {
		t.Fatalf("DefineFeatureView: %v", err)
	}

	// SERVER-AUTHORITATIVE fields must come from the Principal/clock/idgen — never
	// the request (which carries none of them). This is the mass-assignment guard.
	if v.ID == "" {
		t.Error("view id should be server-assigned, got empty")
	}
	if v.OwnerUserID != "alice" || v.OwnerTeam != "platform" {
		t.Errorf("owner must come from Principal, got user=%q team=%q", v.OwnerUserID, v.OwnerTeam)
	}
	if v.SchemaVersion != 1 {
		t.Errorf("first definition should be schema_version 1, got %d", v.SchemaVersion)
	}
	if !v.CreatedAt.Equal(h.clock.Now()) {
		t.Errorf("created_at should be the server clock, got %v want %v", v.CreatedAt, h.clock.Now())
	}

	// And the write must have APPENDED an immutable definition event to the log.
	events, err := h.log.LoadView(context.Background(), v.ID)
	if err != nil {
		t.Fatalf("LoadView: %v", err)
	}
	if len(events) != 1 || events[0].Type != domain.FeatureEventViewDefined {
		t.Fatalf("expected exactly one FeatureViewDefined event, got %#v", events)
	}
}

func TestDefineFeatureView_RejectsEmptyNameAndEmptySchema(t *testing.T) {
	h := newHarness()
	bad := sampleDefineInput()
	bad.Name = ""
	if _, err := h.svc.DefineFeatureView(context.Background(), alice, bad); !errors.Is(err, domain.ErrValidation) {
		t.Errorf("empty name should be ErrValidation, got %v", err)
	}

	bad2 := sampleDefineInput()
	bad2.Features = nil
	if _, err := h.svc.DefineFeatureView(context.Background(), alice, bad2); !errors.Is(err, domain.ErrValidation) {
		t.Errorf("empty schema should be ErrValidation, got %v", err)
	}

	bad3 := sampleDefineInput()
	bad3.Features = []domain.FeatureSpec{{Name: "x", ValueType: domain.FeatureTypeUnspecified}}
	if _, err := h.svc.DefineFeatureView(context.Background(), alice, bad3); !errors.Is(err, domain.ErrValidation) {
		t.Errorf("UNSPECIFIED feature type should be ErrValidation, got %v", err)
	}
}

func TestDefineFeatureView_EvolveBumpsSchemaVersion(t *testing.T) {
	h := newHarness()
	v1 := mustDefine(t, h)

	// Re-define the SAME name with an added feature → schema evolution (append a
	// new definition event, bump schema_version). NOT an in-place update.
	evolve := sampleDefineInput()
	evolve.IdempotencyKey = "def-key-2" // a NEW logical attempt
	evolve.Features = append(evolve.Features, domain.FeatureSpec{Name: "limit", ValueType: domain.FeatureTypeInt64})

	v2, err := h.svc.DefineFeatureView(context.Background(), alice, evolve)
	if err != nil {
		t.Fatalf("evolve: %v", err)
	}
	if v2.ID != v1.ID {
		t.Errorf("evolving should keep the same view id; got %q then %q", v1.ID, v2.ID)
	}
	if v2.SchemaVersion != 2 {
		t.Errorf("evolution should bump schema_version to 2, got %d", v2.SchemaVersion)
	}
	if len(v2.Features) != 3 {
		t.Errorf("evolved view should have 3 features, got %d", len(v2.Features))
	}
}

func TestDefineFeatureView_EntityConflictRejected(t *testing.T) {
	h := newHarness()
	mustDefine(t, h)

	conflict := sampleDefineInput()
	conflict.IdempotencyKey = "def-key-3"
	conflict.Entity = domain.Entity{Name: "merchant", JoinKey: "merchant_id"} // changed entity!
	if _, err := h.svc.DefineFeatureView(context.Background(), alice, conflict); !errors.Is(err, domain.ErrViewNameConflict) {
		t.Errorf("changing a view's entity should be ErrViewNameConflict, got %v", err)
	}
}

func TestDefineFeatureView_Idempotent(t *testing.T) {
	h := newHarness()
	v1 := mustDefine(t, h)
	// SAME idempotency key (a retry of the same logical attempt) → no new event,
	// same view returned, schema_version unchanged.
	v2, err := h.svc.DefineFeatureView(context.Background(), alice, sampleDefineInput())
	if err != nil {
		t.Fatalf("retry define: %v", err)
	}
	if v2.SchemaVersion != v1.SchemaVersion || v2.ID != v1.ID {
		t.Errorf("idempotent retry should return the original view, got v1=%+v v2=%+v", v1, v2)
	}
	events, _ := h.log.LoadView(context.Background(), v1.ID)
	if len(events) != 1 {
		t.Errorf("idempotent retry must NOT append a second definition event, got %d", len(events))
	}
}

// ============================================================================
// WriteFeatures — append value events; schema validation; online projection
// ============================================================================

func TestWriteFeatures_AppendsAndProjectsOnline(t *testing.T) {
	h := newHarness()
	v := mustDefine(t, h)

	in := domain.WriteFeaturesInput{
		FeatureViewID:  v.ID,
		IdempotencyKey: "w-1",
		Rows: []domain.FeatureRow{
			{EntityID: "u1", Values: map[string]domain.FeatureValue{
				"score": {Kind: domain.FeatureTypeDouble, Double: 0.9},
				"tier":  {Kind: domain.FeatureTypeString, Str: "gold"},
			}, EventTime: at(1)},
		},
	}
	res, err := h.svc.WriteFeatures(context.Background(), alice, in)
	if err != nil {
		t.Fatalf("WriteFeatures: %v", err)
	}
	if res.WrittenCount != 1 {
		t.Errorf("written_count = %d, want 1", res.WrittenCount)
	}
	if res.WrittenThroughVersion == 0 {
		t.Error("written_through_version should be the assigned log version, got 0")
	}

	// The online projection must now serve the latest value (append → project).
	page, err := h.svc.GetOnlineFeatures(context.Background(), alice, domain.GetOnlineFeaturesInput{
		FeatureViewID: v.ID, EntityIDs: []string{"u1"},
	})
	if err != nil {
		t.Fatalf("GetOnlineFeatures: %v", err)
	}
	if len(page.Vectors) != 1 || page.Vectors[0].Values["score"].Double != 0.9 {
		t.Fatalf("online read should reflect the write, got %#v", page.Vectors)
	}
	// Provenance: the served vector carries the schema_version it was validated against.
	if page.Vectors[0].SchemaVersion != v.SchemaVersion {
		t.Errorf("served vector schema_version = %d, want %d", page.Vectors[0].SchemaVersion, v.SchemaVersion)
	}
}

func TestWriteFeatures_SchemaViolation_FailsWholeBatch(t *testing.T) {
	h := newHarness()
	v := mustDefine(t, h)

	// Row 1 is valid; row 2 has a TYPE MISMATCH (score declared DOUBLE, sent STRING).
	// The WHOLE batch must fail — no partial append (event sourcing must not record
	// a half-corrupt write). Then the log must contain ZERO value events.
	in := domain.WriteFeaturesInput{
		FeatureViewID:  v.ID,
		IdempotencyKey: "w-2",
		Rows: []domain.FeatureRow{
			{EntityID: "u1", Values: map[string]domain.FeatureValue{"score": {Kind: domain.FeatureTypeDouble, Double: 1}}, EventTime: at(1)},
			{EntityID: "u2", Values: map[string]domain.FeatureValue{"score": {Kind: domain.FeatureTypeString, Str: "oops"}}, EventTime: at(1)},
		},
	}
	_, err := h.svc.WriteFeatures(context.Background(), alice, in)
	if !errors.Is(err, domain.ErrSchemaViolation) {
		t.Fatalf("type mismatch should be ErrSchemaViolation, got %v", err)
	}
	// No value event should have been appended (all-or-nothing).
	all, _ := h.log.Load(context.Background(), v.ID)
	for _, e := range all {
		if e.Type == domain.FeatureEventValuesWritten {
			t.Fatalf("schema violation must append NO value events, found %#v", e)
		}
	}
}

func TestWriteFeatures_UnknownFeature_Rejected(t *testing.T) {
	h := newHarness()
	v := mustDefine(t, h)
	in := domain.WriteFeaturesInput{
		FeatureViewID:  v.ID,
		IdempotencyKey: "w-3",
		Rows: []domain.FeatureRow{
			{EntityID: "u1", Values: map[string]domain.FeatureValue{"not_a_feature": {Kind: domain.FeatureTypeDouble, Double: 1}}, EventTime: at(1)},
		},
	}
	if _, err := h.svc.WriteFeatures(context.Background(), alice, in); !errors.Is(err, domain.ErrSchemaViolation) {
		t.Errorf("unknown feature name should be ErrSchemaViolation, got %v", err)
	}
}

func TestWriteFeatures_DimensionMismatch_Rejected(t *testing.T) {
	h := newHarness()
	// Define a view with a fixed-dimension embedding.
	in := sampleDefineInput()
	in.Name = "emb_view"
	in.IdempotencyKey = "def-emb"
	in.Features = []domain.FeatureSpec{{Name: "embedding", ValueType: domain.FeatureTypeDoubleList, Dimension: 3}}
	v, err := h.svc.DefineFeatureView(context.Background(), alice, in)
	if err != nil {
		t.Fatalf("define emb: %v", err)
	}
	// A 2-length vector violates the declared dimension 3.
	w := domain.WriteFeaturesInput{
		FeatureViewID:  v.ID,
		IdempotencyKey: "w-emb",
		Rows: []domain.FeatureRow{
			{EntityID: "u1", Values: map[string]domain.FeatureValue{"embedding": {Kind: domain.FeatureTypeDoubleList, List: []float64{1, 2}}}, EventTime: at(1)},
		},
	}
	if _, err := h.svc.WriteFeatures(context.Background(), alice, w); !errors.Is(err, domain.ErrSchemaViolation) {
		t.Errorf("wrong embedding dimension should be ErrSchemaViolation, got %v", err)
	}
}

func TestWriteFeatures_BatchTooLarge_Rejected(t *testing.T) {
	h := newHarness()
	v := mustDefine(t, h)
	rows := make([]domain.FeatureRow, domain.MaxBatchSize+1)
	for i := range rows {
		rows[i] = domain.FeatureRow{EntityID: "u", Values: map[string]domain.FeatureValue{"score": {Kind: domain.FeatureTypeDouble, Double: 1}}, EventTime: at(1)}
	}
	in := domain.WriteFeaturesInput{FeatureViewID: v.ID, IdempotencyKey: "w-big", Rows: rows}
	if _, err := h.svc.WriteFeatures(context.Background(), alice, in); !errors.Is(err, domain.ErrBatchTooLarge) {
		t.Errorf("over-cap batch should be ErrBatchTooLarge (rejected, not truncated), got %v", err)
	}
}

func TestWriteFeatures_Idempotent(t *testing.T) {
	h := newHarness()
	v := mustDefine(t, h)
	in := domain.WriteFeaturesInput{
		FeatureViewID:  v.ID,
		IdempotencyKey: "w-dup",
		Rows: []domain.FeatureRow{
			{EntityID: "u1", Values: map[string]domain.FeatureValue{"score": {Kind: domain.FeatureTypeDouble, Double: 1}}, EventTime: at(1)},
		},
	}
	r1, err := h.svc.WriteFeatures(context.Background(), alice, in)
	if err != nil {
		t.Fatalf("write 1: %v", err)
	}
	r2, err := h.svc.WriteFeatures(context.Background(), alice, in) // retry, same key
	if err != nil {
		t.Fatalf("write 2 (retry): %v", err)
	}
	if r1.WrittenThroughVersion != r2.WrittenThroughVersion {
		t.Errorf("idempotent retry should return the original version range, got %d then %d", r1.WrittenThroughVersion, r2.WrittenThroughVersion)
	}
	// Only ONE value event must exist (the retry deduped).
	all, _ := h.log.Load(context.Background(), v.ID)
	count := 0
	for _, e := range all {
		if e.Type == domain.FeatureEventValuesWritten {
			count++
		}
	}
	if count != 1 {
		t.Errorf("idempotent retry must not append a duplicate value event, got %d", count)
	}
}

func TestWriteFeatures_DefaultsEventTimeToNow(t *testing.T) {
	h := newHarness()
	v := mustDefine(t, h)
	in := domain.WriteFeaturesInput{
		FeatureViewID:  v.ID,
		IdempotencyKey: "w-noet",
		Rows: []domain.FeatureRow{
			{EntityID: "u1", Values: map[string]domain.FeatureValue{"score": {Kind: domain.FeatureTypeDouble, Double: 1}}}, // no EventTime
		},
	}
	if _, err := h.svc.WriteFeatures(context.Background(), alice, in); err != nil {
		t.Fatalf("write: %v", err)
	}
	all, _ := h.log.Load(context.Background(), v.ID)
	for _, e := range all {
		if e.Type == domain.FeatureEventValuesWritten && !e.EventTime.Equal(h.clock.Now()) {
			t.Errorf("missing event_time should default to ingest clock %v, got %v", h.clock.Now(), e.EventTime)
		}
	}
}

// ============================================================================
// GetHistoricalFeatures — point-in-time read over the log (reproducibility)
// ============================================================================

func TestGetHistoricalFeatures_PointInTime(t *testing.T) {
	h := newHarness()
	v := mustDefine(t, h)

	// Three writes for u1 at t1,t2,t3 with increasing score.
	for i, et := range []time.Time{at(1), at(2), at(3)} {
		score := float64(i + 1)
		in := domain.WriteFeaturesInput{
			FeatureViewID:  v.ID,
			IdempotencyKey: "h-" + itoa(i),
			Rows:           []domain.FeatureRow{{EntityID: "u1", Values: map[string]domain.FeatureValue{"score": {Kind: domain.FeatureTypeDouble, Double: score}}, EventTime: et}},
		}
		if _, err := h.svc.WriteFeatures(context.Background(), alice, in); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}

	// As-of t2 must see score=2 (the latest event with event_time ≤ t2), not 3.
	page, err := h.svc.GetHistoricalFeatures(context.Background(), alice, domain.GetHistoricalFeaturesInput{
		FeatureViewID: v.ID, EntityIDs: []string{"u1"}, AsOf: at(2),
	})
	if err != nil {
		t.Fatalf("GetHistoricalFeatures: %v", err)
	}
	if len(page.Vectors) != 1 || page.Vectors[0].Values["score"].Double != 2 {
		t.Fatalf("as-of t2 should reconstruct score=2, got %#v", page.Vectors)
	}
}

func TestGetHistoricalFeatures_RequiresAsOf(t *testing.T) {
	h := newHarness()
	v := mustDefine(t, h)
	_, err := h.svc.GetHistoricalFeatures(context.Background(), alice, domain.GetHistoricalFeaturesInput{
		FeatureViewID: v.ID, EntityIDs: []string{"u1"}, // AsOf zero
	})
	if !errors.Is(err, domain.ErrAsOfRequired) {
		t.Errorf("missing as_of should be ErrAsOfRequired, got %v", err)
	}
}

func TestGetHistoricalFeatures_MissingEntitiesReported(t *testing.T) {
	h := newHarness()
	v := mustDefine(t, h)
	// u1 exists; u2 never written → must be reported as missing, not silently dropped.
	in := domain.WriteFeaturesInput{
		FeatureViewID: v.ID, IdempotencyKey: "h-m",
		Rows: []domain.FeatureRow{{EntityID: "u1", Values: map[string]domain.FeatureValue{"score": {Kind: domain.FeatureTypeDouble, Double: 1}}, EventTime: at(1)}},
	}
	if _, err := h.svc.WriteFeatures(context.Background(), alice, in); err != nil {
		t.Fatalf("write: %v", err)
	}
	page, err := h.svc.GetHistoricalFeatures(context.Background(), alice, domain.GetHistoricalFeaturesInput{
		FeatureViewID: v.ID, EntityIDs: []string{"u1", "u2"}, AsOf: at(5),
	})
	if err != nil {
		t.Fatalf("historical: %v", err)
	}
	if len(page.MissingEntityIDs) != 1 || page.MissingEntityIDs[0] != "u2" {
		t.Errorf("u2 should be reported missing, got %#v", page.MissingEntityIDs)
	}
}

// ============================================================================
// DeleteFeatureView — soft retire (append terminal event), then refuse writes
// ============================================================================

func TestDeleteFeatureView_SoftRetireAndRefuseWrites(t *testing.T) {
	h := newHarness()
	v := mustDefine(t, h)

	deleted, err := h.svc.DeleteFeatureView(context.Background(), alice, v.ID, "del-1")
	if err != nil {
		t.Fatalf("delete: %v", err)
	}
	if !deleted.IsDeleted() {
		t.Error("deleted view should have DeletedAt set")
	}
	// The online projection must have been purged.
	if !h.online.purged[v.ID] {
		t.Error("delete should purge the online projection")
	}
	// New writes to a retired view must be refused.
	in := domain.WriteFeaturesInput{
		FeatureViewID: v.ID, IdempotencyKey: "w-after-del",
		Rows: []domain.FeatureRow{{EntityID: "u1", Values: map[string]domain.FeatureValue{"score": {Kind: domain.FeatureTypeDouble, Double: 1}}, EventTime: at(1)}},
	}
	if _, err := h.svc.WriteFeatures(context.Background(), alice, in); !errors.Is(err, domain.ErrViewDeleted) {
		t.Errorf("write to deleted view should be ErrViewDeleted, got %v", err)
	}
	// History must still be queryable (reproducibility) — GetFeatureView returns it.
	got, err := h.svc.GetFeatureViewByID(context.Background(), alice, v.ID)
	if err != nil {
		t.Fatalf("GetFeatureView on deleted view should still work: %v", err)
	}
	if !got.IsDeleted() {
		t.Error("fetched deleted view should report IsDeleted")
	}
}

func TestDeleteFeatureView_Idempotent(t *testing.T) {
	h := newHarness()
	v := mustDefine(t, h)
	d1, err := h.svc.DeleteFeatureView(context.Background(), alice, v.ID, "del-x")
	if err != nil {
		t.Fatalf("delete 1: %v", err)
	}
	d2, err := h.svc.DeleteFeatureView(context.Background(), alice, v.ID, "del-x") // retry
	if err != nil {
		t.Fatalf("delete 2 (retry): %v", err)
	}
	if !d1.DeletedAt.Equal(d2.DeletedAt) {
		t.Errorf("idempotent re-delete should return the same retirement time, got %v then %v", d1.DeletedAt, d2.DeletedAt)
	}
}

// ============================================================================
// TEAM SCOPING — a cross-team id is NotFound (not PermissionDenied)
// ============================================================================

func TestTeamScoping_CrossTeamViewIsNotFound(t *testing.T) {
	h := newHarness()
	v := mustDefine(t, h) // owned by alice/platform

	bob := domain.Principal{UserID: "bob", Team: "other-team"}
	if _, err := h.svc.GetFeatureViewByID(context.Background(), bob, v.ID); !errors.Is(err, domain.ErrViewNotFound) {
		t.Errorf("cross-team read should be ErrViewNotFound (not a permission leak), got %v", err)
	}
	// And bob must not be able to write to alice's view either.
	in := domain.WriteFeaturesInput{
		FeatureViewID: v.ID, IdempotencyKey: "w-bob",
		Rows: []domain.FeatureRow{{EntityID: "u1", Values: map[string]domain.FeatureValue{"score": {Kind: domain.FeatureTypeDouble, Double: 1}}, EventTime: at(1)}},
	}
	if _, err := h.svc.WriteFeatures(context.Background(), bob, in); !errors.Is(err, domain.ErrViewNotFound) {
		t.Errorf("cross-team write should be ErrViewNotFound, got %v", err)
	}
}

// ============================================================================
// RebuildViews — REPLAY the log to regenerate views (the headline party trick)
// ============================================================================

func TestRebuildViews_ReplaysLogToRegenerateOnline(t *testing.T) {
	h := newHarness()
	v := mustDefine(t, h)
	in := domain.WriteFeaturesInput{
		FeatureViewID: v.ID, IdempotencyKey: "rb-w",
		Rows: []domain.FeatureRow{{EntityID: "u1", Values: map[string]domain.FeatureValue{"score": {Kind: domain.FeatureTypeDouble, Double: 7}}, EventTime: at(1)}},
	}
	if _, err := h.svc.WriteFeatures(context.Background(), alice, in); err != nil {
		t.Fatalf("write: %v", err)
	}

	// Simulate a Redis flush: wipe the online store WITHOUT touching the log.
	h.online.data = map[string]map[string]domain.FeatureVector{}

	// Online read now misses (cache gone, log intact).
	page, _ := h.svc.GetOnlineFeatures(context.Background(), alice, domain.GetOnlineFeaturesInput{FeatureViewID: v.ID, EntityIDs: []string{"u1"}})
	if len(page.Vectors) != 0 {
		t.Fatalf("after flush the online read should miss, got %#v", page.Vectors)
	}

	// REPLAY: rebuild from the log. The cache is regenerated FROM the immutable log,
	// proving the log is the source of truth.
	var frames int
	var sawDone bool
	err := h.svc.RebuildViews(context.Background(), alice, domain.RebuildViewsInput{FeatureViewID: v.ID, Target: domain.RebuildTargetOnline},
		func(p domain.RebuildProgress) error {
			frames++
			if p.Done {
				sawDone = true
			}
			return nil
		})
	if err != nil {
		t.Fatalf("RebuildViews: %v", err)
	}
	if !sawDone {
		t.Error("rebuild should emit a final done=true frame")
	}

	// Online read must now succeed again with the original value (7).
	page2, _ := h.svc.GetOnlineFeatures(context.Background(), alice, domain.GetOnlineFeaturesInput{FeatureViewID: v.ID, EntityIDs: []string{"u1"}})
	if len(page2.Vectors) != 1 || page2.Vectors[0].Values["score"].Double != 7 {
		t.Fatalf("rebuild should regenerate the online value from the log, got %#v", page2.Vectors)
	}
}

func TestRebuildViews_RequiresAdminScope(t *testing.T) {
	h := newHarness()
	v := mustDefine(t, h)
	noAdmin := domain.Principal{UserID: "carol", Team: "platform", Scopes: []string{"features:write"}}
	err := h.svc.RebuildViews(context.Background(), noAdmin, domain.RebuildViewsInput{FeatureViewID: v.ID},
		func(domain.RebuildProgress) error { return nil })
	if err == nil {
		t.Error("RebuildViews without features:admin scope should be rejected")
	}
}
