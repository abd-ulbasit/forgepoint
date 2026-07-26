// registry_service_test.go — TDD specification for the RegistryService impl.
//
// ============================================================================
// WHY package domain (internal test), not domain_test (external)
// ============================================================================
//
// Unlike the Auth service — whose tests had to be EXTERNAL because the mocks
// satisfied repository.* interfaces that imported domain (a cycle for an in-package
// test) — the registry's ports ALL live in the domain package itself, and there is
// no repository shim involved in these tests. So an in-package test (`package
// domain`) has no cycle. We keep it in-package deliberately: it lets the mocks and
// tests use the unexported helpers freely, and the tests still exercise only the
// behavior that matters. (We could go external too; in-package is the simpler
// correct choice here.)
//
// These tests are written BEFORE registry_service_impl.go exists and assert REAL
// behavior — that the single-production swap actually moves BOTH versions, that the
// state machine rejects illegal edges, that idempotency returns the SAME record,
// that server-authoritative fields are stamped from the actor and not the input,
// that the page size is actually clamped, and that the right projection event is
// emitted with the right payload. They are not mock-call-count assertions.
// ============================================================================
package domain

import (
	"context"
	"errors"
	"strconv"
	"testing"
	"time"
)

// ============================================================================
// HAND-WRITTEN MOCKS / FAKES (no codegen)
// ============================================================================
//
// fakeWriteStore is a real in-memory implementation of WriteStore (not a stub):
// it actually stores models/versions, enforces the (team,name) and (model,version)
// uniqueness, records idempotency keys, and performs the promote/archive
// transactions atomically (in-memory, but with the same observable semantics).
// Using a REAL fake rather than per-call function stubs is what lets the tests
// assert true behavior — e.g. that after a promote the demoted version is REALLY
// ARCHIVED in the store, not merely that a function was called.

type fakeWriteStore struct {
	models      map[string]Model        // id -> model
	versions    map[string]ModelVersion // id -> version
	modelIdem   map[string]string       // team|key -> model id (create ledger)
	versionIdem map[string]string       // modelID|key -> version id (create ledger)
	cmdIdem     map[string]string       // team|command|key -> entity id (mutation ledger)

	failCreateModel error // injectable failure for the conflict-path test

	// createVersionHook lets a test inject behavior on each CreateVersion call (e.g.
	// pre-seed a colliding row to simulate a concurrent racer) WITHOUT changing the
	// stored-uniqueness semantics. Called with the version about to be inserted,
	// BEFORE the uniqueness check; returning an error aborts the insert with it.
	createVersionHook func(v ModelVersion) error
}

func newFakeWriteStore() *fakeWriteStore {
	return &fakeWriteStore{
		models:      map[string]Model{},
		versions:    map[string]ModelVersion{},
		modelIdem:   map[string]string{},
		versionIdem: map[string]string{},
		cmdIdem:     map[string]string{},
	}
}

func (f *fakeWriteStore) CreateModel(_ context.Context, m Model, key string) (Model, error) {
	if f.failCreateModel != nil {
		return Model{}, f.failCreateModel
	}
	// Enforce (team, name) uniqueness like the real unique index.
	for _, existing := range f.models {
		if existing.Team == m.Team && existing.Name == m.Name {
			return Model{}, ErrWriteConflict
		}
	}
	f.models[m.ID] = m
	if key != "" {
		f.modelIdem[m.Team+"|"+key] = m.ID
	}
	return m, nil
}

func (f *fakeWriteStore) LookupModelByIdempotencyKey(_ context.Context, team, key string) (Model, error) {
	if key == "" {
		return Model{}, ErrRecordNotFound
	}
	id, ok := f.modelIdem[team+"|"+key]
	if !ok {
		return Model{}, ErrRecordNotFound
	}
	return f.models[id], nil
}

func (f *fakeWriteStore) GetModel(_ context.Context, id string) (Model, error) {
	m, ok := f.models[id]
	if !ok {
		return Model{}, ErrRecordNotFound
	}
	return m, nil
}

func (f *fakeWriteStore) UpdateModel(_ context.Context, m Model) (Model, error) {
	if _, ok := f.models[m.ID]; !ok {
		return Model{}, ErrRecordNotFound
	}
	f.models[m.ID] = m
	return m, nil
}

func (f *fakeWriteStore) CreateVersion(_ context.Context, v ModelVersion, key string) (ModelVersion, error) {
	// The hook simulates a concurrent racer: it can insert a colliding row just
	// before our uniqueness check, reproducing the TOCTOU the retry loop must survive.
	if f.createVersionHook != nil {
		if err := f.createVersionHook(v); err != nil {
			return ModelVersion{}, err
		}
	}
	for _, existing := range f.versions {
		if existing.ModelID == v.ModelID && existing.Version == v.Version {
			return ModelVersion{}, ErrWriteConflict
		}
	}
	f.versions[v.ID] = v
	if key != "" {
		f.versionIdem[v.ModelID+"|"+key] = v.ID
	}
	return v, nil
}

func (f *fakeWriteStore) LookupVersionByIdempotencyKey(_ context.Context, modelID, key string) (ModelVersion, error) {
	if key == "" {
		return ModelVersion{}, ErrRecordNotFound
	}
	id, ok := f.versionIdem[modelID+"|"+key]
	if !ok {
		return ModelVersion{}, ErrRecordNotFound
	}
	return f.versions[id], nil
}

func (f *fakeWriteStore) GetVersion(_ context.Context, id string) (ModelVersion, error) {
	v, ok := f.versions[id]
	if !ok {
		return ModelVersion{}, ErrRecordNotFound
	}
	return v, nil
}

func (f *fakeWriteStore) CountVersions(_ context.Context, modelID string) (int, error) {
	n := 0
	for _, v := range f.versions {
		if v.ModelID == modelID {
			n++
		}
	}
	return n, nil
}

func (f *fakeWriteStore) UpdateVersionStatus(_ context.Context, v ModelVersion) (ModelVersion, error) {
	if _, ok := f.versions[v.ID]; !ok {
		return ModelVersion{}, ErrRecordNotFound
	}
	f.versions[v.ID] = v
	return v, nil
}

func (f *fakeWriteStore) PromoteVersionTx(_ context.Context, promoted, demoted ModelVersion) (ModelVersion, ModelVersion, error) {
	// The atomic swap: persist BOTH rows. In a real adapter this is one tx.
	if _, ok := f.versions[promoted.ID]; !ok {
		return ModelVersion{}, ModelVersion{}, ErrRecordNotFound
	}
	f.versions[promoted.ID] = promoted
	if demoted.ID != "" {
		f.versions[demoted.ID] = demoted
	}
	return promoted, demoted, nil
}

func (f *fakeWriteStore) FindProductionVersion(_ context.Context, modelID string) (ModelVersion, error) {
	for _, v := range f.versions {
		if v.ModelID == modelID && v.Stage == StageProduction {
			return v, nil
		}
	}
	return ModelVersion{}, ErrRecordNotFound
}

func (f *fakeWriteStore) ArchiveModelTx(_ context.Context, m Model) (Model, error) {
	if _, ok := f.models[m.ID]; !ok {
		return Model{}, ErrRecordNotFound
	}
	f.models[m.ID] = m
	// Archive every version of the model (the aggregate is the consistency boundary).
	for id, v := range f.versions {
		if v.ModelID == m.ID {
			v.Stage = StageArchived
			f.versions[id] = v
		}
	}
	return m, nil
}

// cmdKey builds the (team, command, key) composite the mutation ledger is indexed
// by — the same shape the production Postgres table's composite PK uses.
func cmdKey(team string, command Command, key string) string {
	return team + "|" + command.String() + "|" + key
}

func (f *fakeWriteStore) LookupCommandIdempotency(_ context.Context, team string, command Command, key string) (string, error) {
	if key == "" {
		return "", ErrRecordNotFound
	}
	id, ok := f.cmdIdem[cmdKey(team, command, key)]
	if !ok {
		return "", ErrRecordNotFound
	}
	return id, nil
}

func (f *fakeWriteStore) RecordCommandIdempotency(_ context.Context, team string, command Command, key, entityID string) error {
	if key == "" {
		return nil
	}
	k := cmdKey(team, command, key)
	// ON CONFLICT semantics: if this key already maps to a DIFFERENT entity, that is
	// a genuine key-reuse race — surface a conflict rather than overwrite the winner.
	if existing, ok := f.cmdIdem[k]; ok && existing != entityID {
		return ErrWriteConflict
	}
	f.cmdIdem[k] = entityID
	return nil
}

// recordingEmitter captures every ProjectionEvent so a test can assert the EXACT
// event kind + payload a command emitted (the CQRS sync fact), not merely that
// "something" was emitted.
type recordingEmitter struct {
	events  []ProjectionEvent
	failNth int // if >0, the Nth Emit call (1-based) returns an error
	calls   int
}

func (e *recordingEmitter) Emit(_ context.Context, ev ProjectionEvent) error {
	e.calls++
	if e.failNth != 0 && e.calls == e.failNth {
		return errors.New("emit boom")
	}
	e.events = append(e.events, ev)
	return nil
}

func (e *recordingEmitter) last() ProjectionEvent {
	if len(e.events) == 0 {
		return ProjectionEvent{}
	}
	return e.events[len(e.events)-1]
}

// fixedClock returns a constant time so timestamp assertions are deterministic.
type fixedClock struct{ t time.Time }

func (c fixedClock) Now() time.Time { return c.t }

// seqIDGen hands out deterministic ids ("id-1","id-2",...) so tests reference exact
// ids. A real uuid generator would make assertions on specific ids impossible.
type seqIDGen struct{ n int }

func (g *seqIDGen) NewID() string {
	g.n++
	return "id-" + strconv.Itoa(g.n)
}

// ----------------------------------------------------------------------------
// test harness: build a service wired to fresh fakes + a known clock/idgen.
// ----------------------------------------------------------------------------

type harness struct {
	svc     RegistryService
	write   *fakeWriteStore
	emitter *recordingEmitter
	now     time.Time
}

func newHarness() *harness {
	now := time.Date(2026, 6, 17, 12, 0, 0, 0, time.UTC)
	write := newFakeWriteStore()
	emitter := &recordingEmitter{}
	// The READ store is irrelevant to command tests; pass a fake that fails loudly
	// if a command unexpectedly reads the projection (commands must read the truth).
	svc := NewRegistryService(write, panicReadStore{}, emitter, fixedClock{t: now}, &seqIDGen{})
	return &harness{svc: svc, write: write, emitter: emitter, now: now}
}

// panicReadStore ensures COMMAND tests never accidentally read the projection —
// if a command does, the test fails loudly, proving the command/query separation.
type panicReadStore struct{}

func (panicReadStore) GetModelByID(context.Context, string, string) (Model, error) {
	panic("command read the projection")
}
func (panicReadStore) GetModelByName(context.Context, string, string) (Model, error) {
	panic("command read the projection")
}
func (panicReadStore) ListModels(context.Context, string, ListModelsFilter, ListOptions) ([]Model, string, error) {
	panic("command read the projection")
}
func (panicReadStore) CountModels(context.Context, string, ListModelsFilter) (int, error) {
	panic("command read the projection")
}
func (panicReadStore) GetVersionByID(context.Context, string) (ModelVersion, error) {
	panic("command read the projection")
}
func (panicReadStore) GetVersionByLabel(context.Context, string, string) (ModelVersion, error) {
	panic("command read the projection")
}
func (panicReadStore) ListVersions(context.Context, string, ModelStage, ListOptions) ([]ModelVersion, string, error) {
	panic("command read the projection")
}

var testActor = Actor{UserID: "user-42", Team: "ml-platform"}

// ============================================================================
// MODEL STATE MACHINE (pure, no service) — the rules in isolation.
// ============================================================================

func TestModelStage_CanTransitionTo(t *testing.T) {
	cases := []struct {
		from, to ModelStage
		want     bool
	}{
		{StageDev, StageStaging, true},
		{StageDev, StageArchived, true},
		{StageDev, StageProduction, false}, // must not skip STAGING
		{StageStaging, StageProduction, true},
		{StageStaging, StageArchived, true},
		{StageStaging, StageDev, false}, // no demotion back to DEV
		{StageProduction, StageArchived, true},
		{StageProduction, StageStaging, false},
		{StageArchived, StageStaging, false}, // terminal
		{StageArchived, StageProduction, false},
		{StageDev, StageDev, false}, // self-edge is not a move
	}
	for _, c := range cases {
		if got := c.from.CanTransitionTo(c.to); got != c.want {
			t.Errorf("%s→%s: got %v want %v", c.from, c.to, got, c.want)
		}
	}
}

// ============================================================================
// RegisterModel
// ============================================================================

func TestRegisterModel_StampsServerAuthoritativeFields(t *testing.T) {
	h := newHarness()
	// The input even tries to look like it could carry an owner — it can't; the
	// struct has no such field. We assert the model comes back owned by the ACTOR,
	// timestamped by the CLOCK, and id'd by the GENERATOR — none from the input.
	got, err := h.svc.RegisterModel(context.Background(), testActor, RegisterModelInput{
		Name:      "fraud-detector",
		Framework: "pytorch",
		TaskType:  "classification",
		Tags:      map[string]string{"domain": "fraud"},
	})
	if err != nil {
		t.Fatalf("RegisterModel: %v", err)
	}
	if got.ID != "id-1" {
		t.Errorf("id: got %q want id-1 (server-generated)", got.ID)
	}
	if got.OwnerID != testActor.UserID || got.Team != testActor.Team {
		t.Errorf("owner/team not from actor: got owner=%q team=%q", got.OwnerID, got.Team)
	}
	if !got.CreatedAt.Equal(h.now) || !got.UpdatedAt.Equal(h.now) {
		t.Errorf("timestamps not from clock: created=%v updated=%v want %v", got.CreatedAt, got.UpdatedAt, h.now)
	}
	if got.IsArchived() {
		t.Errorf("new model must not be archived")
	}
	// The write store must actually hold it (real persistence, not a no-op).
	if stored, err := h.write.GetModel(context.Background(), got.ID); err != nil || stored.Name != "fraud-detector" {
		t.Errorf("model not persisted to write store: %v / %+v", err, stored)
	}
	// And a ModelRegistered projection event must have been emitted with the right payload.
	ev := h.emitter.last()
	if ev.Kind != EventModelRegistered {
		t.Fatalf("event kind: got %v want EventModelRegistered", ev.Kind)
	}
	if ev.Model.ID != got.ID || ev.Actor.UserID != testActor.UserID || !ev.OccurredAt.Equal(h.now) {
		t.Errorf("event payload wrong: %+v", ev)
	}
}

func TestRegisterModel_RejectsEmptyName(t *testing.T) {
	h := newHarness()
	_, err := h.svc.RegisterModel(context.Background(), testActor, RegisterModelInput{Name: "  "})
	if !errors.Is(err, ErrValidation) {
		t.Fatalf("got %v want ErrValidation", err)
	}
	if len(h.emitter.events) != 0 {
		t.Errorf("no event should be emitted on validation failure")
	}
}

func TestRegisterModel_NameCollision(t *testing.T) {
	h := newHarness()
	in := RegisterModelInput{Name: "dup"}
	if _, err := h.svc.RegisterModel(context.Background(), testActor, in); err != nil {
		t.Fatalf("first register: %v", err)
	}
	// Same team, same name, NO idempotency key → ALREADY_EXISTS business error.
	_, err := h.svc.RegisterModel(context.Background(), testActor, in)
	if !errors.Is(err, ErrModelNameTaken) {
		t.Fatalf("got %v want ErrModelNameTaken", err)
	}
}

func TestRegisterModel_IdempotentRetryReturnsSameModel(t *testing.T) {
	h := newHarness()
	in := RegisterModelInput{Name: "idem", IdempotencyKey: "k-1"}
	first, err := h.svc.RegisterModel(context.Background(), testActor, in)
	if err != nil {
		t.Fatalf("first: %v", err)
	}
	second, err := h.svc.RegisterModel(context.Background(), testActor, in)
	if err != nil {
		t.Fatalf("retry should succeed, got %v", err)
	}
	if first.ID != second.ID {
		t.Errorf("idempotent retry returned a different model: %q vs %q", first.ID, second.ID)
	}
	// Critically: the retry must NOT create a second model and must NOT emit a
	// second event (that would double-notify downstream consumers).
	if len(h.write.models) != 1 {
		t.Errorf("idempotent retry created a duplicate: %d models", len(h.write.models))
	}
	if len(h.emitter.events) != 1 {
		t.Errorf("idempotent retry re-emitted an event: %d events", len(h.emitter.events))
	}
}

// ============================================================================
// CreateVersion
// ============================================================================

// registerModel is a helper that creates a model and returns it.
func (h *harness) mustRegister(t *testing.T, name string) Model {
	t.Helper()
	m, err := h.svc.RegisterModel(context.Background(), testActor, RegisterModelInput{Name: name})
	if err != nil {
		t.Fatalf("mustRegister(%q): %v", name, err)
	}
	return m
}

func TestCreateVersion_AutoAssignsMonotonicLabelAndDefaults(t *testing.T) {
	h := newHarness()
	m := h.mustRegister(t, "m")
	v1, err := h.svc.CreateVersion(context.Background(), testActor, CreateVersionInput{ModelID: m.ID})
	if err != nil {
		t.Fatalf("CreateVersion: %v", err)
	}
	if v1.Version != "1" {
		t.Errorf("first auto version: got %q want \"1\"", v1.Version)
	}
	if v1.Stage != StageDev || v1.Status != StatusPendingUpload {
		t.Errorf("new version defaults: stage=%v status=%v want DEV/PENDING_UPLOAD", v1.Stage, v1.Status)
	}
	if v1.CreatedBy != testActor.UserID {
		t.Errorf("created_by not from actor: %q", v1.CreatedBy)
	}
	// Artifact facts must be empty on create (server-measured later, never on create).
	if v1.ArtifactDigest != "" || v1.SizeBytes != 0 || v1.ArtifactPath != "" {
		t.Errorf("artifact facts must be empty on create: %+v", v1)
	}
	v2, err := h.svc.CreateVersion(context.Background(), testActor, CreateVersionInput{ModelID: m.ID})
	if err != nil {
		t.Fatalf("CreateVersion 2: %v", err)
	}
	if v2.Version != "2" {
		t.Errorf("second auto version: got %q want \"2\"", v2.Version)
	}
	// EventVersionCreated must be emitted (after the ModelRegistered from setup).
	ev := h.emitter.last()
	if ev.Kind != EventVersionCreated || ev.Version.ID != v2.ID || ev.Model.ID != m.ID {
		t.Errorf("expected EventVersionCreated for v2, got %+v", ev)
	}
}

func TestCreateVersion_PinnedLabelCollision(t *testing.T) {
	h := newHarness()
	m := h.mustRegister(t, "m")
	if _, err := h.svc.CreateVersion(context.Background(), testActor, CreateVersionInput{ModelID: m.ID, Version: "v9"}); err != nil {
		t.Fatalf("first pinned: %v", err)
	}
	_, err := h.svc.CreateVersion(context.Background(), testActor, CreateVersionInput{ModelID: m.ID, Version: "v9"})
	if !errors.Is(err, ErrVersionExists) {
		t.Fatalf("got %v want ErrVersionExists", err)
	}
}

func TestCreateVersion_UnknownModel(t *testing.T) {
	h := newHarness()
	_, err := h.svc.CreateVersion(context.Background(), testActor, CreateVersionInput{ModelID: "nope"})
	if !errors.Is(err, ErrModelNotFound) {
		t.Fatalf("got %v want ErrModelNotFound", err)
	}
}

func TestCreateVersion_OnArchivedModelRejected(t *testing.T) {
	h := newHarness()
	m := h.mustRegister(t, "m")
	if _, err := h.svc.ArchiveModel(context.Background(), testActor, m.ID, ""); err != nil {
		t.Fatalf("archive: %v", err)
	}
	_, err := h.svc.CreateVersion(context.Background(), testActor, CreateVersionInput{ModelID: m.ID})
	if !errors.Is(err, ErrModelArchived) {
		t.Fatalf("got %v want ErrModelArchived", err)
	}
}

// ============================================================================
// MarkVersionReady — the PENDING_UPLOAD → READY/FAILED edge.
// ============================================================================

func (h *harness) mustVersion(t *testing.T, modelID string) ModelVersion {
	t.Helper()
	v, err := h.svc.CreateVersion(context.Background(), testActor, CreateVersionInput{ModelID: modelID})
	if err != nil {
		t.Fatalf("mustVersion: %v", err)
	}
	return v
}

func TestMarkVersionReady_Success(t *testing.T) {
	h := newHarness()
	m := h.mustRegister(t, "m")
	v := h.mustVersion(t, m.ID)
	got, err := h.svc.MarkVersionReady(context.Background(), testActor, MarkVersionReadyInput{
		VersionID:      v.ID,
		Success:        true,
		ArtifactPath:   "s3://fp-models/m/1/model.onnx",
		ArtifactDigest: "sha256:abc",
		SizeBytes:      2048,
	})
	if err != nil {
		t.Fatalf("MarkVersionReady: %v", err)
	}
	if got.Status != StatusReady {
		t.Errorf("status: got %v want READY", got.Status)
	}
	// Server-measured artifact facts must be recorded.
	if got.ArtifactDigest != "sha256:abc" || got.SizeBytes != 2048 {
		t.Errorf("artifact facts not recorded: %+v", got)
	}
	ev := h.emitter.last()
	if ev.Kind != EventVersionReady || ev.Version.SizeBytes != 2048 {
		t.Errorf("expected EventVersionReady with size, got %+v", ev)
	}
}

func TestMarkVersionReady_FailureEmitsNoReadyEvent(t *testing.T) {
	h := newHarness()
	m := h.mustRegister(t, "m")
	v := h.mustVersion(t, m.ID)
	before := len(h.emitter.events)
	got, err := h.svc.MarkVersionReady(context.Background(), testActor, MarkVersionReadyInput{VersionID: v.ID, Success: false})
	if err != nil {
		t.Fatalf("MarkVersionReady(fail): %v", err)
	}
	if got.Status != StatusFailed {
		t.Errorf("status: got %v want FAILED", got.Status)
	}
	if len(h.emitter.events) != before {
		t.Errorf("a FAILED confirmation must emit NO event; emitted %d", len(h.emitter.events)-before)
	}
}

func TestMarkVersionReady_IdempotentOnAlreadyReady(t *testing.T) {
	h := newHarness()
	m := h.mustRegister(t, "m")
	v := h.mustVersion(t, m.ID)
	in := MarkVersionReadyInput{VersionID: v.ID, Success: true, ArtifactDigest: "sha256:x", SizeBytes: 1}
	if _, err := h.svc.MarkVersionReady(context.Background(), testActor, in); err != nil {
		t.Fatalf("first ready: %v", err)
	}
	eventsAfterFirst := len(h.emitter.events)
	// A duplicate confirmation (e.g. webhook re-delivery) must be a no-op returning
	// the current state — NOT a second ModelVersionReady (which would double-meter
	// storage in billing).
	got, err := h.svc.MarkVersionReady(context.Background(), testActor, in)
	if err != nil {
		t.Fatalf("idempotent ready retry: %v", err)
	}
	if got.Status != StatusReady {
		t.Errorf("status after retry: got %v want READY", got.Status)
	}
	if len(h.emitter.events) != eventsAfterFirst {
		t.Errorf("idempotent ready retry re-emitted: now %d", len(h.emitter.events))
	}
}

// ============================================================================
// PromoteVersion — state machine + SINGLE-PRODUCTION invariant (the centerpiece).
// ============================================================================

// readyVersion creates a version and marks it READY so it is promotable.
func (h *harness) readyVersion(t *testing.T, modelID string) ModelVersion {
	t.Helper()
	v := h.mustVersion(t, modelID)
	rv, err := h.svc.MarkVersionReady(context.Background(), testActor, MarkVersionReadyInput{
		VersionID: v.ID, Success: true, ArtifactDigest: "sha256:" + v.ID, SizeBytes: 1,
	})
	if err != nil {
		t.Fatalf("readyVersion: %v", err)
	}
	return rv
}

// promote walks a READY version DEV→STAGING→PRODUCTION (the legal path).
func (h *harness) promoteToProd(t *testing.T, versionID string) ModelVersion {
	t.Helper()
	if _, err := h.svc.PromoteVersion(context.Background(), testActor, PromoteVersionInput{VersionID: versionID, TargetStage: StageStaging}); err != nil {
		t.Fatalf("promote→STAGING: %v", err)
	}
	res, err := h.svc.PromoteVersion(context.Background(), testActor, PromoteVersionInput{VersionID: versionID, TargetStage: StageProduction})
	if err != nil {
		t.Fatalf("promote→PRODUCTION: %v", err)
	}
	return res.Promoted
}

func TestPromoteVersion_SingleProductionInvariant(t *testing.T) {
	h := newHarness()
	m := h.mustRegister(t, "m")
	vA := h.readyVersion(t, m.ID)
	vB := h.readyVersion(t, m.ID)

	// Promote A to PRODUCTION (no prior prod → nothing demoted).
	h.promoteToProd(t, vA.ID)
	if got, _ := h.write.GetVersion(context.Background(), vA.ID); got.Stage != StageProduction {
		t.Fatalf("A should be PRODUCTION, got %v", got.Stage)
	}

	// Now promote B to PRODUCTION. The invariant: A must be ATOMICALLY demoted to
	// ARCHIVED, and the result must report B promoted + A demoted.
	if _, err := h.svc.PromoteVersion(context.Background(), testActor, PromoteVersionInput{VersionID: vB.ID, TargetStage: StageStaging}); err != nil {
		t.Fatalf("B→STAGING: %v", err)
	}
	res, err := h.svc.PromoteVersion(context.Background(), testActor, PromoteVersionInput{VersionID: vB.ID, TargetStage: StageProduction})
	if err != nil {
		t.Fatalf("B→PRODUCTION: %v", err)
	}
	if res.Promoted.ID != vB.ID || res.Promoted.Stage != StageProduction {
		t.Errorf("B not reported PRODUCTION: %+v", res.Promoted)
	}
	if res.Demoted.ID != vA.ID || res.Demoted.Stage != StageArchived {
		t.Errorf("A not reported demoted→ARCHIVED: %+v", res.Demoted)
	}
	// Assert the ACTUAL store state, not just the returned values: exactly ONE
	// production version of the model exists, and it is B.
	prodCount := 0
	for _, v := range h.write.versions {
		if v.ModelID == m.ID && v.Stage == StageProduction {
			prodCount++
		}
	}
	if prodCount != 1 {
		t.Fatalf("single-production invariant violated: %d production versions", prodCount)
	}
	if storedA, _ := h.write.GetVersion(context.Background(), vA.ID); storedA.Stage != StageArchived {
		t.Errorf("A not actually ARCHIVED in store: %v", storedA.Stage)
	}
	// The emitted ModelPromoted event must carry the demoted version + the transition.
	ev := h.emitter.last()
	if ev.Kind != EventModelPromoted || ev.DemotedVersion.ID != vA.ID || ev.ToStage != StageProduction || ev.FromStage != StageStaging {
		t.Errorf("ModelPromoted event payload wrong: %+v", ev)
	}
}

func TestPromoteVersion_IllegalTransitionRejected(t *testing.T) {
	h := newHarness()
	m := h.mustRegister(t, "m")
	v := h.readyVersion(t, m.ID) // currently DEV
	// DEV → PRODUCTION skips STAGING → must be rejected, no swap, no event.
	before := len(h.emitter.events)
	_, err := h.svc.PromoteVersion(context.Background(), testActor, PromoteVersionInput{VersionID: v.ID, TargetStage: StageProduction})
	if !errors.Is(err, ErrIllegalTransition) {
		t.Fatalf("got %v want ErrIllegalTransition", err)
	}
	if got, _ := h.write.GetVersion(context.Background(), v.ID); got.Stage != StageDev {
		t.Errorf("version stage changed on a rejected transition: %v", got.Stage)
	}
	if len(h.emitter.events) != before {
		t.Errorf("rejected transition must emit no event")
	}
}

func TestPromoteVersion_NotReadyRejected(t *testing.T) {
	h := newHarness()
	m := h.mustRegister(t, "m")
	v := h.mustVersion(t, m.ID) // PENDING_UPLOAD, not READY
	_, err := h.svc.PromoteVersion(context.Background(), testActor, PromoteVersionInput{VersionID: v.ID, TargetStage: StageStaging})
	if !errors.Is(err, ErrVersionNotReady) {
		t.Fatalf("got %v want ErrVersionNotReady", err)
	}
}

func TestPromoteVersion_IdempotentToSameStage(t *testing.T) {
	h := newHarness()
	m := h.mustRegister(t, "m")
	v := h.readyVersion(t, m.ID)
	if _, err := h.svc.PromoteVersion(context.Background(), testActor, PromoteVersionInput{VersionID: v.ID, TargetStage: StageStaging}); err != nil {
		t.Fatalf("→STAGING: %v", err)
	}
	before := len(h.emitter.events)
	// Promoting again to the CURRENT stage is a no-op returning current state, no event.
	res, err := h.svc.PromoteVersion(context.Background(), testActor, PromoteVersionInput{VersionID: v.ID, TargetStage: StageStaging})
	if err != nil {
		t.Fatalf("idempotent promote: %v", err)
	}
	if res.Promoted.Stage != StageStaging {
		t.Errorf("stage: got %v want STAGING", res.Promoted.Stage)
	}
	if len(h.emitter.events) != before {
		t.Errorf("no-op promote re-emitted an event")
	}
}

// ============================================================================
// ArchiveModel
// ============================================================================

func TestArchiveModel_SoftDeletesAndArchivesVersions(t *testing.T) {
	h := newHarness()
	m := h.mustRegister(t, "m")
	v := h.mustVersion(t, m.ID)
	got, err := h.svc.ArchiveModel(context.Background(), testActor, m.ID, "")
	if err != nil {
		t.Fatalf("ArchiveModel: %v", err)
	}
	if !got.IsArchived() || !got.ArchivedAt.Equal(h.now) {
		t.Errorf("model not archived at clock time: %+v", got)
	}
	// All versions archived (aggregate consistency boundary).
	if sv, _ := h.write.GetVersion(context.Background(), v.ID); sv.Stage != StageArchived {
		t.Errorf("version not archived with model: %v", sv.Stage)
	}
	ev := h.emitter.last()
	if ev.Kind != EventModelArchived || ev.Model.ID != m.ID {
		t.Errorf("expected EventModelArchived, got %+v", ev)
	}
}

func TestArchiveModel_IdempotentSecondCallNoEvent(t *testing.T) {
	h := newHarness()
	m := h.mustRegister(t, "m")
	if _, err := h.svc.ArchiveModel(context.Background(), testActor, m.ID, ""); err != nil {
		t.Fatalf("first archive: %v", err)
	}
	before := len(h.emitter.events)
	if _, err := h.svc.ArchiveModel(context.Background(), testActor, m.ID, ""); err != nil {
		t.Fatalf("second archive should be a no-op success: %v", err)
	}
	if len(h.emitter.events) != before {
		t.Errorf("archiving an already-archived model re-emitted an event")
	}
}

// ============================================================================
// QUERY-SIDE: page-size clamp + team scoping + projection reads.
// ============================================================================

// recordingReadStore captures the ListOptions/team the service passed, so we can
// assert the page-size clamp and team scoping ACTUALLY reach the read port.
type recordingReadStore struct {
	gotTeam     string
	gotOpts     ListOptions
	gotFilter   ListModelsFilter
	gotStage    ModelStage
	returnModel Model
	returnCount int
	returnErr   error
}

func (r *recordingReadStore) GetModelByID(_ context.Context, team, id string) (Model, error) {
	r.gotTeam = team
	if r.returnErr != nil {
		return Model{}, r.returnErr
	}
	return r.returnModel, nil
}
func (r *recordingReadStore) GetModelByName(_ context.Context, team, name string) (Model, error) {
	r.gotTeam = team
	if r.returnErr != nil {
		return Model{}, r.returnErr
	}
	return r.returnModel, nil
}
func (r *recordingReadStore) ListModels(_ context.Context, team string, f ListModelsFilter, o ListOptions) ([]Model, string, error) {
	r.gotTeam, r.gotFilter, r.gotOpts = team, f, o
	return nil, "", nil
}
func (r *recordingReadStore) CountModels(_ context.Context, team string, f ListModelsFilter) (int, error) {
	r.gotTeam, r.gotFilter = team, f
	return r.returnCount, r.returnErr
}
func (r *recordingReadStore) GetVersionByID(context.Context, string) (ModelVersion, error) {
	return ModelVersion{}, r.returnErr
}
func (r *recordingReadStore) GetVersionByLabel(context.Context, string, string) (ModelVersion, error) {
	return ModelVersion{}, r.returnErr
}
func (r *recordingReadStore) ListVersions(_ context.Context, modelID string, stage ModelStage, o ListOptions) ([]ModelVersion, string, error) {
	r.gotStage, r.gotOpts = stage, o
	return nil, "", nil
}

func newQueryHarness(read ReadStore) RegistryService {
	return NewRegistryService(newFakeWriteStore(), read, &recordingEmitter{}, fixedClock{t: time.Now()}, &seqIDGen{})
}

func TestListModels_ClampsPageSizeAndScopesTeam(t *testing.T) {
	read := &recordingReadStore{}
	svc := newQueryHarness(read)
	// Request a page size FAR over the cap — must be clamped to MaxPageSize.
	if _, err := svc.ListModels(context.Background(), testActor, ListModelsInput{PageSize: 5000}); err != nil {
		t.Fatalf("ListModels: %v", err)
	}
	if read.gotOpts.PageSize != MaxPageSize {
		t.Errorf("page size not clamped: got %d want %d", read.gotOpts.PageSize, MaxPageSize)
	}
	if read.gotTeam != testActor.Team {
		t.Errorf("team not scoped from actor: got %q want %q", read.gotTeam, testActor.Team)
	}
}

func TestListModels_DefaultsPageSizeWhenZero(t *testing.T) {
	read := &recordingReadStore{}
	svc := newQueryHarness(read)
	if _, err := svc.ListModels(context.Background(), testActor, ListModelsInput{PageSize: 0}); err != nil {
		t.Fatalf("ListModels: %v", err)
	}
	if read.gotOpts.PageSize != DefaultPageSize {
		t.Errorf("page size not defaulted: got %d want %d", read.gotOpts.PageSize, DefaultPageSize)
	}
}

func TestGetModel_ProjectionMissMapsToModelNotFound(t *testing.T) {
	read := &recordingReadStore{returnErr: ErrRecordNotFound}
	svc := newQueryHarness(read)
	_, err := svc.GetModel(context.Background(), testActor, "id-x", "")
	if !errors.Is(err, ErrModelNotFound) {
		t.Fatalf("got %v want ErrModelNotFound (storage miss translated)", err)
	}
}

func TestGetModel_RequiresIDOrName(t *testing.T) {
	read := &recordingReadStore{}
	svc := newQueryHarness(read)
	_, err := svc.GetModel(context.Background(), testActor, "", "")
	if !errors.Is(err, ErrValidation) {
		t.Fatalf("got %v want ErrValidation when neither id nor name provided", err)
	}
}

func TestListVersions_ClampsAndPassesStageFilter(t *testing.T) {
	read := &recordingReadStore{}
	svc := newQueryHarness(read)
	if _, err := svc.ListVersions(context.Background(), testActor, "m-1", StageProduction, 9999, ""); err != nil {
		t.Fatalf("ListVersions: %v", err)
	}
	if read.gotOpts.PageSize != MaxPageSize {
		t.Errorf("page size not clamped: %d", read.gotOpts.PageSize)
	}
	if read.gotStage != StageProduction {
		t.Errorf("stage filter not passed through: %v", read.gotStage)
	}
}

// ============================================================================
// Production IDGenerator — assert it really produces distinct, well-formed v4 UUIDs.
// (The service tests use a deterministic seqIDGen; this guards the real one.)
// ============================================================================

func TestUUIDGenerator_ProducesDistinctV4(t *testing.T) {
	g := NewUUIDGenerator()
	seen := map[string]bool{}
	for i := 0; i < 1000; i++ {
		id := g.NewID()
		if len(id) != 36 {
			t.Fatalf("uuid wrong length: %q", id)
		}
		// 8-4-4-4-12 dash layout.
		if id[8] != '-' || id[13] != '-' || id[18] != '-' || id[23] != '-' {
			t.Fatalf("uuid wrong dash layout: %q", id)
		}
		// Version nibble must be '4' (random UUID); variant nibble must be 8/9/a/b.
		if id[14] != '4' {
			t.Fatalf("uuid not version 4: %q", id)
		}
		switch id[19] {
		case '8', '9', 'a', 'b':
		default:
			t.Fatalf("uuid not RFC-4122 variant: %q", id)
		}
		if seen[id] {
			t.Fatalf("uuid collision at iteration %d: %q", i, id)
		}
		seen[id] = true
	}
}

// ============================================================================
// FINDING 1 — MUTATION idempotency by KEY (the proto's exactly-once contract).
//
// State-based idempotency ("already READY / already in stage / already archived")
// only dedupes a retry that asks for the SAME outcome. These tests prove the
// stronger KEY contract: a retry bearing the SAME key but a DIFFERENT intent is
// collapsed to the first outcome, and a keyed retry re-fires NO side effect (event).
// ============================================================================

// MarkVersionReady: a retry with the same key but a CONFLICTING intent (first call
// set FAILED; the buggy retry asserts Success) must return the recorded FAILED
// version unchanged and emit nothing — the key, not the asserted Success, decides.
func TestMarkVersionReady_IdempotencyKeyDedupesConflictingIntent(t *testing.T) {
	h := newHarness()
	m := h.mustRegister(t, "m")
	v := h.mustVersion(t, m.ID)

	// First attempt FAILS the upload (Success=false → status FAILED, no event).
	first, err := h.svc.MarkVersionReady(context.Background(), testActor, MarkVersionReadyInput{
		VersionID: v.ID, Success: false, IdempotencyKey: "confirm-1",
	})
	if err != nil {
		t.Fatalf("first confirm(fail): %v", err)
	}
	if first.Status != StatusFailed {
		t.Fatalf("first outcome: got %v want FAILED", first.Status)
	}
	eventsAfterFirst := len(h.emitter.events)

	// Retry with the SAME key but now asserting Success=true and artifact facts. The
	// KEY must win: we get back the recorded FAILED version, NOT a new READY one, and
	// NO ModelVersionReady event fires (which would have double-metered storage).
	got, err := h.svc.MarkVersionReady(context.Background(), testActor, MarkVersionReadyInput{
		VersionID: v.ID, Success: true, ArtifactDigest: "sha256:forged", SizeBytes: 999, IdempotencyKey: "confirm-1",
	})
	if err != nil {
		t.Fatalf("keyed retry should succeed as a no-op, got %v", err)
	}
	if got.Status != StatusFailed {
		t.Errorf("key did not dedupe conflicting intent: status %v want FAILED (the recorded outcome)", got.Status)
	}
	if got.ArtifactDigest != "" || got.SizeBytes != 0 {
		t.Errorf("retry leaked client-asserted artifact facts past the idempotency gate: %+v", got)
	}
	if len(h.emitter.events) != eventsAfterFirst {
		t.Errorf("keyed retry emitted an event: now %d want %d", len(h.emitter.events), eventsAfterFirst)
	}
	// And the stored truth is still FAILED — the retry performed no second write.
	if sv, _ := h.write.GetVersion(context.Background(), v.ID); sv.Status != StatusFailed {
		t.Errorf("stored version mutated by a deduped retry: %v", sv.Status)
	}
}

// MarkVersionReady (Success path): a keyed retry returns the READY version and emits
// NO second ModelVersionReady — proving the key, not just terminal state, suppresses
// the duplicate event (a duplicate webhook delivery cannot double-fire the event).
func TestMarkVersionReady_IdempotencyKeyOnSuccessNoSecondEvent(t *testing.T) {
	h := newHarness()
	m := h.mustRegister(t, "m")
	v := h.mustVersion(t, m.ID)
	in := MarkVersionReadyInput{VersionID: v.ID, Success: true, ArtifactDigest: "sha256:ok", SizeBytes: 7, IdempotencyKey: "confirm-ok"}
	if _, err := h.svc.MarkVersionReady(context.Background(), testActor, in); err != nil {
		t.Fatalf("first ready: %v", err)
	}
	eventsAfterFirst := len(h.emitter.events)
	got, err := h.svc.MarkVersionReady(context.Background(), testActor, in)
	if err != nil {
		t.Fatalf("keyed ready retry: %v", err)
	}
	if got.Status != StatusReady || got.SizeBytes != 7 {
		t.Errorf("keyed retry returned wrong state: %+v", got)
	}
	if len(h.emitter.events) != eventsAfterFirst {
		t.Errorf("keyed ready retry re-emitted: now %d", len(h.emitter.events))
	}
}

// PromoteVersion: a retry with the same key but a DIFFERENT target stage (the
// dangerous race the finding calls out: a retried promote-to-STAGING that now races
// a promote-to-PRODUCTION) must collapse to the FIRST outcome, not execute the
// second transition. This is exactly what state-based idempotency CANNOT catch.
func TestPromoteVersion_IdempotencyKeyDedupesDifferentTargetStage(t *testing.T) {
	h := newHarness()
	m := h.mustRegister(t, "m")
	v := h.readyVersion(t, m.ID) // DEV, READY

	// First promote → STAGING with key "promo-1".
	first, err := h.svc.PromoteVersion(context.Background(), testActor, PromoteVersionInput{
		VersionID: v.ID, TargetStage: StageStaging, IdempotencyKey: "promo-1",
	})
	if err != nil {
		t.Fatalf("first promote→STAGING: %v", err)
	}
	if first.Promoted.Stage != StageStaging {
		t.Fatalf("first outcome stage: got %v want STAGING", first.Promoted.Stage)
	}
	eventsAfterFirst := len(h.emitter.events)

	// Retry with the SAME key but TargetStage=PRODUCTION. The key must short-circuit
	// to the recorded STAGING version — the production transition must NOT run, no
	// second event, no serving-reload/billing side effect re-fired.
	got, err := h.svc.PromoteVersion(context.Background(), testActor, PromoteVersionInput{
		VersionID: v.ID, TargetStage: StageProduction, IdempotencyKey: "promo-1",
	})
	if err != nil {
		t.Fatalf("keyed retry to a different stage should be a no-op success, got %v", err)
	}
	if got.Promoted.Stage != StageStaging {
		t.Errorf("key did not dedupe a different-target retry: stage %v want STAGING (recorded outcome)", got.Promoted.Stage)
	}
	if len(h.emitter.events) != eventsAfterFirst {
		t.Errorf("keyed retry emitted a second promote event: now %d", len(h.emitter.events))
	}
	if sv, _ := h.write.GetVersion(context.Background(), v.ID); sv.Stage != StageStaging {
		t.Errorf("stored version advanced past the idempotency gate: %v", sv.Stage)
	}
}

// ArchiveModel: a keyed retry re-emits NO ModelArchived event. This proves KEY-based
// dedup specifically (the existing TestArchiveModel_IdempotentSecondCallNoEvent
// proves only the keyless state-based path; here both calls carry the same key).
func TestArchiveModel_IdempotencyKeyNoSecondEvent(t *testing.T) {
	h := newHarness()
	m := h.mustRegister(t, "m")
	first, err := h.svc.ArchiveModel(context.Background(), testActor, m.ID, "del-1")
	if err != nil {
		t.Fatalf("first archive: %v", err)
	}
	if !first.IsArchived() {
		t.Fatalf("first archive did not archive: %+v", first)
	}
	before := len(h.emitter.events)
	got, err := h.svc.ArchiveModel(context.Background(), testActor, m.ID, "del-1")
	if err != nil {
		t.Fatalf("keyed archive retry: %v", err)
	}
	if !got.IsArchived() {
		t.Errorf("keyed retry returned an un-archived model: %+v", got)
	}
	if len(h.emitter.events) != before {
		t.Errorf("keyed archive retry re-emitted an event: now %d", len(h.emitter.events))
	}
}

// The mutation ledger is SCOPED by command: the same key string used for two
// DIFFERENT commands must not cross-dedupe. Proves the (team, command, key) shape,
// not just (team, key).
func TestMutationIdempotency_KeyScopedPerCommand(t *testing.T) {
	h := newHarness()
	m := h.mustRegister(t, "m")
	v := h.readyVersion(t, m.ID)

	// Use the literal key "shared" for a PROMOTE.
	if _, err := h.svc.PromoteVersion(context.Background(), testActor, PromoteVersionInput{
		VersionID: v.ID, TargetStage: StageStaging, IdempotencyKey: "shared",
	}); err != nil {
		t.Fatalf("promote with shared key: %v", err)
	}
	// Now use the SAME literal key "shared" for an ARCHIVE. It must NOT be treated as
	// a duplicate of the promote — different command namespace → it really archives.
	got, err := h.svc.ArchiveModel(context.Background(), testActor, m.ID, "shared")
	if err != nil {
		t.Fatalf("archive with shared key: %v", err)
	}
	if !got.IsArchived() {
		t.Errorf("a key reused across commands cross-deduped: archive was suppressed")
	}
}

// ============================================================================
// FINDING 2 — collision-safe auto-version labeling.
//
// An UNPINNED caller must never receive ErrVersionExists for a label they did not
// choose; the service transparently walks to the next free label. A PINNED caller
// still gets ErrVersionExists (their explicit choice collided).
// ============================================================================

// Pinned-then-auto collision: a client pins "2" while only 1 version exists; a later
// UNPINNED create computes count+1 == "2" and would collide. The service must retry
// past "2" and hand back "3" — NOT ErrVersionExists, which the unpinned caller could
// neither understand nor recover from.
func TestCreateVersion_PinnedThenAutoCollisionRetriesToNextFreeLabel(t *testing.T) {
	h := newHarness()
	m := h.mustRegister(t, "m")

	// Pin "2" as the only version (count is now 1, but the label "2" is taken).
	if _, err := h.svc.CreateVersion(context.Background(), testActor, CreateVersionInput{ModelID: m.ID, Version: "2"}); err != nil {
		t.Fatalf("pin v2: %v", err)
	}
	// Unpinned create: seed label = count+1 = "2" → collides with the pinned "2".
	// The collision-safe loop must bump to "3" and succeed.
	got, err := h.svc.CreateVersion(context.Background(), testActor, CreateVersionInput{ModelID: m.ID})
	if err != nil {
		t.Fatalf("unpinned create after pinned collision must succeed, got %v", err)
	}
	if got.Version != "3" {
		t.Errorf("auto-label did not skip the pinned collision: got %q want \"3\"", got.Version)
	}
	// And exactly two versions exist now, with labels {"2","3"} — no duplicate.
	if n, _ := h.write.CountVersions(context.Background(), m.ID); n != 2 {
		t.Errorf("version count: got %d want 2", n)
	}
}

// A PINNED collision still surfaces ErrVersionExists (the caller's explicit choice
// is taken) — proving the retry loop fires ONLY for auto-assigned labels.
func TestCreateVersion_PinnedCollisionStillErrVersionExists(t *testing.T) {
	h := newHarness()
	m := h.mustRegister(t, "m")
	if _, err := h.svc.CreateVersion(context.Background(), testActor, CreateVersionInput{ModelID: m.ID, Version: "rc1"}); err != nil {
		t.Fatalf("pin rc1: %v", err)
	}
	_, err := h.svc.CreateVersion(context.Background(), testActor, CreateVersionInput{ModelID: m.ID, Version: "rc1"})
	if !errors.Is(err, ErrVersionExists) {
		t.Fatalf("pinned collision: got %v want ErrVersionExists", err)
	}
}

// Concurrent unpinned creates (TOCTOU): two creates both seed the SAME label because
// they read the same count. We simulate the loser's race by having the FIRST insert
// of the second create collide with a row a "concurrent" writer slipped in at the
// same label. The retry loop must converge the loser onto the next free label —
// never ErrVersionExists. This reproduces the exact race the finding describes.
func TestCreateVersion_ConcurrentUnpinnedCreatesNeverErrVersionExists(t *testing.T) {
	h := newHarness()
	m := h.mustRegister(t, "m")

	// First unpinned create lands "1" normally.
	if _, err := h.svc.CreateVersion(context.Background(), testActor, CreateVersionInput{ModelID: m.ID}); err != nil {
		t.Fatalf("v1: %v", err)
	}

	// Arm a one-shot racer: on the NEXT CreateVersion attempt for label "2", a
	// concurrent writer inserts "2" first, forcing our insert to lose the race. The
	// hook disarms itself after firing once so the retry (label "3") can succeed.
	racer := func() func(v ModelVersion) error {
		fired := false
		return func(v ModelVersion) error {
			if !fired && v.Version == "2" {
				fired = true
				// Slip a concurrent row in at label "2" using a distinct id.
				h.write.versions["concurrent-2"] = ModelVersion{ID: "concurrent-2", ModelID: m.ID, Version: "2", Stage: StageDev, Status: StatusPendingUpload}
			}
			return nil
		}
	}()
	h.write.createVersionHook = racer

	// This unpinned create seeds "2" (count+1), loses the race to the concurrent "2",
	// and MUST retry to "3" rather than surface ErrVersionExists.
	got, err := h.svc.CreateVersion(context.Background(), testActor, CreateVersionInput{ModelID: m.ID})
	if err != nil {
		t.Fatalf("concurrent unpinned create must converge, got %v", err)
	}
	if got.Version != "3" {
		t.Errorf("loser of the race did not advance to the next free label: got %q want \"3\"", got.Version)
	}
	if errors.Is(err, ErrVersionExists) {
		t.Errorf("an unpinned caller saw ErrVersionExists — the collision fix regressed")
	}
}
