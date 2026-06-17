// write_store_test.go — integration tests for the Postgres WriteStore adapter.
//
// ============================================================================
// THESE TESTS RUN AGAINST A REAL POSTGRES (testcontainers), NOT A MOCK
// ============================================================================
//
// The whole point of the persistence layer is behavior a mock cannot reproduce:
// the (team,name) unique index actually rejecting a duplicate, the partial
// single-production index actually forbidding two prod rows, JSONB tags actually
// round-tripping, a transaction actually rolling BOTH writes back on failure. So
// every test here drives the adapter against a real postgres:17 container started by
// pkg/testutil.StartPostgres and asserts the OBSERVABLE database behavior.
//
// SETUP per test: StartPostgres → a fresh DB → ApplyUp (the embedded .up.sql) → a
// WriteStore over the same pool. Each test gets an ISOLATED container (no shared
// state), so tests can run in any order / in parallel without cross-contamination.
package postgres

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/abd-ulbasit/forgepoint/pkg/testutil"
	"github.com/abd-ulbasit/forgepoint/services/registry/internal/domain"
	"github.com/abd-ulbasit/forgepoint/services/registry/migrations"
)

// newTestStore spins up Postgres, applies migrations, and returns a ready WriteStore
// plus the pool (so a test can poke the raw DB for assertions the port doesn't expose).
// testutil.StartPostgres calls SkipIfNoDocker internally and registers container
// cleanup via t.Cleanup, so the test skips cleanly when Docker is absent.
func newTestStore(t *testing.T) (*WriteStore, *pgxpool.Pool) {
	t.Helper()
	testutil.SkipIfNoDocker(t)

	ctx := context.Background()
	dsn := testutil.StartPostgres(t)

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("create pool: %v", err)
	}
	t.Cleanup(pool.Close)

	if err := migrations.ApplyUp(ctx, pool); err != nil {
		t.Fatalf("apply migrations: %v", err)
	}
	return NewWriteStoreFromPool(pool), pool
}

// sampleModel builds a fully-stamped model the way the service would (server-auth fields
// already populated). Tests tweak fields as needed.
func sampleModel(id, team, name string) domain.Model {
	now := time.Now().UTC().Truncate(time.Microsecond) // Postgres TIMESTAMPTZ is microsecond precision
	return domain.Model{
		ID:          id,
		Name:        name,
		Description: "a model",
		OwnerID:     "user-1",
		Team:        team,
		Framework:   "pytorch",
		TaskType:    "classification",
		Tags:        map[string]string{"env": "prod", "team": "ml"},
		CreatedAt:   now,
		UpdatedAt:   now,
	}
}

// sampleVersion builds a freshly-created version (Stage=DEV, Status=PENDING_UPLOAD).
func sampleVersion(id, modelID, label string) domain.ModelVersion {
	now := time.Now().UTC().Truncate(time.Microsecond)
	return domain.ModelVersion{
		ID:        id,
		ModelID:   modelID,
		Version:   label,
		Metrics:   map[string]float64{"accuracy": 0.97},
		Stage:     domain.StageDev,
		Status:    domain.StatusPendingUpload,
		CreatedBy: "user-1",
		CreatedAt: now,
	}
}

// ----------------------------------------------------------------------------
// MODELS: create / read-back / uniqueness / idempotency
// ----------------------------------------------------------------------------

func TestCreateModel_RoundTrips(t *testing.T) {
	store, _ := newTestStore(t)
	ctx := context.Background()

	want := sampleModel("m-1", "acme", "fraud-detector")
	got, err := store.CreateModel(ctx, want, "")
	if err != nil {
		t.Fatalf("CreateModel: %v", err)
	}
	if got.ID != want.ID {
		t.Fatalf("returned id = %q, want %q", got.ID, want.ID)
	}

	// Read it back through GetModel and assert EVERY field round-tripped — including the
	// JSONB tags map, which a mock would never exercise.
	back, err := store.GetModel(ctx, "m-1")
	if err != nil {
		t.Fatalf("GetModel: %v", err)
	}
	if back.Name != "fraud-detector" || back.Team != "acme" || back.OwnerID != "user-1" {
		t.Fatalf("identity mismatch: %+v", back)
	}
	if back.Framework != "pytorch" || back.TaskType != "classification" {
		t.Fatalf("metadata mismatch: %+v", back)
	}
	if len(back.Tags) != 2 || back.Tags["env"] != "prod" || back.Tags["team"] != "ml" {
		t.Fatalf("tags JSONB did not round-trip: %#v", back.Tags)
	}
	if back.IsArchived() {
		t.Fatalf("new model should not be archived")
	}
	if !back.CreatedAt.Equal(want.CreatedAt) {
		t.Fatalf("created_at = %v, want %v", back.CreatedAt, want.CreatedAt)
	}
}

func TestCreateModel_DuplicateTeamName_IsWriteConflict(t *testing.T) {
	store, _ := newTestStore(t)
	ctx := context.Background()

	if _, err := store.CreateModel(ctx, sampleModel("m-1", "acme", "dup"), ""); err != nil {
		t.Fatalf("first create: %v", err)
	}
	// Same (team, name), different id → the models_team_name_uniq index must reject it as
	// a unique violation, surfaced as domain.ErrWriteConflict (which the service maps to
	// ErrModelNameTaken). This proves the DB constraint, not just app logic, guards it.
	_, err := store.CreateModel(ctx, sampleModel("m-2", "acme", "dup"), "")
	if !errors.Is(err, domain.ErrWriteConflict) {
		t.Fatalf("duplicate (team,name): got %v, want ErrWriteConflict", err)
	}

	// A DIFFERENT team may reuse the name — uniqueness is per-team, not global.
	if _, err := store.CreateModel(ctx, sampleModel("m-3", "other", "dup"), ""); err != nil {
		t.Fatalf("different team same name should succeed: %v", err)
	}
}

func TestModelCreateIdempotency(t *testing.T) {
	store, _ := newTestStore(t)
	ctx := context.Background()

	m := sampleModel("m-1", "acme", "fraud")
	if _, err := store.CreateModel(ctx, m, "key-123"); err != nil {
		t.Fatalf("create with key: %v", err)
	}

	// LookupModelByIdempotencyKey returns the ORIGINAL row for the same (team,key).
	found, err := store.LookupModelByIdempotencyKey(ctx, "acme", "key-123")
	if err != nil {
		t.Fatalf("lookup by key: %v", err)
	}
	if found.ID != "m-1" {
		t.Fatalf("idempotency lookup returned id %q, want m-1", found.ID)
	}

	// An UNKNOWN key → ErrRecordNotFound (the service then proceeds to create).
	if _, err := store.LookupModelByIdempotencyKey(ctx, "acme", "nope"); !errors.Is(err, domain.ErrRecordNotFound) {
		t.Fatalf("unknown key: got %v, want ErrRecordNotFound", err)
	}
	// An EMPTY key never matches (caller opted out), without even hitting the DB.
	if _, err := store.LookupModelByIdempotencyKey(ctx, "acme", ""); !errors.Is(err, domain.ErrRecordNotFound) {
		t.Fatalf("empty key: got %v, want ErrRecordNotFound", err)
	}
	// Reusing the SAME key for a DIFFERENT model in the team is a unique violation
	// (the partial idempotency index) → ErrWriteConflict.
	if _, err := store.CreateModel(ctx, sampleModel("m-2", "acme", "other"), "key-123"); !errors.Is(err, domain.ErrWriteConflict) {
		t.Fatalf("reused key different model: got %v, want ErrWriteConflict", err)
	}
}

func TestGetModel_NotFound(t *testing.T) {
	store, _ := newTestStore(t)
	if _, err := store.GetModel(context.Background(), "ghost"); !errors.Is(err, domain.ErrRecordNotFound) {
		t.Fatalf("get missing model: got %v, want ErrRecordNotFound", err)
	}
}

func TestUpdateModel_PersistsEditsAndArchive(t *testing.T) {
	store, _ := newTestStore(t)
	ctx := context.Background()

	m := sampleModel("m-1", "acme", "fraud")
	if _, err := store.CreateModel(ctx, m, ""); err != nil {
		t.Fatalf("create: %v", err)
	}

	// Edit description + replace tags + stamp archived_at (the soft-delete path used by
	// UpdateModel for non-aggregate edits). Assert each persists.
	m.Description = "updated desc"
	m.Tags = map[string]string{"only": "one"}
	m.ArchivedAt = time.Now().UTC().Truncate(time.Microsecond)
	m.UpdatedAt = m.ArchivedAt
	if _, err := store.UpdateModel(ctx, m); err != nil {
		t.Fatalf("update: %v", err)
	}

	back, err := store.GetModel(ctx, "m-1")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if back.Description != "updated desc" {
		t.Fatalf("description not updated: %q", back.Description)
	}
	if len(back.Tags) != 1 || back.Tags["only"] != "one" {
		t.Fatalf("tags not replaced: %#v", back.Tags)
	}
	if !back.IsArchived() {
		t.Fatalf("archived_at not persisted")
	}
}

func TestUpdateModel_MissingRow_NotFound(t *testing.T) {
	store, _ := newTestStore(t)
	m := sampleModel("ghost", "acme", "nope")
	if _, err := store.UpdateModel(context.Background(), m); !errors.Is(err, domain.ErrRecordNotFound) {
		t.Fatalf("update missing: got %v, want ErrRecordNotFound", err)
	}
}

// ----------------------------------------------------------------------------
// VERSIONS: create / fk / uniqueness / count / status / idempotency
// ----------------------------------------------------------------------------

func TestCreateVersion_RoundTripsAndCounts(t *testing.T) {
	store, _ := newTestStore(t)
	ctx := context.Background()
	mustCreateModel(t, store, sampleModel("m-1", "acme", "fraud"))

	v := sampleVersion("v-1", "m-1", "1")
	if _, err := store.CreateVersion(ctx, v, ""); err != nil {
		t.Fatalf("create version: %v", err)
	}

	back, err := store.GetVersion(ctx, "v-1")
	if err != nil {
		t.Fatalf("get version: %v", err)
	}
	if back.ModelID != "m-1" || back.Version != "1" {
		t.Fatalf("version identity mismatch: %+v", back)
	}
	if back.Stage != domain.StageDev || back.Status != domain.StatusPendingUpload {
		t.Fatalf("new version should be DEV/PENDING_UPLOAD, got %s/%s", back.Stage, back.Status)
	}
	if back.Metrics["accuracy"] != 0.97 {
		t.Fatalf("metrics JSONB did not round-trip: %#v", back.Metrics)
	}

	if n, err := store.CountVersions(ctx, "m-1"); err != nil || n != 1 {
		t.Fatalf("CountVersions = %d, %v; want 1, nil", n, err)
	}
}

func TestCreateVersion_DuplicateLabel_IsWriteConflict(t *testing.T) {
	store, _ := newTestStore(t)
	ctx := context.Background()
	mustCreateModel(t, store, sampleModel("m-1", "acme", "fraud"))

	if _, err := store.CreateVersion(ctx, sampleVersion("v-1", "m-1", "1"), ""); err != nil {
		t.Fatalf("first version: %v", err)
	}
	// Same (model_id, version) → model_versions_model_version_uniq rejects it. This is
	// the conflict the service's collision-safe loop races against.
	if _, err := store.CreateVersion(ctx, sampleVersion("v-2", "m-1", "1"), ""); !errors.Is(err, domain.ErrWriteConflict) {
		t.Fatalf("duplicate label: got %v, want ErrWriteConflict", err)
	}
	// Same label under a DIFFERENT model is fine (labels are per-model).
	mustCreateModel(t, store, sampleModel("m-2", "acme", "fraud2"))
	if _, err := store.CreateVersion(ctx, sampleVersion("v-3", "m-2", "1"), ""); err != nil {
		t.Fatalf("same label different model should succeed: %v", err)
	}
}

func TestCreateVersion_UnknownModel_FKViolation(t *testing.T) {
	store, _ := newTestStore(t)
	// No model m-x exists → the model_id FK rejects the insert. The adapter surfaces it as
	// a generic error (not a unique conflict). We assert it ERRORS (the FK held), which is
	// the real-DB behavior a mock can't give. The service prevents this path by validating
	// the model first, so any FK error here is a genuine integrity backstop.
	_, err := store.CreateVersion(context.Background(), sampleVersion("v-1", "m-x", "1"), "")
	if err == nil {
		t.Fatalf("expected FK violation creating version for missing model")
	}
	if errors.Is(err, domain.ErrWriteConflict) {
		t.Fatalf("FK violation should not map to ErrWriteConflict (that is for unique only): %v", err)
	}
}

func TestVersionCreateIdempotency(t *testing.T) {
	store, _ := newTestStore(t)
	ctx := context.Background()
	mustCreateModel(t, store, sampleModel("m-1", "acme", "fraud"))

	if _, err := store.CreateVersion(ctx, sampleVersion("v-1", "m-1", "1"), "vkey"); err != nil {
		t.Fatalf("create with key: %v", err)
	}
	found, err := store.LookupVersionByIdempotencyKey(ctx, "m-1", "vkey")
	if err != nil || found.ID != "v-1" {
		t.Fatalf("lookup version key = %+v, %v; want v-1", found, err)
	}
	if _, err := store.LookupVersionByIdempotencyKey(ctx, "m-1", "missing"); !errors.Is(err, domain.ErrRecordNotFound) {
		t.Fatalf("unknown version key: got %v, want ErrRecordNotFound", err)
	}
}

func TestUpdateVersionStatus_Ready_RecordsArtifactFacts(t *testing.T) {
	store, _ := newTestStore(t)
	ctx := context.Background()
	mustCreateModel(t, store, sampleModel("m-1", "acme", "fraud"))
	mustCreateVersion(t, store, sampleVersion("v-1", "m-1", "1"))

	// Drive PENDING_UPLOAD → READY with server-measured artifact facts.
	v, _ := store.GetVersion(ctx, "v-1")
	v.Status = domain.StatusReady
	v.ArtifactPath = "s3://bucket/v1"
	v.ArtifactDigest = "sha256:abc"
	v.SizeBytes = 12345
	if _, err := store.UpdateVersionStatus(ctx, v); err != nil {
		t.Fatalf("update status: %v", err)
	}

	back, _ := store.GetVersion(ctx, "v-1")
	if back.Status != domain.StatusReady {
		t.Fatalf("status not READY: %s", back.Status)
	}
	if back.ArtifactPath != "s3://bucket/v1" || back.ArtifactDigest != "sha256:abc" || back.SizeBytes != 12345 {
		t.Fatalf("artifact facts not persisted: %+v", back)
	}
}

// ----------------------------------------------------------------------------
// PROMOTE (single-production invariant) — transaction atomicity + the partial index
// ----------------------------------------------------------------------------

func TestPromoteVersionTx_FirstProduction_NoDemotion(t *testing.T) {
	store, _ := newTestStore(t)
	ctx := context.Background()
	mustCreateModel(t, store, sampleModel("m-1", "acme", "fraud"))
	mustCreateVersion(t, store, readyVersion("v-1", "m-1", "1"))

	// Promote v-1 to STAGING then PRODUCTION (the legal path). First prod: nothing to demote.
	promoted, _ := store.GetVersion(ctx, "v-1")
	promoted.Stage = domain.StageProduction
	newP, newD, err := store.PromoteVersionTx(ctx, promoted, domain.ModelVersion{})
	if err != nil {
		t.Fatalf("promote tx: %v", err)
	}
	if newP.Stage != domain.StageProduction {
		t.Fatalf("promoted stage = %s, want PRODUCTION", newP.Stage)
	}
	if newD.ID != "" {
		t.Fatalf("expected no demoted version, got %s", newD.ID)
	}
	// FindProductionVersion now resolves to v-1.
	prod, err := store.FindProductionVersion(ctx, "m-1")
	if err != nil || prod.ID != "v-1" {
		t.Fatalf("FindProductionVersion = %+v, %v; want v-1", prod, err)
	}
}

func TestPromoteVersionTx_SwapIsAtomic_SingleProductionHolds(t *testing.T) {
	store, _ := newTestStore(t)
	ctx := context.Background()
	mustCreateModel(t, store, sampleModel("m-1", "acme", "fraud"))

	// v-1 is the incumbent PRODUCTION; v-2 is a READY candidate.
	v1 := readyVersion("v-1", "m-1", "1")
	v1.Stage = domain.StageProduction
	mustCreateVersion(t, store, v1)
	mustCreateVersion(t, store, readyVersion("v-2", "m-1", "2"))

	// Promote v-2 → PRODUCTION, atomically demoting v-1 → ARCHIVED.
	promoted, _ := store.GetVersion(ctx, "v-2")
	promoted.Stage = domain.StageProduction
	demoted, _ := store.GetVersion(ctx, "v-1")
	demoted.Stage = domain.StageArchived

	newP, newD, err := store.PromoteVersionTx(ctx, promoted, demoted)
	if err != nil {
		t.Fatalf("swap tx: %v", err)
	}
	if newP.ID != "v-2" || newD.ID != "v-1" {
		t.Fatalf("swap returned wrong pair: promoted=%s demoted=%s", newP.ID, newD.ID)
	}

	// THE INVARIANT: exactly ONE production version, and it is v-2.
	prod, err := store.FindProductionVersion(ctx, "m-1")
	if err != nil || prod.ID != "v-2" {
		t.Fatalf("after swap, production = %+v, %v; want v-2", prod, err)
	}
	v1back, _ := store.GetVersion(ctx, "v-1")
	if v1back.Stage != domain.StageArchived {
		t.Fatalf("incumbent not archived: %s", v1back.Stage)
	}
	// Double-check at the DB level that the partial unique index holds: count prod rows.
	if c := countProductionRows(t, store, "m-1"); c != 1 {
		t.Fatalf("expected exactly 1 production row, got %d", c)
	}
}

func TestPromoteVersionTx_RollsBackOnConflict_NoPartialWrite(t *testing.T) {
	store, _ := newTestStore(t)
	ctx := context.Background()
	mustCreateModel(t, store, sampleModel("m-1", "acme", "fraud"))

	// Incumbent prod v-1; candidate v-2. We construct an ILLEGAL swap that the DB index
	// must reject: promote v-2 to PRODUCTION WITHOUT demoting v-1 (pass a zero demoted).
	// The partial single-production index then rejects the second PRODUCTION row, and the
	// WHOLE tx must roll back — leaving v-2 unchanged (NOT half-promoted).
	v1 := readyVersion("v-1", "m-1", "1")
	v1.Stage = domain.StageProduction
	mustCreateVersion(t, store, v1)
	v2 := readyVersion("v-2", "m-1", "2")
	v2.Stage = domain.StageStaging
	mustCreateVersion(t, store, v2)

	promoted, _ := store.GetVersion(ctx, "v-2")
	promoted.Stage = domain.StageProduction
	// demoted intentionally zero → no demotion → two prod rows → index violation.
	_, _, err := store.PromoteVersionTx(ctx, promoted, domain.ModelVersion{})
	if !errors.Is(err, domain.ErrWriteConflict) {
		t.Fatalf("illegal double-prod swap: got %v, want ErrWriteConflict", err)
	}

	// ATOMICITY ASSERTION: v-2 must still be STAGING (the failed tx left NO partial write),
	// and v-1 must still be the sole PRODUCTION.
	v2back, _ := store.GetVersion(ctx, "v-2")
	if v2back.Stage != domain.StageStaging {
		t.Fatalf("failed tx left a partial write: v-2 stage = %s, want STAGING", v2back.Stage)
	}
	if c := countProductionRows(t, store, "m-1"); c != 1 {
		t.Fatalf("after rollback expected 1 production row, got %d", c)
	}
}

// ----------------------------------------------------------------------------
// ARCHIVE (aggregate cascade in one tx)
// ----------------------------------------------------------------------------

func TestArchiveModelTx_ArchivesModelAndAllVersions(t *testing.T) {
	store, _ := newTestStore(t)
	ctx := context.Background()
	mustCreateModel(t, store, sampleModel("m-1", "acme", "fraud"))
	v1 := readyVersion("v-1", "m-1", "1")
	v1.Stage = domain.StageProduction
	mustCreateVersion(t, store, v1)
	mustCreateVersion(t, store, sampleVersion("v-2", "m-1", "2")) // DEV

	m, _ := store.GetModel(ctx, "m-1")
	m.ArchivedAt = time.Now().UTC().Truncate(time.Microsecond)
	m.UpdatedAt = m.ArchivedAt
	if _, err := store.ArchiveModelTx(ctx, m); err != nil {
		t.Fatalf("archive tx: %v", err)
	}

	back, _ := store.GetModel(ctx, "m-1")
	if !back.IsArchived() {
		t.Fatalf("model not archived")
	}
	if back.ProductionVersion != "" {
		t.Fatalf("archived model should clear production pointer, got %q", back.ProductionVersion)
	}
	// EVERY version is now ARCHIVED (the aggregate cascade).
	for _, id := range []string{"v-1", "v-2"} {
		v, _ := store.GetVersion(ctx, id)
		if v.Stage != domain.StageArchived {
			t.Fatalf("version %s not archived: %s", id, v.Stage)
		}
	}
	// And no PRODUCTION row remains.
	if c := countProductionRows(t, store, "m-1"); c != 0 {
		t.Fatalf("archived model should have 0 production rows, got %d", c)
	}
}

func TestArchiveModelTx_MissingModel_NotFound(t *testing.T) {
	store, _ := newTestStore(t)
	m := sampleModel("ghost", "acme", "nope")
	m.ArchivedAt = time.Now().UTC()
	if _, err := store.ArchiveModelTx(context.Background(), m); !errors.Is(err, domain.ErrRecordNotFound) {
		t.Fatalf("archive missing: got %v, want ErrRecordNotFound", err)
	}
}

// ----------------------------------------------------------------------------
// MUTATION-IDEMPOTENCY LEDGER
// ----------------------------------------------------------------------------

func TestCommandIdempotencyLedger(t *testing.T) {
	store, _ := newTestStore(t)
	ctx := context.Background()

	// Unseen key → not found (the service then proceeds with the mutation).
	if _, err := store.LookupCommandIdempotency(ctx, "acme", domain.CommandPromoteVersion, "k1"); !errors.Is(err, domain.ErrRecordNotFound) {
		t.Fatalf("unseen ledger key: got %v, want ErrRecordNotFound", err)
	}
	// Empty key never matches and never writes.
	if _, err := store.LookupCommandIdempotency(ctx, "acme", domain.CommandPromoteVersion, ""); !errors.Is(err, domain.ErrRecordNotFound) {
		t.Fatalf("empty ledger key: got %v, want ErrRecordNotFound", err)
	}

	// Record k1 → entity v-1, then a lookup returns v-1.
	if err := store.RecordCommandIdempotency(ctx, "acme", domain.CommandPromoteVersion, "k1", "v-1"); err != nil {
		t.Fatalf("record: %v", err)
	}
	got, err := store.LookupCommandIdempotency(ctx, "acme", domain.CommandPromoteVersion, "k1")
	if err != nil || got != "v-1" {
		t.Fatalf("lookup recorded key = %q, %v; want v-1", got, err)
	}

	// RE-recording the SAME (team,command,key)→SAME entity is a benign no-op (DO NOTHING).
	if err := store.RecordCommandIdempotency(ctx, "acme", domain.CommandPromoteVersion, "k1", "v-1"); err != nil {
		t.Fatalf("benign re-record: %v", err)
	}
	// Re-recording the same key for a DIFFERENT entity is a genuine key-reuse race →
	// ErrWriteConflict (the first writer's record stands).
	if err := store.RecordCommandIdempotency(ctx, "acme", domain.CommandPromoteVersion, "k1", "v-2"); !errors.Is(err, domain.ErrWriteConflict) {
		t.Fatalf("key-reuse different entity: got %v, want ErrWriteConflict", err)
	}

	// The SAME key under a DIFFERENT command is independent (command namespaces the key).
	if err := store.RecordCommandIdempotency(ctx, "acme", domain.CommandArchiveModel, "k1", "m-9"); err != nil {
		t.Fatalf("same key different command should be independent: %v", err)
	}
	if got, _ := store.LookupCommandIdempotency(ctx, "acme", domain.CommandArchiveModel, "k1"); got != "m-9" {
		t.Fatalf("archive-command k1 = %q, want m-9", got)
	}
	// And the same key in a DIFFERENT team is independent too (team scoping).
	if _, err := store.LookupCommandIdempotency(ctx, "other-team", domain.CommandPromoteVersion, "k1"); !errors.Is(err, domain.ErrRecordNotFound) {
		t.Fatalf("cross-team ledger leak: got %v, want ErrRecordNotFound", err)
	}
}

// ----------------------------------------------------------------------------
// test helpers (raw-DB assertions + must-create wrappers)
// ----------------------------------------------------------------------------

// readyVersion is a version already READY (so it is promotable to a serving stage).
func readyVersion(id, modelID, label string) domain.ModelVersion {
	v := sampleVersion(id, modelID, label)
	v.Status = domain.StatusReady
	v.ArtifactPath = "s3://bucket/" + id
	v.ArtifactDigest = "sha256:" + id
	v.SizeBytes = 100
	return v
}

func mustCreateModel(t *testing.T, store *WriteStore, m domain.Model) {
	t.Helper()
	if _, err := store.CreateModel(context.Background(), m, ""); err != nil {
		t.Fatalf("seed model %s: %v", m.ID, err)
	}
}

func mustCreateVersion(t *testing.T, store *WriteStore, v domain.ModelVersion) {
	t.Helper()
	if _, err := store.CreateVersion(context.Background(), v, ""); err != nil {
		t.Fatalf("seed version %s: %v", v.ID, err)
	}
}

// countProductionRows reads the raw DB to count PRODUCTION versions of a model — the
// direct evidence the single-production invariant holds (independent of the adapter's
// own query path).
func countProductionRows(t *testing.T, store *WriteStore, modelID string) int {
	t.Helper()
	var n int
	err := store.pool.QueryRow(context.Background(),
		`SELECT count(*) FROM model_versions WHERE model_id = $1 AND stage = $2`,
		modelID, int16(domain.StageProduction)).Scan(&n)
	if err != nil {
		t.Fatalf("count production rows: %v", err)
	}
	return n
}
