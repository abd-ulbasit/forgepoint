// projection_description_test.go — UNIT test for BUG C: a registered model's
// description must round-trip into the Redis read model (it was coming back empty).
//
// ============================================================================
// WHY A UNIT TEST (no testcontainers)
// ============================================================================
//
// The existing projection_test.go drives the full loop over REAL Postgres+Redis+NATS
// (testcontainers) and is SKIPPED without Docker. This test instead drives the EXACT
// production decode→hydrate→upsert code path with in-memory fakes:
//
//	build a fp.models.registered EventEnvelope (protojson payload, the publisher's
//	dialect) ─► Projection.HandlerFor(subject) ─► decode + hydrateModel(read-back from
//	the in-memory ModelReader) ─► UpsertModel into the in-memory ProjectionWriter.
//
// The assertion is that the upserted (projected) model carries the DESCRIPTION (and
// tags) the write-store truth holds — even though the thin ModelRegistered event does
// NOT carry them. That is exactly the field-completeness BUG C demanded, proven without
// any container.
package events_test

import (
	"context"
	"testing"
	"time"

	eventsv1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/events/v1"
	"github.com/abd-ulbasit/forgepoint/pkg/natsutil"
	"github.com/abd-ulbasit/forgepoint/services/registry/internal/domain"
	"github.com/abd-ulbasit/forgepoint/services/registry/internal/events"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// memProjectionWriter is an in-memory events.ProjectionWriter: it records the models
// the projection upserts so the test can assert the projected field set. It also
// satisfies the unscoped-load methods the projection needs (returning not-found until
// an entity has been upserted), so handlers that load-merge work.
type memProjectionWriter struct {
	models   map[string]domain.Model
	versions map[string]domain.ModelVersion
}

func newMemProjectionWriter() *memProjectionWriter {
	return &memProjectionWriter{models: map[string]domain.Model{}, versions: map[string]domain.ModelVersion{}}
}

func (w *memProjectionWriter) UpsertModel(_ context.Context, m domain.Model) error {
	w.models[m.ID] = m
	return nil
}
func (w *memProjectionWriter) UpsertVersion(_ context.Context, v domain.ModelVersion) error {
	w.versions[v.ID] = v
	return nil
}
func (w *memProjectionWriter) GetModelByIDUnscoped(_ context.Context, id string) (domain.Model, error) {
	m, ok := w.models[id]
	if !ok {
		return domain.Model{}, domain.ErrRecordNotFound
	}
	return m, nil
}
func (w *memProjectionWriter) GetVersionByID(_ context.Context, id string) (domain.ModelVersion, error) {
	v, ok := w.versions[id]
	if !ok {
		return domain.ModelVersion{}, domain.ErrRecordNotFound
	}
	return v, nil
}

// memModelReader is an in-memory events.ModelReader — the write-store "truth" the
// projection reads back to hydrate the fields the thin event omits.
type memModelReader struct{ models map[string]domain.Model }

func (r memModelReader) GetModel(_ context.Context, id string) (domain.Model, error) {
	m, ok := r.models[id]
	if !ok {
		return domain.Model{}, domain.ErrRecordNotFound
	}
	return m, nil
}

// registeredEnvelope builds a fp.models.registered EventEnvelope whose Data is the
// protojson-encoded ModelRegistered — symmetric with natsutil.Publisher's encoding, so
// the projection's protojson decode is exercised exactly as in production.
func registeredEnvelope(t *testing.T, ev *eventsv1.ModelRegistered) natsutil.EventEnvelope {
	t.Helper()
	data, err := protojson.Marshal(ev)
	if err != nil {
		t.Fatalf("marshal ModelRegistered: %v", err)
	}
	return natsutil.EventEnvelope{
		ID:     "env-1",
		Type:   "registered",
		Source: events.ServiceName,
		Data:   data,
	}
}

// TestProjection_RegisteredHydratesDescriptionFromTruth is the BUG C proof: the thin
// ModelRegistered event has NO description/tags, but the projected read model still
// gets them — read back from the write-store truth via the ModelReader.
func TestProjection_RegisteredHydratesDescriptionFromTruth(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	store := newMemProjectionWriter()

	// The write-store truth: a FIELD-COMPLETE model (the description the SPA/BFF sent,
	// which RegisterModel persisted to Postgres). This is what GET /models/{id} must
	// eventually return.
	truth := memModelReader{models: map[string]domain.Model{
		"model-1": {
			ID:          "model-1",
			Name:        "fraud-detector",
			Description: "detects card fraud in real time",
			Team:        "team-a",
			OwnerID:     "alice",
			Framework:   "onnx",
			TaskType:    "classification",
			Tags:        map[string]string{"domain": "fraud"},
			CreatedAt:   now,
			UpdatedAt:   now,
		},
	}}

	proj := events.NewProjection(nil, store, truth, events.ProjectionConfig{})
	handler := proj.HandlerFor(events.SubjectModelRegistered)
	if handler == nil {
		t.Fatal("no handler for SubjectModelRegistered")
	}

	// The event itself is THIN — no description, no tags (the proto can't carry them).
	env := registeredEnvelope(t, &eventsv1.ModelRegistered{
		ModelId:      "model-1",
		ModelName:    "fraud-detector",
		Framework:    "onnx",
		TaskType:     "classification",
		OwnerId:      "alice",
		Team:         "team-a",
		RegisteredAt: timestamppb.New(now),
	})

	if err := handler(context.Background(), env); err != nil {
		t.Fatalf("handleModelRegistered: %v", err)
	}

	// The projected model MUST carry the description (and tags) from the truth — the
	// fix. Pre-fix, projected.Description would be "" and the dashboard/detail screen
	// showed an empty description.
	projected, ok := store.models["model-1"]
	if !ok {
		t.Fatal("model was not projected into the read model")
	}
	if projected.Description != "detects card fraud in real time" {
		t.Errorf("description dropped in projection: got %q want the truth's value", projected.Description)
	}
	if projected.Tags["domain"] != "fraud" {
		t.Errorf("tags dropped in projection: got %v", projected.Tags)
	}
	// And the event-authoritative fields are still correct (identity/ownership).
	if projected.ID != "model-1" || projected.Team != "team-a" || projected.Framework != "onnx" {
		t.Errorf("event fields wrong after hydrate: %+v", projected)
	}
}

// TestProjection_RegisteredFallsBackWhenNoTruth proves the nil-ModelReader fallback:
// with no read-back source, the projection still projects the model from the event's
// own fields (it does not crash or drop the model) — just without the enrichment.
func TestProjection_RegisteredFallsBackWhenNoTruth(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	store := newMemProjectionWriter()

	proj := events.NewProjection(nil, store, nil, events.ProjectionConfig{}) // nil truth
	handler := proj.HandlerFor(events.SubjectModelRegistered)

	env := registeredEnvelope(t, &eventsv1.ModelRegistered{
		ModelId:      "model-2",
		ModelName:    "spam",
		Team:         "team-a",
		RegisteredAt: timestamppb.New(now),
	})
	if err := handler(context.Background(), env); err != nil {
		t.Fatalf("handleModelRegistered (no truth): %v", err)
	}
	projected, ok := store.models["model-2"]
	if !ok {
		t.Fatal("model not projected without a ModelReader")
	}
	if projected.Name != "spam" || projected.Team != "team-a" {
		t.Errorf("event-derived projection wrong: %+v", projected)
	}
}
