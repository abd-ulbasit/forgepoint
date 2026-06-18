// projection_test.go — the BUG 2 PROOF: the CQRS read side is now populated.
//
// ============================================================================
// WHAT THIS PROVES
// ============================================================================
//
// The registry is CQRS: COMMANDS write Postgres + emit fp.models.* events; QUERIES
// read Redis. Before the projection consumer was wired, NOTHING updated Redis from
// those events, so a registered model was invisible to reads — GET /models returned
// []. This test drives the FULL loop over REAL infrastructure (testcontainers:
// Postgres + Redis + NATS JetStream):
//
//	RegistryService.RegisterModel ─► Postgres write (truth) + emit fp.models.registered
//	                                        │ (protojson on the wire)
//	                                        ▼
//	                       events.Projection consumes ─► Redis UpsertModel
//	                                        │
//	ReadStore.GetModelByName / ListModels ◄┘  ← now FINDS the model
//
// The assertion is the read-side query finding the model AFTER the projection
// consumed the event — exactly the behavior that was broken. A second test drives a
// raw fp.models.registered publish (no write side) to isolate the projection itself.
//
// Run with -race (the publish→consume hop crosses goroutines) and SERIALLY (each
// test owns its own containers).
package events_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	goredis "github.com/redis/go-redis/v9"

	eventsv1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/events/v1"
	"github.com/abd-ulbasit/forgepoint/pkg/natsutil"
	"github.com/abd-ulbasit/forgepoint/pkg/testutil"
	"github.com/abd-ulbasit/forgepoint/services/registry/internal/domain"
	"github.com/abd-ulbasit/forgepoint/services/registry/internal/events"
	"github.com/abd-ulbasit/forgepoint/services/registry/internal/repository/postgres"
	redisstore "github.com/abd-ulbasit/forgepoint/services/registry/internal/repository/redis"
	"github.com/abd-ulbasit/forgepoint/services/registry/migrations"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// newRedisReadStore spins up Redis and returns a *redisstore.ReadStore (which
// satisfies both domain.ReadStore for queries and events.ProjectionWriter for the
// projection's upserts).
func newRedisReadStore(t *testing.T) *redisstore.ReadStore {
	t.Helper()
	addr := testutil.StartRedis(t) // returns a redis:// URL; SkipIfNoDocker inside
	opt, err := goredis.ParseURL(addr)
	if err != nil {
		t.Fatalf("parse redis url %q: %v", addr, err)
	}
	rdb := goredis.NewClient(opt)
	t.Cleanup(func() { _ = rdb.Close() })
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		t.Fatalf("ping redis: %v", err)
	}
	return redisstore.NewReadStoreFromClient(rdb)
}

// newWriteStore spins up Postgres, applies migrations, and returns the WriteStore.
func newWriteStore(t *testing.T) *postgres.WriteStore {
	t.Helper()
	dsn := testutil.StartPostgres(t) // SkipIfNoDocker inside
	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		t.Fatalf("connect postgres: %v", err)
	}
	t.Cleanup(pool.Close)
	if err := migrations.ApplyUp(context.Background(), pool); err != nil {
		t.Fatalf("apply migrations: %v", err)
	}
	return postgres.NewWriteStoreFromPool(pool)
}

// TestProjection_PopulatesReadModelEndToEnd is the headline BUG 2 proof: a model
// registered through the WRITE side becomes visible to a READ-side query once the
// projection consumes the emitted event. Postgres + Redis + NATS are all REAL.
func TestProjection_PopulatesReadModelEndToEnd(t *testing.T) {
	testutil.SkipIfNoDocker(t)

	js := dialNATS(t) // creates the MODELS stream over fp.models.>
	readStore := newRedisReadStore(t)
	writeStore := newWriteStore(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// The CQRS write service: Postgres write store + Redis read store + the NATS
	// emitter (events.Publisher over natsutil.Publisher — proto payloads go out as
	// canonical protojson). This is exactly main.go's wiring.
	emitter := events.NewPublisher(natsutil.NewPublisher(js, events.ServiceName))
	svc := domain.NewRegistryService(
		writeStore, readStore, emitter,
		domain.NewRealClock(), domain.NewUUIDGenerator(),
	)

	// Start the projection consumer (the thing under test) against the SAME store.
	projection := events.NewProjection(js, readStore, events.ProjectionConfig{})
	if err := projection.Start(ctx); err != nil {
		t.Fatalf("start projection: %v", err)
	}
	t.Cleanup(projection.Close)

	// Register a model on the WRITE side. This writes Postgres AND emits
	// fp.models.registered, which the projection should consume into Redis.
	actor := domain.Actor{UserID: "user-42", Team: "fraud-team"}
	model, err := svc.RegisterModel(ctx, actor, domain.RegisterModelInput{
		Name:      "fraud-detector",
		Framework: "onnx",
		TaskType:  "classification",
	})
	if err != nil {
		t.Fatalf("RegisterModel (write side): %v", err)
	}

	// PRE-FIX BEHAVIOR: GET would return ErrRecordNotFound forever (read model never
	// populated). POST-FIX: the projection upserts it; poll until the read side finds
	// it (eventual consistency — the projection lag).
	var got domain.Model
	if !eventually(t, 20*time.Second, func() bool {
		m, err := readStore.GetModelByName(ctx, actor.Team, "fraud-detector")
		if err != nil {
			if errors.Is(err, domain.ErrRecordNotFound) {
				return false // projection hasn't caught up yet
			}
			t.Fatalf("GetModelByName: %v", err)
		}
		got = m
		return true
	}) {
		t.Fatal("read model never populated — the projection did not consume fp.models.registered")
	}

	// The projected model must carry the write-side identity + ownership.
	if got.ID != model.ID {
		t.Errorf("projected id = %q, want %q (write-side id)", got.ID, model.ID)
	}
	if got.Name != "fraud-detector" || got.Team != actor.Team || got.OwnerID != actor.UserID {
		t.Errorf("projected model mismatch: %+v", got)
	}
	if got.Framework != "onnx" || got.TaskType != "classification" {
		t.Errorf("projected metadata mismatch: framework=%q task=%q", got.Framework, got.TaskType)
	}

	// And a LIST query (the GET /models path) must now include it.
	models, _, err := readStore.ListModels(ctx, actor.Team, domain.ListModelsFilter{}, domain.ListOptions{})
	if err != nil {
		t.Fatalf("ListModels: %v", err)
	}
	if len(models) != 1 || models[0].ID != model.ID {
		t.Fatalf("ListModels returned %d models, want exactly the registered one", len(models))
	}
}

// TestProjection_ConsumesRawRegisteredEvent isolates the projection: publish a
// fp.models.registered event directly (no write side) and assert the read model is
// populated. This pins the consumer behavior independent of the service wiring.
func TestProjection_ConsumesRawRegisteredEvent(t *testing.T) {
	testutil.SkipIfNoDocker(t)

	js := dialNATS(t)
	readStore := newRedisReadStore(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	projection := events.NewProjection(js, readStore, events.ProjectionConfig{})
	if err := projection.Start(ctx); err != nil {
		t.Fatalf("start projection: %v", err)
	}
	t.Cleanup(projection.Close)

	// Publish a real ModelRegistered via natsutil.Publisher (protojson on the wire).
	pub := natsutil.NewPublisher(js, events.ServiceName)
	now := time.Now().UTC().Truncate(time.Microsecond)
	ev := &eventsv1.ModelRegistered{
		ModelId:      "model-raw-1",
		ModelName:    "spam-classifier",
		Framework:    "pytorch",
		TaskType:     "classification",
		OwnerId:      "user-7",
		Team:         "ml-team",
		RegisteredAt: timestamppb.New(now),
	}
	if err := pub.Publish(ctx, events.SubjectModelRegistered, ev); err != nil {
		t.Fatalf("publish ModelRegistered: %v", err)
	}

	var got domain.Model
	if !eventually(t, 20*time.Second, func() bool {
		m, err := readStore.GetModelByID(ctx, "ml-team", "model-raw-1")
		if err != nil {
			if errors.Is(err, domain.ErrRecordNotFound) {
				return false
			}
			t.Fatalf("GetModelByID: %v", err)
		}
		got = m
		return true
	}) {
		t.Fatal("read model never populated from raw fp.models.registered event")
	}

	if got.Name != "spam-classifier" || got.Team != "ml-team" || got.Framework != "pytorch" {
		t.Errorf("projected model mismatch: %+v", got)
	}
	if !got.CreatedAt.Equal(now) {
		t.Errorf("projected created_at = %v, want %v (registered_at must survive protojson)", got.CreatedAt, now)
	}
}

// eventually polls cond until it returns true or the timeout elapses. Returns true
// if cond succeeded. Used to wait out the projection's eventual-consistency lag.
func eventually(t *testing.T, timeout time.Duration, cond func() bool) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(100 * time.Millisecond)
	}
	return false
}
