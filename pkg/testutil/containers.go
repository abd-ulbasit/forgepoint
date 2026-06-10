package testutil

import (
	"context"
	"os/exec"
	"testing"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/minio"
	natscontainer "github.com/testcontainers/testcontainers-go/modules/nats"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/modules/redis"
	"github.com/testcontainers/testcontainers-go/wait"
)

// ============================================================================
// TESTCONTAINER HELPERS
// ============================================================================
//
// WHY testcontainers instead of mocks:
//   Real databases behave differently from mocks in subtle, critical ways:
//   - SQL syntax differences (JSONB operators, ARRAY types, CTEs)
//   - Transaction isolation edge cases
//   - Connection pooling behavior
//   - Index performance characteristics
//   - NATS consumer group rebalancing
//   - Redis cache eviction policies
//
//   Testcontainers spins up real Docker containers (~2-5 seconds each),
//   giving us production-equivalent behavior in tests. The tradeoff is
//   speed (seconds vs milliseconds), but the confidence gain is massive.
//
// HOW IT WORKS:
//   1. Test calls StartPostgres(t)
//   2. Testcontainers pulls Docker image (cached after first run)
//   3. Starts container with random port mapping (no conflicts)
//   4. Waits for readiness (port listening, log message, etc.)
//   5. Returns connection string with the dynamic port
//   6. t.Cleanup() automatically stops and removes container
//
// HOW UBER/SPOTIFY DO IT:
//   - Uber: Custom test infra with shared containers (similar concept)
//   - Spotify: Testcontainers for integration tests in CI
//   - Netflix: Embedded databases for unit tests, real for integration
//   - We use per-test containers (isolated, no shared state between tests)
// ============================================================================

// SkipIfNoDocker skips the test if Docker daemon is not available.
// Integration tests require Docker for testcontainers.
func SkipIfNoDocker(t *testing.T) {
	t.Helper()
	if err := exec.Command("docker", "info").Run(); err != nil {
		t.Skip("Docker not available, skipping integration test")
	}
}

// StartPostgres spins up a real PostgreSQL instance via testcontainers.
// Returns the DSN (connection string) ready for database/sql or pgx.
//
// The container is automatically cleaned up when the test finishes.
// Each test gets its own Postgres instance — no shared state.
//
// Usage:
//
//	dsn := testutil.StartPostgres(t)
//	db, err := sql.Open("pgx", dsn)
func StartPostgres(t *testing.T) string {
	t.Helper()
	ctx := context.Background()

	// postgres module creates a container with the given image,
	// database, username, password, and waits for readiness.
	container, err := postgres.Run(ctx, "postgres:17",
		postgres.WithDatabase("fp_test"),
		postgres.WithUsername("fp"),
		postgres.WithPassword("fp"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2)), // Postgres logs this twice: once for TCP, once for Unix socket
	)
	if err != nil {
		t.Fatalf("failed to start Postgres container: %v", err)
	}

	t.Cleanup(func() {
		if err := testcontainers.TerminateContainer(container); err != nil {
			t.Logf("failed to terminate Postgres container: %v", err)
		}
	})

	dsn, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("failed to get Postgres connection string: %v", err)
	}

	return dsn
}

// StartRedis spins up a real Redis instance via testcontainers.
// Returns the address (host:port) ready for go-redis client.
//
// Usage:
//
//	addr := testutil.StartRedis(t)
//	rdb := redis.NewClient(&redis.Options{Addr: addr})
func StartRedis(t *testing.T) string {
	t.Helper()
	ctx := context.Background()

	container, err := redis.Run(ctx, "redis:7")
	if err != nil {
		t.Fatalf("failed to start Redis container: %v", err)
	}

	t.Cleanup(func() {
		if err := testcontainers.TerminateContainer(container); err != nil {
			t.Logf("failed to terminate Redis container: %v", err)
		}
	})

	connStr, err := container.ConnectionString(ctx)
	if err != nil {
		t.Fatalf("failed to get Redis connection string: %v", err)
	}

	return connStr
}

// StartNATS spins up a real NATS server with JetStream enabled via testcontainers.
// Returns the NATS connection URL.
//
// JetStream is enabled by default in the testcontainers NATS module
// (the module passes -DV -js flags automatically).
//
// Usage:
//
//	url := testutil.StartNATS(t)
//	conn, js, err := natsutil.Connect(url)
func StartNATS(t *testing.T) string {
	t.Helper()
	ctx := context.Background()

	container, err := natscontainer.Run(ctx, "nats:2.11")
	if err != nil {
		t.Fatalf("failed to start NATS container: %v", err)
	}

	t.Cleanup(func() {
		if err := testcontainers.TerminateContainer(container); err != nil {
			t.Logf("failed to terminate NATS container: %v", err)
		}
	})

	url, err := container.ConnectionString(ctx)
	if err != nil {
		t.Fatalf("failed to get NATS connection string: %v", err)
	}

	return url
}

// StartMinIO spins up a real MinIO (S3-compatible) object store via testcontainers.
// Returns the endpoint URL (http://host:port).
//
// Default credentials: minioadmin / minioadmin
//
// Usage:
//
//	endpoint := testutil.StartMinIO(t)
//	client, err := minio.New(endpoint, &minio.Options{...})
func StartMinIO(t *testing.T) string {
	t.Helper()
	ctx := context.Background()

	container, err := minio.Run(ctx, "minio/minio:latest")
	if err != nil {
		t.Fatalf("failed to start MinIO container: %v", err)
	}

	t.Cleanup(func() {
		if err := testcontainers.TerminateContainer(container); err != nil {
			t.Logf("failed to terminate MinIO container: %v", err)
		}
	})

	connStr, err := container.ConnectionString(ctx)
	if err != nil {
		t.Fatalf("failed to get MinIO connection string: %v", err)
	}

	return connStr
}
