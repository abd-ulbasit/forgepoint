package audit

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	pkgaudit "github.com/abd-ulbasit/forgepoint/pkg/audit"
)

// makeRecord builds a deterministic-but-distinct audit record for chain tests.
func makeRecord(action string) pkgaudit.Record {
	return pkgaudit.Record{
		Actor:         pkgaudit.Actor{UserID: "user-1", Email: "a@fp.io", Team: "platform", Role: "admin"},
		Action:        action,
		Resource:      "res-1",
		Decision:      pkgaudit.DecisionAllow,
		GRPCCode:      "OK",
		CorrelationID: "corr-1",
		OccurredAt:    time.Now().UTC().Truncate(time.Microsecond), // Postgres timestamptz μs resolution
		SourceService: "auth",
	}
}

// TestAppend_ChainLinks proves (a) appends succeed and (b) the hash chain links
// correctly across multiple records: each row's prev_hash equals the previous
// row's entry_hash, seq is strictly monotonic, the genesis row chains onto the
// empty string, and every entry_hash matches a fresh ChainHash recomputation
// (the verifier's view).
func TestAppend_ChainLinks(t *testing.T) {
	pool := newTestPool(t)
	repo := NewRepository(pool)
	ctx := context.Background()

	recs := []pkgaudit.Record{
		makeRecord("/forgepoint.auth.v1.AuthService/CreateUser"),
		makeRecord("/forgepoint.auth.v1.AuthService/AssignRole"),
		makeRecord("/forgepoint.registry.v1.RegistryService/DeleteModel"),
	}
	eventIDs := []string{uuid.NewString(), uuid.NewString(), uuid.NewString()}

	for i, rec := range recs {
		res, err := repo.Append(ctx, eventIDs[i], rec)
		if err != nil {
			t.Fatalf("append[%d]: %v", i, err)
		}
		if !res.Appended {
			t.Fatalf("append[%d]: expected Appended=true", i)
		}
		if res.Seq != int64(i+1) {
			t.Errorf("append[%d]: seq = %d, want %d", i, res.Seq, i+1)
		}
	}

	// Read the chain back in seq order and verify every link.
	rows, err := pool.Query(ctx,
		`SELECT seq, prev_hash, entry_hash FROM audit_log ORDER BY seq ASC`)
	if err != nil {
		t.Fatalf("select chain: %v", err)
	}
	defer rows.Close()

	type link struct {
		seq        int64
		prev, hash string
	}
	var chain []link
	for rows.Next() {
		var l link
		if err := rows.Scan(&l.seq, &l.prev, &l.hash); err != nil {
			t.Fatalf("scan: %v", err)
		}
		chain = append(chain, l)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows err: %v", err)
	}
	if len(chain) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(chain))
	}

	// Genesis row chains onto the empty string.
	if chain[0].prev != pkgaudit.GenesisHash {
		t.Errorf("genesis prev_hash = %q, want empty (genesis)", chain[0].prev)
	}
	// Each subsequent prev_hash == previous entry_hash; seq strictly +1.
	prev := pkgaudit.GenesisHash
	for i, l := range chain {
		if l.seq != int64(i+1) {
			t.Errorf("row[%d] seq = %d, want %d", i, l.seq, i+1)
		}
		if l.prev != prev {
			t.Errorf("row[%d] prev_hash = %q, want %q (previous entry_hash)", i, l.prev, prev)
		}
		// Recompute the entry hash exactly as a verifier would and compare.
		want := pkgaudit.ChainHash(prev, recs[i])
		if l.hash != want {
			t.Errorf("row[%d] entry_hash = %q, want recomputed %q", i, l.hash, want)
		}
		prev = l.hash
	}
}

// TestAppend_TriggerBlocksUpdate proves the append-only trigger rejects an UPDATE
// to a persisted audit row — the database-level tamper-resistance control. Even
// with the app role's own connection, history cannot be rewritten.
func TestAppend_TriggerBlocksUpdate(t *testing.T) {
	pool := newTestPool(t)
	repo := NewRepository(pool)
	ctx := context.Background()

	id := uuid.NewString()
	if _, err := repo.Append(ctx, id, makeRecord("/x/CreateThing")); err != nil {
		t.Fatalf("append: %v", err)
	}

	// Attempt to tamper: change the actor on the persisted row.
	_, err := pool.Exec(ctx,
		`UPDATE audit_log SET actor_user_id = 'attacker' WHERE event_id = $1`, id)
	if err == nil {
		t.Fatal("UPDATE on audit_log SUCCEEDED — append-only trigger did not fire")
	}
	// The row must be unchanged.
	var actor string
	if err := pool.QueryRow(ctx,
		`SELECT actor_user_id FROM audit_log WHERE event_id = $1`, id).Scan(&actor); err != nil {
		t.Fatalf("re-read row: %v", err)
	}
	if actor != "user-1" {
		t.Errorf("actor = %q after blocked UPDATE, want unchanged 'user-1'", actor)
	}
}

// TestAppend_TriggerBlocksDelete proves the trigger rejects a DELETE — history
// cannot be erased through the app role.
func TestAppend_TriggerBlocksDelete(t *testing.T) {
	pool := newTestPool(t)
	repo := NewRepository(pool)
	ctx := context.Background()

	id := uuid.NewString()
	if _, err := repo.Append(ctx, id, makeRecord("/x/CreateThing")); err != nil {
		t.Fatalf("append: %v", err)
	}

	_, err := pool.Exec(ctx, `DELETE FROM audit_log WHERE event_id = $1`, id)
	if err == nil {
		t.Fatal("DELETE on audit_log SUCCEEDED — append-only trigger did not fire")
	}
	var count int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM audit_log WHERE event_id = $1`, id).Scan(&count); err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 1 {
		t.Errorf("row count = %d after blocked DELETE, want 1 (still present)", count)
	}
}

// TestAppend_TriggerBlocksTruncate proves the statement-level BEFORE TRUNCATE
// trigger rejects `TRUNCATE audit_log` — closing the gap a row-level trigger leaves
// (a row trigger never fires on TRUNCATE, so without this the whole chain could be
// erased in one statement). Even the connecting role here (the testcontainer's
// owner) is blocked, because a trigger is not a table ACL — it fires for everyone
// short of deliberate DDL that disables it.
func TestAppend_TriggerBlocksTruncate(t *testing.T) {
	pool := newTestPool(t)
	repo := NewRepository(pool)
	ctx := context.Background()

	id := uuid.NewString()
	if _, err := repo.Append(ctx, id, makeRecord("/x/CreateThing")); err != nil {
		t.Fatalf("append: %v", err)
	}

	// Attempt to erase the entire log in one statement.
	_, err := pool.Exec(ctx, `TRUNCATE audit_log`)
	if err == nil {
		t.Fatal("TRUNCATE on audit_log SUCCEEDED — append-only TRUNCATE trigger did not fire")
	}

	// The row must still be present (the chain is intact).
	var count int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM audit_log`).Scan(&count); err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 1 {
		t.Errorf("row count = %d after blocked TRUNCATE, want 1 (chain intact)", count)
	}
}

// TestAppend_Idempotent proves a duplicate event_id (a NATS redelivery) does NOT
// double-append and does NOT break the chain: the second Append reports
// Appended=false and the table still has exactly one row at the same seq.
func TestAppend_Idempotent(t *testing.T) {
	pool := newTestPool(t)
	repo := NewRepository(pool)
	ctx := context.Background()

	id := uuid.NewString()
	rec := makeRecord("/x/CreateThing")

	first, err := repo.Append(ctx, id, rec)
	if err != nil {
		t.Fatalf("first append: %v", err)
	}
	if !first.Appended {
		t.Fatal("first append: expected Appended=true")
	}

	// Re-deliver the SAME event id.
	second, err := repo.Append(ctx, id, rec)
	if err != nil {
		t.Fatalf("duplicate append: %v", err)
	}
	if second.Appended {
		t.Error("duplicate append: expected Appended=false (idempotent skip)")
	}
	if second.Seq != first.Seq {
		t.Errorf("duplicate seq = %d, want the original %d", second.Seq, first.Seq)
	}

	var count int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM audit_log`).Scan(&count); err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 1 {
		t.Errorf("row count = %d after duplicate, want 1 (no double-append)", count)
	}

	// A genuinely new record after a duplicate must still chain correctly onto the
	// FIRST row (seq 2, prev_hash == row1.entry_hash) — the duplicate did not
	// advance the chain.
	id2 := uuid.NewString()
	third, err := repo.Append(ctx, id2, makeRecord("/x/UpdateThing"))
	if err != nil {
		t.Fatalf("post-duplicate append: %v", err)
	}
	if third.Seq != 2 {
		t.Errorf("post-duplicate seq = %d, want 2 (duplicate must not advance the chain)", third.Seq)
	}
}

// TestAppend_ConcurrentSingleWriter stresses the advisory-lock single-writer
// guarantee: many goroutines append concurrently, and the result must be a single,
// well-formed chain — strictly increasing seq with no gaps and no forks (every
// prev_hash links to the immediately-preceding entry_hash).
func TestAppend_ConcurrentSingleWriter(t *testing.T) {
	pool := newTestPool(t)
	repo := NewRepository(pool)
	ctx := context.Background()

	const n = 25
	errc := make(chan error, n)
	for i := 0; i < n; i++ {
		go func() {
			_, err := repo.Append(ctx, uuid.NewString(), makeRecord("/x/CreateThing"))
			errc <- err
		}()
	}
	for i := 0; i < n; i++ {
		if err := <-errc; err != nil {
			t.Fatalf("concurrent append: %v", err)
		}
	}

	// Verify a single linear chain: seq 1..n with no gaps, each prev_hash == the
	// previous entry_hash. A fork (two rows sharing a prev) or a gap would show here.
	rows, err := pool.Query(ctx, `SELECT seq, prev_hash, entry_hash FROM audit_log ORDER BY seq ASC`)
	if err != nil {
		t.Fatalf("select: %v", err)
	}
	defer rows.Close()

	prev := pkgaudit.GenesisHash
	var want int64 = 1
	for rows.Next() {
		var seq int64
		var p, h string
		if err := rows.Scan(&seq, &p, &h); err != nil {
			t.Fatalf("scan: %v", err)
		}
		if seq != want {
			t.Fatalf("seq gap/fork: got %d, want %d", seq, want)
		}
		if p != prev {
			t.Fatalf("chain fork at seq %d: prev_hash %q != preceding entry_hash %q", seq, p, prev)
		}
		prev = h
		want++
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows err: %v", err)
	}
	if want-1 != n {
		t.Errorf("chain length = %d, want %d", want-1, n)
	}
}
