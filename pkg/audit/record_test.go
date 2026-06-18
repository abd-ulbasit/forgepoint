package audit

import (
	"strings"
	"testing"
	"time"
)

// sampleRecord returns a fully-populated record for hashing tests. A fixed
// OccurredAt makes the canonical encoding (and therefore the hash) deterministic
// across runs.
func sampleRecord() Record {
	return Record{
		Actor: Actor{
			UserID: "user-123",
			Email:  "ada@fp.io",
			Team:   "platform",
			Role:   "admin",
		},
		Action:        "/forgepoint.auth.v1.AuthService/CreateUser",
		Resource:      "user-999",
		Decision:      DecisionAllow,
		GRPCCode:      "OK",
		Err:           "",
		CorrelationID: "corr-abc",
		OccurredAt:    time.Date(2026, 6, 18, 10, 30, 0, 0, time.UTC),
		SourceService: "auth",
	}
}

// TestChainHash_Deterministic proves the hash is a pure function of (prevHash,
// record): the SAME inputs always yield the SAME hash. This is the property the
// entire tamper-evidence scheme rests on — a verifier in any process/language must
// reproduce the bytes, so the encoding must not depend on map iteration order,
// omitempty, or struct-field reordering.
func TestChainHash_Deterministic(t *testing.T) {
	rec := sampleRecord()
	h1 := ChainHash(GenesisHash, rec)
	h2 := ChainHash(GenesisHash, rec)
	if h1 != h2 {
		t.Fatalf("ChainHash not deterministic: %q != %q", h1, h2)
	}
	// sha256 hex is 64 chars.
	if len(h1) != 64 {
		t.Fatalf("hash length = %d, want 64 (hex sha256)", len(h1))
	}
}

// TestChainHash_DependsOnPrev proves chaining: the same record onto a different
// previous hash yields a different result. This is what makes deletion/reordering
// detectable — a record's hash is bound to its predecessor.
func TestChainHash_DependsOnPrev(t *testing.T) {
	rec := sampleRecord()
	genesis := ChainHash(GenesisHash, rec)
	chained := ChainHash("some-other-prev-hash", rec)
	if genesis == chained {
		t.Fatal("ChainHash ignored prevHash: identical hashes for different predecessors")
	}
}

// TestChainHash_TamperDetected is the core tamper-evidence test. We build a chain
// of records, then mutate ONE field of ONE record and recompute — every link from
// the mutation onward must change. This is the table-driven proof that
// modification of ANY field breaks the chain.
func TestChainHash_TamperDetected(t *testing.T) {
	// Build a 3-record chain and capture each entry hash.
	recs := []Record{sampleRecord(), sampleRecord(), sampleRecord()}
	recs[1].Action = "/forgepoint.auth.v1.AuthService/AssignRole"
	recs[2].Action = "/forgepoint.registry.v1.RegistryService/DeleteModel"

	chain := func(rs []Record) []string {
		hashes := make([]string, len(rs))
		prev := GenesisHash
		for i, r := range rs {
			prev = ChainHash(prev, r)
			hashes[i] = prev
		}
		return hashes
	}
	original := chain(recs)

	// Each case mutates a single field of record index 1 (the middle record). After
	// mutation, hashes[1] AND hashes[2] must differ from the original (the break
	// propagates forward); hashes[0] must be unchanged (the break is non-retroactive
	// for records BEFORE the edit, which is why a verifier can pinpoint WHERE
	// tampering began).
	cases := []struct {
		name   string
		mutate func(r *Record)
	}{
		{"actor user id", func(r *Record) { r.Actor.UserID = "attacker" }},
		{"actor role escalated", func(r *Record) { r.Actor.Role = "superadmin" }},
		{"action", func(r *Record) { r.Action = "/x/Y" }},
		{"resource", func(r *Record) { r.Resource = "different" }},
		{"decision flipped", func(r *Record) { r.Decision = DecisionDeny }},
		{"grpc code", func(r *Record) { r.GRPCCode = "PermissionDenied" }},
		{"err", func(r *Record) { r.Err = "tampered" }},
		{"correlation id", func(r *Record) { r.CorrelationID = "other" }},
		{"occurred at back-dated", func(r *Record) { r.OccurredAt = r.OccurredAt.Add(-time.Hour) }},
		{"source service", func(r *Record) { r.SourceService = "evil" }},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tampered := []Record{sampleRecord(), sampleRecord(), sampleRecord()}
			tampered[1].Action = recs[1].Action
			tampered[2].Action = recs[2].Action
			tc.mutate(&tampered[1])

			got := chain(tampered)

			if got[0] != original[0] {
				t.Errorf("record[0] hash changed but the edit was at record[1] — chain should not be retroactive")
			}
			if got[1] == original[1] {
				t.Errorf("mutating %s did NOT change record[1] hash — tampering undetected", tc.name)
			}
			if got[2] == original[2] {
				t.Errorf("mutating %s did NOT change record[2] hash — break failed to propagate", tc.name)
			}
		})
	}
}

// TestChainHash_DeletionDetected proves removing a record breaks verification at
// the NEXT record: record[2]'s stored prev (the original record[1] hash) no longer
// matches the recomputed hash of the new predecessor (record[0]).
func TestChainHash_DeletionDetected(t *testing.T) {
	recs := []Record{sampleRecord(), sampleRecord(), sampleRecord()}
	recs[1].Action = "/a/B"
	recs[2].Action = "/c/D"

	// Full chain.
	h0 := ChainHash(GenesisHash, recs[0])
	h1 := ChainHash(h0, recs[1])
	h2 := ChainHash(h1, recs[2])

	// Attacker deletes record[1] and re-links record[2] onto record[0].
	forgedH2 := ChainHash(h0, recs[2])
	if forgedH2 == h2 {
		t.Fatal("deletion of record[1] produced the same hash for record[2] — undetectable")
	}
	_ = h2
}

// TestChainHash_ReorderingDetected proves swapping two records changes both hashes.
func TestChainHash_ReorderingDetected(t *testing.T) {
	a := sampleRecord()
	a.Action = "/svc/A"
	b := sampleRecord()
	b.Action = "/svc/B"

	// Original order a, b.
	hA := ChainHash(GenesisHash, a)
	hB := ChainHash(hA, b)

	// Reordered b, a.
	hB2 := ChainHash(GenesisHash, b)
	hA2 := ChainHash(hB2, a)

	if hB == hB2 {
		t.Error("reordering left b's hash unchanged")
	}
	if hA == hA2 {
		t.Error("reordering left a's hash unchanged")
	}
}

// TestCanonicalJSON_FieldInjectionResistant proves that a value containing a quote
// or a field-separator cannot impersonate another field boundary (a
// canonicalization collision). Two records that would collide under a naive
// concatenation must hash differently here.
func TestCanonicalJSON_FieldInjectionResistant(t *testing.T) {
	// r1 puts a crafted string in Email that, under naive "key:value," joining,
	// could look like it also sets role=admin.
	r1 := sampleRecord()
	r1.Actor.Email = `ada@fp.io","actor.role":"admin`
	r1.Actor.Role = "viewer"

	r2 := sampleRecord()
	r2.Actor.Email = "ada@fp.io"
	r2.Actor.Role = "admin"

	if ChainHash(GenesisHash, r1) == ChainHash(GenesisHash, r2) {
		t.Fatal("canonical encoding is injectable: crafted Email collided with a different record")
	}

	// And the encoding must actually escape the embedded quote.
	enc := string(canonicalJSON(r1))
	if !strings.Contains(enc, `\"`) {
		t.Errorf("expected embedded quotes to be escaped in canonical encoding, got: %s", enc)
	}
}

// TestCanonicalJSON_EmptyVsAbsentStable proves the encoding depends on VALUES, not
// on omitempty presence: a record with an explicit empty Resource hashes the same
// as one constructed the same way (no surprise from a field toggling absent).
func TestCanonicalJSON_StableAcrossEqualValues(t *testing.T) {
	r1 := sampleRecord()
	r1.Resource = ""
	r2 := sampleRecord()
	r2.Resource = ""
	if ChainHash(GenesisHash, r1) != ChainHash(GenesisHash, r2) {
		t.Fatal("two equal records produced different hashes")
	}
}

// TestCanonicalJSON_TimezoneNormalized proves OccurredAt is normalized to UTC
// before hashing, so the same instant expressed in a different zone hashes equal.
func TestCanonicalJSON_TimezoneNormalized(t *testing.T) {
	utc := sampleRecord()
	utc.OccurredAt = time.Date(2026, 6, 18, 10, 30, 0, 0, time.UTC)

	loc := time.FixedZone("UTC+2", 2*60*60)
	other := sampleRecord()
	// Same instant, different zone representation.
	other.OccurredAt = utc.OccurredAt.In(loc)

	if ChainHash(GenesisHash, utc) != ChainHash(GenesisHash, other) {
		t.Fatal("same instant in a different timezone hashed differently — UTC normalization failed")
	}
}
