// providers.go — the production IDProvider and Clock adapters.
//
// These satisfy the domain's two purity-seam ports (ports.go): IDProvider.NewID
// and Clock.Now. The domain injects them so its tests can supply DETERMINISTIC
// ids and a FROZEN clock (asserting exact values), while production uses real
// UUIDs and the real wall clock. They live in the repository package only because
// it is the infrastructure-facing module; they are one-liners with no SQL, wired
// by the composition root (main.go) alongside the Postgres stores.
package postgres

import (
	"time"

	"github.com/google/uuid"

	"github.com/abd-ulbasit/forgepoint/services/billing/internal/domain"
)

// UUIDProvider is the production IDProvider: a fresh UUIDv4 per call. UUIDv4 is
// random (no coordination, no DB sequence round-trip) so two services / two
// goroutines never collide, and it is opaque (leaks no count/ordering) — the right
// shape for ledger row ids and outbox event ids that double as the consumer
// dedupe key.
type UUIDProvider struct{}

var _ domain.IDProvider = UUIDProvider{}

// NewID returns a new random UUIDv4 as a canonical string. uuid.NewString panics
// only if crypto/rand fails (an unrecoverable host condition), which is the
// correct failure mode for "the machine can't generate randomness".
func (UUIDProvider) NewID() string { return uuid.NewString() }

// SystemClock is the production Clock: the real wall clock in UTC. WHY force UTC
// here: the domain derives the monthly billing period from Now(); UTC removes the
// DST/timezone ambiguity at period boundaries that is a classic billing bug. The
// domain also truncates to microseconds when needed; we hand back full precision
// and let the domain decide.
type SystemClock struct{}

var _ domain.Clock = SystemClock{}

// Now returns the current instant in UTC.
func (SystemClock) Now() time.Time { return time.Now().UTC() }
