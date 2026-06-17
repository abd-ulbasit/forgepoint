// defaults.go — the production implementations of the two AMBIENT ports
// (Clock, IDGenerator).
//
// WHY these tiny adapters live in the domain package (not in an adapter package
// like Postgres/HTTP): Clock and IDGenerator are not INFRASTRUCTURE — they wrap
// the standard library (time.Now, uuid.NewString) with no network, no DB, no
// framework. Keeping the trivial real impls beside their interfaces means main.go
// can wire the service with one line each, and tests still inject their own
// deterministic fakes (fixedClock, seqIDGen). The purity rule forbids
// grpc/nats/sql/gen-proto — NOT time or uuid — so this stays within the rules.
//
// (Contrast: the Postgres NotificationRepository or the HTTP Notifier ARE
// infrastructure and live in their own adapter packages, importing this domain.)
package domain

import "time"

// SystemClock is the production Clock: it returns the real wall-clock time.
// Injected by main.go; replaced by a fixedClock in tests so timestamp assertions
// are exact.
type SystemClock struct{}

// Now returns the current time.
func (SystemClock) Now() time.Time { return time.Now() }

// UUIDGenerator is the production IDGenerator: it mints UUIDv4 notification ids.
// WHY UUIDv4 (random) rather than a sequential id: notification ids are returned
// to clients and used in MarkRead/Get; a random, non-guessable id avoids leaking
// volume/ordering information and avoids an enumerable id surface (a defense in
// depth on top of the per-user authority checks in the repository ports).
type UUIDGenerator struct{}

// NewID returns a fresh UUIDv4 string (delegating to newUUID, which wraps
// github.com/google/uuid). Tests inject a deterministic generator instead.
func (UUIDGenerator) NewID() string { return newUUID() }
