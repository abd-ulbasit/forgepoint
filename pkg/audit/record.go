// Package audit implements a TAMPER-EVIDENT AUDIT LOG for the Forgepoint platform.
//
// ============================================================================
// WHAT THIS PACKAGE IS (and the split it enforces)
// ============================================================================
//
// An audit log answers, forever and verifiably: "WHO did WHAT, WHEN, and was it
// ALLOWED or DENIED?" It is the security/compliance record of every sensitive
// action — distinct from application logs (debugging) and from domain events
// (business facts other services react to). Auditors, incident responders, and
// "who deleted prod?" investigations read this, not Loki.
//
// The package splits the problem into two independently-wired halves:
//
//	CAPTURE (every service)                 PERSIST (one service: auth)
//	┌─────────────────────────┐             ┌──────────────────────────────┐
//	│ AuditInterceptor         │   NATS      │ NATS consumer (fp.audit.>)   │
//	│  builds a Record from    │  fp.audit.  │  → append-only, hash-chained │
//	│  claims + method + status│  recorded   │    Postgres sink (auth svc)  │
//	│  → NATSAuditSink.Record  │ ──────────► │                              │
//	└─────────────────────────┘             └──────────────────────────────┘
//
// This is the SAME choreography shape the notification service uses: capture is a
// one-line interceptor every service adds; persistence is a single consumer the
// auth service owns. Decoupling them means adding audit to a new service is a
// one-line wiring change — it publishes to fp.audit.recorded and auth persists it.
//
// ============================================================================
// WHY a Go struct, NOT a proto type (decoupled from the API — ADR 0004)
// ============================================================================
//
// The Record below is a plain Go struct, deliberately NOT a generated proto
// message. Per ADR 0004 (decoupled event schema), the audit trail must not be
// chained to the request/response proto contract: an audit record describes a
// SECURITY EVENT (actor/action/decision), which is a different, slower-changing
// vocabulary than any one service's API. It is JSON-serialized into the
// natsutil EventEnvelope's data field — the same JSON-on-the-bus choice every
// other Forgepoint event makes (human-readable in the NATS CLI / DLQ).
//
// ============================================================================
// TAMPER EVIDENCE vs TAMPER PROOF
// ============================================================================
//
// Each persisted record commits to ALL prior records via a hash chain:
//
//	entry_hash[n] = sha256( entry_hash[n-1] + canonicalJSON(record[n]) )
//
// This makes deletion, reordering, and modification DETECTABLE: change any field
// of any record, and that record's entry_hash no longer matches — and because
// entry_hash[n] feeds into entry_hash[n+1], EVERY subsequent link also breaks.
// A verifier re-walks the chain and the first mismatch pinpoints the tampering.
//
// This is tamper-EVIDENT, not tamper-PROOF. The residual threat: an attacker with
// WRITE access to the audit table can recompute the ENTIRE chain forward from the
// edit point, producing an internally-consistent but forged history. A pure
// in-database chain cannot defend against that. The standard mitigation (and what
// CloudTrail's log-file integrity / certificate-transparency logs do) is to ship
// the HEAD hash off-box periodically — to an append-only store the app role cannot
// reach (a separate account's S3 with object-lock, a notary, a public ledger). A
// forger would then have to also rewrite every externally-witnessed head, which
// they cannot. We document this off-box step as the production hardening; the
// in-DB chain + append-only trigger is the in-scope mechanism.
package audit

import (
	"crypto/sha256"
	"encoding/hex"
	"strconv"
	"strings"
	"time"
)

// Decision is the ALLOW/DENY verdict an audited action resolved to. It is the
// single most important field for an auditor: a stream of DENY records on
// admin RPCs is an attack signature; a successful ALLOW on DeleteModel is the
// "who deleted prod" answer.
type Decision string

const (
	// DecisionAllow means the action completed (handler returned OK / nil).
	DecisionAllow Decision = "ALLOW"
	// DecisionDeny means the action was REJECTED for an authn/authz reason
	// (gRPC Unauthenticated or PermissionDenied). Capturing DENY is the whole
	// point of an audit log — failed access attempts are the security signal.
	DecisionDeny Decision = "DENY"
	// DecisionError means the action failed for a NON-security reason (Internal,
	// NotFound, InvalidArgument, ...). We still record it: an audit trail that
	// only kept successes would hide, e.g., a half-applied privileged mutation
	// that errored mid-flight. The Decision distinguishes "rejected by policy"
	// (DENY) from "attempted but errored" (ERROR) for the auditor.
	DecisionError Decision = "ERROR"
)

// AnonymousActor is the Actor.UserID used when an audited RPC carried no claims
// (e.g. an unauthenticated request that the auth interceptor rejected before
// claims were populated). We MUST still record these — an anonymous DENY on a
// privileged method is exactly the probe an audit log exists to catch.
const AnonymousActor = "anonymous"

// Actor is WHO performed the action — the authenticated identity, projected from
// grpcutil.Claims. We copy the flat identity fields (not the whole Claims, not
// the token) so the audit record is a stable, secret-free snapshot of the caller.
//
// WHY snapshot the role/team into the record instead of joining to the users
// table at read time: the audit log must reflect the actor's identity AS IT WAS
// at the moment of the action. If a user's role changes (or the user is deleted)
// later, the historical record must still show the role they acted under. This is
// the same reason an invoice copies the price instead of joining to a live
// product catalog.
type Actor struct {
	UserID string `json:"user_id"`
	Email  string `json:"email,omitempty"`
	Team   string `json:"team,omitempty"`
	Role   string `json:"role,omitempty"`
}

// Record is one audit-log entry: a security-relevant action, who took it, and how
// it resolved. It is JSON-serialized into the NATS envelope's data field on
// capture, and (after persistence) is the row the hash chain commits to.
//
// SECRET-FREE BY CONSTRUCTION: there is deliberately NO field for the request
// payload. An audit record names the ACTION (the gRPC method) and the best-effort
// target RESOURCE id — never the arguments. This is what guarantees a password,
// API key, or token from a CreateUser/CreateAPIKey request can never reach the
// audit log: the interceptor that builds a Record (interceptor.go) only ever has
// access to the method string, the claims, and the resulting status — it never
// copies the request body into the Record. Err is sanitized (status message
// only) for the same reason.
type Record struct {
	// Actor is the authenticated caller (or the anonymous sentinel).
	Actor Actor `json:"actor"`

	// Action is the FULL gRPC method, e.g. "/forgepoint.auth.v1.AuthService/CreateUser".
	// The method name IS the audited verb — it is stable, enumerable, and reveals
	// no payload. This is what an auditor filters on ("show every CreateUser").
	Action string `json:"action"`

	// Resource is a best-effort target identifier (e.g. the user id a mutation
	// targeted) IF it is cheaply derivable from the request without decoding
	// sensitive fields; empty otherwise. It is a convenience for the auditor, not
	// a guaranteed field — we never reach into the payload for secrets to fill it.
	Resource string `json:"resource,omitempty"`

	// Decision is ALLOW / DENY / ERROR (see Decision).
	Decision Decision `json:"decision"`

	// GRPCCode is the resulting gRPC status code as a string ("OK",
	// "PermissionDenied", ...). It is the machine-readable companion to Decision.
	GRPCCode string `json:"grpc_code"`

	// Err is the SANITIZED error message (the gRPC status message only — never
	// err.Error() verbatim, never the request payload). Empty on success.
	Err string `json:"err,omitempty"`

	// CorrelationID ties this audit record to the wider request/trace so an
	// investigator can pivot from "who did this" to the full distributed trace.
	CorrelationID string `json:"correlation_id,omitempty"`

	// OccurredAt is when the action happened, in UTC. The hash chain commits to
	// this, so it cannot be back-dated without breaking the chain.
	OccurredAt time.Time `json:"occurred_at"`

	// SourceService is which service captured the action ("auth", "registry", ...)
	// — the provenance of the record, mirroring the envelope's Source.
	SourceService string `json:"source_service"`
}

// ============================================================================
// HASH CHAIN — the tamper-evidence mechanism
// ============================================================================

// GenesisHash is the prevHash of the FIRST record in a chain. An empty string is
// the genesis sentinel: chaining onto "" means "this is the root, nothing came
// before". Defining it as a named constant (rather than a bare "") makes the
// genesis case explicit at every call site and in the migration's default.
const GenesisHash = ""

// ChainHash computes the tamper-evidence hash for a record given the previous
// record's hash:
//
//	ChainHash(prev, rec) = hex( sha256( prev + canonicalJSON(rec) ) )
//
// WHY this detects every class of tampering:
//   - MODIFICATION: any field change alters canonicalJSON(rec) → this record's
//     hash changes → it no longer matches the stored entry_hash, AND every later
//     record (which mixed in this hash) also fails to verify.
//   - DELETION: removing record n means record n+1's stored prev_hash no longer
//     equals the recomputed hash of record n-1 → the gap is detected at n+1.
//   - REORDERING: swapping two records feeds the wrong prev into ChainHash → both
//     hashes diverge from what's stored.
//
// The chaining is what makes a single edit non-local: you cannot fix one record
// in isolation; you would have to recompute the entire suffix of the chain (the
// residual tamper-PROOF threat documented in the package doc — mitigated by
// shipping head hashes off-box).
//
// DETERMINISM IS LOAD-BEARING: the hash is only meaningful if every party
// computes canonicalJSON(rec) byte-identically. We therefore define our OWN
// canonical encoding (canonicalJSON below) with a FIXED field order, instead of
// encoding/json (whose struct-field order is stable but whose presence depends on
// omitempty, and which we don't want to couple the hash to) or map iteration
// (whose order is randomized in Go by design). A verifier in any language can
// reproduce the bytes from this spec.
func ChainHash(prevHash string, rec Record) string {
	h := sha256.New()
	h.Write([]byte(prevHash))
	h.Write(canonicalJSON(rec))
	return hex.EncodeToString(h.Sum(nil))
}

// canonicalJSON produces a DETERMINISTIC byte encoding of a Record for hashing.
//
// We hand-build the encoding with an explicit, fixed field order and explicit
// escaping rather than calling json.Marshal, so the hashed bytes are a stable
// contract independent of:
//   - Go's struct-tag/omitempty behavior (a field going from "" to absent would
//     otherwise silently change the hash),
//   - any future reordering of the Record struct's fields,
//   - encoding/json internals across Go versions.
//
// The format is a flat, ordered, JSON-like object. Timestamps use RFC3339Nano in
// UTC so they round-trip exactly. This is the SAME discipline certificate-
// transparency and blockchain systems apply: the thing you hash must have ONE
// canonical byte form, defined by spec, not by a library's incidental behavior.
func canonicalJSON(rec Record) []byte {
	var b strings.Builder
	b.WriteByte('{')

	writeField(&b, "actor.user_id", rec.Actor.UserID, true)
	writeField(&b, "actor.email", rec.Actor.Email, false)
	writeField(&b, "actor.team", rec.Actor.Team, false)
	writeField(&b, "actor.role", rec.Actor.Role, false)
	writeField(&b, "action", rec.Action, false)
	writeField(&b, "resource", rec.Resource, false)
	writeField(&b, "decision", string(rec.Decision), false)
	writeField(&b, "grpc_code", rec.GRPCCode, false)
	writeField(&b, "err", rec.Err, false)
	writeField(&b, "correlation_id", rec.CorrelationID, false)
	// UTC + RFC3339Nano: a fixed, lossless textual form. We normalize to UTC so a
	// record built with a +00:00 vs a Z-suffixed time hashes identically.
	writeField(&b, "occurred_at", rec.OccurredAt.UTC().Format(time.RFC3339Nano), false)
	writeField(&b, "source_service", rec.SourceService, false)

	b.WriteByte('}')
	return []byte(b.String())
}

// writeField appends `"key":"value"` to the canonical encoding, prefixed with a
// comma except for the first field. Values are escaped via strconv.Quote so any
// embedded quotes/backslashes/control characters cannot let one field's value
// impersonate another field boundary (a canonicalization-collision attack). We
// always emit every field (no omitempty) so the byte form depends only on the
// VALUES, never on presence — empties become "".
func writeField(b *strings.Builder, key, value string, first bool) {
	if !first {
		b.WriteByte(',')
	}
	b.WriteString(strconv.Quote(key))
	b.WriteByte(':')
	b.WriteString(strconv.Quote(value))
}
