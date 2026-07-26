package events

import (
	"errors"

	"github.com/abd-ulbasit/forgepoint/services/model-serving/internal/domain"
)

// ============================================================================
// ERROR CLASSIFICATION — PERMANENT vs TRANSIENT (the consumer's hardest call)
// ============================================================================
//
// A NATS consumer must decide, on every handler error, whether a redelivery
// could plausibly succeed:
//
//   - PERMANENT (deterministic) errors will fail identically on every retry.
//     Retrying them burns the redelivery budget and eventually DLQs a message
//     that was NEVER going to be processed — pure noise. The right move is to
//     ACK (drop) and log.
//
//   - TRANSIENT errors (a momentary fetch/network/engine hiccup) MIGHT succeed
//     on retry. NAK so JetStream redelivers; after MaxRetries the message goes to
//     the DLQ for human inspection.
//
// This file centralizes that decision for the LOAD path so the handlers stay
// readable and the policy is one auditable place.
//
// STOPPING A POISON MESSAGE FROM LOOPING FOREVER:
// classify errors; ACK permanent ones immediately, NAK transient ones with a
// bounded retry budget + DLQ. Never NAK an error that can't get better.

// permanentLoadErrors are the domain errors that a redelivery cannot fix, because
// they are deterministic functions of the (immutable) event payload:
//   - ErrValidation: the payload is structurally bad (missing name/version/URI).
//   - ErrArtifactURINotAllowed: the URI fails the SSRF allow-list — a config/
//     payload fact, identical on every retry.
//   - ErrModelAlreadyExists: a DIFFERENT artifact is already Ready under this ref
//     (a refuse-to-swap conflict). Retrying won't change the resident artifact.
//   - ErrDigestMismatch: the fetched bytes don't match the expected digest — a
//     supply-chain fact about THIS artifact; redelivery fetches the same bytes.
var permanentLoadErrors = []error{
	domain.ErrValidation,
	domain.ErrArtifactURINotAllowed,
	domain.ErrModelAlreadyExists,
	domain.ErrDigestMismatch,
}

// isPermanentLoadError reports whether err is one a redelivery cannot fix.
func isPermanentLoadError(err error) bool {
	for _, p := range permanentLoadErrors {
		if errors.Is(err, p) {
			return true
		}
	}
	return false
}

// classifyLoadError returns a short, PII-free label for an error, for structured
// logs. We log the error CLASS, never err.Error() verbatim, because a wrapped
// error string can embed the artifact URI or other payload content (the same
// reasoning natsutil's subscriber uses for its handler-error logging).
func classifyLoadError(err error) string {
	switch {
	case errors.Is(err, domain.ErrValidation):
		return "validation"
	case errors.Is(err, domain.ErrArtifactURINotAllowed):
		return "uri_not_allowed"
	case errors.Is(err, domain.ErrModelAlreadyExists):
		return "already_exists_conflict"
	case errors.Is(err, domain.ErrDigestMismatch):
		return "digest_mismatch"
	default:
		return "transient"
	}
}
