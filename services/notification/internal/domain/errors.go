// errors.go — sentinel errors owned by the Notification domain layer.
//
// WHY sentinels (errors.Is-friendly) rather than ad-hoc fmt.Errorf strings:
//
//	The handler maps each business error to a gRPC status code. A typed sentinel
//	lets the handler do `errors.Is(err, ErrNotFound)` → codes.NotFound without
//	string-matching error messages (brittle, breaks on rewording). The domain is
//	the single place that names a failure; every layer above keys off the name.
//
// WHY the STORAGE sentinels are split from the BUSINESS sentinels (mirrors auth):
//
//	A repository port returns ErrRepoNotFound (a storage fact: "no such row").
//	The service translates that into the right BUSINESS fact — ErrNotFound for an
//	inbox lookup. Keeping the two vocabularies distinct means the handler never
//	needs to know storage exists, and the service is the single translation point.
package domain

import "errors"

var (
	// ErrValidation is returned for malformed input the service rejects before
	// touching any port (empty target on an external channel, a webhook URL that
	// fails the domain's structural SSRF pre-checks, an over-long mute list).
	// Wrapped with %w so callers get a specific message while still being able to
	// errors.Is(err, ErrValidation). The handler maps it to codes.InvalidArgument.
	ErrValidation = errors.New("notification: validation failed")

	// ErrNotFound is returned when a Get/MarkRead targets a notification that
	// does not exist OR belongs to another user. WHY collapse "missing" and
	// "someone else's" into ONE error: returning a different
	// error for "exists but not yours" would let an attacker probe which
	// notification ids exist (existence oracle / IDOR). One generic NotFound — the
	// handler maps it to codes.NotFound — closes that side channel. This is the
	// same anti-enumeration reasoning as auth's single ErrInvalidCredentials.
	ErrNotFound = errors.New("notification: not found")

	// ErrChannelNotConfigured is returned by TestChannel when the caller asks to
	// test a channel they have no enabled ChannelPreference for. Distinct from
	// ErrValidation because it is a STATE problem (nothing to test), not a
	// malformed-input problem — the handler maps it to codes.FailedPrecondition,
	// which tells the client "fix your config first", not "your request was bad".
	ErrChannelNotConfigured = errors.New("notification: channel not configured")

	// ErrSSRFTargetRejected is returned when a webhook/slack target fails the SSRF
	// guard (bad scheme, non-allowlisted Slack host, or — at the adapter — a
	// private/loopback/link-local resolved IP). WHY a DEDICATED sentinel rather
	// than folding into ErrValidation: SSRF rejections are security-relevant and
	// worth surfacing/alerting distinctly (a spike of them may be an attack), and
	// the handler can attach a field-level ErrorDetail pointing at the offending
	// target. It is still mapped to codes.InvalidArgument (the input was bad), but
	// the named sentinel lets callers and metrics single it out. The domain raises
	// it for the checks it CAN do purely (scheme/host/userinfo); the adapter raises
	// it for the network-level checks (resolved-IP denylist, pinned-IP connect).
	ErrSSRFTargetRejected = errors.New("notification: webhook/slack target rejected by SSRF guard")
)
