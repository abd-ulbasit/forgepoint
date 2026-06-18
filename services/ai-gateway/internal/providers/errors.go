// errors.go — a small typed error for provider adapters.
//
// A provider adapter returns these on a START failure (connection refused after
// retries, a non-200 from the backend, a decode error before any token). The
// failover loop only needs to know "this provider failed to start" (it fails over
// on ANY non-nil start error); the typed error carries which provider/op/why for
// the logs and for test assertions, without leaking a transport type into the domain.
package providers

import "fmt"

// providerError is a structured adapter error: which provider, which operation, and
// a human-readable reason, optionally wrapping the underlying cause.
type providerError struct {
	provider string
	op       string
	msg      string
	cause    error
}

func (e *providerError) Error() string {
	if e.cause != nil {
		return fmt.Sprintf("%s provider: %s: %s: %v", e.provider, e.op, e.msg, e.cause)
	}
	return fmt.Sprintf("%s provider: %s: %s", e.provider, e.op, e.msg)
}

// Unwrap exposes the cause for errors.Is/As against the underlying transport error.
func (e *providerError) Unwrap() error { return e.cause }
