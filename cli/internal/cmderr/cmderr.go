// Package cmderr translates errors — especially gRPC status errors — into
// clean, human-facing CLI messages paired with a deterministic process exit
// code.
//
// ============================================================================
// WHY A DEDICATED ERROR-MAPPING LAYER
// ============================================================================
//
// A CLI is a thin gRPC client. When a server-side interceptor rejects a call,
// the failure arrives as a *google.golang.org/grpc/status.Status carrying a
// codes.Code (Unauthenticated, PermissionDenied, NotFound, ...). Those codes
// are precise but unfriendly: a user running `fp models list` with an expired
// token should see "not logged in or token expired — run 'fp login'", not
// "rpc error: code = Unauthenticated desc = ...".
//
// Two responsibilities live here, deliberately separated from business logic:
//
//  1. MESSAGE MAPPING  — codes.Code → a short, actionable sentence.
//  2. EXIT-CODE MAPPING — codes.Code → a stable integer the SHELL can branch on.
//
// WHY STABLE EXIT CODES MATTER (interview framing):
//
//	CLIs are composed in scripts and CI. `fp pipelines status X || handle_err`
//	only works if the exit code is meaningful and stable. We map auth failures
//	to a distinct code (3) so a wrapper script can, e.g., trigger a re-login on
//	exit 3 but fail hard on exit 5 (server error). This mirrors how `kubectl`,
//	`gh`, and `aws` expose differentiated exit codes.
//
// The convention here (loosely following BSD sysexits + curl): 0 success,
// 1 generic, 2 usage error, 3 auth, 4 forbidden, 5 not-found, 6 invalid-arg,
// 7 unavailable, 8 deadline.
// ============================================================================
package cmderr

import (
	"context"
	"errors"
	"fmt"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Exit codes. These are part of the CLI's contract with shell scripts, so they
// must stay stable across releases. Adding new ones is fine; renumbering is a
// breaking change.
const (
	ExitOK          = 0 // success
	ExitGeneric     = 1 // unclassified failure
	ExitUsage       = 2 // bad flags / wrong arguments (caught before any RPC)
	ExitAuth        = 3 // not logged in or token expired (Unauthenticated)
	ExitForbidden   = 4 // authenticated but not permitted (PermissionDenied)
	ExitNotFound    = 5 // the requested resource does not exist (NotFound)
	ExitInvalidArg  = 6 // server rejected the request shape (InvalidArgument)
	ExitUnavailable = 7 // could not reach the service (Unavailable)
	ExitDeadline    = 8 // call timed out (DeadlineExceeded)
)

// CLIError is an error that also knows which process exit code it should
// produce. main() type-asserts the top-level error to *CLIError to choose the
// exit status; anything else falls back to ExitGeneric.
//
// WHY a custom type rather than panicking or calling os.Exit deep in the call
// tree: keeping os.Exit out of library code means every layer stays testable —
// commands RETURN errors, and exactly one place (main) decides the exit status.
type CLIError struct {
	Code    int    // process exit code
	Message string // user-facing message (no "rpc error:" noise)
	cause   error  // wrapped original error, for errors.Is/As and -v debugging
}

func (e *CLIError) Error() string { return e.Message }

// Unwrap lets errors.Is / errors.As see through to the underlying cause (e.g.
// the original gRPC status), so callers can still inspect codes if they want.
func (e *CLIError) Unwrap() error { return e.cause }

// New builds a CLIError with an explicit exit code and message.
func New(code int, format string, args ...any) *CLIError {
	return &CLIError{Code: code, Message: fmt.Sprintf(format, args...)}
}

// Usage is a convenience for argument/flag errors discovered before any RPC.
func Usage(format string, args ...any) *CLIError {
	return &CLIError{Code: ExitUsage, Message: fmt.Sprintf(format, args...)}
}

// ExitCode extracts the process exit code for any error. Non-CLIErrors that
// nonetheless wrap a gRPC status are still classified correctly, so callers can
// return a raw status error and get the right code for free.
func ExitCode(err error) int {
	if err == nil {
		return ExitOK
	}
	var ce *CLIError
	if errors.As(err, &ce) {
		return ce.Code
	}
	return codeForGRPC(err)
}

// FromGRPC converts a gRPC error returned by a generated client stub into a
// CLIError with a friendly message and the matching exit code. The verb is the
// action being attempted (e.g. "list models") so the message reads naturally.
//
// Non-gRPC errors (a dial failure that never produced a status, a context
// cancellation from Ctrl-C, etc.) are handled too, so callers can pipe ALL
// post-dial errors through this single function.
func FromGRPC(verb string, err error) error {
	if err == nil {
		return nil
	}

	// Ctrl-C / parent context cancellation: not a server error, exit cleanly-ish.
	if errors.Is(err, context.Canceled) {
		return New(ExitGeneric, "%s: cancelled", verb)
	}

	st, ok := status.FromError(err)
	if !ok {
		// Not a gRPC status at all (rare post-dial). Surface the raw error.
		return New(ExitGeneric, "%s: %v", verb, err)
	}

	switch st.Code() {
	case codes.OK:
		return nil
	case codes.Unauthenticated:
		// The single most common CLI auth failure: missing/expired token. The
		// server's auth interceptor returns this when ValidateToken fails.
		return New(ExitAuth,
			"not logged in or token expired — run 'fp login' first")
	case codes.PermissionDenied:
		return New(ExitForbidden,
			"forbidden: your role does not permit to %s (%s)", verb, st.Message())
	case codes.NotFound:
		return New(ExitNotFound, "%s: not found (%s)", verb, st.Message())
	case codes.InvalidArgument, codes.FailedPrecondition, codes.AlreadyExists:
		return New(ExitInvalidArg, "%s: %s", verb, st.Message())
	case codes.Unavailable:
		// Typically: nothing listening on the address (no port-forward running).
		return New(ExitUnavailable,
			"%s: service unavailable — is the port-forward running? (%s)",
			verb, st.Message())
	case codes.DeadlineExceeded:
		return New(ExitDeadline, "%s: timed out", verb)
	default:
		return New(ExitGeneric, "%s: %s (%s)", verb, st.Message(), st.Code())
	}
}

// codeForGRPC maps a raw (unwrapped-to-status) error to an exit code without
// rewriting its message. Used by ExitCode for errors that bypassed FromGRPC.
func codeForGRPC(err error) int {
	st, ok := status.FromError(err)
	if !ok {
		return ExitGeneric
	}
	switch st.Code() {
	case codes.OK:
		return ExitOK
	case codes.Unauthenticated:
		return ExitAuth
	case codes.PermissionDenied:
		return ExitForbidden
	case codes.NotFound:
		return ExitNotFound
	case codes.InvalidArgument, codes.FailedPrecondition, codes.AlreadyExists:
		return ExitInvalidArg
	case codes.Unavailable:
		return ExitUnavailable
	case codes.DeadlineExceeded:
		return ExitDeadline
	default:
		return ExitGeneric
	}
}
