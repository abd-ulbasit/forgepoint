package cmderr

import (
	"context"
	"errors"
	"strings"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TestExitCodeForGRPCStatus pins the gRPC code → exit code table. Shell scripts
// depend on these numbers, so this is effectively a contract test.
func TestExitCodeForGRPCStatus(t *testing.T) {
	cases := []struct {
		code codes.Code
		want int
	}{
		{codes.OK, ExitOK},
		{codes.Unauthenticated, ExitAuth},
		{codes.PermissionDenied, ExitForbidden},
		{codes.NotFound, ExitNotFound},
		{codes.InvalidArgument, ExitInvalidArg},
		{codes.FailedPrecondition, ExitInvalidArg},
		{codes.AlreadyExists, ExitInvalidArg},
		{codes.Unavailable, ExitUnavailable},
		{codes.DeadlineExceeded, ExitDeadline},
		{codes.Internal, ExitGeneric},
		{codes.Unknown, ExitGeneric},
	}
	for _, c := range cases {
		err := status.Error(c.code, "boom")
		if got := ExitCode(err); got != c.want {
			t.Errorf("ExitCode(%v) = %d, want %d", c.code, got, c.want)
		}
	}
}

// TestExitCodeNil and non-grpc fallbacks.
func TestExitCodeEdgeCases(t *testing.T) {
	if got := ExitCode(nil); got != ExitOK {
		t.Errorf("ExitCode(nil) = %d, want %d", got, ExitOK)
	}
	if got := ExitCode(errors.New("plain")); got != ExitGeneric {
		t.Errorf("ExitCode(plain) = %d, want %d", got, ExitGeneric)
	}
}

// TestFromGRPCMessages checks the user-facing wording AND the exit code that
// rides along with each mapped error.
func TestFromGRPCMessages(t *testing.T) {
	cases := []struct {
		name      string
		err       error
		wantCode  int
		wantInMsg string
	}{
		{
			name:      "unauthenticated suggests login",
			err:       status.Error(codes.Unauthenticated, "bad token"),
			wantCode:  ExitAuth,
			wantInMsg: "fp login",
		},
		{
			name:      "permission denied says forbidden",
			err:       status.Error(codes.PermissionDenied, "no write"),
			wantCode:  ExitForbidden,
			wantInMsg: "forbidden",
		},
		{
			name:      "not found",
			err:       status.Error(codes.NotFound, "missing"),
			wantCode:  ExitNotFound,
			wantInMsg: "not found",
		},
		{
			name:      "unavailable hints at port-forward",
			err:       status.Error(codes.Unavailable, "conn refused"),
			wantCode:  ExitUnavailable,
			wantInMsg: "port-forward",
		},
		{
			name:      "deadline",
			err:       status.Error(codes.DeadlineExceeded, "slow"),
			wantCode:  ExitDeadline,
			wantInMsg: "timed out",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := FromGRPC("do thing", c.err)
			if got == nil {
				t.Fatal("FromGRPC returned nil for non-nil error")
			}
			if ExitCode(got) != c.wantCode {
				t.Errorf("exit code = %d, want %d", ExitCode(got), c.wantCode)
			}
			if !strings.Contains(got.Error(), c.wantInMsg) {
				t.Errorf("message %q does not contain %q", got.Error(), c.wantInMsg)
			}
		})
	}
}

// TestFromGRPCNil: nil error maps to nil (no spurious failure).
func TestFromGRPCNil(t *testing.T) {
	if err := FromGRPC("x", nil); err != nil {
		t.Errorf("FromGRPC(nil) = %v, want nil", err)
	}
}

// TestFromGRPCContextCanceled: a cancelled context (Ctrl-C) is reported
// distinctly, not as a server error.
func TestFromGRPCContextCanceled(t *testing.T) {
	got := FromGRPC("watch", context.Canceled)
	if got == nil {
		t.Fatal("expected error for context.Canceled")
	}
	if !strings.Contains(got.Error(), "cancelled") {
		t.Errorf("message %q does not mention cancellation", got.Error())
	}
}

// TestCLIErrorUnwrap: errors.As/Is can reach through a CLIError to its cause.
func TestCLIErrorUnwrap(t *testing.T) {
	cause := errors.New("root cause")
	ce := &CLIError{Code: ExitGeneric, Message: "wrapped", cause: cause}
	if !errors.Is(ce, cause) {
		t.Error("errors.Is could not find wrapped cause")
	}
}

// TestUsageHelper builds a usage error with the right code.
func TestUsageHelper(t *testing.T) {
	err := Usage("bad %s", "flag")
	if ExitCode(err) != ExitUsage {
		t.Errorf("exit = %d, want %d", ExitCode(err), ExitUsage)
	}
	if err.Error() != "bad flag" {
		t.Errorf("message = %q, want %q", err.Error(), "bad flag")
	}
}
