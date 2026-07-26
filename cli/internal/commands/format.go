package commands

import (
	"strings"

	commonv1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/common/v1"

	"google.golang.org/protobuf/types/known/timestamppb"
)

// pageReq builds a common PaginationRequest with the given page size. Every
// list RPC across the platform takes this shared message; centralizing its
// construction keeps the size clamp (and any future cursor handling) in one
// place. A non-positive size is left as zero so the SERVER applies its own
// default (20) and max (100) — the CLI does not duplicate server policy.
func pageReq(size int) *commonv1.PaginationRequest {
	if size <= 0 {
		return &commonv1.PaginationRequest{}
	}
	return &commonv1.PaginationRequest{PageSize: int32(size)}
}

// This file holds small, pure formatting helpers shared by the table-rendering
// commands. They convert proto wire types (timestamps, verbose enum names) into
// the short, human-friendly strings a CLI table wants. Keeping them pure (no
// I/O) keeps them trivially testable and keeps the command files focused on
// flow rather than string-fiddling.

// dash is what we print for an absent/zero value, so columns never look blank.
const dash = "-"

// fmtTime renders a protobuf Timestamp as RFC3339, or "-" if nil/zero. We use a
// fixed, unambiguous format (not a localized one) so `--json` and table output
// agree and so scripts can parse it.
func fmtTime(ts *timestamppb.Timestamp) string {
	if ts == nil {
		return dash
	}
	t := ts.AsTime()
	if t.IsZero() {
		return dash
	}
	return t.UTC().Format("2006-01-02T15:04:05Z")
}

// shortEnum trims a proto enum's verbose, prefixed name down to a readable
// token: "EXECUTION_STATUS_RUNNING" → "RUNNING", "DRIFT_SEVERITY_CRITICAL" →
// "CRITICAL". Proto enums are SCREAMING_SNAKE_CASE with a type-name prefix
// repeated on every value (a protobuf convention to avoid C++ enum-scope
// collisions); for display we strip everything up to the last meaningful
// segment matching the known prefix.
//
// We pass the prefix explicitly (e.g. "EXECUTION_STATUS_") rather than guessing,
// so the trim is exact and we never accidentally mangle a value.
func shortEnum(full, prefix string) string {
	s := strings.TrimPrefix(full, prefix)
	if s == "" || s == "UNSPECIFIED" {
		return dash
	}
	return s
}

// orDash returns s, or "-" when s is empty, so table cells are never blank.
func orDash(s string) string {
	if s == "" {
		return dash
	}
	return s
}
