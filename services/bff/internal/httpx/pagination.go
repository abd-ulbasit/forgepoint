package httpx

import (
	"net/http"
	"strconv"

	commonv1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/common/v1"
)

// Pagination reads the BFF's pagination query params and maps them to the
// platform's common.v1.PaginationRequest, which every list RPC accepts.
//
// THE CONTRACT (cursor-based, per common.proto): the client sends
//
//	?page_size=N&page_token=<opaque cursor>
//
// and receives next_page_token in the response (relayed straight through as
// JSON). The BFF does NOT interpret the token — it is an opaque server-issued
// cursor; treating it as opaque is what keeps cursor pagination working across
// concurrent writes (the design's reason for cursors over offsets). The BFF only
// clamps page_size to sane bounds so a hostile client can't request a huge page;
// the service ALSO enforces its own max (defense in depth), but clamping here
// avoids shipping an obviously bad request downstream.
func Pagination(r *http.Request) *commonv1.PaginationRequest {
	q := r.URL.Query()
	p := &commonv1.PaginationRequest{
		PageToken: q.Get("page_token"),
	}
	if raw := q.Get("page_size"); raw != "" {
		if n, err := strconv.Atoi(raw); err == nil && n > 0 {
			// Clamp to the platform max (100, per common.proto) — silently, since a
			// too-large page is a soft constraint, not a client error worth a 400.
			if n > 100 {
				n = 100
			}
			p.PageSize = int32(n)
		}
	}
	return p
}
