package handlers

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"time"

	"google.golang.org/protobuf/encoding/protojson"

	aiv1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/ai/v1"
	"github.com/abd-ulbasit/forgepoint/services/bff/internal/httpx"
)

// ============================================================================
// AIHandler — the BFF surface for the AI Gateway (M7 / LLMOps).
// ============================================================================
//
// Three routes, two shapes:
//
//	POST /api/v1/chat          -> ChatCompletion (server-stream) bridged to SSE.
//	GET  /api/v1/ai/providers  -> ListProviders (unary proxy).
//	GET  /api/v1/ai/usage      -> GetUsage (unary proxy).
//
// The chat route is the interesting one: it is the SECOND gRPC-server-stream ->
// SSE bridge in the BFF (the first is pipelines WatchExecution). We deliberately
// REUSE the streaming machinery already built for Watch — the same package-level
// sseLimiter (global + per-user + lifetime caps), the same writeSSEWithDeadline
// frame writer, and the same sseSubject key — rather than duplicate a parallel
// implementation. That reuse is the whole reason those helpers were factored out
// of pipelines.go into package-level functions: a new streaming endpoint should
// inherit the DoS bounds and goroutine-leak discipline for free.
//
// WHY a SEPARATE handler type (not folded into PipelinesHandler): the two share
// streaming PLUMBING but nothing else — different upstream stub, different
// request shape (POST body vs path var), different SSE event vocabulary. Keeping
// them apart keeps each handler's dependency surface (its port interface) honest.
type AIHandler struct {
	ai     AIGatewayClient
	logger *slog.Logger
	// sse is the SAME limiter type the pipelines watch uses. We give the AI
	// handler its OWN instance so chat streams and watch streams have independent
	// budgets — a flood of chat sessions can't starve the saga-watch UI and vice
	// versa. (A shared limiter would couple two unrelated features' capacity.)
	sse *sseLimiter
}

// NewAIHandler wires the (segmented) AI gateway client and builds its SSE
// limiter from the same SSELimits the watch relay uses.
func NewAIHandler(ai AIGatewayClient, logger *slog.Logger, limits SSELimits) *AIHandler {
	return &AIHandler{
		ai:     ai,
		logger: logger,
		sse:    newSSELimiter(limits),
	}
}

// aiMarshaler serializes ChatCompletionResponse frames to JSON for the SSE body.
// Same options as dashboardMarshaler: lowerCamelCase names, zero-valued fields
// present so the SPA sees a stable shape (servedBy/cacheHit/usage always exist).
var aiMarshaler = protojson.MarshalOptions{EmitUnpopulated: true, UseProtoNames: false}

// Providers handles GET /api/v1/ai/providers — a plain unary proxy that forwards
// the caller's token and relays the configured backends + their live circuit
// state. No body, no transformation: the BFF stays a logic-free relay.
func (h *AIHandler) Providers(w http.ResponseWriter, r *http.Request) {
	ctx := httpx.ContextWithToken(r.Context())
	resp, err := h.ai.ListProviders(ctx, &aiv1.ListProvidersRequest{})
	if err != nil {
		httpx.WriteGRPCError(w, h.logger, err)
		return
	}
	httpx.WriteProto(w, http.StatusOK, resp)
}

// Usage handles GET /api/v1/ai/usage — the caller team's token/cost usage +
// remaining budget. The team is derived DOWNSTREAM from the forwarded JWT (never
// from a query param), so the BFF sends an empty request and just relays the
// answer. An optional `since` window could be added later by parsing a query
// param into the request's Timestamp; we keep it unset for now (full-history).
func (h *AIHandler) Usage(w http.ResponseWriter, r *http.Request) {
	ctx := httpx.ContextWithToken(r.Context())
	resp, err := h.ai.GetUsage(ctx, &aiv1.GetUsageRequest{})
	if err != nil {
		httpx.WriteGRPCError(w, h.logger, err)
		return
	}
	httpx.WriteProto(w, http.StatusOK, resp)
}

// ============================================================================
// SSE RELAY: ChatCompletion (gRPC server-stream) -> text/event-stream
// ============================================================================
//
// POST /api/v1/chat. The browser POSTs the prompt + model selection as proto-JSON
// (parsed straight into ChatCompletionRequest via protojson, like the pipelines
// Create handler), and the BFF opens the upstream ChatCompletion stream and
// relays each frame to the browser as SSE.
//
// THE EVENT VOCABULARY the SPA consumes (mirrors the pipelines watch contract):
//
//	event: delta   data: {"delta":"...", "servedBy":"...", "cacheHit":..., "requestId":"..."}
//	  — one per incremental token chunk. The SPA appends `delta` to the rendered
//	    assistant message. servedBy/cacheHit ride along so the UI can show the
//	    provider/cache the moment the FIRST frame lands (the gateway sets them on
//	    every frame).
//	event: done    data: <the full terminal ChatCompletionResponse as JSON>
//	  — emitted when the upstream sends done=true OR on clean io.EOF. Carries the
//	    finish_reason + usage (prompt/completion/total tokens + cost) + served_by
//	    + cache_hit. This is the "token-usage + served-by + cache-hit readout on
//	    completion" the playground shows.
//	event: error   data: {"error":"stream error"}
//	  — a genuine mid-stream upstream failure (after we've committed to 200).
//
// WHY POST (not GET like watch): a completion request carries a body — the chat
// messages, model, sampling params — which doesn't belong in a URL (length,
// logging, history leakage). SSE is just the RESPONSE encoding; nothing requires
// the request to be a GET. The SPA uses fetch() (not EventSource) for exactly the
// same reason the watch hook does: EventSource can neither send a body nor set
// the Authorization header. So fetch+ReadableStream on the client, SSE framing on
// the wire.
//
// EVERYTHING ELSE IS THE WATCH RELAY, REUSED: header order (acquire slot -> set
// SSE headers -> open stream -> commit 200), the per-write deadline, the 20s
// heartbeat comment to detect a half-open socket and beat proxy idle-timeouts,
// the absolute lifetime cap, and the recvCh goroutine that decouples the blocking
// Recv() from the heartbeat select. See the long comment block in pipelines.go
// Watch for the full rationale and the goroutine-topology diagram — this handler
// is the same machine with a different frame vocabulary.
func (h *AIHandler) Chat(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		httpx.WriteError(w, http.StatusInternalServerError, "streaming unsupported")
		return
	}

	// Parse the request body BEFORE reserving an SSE slot or writing any header,
	// so a malformed body is a cheap 400 that consumes no stream budget. protojson
	// (DiscardUnknown) maps the browser's JSON onto the proto, including the
	// ChatRole/ProviderKind enums by their string names — the same logic-free
	// pass-through the pipelines Create handler uses. The caller's TEAM/budget key
	// comes from the JWT downstream, never from this body (the proto comment is
	// explicit about that), so there is nothing to validate or sanitize here.
	req := &aiv1.ChatCompletionRequest{}
	if r.ContentLength != 0 {
		if err := unmarshalProto(r, req); err != nil {
			httpx.WriteError(w, http.StatusBadRequest, "invalid chat completion JSON")
			return
		}
	}

	// SSE RESOURCE CAP: reserve a stream slot before opening the upstream stream,
	// keyed on the JWT subject so the per-user bound applies. Identical 429-at-the-
	// door defense as the watch relay; chat streams are even longer-lived (a model
	// can stream for many seconds) so the cap matters more here.
	release, ok := h.sse.acquire(sseSubject(r))
	if !ok {
		h.logger.Warn("chat stream rejected — concurrency cap reached")
		w.Header().Set("Retry-After", "5")
		httpx.WriteError(w, http.StatusTooManyRequests, "too many concurrent chat streams")
		return
	}
	defer release()

	rc := http.NewResponseController(w)

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")

	// Token-forwarding context that ALSO cancels on client disconnect: closing the
	// browser tab cancels r.Context(), which cancels the upstream ChatCompletion
	// stream, which unblocks Recv() — no leaked goroutine or stream.
	ctx := httpx.ContextWithToken(r.Context())

	// Absolute lifetime cap (defense against a peer that answers heartbeats forever
	// while the model keeps streaming). Skipped when MaxLifetime <= 0.
	if h.sse.limits.MaxLifetime > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, h.sse.limits.MaxLifetime)
		defer cancel()
	}

	stream, err := h.ai.ChatCompletion(ctx, req)
	if err != nil {
		// Stream never opened (e.g. Unauthenticated / budget exhausted surfaced as
		// ResourceExhausted). We have NOT written 200 yet, so a normal JSON error
		// status is still valid.
		httpx.WriteGRPCError(w, h.logger, err)
		return
	}

	// Commit to the stream.
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	heartbeat := time.NewTicker(sseHeartbeatInterval)
	defer heartbeat.Stop()

	// One outstanding Recv at a time, handed to the select loop via a buffered
	// channel — identical decoupling to the watch relay.
	type recvResult struct {
		frame *aiv1.ChatCompletionResponse
		err   error
	}
	recvCh := make(chan recvResult, 1)
	startRecv := func() {
		go func() {
			f, e := stream.Recv()
			recvCh <- recvResult{f, e}
		}()
	}
	startRecv()

	for {
		select {
		case <-ctx.Done():
			h.logger.Info("chat sse client disconnected")
			return

		case <-heartbeat.C:
			if err := writeSSEWithDeadline(rc, w, flusher, "", nil, true); err != nil {
				h.logger.Info("chat sse heartbeat failed — peer gone", slog.String("error", err.Error()))
				return
			}

		case res := <-recvCh:
			if res.err != nil {
				if errors.Is(res.err, io.EOF) {
					// Clean upstream end WITHOUT an explicit done frame (the stub may
					// signal completion purely via EOF). Emit a terminal "done" so the
					// SPA always gets a completion signal to flip out of streaming.
					_ = writeSSEWithDeadline(rc, w, flusher, "done", []byte(`{"done":true}`), false)
					return
				}
				if ctx.Err() != nil {
					return // disconnect/cancel — not worth surfacing
				}
				h.logger.Warn("chat stream error", slog.String("error", res.err.Error()))
				_ = writeSSEWithDeadline(rc, w, flusher, "error", []byte(`{"error":"stream error"}`), false)
				return
			}

			b, marshalErr := aiMarshaler.Marshal(res.frame)
			if marshalErr != nil {
				h.logger.Error("failed to marshal chat frame", slog.String("error", marshalErr.Error()))
				startRecv()
				continue
			}

			// The terminal frame (done=true) becomes "event: done"; every other frame
			// is an incremental "event: delta". Emitting the WHOLE frame as JSON in
			// both cases means the SPA reads delta/servedBy/cacheHit from a delta and
			// finishReason/usage from the done frame off the same parsed object.
			event := "delta"
			done := res.frame.GetDone()
			if done {
				event = "done"
			}
			if err := writeSSEWithDeadline(rc, w, flusher, event, b, false); err != nil {
				h.logger.Info("chat sse write failed — peer gone", slog.String("error", err.Error()))
				return
			}
			if done {
				// Upstream told us this is the terminal frame; stop here rather than
				// wait for a trailing EOF, so the slot frees promptly.
				return
			}
			startRecv()
		}
	}
}
