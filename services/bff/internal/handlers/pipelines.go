package handlers

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	pipelinev1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/pipeline/v1"
	"github.com/abd-ulbasit/forgepoint/services/bff/internal/httpx"
)

// PipelinesHandler maps /api/v1/pipelines/* and /api/v1/executions/* onto the
// pipeline-orchestrator gRPC service, including the SSE relay of WatchExecution.
type PipelinesHandler struct {
	pipeline PipelineClient
	logger   *slog.Logger
}

func NewPipelinesHandler(pipeline PipelineClient, logger *slog.Logger) *PipelinesHandler {
	return &PipelinesHandler{pipeline: pipeline, logger: logger}
}

// List handles GET /api/v1/pipelines.
func (h *PipelinesHandler) List(w http.ResponseWriter, r *http.Request) {
	ctx := httpx.ContextWithToken(r.Context())
	resp, err := h.pipeline.ListPipelines(ctx, &pipelinev1.ListPipelinesRequest{
		Pagination: httpx.Pagination(r),
	})
	if err != nil {
		httpx.WriteGRPCError(w, h.logger, err)
		return
	}
	httpx.WriteProto(w, http.StatusOK, resp)
}

// unmarshalProto reads an HTTP JSON body straight into a proto request via
// protojson. WHY protojson (not encoding/json into the generated struct):
// CreatePipeline carries well-known types (google.protobuf.Struct config,
// Duration timeout) and an enum (PipelineType) whose JSON form (string name)
// only protojson understands. Hand-mapping those would drag the pipeline's
// schema into the BFF and risk drift on every proto change. Letting protojson
// do the proto<->JSON mapping keeps the BFF a thin, logic-free relay: the
// browser sends the proto's JSON shape, we hand it to the service unmodified.
// DiscardUnknown tolerates extra SPA-only fields without a 400.
//
// Body-size capping is now handled by the BodyLimit middleware applied globally
// in the router (r.Body is already an http.MaxBytesReader by the time this
// function runs). The previous inline io.LimitReader has been removed to keep
// the cap in one canonical place — the middleware — rather than scattered
// defensively across helpers.
func unmarshalProto(r *http.Request, msg proto.Message) error {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return err
	}
	return protojson.UnmarshalOptions{DiscardUnknown: true}.Unmarshal(body, msg)
}

// Create handles POST /api/v1/pipelines. The body is the pipeline definition in
// proto-JSON shape. The BFF performs NO validation — the orchestrator owns DAG
// validity, step-type rules, etc. (ADR guardrail).
func (h *PipelinesHandler) Create(w http.ResponseWriter, r *http.Request) {
	req := &pipelinev1.CreatePipelineRequest{}
	if err := unmarshalProto(r, req); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, "invalid pipeline definition JSON")
		return
	}
	ctx := httpx.ContextWithToken(r.Context())
	resp, err := h.pipeline.CreatePipeline(ctx, req)
	if err != nil {
		httpx.WriteGRPCError(w, h.logger, err)
		return
	}
	httpx.WriteProto(w, http.StatusCreated, resp)
}

// Start handles POST /api/v1/pipelines/{id}/start — triggers an execution. The
// optional JSON body is the run input (google.protobuf.Struct), again parsed via
// protojson so arbitrary input objects pass through faithfully. An empty body is
// allowed (no input).
func (h *PipelinesHandler) Start(w http.ResponseWriter, r *http.Request) {
	req := &pipelinev1.TriggerExecutionRequest{PipelineId: r.PathValue("id")}
	// Body is optional; only parse if present. We read it into the request so the
	// input Struct + idempotency_key (if the SPA sent them) are honored.
	if r.ContentLength != 0 {
		// Parse into a temp request, then keep the path id authoritative.
		tmp := &pipelinev1.TriggerExecutionRequest{}
		if err := unmarshalProto(r, tmp); err != nil {
			httpx.WriteError(w, http.StatusBadRequest, "invalid trigger input JSON")
			return
		}
		req.Input = tmp.GetInput()
		req.IdempotencyKey = tmp.GetIdempotencyKey()
	}
	ctx := httpx.ContextWithToken(r.Context())
	resp, err := h.pipeline.TriggerExecution(ctx, req)
	if err != nil {
		httpx.WriteGRPCError(w, h.logger, err)
		return
	}
	httpx.WriteProto(w, http.StatusAccepted, resp) // 202: work accepted, runs async
}

// GetExecution handles GET /api/v1/executions/{id}.
func (h *PipelinesHandler) GetExecution(w http.ResponseWriter, r *http.Request) {
	ctx := httpx.ContextWithToken(r.Context())
	resp, err := h.pipeline.GetExecution(ctx, &pipelinev1.GetExecutionRequest{
		ExecutionId: r.PathValue("id"),
	})
	if err != nil {
		httpx.WriteGRPCError(w, h.logger, err)
		return
	}
	httpx.WriteProto(w, http.StatusOK, resp)
}

// ============================================================================
// SSE RELAY: gRPC server-stream (WatchExecution) -> text/event-stream
// ============================================================================
//
// GET /api/v1/executions/{id}/watch. This is the ADR's "gRPC server-stream ->
// SSE" bridge — the one piece of real-time the BFF owns. The browser cannot
// consume a gRPC stream; SSE (Server-Sent Events) is the simplest one-way
// (server->client) push that browsers support natively via EventSource, which
// is exactly the shape of execution status updates (the ADR chose SSE over
// WebSocket for this reason).
//
// THE BRIDGE, step by step (interview-critical):
//  1. Set SSE headers (text/event-stream, no-cache, keep-alive) BEFORE writing.
//  2. Open the gRPC WatchExecution stream with the caller's token forwarded.
//  3. Loop: stream.Recv() -> marshal the update to JSON -> write one SSE frame
//     ("data: <json>\n\n") -> Flush so the browser sees it immediately (without
//     Flush, the http.Server buffers and the "stream" arrives only at the end).
//  4. HONOR CLIENT DISCONNECT: r.Context() is cancelled when the browser closes
//     the EventSource. We derive the gRPC call ctx from it, so closing the SSE
//     tab cancels the upstream gRPC stream too — no leaked goroutine/stream. We
//     also select on ctx.Done() so a disconnect breaks the loop promptly.
//  5. HEARTBEAT (ping every 20s): prevents two failure modes:
//     a. Half-open TCP: if the network goes away silently (e.g. mobile radio
//        drops), r.Context() is never cancelled. A periodic write attempt
//        through a dead socket eventually errors (connection reset/broken pipe),
//        which breaks the loop and causes the upstream gRPC stream context to
//        be cancelled. Without this, the goroutine and upstream stream pin
//        indefinitely ("slow-reader pin").
//     b. Proxy idle-timeout eviction: nginx/ALB close idle SSE connections
//        after ~60 s by default. A heartbeat every 20 s keeps the connection
//        classified as "active".
//     The heartbeat is an SSE comment (": ping\n\n"), which the EventSource API
//     silently discards — no change required in the browser SPA.
//  6. WRITE DEADLINE per-write: http.NewResponseController(w).SetWriteDeadline
//     bounds how long a single write (Flush) can block, turning a slow/stuck
//     client from a goroutine pin into an error. We set 5 s per write — enough
//     for a healthy connection, short enough to detect a frozen socket.
//  7. On upstream end (io.EOF) we close cleanly; on upstream error we emit a
//     final SSE "event: error" frame and stop.
//
// ASCII diagram of the goroutine topology:
//
//	Browser ←──SSE──── Watch goroutine ──gRPC stream── Orchestrator
//	         (HTTP/1.1)   (this func)    (ctx-linked)
//
// When the browser disconnects → r.Context() cancelled → gRPC ctx cancelled →
// stream.Recv() returns ctx.Err() → loop exits. OR if TCP goes silently dead:
// heartbeat write fails → loop exits → deferred ctx cancel fires → gRPC stream
// cancelled. Either path: zero leaked goroutines.
//
// INTERVIEW FRAMING: "How do you prevent goroutine leaks in a streaming proxy?"
// Answer: tie the upstream context to the client's request context AND add a
// periodic write to detect half-open TCP; both paths lead to a clean teardown.

const (
	sseHeartbeatInterval = 20 * time.Second // SSE ping cadence
	sseWriteDeadline     = 5 * time.Second  // max time for a single write+flush
)

func (h *PipelinesHandler) Watch(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		// Without flushing we cannot stream; fail loudly rather than buffer.
		httpx.WriteError(w, http.StatusInternalServerError, "streaming unsupported")
		return
	}

	// http.ResponseController exposes per-write deadlines on the underlying
	// net.Conn. http.NewResponseController works with both http.ResponseWriter
	// and the http.Flusher we already checked above. SetWriteDeadline is a
	// no-op if the underlying connection does not implement net.Conn (e.g.
	// httptest.ResponseRecorder) — the method returns an error we ignore in
	// tests, but the write still proceeds.
	rc := http.NewResponseController(w)

	// SSE headers. Set before the first write; once we WriteHeader(200) we are
	// committed to a stream and can no longer change the status code.
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	// Disable proxy buffering (nginx) so frames are not held back.
	w.Header().Set("X-Accel-Buffering", "no")

	// The request context cancels on client disconnect; forward the token onto it
	// so the upstream gRPC stream both authenticates the user AND tears down when
	// the browser goes away.
	ctx := httpx.ContextWithToken(r.Context())

	stream, err := h.pipeline.WatchExecution(ctx, &pipelinev1.WatchExecutionRequest{
		ExecutionId:         r.PathValue("id"),
		IncludeCurrentState: true, // send a snapshot first so the UI isn't blank
	})
	if err != nil {
		// The stream never opened (e.g. NotFound/Unauthenticated). We have not
		// written 200 yet, so we can still return a normal JSON error status.
		httpx.WriteGRPCError(w, h.logger, err)
		return
	}

	// Commit to the stream.
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	heartbeat := time.NewTicker(sseHeartbeatInterval)
	defer heartbeat.Stop()

	// recvCh decouples stream.Recv() (blocking) from the select loop so we can
	// concurrently wait on the heartbeat ticker and the upstream stream. We use
	// a buffered channel of size 1 so the sender goroutine is never permanently
	// blocked: if the loop exits (peer gone) the sender goroutine will eventually
	// unblock when Recv returns (ctx is cancelled) and send into the channel,
	// which is then garbage collected with the channel. One result struct per
	// outstanding Recv — there is always exactly one outstanding Recv.
	type recvResult struct {
		update *pipelinev1.WatchExecutionResponse
		err    error
	}
	recvCh := make(chan recvResult, 1)

	// startRecv issues one async Recv. The loop calls it again after processing
	// each result, keeping exactly one Recv in flight at all times.
	startRecv := func() {
		go func() {
			u, e := stream.Recv()
			recvCh <- recvResult{u, e}
		}()
	}
	startRecv()

	execID := r.PathValue("id")
	for {
		select {
		case <-ctx.Done():
			// Client disconnected or request cancelled.
			h.logger.Info("sse client disconnected", slog.String("execution_id", execID))
			return

		case <-heartbeat.C:
			// Write an SSE comment — invisible to EventSource but keeps TCP alive
			// and detects a half-open connection.
			if err := writeSSEWithDeadline(rc, w, flusher, "", nil, true); err != nil {
				h.logger.Info("sse heartbeat failed — peer gone",
					slog.String("execution_id", execID),
					slog.String("error", err.Error()),
				)
				return // let the deferred cancel propagate to the gRPC stream
			}

		case res := <-recvCh:
			if res.err != nil {
				if errors.Is(res.err, io.EOF) {
					// Upstream finished normally (execution reached terminal state).
					_ = writeSSEWithDeadline(rc, w, flusher, "end", []byte(`{"reason":"complete"}`), false)
					return
				}
				if ctx.Err() != nil {
					// Disconnect/cancel — not an error worth surfacing.
					return
				}
				// A genuine upstream error: tell the client via an SSE error frame.
				h.logger.Warn("watch stream error", slog.String("error", res.err.Error()))
				_ = writeSSEWithDeadline(rc, w, flusher, "error", []byte(`{"error":"stream error"}`), false)
				return
			}

			b, marshalErr := dashboardMarshaler.Marshal(res.update)
			if marshalErr != nil {
				h.logger.Error("failed to marshal watch update", slog.String("error", marshalErr.Error()))
				startRecv()
				continue
			}
			if err := writeSSEWithDeadline(rc, w, flusher, "", b, false); err != nil {
				h.logger.Info("sse write failed — peer gone",
					slog.String("execution_id", execID),
					slog.String("error", err.Error()),
				)
				return
			}
			startRecv()
		}
	}
}

// writeSSEWithDeadline writes one SSE frame (or a heartbeat comment) with a
// per-write deadline so a frozen client cannot pin the goroutine indefinitely.
//
// When heartbeat is true, it writes the SSE comment ": ping\n\n" (invisible to
// EventSource) instead of a data frame, keeping proxies and load balancers from
// treating the connection as idle and closing it.
//
// SetWriteDeadline is best-effort: on an httptest.ResponseRecorder (used in
// tests) it returns an unsupported error, which we ignore — the write still
// proceeds normally. On a real net.Conn the deadline causes the underlying
// Write to return an os.ErrDeadlineExceeded error if the OS buffer is full for
// longer than sseWriteDeadline (i.e. the client is truly stuck).
func writeSSEWithDeadline(rc *http.ResponseController, w http.ResponseWriter, flusher http.Flusher, event string, data []byte, heartbeat bool) error {
	// Set a short deadline for this write. We reset it to zero (disable) after
	// the Flush so subsequent reads of r.Body (none in SSE, but defensive) are
	// not affected by a stale deadline.
	deadline := time.Now().Add(sseWriteDeadline)
	_ = rc.SetWriteDeadline(deadline) // no-op on httptest; real conn: armed
	defer func() { _ = rc.SetWriteDeadline(time.Time{}) }()

	var writeErr error
	if heartbeat {
		_, writeErr = io.WriteString(w, ": ping\n\n")
	} else {
		if event != "" {
			if _, err := io.WriteString(w, "event: "+event+"\n"); err != nil {
				return fmt.Errorf("write sse event: %w", err)
			}
		}
		if _, err := io.WriteString(w, "data: "); err != nil {
			return fmt.Errorf("write sse data prefix: %w", err)
		}
		if _, err := w.Write(data); err != nil {
			return fmt.Errorf("write sse data: %w", err)
		}
		_, writeErr = io.WriteString(w, "\n\n")
	}
	if writeErr != nil {
		return fmt.Errorf("write sse frame: %w", writeErr)
	}
	flusher.Flush()
	return nil
}
