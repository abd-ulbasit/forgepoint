package handlers

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	pipelinev1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/pipeline/v1"
)

// fakeWatchStream is a canned server-streaming client: it yields the queued
// responses in order, then io.EOF — letting us drive the SSE relay
// deterministically without a real gRPC stream.
type fakeWatchStream struct {
	grpc.ServerStreamingClient[pipelinev1.WatchExecutionResponse]
	updates []*pipelinev1.WatchExecutionResponse
	i       int
}

func (s *fakeWatchStream) Recv() (*pipelinev1.WatchExecutionResponse, error) {
	if s.i >= len(s.updates) {
		return nil, io.EOF
	}
	u := s.updates[s.i]
	s.i++
	return u, nil
}

// Context is part of the streaming client surface; the relay doesn't call it but
// the embedded interface requires it to exist.
func (s *fakeWatchStream) Context() context.Context     { return context.Background() }
func (s *fakeWatchStream) Header() (metadata.MD, error) { return nil, nil }
func (s *fakeWatchStream) Trailer() metadata.MD         { return nil }
func (s *fakeWatchStream) CloseSend() error             { return nil }
func (s *fakeWatchStream) SendMsg(any) error            { return nil }
func (s *fakeWatchStream) RecvMsg(any) error            { return io.EOF }

// TestWatch_RelaysSSEFrames verifies the gRPC-stream -> SSE bridge: each update
// becomes a "data: <json>\n\n" frame, the content type is text/event-stream, the
// token is forwarded, and a clean upstream EOF closes with an "end" event.
func TestWatch_RelaysSSEFrames(t *testing.T) {
	t.Parallel()

	var sawAuthz string
	pipe := &mockPipeline{
		watchFn: func(ctx context.Context, in *pipelinev1.WatchExecutionRequest) (grpc.ServerStreamingClient[pipelinev1.WatchExecutionResponse], error) {
			sawAuthz = authzFromCtx(ctx)
			if in.GetExecutionId() != "e-42" {
				t.Fatalf("watch execution_id = %q, want e-42", in.GetExecutionId())
			}
			return &fakeWatchStream{updates: []*pipelinev1.WatchExecutionResponse{
				{Sequence: 1, Execution: &pipelinev1.Execution{Id: "e-42", Status: pipelinev1.ExecutionStatus_EXECUTION_STATUS_RUNNING}},
				{Sequence: 2, Execution: &pipelinev1.Execution{Id: "e-42", Status: pipelinev1.ExecutionStatus_EXECUTION_STATUS_COMPLETED}},
			}}, nil
		},
	}
	h := NewPipelinesHandler(pipe, testLogger(), testSSELimits())

	r := requestWithToken(http.MethodGet, "/api/v1/executions/e-42/watch", "watch-tok", nil)
	r.SetPathValue("id", "e-42")

	// httptest.ResponseRecorder implements http.Flusher, so the SSE Flush path
	// runs. We go through RequireAuth so the token lands in the request context
	// exactly as in production.
	w := serveThroughAuth(h.Watch, r)

	if ct := w.Header().Get("Content-Type"); ct != "text/event-stream" {
		t.Fatalf("content-type = %q, want text/event-stream", ct)
	}
	if sawAuthz != "Bearer watch-tok" {
		t.Errorf("watch did not forward token; saw %q", sawAuthz)
	}
	body := w.Body.String()
	// Two data frames + a terminal end event.
	if got := strings.Count(body, "data: "); got != 3 { // 2 updates + 1 end frame
		t.Errorf("expected 3 data frames, got %d; body=%q", got, body)
	}
	if !strings.Contains(body, "event: end") {
		t.Errorf("expected an 'end' event on clean EOF; body=%q", body)
	}
	if !strings.Contains(body, "\"sequence\":\"1\"") && !strings.Contains(body, "\"sequence\":1") {
		// protojson encodes uint64 as a string; accept either to be robust.
		t.Errorf("expected the first update's sequence in the stream; body=%q", body)
	}
}

// TestWatch_StreamOpenError_MapsHTTPStatus verifies that when the stream never
// opens (e.g. NotFound), we return a normal JSON error status (we haven't
// committed to a 200 stream yet) rather than a half-open SSE.
func TestWatch_StreamOpenError_MapsHTTPStatus(t *testing.T) {
	t.Parallel()
	pipe := &mockPipeline{
		watchFn: func(_ context.Context, _ *pipelinev1.WatchExecutionRequest) (grpc.ServerStreamingClient[pipelinev1.WatchExecutionResponse], error) {
			return nil, status.Error(codes.NotFound, "no such execution")
		},
	}
	h := NewPipelinesHandler(pipe, testLogger(), testSSELimits())
	r := requestWithToken(http.MethodGet, "/api/v1/executions/missing/watch", "tok", nil)
	r.SetPathValue("id", "missing")
	w := serveThroughAuth(h.Watch, r)

	if w.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404; body=%s", w.Code, w.Body.String())
	}
}

// ============================================================================
// SSE DISCONNECT: cancelling the request context mid-stream must cause the
// handler to return AND must cancel the upstream gRPC stream context.
// ============================================================================

// blockingWatchStream blocks on Recv until the provided context is cancelled,
// then returns ctx.Err() wrapped as an error. This simulates a long-lived gRPC
// stream from an orchestrator that is mid-execution when the browser closes the
// EventSource tab.
//
// The streamCtx is the context passed to WatchExecution — the BFF derives it
// from r.Context(), so it is cancelled when the request context is cancelled.
// We capture it and expose it via Done() so the test can assert it was cancelled.
type blockingWatchStream struct {
	grpc.ServerStreamingClient[pipelinev1.WatchExecutionResponse]
	streamCtx context.Context
}

func (s *blockingWatchStream) Recv() (*pipelinev1.WatchExecutionResponse, error) {
	<-s.streamCtx.Done() // block until the context is cancelled
	return nil, s.streamCtx.Err()
}

func (s *blockingWatchStream) Context() context.Context     { return s.streamCtx }
func (s *blockingWatchStream) Header() (metadata.MD, error) { return nil, nil }
func (s *blockingWatchStream) Trailer() metadata.MD         { return nil }
func (s *blockingWatchStream) CloseSend() error             { return nil }
func (s *blockingWatchStream) SendMsg(any) error            { return nil }
func (s *blockingWatchStream) RecvMsg(any) error            { return io.EOF }

// TestWatch_ClientDisconnect_CancelsUpstreamStream is the core leak-prevention
// test.
//
// Setup:
//   - The fake stream blocks in Recv() until its context is cancelled.
//   - We capture the context the BFF passed to WatchExecution (this is the
//     upstream gRPC stream context).
//   - We run the Watch handler in a goroutine and cancel the HTTP request
//     context after a short delay (simulating the browser closing the tab).
//
// Assertions:
//  1. The Watch handler goroutine returns within a reasonable timeout (no leak).
//  2. The upstream stream context is cancelled (no leaked gRPC stream).
//
// WHY this test matters: without the disconnect check the goroutine would be
// stuck in Recv() forever (or until the process exits). With the check the
// request context cancellation propagates to the gRPC context (same ctx), which
// unblocks Recv(), which exits the loop, which lets the goroutine return.
func TestWatch_ClientDisconnect_CancelsUpstreamStream(t *testing.T) {
	t.Parallel()

	// streamCtxCh receives the context the BFF passed to WatchExecution so we
	// can assert it is cancelled after the HTTP request context is cancelled.
	streamCtxCh := make(chan context.Context, 1)

	pipe := &mockPipeline{
		watchFn: func(ctx context.Context, _ *pipelinev1.WatchExecutionRequest) (grpc.ServerStreamingClient[pipelinev1.WatchExecutionResponse], error) {
			streamCtxCh <- ctx // capture for the assertion below
			return &blockingWatchStream{streamCtx: ctx}, nil
		},
	}
	h := NewPipelinesHandler(pipe, testLogger(), testSSELimits())

	// Create a cancellable request context that we control.
	reqCtx, cancelReq := context.WithCancel(context.Background())

	r := httptest.NewRequest(http.MethodGet, "/api/v1/executions/e-99/watch", nil)
	r = r.WithContext(reqCtx)
	r.Header.Set("Authorization", "Bearer test-tok")
	r.SetPathValue("id", "e-99")

	w := httptest.NewRecorder()

	// handlerDone signals when Watch returns.
	handlerDone := make(chan struct{})
	go func() {
		defer close(handlerDone)
		// Route through RequireAuth so the token lands in ctx exactly as in prod.
		// (RequireAuth preserves the request context, so cancelReq still works.)
		h.Watch(w, r)
	}()

	// Wait until the BFF has opened the stream (and is blocked in Recv).
	var upstreamCtx context.Context
	select {
	case upstreamCtx = <-streamCtxCh:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for Watch to open the gRPC stream")
	}

	// Simulate the browser closing the EventSource tab.
	cancelReq()

	// ASSERTION 1: the Watch handler must return promptly (no goroutine leak).
	select {
	case <-handlerDone:
		// good
	case <-time.After(2 * time.Second):
		t.Fatal("Watch handler did not return after request context was cancelled — goroutine leak")
	}

	// ASSERTION 2: the upstream gRPC stream context must be cancelled (no stream
	// leak). The upstream ctx IS r.Context() (possibly wrapped by ContextWithToken
	// which copies values but inherits cancellation), so it must be cancelled too.
	select {
	case <-upstreamCtx.Done():
		// good — upstream stream was cancelled
	default:
		t.Error("upstream gRPC stream context was NOT cancelled after client disconnect — stream leak")
	}
}
