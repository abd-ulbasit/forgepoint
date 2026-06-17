// watch.go — the WatchExecution server-streaming bridge.
//
// ============================================================================
// STREAMING A TRANSPORT-FREE DOMAIN: POLL-AND-DIFF
// ============================================================================
//
// The domain PipelineService has NO WatchExecution method — streaming is a
// transport concern (see pipeline_service.go). The domain exposes the queryable
// state (GetExecution) and the engine drives transitions; the HANDLER is
// responsible for turning that into a live gRPC stream.
//
// This file bridges the two by POLLING GetExecution on an interval and emitting a
// WatchExecutionResponse whenever the snapshot CHANGES (a coarse fingerprint of
// status + per-step state). It is the simplest correct bridge:
//
//	┌────────────┐  GetExecution (poll)   ┌──────────────┐  changed?  ┌────────┐
//	│ poll ticker │ ─────────────────────▶ │ diff vs prev  │ ─────────▶ │ Send   │
//	└────────────┘                         └──────────────┘            └────────┘
//	     │                                        │ terminal? → Send final, return nil
//	     └── ctx.Done() (client hung up) ─────────┴── return ctx error
//
// CONTRACT REALIZED (matches the proto):
//   - include_current_state=true → emit the current snapshot FIRST (seq 1), then
//     stream subsequent changes. This avoids a get-then-watch race.
//   - include_current_state=false → do NOT emit the initial snapshot; only send
//     updates that represent a CHANGE observed after the stream opened.
//   - monotonic `sequence` per stream so a reconnecting client can detect gaps.
//   - terminal state (COMPLETED/FAILED/CANCELLED) → emit final snapshot, close.
//
// WHY POLL AND NOT A SUBSCRIPTION (tradeoff, interview-ready): an in-process
// update channel or a NATS subscription would be push (zero poll latency), but it
// couples the handler to the engine's internals or to the event bus. Polling
// keeps the domain transport-free and is trivially correct; for a saga of a few
// steps over seconds-to-minutes the up-to-one-interval latency is negligible. The
// stream CONTRACT (full-snapshot-per-update + sequence) is identical whether the
// source is a poll or a push, so this can be swapped later without touching the
// proto, the domain, or clients.
//
// RESOURCE SAFETY: the loop selects on ctx.Done() every tick, so a client
// disconnect (or server shutdown — the gRPC stream's context is cancelled) stops
// the poll promptly. We never leak a goroutine polling for an execution nobody is
// watching, and we never block forever on a wedged Send (Send respects ctx).
// ============================================================================
package handler

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	pipelinev1 "github.com/abd-ulbasit/forgepoint/gen/go/forgepoint/pipeline/v1"
	"github.com/abd-ulbasit/forgepoint/services/pipeline-orchestrator/internal/domain"
)

// watchPollInterval is how often the bridge polls GetExecution. A modest cadence:
// fast enough that a UI feels live, slow enough that a watched-but-idle execution
// barely touches the store. (Surfaced as a const so a future config can tune it;
// not wired to config yet — that lands with the repo phase that adds the store.)
const watchPollInterval = 500 * time.Millisecond

// nowFunc is the clock the watch loop stamps emitted_at with. A package var (not
// time.Now inline) so tests can make emitted_at deterministic without a real
// clock dependency. Defaults to real wall-clock UTC.
var nowFunc = func() time.Time { return time.Now().UTC() }

// watchExecution runs the poll-and-diff loop until the execution is terminal, the
// client disconnects, or a read fails. It is split out of the RPC method so the
// method stays a thin guard+validate wrapper.
//
// The first GetExecution doubles as an AUTHORIZATION + EXISTENCE check: it is
// tenancy-scoped in the domain, so watching another team's (or a missing)
// execution returns ErrExecutionNotFound → NotFound here, before any streaming.
func (h *PipelineHandler) watchExecution(
	ctx context.Context,
	actor domain.Actor,
	req *pipelinev1.WatchExecutionRequest,
	stream pipelinev1.PipelineOrchestratorService_WatchExecutionServer,
) error {
	execID := req.GetExecutionId()

	// Initial read: authorize + fetch the starting state.
	current, err := h.svc.GetExecution(ctx, actor, execID)
	if err != nil {
		return mapDomainError(err)
	}

	var seq uint64
	prevFingerprint := executionFingerprint(current)

	// include_current_state: replay the current snapshot as the first update so
	// the client has a complete starting view with no get-then-watch race.
	if req.GetIncludeCurrentState() {
		seq++
		if err := sendUpdate(stream, current, nil, seq); err != nil {
			return err
		}
	}

	// If the execution is ALREADY terminal, there will be no further transitions —
	// close cleanly. (When include_current_state was true we already sent the
	// final snapshot above; when false we emit it once so the watcher sees the
	// terminal outcome rather than an empty stream.)
	if current.Status.IsTerminal() {
		if !req.GetIncludeCurrentState() {
			seq++
			if err := sendUpdate(stream, current, nil, seq); err != nil {
				return err
			}
		}
		return nil
	}

	ticker := time.NewTicker(watchPollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			// Client hung up or server is shutting down. Surface the context
			// cause as a gRPC status (Canceled / DeadlineExceeded) and stop —
			// promptly releasing the poll loop.
			return status.FromContextError(ctx.Err()).Err()

		case <-ticker.C:
			latest, err := h.svc.GetExecution(ctx, actor, execID)
			if err != nil {
				return mapDomainError(err)
			}

			fp := executionFingerprint(latest)
			if fp == prevFingerprint {
				continue // no observable change since the last emit
			}
			prevFingerprint = fp

			// Identify which step changed (best-effort) so a UI can highlight it.
			changed := changedStep(current, latest)
			current = latest

			seq++
			if err := sendUpdate(stream, latest, changed, seq); err != nil {
				return err
			}

			if latest.Status.IsTerminal() {
				// Final snapshot already sent above; the run will not transition
				// again — close the stream cleanly.
				return nil
			}
		}
	}
}

// sendUpdate marshals a domain Execution (and the step that changed, if any) into
// a WatchExecutionResponse and pushes it. stream.Send respects the stream's
// context, so a disconnected client surfaces as a Send error and unwinds the loop.
func sendUpdate(
	stream pipelinev1.PipelineOrchestratorService_WatchExecutionServer,
	exec domain.Execution,
	changed *domain.StepExecution,
	seq uint64,
) error {
	resp := &pipelinev1.WatchExecutionResponse{
		Execution: executionToProto(exec),
		Sequence:  seq,
		EmittedAt: timestamppb.New(nowFunc()),
	}
	if changed != nil {
		resp.ChangedStep = stepExecutionToProto(*changed)
	}
	if err := stream.Send(resp); err != nil {
		// A Send failure is almost always the client disconnecting mid-stream.
		// Wrap it as a status so the framework reports it consistently; the
		// message is sanitized (no internals).
		return status.Error(codes.Unavailable, "watch stream send failed")
	}
	return nil
}

// executionFingerprint is a cheap, total summary of an Execution's OBSERVABLE
// state, used to detect "did anything change since the last poll". It folds in the
// execution status + current step + each step's id/status/attempt. Two snapshots
// with the same fingerprint are treated as no-change (no redundant emit). It is
// NOT a security boundary — just a change detector — so a plain string is fine.
//
// WHY include attempt: a step retrying (attempt 1 → 2) IS an observable change
// worth pushing ("it's on attempt 3") even though the status (RUNNING) is the same.
func executionFingerprint(e domain.Execution) string {
	fp := fmt.Sprintf("%d|%s|", e.Status, e.CurrentStep)
	for i := range e.Steps {
		s := e.Steps[i]
		fp += fmt.Sprintf("%s:%d:%d;", s.StepID, s.Status, s.Attempt)
	}
	return fp
}

// changedStep returns the step whose status/attempt differs between two snapshots
// (best-effort: the first one found). Used only to populate ChangedStep for UI
// highlighting; nil is a valid result (an execution-level transition with no
// single owning step, e.g. RUNNING → COMPENSATING).
func changedStep(prev, cur domain.Execution) *domain.StepExecution {
	prevByID := make(map[string]domain.StepExecution, len(prev.Steps))
	for _, s := range prev.Steps {
		prevByID[s.StepID] = s
	}
	for i := range cur.Steps {
		s := cur.Steps[i]
		old, existed := prevByID[s.StepID]
		if !existed || old.Status != s.Status || old.Attempt != s.Attempt {
			cp := s
			return &cp
		}
	}
	return nil
}
