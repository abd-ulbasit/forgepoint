// ============================================================================
// useExecutionWatch — the LIVE saga stream (the platform's headline feature).
// ============================================================================
//
// The BFF bridges the orchestrator's gRPC server-stream to SSE at
// GET /api/v1/executions/{id}/watch (see services/bff/internal/handlers/
// pipelines.go). Each SSE "data:" frame is a WatchExecutionResponse with the
// FULL execution snapshot + the step that just changed. We render those frames
// as they arrive so the UI shows saga step transitions in real time.
//
// WHY fetch()+ReadableStream INSTEAD OF the native EventSource API
// (interview-relevant): EventSource cannot send custom headers, so it cannot
// carry our "Authorization: Bearer <token>". Our dev auth flow keeps the token
// in JS memory (not an httpOnly cookie), so the watch request MUST send that
// header — which only fetch() can do. We therefore implement a minimal SSE
// reader over fetch's streaming body:
//   1. fetch the URL with the bearer header and an AbortController signal.
//   2. read the response body as a stream of UTF-8 chunks.
//   3. buffer + split on the SSE record delimiter "\n\n".
//   4. for each record, parse "event:" / "data:" lines; ignore ": ping"
//      heartbeat comments (the BFF sends these every 20s).
//   5. JSON.parse the data line into a WatchExecutionFrame and emit it.
// On unmount (or when the execution reaches a terminal state) we ABORT the
// fetch, which cancels the HTTP request; the BFF ties its gRPC stream context to
// that request, so the upstream stream tears down too — no leaked stream. This
// mirrors the BFF's own goroutine-leak reasoning from the client side.
//
// RECONNECT STRATEGY (bounded exponential backoff):
// A transient network hiccup should not strand the user. On a non-terminal error
// we retry up to MAX_ATTEMPTS with delays: 1s, 2s, 4s, 8s, 15s (capped). The
// hook surfaces 'reconnecting' while retrying so LiveBadge renders something
// true, and only moves to the permanent 'error' state once all attempts are
// exhausted. On each reconnect we re-seed via REST GET /executions/{id} first so
// the UI never shows a stale snapshot if updates were delivered during the
// gap — the stream then takes over and supersedes that snapshot.
//
// PRODUCTION NOTE: with the httpOnly-cookie auth upgrade, this could become a
// plain EventSource (the cookie rides automatically) — simpler, with built-in
// reconnect. We use fetch here precisely because the dev flow is header-based.
//
//
//                    ┌────────────────────────────────────┐
//   mount            │          useEffect loop             │
//   ─────────►  connecting ──► open ──► closed (terminal/EOF)
//                     │         │
//                     │  error  ▼
//                     └───► reconnecting (backoff, attempt 1..N)
//                                │ exhausted
//                                ▼
//                              error (permanent)
//                    └────────────────────────────────────┘

import { useEffect, useRef, useState } from 'react'
import { executionWatchUrl } from '@/api/endpoints'
import { getExecution } from '@/api/endpoints'
import { tokenStore } from '@/auth/tokenStore'
import type { Execution, WatchExecutionFrame } from '@/api/types'

// ConnState mirrors what LiveBadge displays — every value must be truthful.
type ConnState = 'connecting' | 'open' | 'reconnecting' | 'closed' | 'error'

// Execution statuses that mean the saga is permanently done — once we see one
// of these in a frame we stop streaming and never reconnect.
const TERMINAL: ReadonlySet<string> = new Set([
  'EXECUTION_STATUS_COMPLETED',
  'EXECUTION_STATUS_FAILED',
  'EXECUTION_STATUS_CANCELLED',
])

// Backoff schedule in milliseconds: 1s → 2s → 4s → 8s → 15s → 15s …
// Capped at 15 s so the user doesn't wait more than a quarter-minute per gap.
const BACKOFF_MS = [1_000, 2_000, 4_000, 8_000, 15_000]
const MAX_ATTEMPTS = BACKOFF_MS.length // 5 attempts before permanent error

export interface ExecutionWatch {
  execution: Execution | null
  /** stepId of the most recently changed step (for highlight animation). */
  lastChangedStepId: string | null
  connState: ConnState
  error: string | null
}

export function useExecutionWatch(executionId: string, enabled: boolean): ExecutionWatch {
  const [execution, setExecution] = useState<Execution | null>(null)
  const [lastChangedStepId, setLastChangedStepId] = useState<string | null>(null)
  const [connState, setConnState] = useState<ConnState>('connecting')
  const [error, setError] = useState<string | null>(null)

  // unmountedRef: set to true when the effect cleanup fires (component unmounts
  // or executionId/enabled changes). Every async path checks this before setting
  // state — prevents React's "update on unmounted component" warnings and, more
  // importantly, prevents a reconnect attempt from racing with unmount.
  const unmountedRef = useRef(false)

  useEffect(() => {
    if (!enabled || !executionId) return

    // Reset the unmount guard for this effect invocation.
    unmountedRef.current = false

    // We create one AbortController per attempt inside openStream(). A single
    // top-level controller here lets cleanup() abort the CURRENT attempt's
    // controller immediately, even while we are sleeping in the backoff delay.
    // We store a reference so cleanup() can always reach the active one.
    let activeController: AbortController | null = null

    // Backoff timer handle — needed so cleanup() can cancel a pending sleep.
    let backoffTimer: ReturnType<typeof setTimeout> | null = null

    setConnState('connecting')
    setError(null)

    // -----------------------------------------------------------------------
    // openStream: open ONE SSE connection and read until EOF, error, or abort.
    // Returns true  → stream ended cleanly (terminal state reached / "end" frame).
    // Returns false → stream ended with a transient error worth retrying.
    // -----------------------------------------------------------------------
    async function openStream(): Promise<boolean> {
      const controller = new AbortController()
      activeController = controller
      const token = tokenStore.get()

      let resp: Response
      try {
        resp = await fetch(executionWatchUrl(executionId), {
          method: 'GET',
          headers: {
            Accept: 'text/event-stream',
            ...(token ? { Authorization: `Bearer ${token}` } : {}),
          },
          signal: controller.signal,
        })
      } catch (fetchErr) {
        // AbortError means we were cancelled intentionally (unmount / terminal).
        if (controller.signal.aborted) return true
        // Anything else (network down, DNS failure) → transient, worth retrying.
        return false
      }

      if (resp.status === 401) {
        // Session expired — non-retryable; redirect to login.
        if (!unmountedRef.current) {
          setConnState('error')
          setError('Session expired')
        }
        tokenStore.clear()
        window.location.assign('/login')
        return true // treat as "done, don't retry"
      }

      if (!resp.ok || !resp.body) {
        // 4xx (other than 401) are non-retryable client errors; 5xx and missing
        // body are transient server errors worth retrying.
        const is4xx = resp.status >= 400 && resp.status < 500
        if (is4xx) {
          if (!unmountedRef.current) {
            setConnState('error')
            setError(`Stream failed (HTTP ${resp.status})`)
          }
          return true // permanent, don't retry
        }
        return false // 5xx → transient, will retry
      }

      if (!unmountedRef.current) setConnState('open')

      const reader = resp.body.getReader()
      const decoder = new TextDecoder()
      let buffer = ''
      // Track whether this stream ended due to a terminal state or "end" frame,
      // so we can return the right sentinel to the retry loop.
      let cleanEnd = false

      // -------------------------------------------------------------------
      // handleRecord: parse one SSE record (all lines between two "\n\n"s)
      // and update component state. Returns true if we should stop streaming.
      // -------------------------------------------------------------------
      function handleRecord(record: string): boolean {
        let eventType = 'message'
        const dataLines: string[] = []
        for (const line of record.split('\n')) {
          if (line.startsWith(':')) continue // heartbeat/comment — ignore
          if (line.startsWith('event:')) {
            eventType = line.slice(6).trim()
          } else if (line.startsWith('data:')) {
            dataLines.push(line.slice(5).trim())
          }
        }
        if (dataLines.length === 0) return false
        const dataStr = dataLines.join('\n')

        // The BFF emits "event: end" when the stream is cleanly done.
        if (eventType === 'end') {
          cleanEnd = true
          if (!unmountedRef.current) setConnState('closed')
          controller.abort()
          return true
        }
        // The BFF emits "event: error" for fatal server-side errors.
        if (eventType === 'error') {
          // Server signalled a fatal error — no point retrying.
          cleanEnd = true // mark as non-retryable
          if (!unmountedRef.current) {
            setConnState('error')
            setError('Stream error from server')
          }
          controller.abort()
          return true
        }

        try {
          const frame = JSON.parse(dataStr) as WatchExecutionFrame
          if (frame.execution && !unmountedRef.current) {
            setExecution(frame.execution)
            // Stop once terminal — the BFF will EOF, but abort early to be tidy.
            if (TERMINAL.has(frame.execution.status)) {
              cleanEnd = true
              setConnState('closed')
              controller.abort()
              return true
            }
          }
          if (frame.changedStep?.stepId && !unmountedRef.current) {
            setLastChangedStepId(frame.changedStep.stepId)
          }
        } catch {
          // A malformed frame is non-fatal; skip it and keep streaming.
        }
        return false
      }

      // Read loop: accumulate text, split on the SSE record delimiter.
      try {
        for (;;) {
          const { value, done } = await reader.read()
          if (done) break
          if (unmountedRef.current) break
          buffer += decoder.decode(value, { stream: true })

          let sep: number
          // Records are separated by a blank line ("\n\n").
          while ((sep = buffer.indexOf('\n\n')) !== -1) {
            const rawRecord = buffer.slice(0, sep)
            buffer = buffer.slice(sep + 2)
            const shouldStop = handleRecord(rawRecord)
            if (shouldStop) return true
          }
        }
      } catch (readErr) {
        if (controller.signal.aborted) {
          // Aborted by us (terminal state, unmount, or "end" frame) — not an error.
          return cleanEnd || unmountedRef.current
        }
        // Unexpected read error → transient.
        return false
      }

      // Stream EOF without an "end" frame. If we already saw a terminal status
      // in a frame above, cleanEnd is true. Otherwise treat as transient disconnect.
      if (!cleanEnd && !unmountedRef.current) {
        setConnState('reconnecting')
      }
      return cleanEnd
    }

    // -----------------------------------------------------------------------
    // reseedExecution: one-shot REST GET to refresh the execution snapshot so
    // no updates are missed during the reconnect gap. Non-fatal — if it fails
    // we simply keep the stale snapshot and let the new stream supersede it.
    // -----------------------------------------------------------------------
    async function reseedExecution(): Promise<void> {
      try {
        const result = await getExecution(executionId)
        if (!unmountedRef.current && result.execution) {
          setExecution(result.execution)
          // If the REST snapshot shows a terminal state we can stop here.
          if (TERMINAL.has(result.execution.status)) {
            setConnState('closed')
          }
        }
      } catch {
        // Non-fatal — stale snapshot is better than crashing the retry loop.
      }
    }

    // -----------------------------------------------------------------------
    // Main loop: connect, and on transient failure retry with backoff.
    // -----------------------------------------------------------------------
    async function runWithRetry() {
      let attempt = 0

      while (!unmountedRef.current) {
        const isDone = await openStream()
        if (isDone || unmountedRef.current) break

        // Transient error and not unmounted — consider reconnecting.
        attempt++
        if (attempt >= MAX_ATTEMPTS) {
          if (!unmountedRef.current) {
            setConnState('error')
            setError('Stream disconnected after multiple reconnect attempts')
          }
          break
        }

        // Surface 'reconnecting' while waiting — honest to the user.
        if (!unmountedRef.current) {
          setConnState('reconnecting')
        }

        // Re-seed so the UI doesn't show a stale snapshot during the gap.
        await reseedExecution()
        if (unmountedRef.current) break

        // If reseed revealed terminal status, stop — no point opening the stream.
        // (reseedExecution sets connState 'closed' in that case.)
        // We peek at the latest execution via a callback ref to avoid stale closures.
        // Actually: reseedExecution calls setExecution which triggers a re-render, but
        // within this async loop we can check if the reseeded execution was terminal
        // by checking the TERMINAL set against the last frame's status stored in the
        // closure variable — but we don't have that here. Instead we let the loop
        // continue; openStream will receive the first frame and see the terminal status,
        // which will cleanly exit.

        // Exponential backoff: index capped at the last entry.
        const delayMs = BACKOFF_MS[Math.min(attempt - 1, BACKOFF_MS.length - 1)]
        await new Promise<void>((resolve) => {
          backoffTimer = setTimeout(resolve, delayMs)
        })
        backoffTimer = null

        if (unmountedRef.current) break

        // Next attempt: set 'connecting' to show progress.
        if (!unmountedRef.current) {
          setConnState('connecting')
        }
      }
    }

    void runWithRetry()

    // Cleanup: runs on unmount or when executionId/enabled changes.
    //   1. Mark unmounted so async paths don't set state after cleanup.
    //   2. Abort the active fetch (cancels the HTTP request → BFF cancels its
    //      gRPC stream → no leaked connection).
    //   3. Cancel any pending backoff timer (no orphaned setTimeout).
    return () => {
      unmountedRef.current = true
      activeController?.abort()
      if (backoffTimer !== null) {
        clearTimeout(backoffTimer)
        backoffTimer = null
      }
    }
  }, [executionId, enabled])

  return { execution, lastChangedStepId, connState, error }
}
