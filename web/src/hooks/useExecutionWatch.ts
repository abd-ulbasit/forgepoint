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
// PRODUCTION NOTE: with the httpOnly-cookie auth upgrade, this could become a
// plain EventSource (the cookie rides automatically) — simpler, with built-in
// reconnect. We use fetch here precisely because the dev flow is header-based.

import { useEffect, useRef, useState } from 'react'
import { executionWatchUrl } from '@/api/endpoints'
import { tokenStore } from '@/auth/tokenStore'
import type { Execution, WatchExecutionFrame } from '@/api/types'

type ConnState = 'connecting' | 'open' | 'closed' | 'error'

const TERMINAL: ReadonlySet<string> = new Set([
  'EXECUTION_STATUS_COMPLETED',
  'EXECUTION_STATUS_FAILED',
  'EXECUTION_STATUS_CANCELLED',
])

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

  // Keep the latest execution in a ref so the reader loop can check terminality
  // without re-subscribing on every state update.
  const stoppedRef = useRef(false)

  useEffect(() => {
    if (!enabled || !executionId) return

    stoppedRef.current = false
    const controller = new AbortController()
    setConnState('connecting')
    setError(null)

    async function run() {
      const token = tokenStore.get()
      try {
        const resp = await fetch(executionWatchUrl(executionId), {
          method: 'GET',
          headers: {
            Accept: 'text/event-stream',
            ...(token ? { Authorization: `Bearer ${token}` } : {}),
          },
          signal: controller.signal,
        })

        if (resp.status === 401) {
          // Session expired mid-stream — let the global handler take over.
          setConnState('error')
          setError('Session expired')
          tokenStore.clear()
          window.location.assign('/login')
          return
        }
        if (!resp.ok || !resp.body) {
          setConnState('error')
          setError(`Stream failed (HTTP ${resp.status})`)
          return
        }

        setConnState('open')
        const reader = resp.body.getReader()
        const decoder = new TextDecoder()
        let buffer = ''

        // Read loop: accumulate text, split on the SSE record delimiter.
        for (;;) {
          const { value, done } = await reader.read()
          if (done) break
          buffer += decoder.decode(value, { stream: true })

          let sep: number
          // Records are separated by a blank line ("\n\n").
          while ((sep = buffer.indexOf('\n\n')) !== -1) {
            const rawRecord = buffer.slice(0, sep)
            buffer = buffer.slice(sep + 2)
            handleRecord(rawRecord)
          }
        }
        // Stream ended cleanly.
        if (!stoppedRef.current) setConnState('closed')
      } catch (err) {
        if (controller.signal.aborted) return // unmount/terminal — not an error
        setConnState('error')
        setError(err instanceof Error ? err.message : 'Stream error')
      }
    }

    function handleRecord(record: string) {
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
      if (dataLines.length === 0) return
      const dataStr = dataLines.join('\n')

      // The BFF emits "event: end" / "event: error" terminal frames.
      if (eventType === 'end') {
        stoppedRef.current = true
        setConnState('closed')
        controller.abort()
        return
      }
      if (eventType === 'error') {
        setConnState('error')
        setError('Stream error from server')
        controller.abort()
        return
      }

      try {
        const frame = JSON.parse(dataStr) as WatchExecutionFrame
        if (frame.execution) {
          setExecution(frame.execution)
          // Stop once terminal — the BFF will EOF, but abort early to be tidy.
          if (TERMINAL.has(frame.execution.status)) {
            stoppedRef.current = true
            setConnState('closed')
            controller.abort()
          }
        }
        if (frame.changedStep?.stepId) {
          setLastChangedStepId(frame.changedStep.stepId)
        }
      } catch {
        // A malformed frame is non-fatal; skip it and keep streaming.
      }
    }

    void run()

    // Cleanup: abort the fetch -> cancels the HTTP request -> BFF cancels its
    // gRPC stream. No leaked connection on unmount or executionId change.
    return () => {
      stoppedRef.current = true
      controller.abort()
    }
  }, [executionId, enabled])

  return { execution, lastChangedStepId, connState, error }
}
