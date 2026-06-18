// ============================================================================
// useChat — the streaming chat playground hook (M7 AI Gateway).
// ============================================================================
//
// The BFF bridges the AI gateway's ChatCompletion gRPC server-stream to SSE at
// POST /api/v1/chat (see services/bff/internal/handlers/ai.go). Each SSE frame is
// a ChatCompletionFrame:
//   event: delta  → incremental token text (append to the assistant message).
//   event: done   → terminal frame with finishReason + usage + servedBy + cacheHit.
//   event: error  → fatal mid-stream server error.
//
// WHY fetch()+ReadableStream (NOT EventSource), same reasoning as useExecutionWatch:
// EventSource can neither send a request BODY (the chat messages + model) nor set
// the Authorization header our dev token flow needs. So we POST with fetch, read
// the streaming response body, split on the SSE record delimiter "\n\n", and parse
// each record's event/data lines. Heartbeat comments (": ping") are ignored.
//
// WHY NO RECONNECT (the deliberate difference from useExecutionWatch): a watch is
// a VIEW of a long-lived resource — reconnecting just re-attaches to the same
// execution. A chat completion is a ONE-SHOT side-effecting request: it consumes
// the team's token budget and produces output exactly once. Silently reconnecting
// on a dropped stream would RE-BILL the request and splice a duplicate/garbled
// continuation into the user's message — worse than surfacing the error. So on any
// failure we stop and report it; the user explicitly re-sends if they want to
// retry. This is the correct semantics for a non-idempotent streaming POST.
//
// BOUNDEDNESS + CLEANUP (the leak discipline we DO keep): exactly one in-flight
// request at a time (a new send aborts the previous), an AbortController tied to
// the request so unmount/abort cancels the fetch → the BFF cancels its upstream
// gRPC stream → no leaked stream, and an unmount guard so no state is set after
// teardown. Mirrors the watch hook's cleanup, minus the retry loop.

import { useCallback, useEffect, useRef, useState } from 'react'
import { chatStreamUrl } from '@/api/endpoints'
import { tokenStore } from '@/auth/tokenStore'
import type {
  ChatCompletionFrame,
  ChatMessage,
  FinishReason,
  ProviderKind,
  TokenUsage,
} from '@/api/types'

// ChatStatus mirrors what the page renders — every value must be truthful.
export type ChatStatus = 'idle' | 'streaming' | 'done' | 'error'

/** The completion metadata the playground shows on finish. */
export interface ChatCompletionMeta {
  finishReason?: FinishReason
  usage?: TokenUsage
  servedBy?: ProviderKind
  cacheHit?: boolean
  requestId?: string
}

/** Parameters for one send. The team/budget key is derived from the JWT
 *  downstream, never sent here. */
export interface SendParams {
  model?: string
  provider?: ProviderKind
  /** The full prior turns (system/user/assistant) plus the new user turn. */
  messages: ChatMessage[]
}

export interface ChatStream {
  /** The assistant text accumulated so far for the in-flight (or last) turn. */
  assistant: string
  status: ChatStatus
  /** Completion metadata, set when the terminal `done` frame arrives. */
  meta: ChatCompletionMeta | null
  error: string | null
  /** Start a completion. Aborts any in-flight stream first. */
  send: (params: SendParams) => void
  /** Abort the in-flight stream (user pressed Stop / navigated away). */
  stop: () => void
  /** Reset to idle (clear the last assistant text + meta). */
  reset: () => void
}

export function useChat(): ChatStream {
  const [assistant, setAssistant] = useState('')
  const [status, setStatus] = useState<ChatStatus>('idle')
  const [meta, setMeta] = useState<ChatCompletionMeta | null>(null)
  const [error, setError] = useState<string | null>(null)

  // The active request's AbortController, so a new send / stop / unmount can
  // cancel the previous stream. A ref (not state) because aborting must not
  // trigger a re-render and we read it synchronously.
  const controllerRef = useRef<AbortController | null>(null)
  // Guards against setting state after unmount (React warning + races).
  const unmountedRef = useRef(false)

  useEffect(() => {
    unmountedRef.current = false
    return () => {
      unmountedRef.current = true
      controllerRef.current?.abort()
    }
  }, [])

  const stop = useCallback(() => {
    controllerRef.current?.abort()
    controllerRef.current = null
    // Only downgrade an actively-streaming state; a finished/errored stream keeps
    // its terminal status so the readout stays visible.
    setStatus((s) => (s === 'streaming' ? 'done' : s))
  }, [])

  const reset = useCallback(() => {
    controllerRef.current?.abort()
    controllerRef.current = null
    setAssistant('')
    setMeta(null)
    setError(null)
    setStatus('idle')
  }, [])

  const send = useCallback((params: SendParams) => {
    // One in-flight stream at a time: abort the previous before starting.
    controllerRef.current?.abort()
    const controller = new AbortController()
    controllerRef.current = controller

    // Fresh turn — clear the previous assistant text + meta.
    setAssistant('')
    setMeta(null)
    setError(null)
    setStatus('streaming')

    const token = tokenStore.get()

    // Accumulate completion metadata across frames; the gateway sets servedBy /
    // cacheHit on every frame and usage / finishReason on the terminal one.
    let acc: ChatCompletionMeta = {}

    async function run() {
      let resp: Response
      try {
        resp = await fetch(chatStreamUrl(), {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            Accept: 'text/event-stream',
            ...(token ? { Authorization: `Bearer ${token}` } : {}),
          },
          body: JSON.stringify({
            model: params.model || undefined,
            provider: params.provider || undefined,
            messages: params.messages,
            stream: true,
          }),
          signal: controller.signal,
        })
      } catch {
        if (controller.signal.aborted) return // intentional stop/unmount
        if (!unmountedRef.current) {
          setStatus('error')
          setError('Network error — is the BFF reachable?')
        }
        return
      }

      if (resp.status === 401) {
        // Session expired — clear + redirect, consistent with the axios client.
        tokenStore.clear()
        window.location.assign('/login')
        return
      }
      if (!resp.ok || !resp.body) {
        // Try to surface the BFF's sanitized error message from the JSON body.
        let msg = `Chat failed (HTTP ${resp.status})`
        try {
          const body = (await resp.json()) as { error?: { message?: string } }
          if (body?.error?.message) msg = body.error.message
        } catch {
          /* non-JSON body — keep the generic message */
        }
        if (!unmountedRef.current) {
          setStatus('error')
          setError(msg)
        }
        return
      }

      const reader = resp.body.getReader()
      const decoder = new TextDecoder()
      let buffer = ''

      // Parse one SSE record (the lines between two blank lines). Returns true if
      // the stream is finished (done/error frame) and we should stop reading.
      function handleRecord(record: string): boolean {
        let eventType = 'message'
        const dataLines: string[] = []
        for (const line of record.split('\n')) {
          if (line.startsWith(':')) continue // heartbeat comment — ignore
          if (line.startsWith('event:')) eventType = line.slice(6).trim()
          else if (line.startsWith('data:')) dataLines.push(line.slice(5).trim())
        }
        if (dataLines.length === 0) return false
        const dataStr = dataLines.join('\n')

        if (eventType === 'error') {
          if (!unmountedRef.current) {
            setStatus('error')
            setError('The model stream failed.')
          }
          controller.abort()
          return true
        }

        let frame: ChatCompletionFrame
        try {
          frame = JSON.parse(dataStr) as ChatCompletionFrame
        } catch {
          return false // a malformed frame is non-fatal; skip it
        }

        // Carry forward provider/cache/request metadata from any frame.
        if (frame.servedBy) acc = { ...acc, servedBy: frame.servedBy }
        if (frame.cacheHit !== undefined) acc = { ...acc, cacheHit: frame.cacheHit }
        if (frame.requestId) acc = { ...acc, requestId: frame.requestId }

        // Append incremental text (render as TEXT — never as HTML).
        if (frame.delta && !unmountedRef.current) {
          setAssistant((prev) => prev + frame.delta)
        }

        if (eventType === 'done' || frame.done) {
          if (frame.usage) acc = { ...acc, usage: frame.usage }
          if (frame.finishReason) acc = { ...acc, finishReason: frame.finishReason }
          if (!unmountedRef.current) {
            setMeta(acc)
            setStatus('done')
          }
          controller.abort()
          return true
        }
        return false
      }

      try {
        for (;;) {
          const { value, done } = await reader.read()
          if (done) break
          if (unmountedRef.current) break
          buffer += decoder.decode(value, { stream: true })
          let sep: number
          while ((sep = buffer.indexOf('\n\n')) !== -1) {
            const rawRecord = buffer.slice(0, sep)
            buffer = buffer.slice(sep + 2)
            if (handleRecord(rawRecord)) return
          }
        }
      } catch {
        if (controller.signal.aborted) return // we aborted (done/stop/unmount)
        if (!unmountedRef.current) {
          setStatus('error')
          setError('The connection dropped mid-stream.')
        }
        return
      }

      // Stream EOF without an explicit done frame: the BFF synthesizes a done
      // event, but guard anyway — settle to 'done' so the UI leaves 'streaming'.
      if (!unmountedRef.current) {
        setMeta((m) => m ?? acc)
        setStatus((s) => (s === 'streaming' ? 'done' : s))
      }
    }

    void run()
  }, [])

  return { assistant, status, meta, error, send, stop, reset }
}
