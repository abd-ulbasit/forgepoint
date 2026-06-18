// ============================================================================
// ChatPage — the AI Gateway playground (M7 / LLMOps).
// ============================================================================
// A minimal chat console over the platform's AI Gateway. It exercises the second
// gRPC-server-stream -> SSE bridge in the BFF (POST /api/v1/chat): the user types
// a prompt, the assistant response streams in token-by-token, and on completion a
// readout shows which provider served the request, whether it was a semantic
// cache hit, and the token usage + cost.
//
// THE SHAPE (three regions):
//   1. A control bar: a model + provider selector sourced from
//      GET /api/v1/ai/providers (so the options reflect the actually-configured
//      backends and their live circuit state) + an AI-usage tile.
//   2. The transcript: the running conversation. Each turn renders as TEXT — we
//      NEVER use dangerouslySetInnerHTML; model output is untrusted and could
//      contain markup, so we let React escape it. (Interview point: rendering LLM
//      output as HTML is a classic stored-XSS sink.)
//   3. The composer: a textarea + Send/Stop. Enter sends, Shift+Enter newlines.
//
// STREAMING UX: while a completion streams, the in-flight assistant turn shows the
// accumulating text with a blinking cursor; the Send button becomes Stop (which
// aborts the fetch → the BFF cancels its upstream gRPC stream). The useChat hook
// owns all the SSE plumbing + the leak-safe teardown; this page is pure view.

import { useEffect, useMemo, useRef, useState } from 'react'
import { useAIProviders, useAIUsage } from '@/hooks/queries'
import { useChat } from '@/hooks/useChat'
import { Card, PageHeader, StatCard } from '@/components/ui'
import { StatusPill } from '@/components/StatusPill'
import { ExperimentsIcon } from '@/components/layout/icons'
import { humanizeEnum } from '@/lib/format'
import type { ChatMessage, Provider, ProviderKind } from '@/api/types'

// A locally-tracked transcript turn. We keep the conversation in page state and
// pass the full history to the gateway on each send (multi-turn context). The
// in-flight assistant turn is rendered from the hook's live `assistant` buffer,
// then committed into this array when the stream completes.
interface Turn {
  role: 'user' | 'assistant'
  content: string
}

export function ChatPage() {
  const providersQ = useAIProviders()
  const usageQ = useAIUsage()
  const chat = useChat()

  const [turns, setTurns] = useState<Turn[]>([])
  const [draft, setDraft] = useState('')
  const [model, setModel] = useState('')
  const [provider, setProvider] = useState<ProviderKind | ''>('')

  // Flatten every model across enabled providers into selector options. Disabled
  // providers (or those with an OPEN circuit) are still listed but visually
  // marked — the gateway will fail over, but it's honest to show their state.
  const providers: Provider[] = providersQ.data?.providers ?? []
  const modelOptions = useMemo(() => {
    const opts: { value: string; label: string }[] = []
    for (const p of providers) {
      for (const m of p.models ?? []) {
        opts.push({ value: m, label: `${m} · ${p.name}` })
      }
    }
    return opts
  }, [providers])

  // Default the model to the first available option once providers load.
  useEffect(() => {
    if (!model && modelOptions.length > 0) setModel(modelOptions[0].value)
  }, [model, modelOptions])

  // Auto-scroll the transcript to the bottom as tokens stream in.
  const scrollRef = useRef<HTMLDivElement>(null)
  useEffect(() => {
    scrollRef.current?.scrollTo({ top: scrollRef.current.scrollHeight })
  }, [turns, chat.assistant])

  // When a stream finishes, commit the assistant buffer as a transcript turn so
  // the next send carries it as context and the live buffer can be reused.
  const committedRef = useRef(false)
  useEffect(() => {
    if (chat.status === 'done' && chat.assistant && !committedRef.current) {
      committedRef.current = true
      setTurns((prev) => [...prev, { role: 'assistant', content: chat.assistant }])
    }
  }, [chat.status, chat.assistant])

  const streaming = chat.status === 'streaming'

  function handleSend() {
    const text = draft.trim()
    if (!text || streaming) return

    // Build the full message history (system-less for now) to send as context.
    const history: ChatMessage[] = turns.map((t) => ({
      role: t.role === 'user' ? 'CHAT_ROLE_USER' : 'CHAT_ROLE_ASSISTANT',
      content: t.content,
    }))
    const userTurn: ChatMessage = { role: 'CHAT_ROLE_USER', content: text }

    // Optimistically add the user's turn to the transcript and clear the draft.
    setTurns((prev) => [...prev, { role: 'user', content: text }])
    setDraft('')
    committedRef.current = false

    chat.send({
      model: model || undefined,
      provider: provider || undefined,
      messages: [...history, userTurn],
    })
  }

  function handleKeyDown(e: React.KeyboardEvent<HTMLTextAreaElement>) {
    // Enter sends; Shift+Enter inserts a newline (chat-app convention).
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault()
      handleSend()
    }
  }

  function handleNewChat() {
    chat.reset()
    setTurns([])
    setDraft('')
    committedRef.current = false
  }

  const usage = usageQ.data

  return (
    <div className="flex h-[calc(100vh-7rem)] flex-col">
      <PageHeader
        title="AI Playground"
        description="Chat against the AI Gateway — streamed token-by-token, with provider failover, a semantic cache, and per-team token budgets."
        actions={
          <button type="button" className="btn-secondary" onClick={handleNewChat}>
            New chat
          </button>
        }
      />

      {/* ---- Control bar: model/provider selectors + usage tile ---- */}
      <div className="mb-4 grid grid-cols-1 gap-4 lg:grid-cols-3">
        <Card className="p-4 lg:col-span-2">
          <div className="grid grid-cols-1 gap-3 sm:grid-cols-2">
            <div>
              <label htmlFor="chat-model" className="label">
                Model
              </label>
              <select
                id="chat-model"
                className="input"
                value={model}
                onChange={(e) => setModel(e.target.value)}
                disabled={modelOptions.length === 0}
              >
                {modelOptions.length === 0 ? (
                  <option value="">{providersQ.isLoading ? 'Loading…' : 'No models available'}</option>
                ) : (
                  modelOptions.map((o) => (
                    <option key={o.value} value={o.value}>
                      {o.label}
                    </option>
                  ))
                )}
              </select>
            </div>
            <div>
              <label htmlFor="chat-provider" className="label">
                Provider (optional pin)
              </label>
              <select
                id="chat-provider"
                className="input"
                value={provider}
                onChange={(e) => setProvider(e.target.value as ProviderKind | '')}
              >
                <option value="">Auto (route + failover)</option>
                {providers.map((p) => (
                  <option key={p.kind} value={p.kind}>
                    {p.name} ({p.circuitState})
                  </option>
                ))}
              </select>
            </div>
          </div>
          {/* Provider health chips so the user sees circuit state at a glance. */}
          {providers.length > 0 && (
            <div className="mt-3 flex flex-wrap gap-2">
              {providers.map((p) => (
                <span
                  key={p.kind}
                  className="inline-flex items-center gap-1.5 rounded-full bg-ink-50 px-2.5 py-1 text-xs text-ink-600 ring-1 ring-ink-200"
                  title={p.enabled ? 'enabled' : 'disabled'}
                >
                  <span
                    className={`h-1.5 w-1.5 rounded-full ${
                      p.circuitState === 'CLOSED'
                        ? 'bg-emerald-500'
                        : p.circuitState === 'HALF_OPEN'
                          ? 'bg-amber-500'
                          : 'bg-red-500'
                    }`}
                    aria-hidden="true"
                  />
                  {p.name}
                  <span className="text-ink-400">· {p.circuitState}</span>
                </span>
              ))}
            </div>
          )}
        </Card>

        <StatCard
          label="Tokens used (your team)"
          value={usage?.total ? usage.total.totalTokens.toLocaleString() : '—'}
          hint={
            usage
              ? usage.budgetTokens === '0'
                ? 'unlimited budget'
                : `${Number(usage.remainingTokens).toLocaleString()} of ${Number(
                    usage.budgetTokens,
                  ).toLocaleString()} remaining`
              : undefined
          }
          icon={<ExperimentsIcon className="h-5 w-5" />}
          tone="brand"
          degraded={usageQ.isError}
        />
      </div>

      {/* ---- Transcript ---- */}
      <Card className="flex min-h-0 flex-1 flex-col">
        <div ref={scrollRef} className="flex-1 space-y-4 overflow-y-auto px-5 py-5">
          {turns.length === 0 && chat.status === 'idle' ? (
            <div className="flex h-full flex-col items-center justify-center text-center text-ink-400">
              <ExperimentsIcon className="mb-3 h-10 w-10 text-ink-300" />
              <p className="text-sm font-medium text-ink-500">Start a conversation</p>
              <p className="mt-1 max-w-sm text-xs">
                Pick a model and send a prompt. Responses stream in live; the readout shows which
                provider served it and the token cost.
              </p>
            </div>
          ) : (
            <>
              {turns.map((t, i) => (
                <Bubble key={i} role={t.role} text={t.content} />
              ))}
              {/* The in-flight assistant turn (live buffer). Once 'done' it is
                  committed into `turns`, so render the live bubble only while it
                  has NOT yet been committed to avoid a duplicate. */}
              {streaming && <Bubble role="assistant" text={chat.assistant} streaming />}
            </>
          )}

          {chat.status === 'error' && chat.error && (
            <div className="rounded-lg bg-red-50 px-4 py-3 text-sm text-red-700 ring-1 ring-red-200" role="alert">
              {chat.error}
            </div>
          )}
        </div>

        {/* Completion readout: provider served-by, cache-hit, token usage + cost. */}
        {chat.status === 'done' && chat.meta && <CompletionReadout meta={chat.meta} />}

        {/* ---- Composer ---- */}
        <div className="border-t border-ink-100 p-4">
          <div className="flex items-end gap-3">
            <textarea
              className="input min-h-[2.75rem] resize-none"
              rows={1}
              placeholder="Send a message…  (Enter to send, Shift+Enter for newline)"
              value={draft}
              onChange={(e) => setDraft(e.target.value)}
              onKeyDown={handleKeyDown}
              aria-label="Message"
            />
            {streaming ? (
              <button type="button" className="btn-secondary shrink-0" onClick={chat.stop}>
                Stop
              </button>
            ) : (
              <button
                type="button"
                className="btn-primary shrink-0"
                onClick={handleSend}
                disabled={!draft.trim() || !model}
              >
                Send
              </button>
            )}
          </div>
        </div>
      </Card>
    </div>
  )
}

// ---- Sub-components --------------------------------------------------------

/** One transcript turn. Model output is rendered as TEXT (whitespace-preserved)
 *  — NEVER as HTML — so untrusted markup in a completion can't execute. */
function Bubble({
  role,
  text,
  streaming = false,
}: {
  role: 'user' | 'assistant'
  text: string
  streaming?: boolean
}) {
  const isUser = role === 'user'
  return (
    <div className={`flex ${isUser ? 'justify-end' : 'justify-start'}`}>
      <div
        className={`max-w-[80%] whitespace-pre-wrap break-words rounded-2xl px-4 py-2.5 text-sm ${
          isUser
            ? 'bg-brand-600 text-white'
            : 'bg-ink-50 text-ink-800 ring-1 ring-ink-200'
        }`}
      >
        {text}
        {streaming && <span className="ml-0.5 inline-block animate-pulse">▋</span>}
      </div>
    </div>
  )
}

/** The on-completion metadata strip: which backend served, cache hit, finish
 *  reason, and the token usage + cost. */
function CompletionReadout({
  meta,
}: {
  meta: NonNullable<ReturnType<typeof useChat>['meta']>
}) {
  const cost =
    meta.usage && meta.usage.costMicroUsd
      ? `$${(Number(meta.usage.costMicroUsd) / 1_000_000).toFixed(6)}`
      : '—'
  return (
    <div className="flex flex-wrap items-center gap-x-4 gap-y-2 border-t border-ink-100 bg-ink-50/60 px-5 py-2.5 text-xs text-ink-600">
      {meta.servedBy && (
        <span className="inline-flex items-center gap-1.5">
          Served by <StatusPill value={meta.servedBy} />
        </span>
      )}
      <span className="inline-flex items-center gap-1.5">
        Cache{' '}
        <span
          className={`font-semibold ${meta.cacheHit ? 'text-emerald-600' : 'text-ink-500'}`}
        >
          {meta.cacheHit ? 'HIT' : 'miss'}
        </span>
      </span>
      {meta.finishReason && (
        <span>Finish: {humanizeEnum(meta.finishReason, 'FINISH_REASON_')}</span>
      )}
      {meta.usage && (
        <span className="font-mono">
          {meta.usage.promptTokens}↑ / {meta.usage.completionTokens}↓ ={' '}
          <span className="font-semibold text-ink-800">{meta.usage.totalTokens}</span> tok
        </span>
      )}
      <span className="font-mono">{cost}</span>
      {meta.requestId && <span className="text-ink-400">req {meta.requestId.slice(0, 8)}</span>}
    </div>
  )
}
