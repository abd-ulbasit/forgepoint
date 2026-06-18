// ============================================================================
// ExecutionDetailPage — the LIVE saga view (the platform's headline).
// ============================================================================
// Opens the SSE stream (useExecutionWatch) and renders each step's status as it
// changes in REAL TIME. The visual story is the saga state machine: steps move
// PENDING → RUNNING → COMPLETED, and on failure the orchestrator COMPENSATES in
// reverse (steps flip to COMPENSATING/COMPENSATED) — exactly the transitions an
// interviewer probes on "how does your saga roll back?". The most-recently
// changed step pulses so the eye follows the action.
//
// We SEED the view with a one-shot REST GetExecution (so the page isn't blank
// before the first frame), then let the stream take over and supersede it.

import { useParams } from 'react-router-dom'
import { useExecution } from '@/hooks/queries'
import { useExecutionWatch } from '@/hooks/useExecutionWatch'
import { LoadingState } from '@/components/states'
import { Card, CardHeader, KeyValue, PageHeader, Mono } from '@/components/ui'
import { StatusPill } from '@/components/StatusPill'
import { formatDuration, formatTime, shortId } from '@/lib/format'
import type { Execution, StepExecution } from '@/api/types'

const TERMINAL: ReadonlySet<string> = new Set([
  'EXECUTION_STATUS_COMPLETED',
  'EXECUTION_STATUS_FAILED',
  'EXECUTION_STATUS_CANCELLED',
])

export function ExecutionDetailPage() {
  const { id = '' } = useParams()

  // 1) One-shot REST seed so the timeline renders immediately.
  const seed = useExecution(id)
  // 2) The live SSE stream — the authoritative source once connected.
  const watch = useExecutionWatch(id, Boolean(id))

  // Prefer the live execution; fall back to the REST snapshot.
  const execution: Execution | null = watch.execution ?? seed.data?.execution ?? null

  if (!execution && seed.isLoading) return <LoadingState label="Loading execution…" />

  if (!execution) {
    return (
      <div>
        <PageHeader title="Execution" />
        <Card>
          <p className="px-5 py-10 text-center text-sm text-ink-400">
            {seed.isError ? 'Execution not found.' : 'No data.'}
          </p>
        </Card>
      </div>
    )
  }

  const isTerminal = TERMINAL.has(execution.status)
  const steps = [...execution.stepExecutions].sort(stepOrder)

  return (
    <div>
      <PageHeader
        title={`Execution ${shortId(execution.id)}`}
        description={`Pipeline ${shortId(execution.pipelineId)} · triggered by ${execution.triggeredBy || 'unknown'}`}
        actions={<LiveBadge connState={watch.connState} isTerminal={isTerminal} />}
      />

      {/* Summary */}
      <Card className="mb-6">
        <div className="p-5">
          <div className="mb-4 flex items-center gap-3">
            <StatusPill value={execution.status} />
            <span className="text-sm text-ink-500">
              {formatDuration(execution.startedAt, execution.completedAt)}
            </span>
          </div>
          <KeyValue
            items={[
              { label: 'Execution ID', value: <Mono>{shortId(execution.id)}</Mono> },
              { label: 'Current step', value: execution.currentStep || '—' },
              { label: 'Started', value: formatTime(execution.startedAt) },
              { label: 'Completed', value: execution.completedAt ? formatTime(execution.completedAt) : '—' },
            ]}
          />
          {execution.error && (
            <div className="mt-4 rounded-lg border border-red-200 bg-red-50 px-3 py-2.5 text-sm text-red-700">
              <span className="font-semibold">Failure: </span>
              {execution.error}
            </div>
          )}
        </div>
      </Card>

      {/* Live step timeline — the saga in motion */}
      <Card>
        <CardHeader
          title="Step timeline"
          subtitle="Saga steps update live as the orchestrator advances or compensates."
        />
        {steps.length === 0 ? (
          <p className="px-5 py-10 text-center text-sm text-ink-400">Waiting for the first step to start…</p>
        ) : (
          <ol className="relative px-5 py-5">
            {/* The connecting rail behind the dots. */}
            <div className="absolute bottom-6 left-[34px] top-8 w-px bg-ink-200" aria-hidden="true" />
            {steps.map((step) => (
              <StepRow key={step.id || step.stepId} step={step} highlight={watch.lastChangedStepId === step.stepId} />
            ))}
          </ol>
        )}
      </Card>

      {watch.connState === 'error' && !isTerminal && (
        <p className="mt-3 text-center text-xs text-amber-600">
          Live stream interrupted{watch.error ? `: ${watch.error}` : ''}. Showing the latest snapshot.
        </p>
      )}
    </div>
  )
}

/** Order steps by start time, then by id, so the timeline reads top-to-bottom. */
function stepOrder(a: StepExecution, b: StepExecution): number {
  const ta = a.startedAt ? new Date(a.startedAt).getTime() : Number.MAX_SAFE_INTEGER
  const tb = b.startedAt ? new Date(b.startedAt).getTime() : Number.MAX_SAFE_INTEGER
  if (ta !== tb) return ta - tb
  return (a.stepId || '').localeCompare(b.stepId || '')
}

const STEP_DOT: Record<string, string> = {
  STEP_STATUS_PENDING: 'bg-ink-200 text-ink-400',
  STEP_STATUS_RUNNING: 'bg-sky-500 text-white',
  STEP_STATUS_COMPLETED: 'bg-emerald-500 text-white',
  STEP_STATUS_FAILED: 'bg-red-500 text-white',
  STEP_STATUS_SKIPPED: 'bg-ink-200 text-ink-400',
  STEP_STATUS_COMPENSATING: 'bg-amber-500 text-white',
  STEP_STATUS_COMPENSATED: 'bg-amber-400 text-white',
  STEP_STATUS_COMPENSATION_FAILED: 'bg-red-600 text-white',
}

function StepRow({ step, highlight }: { step: StepExecution; highlight: boolean }) {
  const dot = STEP_DOT[step.status] ?? 'bg-ink-200 text-ink-400'
  const running = step.status === 'STEP_STATUS_RUNNING' || step.status === 'STEP_STATUS_COMPENSATING'

  return (
    <li
      className={`relative flex gap-4 pb-6 last:pb-0 ${highlight ? 'animate-fade-in' : ''}`}
    >
      {/* Status dot on the rail */}
      <span
        className={`relative z-10 mt-0.5 flex h-7 w-7 flex-shrink-0 items-center justify-center rounded-full ${dot} ${running ? 'ring-4 ring-sky-100' : ''}`}
        aria-hidden="true"
      >
        {running ? (
          <span className="h-2 w-2 animate-ping rounded-full bg-white" />
        ) : step.status === 'STEP_STATUS_COMPLETED' ? (
          <svg className="h-4 w-4" fill="none" viewBox="0 0 24 24" strokeWidth={3} stroke="currentColor">
            <path strokeLinecap="round" strokeLinejoin="round" d="M4.5 12.75l6 6 9-13.5" />
          </svg>
        ) : step.status === 'STEP_STATUS_FAILED' || step.status === 'STEP_STATUS_COMPENSATION_FAILED' ? (
          <svg className="h-4 w-4" fill="none" viewBox="0 0 24 24" strokeWidth={3} stroke="currentColor">
            <path strokeLinecap="round" strokeLinejoin="round" d="M6 18L18 6M6 6l12 12" />
          </svg>
        ) : (
          <span className="h-1.5 w-1.5 rounded-full bg-current" />
        )}
      </span>

      <div className={`min-w-0 flex-1 rounded-lg border px-4 py-3 transition-colors ${highlight ? 'border-brand-200 bg-brand-50/50' : 'border-ink-100 bg-white'}`}>
        <div className="flex items-center justify-between gap-3">
          <p className="truncate font-medium text-ink-900">{step.stepId || shortId(step.id)}</p>
          <StatusPill value={step.status} />
        </div>
        <div className="mt-1.5 flex flex-wrap items-center gap-x-4 gap-y-1 text-xs text-ink-500">
          <span>{formatDuration(step.startedAt, step.completedAt)}</span>
          {step.attempt > 1 && <span>attempt {step.attempt}</span>}
          {step.startedAt && <span>started {formatTime(step.startedAt)}</span>}
        </div>
        {step.error && (
          <p className="mt-2 rounded bg-red-50 px-2 py-1 text-xs text-red-700">{step.error}</p>
        )}
      </div>
    </li>
  )
}

function LiveBadge({ connState, isTerminal }: { connState: string; isTerminal: boolean }) {
  if (isTerminal || connState === 'closed') {
    return (
      <span className="pill bg-ink-100 text-ink-600 ring-ink-200">
        <span className="h-1.5 w-1.5 rounded-full bg-current" /> Finished
      </span>
    )
  }
  if (connState === 'open' || connState === 'connecting') {
    return (
      <span className="pill bg-sky-50 text-sky-700 ring-sky-200">
        <span className="h-1.5 w-1.5 animate-pulse rounded-full bg-current" />
        {connState === 'open' ? 'Live' : 'Connecting…'}
      </span>
    )
  }
  return (
    <span className="pill bg-amber-50 text-amber-700 ring-amber-200">
      <span className="h-1.5 w-1.5 rounded-full bg-current" /> Reconnecting
    </span>
  )
}
