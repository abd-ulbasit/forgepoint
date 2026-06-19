// ============================================================================
// EvalsPage — the LLM-EVAL DASHBOARD (L4).
// ============================================================================
// The read surface over the model-monitor's LLM-as-judge scores. A judge model
// scores a sample of completions on four 0–5 axes — relevance, coherence,
// safety, overall — and the monitor ALSO rolls these into an `llm_quality`
// performance-drift signal that can trip auto-retrain. This page makes that
// quality story visible:
//
//   1. PER-MODEL SUMMARY — average score per axis, computed CLIENT-SIDE from the
//      eval list (the BFF stays logic-free; aggregation that's purely a view
//      concern belongs in the view).
//   2. A SCORES-BY-AXIS bar chart (recharts) for the selected/overall model — a
//      quick visual of where quality is strong vs weak.
//   3. The raw SCORES TABLE (model · relevance · coherence · safety · overall ·
//      scored badge · time).
//   4. A QUALITY-DRIFT strip that surfaces any `llm_quality` drift report from
//      GET /api/v1/drift-reports, cross-linking to Monitoring for the full loop.
//
// All values are NUMBERS or enum strings; there is no untrusted HTML here, but we
// still render the model name + request id as TEXT (React-escaped) on principle.

import { useMemo, useState } from 'react'
import { Link } from 'react-router-dom'
import {
  Bar,
  BarChart,
  CartesianGrid,
  Cell,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts'
import { useEvalScores, useDriftReports } from '@/hooks/queries'
import { usePagination } from '@/hooks/usePagination'
import { EmptyState, ErrorState, TableSkeleton } from '@/components/states'
import { Card, CardHeader, PageHeader, PaginationBar, Table, Td, Th, Mono } from '@/components/ui'
import { EvalsIcon } from '@/components/layout/icons'
import { formatRelative, shortId } from '@/lib/format'
import type { DriftReport, EvalScore } from '@/api/types'

// The four judged axes, in display order. Centralized so the summary, the chart,
// and the table all iterate the same set.
const AXES = [
  { key: 'relevance', label: 'Relevance' },
  { key: 'coherence', label: 'Coherence' },
  { key: 'safety', label: 'Safety' },
  { key: 'overall', label: 'Overall' },
] as const

type AxisKey = (typeof AXES)[number]['key']

// A per-model rollup of averages + sample count, computed from the eval list.
interface ModelSummary {
  model: string
  count: number
  averages: Record<AxisKey, number>
}

/** Color a 0–5 score: green (good) → amber → red (poor). Used for the chart bars
 *  and the table cells so quality reads at a glance. */
function scoreColor(v: number): string {
  if (v >= 4) return '#10b981' // emerald-500
  if (v >= 3) return '#f59e0b' // amber-500
  return '#ef4444' // red-500
}

/** Average the four axes across the scored samples, grouped by model. We only
 *  count rows where `scored` is true — an unscored placeholder would drag the
 *  average toward zero and misrepresent quality. */
function summarize(scores: EvalScore[]): ModelSummary[] {
  const byModel = new Map<string, { count: number; sums: Record<AxisKey, number> }>()
  for (const s of scores) {
    if (!s.scored) continue
    let entry = byModel.get(s.model)
    if (!entry) {
      entry = { count: 0, sums: { relevance: 0, coherence: 0, safety: 0, overall: 0 } }
      byModel.set(s.model, entry)
    }
    entry.count += 1
    for (const { key } of AXES) entry.sums[key] += s[key]
  }
  const out: ModelSummary[] = []
  for (const [model, { count, sums }] of byModel) {
    const averages = {
      relevance: count ? sums.relevance / count : 0,
      coherence: count ? sums.coherence / count : 0,
      safety: count ? sums.safety / count : 0,
      overall: count ? sums.overall / count : 0,
    }
    out.push({ model, count, averages })
  }
  // Highest overall first — the strongest model leads.
  out.sort((a, b) => b.averages.overall - a.averages.overall)
  return out
}

export function EvalsPage() {
  const { page, pageSize, pageToken, goNext, reset } = usePagination(25)
  const evals = useEvalScores({ pageSize, pageToken })
  // Quality drift is a small side-read; one page of recent reports is enough to
  // surface any active llm_quality signal.
  const drift = useDriftReports({ pageSize: 50 })

  const scores = evals.data?.scores ?? []
  const total = evals.data?.pagination.totalCount ?? 0
  const nextToken = evals.data?.pagination.nextPageToken ?? ''

  const summaries = useMemo(() => summarize(scores), [scores])

  // The model whose per-axis bars are charted. Defaults to the top model; the
  // user can switch via the summary cards.
  const [focusModel, setFocusModel] = useState<string | null>(null)
  const focus = useMemo(
    () => summaries.find((s) => s.model === focusModel) ?? summaries[0] ?? null,
    [summaries, focusModel],
  )

  // The active llm_quality drift signals — a DriftReport carrying a metric named
  // "llm_quality" is the eval→drift bridge the monitor emits.
  const qualityDrift = useMemo(() => qualityDriftReports(drift.data?.reports ?? []), [drift.data])

  return (
    <div>
      <PageHeader
        title="LLM Evals"
        description="LLM-as-judge quality scores across relevance, coherence, safety, and overall — plus the llm_quality drift that can trip auto-retrain."
      />

      {/* ---- Quality-drift strip ---- */}
      {qualityDrift.length > 0 && (
        <div className="mb-6 space-y-2">
          {qualityDrift.map((q) => (
            <QualityDriftBanner key={q.report.id} report={q.report} score={q.score} severity={q.severity} />
          ))}
        </div>
      )}

      {/* ---- Per-model summary cards ---- */}
      {evals.isLoading ? (
        <Card className="mb-6">
          <TableSkeleton rows={2} cols={5} />
        </Card>
      ) : summaries.length > 0 ? (
        <div className="mb-6 grid grid-cols-1 gap-4 md:grid-cols-2 xl:grid-cols-3">
          {summaries.map((s) => (
            <ModelSummaryCard
              key={s.model}
              summary={s}
              active={focus?.model === s.model}
              onSelect={() => setFocusModel(s.model)}
            />
          ))}
        </div>
      ) : null}

      <div className="grid grid-cols-1 gap-6 lg:grid-cols-5">
        {/* ---- Per-axis bar chart for the focused model ---- */}
        <Card className="lg:col-span-2">
          <CardHeader
            title="Score by axis"
            subtitle={focus ? `${focus.model} · ${focus.count} scored` : 'Average per axis'}
          />
          <div className="p-5">
            {focus ? (
              <div className="h-56 w-full">
                <ResponsiveContainer width="100%" height="100%">
                  <BarChart
                    data={AXES.map((a) => ({ name: a.label, value: Number(focus.averages[a.key].toFixed(2)) }))}
                    margin={{ top: 4, right: 8, bottom: 4, left: -20 }}
                  >
                    <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" vertical={false} />
                    <XAxis dataKey="name" tick={{ fontSize: 11, fill: '#64748b' }} tickLine={false} axisLine={{ stroke: '#e2e8f0' }} />
                    <YAxis domain={[0, 5]} tick={{ fontSize: 11, fill: '#64748b' }} tickLine={false} axisLine={false} />
                    <Tooltip
                      cursor={{ fill: '#f1f5f9' }}
                      contentStyle={{ fontSize: 12, borderRadius: 8, border: '1px solid #e2e8f0' }}
                    />
                    <Bar dataKey="value" radius={[4, 4, 0, 0]} maxBarSize={56}>
                      {AXES.map((a) => (
                        <Cell key={a.key} fill={scoreColor(focus.averages[a.key])} />
                      ))}
                    </Bar>
                  </BarChart>
                </ResponsiveContainer>
              </div>
            ) : (
              <p className="py-12 text-center text-sm text-ink-400">No scored evals to chart yet.</p>
            )}
          </div>
        </Card>

        {/* ---- Raw scores table ---- */}
        <Card className="lg:col-span-3">
          <CardHeader title="Recent evals" subtitle="Per-response judge scores, newest first" />
          {evals.isLoading ? (
            <TableSkeleton rows={6} cols={6} />
          ) : evals.isError ? (
            <ErrorState
              message={evals.error instanceof Error ? evals.error.message : undefined}
              onRetry={() => evals.refetch()}
            />
          ) : scores.length === 0 ? (
            <EmptyState
              title="No evals yet"
              description="Quality scores appear here as the monitor judges sampled completions."
              icon={<EvalsIcon className="h-6 w-6" />}
            />
          ) : (
            <>
              <Table>
                <thead>
                  <tr className="border-b border-ink-100">
                    <Th>Model</Th>
                    <Th className="text-center">Rel</Th>
                    <Th className="text-center">Coh</Th>
                    <Th className="text-center">Saf</Th>
                    <Th className="text-center">Overall</Th>
                    <Th>Scored</Th>
                    <Th>When</Th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-ink-100">
                  {scores.map((s, i) => (
                    <tr key={`${s.requestId}-${i}`} className="hover:bg-ink-50">
                      <Td>
                        <span className="font-medium text-ink-800">{s.model || '—'}</span>
                        {s.requestId && (
                          <p className="text-xs text-ink-400">req {shortId(s.requestId)}</p>
                        )}
                      </Td>
                      <ScoreCell value={s.relevance} scored={s.scored} />
                      <ScoreCell value={s.coherence} scored={s.scored} />
                      <ScoreCell value={s.safety} scored={s.scored} />
                      <ScoreCell value={s.overall} scored={s.scored} strong />
                      <Td>
                        {s.scored ? (
                          <span className="pill bg-emerald-50 text-emerald-700 ring-emerald-200">
                            <span className="h-1.5 w-1.5 rounded-full bg-current" aria-hidden="true" />
                            Scored
                          </span>
                        ) : (
                          <span className="pill bg-ink-100 text-ink-500 ring-ink-200">
                            <span className="h-1.5 w-1.5 rounded-full bg-current" aria-hidden="true" />
                            Pending
                          </span>
                        )}
                      </Td>
                      <Td className="text-ink-500">{formatRelative(s.createdAt)}</Td>
                    </tr>
                  ))}
                </tbody>
              </Table>
              <PaginationBar
                total={total}
                shown={scores.length}
                hasNext={Boolean(nextToken) && !evals.isFetching}
                onNext={() => goNext(nextToken)}
                onReset={reset}
                page={page}
              />
            </>
          )}
        </Card>
      </div>
    </div>
  )
}

// ---- Sub-components --------------------------------------------------------

/** One 0–5 score cell, color-coded. Unscored rows render a muted dash. */
function ScoreCell({ value, scored, strong = false }: { value: number; scored: boolean; strong?: boolean }) {
  if (!scored) {
    return <Td className="text-center text-ink-300">—</Td>
  }
  return (
    <Td className="text-center">
      <span
        className={`inline-flex h-6 min-w-6 items-center justify-center rounded px-1.5 text-xs font-semibold ${strong ? 'ring-1' : ''}`}
        style={{
          color: scoreColor(value),
          backgroundColor: `${scoreColor(value)}1a`, // ~10% alpha
        }}
      >
        {value}
      </span>
    </Td>
  )
}

/** A per-model summary card; clicking focuses its bars in the chart. */
function ModelSummaryCard({
  summary,
  active,
  onSelect,
}: {
  summary: ModelSummary
  active: boolean
  onSelect: () => void
}) {
  return (
    <button
      type="button"
      onClick={onSelect}
      className={`card p-4 text-left transition-shadow hover:shadow-card-hover ${active ? 'ring-2 ring-brand-300' : ''}`}
    >
      <div className="flex items-center justify-between">
        <p className="truncate text-sm font-semibold text-ink-900">{summary.model || '—'}</p>
        <span className="text-xs text-ink-400">{summary.count} scored</span>
      </div>
      <div className="mt-3 grid grid-cols-4 gap-2">
        {AXES.map((a) => (
          <div key={a.key} className="text-center">
            <p className="text-[10px] font-medium uppercase tracking-wide text-ink-400">{a.label.slice(0, 3)}</p>
            <p className="mt-0.5 text-lg font-bold" style={{ color: scoreColor(summary.averages[a.key]) }}>
              {summary.averages[a.key].toFixed(1)}
            </p>
          </div>
        ))}
      </div>
    </button>
  )
}

/** The llm_quality drift banner — links to Monitoring for the full report. */
function QualityDriftBanner({
  report,
  score,
  severity,
}: {
  report: DriftReport
  score: number
  severity: string
}) {
  const isCritical = severity === 'DRIFT_SEVERITY_CRITICAL'
  return (
    <div
      className={`flex flex-wrap items-center justify-between gap-3 rounded-lg px-4 py-3 text-sm ring-1 ${
        isCritical
          ? 'bg-red-50 text-red-800 ring-red-200'
          : 'bg-amber-50 text-amber-800 ring-amber-200'
      }`}
      role="alert"
    >
      <div className="flex items-center gap-2">
        <EvalsIcon className="h-4 w-4 flex-shrink-0" />
        <span>
          Quality drift on <span className="font-semibold">{report.modelName || '—'}</span> —{' '}
          <Mono>llm_quality</Mono> score {score.toFixed(3)} ({isCritical ? 'critical' : 'warning'})
        </span>
      </div>
      <Link to="/monitoring" className="font-semibold underline-offset-2 hover:underline">
        View in Monitoring →
      </Link>
    </div>
  )
}

// ---- Drift helper ----------------------------------------------------------

interface QualitySignal {
  report: DriftReport
  score: number
  severity: string
}

/** Pick out the reports whose metrics include the `llm_quality` axis (the eval→
 *  drift bridge). We surface the metric's own score/severity, not the report's,
 *  so the banner reflects the quality signal specifically. */
function qualityDriftReports(reports: DriftReport[]): QualitySignal[] {
  const out: QualitySignal[] = []
  for (const report of reports) {
    const metric = report.metrics.find((m) => m.name === 'llm_quality')
    if (!metric) continue
    // Only surface non-OK signals — an OK quality metric is the happy path and
    // doesn't need a banner.
    if (metric.severity === 'DRIFT_SEVERITY_OK' || metric.severity === 'DRIFT_SEVERITY_UNSPECIFIED') {
      continue
    }
    out.push({ report, score: metric.score, severity: metric.severity })
  }
  return out
}
