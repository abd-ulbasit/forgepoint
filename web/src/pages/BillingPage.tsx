// ============================================================================
// BillingPage — usage summary per team and meter.
// ============================================================================
// GET /api/v1/usage returns per-team UsageSummary rows (a map of meter -> usage)
// plus a grand total. The outbox-pattern billing service is the source; we just
// present the rolled-up numbers. Money arrives as int64 micros (string) and is
// formatted via formatMoney; quantities are int64 strings too.

import { useUsage } from '@/hooks/queries'
import { ErrorState, LoadingState } from '@/components/states'
import { Card, CardHeader, PageHeader, StatCard, Table, Td, Th } from '@/components/ui'
import { BillingIcon } from '@/components/layout/icons'
import { formatCompact, formatMoney, formatTime, humanizeEnum } from '@/lib/format'
import type { UsageSummary } from '@/api/types'

export function BillingPage() {
  const { data, isLoading, isError, error, refetch } = useUsage({ pageSize: 50 })

  if (isLoading) return <LoadingState label="Loading usage…" />
  if (isError || !data) {
    return <ErrorState message={error instanceof Error ? error.message : undefined} onRetry={() => refetch()} />
  }

  const summaries = data.summaries ?? []
  const totalMeters = summaries.reduce((acc, s) => acc + Object.keys(s.byMeter ?? {}).length, 0)

  return (
    <div>
      <PageHeader title="Billing & Usage" description="Metered consumption per team, aggregated from the billing service." />

      <div className="mb-6 grid grid-cols-1 gap-4 sm:grid-cols-3">
        <StatCard
          label="Grand total"
          value={formatMoney(data.grandTotal)}
          icon={<BillingIcon className="h-5 w-5" />}
          tone="emerald"
        />
        <StatCard label="Teams" value={summaries.length} tone="brand" />
        <StatCard label="Active meters" value={totalMeters} tone="sky" />
      </div>

      {summaries.length === 0 ? (
        <Card>
          <p className="px-5 py-12 text-center text-sm text-ink-400">No usage recorded for this period.</p>
        </Card>
      ) : (
        <div className="space-y-6">
          {summaries.map((s) => (
            <TeamUsageCard key={s.team || 'default'} summary={s} />
          ))}
        </div>
      )}
    </div>
  )
}

function TeamUsageCard({ summary }: { summary: UsageSummary }) {
  const meters = Object.entries(summary.byMeter ?? {})

  return (
    <Card>
      <CardHeader
        title={summary.team || 'All teams'}
        subtitle={
          summary.periodStart
            ? `${formatTime(summary.periodStart)} – ${summary.periodEnd ? formatTime(summary.periodEnd) : 'now'}`
            : undefined
        }
        action={<span className="text-sm font-semibold text-ink-900">{formatMoney(summary.totalCost)}</span>}
      />
      {meters.length === 0 ? (
        <p className="px-5 py-8 text-center text-sm text-ink-400">No metered usage.</p>
      ) : (
        <Table>
          <thead>
            <tr className="border-b border-ink-100">
              <Th>Meter</Th>
              <Th className="text-right">Quantity</Th>
              <Th className="text-right">Cost</Th>
            </tr>
          </thead>
          <tbody className="divide-y divide-ink-100">
            {meters.map(([key, usage]) => (
              <tr key={key} className="hover:bg-ink-50">
                <Td className="font-medium text-ink-700">{humanizeEnum(usage.meterType, 'METER_TYPE_')}</Td>
                <Td className="text-right font-mono text-ink-700">{formatCompact(usage.totalQuantity)}</Td>
                <Td className="text-right font-mono text-ink-900">{formatMoney(usage.totalCost)}</Td>
              </tr>
            ))}
          </tbody>
        </Table>
      )}
    </Card>
  )
}
