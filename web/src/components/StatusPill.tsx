// ============================================================================
// StatusPill — the one component that maps every backend enum to a color.
// ============================================================================
// All status enums in the platform (execution status, step status, drift
// severity, version status, run status, monitor state, notification severity)
// funnel through ONE tone vocabulary: neutral / info / running / success /
// warning / danger. Centralizing the mapping here means the entire app speaks a
// consistent visual language for state, and adding a new enum value is a
// one-line change.

import { humanizeEnum } from '@/lib/format'

type Tone = 'neutral' | 'info' | 'running' | 'success' | 'warning' | 'danger'

const TONE_CLASS: Record<Tone, string> = {
  neutral: 'bg-ink-100 text-ink-600 ring-ink-200',
  info: 'bg-brand-50 text-brand-700 ring-brand-200',
  running: 'bg-sky-50 text-sky-700 ring-sky-200',
  success: 'bg-emerald-50 text-emerald-700 ring-emerald-200',
  warning: 'bg-amber-50 text-amber-700 ring-amber-200',
  danger: 'bg-red-50 text-red-700 ring-red-200',
}

// Whether the tone should show a pulsing dot (in-flight states).
const PULSE_TONES = new Set<Tone>(['running'])

/**
 * The master tone map keyed by enum VALUE. We list values across every enum;
 * because the values are globally unique SCREAMING_SNAKE strings, a single flat
 * map is unambiguous and exhaustive.
 */
const VALUE_TONE: Record<string, Tone> = {
  // Execution status
  EXECUTION_STATUS_PENDING: 'neutral',
  EXECUTION_STATUS_RUNNING: 'running',
  EXECUTION_STATUS_COMPENSATING: 'warning',
  EXECUTION_STATUS_COMPLETED: 'success',
  EXECUTION_STATUS_FAILED: 'danger',
  EXECUTION_STATUS_CANCELLED: 'neutral',
  // Step status
  STEP_STATUS_PENDING: 'neutral',
  STEP_STATUS_RUNNING: 'running',
  STEP_STATUS_COMPLETED: 'success',
  STEP_STATUS_FAILED: 'danger',
  STEP_STATUS_SKIPPED: 'neutral',
  STEP_STATUS_COMPENSATING: 'warning',
  STEP_STATUS_COMPENSATED: 'warning',
  STEP_STATUS_COMPENSATION_FAILED: 'danger',
  // Version status
  VERSION_STATUS_PENDING_UPLOAD: 'warning',
  VERSION_STATUS_READY: 'success',
  VERSION_STATUS_FAILED: 'danger',
  // Model stage
  MODEL_STAGE_DEV: 'neutral',
  MODEL_STAGE_STAGING: 'info',
  MODEL_STAGE_PRODUCTION: 'success',
  MODEL_STAGE_ARCHIVED: 'neutral',
  // Run status
  RUN_STATUS_RUNNING: 'running',
  RUN_STATUS_FINISHED: 'success',
  RUN_STATUS_FAILED: 'danger',
  RUN_STATUS_KILLED: 'warning',
  // Drift severity
  DRIFT_SEVERITY_OK: 'success',
  DRIFT_SEVERITY_WARNING: 'warning',
  DRIFT_SEVERITY_CRITICAL: 'danger',
  // Monitor state
  MONITOR_STATE_PENDING_BASELINE: 'neutral',
  MONITOR_STATE_WARMING_UP: 'running',
  MONITOR_STATE_ACTIVE: 'success',
  MONITOR_STATE_PAUSED: 'warning',
  // Notification severity
  NOTIFICATION_SEVERITY_INFO: 'info',
  NOTIFICATION_SEVERITY_WARNING: 'warning',
  NOTIFICATION_SEVERITY_ERROR: 'danger',
  NOTIFICATION_SEVERITY_CRITICAL: 'danger',
  // AI Gateway provider kind (which backend served the completion)
  PROVIDER_KIND_OLLAMA: 'info',
  PROVIDER_KIND_STUB: 'neutral',
  PROVIDER_KIND_OPENAI: 'success',
  PROVIDER_KIND_ANTHROPIC: 'success',
  PROVIDER_KIND_UNSPECIFIED: 'neutral',
}

/** Strip the longest known prefix so the label reads cleanly. */
function labelFor(value: string): string {
  const prefixes = [
    'EXECUTION_STATUS_',
    'STEP_STATUS_',
    'VERSION_STATUS_',
    'MODEL_STAGE_',
    'RUN_STATUS_',
    'DRIFT_SEVERITY_',
    'DRIFT_TYPE_',
    'MONITOR_STATE_',
    'NOTIFICATION_SEVERITY_',
    'PIPELINE_TYPE_',
    'STEP_TYPE_',
    'METER_TYPE_',
    'PROVIDER_KIND_',
  ]
  const p = prefixes.find((pre) => value.startsWith(pre))
  return humanizeEnum(value, p)
}

export function StatusPill({
  value,
  toneOverride,
  className = '',
}: {
  value: string
  toneOverride?: Tone
  className?: string
}) {
  const tone = toneOverride ?? VALUE_TONE[value] ?? 'neutral'
  const pulse = PULSE_TONES.has(tone)
  return (
    <span className={`pill ${TONE_CLASS[tone]} ${className}`}>
      <span
        className={`h-1.5 w-1.5 rounded-full bg-current ${pulse ? 'animate-pulse' : ''}`}
        aria-hidden="true"
      />
      {labelFor(value)}
    </span>
  )
}
