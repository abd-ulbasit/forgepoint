// ============================================================================
// API TYPES — the TypeScript mirror of the BFF's JSON contract.
// ============================================================================
//
// These types are hand-written from the BFF handlers + the generated proto JSON
// shapes (services/bff/internal/handlers/*.go and gen/go/forgepoint/**). Two
// protojson conventions drive every field name and type here, so they are worth
// internalizing (this is the #1 source of UI<->backend drift):
//
//   1. FIELD NAMES are lowerCamelCase. The BFF marshals with protojson
//      UseProtoNames:false, so proto `task_type` becomes JSON `taskType`,
//      `created_at` -> `createdAt`, `next_page_token` -> `nextPageToken`, etc.
//
//   2. NUMBER WIDTHS: protojson encodes 64-bit ints (int64/uint64) as STRINGS
//      (JS numbers lose precision past 2^53), while 32-bit ints and doubles are
//      JSON numbers. So `sizeBytes`, `quantity`, `amountMicros`, and `sequence`
//      arrive as strings; `score`, `value`, `totalCount` arrive as numbers.
//      We type them accordingly and parse strings where we need arithmetic.
//
//   3. ENUMS serialize as their full SCREAMING_SNAKE name
//      (e.g. "EXECUTION_STATUS_RUNNING"). We keep them as string-literal unions
//      so the UI can switch on them exhaustively, and use a `| string` escape on
//      a couple to stay forward-compatible if the backend adds a value.
//
//   4. EmitUnpopulated:true means zero-valued fields are PRESENT (empty string,
//      0, [], etc.) rather than missing — so most fields are non-optional. We
//      still mark message-typed nested objects optional/nullable defensively.

// ---- Well-known wrappers ---------------------------------------------------

/** protojson Timestamp -> RFC3339 string (e.g. "2026-06-18T10:30:00Z"). */
export type Timestamp = string

/** protojson google.protobuf.Struct -> an arbitrary JSON object. */
export type Struct = Record<string, unknown>

export interface PaginationResponse {
  nextPageToken: string
  totalCount: number
}

// ---- Auth ------------------------------------------------------------------

export interface User {
  id: string
  email: string
  name: string
  team: string
  role: string
}

export interface LoginResponse {
  accessToken: string
  expiresAt: Timestamp
  user: User
}

// ---- Registry (models) -----------------------------------------------------

export type ModelStage =
  | 'MODEL_STAGE_UNSPECIFIED'
  | 'MODEL_STAGE_DEV'
  | 'MODEL_STAGE_STAGING'
  | 'MODEL_STAGE_PRODUCTION'
  | 'MODEL_STAGE_ARCHIVED'

export type VersionStatus =
  | 'VERSION_STATUS_UNSPECIFIED'
  | 'VERSION_STATUS_PENDING_UPLOAD'
  | 'VERSION_STATUS_READY'
  | 'VERSION_STATUS_FAILED'

export interface Model {
  id: string
  name: string
  description: string
  ownerId: string
  team: string
  framework: string
  taskType: string
  tags: Record<string, string>
  productionVersion: string
  latestVersion: string
  createdAt: Timestamp
  updatedAt: Timestamp
  archivedAt: Timestamp | null
}

export interface ModelVersion {
  id: string
  modelId: string
  version: string
  description: string
  artifactPath: string
  artifactDigest: string
  sizeBytes: string // int64 -> string
  metrics: Struct | null
  stage: ModelStage
  status: VersionStatus
  createdBy: string
  createdAt: Timestamp
}

export interface ListModelsResponse {
  models: Model[]
  pagination: PaginationResponse
}

export interface ListVersionsResponse {
  versions: ModelVersion[]
  pagination: PaginationResponse
}

export interface GetModelResponse {
  model: Model
}

export interface RegisterModelRequest {
  name: string
  description: string
  framework: string
  taskType: string
  tags?: Record<string, string>
  idempotencyKey?: string
}

export interface RegisterModelResponse {
  model: Model
  version?: ModelVersion
}

// ---- Pipelines / executions ------------------------------------------------

export type PipelineType =
  | 'PIPELINE_TYPE_UNSPECIFIED'
  | 'PIPELINE_TYPE_DEPLOYMENT_SAGA'
  | 'PIPELINE_TYPE_TRAINING_DAG'
  | 'PIPELINE_TYPE_BATCH_INFERENCE'

export type StepType =
  | 'STEP_TYPE_UNSPECIFIED'
  | 'STEP_TYPE_VALIDATE'
  | 'STEP_TYPE_BUILD'
  | 'STEP_TYPE_DEPLOY'
  | 'STEP_TYPE_CANARY'
  | 'STEP_TYPE_PROMOTE'
  | 'STEP_TYPE_TRAIN'
  | 'STEP_TYPE_EVALUATE'
  | 'STEP_TYPE_REGISTER'
  | 'STEP_TYPE_CUSTOM'

export type ExecutionStatus =
  | 'EXECUTION_STATUS_UNSPECIFIED'
  | 'EXECUTION_STATUS_PENDING'
  | 'EXECUTION_STATUS_RUNNING'
  | 'EXECUTION_STATUS_COMPENSATING'
  | 'EXECUTION_STATUS_COMPLETED'
  | 'EXECUTION_STATUS_FAILED'
  | 'EXECUTION_STATUS_CANCELLED'

export type StepStatus =
  | 'STEP_STATUS_UNSPECIFIED'
  | 'STEP_STATUS_PENDING'
  | 'STEP_STATUS_RUNNING'
  | 'STEP_STATUS_COMPLETED'
  | 'STEP_STATUS_FAILED'
  | 'STEP_STATUS_SKIPPED'
  | 'STEP_STATUS_COMPENSATING'
  | 'STEP_STATUS_COMPENSATED'
  | 'STEP_STATUS_COMPENSATION_FAILED'

export interface StepDefinition {
  id: string
  name: string
  type: StepType
  dependsOn: string[]
  compensationStepId: string
  config: Struct | null
  timeout: string // Duration -> "30s"
  maxRetries: number
}

export interface PipelineDefinition {
  id: string
  name: string
  type: PipelineType
  steps: StepDefinition[]
  createdBy: string
  createdAt: Timestamp
  team: string
}

export interface StepExecution {
  id: string
  executionId: string
  stepId: string
  status: StepStatus
  startedAt: Timestamp | null
  completedAt: Timestamp | null
  output: Struct | null
  error: string
  attempt: number
}

export interface Execution {
  id: string
  pipelineId: string
  status: ExecutionStatus
  currentStep: string
  stepExecutions: StepExecution[]
  triggeredBy: string
  startedAt: Timestamp | null
  completedAt: Timestamp | null
  input: Struct | null
  error: string
}

export interface ListPipelinesResponse {
  pipelines: PipelineDefinition[]
  pagination: PaginationResponse
}

export interface GetExecutionResponse {
  execution: Execution
}

export interface TriggerExecutionResponse {
  execution: Execution
}

/** SSE frame shape from GET /executions/{id}/watch (WatchExecutionResponse). */
export interface WatchExecutionFrame {
  execution: Execution
  changedStep: StepExecution | null
  sequence: string // uint64 -> string
  emittedAt: Timestamp | null
}

// ---- Experiments (runs) ----------------------------------------------------

export type RunStatus =
  | 'RUN_STATUS_UNSPECIFIED'
  | 'RUN_STATUS_RUNNING'
  | 'RUN_STATUS_FINISHED'
  | 'RUN_STATUS_FAILED'
  | 'RUN_STATUS_KILLED'

export type RunSource = 'RUN_SOURCE_UNSPECIFIED' | 'RUN_SOURCE_API' | 'RUN_SOURCE_EVENT'

export interface Param {
  key: string
  value: string
}

export interface MetricPoint {
  key: string
  value: number // double -> number
  step: string // int64 -> string
  timestamp: Timestamp | null
}

export interface Run {
  id: string
  experimentId: string
  displayName: string
  status: RunStatus
  source: RunSource
  modelVersionId: string
  ownerId: string
  params: Param[]
  finalMetrics: MetricPoint[]
  startedAt: Timestamp | null
  endedAt: Timestamp | null
  artifacts: Struct | null
}

export interface ListRunsResponse {
  runs: Run[]
  pagination: PaginationResponse
}

export interface GetRunResponse {
  run: Run
}

// ---- Monitoring (drift) ----------------------------------------------------

export type DriftSeverity =
  | 'DRIFT_SEVERITY_UNSPECIFIED'
  | 'DRIFT_SEVERITY_OK'
  | 'DRIFT_SEVERITY_WARNING'
  | 'DRIFT_SEVERITY_CRITICAL'

export type DriftType =
  | 'DRIFT_TYPE_UNSPECIFIED'
  | 'DRIFT_TYPE_DATA'
  | 'DRIFT_TYPE_PREDICTION'
  | 'DRIFT_TYPE_PERFORMANCE'

export type DriftMethod =
  | 'DRIFT_METHOD_UNSPECIFIED'
  | 'DRIFT_METHOD_PSI'
  | 'DRIFT_METHOD_KL'
  | 'DRIFT_METHOD_KS'

export type MonitorState =
  | 'MONITOR_STATE_UNSPECIFIED'
  | 'MONITOR_STATE_PENDING_BASELINE'
  | 'MONITOR_STATE_WARMING_UP'
  | 'MONITOR_STATE_ACTIVE'
  | 'MONITOR_STATE_PAUSED'

export interface DriftMetric {
  name: string
  method: DriftMethod
  score: number
  baselineValue: number
  currentValue: number
  severity: DriftSeverity
}

export interface ThresholdConfig {
  driftType: DriftType
  method: DriftMethod
  warnScore: number
  criticalScore: number
}

export interface Monitor {
  id: string
  modelName: string
  ownerTeam: string
  windowDuration: string // Duration
  windowSize: number
  minSamples: number
  thresholds: ThresholdConfig[]
  autoRetrain: boolean
  retrainPipelineId: string
  state: MonitorState
  baselineVersion: string
  baselineCapturedAt: Timestamp | null
  createdAt: Timestamp
  updatedAt: Timestamp
}

export interface ModelHealth {
  modelName: string
  modelVersion: string
  overallSeverity: DriftSeverity
  severityByType: Record<string, DriftSeverity>
  state: MonitorState
  latestReportId: string
  lastEventAt: Timestamp | null
  driftEventsTotal: string // int64
}

/** ListMonitorsResponse entries pair a Monitor with its ModelHealth. */
export interface MonitorEntry {
  monitor: Monitor
  health: ModelHealth | null
}

export interface ListMonitorsResponse {
  entries: MonitorEntry[]
  pagination: PaginationResponse
}

export interface DriftReport {
  id: string
  monitorId: string
  modelName: string
  modelVersion: string
  driftType: DriftType
  severity: DriftSeverity
  metrics: DriftMetric[]
  windowId: string
  sampleCount: number
  windowStart: Timestamp | null
  windowEnd: Timestamp | null
  createdAt: Timestamp
}

export interface ListDriftReportsResponse {
  reports: DriftReport[]
  pagination: PaginationResponse
}

// ---- Billing (usage) -------------------------------------------------------

export type MeterType =
  | 'METER_TYPE_UNSPECIFIED'
  | 'METER_TYPE_INFERENCE_REQUEST'
  | 'METER_TYPE_INFERENCE_TOKENS'
  | 'METER_TYPE_COMPUTE_SECONDS'
  | 'METER_TYPE_STORAGE_BYTES'

export interface Money {
  amountMicros: string // int64 -> string
  currencyCode: string
}

export interface MeterUsage {
  meterType: MeterType
  totalQuantity: string // int64
  totalCost: Money | null
}

export interface UsageSummary {
  team: string
  periodStart: Timestamp | null
  periodEnd: Timestamp | null
  byMeter: Record<string, MeterUsage>
  totalCost: Money | null
}

export interface GetUsageResponse {
  summaries: UsageSummary[]
  grandTotal: Money | null
  pagination: PaginationResponse
}

// ---- Notifications ---------------------------------------------------------

export type NotificationSeverity =
  | 'NOTIFICATION_SEVERITY_UNSPECIFIED'
  | 'NOTIFICATION_SEVERITY_INFO'
  | 'NOTIFICATION_SEVERITY_WARNING'
  | 'NOTIFICATION_SEVERITY_ERROR'
  | 'NOTIFICATION_SEVERITY_CRITICAL'

export interface Notification {
  id: string
  recipientUserId: string
  title: string
  body: string
  severity: NotificationSeverity
  channels: string[]
  read: boolean
  eventId: string
  eventType: string
  sourceService: string
  createdAt: Timestamp
  readAt: Timestamp | null
}

export interface ListNotificationsResponse {
  notifications: Notification[]
  pagination: PaginationResponse
  unreadCount: number
}

// ---- Dashboard (BFF aggregate) ---------------------------------------------

/** A degradable dashboard tile: `ok=false` means this panel's service failed
 *  but the others still rendered (BFF partial-failure tolerance). `data` is the
 *  raw nested JSON, shaped per tile — typed loosely and narrowed at the page. */
export interface DashboardTile<T = unknown> {
  ok: boolean
  error?: string
  data?: T
}

export interface DashboardResponse {
  models: DashboardTile<{ total: number }>
  activeExecutions: DashboardTile<{ executions: Execution[]; pagination: PaginationResponse }>
  recentDrift: DashboardTile<{ reports: DriftReport[]; pagination: PaginationResponse }>
  usage: DashboardTile<GetUsageResponse>
}

// ---- Generic ---------------------------------------------------------------

/** The BFF's sanitized error envelope (httpx.errorBody). */
export interface ApiErrorBody {
  error: {
    code: number
    message: string
  }
}

export interface PageParams {
  pageSize?: number
  pageToken?: string
}
