// ============================================================================
// API ENDPOINTS — one typed function per BFF route.
// ============================================================================
//
// Every function maps 1:1 to a route in services/bff/internal/router/router.go.
// They are thin: build the URL + params, call axios, return the typed body.
// React Query (see src/hooks/queries.ts) wraps these with caching, loading, and
// error states — these functions stay free of UI concerns so they're trivially
// testable and reusable.
//
// Pagination note: the BFF reads `page_size` / `page_token` from the query
// string (httpx.Pagination), NOT camelCase — so we send snake_case params here
// even though the RESPONSE bodies are camelCase. This snake-in / camel-out split
// is a real contract detail and a classic place to get a silent mismatch.

import { apiClient } from './client'
import type {
  AIUsageResponse,
  ListProvidersResponse,
  DashboardResponse,
  GetExecutionResponse,
  GetModelResponse,
  GetRunResponse,
  GetUsageResponse,
  ListDriftReportsResponse,
  ListModelsResponse,
  ListMonitorsResponse,
  ListNotificationsResponse,
  ListPipelinesResponse,
  ListRunsResponse,
  ListVersionsResponse,
  LoginResponse,
  PageParams,
  RegisterModelRequest,
  RegisterModelResponse,
  TriggerExecutionResponse,
} from './types'

/** Convert UI page params into the BFF's snake_case query keys. */
function pageQuery(p?: PageParams): Record<string, string> {
  const q: Record<string, string> = {}
  if (p?.pageSize) q.page_size = String(p.pageSize)
  if (p?.pageToken) q.page_token = p.pageToken
  return q
}

// ---- Auth ------------------------------------------------------------------

export async function login(email: string, password: string): Promise<LoginResponse> {
  // The ONLY public route. The token comes back in the body (dev flow).
  const { data } = await apiClient.post<LoginResponse>('/v1/login', { email, password })
  return data
}

// ---- Models ----------------------------------------------------------------

export async function listModels(
  params?: PageParams & { taskType?: string; framework?: string; includeArchived?: boolean },
): Promise<ListModelsResponse> {
  const { data } = await apiClient.get<ListModelsResponse>('/v1/models', {
    params: {
      ...pageQuery(params),
      ...(params?.taskType ? { task_type: params.taskType } : {}),
      ...(params?.framework ? { framework: params.framework } : {}),
      ...(params?.includeArchived ? { include_archived: 'true' } : {}),
    },
  })
  return data
}

export async function getModel(id: string): Promise<GetModelResponse> {
  const { data } = await apiClient.get<GetModelResponse>(`/v1/models/${encodeURIComponent(id)}`)
  return data
}

export async function listVersions(
  modelId: string,
  params?: PageParams,
): Promise<ListVersionsResponse> {
  const { data } = await apiClient.get<ListVersionsResponse>(
    `/v1/models/${encodeURIComponent(modelId)}/versions`,
    { params: pageQuery(params) },
  )
  return data
}

export async function registerModel(
  body: RegisterModelRequest,
): Promise<RegisterModelResponse> {
  const { data } = await apiClient.post<RegisterModelResponse>('/v1/models', body)
  return data
}

// ---- Pipelines / executions ------------------------------------------------

export async function listPipelines(params?: PageParams): Promise<ListPipelinesResponse> {
  const { data } = await apiClient.get<ListPipelinesResponse>('/v1/pipelines', {
    params: pageQuery(params),
  })
  return data
}

export async function startPipeline(
  pipelineId: string,
  input?: Record<string, unknown>,
): Promise<TriggerExecutionResponse> {
  // Body is optional (the BFF allows an empty body). When present it is the
  // proto-JSON TriggerExecutionRequest shape: { input: {...} }.
  const body = input ? { input } : {}
  const { data } = await apiClient.post<TriggerExecutionResponse>(
    `/v1/pipelines/${encodeURIComponent(pipelineId)}/start`,
    body,
  )
  return data
}

export async function getExecution(id: string): Promise<GetExecutionResponse> {
  const { data } = await apiClient.get<GetExecutionResponse>(
    `/v1/executions/${encodeURIComponent(id)}`,
  )
  return data
}

// ---- Experiments (runs) ----------------------------------------------------

export async function listRuns(
  params?: PageParams & { experimentId?: string },
): Promise<ListRunsResponse> {
  const { data } = await apiClient.get<ListRunsResponse>('/v1/runs', {
    params: {
      ...pageQuery(params),
      ...(params?.experimentId ? { experiment_id: params.experimentId } : {}),
    },
  })
  return data
}

export async function getRun(id: string): Promise<GetRunResponse> {
  const { data } = await apiClient.get<GetRunResponse>(`/v1/runs/${encodeURIComponent(id)}`)
  return data
}

// ---- Monitoring ------------------------------------------------------------

export async function listMonitors(params?: PageParams): Promise<ListMonitorsResponse> {
  const { data } = await apiClient.get<ListMonitorsResponse>('/v1/monitors', {
    params: pageQuery(params),
  })
  return data
}

export async function listDriftReports(
  params?: PageParams & { modelName?: string },
): Promise<ListDriftReportsResponse> {
  const { data } = await apiClient.get<ListDriftReportsResponse>('/v1/drift-reports', {
    params: {
      ...pageQuery(params),
      ...(params?.modelName ? { model_name: params.modelName } : {}),
    },
  })
  return data
}

// ---- Billing ---------------------------------------------------------------

export async function getUsage(
  params?: PageParams & { team?: string },
): Promise<GetUsageResponse> {
  const { data } = await apiClient.get<GetUsageResponse>('/v1/usage', {
    params: {
      ...pageQuery(params),
      ...(params?.team ? { team: params.team } : {}),
    },
  })
  return data
}

// ---- Notifications ---------------------------------------------------------

export async function listNotifications(
  params?: PageParams & { unreadOnly?: boolean },
): Promise<ListNotificationsResponse> {
  const { data } = await apiClient.get<ListNotificationsResponse>('/v1/notifications', {
    params: {
      ...pageQuery(params),
      ...(params?.unreadOnly ? { unread_only: 'true' } : {}),
    },
  })
  return data
}

// ---- Dashboard -------------------------------------------------------------

export async function getDashboard(): Promise<DashboardResponse> {
  const { data } = await apiClient.get<DashboardResponse>('/v1/dashboard')
  return data
}

// ---- AI Gateway (M7) -------------------------------------------------------

/** GET /api/v1/ai/providers — configured backends + live circuit state. */
export async function listAIProviders(): Promise<ListProvidersResponse> {
  const { data } = await apiClient.get<ListProvidersResponse>('/v1/ai/providers')
  return data
}

/** GET /api/v1/ai/usage — the caller team's token/cost usage + remaining budget. */
export async function getAIUsage(): Promise<AIUsageResponse> {
  const { data } = await apiClient.get<AIUsageResponse>('/v1/ai/usage')
  return data
}

/**
 * The chat endpoint is a POST whose RESPONSE is an SSE stream (the BFF bridges
 * the AI gateway's ChatCompletion server-stream). Like the watch stream it is
 * consumed via fetch()+ReadableStream (NOT axios, NOT EventSource) because the
 * request carries a JSON body AND must set the Authorization header — neither of
 * which EventSource can do. The useChat hook owns that fetch; this just names the
 * relative, same-origin URL so the host is never hardcoded in the bundle.
 */
export function chatStreamUrl(): string {
  return `/api/v1/chat`
}

// ---- SSE watch URL ---------------------------------------------------------

/**
 * The watch endpoint is consumed via the browser EventSource API (see
 * useExecutionWatch), not axios, because SSE is a long-lived stream. EventSource
 * cannot set an Authorization header, so we rely on the SAME-ORIGIN cookie/proxy
 * path: in dev the Vite proxy forwards it; the BFF's RequireAuth also accepts a
 * cookie. For the body-token dev flow we pass the token via a header-less path
 * — so this returns the relative URL and the hook documents the auth caveat.
 */
export function executionWatchUrl(id: string): string {
  return `/api/v1/executions/${encodeURIComponent(id)}/watch`
}
