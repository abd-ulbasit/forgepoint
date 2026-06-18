// ============================================================================
// REACT QUERY HOOKS — server state, cached and deduped.
// ============================================================================
//
// Each hook wraps one endpoint function with @tanstack/react-query, which gives
// us, for free: in-flight dedup, background refetch, cache, and the
// {data,isLoading,isError,error} state machine every page renders against. We
// keep a single QUERY-KEY factory so cache invalidation (e.g. after registering
// a model) is precise and typo-proof.

import {
  useMutation,
  useQuery,
  useQueryClient,
  keepPreviousData,
} from '@tanstack/react-query'
import * as api from '@/api/endpoints'
import type { PageParams, RegisterModelRequest } from '@/api/types'

// Centralized cache keys. Arrays are stable & serializable, so identical params
// hit the same cache entry.
export const qk = {
  dashboard: ['dashboard'] as const,
  models: (p?: PageParams) => ['models', p ?? {}] as const,
  model: (id: string) => ['model', id] as const,
  versions: (id: string) => ['model-versions', id] as const,
  pipelines: (p?: PageParams) => ['pipelines', p ?? {}] as const,
  execution: (id: string) => ['execution', id] as const,
  runs: (p?: PageParams & { experimentId?: string }) => ['runs', p ?? {}] as const,
  run: (id: string) => ['run', id] as const,
  monitors: (p?: PageParams) => ['monitors', p ?? {}] as const,
  driftReports: (p?: PageParams & { modelName?: string }) => ['drift-reports', p ?? {}] as const,
  usage: (p?: PageParams & { team?: string }) => ['usage', p ?? {}] as const,
  notifications: (p?: PageParams & { unreadOnly?: boolean }) => ['notifications', p ?? {}] as const,
}

// ---- Dashboard -------------------------------------------------------------

export function useDashboard() {
  return useQuery({
    queryKey: qk.dashboard,
    queryFn: api.getDashboard,
    // The dashboard is a live overview; refresh every 30s in the background.
    refetchInterval: 30_000,
  })
}

// ---- Models ----------------------------------------------------------------

export function useModels(
  params?: PageParams & { taskType?: string; framework?: string; includeArchived?: boolean },
) {
  return useQuery({
    queryKey: qk.models(params),
    queryFn: () => api.listModels(params),
    placeholderData: keepPreviousData, // keep the old page visible while paging
  })
}

export function useModel(id: string) {
  return useQuery({
    queryKey: qk.model(id),
    queryFn: () => api.getModel(id),
    enabled: Boolean(id),
  })
}

export function useModelVersions(id: string) {
  return useQuery({
    queryKey: qk.versions(id),
    queryFn: () => api.listVersions(id, { pageSize: 50 }),
    enabled: Boolean(id),
  })
}

export function useRegisterModel() {
  const qc = useQueryClient()
  return useMutation({
    mutationFn: (body: RegisterModelRequest) => api.registerModel(body),
    onSuccess: () => {
      // Invalidate the list so the new model appears, and the dashboard count.
      qc.invalidateQueries({ queryKey: ['models'] })
      qc.invalidateQueries({ queryKey: qk.dashboard })
    },
  })
}

// ---- Pipelines / executions ------------------------------------------------

export function usePipelines(params?: PageParams) {
  return useQuery({
    queryKey: qk.pipelines(params),
    queryFn: () => api.listPipelines(params),
    placeholderData: keepPreviousData,
  })
}

export function useStartPipeline() {
  const qc = useQueryClient()
  return useMutation({
    mutationFn: (vars: { pipelineId: string; input?: Record<string, unknown> }) =>
      api.startPipeline(vars.pipelineId, vars.input),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: qk.dashboard })
    },
  })
}

export function useExecution(id: string, options?: { refetchInterval?: number }) {
  return useQuery({
    queryKey: qk.execution(id),
    queryFn: () => api.getExecution(id),
    enabled: Boolean(id),
    refetchInterval: options?.refetchInterval,
  })
}

// ---- Experiments -----------------------------------------------------------

export function useRuns(params?: PageParams & { experimentId?: string }) {
  return useQuery({
    queryKey: qk.runs(params),
    queryFn: () => api.listRuns(params),
    placeholderData: keepPreviousData,
  })
}

export function useRun(id: string) {
  return useQuery({
    queryKey: qk.run(id),
    queryFn: () => api.getRun(id),
    enabled: Boolean(id),
  })
}

// ---- Monitoring ------------------------------------------------------------

export function useMonitors(params?: PageParams) {
  return useQuery({
    queryKey: qk.monitors(params),
    queryFn: () => api.listMonitors(params),
    placeholderData: keepPreviousData,
  })
}

export function useDriftReports(params?: PageParams & { modelName?: string }) {
  return useQuery({
    queryKey: qk.driftReports(params),
    queryFn: () => api.listDriftReports(params),
    placeholderData: keepPreviousData,
  })
}

// ---- Billing ---------------------------------------------------------------

export function useUsage(params?: PageParams & { team?: string }) {
  return useQuery({
    queryKey: qk.usage(params),
    queryFn: () => api.getUsage(params),
  })
}

// ---- Notifications ---------------------------------------------------------

export function useNotifications(params?: PageParams & { unreadOnly?: boolean }) {
  return useQuery({
    queryKey: qk.notifications(params),
    queryFn: () => api.listNotifications(params),
    placeholderData: keepPreviousData,
  })
}
