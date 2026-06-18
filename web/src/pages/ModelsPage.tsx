// ============================================================================
// ModelsPage — paginated model registry table + register-model modal.
// ============================================================================
// Lists models from GET /api/v1/models with cursor pagination, and opens a modal
// that POSTs a new model to /api/v1/models. The BFF does no validation — any
// rejection (duplicate name -> 409, bad framework -> 400) surfaces as a toast
// with the BFF's sanitized message.

import { useState } from 'react'
import { Link } from 'react-router-dom'
import { useModels, useRegisterModel } from '@/hooks/queries'
import { usePagination } from '@/hooks/usePagination'
import { useToast } from '@/components/Toast'
import { ApiError } from '@/api/client'
import { EmptyState, ErrorState, TableSkeleton } from '@/components/states'
import { Card, Modal, PageHeader, PaginationBar, Table, Td, Th, Mono } from '@/components/ui'
import { StatusPill } from '@/components/StatusPill'
import { ModelsIcon, PlusIcon } from '@/components/layout/icons'
import { formatRelative } from '@/lib/format'
import type { FormEvent } from 'react'

export function ModelsPage() {
  const { page, pageSize, pageToken, goNext, reset } = usePagination(20)
  const { data, isLoading, isError, error, refetch, isFetching } = useModels({ pageSize, pageToken })
  const [modalOpen, setModalOpen] = useState(false)

  const models = data?.models ?? []
  const total = data?.pagination.totalCount ?? 0
  const nextToken = data?.pagination.nextPageToken ?? ''

  return (
    <div>
      <PageHeader
        title="Models"
        description="The model registry — every trained model and its lifecycle stage."
        actions={
          <button type="button" className="btn-primary" onClick={() => setModalOpen(true)}>
            <PlusIcon className="h-4 w-4" /> Register model
          </button>
        }
      />

      <Card>
        {isLoading ? (
          <TableSkeleton rows={6} cols={5} />
        ) : isError ? (
          <ErrorState message={error instanceof Error ? error.message : undefined} onRetry={() => refetch()} />
        ) : models.length === 0 ? (
          <EmptyState
            title="No models yet"
            description="Register your first model to start tracking versions and deployments."
            icon={<ModelsIcon className="h-6 w-6" />}
            action={
              <button type="button" className="btn-primary mt-1" onClick={() => setModalOpen(true)}>
                <PlusIcon className="h-4 w-4" /> Register model
              </button>
            }
          />
        ) : (
          <>
            <Table>
              <thead>
                <tr className="border-b border-ink-100">
                  <Th>Name</Th>
                  <Th>Framework</Th>
                  <Th>Task</Th>
                  <Th>Production</Th>
                  <Th>Updated</Th>
                  <Th className="text-right">{''}</Th>
                </tr>
              </thead>
              <tbody className="divide-y divide-ink-100">
                {models.map((m) => (
                  <tr key={m.id} className="transition-colors hover:bg-ink-50">
                    <Td>
                      <Link to={`/models/${m.id}`} className="font-medium text-ink-900 hover:text-brand-700">
                        {m.name}
                      </Link>
                      {m.team && <p className="text-xs text-ink-400">{m.team}</p>}
                    </Td>
                    <Td>{m.framework ? <Mono>{m.framework}</Mono> : <span className="text-ink-400">—</span>}</Td>
                    <Td>{m.taskType || <span className="text-ink-400">—</span>}</Td>
                    <Td>
                      {m.productionVersion ? (
                        <span className="inline-flex items-center gap-1.5">
                          <StatusPill value="MODEL_STAGE_PRODUCTION" />
                          <span className="text-xs text-ink-500">v{m.productionVersion}</span>
                        </span>
                      ) : (
                        <span className="text-xs text-ink-400">none</span>
                      )}
                    </Td>
                    <Td className="text-ink-500">{formatRelative(m.updatedAt)}</Td>
                    <Td className="text-right">
                      <Link to={`/models/${m.id}`} className="text-sm font-semibold text-brand-600 hover:text-brand-700">
                        Details
                      </Link>
                    </Td>
                  </tr>
                ))}
              </tbody>
            </Table>
            <PaginationBar
              total={total}
              shown={models.length}
              hasNext={Boolean(nextToken) && !isFetching}
              onNext={() => goNext(nextToken)}
              onReset={reset}
              page={page}
            />
          </>
        )}
      </Card>

      <RegisterModelModal open={modalOpen} onClose={() => setModalOpen(false)} />
    </div>
  )
}

// ----------------------------------------------------------------------------

function RegisterModelModal({ open, onClose }: { open: boolean; onClose: () => void }) {
  const toast = useToast()
  const register = useRegisterModel()

  const [name, setName] = useState('')
  const [description, setDescription] = useState('')
  const [framework, setFramework] = useState('')
  const [taskType, setTaskType] = useState('')

  function resetForm() {
    setName('')
    setDescription('')
    setFramework('')
    setTaskType('')
  }

  async function handleSubmit(e: FormEvent) {
    e.preventDefault()
    try {
      await register.mutateAsync({
        name: name.trim(),
        description: description.trim(),
        framework: framework.trim(),
        taskType: taskType.trim(),
        // A client-generated idempotency key makes a retried submit safe (the
        // registry dedups on it — the platform's idempotent-write contract).
        idempotencyKey: crypto.randomUUID(),
      })
      toast.success(`Model "${name}" registered.`)
      resetForm()
      onClose()
    } catch (err) {
      const msg =
        err instanceof ApiError
          ? err.status === 409
            ? 'A model with that name already exists.'
            : err.message
          : 'Failed to register model.'
      toast.error(msg)
    }
  }

  return (
    <Modal
      open={open}
      onClose={onClose}
      title="Register a new model"
      footer={
        <>
          <button type="button" className="btn-secondary" onClick={onClose}>
            Cancel
          </button>
          <button type="submit" form="register-model-form" className="btn-primary" disabled={register.isPending || !name.trim()}>
            {register.isPending ? 'Registering…' : 'Register'}
          </button>
        </>
      }
    >
      <form id="register-model-form" onSubmit={handleSubmit} className="space-y-4">
        <div>
          <label htmlFor="m-name" className="label">
            Name <span className="text-red-500">*</span>
          </label>
          <input
            id="m-name"
            className="input"
            required
            placeholder="fraud-detector"
            value={name}
            onChange={(e) => setName(e.target.value)}
          />
        </div>
        <div>
          <label htmlFor="m-desc" className="label">
            Description
          </label>
          <textarea
            id="m-desc"
            className="input min-h-[72px] resize-y"
            placeholder="What this model does…"
            value={description}
            onChange={(e) => setDescription(e.target.value)}
          />
        </div>
        <div className="grid grid-cols-1 gap-4 sm:grid-cols-2">
          <div>
            <label htmlFor="m-fw" className="label">
              Framework
            </label>
            <input
              id="m-fw"
              className="input"
              placeholder="pytorch / sklearn / onnx"
              value={framework}
              onChange={(e) => setFramework(e.target.value)}
            />
          </div>
          <div>
            <label htmlFor="m-task" className="label">
              Task type
            </label>
            <input
              id="m-task"
              className="input"
              placeholder="classification"
              value={taskType}
              onChange={(e) => setTaskType(e.target.value)}
            />
          </div>
        </div>
      </form>
    </Modal>
  )
}
