// ============================================================================
// PromptsPage — the PROMPT REGISTRY console (L3).
// ============================================================================
// The read/write surface over the AI Gateway's versioned prompt registry. Three
// regions:
//   1. A LIST table (name · version · stage) from GET /api/v1/prompts.
//   2. A "New prompt" modal that POSTs a template with {{variables}} + a
//      description to /api/v1/prompts. The gateway extracts the variable names
//      from the {{ }} placeholders, so the form just submits raw template text.
//   3. A DETAIL panel for the selected prompt: its template + extracted
//      variables, and a RENDER panel — fill the {{variables}} and the server
//      expands the template (POST /api/v1/prompts/{name}/render), showing the
//      rendered text.
//
// SECURITY NOTE (interview point): every server-supplied string — the template,
// the description, and CRUCIALLY the rendered output — is rendered as TEXT. We
// never use dangerouslySetInnerHTML. A prompt template (or a rendered result
// derived from user-supplied variables) is untrusted content; injecting it as
// HTML would be a stored-XSS sink, exactly like rendering an LLM completion as
// HTML. React's default escaping is the whole defense.
//
// WHY RENDER ON THE SERVER: the preview uses the SAME templating engine the
// gateway uses at completion time. A client-side `{{x}}` → value replace would
// risk drifting from the real behavior (escaping, missing-variable handling,
// nested braces) and would be a second, untested code path. The BFF forwards the
// variables map; the gateway renders; we display the result verbatim.

import { useEffect, useMemo, useState } from 'react'
import type { FormEvent } from 'react'
import { usePrompts, useCreatePrompt, useRenderPrompt } from '@/hooks/queries'
import { usePagination } from '@/hooks/usePagination'
import { useToast } from '@/components/Toast'
import { ApiError } from '@/api/client'
import { EmptyState, ErrorState, TableSkeleton } from '@/components/states'
import { Card, CardHeader, Modal, PageHeader, PaginationBar, Table, Td, Th, Mono } from '@/components/ui'
import { StatusPill } from '@/components/StatusPill'
import { PromptsIcon, PlusIcon } from '@/components/layout/icons'
import { formatRelative } from '@/lib/format'
import type { Prompt } from '@/api/types'

// Extract the distinct {{variable}} names from a template, in order of first
// appearance. This is a DISPLAY convenience for the create form's live preview
// only — the AUTHORITATIVE variable list comes from the server (prompt.variables),
// which is what the render panel binds to. We keep the regex permissive
// (alphanumeric + underscore) to match the gateway's mustache-ish convention.
function extractVars(template: string): string[] {
  const seen = new Set<string>()
  const out: string[] = []
  const re = /\{\{\s*([a-zA-Z0-9_]+)\s*\}\}/g
  let m: RegExpExecArray | null
  while ((m = re.exec(template)) !== null) {
    const name = m[1]
    if (!seen.has(name)) {
      seen.add(name)
      out.push(name)
    }
  }
  return out
}

export function PromptsPage() {
  const { page, pageSize, pageToken, goNext, reset } = usePagination(20)
  const { data, isLoading, isError, error, refetch, isFetching } = usePrompts({ pageSize, pageToken })
  const [modalOpen, setModalOpen] = useState(false)
  const [selected, setSelected] = useState<Prompt | null>(null)

  const prompts = data?.prompts ?? []
  const total = data?.pagination.totalCount ?? 0
  const nextToken = data?.pagination.nextPageToken ?? ''

  return (
    <div>
      <PageHeader
        title="Prompts"
        description="The prompt registry — versioned, immutable templates the AI Gateway renders from. Create a new version, inspect a template, and render it against variables."
        actions={
          <button type="button" className="btn-primary" onClick={() => setModalOpen(true)}>
            <PlusIcon className="h-4 w-4" /> New prompt
          </button>
        }
      />

      <div className="grid grid-cols-1 gap-6 lg:grid-cols-5">
        {/* ---- List ---- */}
        <Card className="lg:col-span-3">
          {isLoading ? (
            <TableSkeleton rows={6} cols={4} />
          ) : isError ? (
            <ErrorState message={error instanceof Error ? error.message : undefined} onRetry={() => refetch()} />
          ) : prompts.length === 0 ? (
            <EmptyState
              title="No prompts yet"
              description="Create your first prompt template to version and render it."
              icon={<PromptsIcon className="h-6 w-6" />}
              action={
                <button type="button" className="btn-primary mt-1" onClick={() => setModalOpen(true)}>
                  <PlusIcon className="h-4 w-4" /> New prompt
                </button>
              }
            />
          ) : (
            <>
              <Table>
                <thead>
                  <tr className="border-b border-ink-100">
                    <Th>Name</Th>
                    <Th>Version</Th>
                    <Th>Stage</Th>
                    <Th>Created</Th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-ink-100">
                  {prompts.map((p) => {
                    const isSel = selected?.id === p.id
                    return (
                      <tr
                        key={p.id}
                        className={`cursor-pointer transition-colors hover:bg-ink-50 ${isSel ? 'bg-brand-50/60' : ''}`}
                        onClick={() => setSelected(p)}
                      >
                        <Td>
                          <button
                            type="button"
                            className="text-left font-medium text-ink-900 hover:text-brand-700"
                          >
                            {p.name}
                          </button>
                          {p.team && <p className="text-xs text-ink-400">{p.team}</p>}
                        </Td>
                        <Td>
                          <Mono>v{p.version}</Mono>
                        </Td>
                        <Td>
                          <StatusPill value={p.stage} />
                        </Td>
                        <Td className="text-ink-500">{formatRelative(p.createdAt)}</Td>
                      </tr>
                    )
                  })}
                </tbody>
              </Table>
              <PaginationBar
                total={total}
                shown={prompts.length}
                hasNext={Boolean(nextToken) && !isFetching}
                onNext={() => goNext(nextToken)}
                onReset={reset}
                page={page}
              />
            </>
          )}
        </Card>

        {/* ---- Detail + render ---- */}
        <div className="lg:col-span-2">
          {selected ? (
            <PromptDetail key={selected.id} prompt={selected} />
          ) : (
            <Card className="flex h-full items-center justify-center p-8">
              <div className="text-center text-ink-400">
                <PromptsIcon className="mx-auto mb-3 h-8 w-8 text-ink-300" />
                <p className="text-sm font-medium text-ink-500">Select a prompt</p>
                <p className="mt-1 text-xs">Pick a row to view its template and render it.</p>
              </div>
            </Card>
          )}
        </div>
      </div>

      <CreatePromptModal open={modalOpen} onClose={() => setModalOpen(false)} />
    </div>
  )
}

// ----------------------------------------------------------------------------
// PromptDetail — the selected prompt's template + a render panel.
// ----------------------------------------------------------------------------

function PromptDetail({ prompt }: { prompt: Prompt }) {
  const render = useRenderPrompt()

  // One controlled input per server-declared variable. We seed from the
  // AUTHORITATIVE prompt.variables (not the client-side regex) so the form binds
  // exactly to what the gateway expects.
  const [values, setValues] = useState<Record<string, string>>({})
  const [rendered, setRendered] = useState<string | null>(null)
  const [renderError, setRenderError] = useState<string | null>(null)

  function setVar(name: string, v: string) {
    setValues((prev) => ({ ...prev, [name]: v }))
  }

  async function handleRender(e: FormEvent) {
    e.preventDefault()
    setRenderError(null)
    try {
      const res = await render.mutateAsync({
        name: prompt.name,
        version: prompt.version,
        // Send only the declared variables (ignore any stale keys).
        variables: Object.fromEntries(prompt.variables.map((v) => [v, values[v] ?? ''])),
      })
      setRendered(res.rendered)
    } catch (err) {
      setRendered(null)
      setRenderError(err instanceof ApiError ? err.message : 'Failed to render prompt.')
    }
  }

  return (
    <Card>
      <CardHeader
        title={prompt.name}
        subtitle={`Version ${prompt.version}`}
        action={<StatusPill value={prompt.stage} />}
      />
      <div className="space-y-5 p-5">
        {prompt.description && (
          <p className="text-sm text-ink-600">{prompt.description}</p>
        )}

        {/* Template — rendered as TEXT inside a <pre>, never as HTML. */}
        <div>
          <p className="label">Template</p>
          <pre className="max-h-48 overflow-auto whitespace-pre-wrap break-words rounded-lg bg-ink-50 p-3 font-mono text-xs text-ink-800 ring-1 ring-ink-200">
            {prompt.template}
          </pre>
        </div>

        {/* Variables + render form. */}
        <form onSubmit={handleRender} className="space-y-3">
          <p className="label">Render</p>
          {prompt.variables.length === 0 ? (
            <p className="text-xs text-ink-400">
              This template has no variables — render it to see the final text.
            </p>
          ) : (
            <div className="space-y-2.5">
              {prompt.variables.map((v) => (
                <div key={v}>
                  <label htmlFor={`var-${v}`} className="mb-1 block text-xs font-medium text-ink-600">
                    <Mono>{'{{' + v + '}}'}</Mono>
                  </label>
                  <input
                    id={`var-${v}`}
                    className="input"
                    placeholder={`value for ${v}`}
                    value={values[v] ?? ''}
                    onChange={(e) => setVar(v, e.target.value)}
                  />
                </div>
              ))}
            </div>
          )}
          <button type="submit" className="btn-primary w-full" disabled={render.isPending}>
            {render.isPending ? 'Rendering…' : 'Render'}
          </button>
        </form>

        {renderError && (
          <div className="rounded-lg bg-red-50 px-3 py-2 text-sm text-red-700 ring-1 ring-red-200" role="alert">
            {renderError}
          </div>
        )}

        {/* Rendered output — server string, shown as TEXT. */}
        {rendered !== null && !renderError && (
          <div>
            <p className="label">Rendered output</p>
            <pre className="max-h-60 overflow-auto whitespace-pre-wrap break-words rounded-lg bg-emerald-50 p-3 text-sm text-ink-800 ring-1 ring-emerald-200">
              {rendered}
            </pre>
          </div>
        )}
      </div>
    </Card>
  )
}

// ----------------------------------------------------------------------------
// CreatePromptModal — POST a new prompt template.
// ----------------------------------------------------------------------------

function CreatePromptModal({ open, onClose }: { open: boolean; onClose: () => void }) {
  const toast = useToast()
  const create = useCreatePrompt()

  const [name, setName] = useState('')
  const [template, setTemplate] = useState('')
  const [description, setDescription] = useState('')

  // Live preview of the {{variables}} we detect in the template, so the author
  // sees what the gateway will extract before submitting (display-only).
  const detectedVars = useMemo(() => extractVars(template), [template])

  function resetForm() {
    setName('')
    setTemplate('')
    setDescription('')
  }

  // Clear the form whenever the modal is freshly opened.
  useEffect(() => {
    if (open) resetForm()
  }, [open])

  async function handleSubmit(e: FormEvent) {
    e.preventDefault()
    try {
      await create.mutateAsync({
        name: name.trim(),
        template,
        description: description.trim(),
        // A client-generated idempotency key makes a retried submit safe (the
        // gateway dedups on it — the platform's idempotent-write contract).
        idempotencyKey: crypto.randomUUID(),
      })
      toast.success(`Prompt "${name}" saved.`)
      onClose()
    } catch (err) {
      const msg =
        err instanceof ApiError
          ? err.status === 409
            ? 'A prompt with that name/version already exists.'
            : err.message
          : 'Failed to create prompt.'
      toast.error(msg)
    }
  }

  return (
    <Modal
      open={open}
      onClose={onClose}
      title="New prompt"
      footer={
        <>
          <button type="button" className="btn-secondary" onClick={onClose}>
            Cancel
          </button>
          <button
            type="submit"
            form="create-prompt-form"
            className="btn-primary"
            disabled={create.isPending || !name.trim() || !template.trim()}
          >
            {create.isPending ? 'Saving…' : 'Save'}
          </button>
        </>
      }
    >
      <form id="create-prompt-form" onSubmit={handleSubmit} className="space-y-4">
        <div>
          <label htmlFor="p-name" className="label">
            Name <span className="text-red-500">*</span>
          </label>
          <input
            id="p-name"
            className="input"
            required
            placeholder="summarize-ticket"
            value={name}
            onChange={(e) => setName(e.target.value)}
          />
          <p className="mt-1 text-xs text-ink-400">
            An existing name mints a new VERSION; a new name starts at v1.
          </p>
        </div>
        <div>
          <label htmlFor="p-template" className="label">
            Template <span className="text-red-500">*</span>
          </label>
          <textarea
            id="p-template"
            className="input min-h-[120px] resize-y font-mono text-xs"
            required
            placeholder="Summarize the following ticket for {{audience}}:\n\n{{ticket}}"
            value={template}
            onChange={(e) => setTemplate(e.target.value)}
          />
          <div className="mt-1.5 flex flex-wrap items-center gap-1.5">
            <span className="text-xs text-ink-400">Detected variables:</span>
            {detectedVars.length === 0 ? (
              <span className="text-xs text-ink-400">none</span>
            ) : (
              detectedVars.map((v) => (
                <Mono key={v}>{'{{' + v + '}}'}</Mono>
              ))
            )}
          </div>
        </div>
        <div>
          <label htmlFor="p-desc" className="label">
            Description
          </label>
          <textarea
            id="p-desc"
            className="input min-h-[64px] resize-y"
            placeholder="What this prompt is for…"
            value={description}
            onChange={(e) => setDescription(e.target.value)}
          />
        </div>
      </form>
    </Modal>
  )
}
