// ============================================================================
// usePagination — cursor pagination state for list pages.
// ============================================================================
// The platform uses OPAQUE CURSOR tokens (common.proto next_page_token), not
// offsets. So "next page" means: remember the current response's
// nextPageToken and use it as the request's pageToken. We keep a stack of tokens
// so the page counter is accurate and "First" resets cleanly. We deliberately do
// NOT implement "Previous" by token math — cursors aren't reversible — so the
// UI offers First + Next, which is the honest cursor UX.

import { useCallback, useState } from 'react'

export function usePagination(pageSize = 20) {
  // token stack: index 0 is page 1 (no token). Each Next pushes the server's
  // nextPageToken; First clears back to [undefined].
  const [tokens, setTokens] = useState<(string | undefined)[]>([undefined])
  const page = tokens.length - 1
  const currentToken = tokens[page]

  const next = useCallback((nextPageToken: string) => {
    if (!nextPageToken) return
    setTokens((t) => [...t, nextPageToken])
  }, [])

  const reset = useCallback(() => setTokens([undefined]), [])

  return {
    page,
    pageSize,
    pageToken: currentToken,
    /** Pass the response's nextPageToken to advance. */
    goNext: next,
    reset,
  }
}
