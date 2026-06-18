/// <reference types="vite/client" />

// Strongly-typed access to the build-time env vars we read. Only VITE_-prefixed
// vars are exposed to the client bundle by Vite.
interface ImportMetaEnv {
  readonly VITE_BFF_URL?: string
}

interface ImportMeta {
  readonly env: ImportMetaEnv
}
