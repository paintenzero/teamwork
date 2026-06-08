/// <reference types="vite/client" />

interface ImportMetaEnv {
  /** Gateway bearer token (Phase 6, WS2). Dev default lives in api.ts. */
  readonly VITE_GATEWAY_TOKEN?: string;
}
interface ImportMeta {
  readonly env: ImportMetaEnv;
}
