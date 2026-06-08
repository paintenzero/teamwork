/**
 * Gateway client (Phase 6, WS2). Every call to the orchestrator carries the shared
 * bearer token: as an `Authorization` header for fetch, or as a `?token=` query
 * param for things the browser loads without headers — WebSockets, `<img>`, `<a>`.
 * A dev default keeps the local stack runnable; set VITE_GATEWAY_TOKEN to match the
 * orchestrator's GATEWAY_TOKEN otherwise. (A shared token is the self-hosted-operator
 * form; per-user sessions / OIDC is the stronger variant noted as future work.)
 */
const TOKEN = import.meta.env.VITE_GATEWAY_TOKEN ?? "dev-gateway-token";

/** fetch() against the gateway with the bearer token attached. */
export function api(path: string, init: RequestInit = {}): Promise<Response> {
  const headers = new Headers(init.headers);
  headers.set("authorization", `Bearer ${TOKEN}`);
  return fetch(path, { ...init, headers });
}

/** Append the token as a query param (for `<img>`, `<a>`, and other header-less loads). */
export function apiUrl(path: string): string {
  const sep = path.includes("?") ? "&" : "?";
  return `${path}${sep}token=${encodeURIComponent(TOKEN)}`;
}

/** Build a ws:// URL to the gateway with the token as a query param. */
export function wsUrl(path: string): string {
  const sep = path.includes("?") ? "&" : "?";
  return `ws://${location.host}${path}${sep}token=${encodeURIComponent(TOKEN)}`;
}
