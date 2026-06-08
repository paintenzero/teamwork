/**
 * Single source of truth for Redis key / channel naming. Keeping this in the
 * shared package means agents, orchestrator, and UI can never disagree on where
 * a message lives.
 *
 *   req:<agentId>                inbound request stream (consumer group "workers")
 *   reply:<clientId>:<corrId>    per-request reply stream (short-lived, TTL'd)
 *   obs:<sessionId>              append-only observability stream (replayable)
 *   control:<agentId>           pub/sub control channel (cancel / steer)
 *   presence:agent:<agentId>    presence key with TTL, refreshed by heartbeat
 *   presence:events             pub/sub announcements of register/update/deregister
 *   sessions:events             pub/sub announcements of session open (for recorders)
 *   cstream:<agentId>           durable control stream (Phase 6: replayable, resumable)
 *   cpos:<agentId>              last-processed control id (Phase 6: resume point)
 *   req:<agentId>:dlq           dead-letter stream for poison requests (Phase 6)
 *   dedup:<clientId>:<key>      idempotency marker (Phase 6: SET NX, TTL'd)
 *   taskctx:<clientId>:<taskId> a task's snapshotted working memory (per-task sessions)
 */
export const channels = {
  request: (agentId: string) => `req:${agentId}`,
  reply: (clientId: string, correlationId: string) => `reply:${clientId}:${correlationId}`,
  observability: (sessionId: string) => `obs:${sessionId}`,
  control: (agentId: string) => `control:${agentId}`,
  controlStream: (agentId: string) => `cstream:${agentId}`,
  controlPos: (agentId: string) => `cpos:${agentId}`,
  requestDlq: (agentId: string) => `req:${agentId}:dlq`,
  dedupe: (clientId: string, key: string) => `dedup:${clientId}:${key}`,
  taskContext: (clientId: string, taskId: string) => `taskctx:${clientId}:${taskId}`,
  presenceKey: (agentId: string) => `presence:agent:${agentId}`,
  presenceScan: "presence:agent:*",
  presenceEvents: "presence:events",
  sessionEvents: "sessions:events",
} as const;
