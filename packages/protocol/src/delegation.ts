/**
 * Agent-to-agent delegation (Phase 4). One agent calls another over the existing
 * request/reply machinery; `DelegateRequest` is the request body and
 * `DelegateResult` the correlated reply. The delegation forms a parent→child
 * session tree (recorded via `SessionEvent.parentSessionId`) and is guarded
 * against cycles, excessive depth, and runaway time by the fields below.
 */
export interface DelegateRequest {
  prompt: string;
  parentSessionId: string;
  delegationPath: string[]; // agent ids from the root delegator down to the caller
  /**
   * The persistent task/session id on the **receiver**. A collaboration on one task
   * can be many request/responses — every turn carries the same `taskId`, so the
   * receiver continues that conversation's context; a different `taskId` is a
   * different task with its own context. Omitted ⇒ the receiver mints a one-shot id.
   */
  taskId?: string;
  /** Finalize the task identified by `taskId` (drop its in-memory context) instead of running a turn. */
  close?: boolean;
  budget?: { maxDepth?: number; deadlineTs?: number };
}

export interface DelegateResult {
  text: string;
  stopReason?: string;
  /** The receiver-side task id this reply belongs to (continue the task with it). */
  taskId?: string;
}

/** Cheap structural check: a DelegateRequest carries a delegationPath, a RunRequest doesn't. */
export function isDelegateRequest(payload: unknown): payload is DelegateRequest {
  return (
    typeof payload === "object" &&
    payload !== null &&
    Array.isArray((payload as { delegationPath?: unknown }).delegationPath)
  );
}
