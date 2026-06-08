export interface BusOptions {
  /** This participant's stable id (agent id, "orchestrator", "ui:<session>"). */
  clientId: string;
  /** Defaults to env REDIS_URL or redis://localhost:6379. */
  redisUrl?: string;
  /** Redis ACL user (Phase 6). Defaults to env REDIS_USERNAME, else the dev user. */
  username?: string;
  /** Redis ACL password (Phase 6). Defaults to env REDIS_PASSWORD, else the dev password. */
  password?: string;
}

export interface RequestOptions {
  timeoutMs?: number;
  sessionId?: string;
  causationId?: string;
}

export interface RequestMeta {
  messageId: string;
  source: string;
  correlationId: string;
  sessionId?: string;
  causationId?: string;
}

export type RequestHandler = (payload: unknown, meta: RequestMeta) => Promise<unknown> | unknown;

export type Unsubscribe = () => void;
