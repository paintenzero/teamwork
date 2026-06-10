import pg from "pg";

const { Pool } = pg;

/** A persisted session row, shaped for callers (camelCase, not pg's snake_case). */
export interface SessionRow {
  id: string;
  agentId: string;
  parentSessionId: string | null;
  status: string;
  title: string | null;
  summary: string | null;
  traceUri: string | null;
  contextUri: string | null;
  startedAt: string;
  endedAt: string | null;
}

interface RawSessionRow {
  id: string;
  agent_id: string;
  parent_session_id: string | null;
  status: string;
  title: string | null;
  summary: string | null;
  trace_uri: string | null;
  context_uri: string | null;
  started_at: Date;
  ended_at: Date | null;
}

const toSessionRow = (r: RawSessionRow): SessionRow => ({
  id: r.id,
  agentId: r.agent_id,
  parentSessionId: r.parent_session_id,
  status: r.status,
  title: r.title,
  summary: r.summary,
  traceUri: r.trace_uri,
  contextUri: r.context_uri,
  startedAt: r.started_at.toISOString(),
  endedAt: r.ended_at ? r.ended_at.toISOString() : null,
});

const SESSION_COLS =
  "id, agent_id, parent_session_id, status, title, summary, trace_uri, context_uri, started_at, ended_at";

/** A persisted artifact row, shaped for callers (camelCase). */
export interface ArtifactRow {
  id: string;
  sessionId: string;
  s3Uri: string;
  contentType: string | null;
  name: string | null;
  createdAt: string;
}

interface RawArtifactRow {
  id: string;
  session_id: string;
  s3_uri: string;
  content_type: string | null;
  name: string | null;
  created_at: Date;
}

const toArtifactRow = (r: RawArtifactRow): ArtifactRow => ({
  id: r.id,
  sessionId: r.session_id,
  s3Uri: r.s3_uri,
  contentType: r.content_type,
  name: r.name,
  createdAt: r.created_at.toISOString(),
});

/**
 * The Postgres "queryable spine": agents, sessions, the clean request/response
 * message audit, and artifact pointers. The recorder writes; the orchestrator
 * reads for history browsing. All writes are idempotent (ON CONFLICT) so a
 * recorder that re-reads a stream from the start never duplicates rows.
 */
export class Store {
  private readonly pool: pg.Pool;

  constructor(connectionString?: string) {
    this.pool = new Pool({
      connectionString:
        connectionString ??
        process.env.DATABASE_URL ??
        "postgres://orchestra:orchestra@localhost:5432/orchestra",
    });
  }

  /** Apply the schema (CREATE TABLE IF NOT EXISTS …); safe to run every startup. */
  async applySchema(sql: string): Promise<void> {
    await this.pool.query(sql);
  }

  // ---- agents -------------------------------------------------------------

  /** Upsert an agent from presence: refresh capabilities/metadata and last_seen. */
  async upsertAgent(a: {
    id: string;
    capabilities?: string[];
    metadata?: Record<string, unknown>;
  }): Promise<void> {
    await this.pool.query(
      `INSERT INTO agents (id, capabilities, metadata, last_seen)
       VALUES ($1, $2, $3, now())
       ON CONFLICT (id) DO UPDATE
         SET capabilities = EXCLUDED.capabilities,
             metadata     = EXCLUDED.metadata,
             last_seen    = now()`,
      [a.id, a.capabilities ?? [], a.metadata ?? {}],
    );
  }

  /** Ensure an agent row exists (FK target) without clobbering its details. */
  async ensureAgent(id: string): Promise<void> {
    await this.pool.query(`INSERT INTO agents (id) VALUES ($1) ON CONFLICT (id) DO NOTHING`, [id]);
  }

  /** Bump last_seen (e.g. on deregister) without touching capabilities. */
  async touchAgent(id: string): Promise<void> {
    await this.pool.query(`UPDATE agents SET last_seen = now() WHERE id = $1`, [id]);
  }

  // ---- sessions -----------------------------------------------------------

  async insertSession(s: {
    id: string;
    agentId: string;
    parentSessionId?: string;
  }): Promise<void> {
    await this.pool.query(
      `INSERT INTO sessions (id, agent_id, parent_session_id)
       VALUES ($1, $2, $3) ON CONFLICT (id) DO NOTHING`,
      [s.id, s.agentId, s.parentSessionId ?? null],
    );
  }

  /** Set a session's human-given display name (empty string clears it). */
  async renameSession(sessionId: string, title: string): Promise<void> {
    await this.pool.query(`UPDATE sessions SET title = $2 WHERE id = $1`, [
      sessionId,
      title || null,
    ]);
  }

  /** Point a session at its latest S3 trace archive. */
  async setTrace(sessionId: string, uri: string): Promise<void> {
    await this.pool.query(`UPDATE sessions SET trace_uri = $2 WHERE id = $1`, [sessionId, uri]);
  }

  /** Point a session at its latest S3 canonical-context archive. */
  async setContext(sessionId: string, uri: string): Promise<void> {
    await this.pool.query(`UPDATE sessions SET context_uri = $2 WHERE id = $1`, [sessionId, uri]);
  }

  /**
   * Flip open sessions idle longer than `ttlMs` to read-only `archived` (their hot
   * Redis context has the same TTL, so by now it has expired). Last activity is the
   * newest message, falling back to started_at for sessions that never got one.
   * Returns the archived session ids (for logging).
   */
  async archiveExpiredSessions(ttlMs: number): Promise<string[]> {
    const r = await this.pool.query<{ id: string }>(
      `UPDATE sessions s
          SET status = 'archived', ended_at = now()
        WHERE s.status = 'open'
          AND GREATEST(
                s.started_at,
                COALESCE((SELECT max(m.created_at) FROM messages m WHERE m.session_id = s.id), s.started_at)
              ) < now() - make_interval(secs => $1)
        RETURNING id`,
      [ttlMs / 1000],
    );
    return r.rows.map((x) => x.id);
  }

  /** Sessions still open — re-tailed on recorder startup to catch missed turns. */
  async listOpenSessions(): Promise<Array<{ id: string; agentId: string }>> {
    const r = await this.pool.query<{ id: string; agent_id: string }>(
      `SELECT id, agent_id FROM sessions WHERE status = 'open'`,
    );
    return r.rows.map((x) => ({ id: x.id, agentId: x.agent_id }));
  }

  async listSessions(opts: { agentId?: string; limit?: number } = {}): Promise<SessionRow[]> {
    const limit = opts.limit ?? 100;
    const r = opts.agentId
      ? await this.pool.query<RawSessionRow>(
          `SELECT ${SESSION_COLS} FROM sessions WHERE agent_id = $1 ORDER BY started_at DESC LIMIT $2`,
          [opts.agentId, limit],
        )
      : await this.pool.query<RawSessionRow>(
          `SELECT ${SESSION_COLS} FROM sessions ORDER BY started_at DESC LIMIT $1`,
          [limit],
        );
    return r.rows.map(toSessionRow);
  }

  async getSession(id: string): Promise<SessionRow | undefined> {
    const r = await this.pool.query<RawSessionRow>(
      `SELECT ${SESSION_COLS} FROM sessions WHERE id = $1`,
      [id],
    );
    return r.rows[0] ? toSessionRow(r.rows[0]) : undefined;
  }

  /**
   * The delegation tree rooted at `id`: the session plus every descendant linked
   * by `parent_session_id`, walked with a recursive CTE. Flat list (each row keeps
   * its `parentSessionId`); the UI nests them.
   */
  async getSessionTree(id: string): Promise<SessionRow[]> {
    const r = await this.pool.query<RawSessionRow>(
      `WITH RECURSIVE tree AS (
         SELECT ${SESSION_COLS} FROM sessions WHERE id = $1
         UNION ALL
         SELECT ${SESSION_COLS.split(", ")
           .map((c) => `s.${c}`)
           .join(", ")}
           FROM sessions s JOIN tree t ON s.parent_session_id = t.id
       )
       SELECT ${SESSION_COLS} FROM tree ORDER BY started_at`,
      [id],
    );
    return r.rows.map(toSessionRow);
  }

  // ---- messages / artifacts ----------------------------------------------

  /** Clean request/response audit (id = the originating event's messageId). */
  async insertMessage(m: {
    id: string;
    sessionId: string;
    correlationId?: string;
    direction: "request" | "response";
    source: string;
    target?: string;
    payload: unknown;
  }): Promise<void> {
    await this.pool.query(
      `INSERT INTO messages (id, session_id, correlation_id, direction, source, target, payload)
       VALUES ($1, $2, $3, $4, $5, $6, $7) ON CONFLICT (id) DO NOTHING`,
      [
        m.id,
        m.sessionId,
        m.correlationId ?? null,
        m.direction,
        m.source,
        m.target ?? null,
        JSON.stringify(m.payload),
      ],
    );
  }

  /** A session's artifacts, oldest first (id = the originating event's messageId). */
  async listArtifacts(sessionId: string): Promise<ArtifactRow[]> {
    const r = await this.pool.query<RawArtifactRow>(
      `SELECT id, session_id, s3_uri, content_type, name, created_at
         FROM artifacts WHERE session_id = $1 ORDER BY created_at`,
      [sessionId],
    );
    return r.rows.map(toArtifactRow);
  }

  async insertArtifact(a: {
    id: string;
    sessionId: string;
    s3Uri: string;
    contentType?: string;
    name?: string;
  }): Promise<void> {
    await this.pool.query(
      `INSERT INTO artifacts (id, session_id, s3_uri, content_type, name)
       VALUES ($1, $2, $3, $4, $5) ON CONFLICT (id) DO NOTHING`,
      [a.id, a.sessionId, a.s3Uri, a.contentType ?? null, a.name ?? null],
    );
  }

  /** Liveness check for Postgres (Phase 6, WS8 /readyz). */
  async ping(): Promise<boolean> {
    try {
      await this.pool.query("SELECT 1");
      return true;
    } catch {
      return false;
    }
  }

  async close(): Promise<void> {
    await this.pool.end();
  }
}
