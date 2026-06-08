import { Redis, type RedisOptions } from "ioredis";
import { readFileSync } from "node:fs";
import {
  channels,
  makeEnvelope,
  uuidv7,
  isSchemaCompatible,
  SCHEMA_VERSION,
  type Envelope,
  type ObservabilityEvent,
  type ControlMessage,
  type ResponseBody,
  type AgentRegistration,
  type AgentStatus,
  type PresenceEvent,
  type SessionEvent,
} from "@paintenzero/orchestra-protocol";
import type {
  BusOptions,
  RequestOptions,
  RequestHandler,
  Unsubscribe,
} from "./types.js";

const DEFAULT_TIMEOUT_MS = 30_000;
const PRESENCE_TTL_S = 15;
const HEARTBEAT_MS = 5_000;
const OBS_MAXLEN = Number(process.env.OBS_MAXLEN ?? 10_000); // cap on a session's live stream
const REPLY_TTL_S = 60; // safety net so abandoned reply streams self-clean
const CONTROL_MAXLEN = 1_000; // cap on a durable control stream
const DEDUPE_TTL_MS = Number(process.env.DEDUPE_TTL_MS ?? 86_400_000); // 24h idempotency window
const RECLAIM_IDLE_MS = Number(process.env.RECLAIM_IDLE_MS ?? 30_000); // reclaim pending idle this long
const RECLAIM_EVERY_MS = Number(process.env.RECLAIM_EVERY_MS ?? 10_000); // how often to sweep for stuck requests
const MAX_DELIVERIES = Number(process.env.MAX_DELIVERIES ?? 5); // attempts before dead-lettering
const TASK_CONTEXT_TTL_MS = Number(process.env.TASK_CONTEXT_TTL_MS ?? 30 * 86_400_000); // task snapshot retention (resume window)

// ioredis returns XREAD/XREADGROUP results as [stream, [[id, [f, v, ...]], ...]][]
type StreamReply = Array<[string, Array<[string, string[]]>]>;

export class RequestTimeoutError extends Error {
  constructor(public target: string, public correlationId: string) {
    super(`request to "${target}" timed out (correlationId=${correlationId})`);
    this.name = "RequestTimeoutError";
  }
}
export class RemoteError extends Error {
  constructor(public target: string, message: string) {
    super(message);
    this.name = "RemoteError";
  }
}

function safeJson<T>(raw: string): T | undefined {
  try {
    return JSON.parse(raw) as T;
  } catch {
    return undefined;
  }
}

/** Extract the JSON envelope from a stream entry's field array (["e", "<json>"]). */
function parseEntry<P>(fields: string[]): Envelope<P> | undefined {
  const i = fields.indexOf("e");
  if (i < 0 || i + 1 >= fields.length) return undefined;
  return safeJson<Envelope<P>>(fields[i + 1]);
}

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

/**
 * One bus client per participant. Every agent, the orchestrator, and each UI
 * session constructs one of these. The transport (Redis) is fully hidden behind
 * publishEvent / subscribeEvents / request / onRequest / control / presence.
 */
export class Bus {
  readonly clientId: string;
  private readonly url: string;
  private readonly redis: Redis; // command connection (non-blocking)
  private readonly auxConns = new Set<Redis>(); // blocking reads + pub/sub
  private running = true;
  private heartbeat?: ReturnType<typeof setInterval>;
  private registration?: AgentRegistration;

  private readonly redisOpts: RedisOptions;

  constructor(opts: BusOptions) {
    this.clientId = opts.clientId;
    this.url = opts.redisUrl ?? process.env.REDIS_URL ?? "redis://localhost:6379";
    // Phase 6: authenticate to Redis. A dev user keeps the local stack runnable;
    // a deployment sets REDIS_USERNAME/REDIS_PASSWORD per role (and per agent, so a
    // compromised agent's ACL can't touch another's keys/channels). Options are kept
    // so every duplicate() (blocking reads, pub/sub) inherits the same credentials.
    this.redisOpts = {
      maxRetriesPerRequest: null,
      lazyConnect: false,
      // Our ACL intentionally withholds INFO; skip ioredis's INFO-based ready check
      // (the connection is ready after AUTH/HELLO) so least-privilege users are quiet.
      enableReadyCheck: false,
      username: opts.username ?? process.env.REDIS_USERNAME ?? "dev",
      password: opts.password ?? process.env.REDIS_PASSWORD ?? "orchestra-dev",
    };
    // TLS to Redis: enable via rediss:// or REDIS_TLS=1; REDIS_TLS_CA points at a
    // CA bundle for a self-signed dev cert (else system roots / rejectUnauthorized).
    if (this.url.startsWith("rediss://") || process.env.REDIS_TLS === "1") {
      const ca = process.env.REDIS_TLS_CA;
      this.redisOpts.tls = ca ? { ca: readFileSync(ca) } : {};
    }
    this.redis = new Redis(this.url, this.redisOpts);
    // WS6: ioredis auto-reconnects the socket and the blocking loops resume from
    // their last id; on every (re)connect re-assert our presence so a Redis restart
    // (which drops the TTL'd key) or a blip doesn't make us vanish from the roster.
    this.redis.on("ready", () => {
      if (!this.registration) return;
      void this.writePresence().catch(() => {});
      void this.announce({ event: "register", registration: this.registration }).catch(() => {});
    });
  }

  private aux(): Redis {
    const c = this.redis.duplicate();
    this.auxConns.add(c);
    return c;
  }

  // ---- observability ------------------------------------------------------

  /** Append an event to a session's replayable stream. */
  async publishEvent(
    sessionId: string,
    event: ObservabilityEvent,
    opts: { causationId?: string } = {},
  ): Promise<void> {
    const env = makeEnvelope({
      type: "event",
      source: this.clientId,
      sessionId,
      causationId: opts.causationId,
      payload: event,
    });
    await this.redis.xadd(
      channels.observability(sessionId),
      "MAXLEN",
      "~",
      String(OBS_MAXLEN),
      "*",
      "e",
      JSON.stringify(env),
    );
  }

  /**
   * Tail a session's event stream. `from`: "now" (default, only new), "start"
   * (replay full history then tail), or a specific stream id to resume from.
   */
  subscribeEvents(
    sessionId: string,
    handler: (event: ObservabilityEvent, env: Envelope<ObservabilityEvent>) => void,
    opts: { from?: "now" | "start" | string } = {},
  ): Unsubscribe {
    const conn = this.aux();
    let lastId = opts.from === "start" ? "0" : opts.from && opts.from !== "now" ? opts.from : "$";
    let active = true;
    void (async () => {
      while (active && this.running) {
        try {
          const res = (await (conn as any).xread(
            "BLOCK",
            0,
            "COUNT",
            100,
            "STREAMS",
            channels.observability(sessionId),
            lastId,
          )) as StreamReply | null;
          if (!res) continue;
          for (const [, entries] of res) {
            for (const [id, fields] of entries) {
              lastId = id;
              const env = parseEntry<ObservabilityEvent>(fields);
              if (env) handler(env.payload, env);
            }
          }
        } catch {
          if (active) await sleep(200);
        }
      }
    })();
    return () => {
      active = false;
      this.dropAux(conn);
    };
  }

  /**
   * One-shot read of a session's full event history (XRANGE - +). Used by
   * persisters to archive the trace and by the orchestrator to serve a durable
   * transcript when the S3 archive isn't there yet. Returns full envelopes so
   * callers keep messageId / ts / source.
   */
  async readEvents(sessionId: string): Promise<Envelope<ObservabilityEvent>[]> {
    const entries = (await this.redis.xrange(
      channels.observability(sessionId),
      "-",
      "+",
    )) as Array<[string, string[]]>;
    const out: Envelope<ObservabilityEvent>[] = [];
    for (const [, fields] of entries) {
      const env = parseEntry<ObservabilityEvent>(fields);
      if (env) out.push(env);
    }
    return out;
  }

  // ---- idempotency & introspection ---------------------------------------

  /**
   * Claim a message id exactly once (Phase 6, WS3). Returns true the first time a
   * key is seen, false on every repeat within the TTL window — so a redelivered
   * request/control message executes at most once. Namespaced per client.
   */
  async dedupe(key: string, ttlMs = DEDUPE_TTL_MS): Promise<boolean> {
    const res = await this.redis.set(channels.dedupe(this.clientId, key), "1", "PX", ttlMs, "NX");
    return res === "OK";
  }

  /**
   * Snapshot a task's working memory (e.g. an agent's per-task conversation) so the
   * task survives idle-eviction and process restarts. Keyed per client + task and
   * TTL'd. `messages` is opaque JSON to the bus (the agent owns its shape).
   */
  async saveTaskContext(taskId: string, messages: unknown[]): Promise<void> {
    await this.redis.set(
      channels.taskContext(this.clientId, taskId),
      JSON.stringify(messages),
      "PX",
      TASK_CONTEXT_TTL_MS,
    );
  }

  /** Load a previously snapshotted task working memory, or undefined if none. */
  async loadTaskContext(taskId: string): Promise<unknown[] | undefined> {
    const raw = await this.redis.get(channels.taskContext(this.clientId, taskId));
    if (!raw) return undefined;
    return safeJson<unknown[]>(raw);
  }

  /** Liveness check for the Redis connection (Phase 6, WS8 /readyz). */
  async ping(): Promise<boolean> {
    try {
      return (await this.redis.ping()) === "PONG";
    } catch {
      return false;
    }
  }

  /** Length of a stream (e.g. a DLQ depth), 0 if it doesn't exist. */
  async streamLen(key: string): Promise<number> {
    return (await this.redis.xlen(key).catch(() => 0)) as number;
  }

  /** Count of pending (delivered, un-acked) requests for an agent's worker group. */
  async pendingCount(agentId: string, group = "workers"): Promise<number> {
    try {
      const summary = (await this.redis.xpending(channels.request(agentId), group)) as [
        number,
        ...unknown[],
      ];
      return Array.isArray(summary) ? Number(summary[0] ?? 0) : 0;
    } catch {
      return 0; // no group yet
    }
  }

  // ---- sessions -----------------------------------------------------------

  /** Announce a newly allocated session so recorders can persist + tail it. */
  async announceSession(ev: SessionEvent): Promise<void> {
    await this.redis.publish(channels.sessionEvents, JSON.stringify(ev));
  }

  /** React to sessions being opened in real time (mirrors watchPresence). */
  watchSessions(handler: (event: SessionEvent) => void): Unsubscribe {
    const conn = this.aux();
    void conn.subscribe(channels.sessionEvents);
    conn.on("message", (_ch: string, raw: string) => {
      const e = safeJson<SessionEvent>(raw);
      if (e) handler(e);
    });
    return () => this.dropAux(conn);
  }

  // ---- request / reply ----------------------------------------------------

  /** Send a directed request to an agent and await its correlated reply. */
  async request<R = unknown>(target: string, payload: unknown, opts: RequestOptions = {}): Promise<R> {
    const correlationId = uuidv7();
    const replyTo = channels.reply(this.clientId, correlationId);
    const env = makeEnvelope({
      type: "request",
      source: this.clientId,
      target,
      correlationId,
      replyTo,
      sessionId: opts.sessionId,
      causationId: opts.causationId,
      payload,
    });
    await this.redis.xadd(channels.request(target), "*", "e", JSON.stringify(env));

    const conn = this.aux();
    try {
      // Read from "0": if the reply already landed we get it immediately,
      // otherwise BLOCK waits for the first entry on this unique stream.
      const res = (await (conn as any).xread(
        "BLOCK",
        opts.timeoutMs ?? DEFAULT_TIMEOUT_MS,
        "COUNT",
        1,
        "STREAMS",
        replyTo,
        "0",
      )) as StreamReply | null;
      if (!res) throw new RequestTimeoutError(target, correlationId);
      const env2 = parseEntry<ResponseBody<R>>(res[0][1][0][1]);
      if (!env2) throw new RemoteError(target, "malformed reply");
      if (!env2.payload.ok) throw new RemoteError(target, env2.payload.error);
      return env2.payload.result;
    } finally {
      this.dropAux(conn);
      await this.redis.del(replyTo).catch(() => {});
    }
  }

  /**
   * Register a handler for inbound requests addressed to this client. Uses a
   * consumer group so multiple instances of the same agent load-balance work.
   * NOTE: delivery is at-least-once — make handlers idempotent or dedupe on
   * meta.messageId.
   */
  onRequest(handler: RequestHandler, opts: { group?: string; consumer?: string } = {}): Unsubscribe {
    const stream = channels.request(this.clientId);
    const group = opts.group ?? "workers";
    const consumer = opts.consumer ?? `${this.clientId}#${process.pid}`;
    const conn = this.aux();
    let active = true;
    void (async () => {
      try {
        await conn.xgroup("CREATE", stream, group, "$", "MKSTREAM");
      } catch {
        /* BUSYGROUP: group already exists */
      }
      while (active && this.running) {
        try {
          const res = (await (conn as any).xreadgroup(
            "GROUP",
            group,
            consumer,
            "BLOCK",
            5000,
            "COUNT",
            10,
            "STREAMS",
            stream,
            ">",
          )) as StreamReply | null;
          if (!res) continue;
          for (const [, entries] of res) {
            for (const [id, fields] of entries) {
              await this.handleRequest(stream, group, id, fields, handler);
            }
          }
        } catch {
          if (active) await sleep(200);
        }
      }
    })();
    // WS4: sweep for requests a dead worker left pending — reclaim them to this
    // consumer (re-process; the dedupe guard prevents a double run), or after too
    // many delivery attempts dead-letter the poison message rather than loop forever.
    const reclaim = setInterval(() => {
      if (active && this.running) void this.reclaimStuck(stream, group, consumer, handler);
    }, RECLAIM_EVERY_MS);
    reclaim.unref?.(); // a background sweep must not keep the process alive on its own
    return () => {
      active = false;
      clearInterval(reclaim);
      this.dropAux(conn);
    };
  }

  private async reclaimStuck(
    stream: string,
    group: string,
    consumer: string,
    handler: RequestHandler,
  ): Promise<void> {
    try {
      // [count, [[id, consumer, idleMs, deliveries], ...]] — pending entries idle long enough.
      const pending = (await (this.redis as any).xpending(
        stream,
        group,
        "IDLE",
        RECLAIM_IDLE_MS,
        "-",
        "+",
        50,
      )) as Array<[string, string, number, number]>;
      if (!Array.isArray(pending)) return;
      for (const [id, , , deliveries] of pending) {
        const claimed = (await (this.redis as any).xclaim(
          stream,
          group,
          consumer,
          RECLAIM_IDLE_MS,
          id,
        )) as Array<[string, string[]]>;
        if (!claimed?.length) continue; // someone else got it, or it was acked
        const [, fields] = claimed[0];
        if (Number(deliveries) > MAX_DELIVERIES) {
          await this.redis.xadd(channels.requestDlq(this.clientId), "*", ...fields);
          await this.redis.xack(stream, group, id).catch(() => {});
          console.warn(
            `[bus] dead-lettered ${id} after ${deliveries} attempts → ${channels.requestDlq(this.clientId)}`,
          );
        } else {
          await this.handleRequest(stream, group, id, fields, handler);
        }
      }
    } catch {
      /* transient; the next sweep retries */
    }
  }

  private async handleRequest(
    stream: string,
    group: string,
    id: string,
    fields: string[],
    handler: RequestHandler,
  ): Promise<void> {
    const env = parseEntry(fields);
    if (!env || !env.correlationId) {
      await this.redis.xack(stream, group, id).catch(() => {});
      return;
    }
    // WS7: reject an envelope from an incompatible schema rather than mis-parsing —
    // reply with a clear error so the caller isn't left hanging, then ack.
    if (!isSchemaCompatible(env)) {
      const error = `unsupported schemaVersion ${env.schemaVersion} (expected ${SCHEMA_VERSION})`;
      if (env.replyTo) await this.sendReply(env, { ok: false, error });
      await this.redis.xack(stream, group, id).catch(() => {});
      return;
    }
    // WS3: claim this messageId once. A redelivery (e.g. after a worker died before
    // acking) is skipped here so an expensive handler never runs twice.
    if (!(await this.dedupe(`req:${env.messageId}`))) {
      await this.redis.xack(stream, group, id).catch(() => {});
      return;
    }
    let body: ResponseBody;
    try {
      const result = await handler(env.payload, {
        messageId: env.messageId,
        source: env.source,
        correlationId: env.correlationId,
        sessionId: env.sessionId,
        causationId: env.causationId,
      });
      body = { ok: true, result };
    } catch (err) {
      body = { ok: false, error: err instanceof Error ? err.message : String(err) };
    }
    if (env.replyTo) await this.sendReply(env, body);
    await this.redis.xack(stream, group, id);
  }

  /** Write a correlated reply onto the request's replyTo stream. */
  private async sendReply(env: Envelope, body: ResponseBody): Promise<void> {
    if (!env.replyTo) return;
    const reply = makeEnvelope({
      type: "response",
      source: this.clientId,
      target: env.source,
      correlationId: env.correlationId,
      causationId: env.messageId,
      sessionId: env.sessionId,
      payload: body,
    });
    await this.redis.xadd(env.replyTo, "*", "e", JSON.stringify(reply));
    await this.redis.expire(env.replyTo, REPLY_TTL_S);
  }

  // ---- control ------------------------------------------------------------

  /**
   * Fire a control signal at an agent's current run (chat / steer / cancel / …).
   * `opts.sessionId` rides on the envelope so the agent knows which conversation
   * the message belongs to. WS5: this goes to a **durable, replayable stream**, not
   * fire-and-forget pub/sub — so a message sent while the agent is briefly offline
   * is delivered when it reconnects (a blocking XREAD is still instant when live).
   */
  async publishControl(
    agentId: string,
    msg: ControlMessage,
    opts: { sessionId?: string } = {},
  ): Promise<void> {
    const env = makeEnvelope({
      type: "control",
      source: this.clientId,
      target: agentId,
      sessionId: opts.sessionId,
      payload: msg,
    });
    await this.redis.xadd(
      channels.controlStream(agentId),
      "MAXLEN",
      "~",
      String(CONTROL_MAXLEN),
      "*",
      "e",
      JSON.stringify(env),
    );
  }

  /**
   * Listen for control signals addressed to this client (WS5). Tails the durable
   * control stream with a blocking XREAD, resuming from the last id this client
   * processed (persisted in Redis), so a process that was briefly down catches up on
   * messages it missed. Incompatible-schema envelopes are dropped with a warning.
   */
  subscribeControl(handler: (msg: ControlMessage, env: Envelope<ControlMessage>) => void): Unsubscribe {
    const conn = this.aux();
    const stream = channels.controlStream(this.clientId);
    const posKey = channels.controlPos(this.clientId);
    let active = true;
    void (async () => {
      // Resume from the persisted position; first-ever start tails only new ("$").
      let lastId = ((await this.redis.get(posKey).catch(() => null)) as string | null) ?? "$";
      while (active && this.running) {
        try {
          const res = (await (conn as any).xread(
            "BLOCK",
            0,
            "COUNT",
            50,
            "STREAMS",
            stream,
            lastId,
          )) as StreamReply | null;
          if (!res) continue;
          for (const [, entries] of res) {
            for (const [id, fields] of entries) {
              lastId = id;
              const env = parseEntry<ControlMessage>(fields);
              if (env && !isSchemaCompatible(env)) {
                console.warn(`[bus] dropped control with schemaVersion ${env.schemaVersion}`);
              } else if (env) {
                handler(env.payload, env);
              }
              await this.redis.set(posKey, id).catch(() => {});
            }
          }
        } catch {
          if (active) await sleep(200);
        }
      }
    })();
    return () => {
      active = false;
      this.dropAux(conn);
    };
  }

  // ---- presence -----------------------------------------------------------

  /** Announce this agent and start the heartbeat that keeps it "online". */
  async register(reg: Omit<AgentRegistration, "startedAt"> & { startedAt?: number }): Promise<void> {
    this.registration = { startedAt: Date.now(), ...reg };
    await this.writePresence();
    this.heartbeat = setInterval(() => void this.writePresence().catch(() => {}), HEARTBEAT_MS);
    await this.announce({ event: "register", registration: this.registration });
  }

  async updateStatus(status: AgentStatus): Promise<void> {
    if (!this.registration) return;
    this.registration.status = status;
    await this.writePresence();
    await this.announce({ event: "update", registration: this.registration });
  }

  async deregister(): Promise<void> {
    if (this.heartbeat) clearInterval(this.heartbeat);
    if (!this.registration) return;
    const id = this.registration.id;
    await this.redis.del(channels.presenceKey(id)).catch(() => {});
    await this.announce({ event: "deregister", id });
    this.registration = undefined;
  }

  /** Snapshot of all agents currently online. */
  async listAgents(): Promise<AgentRegistration[]> {
    const keys: string[] = [];
    let cursor = "0";
    do {
      const [next, batch] = await this.redis.scan(cursor, "MATCH", channels.presenceScan, "COUNT", 100);
      cursor = next;
      keys.push(...batch);
    } while (cursor !== "0");
    if (keys.length === 0) return [];
    const vals = (await this.redis.mget(keys)) as (string | null)[];
    return vals
      .filter((v): v is string => Boolean(v))
      .map((v: string) => safeJson<AgentRegistration>(v))
      .filter((r): r is AgentRegistration => Boolean(r));
  }

  /** Agents currently online that advertise a given capability. */
  async findAgents(capability: string): Promise<AgentRegistration[]> {
    const all = await this.listAgents();
    return all.filter((a) => a.capabilities.includes(capability));
  }

  /** React to agents coming and going in real time. */
  watchPresence(handler: (event: PresenceEvent) => void): Unsubscribe {
    const conn = this.aux();
    void conn.subscribe(channels.presenceEvents);
    conn.on("message", (_ch: string, raw: string) => {
      const e = safeJson<PresenceEvent>(raw);
      if (e) handler(e);
    });
    return () => this.dropAux(conn);
  }

  private async writePresence(): Promise<void> {
    if (!this.registration) return;
    await this.redis.set(
      channels.presenceKey(this.registration.id),
      JSON.stringify(this.registration),
      "EX",
      PRESENCE_TTL_S,
    );
  }

  private async announce(event: PresenceEvent): Promise<void> {
    await this.redis.publish(channels.presenceEvents, JSON.stringify(event));
  }

  // ---- lifecycle ----------------------------------------------------------

  private dropAux(conn: Redis): void {
    this.auxConns.delete(conn);
    conn.disconnect();
  }

  async close(): Promise<void> {
    this.running = false;
    if (this.heartbeat) clearInterval(this.heartbeat);
    for (const c of this.auxConns) c.disconnect();
    this.auxConns.clear();
    this.redis.disconnect();
  }
}
