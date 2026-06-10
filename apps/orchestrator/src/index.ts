/**
 * Phase 3 orchestrator. The only bus↔UI bridge: the browser talks HTTP/WebSocket
 * to this process, and this process is the only thing that touches Redis on the
 * UI's behalf. It now also reads the Postgres/S3 history the recorder writes.
 *
 *   GET  /api/agents                          current roster (live SCAN)
 *   GET  /api/sessions                        ?agentId= — durable session list (Postgres)
 *   GET  /api/sessions/:sessionId/transcript  past transcript (S3 archive, else Redis)
 *   GET  /api/sessions/:sessionId/tree        delegation tree (sessions on parent_session_id)
 *   GET  /api/sessions/:sessionId/artifacts   a session's artifacts (Postgres)
 *   GET  /api/artifacts?uri=                   artifact download proxy (S3 → browser, no creds)
 *   POST /api/sessions                        { agentId, text } -> { sessionId }; opens a chat
 *   POST /api/sessions/:sessionId/messages    { text } -> {}     ; next chat turn (prompt or steer)
 *   POST /api/sessions/:sessionId/rename      { title } -> {}    ; set a human display name
 *   POST /api/sessions/:sessionId/cancel      {} -> {}           ; hard stop the run
 *   POST /api/runs                            { agentId, prompt } -> { sessionId }; non-interactive kickoff
 *   WS   /api/sessions/:sessionId/stream      forwards the session's ObservabilityEvents (from start)
 *   WS   /api/presence/stream                 forwards PresenceEvents
 *
 * Chat input rides the control channel (fire-and-forget); the agent's responses
 * come back out on the observability stream the UI already renders. The agent
 * itself decides prompt-vs-steer from its own state — the orchestrator only
 * routes. On session create it announces the session on the bus so the recorder
 * persists it; reads of past sessions go straight to the recorder's spine.
 */
import Fastify from "fastify";
import websocket from "@fastify/websocket";
import { Bus } from "@paintenzero/orchestra-bus";
import { Store, TraceArchive, ArtifactStore } from "@paintenzero/orchestra-store";
import {
  uuidv7,
  type Envelope,
  type ObservabilityEvent,
  type RunRequest,
  type RunResult,
} from "@paintenzero/orchestra-protocol";

const PORT = Number(process.env.PORT ?? 3001);
const bus = new Bus({ clientId: "orchestrator" });
const store = new Store(); // read-only here: the recorder owns writes
const archive = new TraceArchive();
const artifactStore = new ArtifactStore(); // Phase 5: proxy reads; bytes never reach the browser raw

/** Which agent owns each chat session, so follow-ups route to the same place. */
const sessionAgent = new Map<string, string>();

const app = Fastify({ logger: true });
await app.register(websocket);

// WS8 metrics: cheap in-process counters, exposed in Prometheus text format at
// /metrics. Latency is a summary (sum+count); gauges (presence, pending, DLQ) are
// sampled on scrape. Token spend is a counter (0 under the faux provider).
const metrics = {
  httpRequests: new Map<string, number>(), // "<status>" -> count
  httpLatencySum: 0,
  httpLatencyCount: 0,
  rateLimited: 0,
  llmTokens: 0,
};
const bump = (m: Map<string, number>, k: string) => m.set(k, (m.get(k) ?? 0) + 1);

// WS9 global rate limit: a fixed 1s window over mutating /api calls, so a runaway
// client can't flood the bus with runs. Ops endpoints and reads are exempt.
const RATE_LIMIT_RPS = Number(process.env.GATEWAY_RATE_LIMIT_RPS ?? 50);
let rlWindow = Math.floor(Date.now() / 1000);
let rlCount = 0;

const OPS_PATHS = ["/healthz", "/readyz", "/metrics"];
const isOpsPath = (url: string) => OPS_PATHS.some((p) => url === p || url.startsWith(p + "?"));

// Gateway auth (Phase 6, WS2). Every request must carry the shared token, as an
// `Authorization: Bearer <token>` header or — for browser WebSockets, which can't
// set headers — a `?token=<token>` query param. The browser still never touches
// Redis; this authenticated gateway stays the only bus↔UI bridge. A dev default
// keeps the local stack runnable; a real deployment sets GATEWAY_TOKEN. Ops
// endpoints (health/metrics) are exempt — a real deploy keeps them on an internal
// interface.
const GATEWAY_TOKEN = process.env.GATEWAY_TOKEN ?? "dev-gateway-token";
app.addHook("onRequest", async (req, reply) => {
  if (isOpsPath(req.url)) return;
  const auth = req.headers.authorization;
  const headerToken = auth?.startsWith("Bearer ") ? auth.slice(7) : undefined;
  const queryToken = (req.query as { token?: string } | undefined)?.token;
  if (headerToken !== GATEWAY_TOKEN && queryToken !== GATEWAY_TOKEN) {
    return reply.code(401).send({ error: "unauthorized" });
  }
  // Rate-limit mutating calls (WS9).
  if (req.method === "POST") {
    const now = Math.floor(Date.now() / 1000);
    if (now !== rlWindow) {
      rlWindow = now;
      rlCount = 0;
    }
    if (++rlCount > RATE_LIMIT_RPS) {
      metrics.rateLimited++;
      return reply.code(429).send({ error: "rate limited" });
    }
  }
});

// WS8: count every response + its latency, keyed by status.
app.addHook("onResponse", async (_req, reply) => {
  bump(metrics.httpRequests, String(reply.statusCode));
  const ms = reply.elapsedTime ?? 0;
  metrics.httpLatencySum += ms / 1000;
  metrics.httpLatencyCount++;
});

// WS8 health: liveness (process up) and readiness (deps reachable).
app.get("/healthz", async () => ({ status: "ok" }));
app.get("/readyz", async (_req, reply) => {
  const [redis, pg] = await Promise.all([bus.ping(), store.ping()]);
  const ok = redis && pg;
  return reply.code(ok ? 200 : 503).send({ status: ok ? "ready" : "degraded", redis, pg });
});

// WS8 metrics: Prometheus text exposition. Counters from the hooks above; gauges
// (presence, pending/DLQ depth) sampled from the bus per scrape.
app.get("/metrics", async (_req, reply) => {
  const agents = await bus.listAgents();
  const pending = await Promise.all(
    agents.map(async (a) => ({
      id: a.id,
      pending: await bus.pendingCount(a.id),
      dlq: await bus.streamLen(`req:${a.id}:dlq`),
    })),
  );
  const lines: string[] = [];
  lines.push("# TYPE orchestra_up gauge", "orchestra_up 1");
  lines.push("# TYPE orchestra_http_requests_total counter");
  for (const [status, n] of metrics.httpRequests) {
    lines.push(`orchestra_http_requests_total{status="${status}"} ${n}`);
  }
  lines.push(
    "# TYPE orchestra_http_request_duration_seconds summary",
    `orchestra_http_request_duration_seconds_sum ${metrics.httpLatencySum}`,
    `orchestra_http_request_duration_seconds_count ${metrics.httpLatencyCount}`,
  );
  lines.push("# TYPE orchestra_rate_limited_total counter", `orchestra_rate_limited_total ${metrics.rateLimited}`);
  lines.push("# TYPE orchestra_agents_online gauge", `orchestra_agents_online ${agents.length}`);
  lines.push("# TYPE orchestra_request_pending gauge");
  for (const p of pending) lines.push(`orchestra_request_pending{agent="${p.id}"} ${p.pending}`);
  lines.push("# TYPE orchestra_dlq_depth gauge");
  for (const p of pending) lines.push(`orchestra_dlq_depth{agent="${p.id}"} ${p.dlq}`);
  lines.push("# TYPE orchestra_llm_tokens_total counter", `orchestra_llm_tokens_total ${metrics.llmTokens}`);
  return reply.header("content-type", "text/plain; version=0.0.4").send(lines.join("\n") + "\n");
});

app.get("/api/agents", async () => {
  return await bus.listAgents();
});

// Durable session history (written by the recorder). Optionally per-agent.
app.get("/api/sessions", async (req) => {
  const { agentId } = req.query as { agentId?: string };
  return await store.listSessions({ agentId });
});

// The canonical session record: the pi AgentMessage[] the LLM actually receives —
// the same array the transcript should render so operator and model never disagree.
// Hot sessions read the agent's live Redis snapshot; archived ones read the S3 copy
// the recorder wrote on each idle. `messages: null` (pre-migration session with
// neither) tells the UI to fall back to the event-folded transcript.
app.get("/api/sessions/:sessionId/context", async (req) => {
  const { sessionId } = req.params as { sessionId: string };
  const session = await store.getSession(sessionId);
  if (session) {
    if (session.status === "open") {
      const hot = await bus.loadTaskContextOf(session.agentId, sessionId);
      if (hot) return { session, messages: hot };
    }
    if (session.contextUri) {
      try {
        return { session, messages: await archive.getContext(session.contextUri) };
      } catch {
        /* fall through to null */
      }
    }
  }
  return { session: session ?? null, messages: null };
});

// A past session's raw event trace (debug view): prefer the durable S3 archive,
// fall back to the live Redis stream (e.g. a session not yet archived).
app.get("/api/sessions/:sessionId/transcript", async (req) => {
  const { sessionId } = req.params as { sessionId: string };
  const session = await store.getSession(sessionId);
  let envs: Envelope<ObservabilityEvent>[];
  if (session?.traceUri) {
    try {
      envs = (await archive.getTrace(session.traceUri)) as Envelope<ObservabilityEvent>[];
    } catch {
      envs = await bus.readEvents(sessionId);
    }
  } else {
    envs = await bus.readEvents(sessionId);
  }
  return { session: session ?? null, events: envs.map((e) => e.payload) };
});

// The delegation tree rooted at a session: the session plus every descendant
// linked by parent_session_id (flat list; each row keeps its parentSessionId).
app.get("/api/sessions/:sessionId/tree", async (req) => {
  const { sessionId } = req.params as { sessionId: string };
  return await store.getSessionTree(sessionId);
});

// A session's artifacts (Phase 5). The cards themselves render from the trace's
// `artifact` events; this is the list endpoint for programmatic/history use.
app.get("/api/sessions/:sessionId/artifacts", async (req) => {
  const { sessionId } = req.params as { sessionId: string };
  return await store.listArtifacts(sessionId);
});

// Artifact download proxy (Phase 5). The browser never gets S3 credentials: it
// asks the orchestrator, which streams the object straight through. Restricted to
// our own bucket so the proxy can't be turned into an SSRF for arbitrary uris.
app.get("/api/artifacts", async (req, reply) => {
  const { uri, download } = req.query as { uri?: string; download?: string };
  if (!uri || !uri.startsWith(`s3://${artifactStore.bucket}/`)) {
    return reply.code(400).send({ error: "missing or out-of-bucket uri" });
  }
  try {
    const { body, contentType, contentLength } = await artifactStore.getArtifact(uri);
    const name = uri.split("/").pop() ?? "artifact";
    reply.header("content-type", contentType ?? "application/octet-stream");
    if (contentLength) reply.header("content-length", String(contentLength));
    reply.header("content-disposition", `${download ? "attachment" : "inline"}; filename="${name}"`);
    return reply.send(body); // Fastify streams the Readable to the client
  } catch (err) {
    return reply.code(404).send({ error: String(err) });
  }
});

// Announce a new session so the recorder opens its row and tails its stream.
function announce(sessionId: string, agentId: string): Promise<void> {
  return bus.announceSession({
    event: "open",
    sessionId,
    agentId,
    source: "orchestrator",
    ts: Date.now(),
  });
}

// Start a chat: allocate the session, remember its agent, send the first turn.
app.post("/api/sessions", async (req, reply) => {
  const { agentId, text } = req.body as { agentId: string; text: string };
  const sessionId = uuidv7();
  sessionAgent.set(sessionId, agentId);
  await announce(sessionId, agentId);
  await bus.publishControl(agentId, { type: "user_message", text }, { sessionId });
  return reply.send({ sessionId });
});

// Resolve a session's agent and lifecycle. The agent id is cached in memory (and
// falls back to the cache for sessions the recorder hasn't written yet), but the
// archived flag always comes from Postgres — a session can be flipped read-only
// underneath us, so the cache can't be trusted for status.
async function resolveSession(
  sessionId: string,
): Promise<{ agentId?: string; archived: boolean }> {
  const s = await store.getSession(sessionId);
  if (s) sessionAgent.set(sessionId, s.agentId);
  return { agentId: s?.agentId ?? sessionAgent.get(sessionId), archived: s?.status === "archived" };
}

// Continue a chat: route to the session's agent, which decides prompt-vs-steer.
// An archived session is strictly read-only — its hot context expired from Redis,
// so a new turn would silently start with amnesia; refuse it instead.
app.post("/api/sessions/:sessionId/messages", async (req, reply) => {
  const { sessionId } = req.params as { sessionId: string };
  const { text } = req.body as { text: string };
  const { agentId, archived } = await resolveSession(sessionId);
  if (archived) return reply.code(410).send({ error: "session archived (read-only)" });
  if (!agentId) return reply.code(404).send({ error: "unknown session" });
  await bus.publishControl(agentId, { type: "user_message", text }, { sessionId });
  return reply.send({});
});

// Rename a session by hand: announce a rename on the bus so the recorder (the
// only Postgres writer) updates the title. Empty title clears it.
app.post("/api/sessions/:sessionId/rename", async (req, reply) => {
  const { sessionId } = req.params as { sessionId: string };
  const { title } = req.body as { title?: string };
  await bus.announceSession({
    event: "rename",
    sessionId,
    title: (title ?? "").trim(),
    source: "orchestrator",
    ts: Date.now(),
  });
  return reply.send({});
});

// Hard stop a running agent on this session.
app.post("/api/sessions/:sessionId/cancel", async (req, reply) => {
  const { sessionId } = req.params as { sessionId: string };
  const { agentId, archived } = await resolveSession(sessionId);
  if (archived) return reply.code(410).send({ error: "session archived (read-only)" });
  if (!agentId) return reply.code(404).send({ error: "unknown session" });
  await bus.publishControl(agentId, { type: "cancel" }, { sessionId });
  return reply.send({});
});

app.post("/api/runs", (req, reply) => {
  const { agentId, prompt } = req.body as { agentId: string; prompt: string };
  const sessionId = uuidv7();
  sessionAgent.set(sessionId, agentId);
  void announce(sessionId, agentId);

  // Fire-and-forget: return the sessionId now so the UI can subscribe before the
  // run starts emitting, then run the request in the background and log the reply.
  void bus
    .request<RunResult>(agentId, { prompt } satisfies RunRequest, { sessionId, timeoutMs: 120_000 })
    .then((result) => app.log.info({ sessionId, agentId, result }, "run complete"))
    .catch((err) => app.log.warn({ sessionId, agentId, err: String(err) }, "run failed"));

  reply.send({ sessionId });
});

app.get("/api/sessions/:sessionId/stream", { websocket: true }, (socket, req) => {
  const { sessionId } = req.params as { sessionId: string };
  // `from=now` tails only new events — used when resuming a past session whose
  // transcript the UI has already seeded from the durable archive (so the replay
  // isn't duplicated). Default `start` replays the whole stream then tails.
  const { from } = req.query as { from?: string };
  const stop = bus.subscribeEvents(
    sessionId,
    (ev) => {
      if (socket.readyState === socket.OPEN) socket.send(JSON.stringify(ev));
    },
    { from: from === "now" ? "now" : "start" },
  );
  socket.on("close", stop);
});

app.get("/api/presence/stream", { websocket: true }, (socket) => {
  const stop = bus.watchPresence((ev) => {
    if (socket.readyState === socket.OPEN) socket.send(JSON.stringify(ev));
  });
  socket.on("close", stop);
});

await app.listen({ port: PORT, host: "0.0.0.0" });

const shutdown = async () => {
  await app.close();
  await bus.close();
  await store.close();
  archive.close();
  artifactStore.close();
  process.exit(0);
};
process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);
