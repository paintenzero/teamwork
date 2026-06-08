# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

`orchestra` — a flat, broker-based AI agent network where every participant
(agents, the orchestrator, each web-UI session) is an equal peer on a Redis bus.
The repo is built in phases. **Phase 0** shipped the wire contract, the `Bus`
client SDK, and the local infra (Redis + Postgres + SeaweedFS). **Phase 1** added
a real `pi-agent-core` agent, the orchestrator (the only bus↔UI bridge), and a
Vite/React UI for watching one agent run live. **Phase 2** made it two-way: a chat
input, multi-turn conversation, and mid-run **steer**/**stop** — chat input and
agent-to-agent steering are the same control message on the same channel.
Phase 3 added durability: a dedicated **recorder** (`apps/recorder`) writes the
Postgres spine + S3 trace archive, with a **history** view in the UI.
Phase 4 made agents talk to each other: an agent delegates a subtask
to another via the **`ask_agent`** tool over request/reply (*another agent is just a
`pi` tool*), forming a parent→child session tree, guarded against cycles, excessive
depth, and runaway time budgets. Human chat stays on the control plane; A2A is RPC;
one agent serves both. Phase 5 adds **artifacts**: large outputs go to
S3 and only a reference (`ArtifactRef`) travels on the bus — an agent stores one with
**`save_artifact`** and reads one with **`read_artifact`** (the A→B handoff is just
passing a URI), the orchestrator serves them through a credential-free download
**proxy**, and the UI renders artifact cards. **Phase 6 (current)** hardens the
system — all ten workstreams shipped: security (Redis ACL + bearer-token gateway),
idempotency, delivery recovery + DLQ, durable control delivery, reconnection,
schema-version checks, observability (`/metrics`,`/healthz`,`/readyz`), budgets +
rate limits, and a transport ADR. `plans/phase-6-task.md` is the spec (with an
as-built note); `docs/security.md` has the threat model and
`docs/adr/0001-redis-vs-rabbitmq.md` the Redis-vs-RabbitMQ decision.

## Layout

A pnpm workspace (`pnpm-workspace.yaml` → `packages/*`, `apps/*`):

    packages/protocol   @paintenzero/orchestra-protocol — src/{envelope,channels,events,ids,runs}.ts, barrel src/index.ts
    packages/bus        @paintenzero/orchestra-bus — src/{bus,types}.ts, barrel src/index.ts (deps: protocol, ioredis)
    packages/store      @paintenzero/orchestra-store — src/{store,archive}.ts; Store (pg) + TraceArchive + ArtifactStore (S3). ONLY home of pg / @aws-sdk
    apps/agent          @orchestra/agent — pi-agent-core agent + ask_agent / save_artifact / read_artifact tools; env-roled (AGENT_ID/DESCRIPTION/CAPABILITIES/SYSTEM_PROMPT); run via tsx (deps: bus, protocol, store)
    apps/orchestrator   @orchestra/orchestrator — Fastify + @fastify/websocket bus↔UI bridge + artifact download proxy; run via tsx (deps: bus, protocol, store)
    apps/recorder       @orchestra/recorder — bus peer, the single persistence writer (deps: bus, protocol, store); run via tsx
    apps/web            @orchestra/web — Vite/React UI; imports protocol TYPES ONLY (never bus / store / ioredis / pg)
    examples/ping-pong.ts   the bus smoke test (a loose file, run by the root `tsx`, not its own package)
    db/init.sql         Postgres schema; mounted by docker-compose AND applied by the recorder on startup (stale volume self-heals)
    .env.example        copy to .env
    tsconfig.base.json  every package's tsconfig extends this

`packages/*` build with `tsc -p tsconfig.json` to `dist/` (the `exports` point
there, so **`pnpm build` must run before `pnpm smoke`**). The `apps/*` run
straight from source via `tsx` (agent, orchestrator, recorder) or `vite` (web); their
`build` scripts only type-check (`tsc --noEmit`), except web which also does a
`vite build`. Intra-package imports use explicit `.js` extensions (NodeNext ESM);
app sources use bundler-style resolution. Cross-package imports use the
`@orchestra/*` specifiers — don't rewrite those to relative paths.

This is **not a git repository.**

## Commands

    pnpm install
    pnpm build          # pnpm -r build: type-checks every package + app, vite-builds web
    pnpm infra:up       # docker compose up -d: Redis, Postgres, SeaweedFS
    pnpm infra:down
    pnpm smoke          # tsx examples/ping-pong.ts — the end-to-end bus test
    pnpm agent          # start the agent  (faux model unless ANTHROPIC_API_KEY is set)
    pnpm orchestrator   # start the orchestrator on :3001
    pnpm recorder       # start the persistence recorder (Postgres spine + S3 trace archive)
    pnpm web            # start the Vite dev server on :5173 (proxies /api → :3001)

`pnpm smoke` is the only automated test. It requires `pnpm infra:up` first (it
needs a live Redis). A successful run prints presence, an echoed reply, a control
signal, a `status` event, a deliberate timeout, and ends in `OK`. The Phase 1
demo (`agent` + `orchestrator` + `web`, then open <http://localhost:5173>) is the
manual end-to-end check. There is no unit test runner, linter, or formatter.

Requires Node >= 22.19 (a `legacy-node20` path is mentioned for `pi-agent-core`).

## Architecture

Everything is mediated by **`Bus`** (`packages/bus/src/bus.ts`). One `Bus` instance per
participant, constructed with a stable `clientId`. The transport (Redis Streams +
pub/sub) is fully hidden behind the SDK; swapping transports later is meant to be
contained to this one class. The capabilities:

- **Presence** — `register()` writes a TTL'd key (15s) and starts a 5s heartbeat;
  `listAgents()` SCANs them; `findAgents(cap)` filters those by capability (the A2A
  discovery primitive); `watchPresence()` subscribes to register/update/deregister.
- **Request/reply** — `request(target, payload)` XADDs to `req:<target>` and
  BLOCK-reads a unique `reply:<client>:<corrId>` stream. `onRequest()` consumes
  `req:<self>` via a consumer group (`workers`) so multiple instances of one
  agent load-balance. **Delivery is at-least-once** (explicit XACK, redelivery
  possible) — keep handlers idempotent or dedupe on `meta.messageId`.
- **Observability** — `publishEvent(sessionId, ev)` appends to `obs:<sessionId>`
  (XADD MAXLEN ~10k, replayable). `subscribeEvents(sessionId, fn, {from})` tails
  it; `from: "start"` replays full history then tails, `"now"` (default) only new.
  This is the per-session trace the UI watches. `readEvents(sessionId)` is a
  one-shot XRANGE of the whole stream (used for trace archival + durable reads).
- **Control** — `publishControl(agentId, msg, {sessionId?})` / `subscribeControl()`
  over pub/sub for chat/steer/cancel/ping against an agent's current run. The
  optional `sessionId` rides on the envelope so the agent knows which conversation
  a message belongs to; `subscribeControl` hands the handler the full envelope.
- **Sessions** — `announceSession(ev)` publishes a `SessionEvent` (`open`) to
  `sessions:events`; `watchSessions(fn)` subscribes (mirrors presence). The
  initiator announces each new session so the recorder can persist + tail it.

Two aux Redis connections exist per blocking read / pub/sub subscription
(`aux()` duplicates the command connection); the command connection itself is
kept non-blocking. `close()`/unsubscribe functions tear these down.

**The wire contract** (`@paintenzero/orchestra-protocol`, in `packages/protocol/src/`) is
shared by all peers so no two can disagree:

- `envelope.ts` — `Envelope<P>`, the single shape on every channel. `makeEnvelope`
  stamps `schemaVersion` (`SCHEMA_VERSION`, bump on incompatible changes),
  `messageId` (uuidv7), `ts`. `correlationId` ties a reply to its request and a
  whole delegation tree; `causationId` is the direct parent for causal chains.
- `channels.ts` — the **single source of truth for every Redis key name**. Never
  hardcode a key elsewhere; add/derive it here.
- `events.ts` — `ObservabilityEvent` (its first 10 members mirror
  `@earendil-works/pi-agent-core`'s `AgentEvent` exactly so an agent can forward
  `agent.subscribe(...)` output with zero translation; the last three —
  `user_message` (the recorded user/peer turn, for replay), `artifact`, `status` —
  are orchestra additions), `ControlMessage` (chat `user_message` /
  `inject_message` / `follow_up` / `cancel` / `ping`), `ResponseBody`,
  `AgentRegistration`, `PresenceEvent`, `SessionEvent` (session-open announcement).
- `ids.ts` — `uuidv7()`: time-sortable ids, so stream order and id order agree.
  Used for `messageId` and any entity id you want chronologically sortable.
- `delegation.ts` — `DelegateRequest` (`prompt`, `parentSessionId`,
  `delegationPath`, optional `budget`) / `DelegateResult`, and `isDelegateRequest`
  (structural: a DelegateRequest carries a `delegationPath`, a RunRequest doesn't).

**The browser never touches Redis.** It talks WebSocket to the orchestrator,
which is the only bus↔UI bridge.

**Apps and the seam.** `apps/agent` constructs one `Bus` and one pi `Agent` **per
task** (see *Agent-to-agent* below for the per-task session model); each task's
`agent.subscribe(...)` forwards that task's `AgentEvent`s to `publishEvent(taskId, …)`
(fire-and-forget — ordering holds because the shared command connection pipelines
XADDs in call order).

*Chat & steering (Phase 2).* The interactive path rides the **control** channel:
the orchestrator's `POST /api/sessions` (+ `…/messages`, `…/cancel`) calls
`publishControl(agentId, {type:"user_message", text}, {sessionId})`. The agent routes
by **session id = task** (see *Agent-to-agent*): if that task's turn is in-flight ⇒
`steer` it (injected at the next turn boundary, `steeringMode = "all"`); otherwise run
a turn in that task's own context (created/restored on first use, queued behind any
in-flight turn by the single-flight `runChain`). `cancel` ⇒ that task's
`agent.abort()`. Every user turn is emitted as a `user_message` observability event
**before** running, so the transcript replays with user turns in order. A task spans
many turns and its context persists (snapshotted to Redis).

*Non-interactive kickoff (Phase 1 path, kept).* `onRequest` sets `currentSessionId`
from the envelope, emits the user turn, runs the prompt via the same `runTurn`
helper, and returns the last assistant message's text as the `RunResult` reply.
**The initiator allocates the session id**: `apps/orchestrator` generates it,
returns it from `POST /api/runs` / `POST /api/sessions` *before* the run starts (so
the UI can subscribe with no race) and puts it on the envelope; the agent reuses it
for both the run and its events. Each session id is a **task** with its own
conversation (see *Agent-to-agent*), so a second conversation is simply a second task
(its own context), not a steer of the first; turns are serialized single-flight. The
session→agent map is an in-memory
cache in the orchestrator that **falls back to Postgres** (`store.getSession`) on a
miss, so chat routing survives an orchestrator restart. The agent's model is
env-selected (`buildModel`), highest priority
first: a **custom OpenAI-compatible endpoint** (`OPENAI_BASE_URL` + `OPENAI_MODEL`
+ `OPENAI_API_KEY` — just a `Model<"openai-completions">` literal pointed at the
`baseUrl`; reasoning surfaces via `OPENAI_THINKING_FORMAT`/`OPENAI_THINKING_LEVEL`,
e.g. `qwen-chat-template` for qwen on llama.cpp), then **Anthropic** `claude-haiku-4-5`,
then the keyless **faux** provider so the whole stack runs token-free. The
`getApiKey` hook returns `OPENAI_API_KEY` for the `openai-compat` provider tag.

*Persistence (Phase 3).* `apps/recorder` is a **bus peer, not a UI bridge**, and
the **single writer** of the spine. It taps three things and persists via
`@paintenzero/orchestra-store`: `presence:events` ⇒ upsert `agents`; `sessions:events` ⇒ insert
a `sessions` row then `subscribeEvents(sid, …, {from:"start"})` to tail it;
`obs:<sid>` ⇒ `messages` (user turn = `request`, `agent_end` = `response`),
`artifacts`, and on each `status:"idle"` re-archive the full trace
(`bus.readEvents`) as JSONL to S3 (`traces/<sid>.jsonl`) and store the `trace_uri`.
**Every write is idempotent** — the row id is the originating event's `messageId`
(uuidv7, stable across replays), inserted `ON CONFLICT DO NOTHING` — so a recorder
restart that re-reads a stream from the start never duplicates; it also re-tails
still-open sessions (`store.listOpenSessions`) on startup to catch missed turns.
`@paintenzero/orchestra-store` is the **only** package that imports `pg` / `@aws-sdk/client-s3`;
the orchestrator uses it **read-only** for `GET /api/sessions` and
`GET /api/sessions/:id/transcript` (S3 archive, else `bus.readEvents` Redis range).
The recorder applies `db/init.sql` on startup (idempotent), so a stale Postgres
volume self-heals. **Update (per-task sessions):** an agent now snapshots each task's
working memory to Redis (`taskctx:*`) after every turn and restores it on the next
turn, so a task's in-memory conversation context **does** survive idle eviction and an
agent restart — the Phase 3 `SessionStorage` follow-up, delivered per task (see
*Agent-to-agent*). `db/init.sql` still also provisions `artifacts` (Phase 5).

*Agent-to-agent (Phase 4, with description-based discovery).* **Discovery is by
description, not tags:** each agent registers an `AGENT_DESCRIPTION` (on
`AgentRegistration.description`), and every agent injects the **roster of the other
online agents' descriptions into its system prompt on every run** (via
`composeSystemPrompt`, set on `agent.state.systemPrompt` before each `agent.prompt` —
pi's `createContextSnapshot` reads it fresh each turn). The roster is refreshed on a
timer (`ROSTER_REFRESH_MS`, default 60s, `unref`'d) from `bus.listAgents()`, so
connect/disconnect is reflected. The model then picks whom to delegate to by reading
the descriptions. Another agent is a `pi` tool: every agent carries
**`ask_agent`** (`apps/agent`), targeting a peer **by id**. A collaboration is a
**persistent per-task session**, not a one-shot: `ask_agent({ agent, prompt })` opens
a task (mints a `taskId`, runs the first round-trip, returns the id), **`continue_task({
task, prompt })`** does further round-trips on the *same* task (the peer keeps that
conversation's context), and **`close_task({ task })`** finalizes it.
**Per-task sessions (the model that replaced "single active session per agent"):** an
agent holds a `Map<taskId, { agent: pi Agent, busy, delegation ctx }>` — **one pi
`Agent` (one conversation) per task**, keyed by `taskId` (= the session id). Multi-turn
on one task continues that context; a different `taskId` is a different conversation.
Working memory is **snapshotted to Redis** (`bus.saveTaskContext` →
`taskctx:<agent>:<taskId>`, TTL'd) after every turn and **restored** on the next turn,
so a task survives idle eviction (`TASK_TTL_MS`/`MAX_TASKS`) and a process restart.
Runs are **single-flight** (a `runChain` serializes turns across tasks) so the faux
provider and the `activeTaskId` pointer the tools read stay unambiguous — parallelism
is by running more agent instances (the `req:` consumer group load-balances). The
**receive side** is the same `onRequest` that serves the Phase 1 kickoff —
`isDelegateRequest(payload)` splits it: a `DelegateRequest` runs in the task named by
`req.taskId` (minted if absent), `announceSession`d with `parentSessionId` on first use
(so the recorder persists the parent→child edge); `req.close` finalizes; a `RunRequest`
runs in the envelope's session-as-task. Independent of the control (human-chat) path —
human chat is just a task keyed by its session id — so an agent serves humans and peers
the same way and can hold **many** task conversations at once.
**Guards are mandatory and live in `ask_agent` (they throw → surfaced as a
`tool_execution_end` error in the trace, never a hang):** *cycle* (target already in
`delegationPath`), *depth* (`path.length > maxDepth`), *budget* (`deadlineTs`
propagates down; each hop's `timeoutMs = min(default, remaining)`; an exhausted
budget fails before the call). A run sets its delegation context (`delegationPath`,
`deadlineTs`, `maxDepth`, `causationId`) at the start: empty/root for a human turn
or `RunRequest`, inherited-from-`budget` for a `DelegateRequest`. The orchestrator
exposes the tree via `GET /api/sessions/:id/tree` (recursive CTE in
`@paintenzero/orchestra-store`); the UI renders it as a **Delegations** panel you can drill into.
Dispatch is **directed by id** (the model's choice from the description roster);
`findAgents(capability)` remains for any tag-based filtering. Capability-queue is deferred.

*Artifacts (Phase 5).* Large outputs live in S3; **only an `ArtifactRef`
(`{uri, contentType?, name?}`) ever travels on the bus** — never the bytes. The
bytes go through a new **`ArtifactStore`** in `@paintenzero/orchestra-store` (`putArtifact` /
`getArtifact`, **streaming**), sitting next to `TraceArchive` and sharing one
`makeS3Client` factory — so the **`@aws-sdk/*`-only-in-store** invariant holds and
the bus SDK stays Redis-only (`apps/agent` gained an `@paintenzero/orchestra-store` dep for
this). Each artifact is its own object at `s3://<bucket>/<sessionId>/<uuid>-<name>`.
Two agent tools (`apps/agent`): **`save_artifact`** puts bytes and emits the
`artifact` obs event carrying just the ref; **`read_artifact`** streams an object
back, inlining text up to `MAX_INLINE_BYTES` (64 KiB) and returning a *reference*
for binary/oversized so the LLM context never bloats. An **A→B handoff** is just A
passing a URI in its result and B calling `read_artifact` — bytes never touch the
bus. The **recorder stays the single writer** (the Phase 3 `artifact`-event →
`artifacts`-row path is unchanged); Phase 5 added only the read side
(`Store.listArtifacts`, `GET /api/sessions/:id/artifacts`) and a download **proxy**
(`GET /api/artifacts?uri=`) that streams S3→browser, restricted to our own bucket
(out-of-bucket uris → 400, so it can't be an SSRF) — **the browser never gets S3
credentials**. The UI renders **artifact cards** from the trace's `artifact` events
(so they show in both live and history): inline image preview, short text preview,
or a download link, all via the proxy. *Deliberately not done* (see the spec's
as-built note): a custom pi `artifact` message + `convertToLlm` — the
`save_artifact` tool-result already carries the ref into context, so it'd be
redundant (and keeping only the ref in context is what "don't bloat the context"
wants).

*Security (Phase 6, security increment).* **Bus authn/authz:** Redis runs with an
ACL file (`redis/users.acl`, mounted via `--aclfile`); the `default` user has a
password so **no-auth is refused**. The Bus authenticates with `REDIS_USERNAME`/
`REDIS_PASSWORD` (dev defaults `dev`/`orchestra-dev` keep the stack runnable; every
`duplicate()` inherits the creds). Roles: `dev` (broad, tooling/smoke),
`orchestrator`/`recorder` (broad non-dangerous — they span all sessions), and
**per-agent `agent-<id>`** users that are namespaced — read any presence (discovery)
but write only their own `presence:agent:<id>`, subscribe only their own
`control:<id>`, with `-@dangerous -@admin`. So a compromised agent **cannot** read
another's `control:` or forge another's `presence:` key (verified via `NOPERM`).
**TLS:** opt-in via `rediss://`/`REDIS_TLS=1` + `REDIS_TLS_CA`; `docker-compose.tls.yml`
runs Redis TLS-only (plaintext port disabled). **Gateway auth (WS2):** the
orchestrator's `onRequest` hook requires a bearer token on every HTTP/WS request
(`Authorization: Bearer` or `?token=` for browser WebSockets, which can't set
headers) ⇒ `401` without it; `apps/web/src/api.ts` attaches it (header for fetch,
query for `<img>`/`<a>`/WS). The browser still never touches Redis. Residual gaps and
the threat model are written down in `docs/security.md` (shared announcement channels
are forgeable-but-transient; `obs:*` isn't per-session scoped; the gateway token is a
shared secret; pi `streamProxy` LLM-key centralization is deferred).

*Delivery, resilience & observability (Phase 6, WS3–WS9).* **Idempotency:**
`bus.dedupe(key)` = `SET NX PX` (24h, namespaced per client); `handleRequest` claims
`req:<messageId>` *before* executing so a redelivery never double-runs an expensive
handler, and the agent dedupes control on `ctrl:<messageId>`. **Delivery recovery:**
`onRequest` runs a periodic (`unref`'d) sweep — `XPENDING`→`XCLAIM` reclaims requests a
dead worker left pending (re-process, dedupe-guarded), and after `MAX_DELIVERIES`
routes poison to `req:<agent>:dlq`. **Durable control (WS5):** `publishControl` `XADD`s
to a MAXLEN'd `cstream:<agent>`; `subscribeControl` tails it via blocking `XREAD`
resuming from a persisted `cpos:<agent>`, so a chat sent while an agent is briefly
offline is delivered on reconnect (replaces Phase-2 fire-and-forget pub/sub).
**Resilience (WS6):** the Bus re-asserts presence on every Redis `ready`; blocking loops
resume from their last id; the UI auto-reconnects (1s backoff) and replays without a
reload. **Schema (WS7):** `isSchemaCompatible(env)` — a mismatched request gets an
`{ok:false}` reply; control/events are dropped with a warning. **Observability (WS8):**
the orchestrator exposes `/healthz`, `/readyz` (pings Redis+Postgres), and `/metrics`
(Prometheus text), token-exempt as ops endpoints. **Budgets/limits (WS9):** `runTurn`
aborts when the tree's `deadlineTs` (or `MAX_RUN_MS` root cap) passes; a fixed-window
gateway rate limit (`GATEWAY_RATE_LIMIT_RPS`) → `429`; `OBS_MAXLEN`/`DEDUPE_TTL_MS`/
reclaim windows are env-tunable; the recorder drops `message_update` deltas from the S3
archive. All new tunables have safe defaults (`.env.example`). Transport stays Redis
(ADR 0001).

## Conventions

- ESM throughout (`"type": "module"`, NodeNext). Intra-package relative imports
  use explicit `.js` extensions; cross-package imports use `@orchestra/*`.
- TypeScript `strict` with `verbatimModuleSyntax` — use `import type` for types.
- Redis access goes through `channels.*` helpers only.
- Postgres/S3 access goes through `@paintenzero/orchestra-store` only; never import `pg` or
  `@aws-sdk/*` elsewhere (and never in `apps/web`). The recorder is the only writer.
