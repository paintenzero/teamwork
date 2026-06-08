# orchestra — Phase 6 (hardening)

A flat, broker-based AI agent network. Everything — agents, the orchestrator,
each web-UI session — is an equal peer on a Redis bus. Phase 0 shipped the wire
contract, the client SDK every participant imports, and the local infra. Phase 1
added a real agent, the orchestrator, and a web UI for *watching* a single agent.
Phase 2 made it two-way (chat, steer, stop); Phase 3 made it durable (a recorder
writes the Postgres spine + S3 trace archive, with a history view); Phase 4 made
agents talk to each other (one delegates a subtask to another — *another agent is
just a tool* — recorded as a guarded parent→child session tree).
Phase 5 let agents exchange artifacts (large outputs to S3; only a *reference*
travels on the bus). **Phase 6 hardens the system** into something you can run with
confidence: **security** (Redis ACL — no-auth refused, TLS available, agents
namespaced; bearer-token gateway), **delivery correctness** (idempotent
`messageId` dedupe, `XPENDING`/`XCLAIM` reclaim of dead workers + a dead-letter
stream, durable replayable control so a chat isn't lost if an agent blips),
**resilience** (clients reconnect and re-register; the UI auto-reconnects without a
reload), **schema-version** rejection, **observability** (`/metrics`, `/healthz`,
`/readyz`), and **budgets/limits** (per-tree time budget, gateway rate limit). The
transport decision is recorded in an ADR. See [`docs/security.md`](docs/security.md)
for the threat model and [`docs/adr/0001-redis-vs-rabbitmq.md`](docs/adr/0001-redis-vs-rabbitmq.md)
for the Redis-vs-RabbitMQ decision. The browser still never touches Redis.

## Layout

A pnpm workspace (`pnpm-workspace.yaml` → `packages/*`, `apps/*`):

    packages/protocol      @paintenzero/orchestra-protocol — wire contract (envelope, channels, events, ids, runs) — zero runtime deps
    packages/bus           @paintenzero/orchestra-bus — Redis-backed Bus client (presence, request/reply, events, control, sessions)
    packages/store         @paintenzero/orchestra-store — Postgres spine (Store) + S3 trace archive (TraceArchive) + artifact store (ArtifactStore); the only home of pg / aws-sdk
    apps/agent             @orchestra/agent — pi-agent-core agent; ask_agent delegation + save_artifact/read_artifact tools, env-roled (id/caps/prompt)
    apps/orchestrator      @orchestra/orchestrator — Fastify bus↔UI bridge (roster, chat, history + tree reads, artifact proxy, WS streams)
    apps/recorder          @orchestra/recorder — bus peer: persists agents/sessions/messages/artifacts (+ parent links), archives traces to S3
    apps/web               @orchestra/web — Vite/React chat + history + delegation tree + artifact cards (types-only from protocol; never touches Redis)
    examples/ping-pong.ts  end-to-end bus smoke test (loose file, run by the root `tsx`)
    docker-compose.yml     Redis + Postgres + SeaweedFS (S3)
    db/init.sql            Postgres schema (recorder applies it on startup too, so a stale volume self-heals)

Each package builds with `tsc` to `dist/`; `pnpm -r build` runs them in
dependency order (protocol before bus), and the package `exports` resolve to the
built output — so **`pnpm build` must run before `pnpm smoke`.**

## Prerequisites

- Node >= 22.19 (pi-agent-core requires it; a `legacy-node20` path exists)
- pnpm (`corepack enable && corepack prepare pnpm@9 --activate`), Docker

## Run

    cp .env.example .env   # optional — sensible dev defaults are baked in
    pnpm install
    pnpm build
    pnpm infra:up      # starts Redis (ACL-protected), Postgres, SeaweedFS
    pnpm smoke         # runs examples/ping-pong.ts against Redis
    pnpm infra:down    # stop the stack when done

Redis now runs with an ACL (no-auth refused) and the gateway requires a token —
both ship with **dev defaults** baked into the Bus / web build, so the commands
above Just Work. For a real deployment set `REDIS_PASSWORD`/per-agent users (see
`redis/users.acl`), `GATEWAY_TOKEN`/`VITE_GATEWAY_TOKEN`, and optionally TLS
(`docker compose -f docker-compose.yml -f docker-compose.tls.yml up -d`).

Expected: presence listing, an echoed reply, a delivered control signal, a
`status` observability event, and a deliberate timeout — ending in `OK`.

## Run the demo

With `pnpm install`, `pnpm build`, and `pnpm infra:up` done, start the four
processes in separate terminals:

    pnpm recorder       # persists agents/sessions/messages to Postgres; archives traces to S3
    pnpm agent          # the pi-agent-core agent (registers as agent-researcher)
    pnpm orchestrator   # Fastify bus↔UI bridge on :3001
    pnpm web            # Vite dev server on :5173 (proxies /api → orchestrator)

Then open <http://localhost:5173>:

1. `agent-researcher` appears in the roster (green = idle). Kill the agent and it
   drops off live (or within ~15s via presence TTL); restart it and it returns.
2. Type a message and hit **Send**. The transcript streams live and in order:
   your turn as a bubble, then the agent's *thinking* (rendered distinctly),
   assistant text, and any `clock` tool call with its args and result.
3. Send a follow-up — the agent continues the **same** conversation with prior
   context retained.
4. While the agent is running, send another message: it's injected as **steering**
   at the next turn boundary (a hint in the UI says so) and visibly changes course
   — it is *not* a second run.
5. While the agent is running, hit **Stop** — the run aborts promptly and the
   status returns to idle.
6. Refresh the browser mid-conversation — the session id rides in the URL hash, so
   it re-subscribes and replays the whole transcript (your turns included) from the
   start.
7. The **History** list in the sidebar shows past sessions (written by the
   recorder). Click one to reload its full transcript read-only — sourced from the
   durable S3 archive, so it survives Redis trimming and restarts.

### Agent-to-agent

Agents advertise themselves **by description** (natural language), not by tags. Each
agent loads the roster of the other online agents and **injects their descriptions
into its system prompt on every run** (refreshed every `ROSTER_REFRESH_MS`, default
60s, so connect/disconnect is picked up) — so the model decides whom to delegate to
the same way it picks a tool: by reading what each one does. For the demo, run **two**
agents with descriptions (token-free with faux roles), plus recorder + orchestrator + web:

    AGENT_ID=agent-writer     AGENT_FAUX=1 AGENT_FAUX_ROLE=worker      AGENT_DESCRIPTION="Writes and edits short prose and reports." pnpm agent
    AGENT_ID=agent-researcher AGENT_FAUX=1 AGENT_DELEGATE_TO=agent-writer AGENT_DESCRIPTION="Researches topics and coordinates teammates." pnpm agent

Ask `agent-researcher` for "a short report". It calls its **`ask_agent`** tool —
another agent is just a `pi` tool — targeting the writer **by id** (chosen from the
roster in its prompt), `bus.request`s it, waits for the draft, and folds it into its
answer. The UI shows a **Delegations** panel: the researcher's session with the
writer's child session linked beneath it (click to drill into the child's trace). The
delegation is guarded:

- **Cycle** — delegating to an agent already on the path is rejected.
- **Depth** — paths longer than `MAX_DELEGATION_DEPTH` are rejected.
- **Budget** — a `deadlineTs` propagates downward; each hop's timeout is
  `min(default, remaining)`; an exhausted budget fails before the call.

Guards **throw**, so the failure shows in the trace as a tool error (and the call
returns rather than hangs). `tsx examples/a2a-check.ts` (against a running stack)
exercises all four: happy / cycle / depth / deadline.

**Multi-turn tasks.** A collaboration is a *persistent per-task session*, not a
one-shot. `ask_agent` opens a task and returns a **task id**; the caller uses
**`continue_task(task, prompt)`** for further round-trips on the *same* task (the
peer keeps that conversation's context) and **`close_task(task)`** to finish. On the
receiver, **each task is its own conversation** keyed by id — multi-turn on one task
continues its context; a different task id is a separate context. Working memory is
snapshotted to Redis after every turn, so a task survives idle eviction
(`TASK_TTL_MS` / `MAX_TASKS`) and an agent restart. `tsx examples/task-session-check.ts`
proves it: two turns on one task share one session, a second task is separate, and the
context is restored after the worker restarts.

### Artifacts (Phase 5)

Large outputs go to S3; only a reference travels on the bus. An agent calls
**`save_artifact`** (bytes → S3, returns an `ArtifactRef`, emits an `artifact`
event) and **`read_artifact`** (fetches by URI; text inline, binary by reference
so the context never bloats). The **A→B handoff** is just A passing the URI to B,
who reads it out-of-band — the bytes never touch the bus. The UI renders an
**artifact card** (inline image preview / short text preview / download) that
loads through the orchestrator's **proxy** (`GET /api/artifacts?uri=`), so the
browser never sees S3 credentials and the proxy only serves our own bucket.

Token-free demo — run the recorder + two faux agents, then the check script:

    AGENT_ID=agent-producer AGENT_CAPABILITIES=produce AGENT_FAUX=1 AGENT_FAUX_ROLE=producer  pnpm agent
    AGENT_ID=agent-reader   AGENT_CAPABILITIES=read    AGENT_FAUX=1 AGENT_FAUX_ROLE=reader     pnpm agent

`tsx examples/artifacts-check.ts` then drives the producer (which saves a report
artifact), captures the URI from the bus, hands it to the reader, and asserts the
reader reads back exactly what the producer wrote — with only the reference ever
on the bus.

Input rides the **control** channel (fire-and-forget); the agent's responses come
back on the **observability** stream the UI renders. The agent itself decides
prompt-vs-steer from its own state — so Stop is honored even while a dense token
stream is in flight. The Phase 1 `POST /api/runs` kickoff is kept for
non-interactive / programmatic use.

**Persistence (Phase 3).** The recorder is a bus peer, not part of the UI path: it
subscribes to presence, a new `sessions:events` announcement (the orchestrator
emits one per new session), and each session's observability stream. From those it
writes `agents`, `sessions`, and a clean `messages` audit to Postgres, and on each
idle re-archives the full trace as JSONL to S3 with a `trace_uri` pointer. Writes
are idempotent (row id = the event's `messageId`), so a recorder restart re-reads a
stream without duplicating; it also re-tails still-open sessions on startup. The
orchestrator reads this spine for `GET /api/sessions` and
`GET /api/sessions/:id/transcript` and resolves session→agent from it, so chat
routing survives an orchestrator restart. *Not yet:* restoring the agent's
in-memory conversation context across an **agent** restart (history is reloadable
for viewing; resuming a dead agent's context needs pi `SessionStorage` — a Phase 3
follow-up noted in `plans/phase-3-task.md`).

**Model:** the agent picks its model from env, highest priority first:

1. **Custom OpenAI-compatible endpoint** — set `OPENAI_BASE_URL` (plus
   `OPENAI_MODEL` and `OPENAI_API_KEY`). Works with vLLM, llama.cpp, LM Studio, a
   gateway, etc. Reasoning models surface their thinking when the endpoint is
   told how to emit it — e.g. for qwen on llama.cpp set
   `OPENAI_THINKING_FORMAT=qwen-chat-template` and `OPENAI_THINKING_LEVEL=medium`:

       OPENAI_BASE_URL=http://<model-host>:10000/v1 OPENAI_API_KEY=not-used \
       OPENAI_MODEL=qwen3.6-think-code OPENAI_THINKING_FORMAT=qwen-chat-template \
       OPENAI_THINKING_LEVEL=medium pnpm agent

2. **Anthropic** `claude-haiku-4-5` — when `OPENAI_BASE_URL` is unset and
   `ANTHROPIC_API_KEY` is present.
3. **faux** — pi-ai's keyless provider: a scripted stream (thinking + text + a
   tool call) that exercises the full pipeline with zero tokens.

Force faux even when a key is present with `AGENT_FAUX=1`. See `.env.example` for
all the knobs.

## Deploy (remote server)

Production runs from two multi-arch images (`linux/amd64` + `linux/arm64`) under the
**paintezero** org — `paintezero/orchestra` (the Node apps; the compose picks the role
via `command`) and `paintezero/orchestra-web` (the SPA on nginx, which reverse-proxies
`/api` + WebSockets to the orchestrator and injects the gateway token server-side).

```bash
docker login && ./scripts/build-images.sh          # buildx amd64+arm64, push to paintezero
cp .env.prod.example .env && node scripts/gen-acl.mjs   # set secrets, sync the Redis ACL
docker compose -f docker-compose.prod.yml --env-file .env up -d
```

Only the web port (`WEB_PORT`, default 80) is published; Redis / Postgres / S3 stay on
the internal network. Full guide — including the model/agent options and TLS — in
[`docs/deploy.md`](docs/deploy.md).

## What the SDK gives you

| Need                                   | API                                            |
|----------------------------------------|------------------------------------------------|
| Agent comes online / heartbeat         | `bus.register(...)`, auto heartbeat            |
| Who's connected                        | `bus.listAgents()`, `bus.watchPresence(fn)`    |
| Directed call + correlated reply       | `bus.request(target, payload)` / `bus.onRequest(fn)` |
| Per-session trace (replayable)         | `bus.publishEvent(sid, ev)` / `bus.subscribeEvents(sid, fn)` |
| One-shot full trace read               | `bus.readEvents(sid)` (XRANGE)                  |
| Steer / cancel a running agent         | `bus.publishControl(id, msg)` / `bus.subscribeControl(fn)` |
| Announce / watch new sessions          | `bus.announceSession(ev)` / `bus.watchSessions(fn)` |
| Who's online + what they do (roster)   | `bus.listAgents()` → each `{ id, description }` |
| Delegate to another agent (by id)      | `ask_agent` tool → `bus.request<DelegateResult>(...)` |
| Store / read an artifact (refs on bus) | `save_artifact` / `read_artifact` tools → `ArtifactStore` |
| Durable spine + trace + artifacts      | `@paintenzero/orchestra-store` — `Store` (pg) / `TraceArchive` + `ArtifactStore` (S3) |

## Channel map (Redis)

    req:<agent>               inbound request stream (consumer group "workers")
    reply:<client>:<corrId>   per-request reply stream (TTL'd)
    obs:<session>             observability stream (XADD MAXLEN ~10k, replayable)
    control:<agent>           pub/sub control channel
    presence:agent:<id>       presence key, TTL 15s, heartbeat every 5s
    presence:events           pub/sub register/update/deregister
    sessions:events           pub/sub session-open announcements (recorder picks these up)
    cstream:<agent>           durable control stream (Phase 6: replayable, resumable)
    cpos:<agent>              last-processed control id (Phase 6: resume point)
    req:<agent>:dlq           dead-letter stream for poison requests (Phase 6)
    dedup:<client>:<key>      idempotency marker (Phase 6: SET NX, TTL'd)

## Decisions baked in

- **Redis Streams, not RabbitMQ.** Replayable observability + presence in one
  dependency. The bus interface is the only thing agents see, so swapping the
  transport later is contained.
- **Delivery is at-least-once.** Requests use a consumer group with explicit
  XACK; redelivery is possible, so keep `onRequest` handlers idempotent or
  dedupe on `meta.messageId`.
- **The browser never touches Redis.** It talks WebSocket to the orchestrator,
  which is the only bus<->UI bridge.
- **Initiator allocates the session id.** The orchestrator generates the
  `sessionId`, returns it from `POST /api/runs` before the run starts, and puts
  it in the request envelope; the agent reuses it for both its run and its
  `publishEvent` calls. The UI can subscribe to `obs:<sessionId>` immediately,
  with no race over which stream to watch.
- **IDs are uuidv7** — time-sortable, so stream order and id order agree.

## Phase 6 status

All ten hardening workstreams are shipped and verified: bus authn/authz + gateway
auth (1, 2), idempotency (3), delivery recovery + DLQ (4), durable control delivery
(5), reconnection/resilience (6), schema-version checks (7), Prometheus + health (8),
budgets + rate limits (9), and the transport ADR (10). With this phase the roadmap
from the original design is complete.

**Deferred (noted, not blocking):** centralizing LLM provider keys behind pi's
`streamProxy` with short-lived per-agent tokens (a separate service — see
`docs/security.md`); and the carried-over enhancements — artifact access control
(signed/scoped URLs), capability-queue dispatch (`req:cap:<capability>`), and pi
`SessionStorage` so a restarted agent resumes its in-memory conversation context.
