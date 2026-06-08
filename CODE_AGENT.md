# Build your own agent from scratch

This is an exact, copy‑paste guide to writing a brand‑new agent that connects to a
running orchestra backend, appears in the roster, is callable by humans (the UI) and
by other agents (`ask_agent`), and runs an LLM with your own tools.

## Mental model (read this first)

An agent is just a **peer on the bus (Redis)** — it does **not** talk to the
orchestrator. To "connect an agent" you start a process that:

1. opens a `Bus` to Redis with valid ACL credentials,
2. `register()`s its presence (id + a natural‑language **description**),
3. answers **requests** (`onRequest` — this is where the orchestrator's `/api/runs`
   kickoff and other agents' `ask_agent` land) and **control** messages
   (`subscribeControl` — human chat / steer / cancel from the UI),
4. runs a `pi` `Agent` (the LLM loop) and **forwards its events** to the session's
   observability stream so the UI/trace shows the run.

It needs three things at runtime: **reachable Redis + its ACL creds**, an **LLM**
(an OpenAI‑compatible endpoint, Anthropic, etc.), and your **tools**.

> The minimal agent below uses **one active session at a time** — the smallest thing
> that works and is callable. The full production reference (`apps/agent/src/index.ts`)
> adds per‑task sessions, the `ask_agent`/`continue_task`/`close_task` delegation
> tools, artifacts, time budgets, idempotency and durable control. Start here, graduate
> to that.

## Prerequisites

- The orchestra repo (this monorepo) on your machine — your agent lives inside the
  pnpm workspace so it can import `@orchestra/*` (those packages are not on npm).
- Node ≥ 22.19 and pnpm (`corepack enable && corepack prepare pnpm@9.12.0 --activate`).
- Network reachability + credentials for the backend you're connecting to. For the
  deployed VPS backend that means being on the **Tailscale** tailnet and using
  `redis://<vps-tailscale-ip>:6380` (the deploy uses port **6380**).

---

## Step 1 — Scaffold a new app in the workspace

```bash
mkdir -p apps/my-agent/src
```

`apps/my-agent/package.json`:

```json
{
  "name": "my-agent",
  "version": "0.0.0",
  "private": true,
  "type": "module",
  "scripts": {
    "start": "tsx src/index.ts",
    "build": "tsc -p tsconfig.json"
  },
  "dependencies": {
    "@paintenzero/orchestra-bus": "workspace:^",
    "@paintenzero/orchestra-protocol": "workspace:^",
    "@earendil-works/pi-agent-core": "^0.78.1",
    "@earendil-works/pi-ai": "^0.78.1"
  },
  "devDependencies": {
    "tsx": "^4.19.0",
    "typescript": "^5.6.0"
  }
}
```

`apps/my-agent/tsconfig.json`:

```json
{
  "extends": "../../tsconfig.base.json",
  "compilerOptions": { "noEmit": true },
  "include": ["src"]
}
```

(`pnpm-workspace.yaml` already globs `apps/*`, so the new app is picked up
automatically.)

---

## Step 2 — The agent source

`apps/my-agent/src/index.ts` — a complete, working agent. Read the comments; the only
part you change for your use case is `buildModel()` and the **tools**.

```ts
import { Bus } from "@paintenzero/orchestra-bus";
import {
  uuidv7,
  isDelegateRequest,
  type Envelope,
  type ControlMessage,
  type ObservabilityEvent,
  type RunRequest,
  type RunResult,
  type DelegateRequest,
  type DelegateResult,
} from "@paintenzero/orchestra-protocol";
import {
  Agent,
  type AgentEvent,
  type AgentMessage,
  type AgentTool,
} from "@earendil-works/pi-agent-core";
import { streamSimple, Type, type Model, type TextContent } from "@earendil-works/pi-ai";

// ---- identity (everything comes from env so the same binary is reusable) --------
const AGENT_ID = process.env.AGENT_ID ?? "my-agent";
const AGENT_DESCRIPTION =
  process.env.AGENT_DESCRIPTION ?? "Describe in plain language what this agent does.";
const SYSTEM_PROMPT =
  process.env.AGENT_SYSTEM_PROMPT ??
  "You are a helpful agent. Use your tools when they are relevant, then answer.";
const OPENAI_PROVIDER = "openai-compat";

// ---- the LLM: an OpenAI-compatible endpoint (vLLM / llama.cpp / LM Studio / …) ---
// Set OPENAI_BASE_URL + OPENAI_MODEL (+ OPENAI_API_KEY). For Anthropic instead, use
// getModel("anthropic","claude-haiku-4-5") from @earendil-works/pi-ai.
function buildModel(): Model<any> {
  const baseUrl = process.env.OPENAI_BASE_URL;
  if (!baseUrl) throw new Error("set OPENAI_BASE_URL + OPENAI_MODEL");
  const id = process.env.OPENAI_MODEL ?? "gpt-4o-mini";
  return {
    id,
    name: id,
    api: "openai-completions",
    provider: OPENAI_PROVIDER,
    baseUrl,
    reasoning: process.env.OPENAI_REASONING === "1", // instruct models: leave unset (false)
    input: ["text"],
    cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
    contextWindow: Number(process.env.OPENAI_CONTEXT_WINDOW ?? 131072),
    maxTokens: Number(process.env.OPENAI_MAX_TOKENS ?? 4096),
    compat: { maxTokensField: "max_tokens" },
  };
}

// ---- your tool(s): this is where the agent does real work -----------------------
// A tool is { name, description, parameters (TypeBox), execute }. Return content +
// details; THROW on failure (the runtime surfaces it as a tool error in the trace).
const echoParams = Type.Object({ text: Type.String({ description: "text to echo back" }) });
const echoTool: AgentTool<typeof echoParams> = {
  name: "echo",
  label: "Echo",
  description: "Echo text back. Replace this with your real capability.",
  parameters: echoParams,
  execute: async (_toolCallId, { text }) => ({
    content: [{ type: "text", text: `echo: ${text}` }],
    details: { text },
  }),
};

async function main() {
  // Bus reads REDIS_URL / REDIS_USERNAME / REDIS_PASSWORD (and REDIS_TLS) from env.
  const bus = new Bus({ clientId: AGENT_ID });

  const agent = new Agent({
    initialState: {
      systemPrompt: SYSTEM_PROMPT,
      model: buildModel(),
      thinkingLevel: "off",
      tools: [echoTool], // ← add your tools here
    },
    streamFn: streamSimple,
    getApiKey: (provider) =>
      provider === OPENAI_PROVIDER
        ? process.env.OPENAI_API_KEY ?? "not-used"
        : process.env[`${provider.toUpperCase()}_API_KEY`],
  });
  agent.steeringMode = "all"; // drain queued steer messages at the next turn boundary

  // One active session at a time (the simplest correct model).
  let currentSessionId: string | undefined;
  let running = false;

  // Forward EVERY model event to the running session's observability stream — this is
  // what makes the run show up live in the UI / trace. (pi AgentEvent's shape matches
  // ObservabilityEvent for these members, so it forwards with a cast.)
  agent.subscribe((ev: AgentEvent) => {
    if (currentSessionId) void bus.publishEvent(currentSessionId, ev as ObservabilityEvent);
  });

  const userMessage = (text: string): AgentMessage =>
    ({ role: "user", content: text, timestamp: Date.now() }) as AgentMessage;

  const lastAssistantText = (msgs: AgentMessage[]): string => {
    for (let i = msgs.length - 1; i >= 0; i--) {
      const m = msgs[i];
      if (m.role === "assistant")
        return m.content
          .filter((c): c is TextContent => c.type === "text")
          .map((c) => c.text)
          .join("");
    }
    return "";
  };

  // Drive one turn: mark busy, run the model, mark idle; return the clean text.
  async function runTurn(sessionId: string, prompt: string): Promise<RunResult> {
    running = true;
    await bus.updateStatus("busy");
    await bus.publishEvent(sessionId, { type: "status", status: "busy" });
    try {
      await agent.prompt(prompt);
      return { text: lastAssistantText(agent.state.messages) };
    } catch (err) {
      await bus.publishEvent(sessionId, { type: "status", status: "error", detail: String(err) });
      throw err;
    } finally {
      running = false;
      await bus.updateStatus("idle");
      await bus.publishEvent(sessionId, { type: "status", status: "idle" });
    }
  }

  // (1) Requests: the orchestrator's POST /api/runs kickoff AND other agents'
  // ask_agent both arrive here. isDelegateRequest splits a peer delegation from a
  // plain run.
  bus.onRequest(async (payload, meta): Promise<RunResult | DelegateResult> => {
    if (isDelegateRequest(payload)) {
      const req = payload;
      // Run the delegated task in its own session, linked to the caller's so the
      // recorder records the parent→child edge and the UI shows the call tree.
      const sid = req.taskId ?? uuidv7();
      await bus.announceSession({
        event: "open",
        sessionId: sid,
        agentId: AGENT_ID,
        parentSessionId: req.parentSessionId,
        source: meta.source,
        ts: Date.now(),
      });
      if (sid !== currentSessionId) {
        agent.reset();
        currentSessionId = sid;
      }
      await bus.publishEvent(sid, { type: "user_message", text: req.prompt, from: meta.source });
      const r = await runTurn(sid, req.prompt);
      return { text: r.text, stopReason: r.stopReason, taskId: sid } satisfies DelegateResult;
    }

    const { prompt } = payload as RunRequest;
    const sid = meta.sessionId ?? uuidv7();
    if (sid !== currentSessionId) {
      agent.reset();
      currentSessionId = sid;
    }
    await bus.publishEvent(sid, { type: "user_message", text: prompt, from: meta.source });
    return runTurn(sid, prompt); // throws ⇒ the bus turns it into an { ok:false } reply
  });

  // (2) Control plane: human chat / steer / cancel from the UI.
  bus.subscribeControl((msg: ControlMessage, env: Envelope) => {
    void (async () => {
      if (msg.type === "cancel") {
        agent.abort();
        return;
      }
      if (msg.type !== "user_message") return;
      const sid = env.sessionId;
      if (!sid) return;
      await bus.publishEvent(sid, { type: "user_message", text: msg.text, from: env.source });
      // busy ⇒ steer the running turn; idle ⇒ start/continue a conversation.
      if (agent.state.isStreaming || running) {
        agent.steer(userMessage(msg.text));
        return;
      }
      if (sid !== currentSessionId) {
        agent.reset();
        currentSessionId = sid;
      }
      await runTurn(sid, msg.text).catch(() => {}); // errors already surfaced as a status event
    })();
  });

  // (3) Go online — advertise by DESCRIPTION (how other agents discover you).
  await bus.register({
    id: AGENT_ID,
    description: AGENT_DESCRIPTION,
    capabilities: [],
    status: "idle",
    metadata: { model: process.env.OPENAI_MODEL },
  });
  console.log(`[${AGENT_ID}] online → ${AGENT_DESCRIPTION}`);

  const shutdown = async () => {
    await bus.deregister();
    await bus.close();
    process.exit(0);
  };
  process.on("SIGINT", shutdown);
  process.on("SIGTERM", shutdown);
}

main().catch((err) => {
  console.error("fatal:", err);
  process.exit(1);
});
```

---

## Step 3 — Install & build the libraries

From the repo root (the apps run from source via `tsx`; only the `@orchestra/*`
libraries need compiling):

```bash
pnpm install
pnpm --filter "./packages/*" build
```

---

## Step 4 — Run it, connected to the orchestra

Point it at the backend over Tailscale and at your LLM. Values below match the
deployed VPS (`redis` on **6380**); change them for your backend.

```bash
AGENT_ID=my-agent \
AGENT_DESCRIPTION="What this agent does, in one sentence." \
OPENAI_BASE_URL=http://<model-host>:10000/v1 \
OPENAI_MODEL=gemma4-instruct-general \
OPENAI_API_KEY=not-used \
REDIS_URL=redis://<vps-tailscale-ip>:6380 \
REDIS_USERNAME=dev REDIS_PASSWORD=orchestra-dev \
pnpm --filter my-agent start
```

Notes:
- `REDIS_USERNAME/PASSWORD`: use the broad **`dev`** user for a quick start. For
  least privilege, add a dedicated ACL user (see below) and use it.
- This minimal agent does **not** use artifacts, so it needs **no** S3 config (the
  full `apps/agent` does — it ensures its S3 bucket at startup).
- Anthropic instead of an endpoint: drop the `OPENAI_*` vars, set
  `ANTHROPIC_API_KEY`, and change `buildModel()` to
  `return getModel("anthropic", "claude-haiku-4-5");` (`import { getModel } from "@earendil-works/pi-ai"`).

---

## Step 5 — Verify it connected

```bash
# from any tailnet device (or the UI at http://<vps>:8088)
curl -s http://<vps-tailscale-ip>:8088/api/agents      # your agent appears, with its description
```
You should also see `[my-agent] online → …` in its log, the agent in the UI roster,
and — within ≤60 s — its description in other agents' system prompts (so they can
`ask_agent` it). Send it a message from the UI to watch a turn stream.

---

## Adding your own tools

Copy the `echoTool` shape. The contract: TypeBox `parameters`, an `execute` that
returns `{ content: [...], details }`, and **throws** on failure.

```ts
import { readdir } from "node:fs/promises";

const listParams = Type.Object({
  path: Type.Optional(Type.String({ description: "directory to list (default: cwd)" })),
});
const listFiles: AgentTool<typeof listParams> = {
  name: "list_files",
  description: "List entries of a local directory.",
  parameters: listParams,
  execute: async (_id, { path }) => {
    const dir = path?.trim() ? path : process.cwd();
    const names = (await readdir(dir, { withFileTypes: true })).map(
      (e) => `${e.isDirectory() ? "[dir] " : ""}${e.name}`,
    );
    return { content: [{ type: "text", text: `${dir}:\n${names.join("\n")}` }], details: { dir } };
  },
};
```

Then add it to the `tools: [echoTool, listFiles]` array. `execute`'s 3rd argument is
an `AbortSignal` — honor it in long‑running tools (e.g. pass it to `fetch`/subprocess)
so cancel and time budgets actually stop the work.

---

## The bus contract you used (reference)

| Call | Purpose |
|---|---|
| `new Bus({ clientId })` | open the connection (env: `REDIS_URL`, `REDIS_USERNAME`, `REDIS_PASSWORD`, `REDIS_TLS`) |
| `bus.register({ id, description, capabilities, status })` | go online + advertise; starts the presence heartbeat |
| `bus.onRequest(handler)` | serve directed requests (`/api/runs` kickoff + peers' `ask_agent`); reply = handler's return |
| `bus.subscribeControl((msg, env) => …)` | human chat / steer / cancel |
| `bus.publishEvent(sessionId, event)` | stream the run to the UI/recorder |
| `bus.updateStatus("busy"\|"idle")` | presence status (green/idle in the roster) |
| `bus.announceSession({...})` | tell the recorder a new session opened (parent link for the call tree) |
| `bus.listAgents()` / `bus.findAgents(cap)` | discover peers |
| `bus.request(target, payload, { sessionId, timeoutMs })` | call another agent (what `ask_agent` is built on) |
| `bus.deregister()` / `bus.close()` | clean shutdown |

**Observability events the UI renders** (what you emit via `publishEvent`): the `pi`
events forwarded from `agent.subscribe` (`message_*`, `tool_execution_*`, `agent_*`)
plus three orchestra additions you emit yourself — `user_message` (the incoming turn),
`status` (busy/idle/error), and `artifact`. Emit the `user_message` **before** running
so the transcript replays in order (the code above does this).

---

## ACL: give your agent its own user (optional but recommended)

Each agent authenticates as a Redis ACL user. The `dev` user works for any id but is
broad. For least privilege, add a scoped user on the backend host:

```bash
# in the repo on the VPS: add "my-agent" to the agent list in scripts/gen-acl.mjs
#   and a MY_AGENT_PASSWORD in .env, then:
node scripts/gen-acl.mjs
docker compose -f docker-compose.backend.yml exec redis redis-cli -a <admin-pass> ACL LOAD
```
Then run your agent with `REDIS_USERNAME=my-agent REDIS_PASSWORD=<its-pass>`.

---

## Run it as a service (production)

`/etc/orchestra-my-agent.env` holds the env vars (one `KEY=value` per line), then:

```ini
# /etc/systemd/system/orchestra-my-agent.service
[Unit]
Description=orchestra my-agent
After=network-online.target tailscaled.service
Wants=network-online.target
[Service]
WorkingDirectory=/path/to/teamwork
EnvironmentFile=/etc/orchestra-my-agent.env
ExecStart=/usr/bin/pnpm --filter my-agent start   # use `which pnpm`
Restart=always
RestartSec=5
[Install]
WantedBy=multi-user.target
```
```bash
systemctl daemon-reload && systemctl enable --now orchestra-my-agent
```
The bus auto‑reconnects and re‑registers across blips, so the agent survives Redis/
network hiccups.

---

## Going further

When you want more than the minimal agent, read **`apps/agent/src/index.ts`** — the
production reference adds, on top of everything here:

- **per‑task sessions** (one conversation per task id, restored from a Redis snapshot
  across restarts) instead of one active session;
- the **`ask_agent` / `continue_task` / `close_task`** tools (multi‑turn collaboration
  with other agents by description/id);
- **`save_artifact` / `read_artifact`** (large outputs to S3, only references on the
  bus);
- **time budgets**, **idempotency** (dedupe on `messageId`), and **durable control**.

Copy whichever of those you need into your agent; the bus contract is identical.
