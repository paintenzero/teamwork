/**
 * Phase 1 example agent. A long-lived process that registers on the bus, runs a
 * real pi-agent-core loop, forwards every run event to the session's
 * observability stream, and returns the run's clean result as the correlated
 * reply.
 *
 * Model: uses Anthropic `claude-haiku-4-5` when ANTHROPIC_API_KEY is set;
 * otherwise falls back to pi-ai's `faux` provider — a scripted, keyless stream
 * that still emits thinking, streaming text, and a tool call so the whole
 * pipeline can be exercised end-to-end without burning tokens. Force faux with
 * AGENT_FAUX=1.
 *
 * Phase 2 adds two-way chat over the control channel: a `user_message` control
 * signal is routed by the agent itself — prompt when idle, steer when busy — and
 * every user turn is echoed to the observability stream so the transcript
 * replays. `cancel` aborts the run.
 */
import type { Readable } from "node:stream";
import { readdir, stat } from "node:fs/promises";
import { join } from "node:path";
import { Bus } from "@paintenzero/orchestra-bus";
import { ArtifactStore } from "@paintenzero/orchestra-store";
import {
  uuidv7,
  isDelegateRequest,
  type AgentRegistration,
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
  type ThinkingLevel,
} from "@earendil-works/pi-agent-core";
import {
  getModel,
  streamSimple,
  Type,
  registerFauxProvider,
  fauxAssistantMessage,
  fauxText,
  fauxThinking,
  fauxToolCall,
  type Model,
  type TextContent,
} from "@earendil-works/pi-ai";

const AGENT_ID = process.env.AGENT_ID ?? "agent-researcher";
const CAPABILITIES = (process.env.AGENT_CAPABILITIES ?? "chat,research,clock")
  .split(",")
  .map((c) => c.trim())
  .filter(Boolean);
/**
 * Natural-language advertisement (not tags). Every agent broadcasts this on
 * presence, and every other agent injects the roster of these descriptions into its
 * system prompt — so the model chooses who to delegate to by reading what each peer
 * does, the same way it picks a tool by description.
 */
const AGENT_DESCRIPTION =
  process.env.AGENT_DESCRIPTION ??
  "A general research assistant: answers questions, reasons step by step, and can check the time.";
const SYSTEM_PROMPT =
  process.env.AGENT_SYSTEM_PROMPT ??
  "You are a helpful research agent. Think step by step and use the clock tool when the " +
    "time is relevant. When another agent on the team is better suited to a subtask, call " +
    "it with ask_agent and use its result.";
/** How often to refresh the roster of peer agents injected into the prompt. */
const ROSTER_REFRESH_MS = Number(process.env.ROSTER_REFRESH_MS ?? 60_000);

/** Delegation guards (Phase 4). Overridable per-call by a DelegateRequest budget. */
const MAX_DELEGATION_DEPTH = Number(process.env.MAX_DELEGATION_DEPTH ?? 4);
const DEFAULT_DELEGATE_TIMEOUT_MS = Number(process.env.DELEGATE_TIMEOUT_MS ?? 60_000);
/** Wall-clock cap on a root run (Phase 6, WS9). 0 = no cap; delegated runs inherit
 *  the caller's deadline instead, so a whole tree shares one budget. */
const MAX_RUN_MS = Number(process.env.MAX_RUN_MS ?? 0);
/** Per-task sessions: drop a task's in-memory context after this idle (its working
 *  memory is snapshotted to Redis first, so the next turn restores it), and cap how
 *  many task contexts are held at once. */
const TASK_TTL_MS = Number(process.env.TASK_TTL_MS ?? 1_800_000);
const MAX_TASKS = Number(process.env.MAX_TASKS ?? 50);

/** `provider` tag for the env-configured OpenAI-compatible endpoint. */
const OPENAI_COMPAT_PROVIDER = "openai-compat";

/** A trivial tool, just to exercise the tool-call path in the trace. */
const clockTool: AgentTool = {
  name: "clock",
  label: "Clock",
  description: "Returns the current date and time in ISO-8601 format.",
  parameters: Type.Object({}),
  execute: async () => {
    const now = new Date().toISOString();
    return { content: [{ type: "text", text: now }], details: { now } };
  },
};

/** List a directory on the agent's local filesystem (where this process runs). */
const listFilesParams = Type.Object({
  path: Type.Optional(
    Type.String({ description: "directory path (absolute or relative); defaults to the working directory" }),
  ),
});
const listFilesTool: AgentTool<typeof listFilesParams> = {
  name: "list_files",
  label: "List files",
  description:
    "List the entries (files and subdirectories) of a directory on this agent's local filesystem.",
  parameters: listFilesParams,
  execute: async (_id, { path }) => {
    const dir = path && path.trim() ? path : process.cwd();
    const entries = await readdir(dir, { withFileTypes: true });
    const rows = await Promise.all(
      entries.slice(0, 500).map(async (e) => {
        if (e.isDirectory()) return `[dir]  ${e.name}`;
        let size = "";
        try {
          size = ` (${(await stat(join(dir, e.name))).size} B)`;
        } catch {
          /* unreadable entry — list the name without a size */
        }
        return `       ${e.name}${size}`;
      }),
    );
    const more = entries.length > 500 ? `\n… and ${entries.length - 500} more` : "";
    return {
      content: [{ type: "text", text: `${dir} (${entries.length} entries):\n${rows.join("\n")}${more}` }],
      details: { dir, count: entries.length },
    };
  },
};

/**
 * Build a Model for any OpenAI-compatible endpoint (vLLM, llama.cpp, LM Studio,
 * a gateway, …). It's just a `Model<"openai-completions">` literal pointed at
 * `baseUrl`; the api is registered by pi-ai's builtins, so `streamSimple` routes
 * to it. The key is supplied separately via the agent's `getApiKey` hook.
 *
 * Env:
 *   OPENAI_BASE_URL          e.g. http://<model-host>:10000/v1   (enables this mode)
 *   OPENAI_MODEL             model id, e.g. qwen3.6-instruct-reasoning
 *   OPENAI_API_KEY           bearer token (default "not-used")
 *   OPENAI_REASONING         "1" (default) to mark the model as a reasoning model
 *   OPENAI_THINKING_FORMAT   how the endpoint wants thinking requested. For qwen on
 *                            llama.cpp use "qwen-chat-template" (chat_template_kwargs.
 *                            enable_thinking). Default: pi-ai auto-detect ("openai").
 *   OPENAI_THINKING_LEVEL    off|minimal|low|medium|high|xhigh (default "medium" when
 *                            reasoning) — must be non-off for the endpoint to think.
 */
function buildOpenAICompatModel(baseUrl: string): Model<"openai-completions"> {
  const id = process.env.OPENAI_MODEL ?? "gpt-4o-mini";
  const thinkingFormat = process.env.OPENAI_THINKING_FORMAT as
    | "openai" | "openrouter" | "deepseek" | "together" | "zai" | "qwen"
    | "qwen-chat-template" | "string-thinking" | "ant-ling" | undefined;
  return {
    id,
    name: id,
    api: "openai-completions",
    provider: OPENAI_COMPAT_PROVIDER,
    baseUrl,
    reasoning: process.env.OPENAI_REASONING !== "0",
    input: ["text"],
    cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
    contextWindow: Number(process.env.OPENAI_CONTEXT_WINDOW ?? 131072),
    maxTokens: Number(process.env.OPENAI_MAX_TOKENS ?? 8192),
    compat: { maxTokensField: "max_tokens", ...(thinkingFormat ? { thinkingFormat } : {}) },
  };
}

/** Scripted faux run for a given role (see `arm` below). `prompt` is the current
 *  turn's prompt, so a role can react to it (the `reader` reads the URI it's given,
 *  exactly as a real model would parse it out of the prompt). */
function fauxResponsesFor(role: string, delegateCap: string, prompt: string) {
  if (role === "producer") {
    return [
      fauxAssistantMessage(
        [
          fauxThinking("I'll write a short report and store it as an artifact."),
          fauxText("Saving the report as an artifact."),
          fauxToolCall("save_artifact", {
            content: "Tide pools shelter anemones and crabs; they refill at high tide.",
            name: "report.md",
            contentType: "text/markdown",
          }),
        ],
        { stopReason: "toolUse" },
      ),
      fauxAssistantMessage(fauxText("Stored the report as an artifact. Done!"), {
        stopReason: "stop",
      }),
    ];
  }
  if (role === "reader") {
    return [
      fauxAssistantMessage(
        [
          fauxThinking("I was handed an artifact URI; I'll read it back."),
          fauxToolCall("read_artifact", { uri: prompt.trim() }),
        ],
        { stopReason: "toolUse" },
      ),
      fauxAssistantMessage(fauxText("Read the handed-off artifact. Done!"), { stopReason: "stop" }),
    ];
  }
  if (role === "delegator") {
    return [
      fauxAssistantMessage(
        [
          fauxThinking(`The agent "${delegateCap}" looks right for the drafting; I'll hand it off with ask_agent.`),
          fauxText("Let me delegate the drafting to a teammate."),
          fauxToolCall("ask_agent", {
            agent: delegateCap,
            prompt: "Write a two-sentence report on tide pools.",
          }),
        ],
        { stopReason: "toolUse" },
      ),
      fauxAssistantMessage(
        fauxText("Combined the teammate's draft into the final report. Done!"),
        { stopReason: "stop" },
      ),
    ];
  }
  if (role === "worker") {
    return [
      fauxAssistantMessage(
        [
          fauxThinking("I'll write a concise draft."),
          fauxText("Draft: Tide pools shelter anemones and crabs; they refill at high tide."),
        ],
        { stopReason: "stop" },
      ),
    ];
  }
  // "clock" (default): exercises the tool-call path, as in the Phase 2/3 demos.
  return [
    fauxAssistantMessage(
      [
        fauxThinking("The user asked me something. Let me check the current time with the clock tool first."),
        fauxText("Sure — let me check the current time."),
        fauxToolCall("clock", {}),
      ],
      { stopReason: "toolUse" },
    ),
    fauxAssistantMessage(fauxText("I checked the clock tool and have the current time. All done!"), {
      stopReason: "stop",
    }),
  ];
}

/**
 * Pick the model by env, highest priority first:
 *   1. OPENAI_BASE_URL  → custom OpenAI-compatible endpoint
 *   2. ANTHROPIC_API_KEY → Anthropic claude-haiku-4-5
 *   3. (none)           → keyless faux provider
 * Returns an `arm` callback for faux mode to re-script each run.
 */
function buildModel(): {
  model: Model<any>;
  label: string;
  thinkingLevel: ThinkingLevel;
  faux: boolean;
  arm?: (prompt?: string) => void;
} {
  if (process.env.OPENAI_BASE_URL && process.env.AGENT_FAUX !== "1") {
    const model = buildOpenAICompatModel(process.env.OPENAI_BASE_URL);
    const thinkingLevel = model.reasoning
      ? ((process.env.OPENAI_THINKING_LEVEL as ThinkingLevel) ?? "medium")
      : "off";
    return { model, label: `${model.id} @ ${model.baseUrl}`, thinkingLevel, faux: false };
  }
  if (process.env.ANTHROPIC_API_KEY && process.env.AGENT_FAUX !== "1") {
    return {
      model: getModel("anthropic", "claude-haiku-4-5"),
      label: "claude-haiku-4-5",
      thinkingLevel: "off",
      faux: false,
    };
  }
  const reg = registerFauxProvider({
    models: [{ id: "faux-haiku", reasoning: true }],
    tokensPerSecond: 80,
  });
  // Faux role picks the scripted run so the whole stack (incl. Phase 4 delegation
  // and Phase 5 artifacts) is exercisable token-free: "delegator" calls ask_agent,
  // "worker" drafts text, "producer" calls save_artifact, "reader" calls
  // read_artifact on the URI it's handed, "clock" (default) calls the clock tool.
  const role =
    process.env.AGENT_FAUX_ROLE ?? (process.env.AGENT_DELEGATE_TO ? "delegator" : "clock");
  // The faux delegator targets this agent id (the model would pick it from the roster).
  const delegateTo = process.env.AGENT_DELEGATE_TO ?? "agent-writer";
  const arm = (prompt = "") => reg.setResponses(fauxResponsesFor(role, delegateTo, prompt));
  arm();
  return { model: reg.getModel(), label: `faux(${role})`, thinkingLevel: "off", faux: true, arm };
}

/** Cap on how many artifact bytes read_artifact inlines into the LLM context —
 *  anything larger (or binary) comes back as a reference only, so a big artifact
 *  never bloats the context window (Phase 5 acceptance criterion). */
const MAX_INLINE_BYTES = 64 * 1024;

/** Drain a readable stream to a string, stopping once `max` bytes are collected. */
async function streamToString(stream: Readable, max: number): Promise<string> {
  const chunks: Buffer[] = [];
  let total = 0;
  for await (const chunk of stream) {
    const buf = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk as Uint8Array);
    chunks.push(buf);
    total += buf.length;
    if (total >= max) {
      stream.destroy();
      break;
    }
  }
  return Buffer.concat(chunks).subarray(0, max).toString("utf8");
}

/**
 * Build the per-run system prompt: the base persona plus a live roster of the other
 * agents on the bus, advertised by their descriptions. The model reads this to decide
 * whether (and whom) to delegate to via `ask_agent`. Rebuilt on every run so it always
 * reflects the latest roster; empty roster ⇒ no delegation section.
 */
function composeSystemPrompt(base: string, peers: AgentRegistration[]): string {
  if (peers.length === 0) return base;
  const lines = peers
    .map((p) => `- ${p.id}${p.status === "busy" ? " (busy)" : ""}: ${p.description ?? "(no description)"}`)
    .join("\n");
  return (
    `${base}\n\n` +
    `## Other agents you can delegate to\n` +
    `Call \`ask_agent\` with the agent's id and a prompt to hand off a subtask and wait for its reply. ` +
    `Pick the agent whose description best fits; if none fit, do it yourself.\n${lines}`
  );
}

/** The run's clean result: the last assistant message's concatenated text. */
function finalResult(messages: AgentMessage[]): RunResult {
  for (let i = messages.length - 1; i >= 0; i--) {
    const m = messages[i];
    if (m.role === "assistant") {
      const text = m.content
        .filter((c): c is TextContent => c.type === "text")
        .map((c) => c.text)
        .join("");
      return { text, stopReason: m.stopReason };
    }
  }
  return { text: "", stopReason: "stop" };
}

async function main() {
  const bus = new Bus({ clientId: AGENT_ID });
  const artifacts = new ArtifactStore(); // Phase 5: bytes to S3, only refs on the bus
  await artifacts.ensureBucket();
  const { model, label, thinkingLevel, arm } = buildModel();

  // ---- per-task sessions ------------------------------------------------------
  // Each task this agent works on gets its OWN pi Agent (its own conversation),
  // keyed by taskId (= the session id). Multiple request/responses about one task
  // share that context; a different taskId is a different task with separate context.
  // Working memory is snapshotted to Redis after each turn, so a task survives idle
  // eviction and process restarts. Runs are single-flight (serialized) so the faux
  // provider and the `activeTaskId` pointer the tools read stay unambiguous.
  interface TaskCtx {
    agent: Agent;
    busy: boolean;
    lastUsed: number;
    // delegation context for THIS task (set when the task's turn starts):
    delegationPath: string[]; // agent ids from the root delegator down to here
    deadlineTs?: number; // budget deadline, propagated downward
    maxDepth: number;
    causationId?: string; // messageId that triggered the current turn (audit chain)
  }
  const tasks = new Map<string, TaskCtx>();
  let activeTaskId: string | undefined; // the task whose turn is currently running
  let runChain: Promise<unknown> = Promise.resolve(); // serializes turns across tasks

  /** The task whose turn is currently running — what the delegation tools act within. */
  function active(): TaskCtx {
    const t = activeTaskId ? tasks.get(activeTaskId) : undefined;
    if (!t) throw new Error("no active task to act within");
    return t;
  }

  // A-side bookkeeping: a task we opened with a downstream agent → that agent's id,
  // so continue_task / close_task only need the task id.
  const openTasks = new Map<string, string>();

  // Live roster of OTHER agents (advertised by description), refreshed on a timer and
  // injected into the system prompt each run. The model picks who to delegate to by
  // reading the descriptions — no capability tags involved.
  let peers: AgentRegistration[] = [];
  async function refreshRoster(): Promise<void> {
    try {
      peers = (await bus.listAgents()).filter((a) => a.id !== AGENT_ID);
    } catch {
      /* keep the last roster on a transient failure */
    }
  }

  /**
   * Delegate a subtask to another agent **by id** (the model chose it from the roster
   * of descriptions in its system prompt). Another agent is just a `pi` tool: this
   * issues a `bus.request` and returns the reply as the tool result. Guards **throw**
   * (surfaced in the trace as a `tool_execution_end` error): unknown/offline agent,
   * delegation cycle, max depth, or an exhausted time budget.
   */
  // ---- delegation tools (open / continue / close a task with another agent) ----
  // A task is a persistent collaboration: `ask_agent` opens one (mints a taskId,
  // runs the first round-trip, returns the id), `continue_task` does further
  // round-trips on the SAME task (the peer keeps that conversation's context), and
  // `close_task` finalizes it. Guards (cycle/depth/budget) are checked at open.
  function delegateBudgetRemaining(t: TaskCtx): number {
    return t.deadlineTs ? t.deadlineTs - Date.now() : DEFAULT_DELEGATE_TIMEOUT_MS;
  }

  const askAgentParams = Type.Object({
    agent: Type.String({ description: "the id of the agent to delegate to (see the roster in your prompt)" }),
    prompt: Type.String({ description: "the subtask to hand to that agent" }),
  });
  const askAgent: AgentTool<typeof askAgentParams> = {
    name: "ask_agent",
    label: "Ask agent",
    description:
      "Open a task with another agent (by id) and get its first response. Returns a task id; " +
      "use continue_task to keep working with it on the same task, or close_task when done. " +
      "Choose the agent from the roster of agent descriptions in your system prompt.",
    parameters: askAgentParams,
    execute: async (_id, { agent, prompt }, signal) => {
      const t = active();
      if (agent === AGENT_ID) throw new Error("cannot delegate to yourself");
      const online = await bus.listAgents();
      if (!online.some((a) => a.id === agent))
        throw new Error(`no agent "${agent}" is online (available: ${online.map((a) => a.id).join(", ") || "none"})`);

      const path = [...t.delegationPath, AGENT_ID];
      if (path.includes(agent))
        throw new Error(`delegation cycle: ${agent} already in [${path.join(" → ")}]`);
      if (path.length > t.maxDepth)
        throw new Error(`max delegation depth ${t.maxDepth} exceeded (path length ${path.length})`);
      const remaining = delegateBudgetRemaining(t);
      if (remaining <= 0) throw new Error("delegation budget exhausted (deadline passed)");
      if (signal?.aborted) throw new Error("aborted before delegating");

      const taskId = uuidv7();
      openTasks.set(taskId, agent);
      const res = await bus.request<DelegateResult>(
        agent,
        {
          prompt,
          taskId,
          parentSessionId: activeTaskId!,
          delegationPath: path,
          budget: { maxDepth: t.maxDepth, deadlineTs: t.deadlineTs },
        } satisfies DelegateRequest,
        { sessionId: activeTaskId, timeoutMs: Math.min(DEFAULT_DELEGATE_TIMEOUT_MS, remaining), causationId: t.causationId },
      );
      return {
        content: [{ type: "text", text: `[task ${taskId}] ${res.text}` }],
        details: { task: taskId, agent },
      };
    },
  };

  const continueTaskParams = Type.Object({
    task: Type.String({ description: "the task id returned by a previous ask_agent" }),
    prompt: Type.String({ description: "the next message in that ongoing task" }),
  });
  const continueTask: AgentTool<typeof continueTaskParams> = {
    name: "continue_task",
    label: "Continue task",
    description:
      "Send another message to an agent on an already-open task (from ask_agent) and wait for its " +
      "reply. The agent keeps the task's context across turns.",
    parameters: continueTaskParams,
    execute: async (_id, { task, prompt }, signal) => {
      const t = active();
      const agent = openTasks.get(task);
      if (!agent) throw new Error(`unknown task "${task}" — open one with ask_agent first`);
      const remaining = delegateBudgetRemaining(t);
      if (remaining <= 0) throw new Error("delegation budget exhausted (deadline passed)");
      if (signal?.aborted) throw new Error("aborted before continuing task");
      const res = await bus.request<DelegateResult>(
        agent,
        {
          prompt,
          taskId: task,
          parentSessionId: activeTaskId!,
          delegationPath: [...t.delegationPath, AGENT_ID],
          budget: { maxDepth: t.maxDepth, deadlineTs: t.deadlineTs },
        } satisfies DelegateRequest,
        { sessionId: activeTaskId, timeoutMs: Math.min(DEFAULT_DELEGATE_TIMEOUT_MS, remaining), causationId: t.causationId },
      );
      return { content: [{ type: "text", text: res.text }], details: { task, agent } };
    },
  };

  const closeTaskParams = Type.Object({
    task: Type.String({ description: "the task id to finalize" }),
  });
  const closeTask: AgentTool<typeof closeTaskParams> = {
    name: "close_task",
    label: "Close task",
    description: "Finalize an open task with another agent when the collaboration is done.",
    parameters: closeTaskParams,
    execute: async (_id, { task }) => {
      const agent = openTasks.get(task);
      if (!agent) return { content: [{ type: "text", text: `no open task ${task}` }], details: { task } };
      openTasks.delete(task);
      await bus
        .request(
          agent,
          { prompt: "", taskId: task, close: true, parentSessionId: activeTaskId ?? "", delegationPath: [] } satisfies DelegateRequest,
          { sessionId: activeTaskId, timeoutMs: 5_000 },
        )
        .catch(() => {});
      return { content: [{ type: "text", text: `closed task ${task}` }], details: { task } };
    },
  };

  /**
   * Store a text artifact in S3 and return only its reference (Phase 5). The bytes
   * go out-of-band; the bus sees just an `ArtifactRef`. We also emit an `artifact`
   * observability event so the UI/recorder learn of it without the bytes.
   */
  const saveArtifactParams = Type.Object({
    content: Type.String({ description: "the artifact's text content to store" }),
    name: Type.Optional(Type.String({ description: "file name, e.g. report.md" })),
    contentType: Type.Optional(Type.String({ description: "MIME type, e.g. text/markdown" })),
  });
  const saveArtifact: AgentTool<typeof saveArtifactParams> = {
    name: "save_artifact",
    label: "Save artifact",
    description:
      "Store a text artifact out-of-band and get back a reference (URI). Use for outputs too " +
      "large or file-like to keep in the conversation; the bytes never travel on the bus.",
    parameters: saveArtifactParams,
    execute: async (_id, { content, name, contentType }) => {
      if (!activeTaskId) throw new Error("no active task to save an artifact in");
      const ref = await artifacts.putArtifact(Buffer.from(content), {
        sessionId: activeTaskId,
        name,
        contentType,
      });
      await bus.publishEvent(activeTaskId, {
        type: "artifact",
        uri: ref.uri,
        contentType: ref.contentType,
        name: ref.name,
      });
      return {
        content: [{ type: "text", text: `Saved artifact${ref.name ? ` "${ref.name}"` : ""} → ${ref.uri}` }],
        details: ref,
      };
    },
  };

  /**
   * Read an artifact back by URI (Phase 5). Text comes back inline (capped at
   * MAX_INLINE_BYTES); binary or oversized artifacts return a reference only, so a
   * large artifact never bloats the LLM context. This is the receiving half of an
   * A→B handoff: A passes a URI, B reads it here — the bytes never touch the bus.
   */
  const readArtifactParams = Type.Object({
    uri: Type.String({ description: "the artifact URI (s3://…) to read" }),
  });
  const readArtifact: AgentTool<typeof readArtifactParams> = {
    name: "read_artifact",
    label: "Read artifact",
    description: "Fetch an artifact by URI. Returns text inline; binary returns a reference only.",
    parameters: readArtifactParams,
    execute: async (_id, { uri }) => {
      const { body, contentType, contentLength } = await artifacts.getArtifact(uri);
      const isText =
        !contentType || /^(text\/|application\/(json|xml|x-ndjson|yaml|javascript))/.test(contentType);
      if (!isText || (contentLength ?? 0) > MAX_INLINE_BYTES) {
        body.destroy();
        return {
          content: [
            {
              type: "text",
              text: `[binary artifact ${uri} (${contentType ?? "unknown type"}, ${contentLength ?? "?"} bytes) — not inlined]`,
            },
          ],
          details: { uri, contentType, contentLength },
        };
      }
      const text = await streamToString(body, MAX_INLINE_BYTES);
      return { content: [{ type: "text", text }], details: { uri, contentType } };
    },
  };

  const userMessage = (text: string): AgentMessage =>
    ({ role: "user", content: text, timestamp: Date.now() }) as AgentMessage;

  /** A fresh pi Agent for one task, forwarding its events to that task's obs stream. */
  function buildAgent(taskId: string): Agent {
    const a = new Agent({
      initialState: {
        systemPrompt: SYSTEM_PROMPT,
        model,
        thinkingLevel,
        tools: [clockTool, listFilesTool, askAgent, continueTask, closeTask, saveArtifact, readArtifact],
      },
      streamFn: streamSimple,
      getApiKey: (provider) =>
        provider === OPENAI_COMPAT_PROVIDER
          ? process.env.OPENAI_API_KEY ?? "not-used"
          : process.env[`${provider.toUpperCase()}_API_KEY`],
    });
    a.steeringMode = "all"; // drain every queued steer at the next turn boundary
    // Each task forwards its own events to its own session (fire-and-forget; ordering
    // holds because publishEvent shares one Redis connection).
    a.subscribe((event: AgentEvent) => void bus.publishEvent(taskId, event as ObservabilityEvent));
    return a;
  }

  /** Get the task's live context, creating it (and restoring its snapshot) on first use. */
  async function getTask(taskId: string): Promise<TaskCtx> {
    let t = tasks.get(taskId);
    if (!t) {
      const a = buildAgent(taskId);
      const restored = (await bus.loadTaskContext(taskId)) as AgentMessage[] | undefined;
      if (restored?.length) a.state.messages = restored; // resume working memory
      t = { agent: a, busy: false, lastUsed: Date.now(), delegationPath: [], maxDepth: MAX_DELEGATION_DEPTH };
      tasks.set(taskId, t);
    }
    t.lastUsed = Date.now();
    return t;
  }

  /** Drop idle task contexts (their working memory is already snapshotted to Redis). */
  function evictIdle(): void {
    const now = Date.now();
    for (const [id, t] of tasks)
      if (!t.busy && id !== activeTaskId && now - t.lastUsed > TASK_TTL_MS) tasks.delete(id);
    if (tasks.size > MAX_TASKS) {
      const idle = [...tasks.entries()]
        .filter(([id, t]) => !t.busy && id !== activeTaskId)
        .sort((a, b) => a[1].lastUsed - b[1].lastUsed);
      for (const [id] of idle.slice(0, tasks.size - MAX_TASKS)) tasks.delete(id);
    }
  }

  /** Run one turn for a task — serialized across all tasks (single-flight). */
  function runTurn(taskId: string, prompt: string): Promise<RunResult> {
    const next = runChain.then(() => runTurnInner(taskId, prompt));
    runChain = next.then(
      () => {},
      () => {},
    );
    return next;
  }
  async function runTurnInner(taskId: string, prompt: string): Promise<RunResult> {
    const t = await getTask(taskId);
    activeTaskId = taskId;
    t.busy = true;
    // Inject the live peer roster into THIS task's system prompt (read fresh by pi's
    // context snapshot at the start of the prompt).
    t.agent.state.systemPrompt = composeSystemPrompt(SYSTEM_PROMPT, peers);
    if (process.env.AGENT_LOG_PROMPT === "1")
      console.log(`[agent] system prompt (task ${taskId.slice(0, 8)}):\n${t.agent.state.systemPrompt}\n---`);
    arm?.(prompt); // re-script the faux provider for this run (no-op in real mode)
    const budgetTimer =
      t.deadlineTs !== undefined
        ? setTimeout(
            () => {
              if (!t.busy) return;
              void bus.publishEvent(taskId, {
                type: "status",
                status: "error",
                detail: "time budget exceeded — run aborted",
              });
              t.agent.abort();
            },
            Math.max(0, t.deadlineTs - Date.now()),
          )
        : undefined;
    await bus.updateStatus("busy");
    await bus.publishEvent(taskId, { type: "status", status: "busy" });
    try {
      await t.agent.prompt(prompt);
      return finalResult(t.agent.state.messages);
    } catch (err) {
      await bus.publishEvent(taskId, { type: "status", status: "error", detail: String(err) });
      throw err;
    } finally {
      if (budgetTimer) clearTimeout(budgetTimer);
      t.busy = false;
      t.lastUsed = Date.now();
      // Snapshot working memory so the task survives eviction / restart.
      await bus.saveTaskContext(taskId, t.agent.state.messages).catch(() => {});
      if (activeTaskId === taskId) activeTaskId = undefined;
      await bus.updateStatus("idle");
      await bus.publishEvent(taskId, { type: "status", status: "idle" });
      evictIdle();
    }
  }

  // ---- chat / steering over the control channel --------------------------------
  // The agent is the arbiter of prompt-vs-steer, deciding from its own state — the
  // race-free place to decide. Same session id ⇒ continue; new id while idle ⇒
  // fresh conversation (reset). The handler is async; the bus ignores the promise.
  async function handleControl(msg: ControlMessage, env: Envelope): Promise<void> {
    // WS3: idempotent control handling — a re-delivered control message (e.g. a
    // duplicate publish) is processed at most once.
    if (env.messageId && !(await bus.dedupe(`ctrl:${env.messageId}`))) return;
    switch (msg.type) {
      case "user_message": {
        const sid = env.sessionId; // the task/session this chat turn belongs to
        if (!sid) return;
        const emitUserTurn = () =>
          bus.publishEvent(sid, { type: "user_message", text: msg.text, from: env.source });

        // If this task's turn is mid-flight ⇒ steer it (injected at the next turn
        // boundary), rather than starting a second turn.
        const existing = tasks.get(sid);
        if (existing && existing.busy && existing.agent.state.isStreaming) {
          existing.agent.steer(userMessage(msg.text));
          await emitUserTurn();
          return;
        }
        // Otherwise run a turn in this task's context (created/restored on first use;
        // queued behind any in-flight turn). A human turn is the root of any
        // delegation it starts: empty path, MAX_RUN_MS budget.
        const t = await getTask(sid);
        t.delegationPath = [];
        t.deadlineTs = MAX_RUN_MS > 0 ? Date.now() + MAX_RUN_MS : undefined;
        t.maxDepth = MAX_DELEGATION_DEPTH;
        t.causationId = env.messageId;
        await emitUserTurn();
        await runTurn(sid, msg.text).catch(() => {}); // errors already surfaced as a status event
        break;
      }
      case "cancel": {
        const sid = env.sessionId;
        const t = sid ? tasks.get(sid) : activeTaskId ? tasks.get(activeTaskId) : undefined;
        t?.agent.abort();
        break;
      }
      case "follow_up": {
        const sid = env.sessionId;
        const t = sid ? tasks.get(sid) : undefined;
        t?.agent.followUp(userMessage(String((msg as { message: unknown }).message)));
        break;
      }
      case "ping":
        break;
    }
  }
  bus.subscribeControl((msg, env) => void handleControl(msg, env));

  // Receive side (request/reply). Two request shapes share this handler, and both
  // are independent of the human-chat control path, so an agent serves humans and
  // peers without special-casing:
  //   • DelegateRequest (Phase 4) ⇒ run in a fresh CHILD session linked to the
  //     caller's session via parent_session_id, adopting the caller's delegation
  //     path + budget so the cycle/depth/deadline guards compound down the tree.
  //   • RunRequest (Phase 1 kickoff) ⇒ run in the envelope's session, as before.
  bus.onRequest(async (payload, meta): Promise<RunResult | DelegateResult> => {
    if (isDelegateRequest(payload)) {
      const req = payload;
      // Each task is its own (receiver-side) session, keyed by req.taskId so multi-turn
      // collaboration on one task continues that context; a new taskId is a new task.
      const taskId = req.taskId ?? uuidv7();

      // Close: finalize the task (drop in-memory; the snapshot lingers, TTL'd).
      if (req.close) {
        const t = tasks.get(taskId);
        if (t) {
          await bus.saveTaskContext(taskId, t.agent.state.messages).catch(() => {});
          tasks.delete(taskId);
        }
        return { text: "", stopReason: "stop", taskId } satisfies DelegateResult;
      }

      // First time we see this task ⇒ announce its session (with the parent link) so
      // the recorder persists the parent→child edge.
      if (!tasks.has(taskId)) {
        await bus.announceSession({
          event: "open",
          sessionId: taskId,
          agentId: AGENT_ID,
          parentSessionId: req.parentSessionId,
          source: meta.source,
          ts: Date.now(),
        });
      }
      const t = await getTask(taskId);
      t.delegationPath = req.delegationPath;
      t.deadlineTs = req.budget?.deadlineTs;
      t.maxDepth = req.budget?.maxDepth ?? MAX_DELEGATION_DEPTH;
      t.causationId = meta.messageId;
      await bus.publishEvent(taskId, { type: "user_message", text: req.prompt, from: meta.source });
      const result = await runTurn(taskId, req.prompt);
      return { text: result.text, stopReason: result.stopReason, taskId } satisfies DelegateResult;
    }

    // RunRequest (Phase 1 kickoff / non-interactive): the envelope's session is the task.
    const { prompt } = payload as RunRequest;
    const sid = meta.sessionId ?? uuidv7();
    const t = await getTask(sid);
    t.delegationPath = [];
    t.deadlineTs = MAX_RUN_MS > 0 ? Date.now() + MAX_RUN_MS : undefined;
    t.maxDepth = MAX_DELEGATION_DEPTH;
    t.causationId = meta.messageId;
    await bus.publishEvent(sid, { type: "user_message", text: prompt, from: meta.source });
    return runTurn(sid, prompt); // throws ⇒ the bus turns it into an { ok:false } reply
  });

  await bus.register({
    id: AGENT_ID,
    capabilities: CAPABILITIES,
    description: AGENT_DESCRIPTION,
    status: "idle",
    metadata: { model: label },
  });

  // Seed the peer roster now, then refresh on a timer so connect/disconnect is picked
  // up (the injected prompt is at most one interval stale). Unref'd so it never keeps
  // the process alive on its own.
  await refreshRoster();
  const rosterTimer = setInterval(() => void refreshRoster(), ROSTER_REFRESH_MS);
  rosterTimer.unref?.();

  console.log(
    `[agent] ${AGENT_ID} online (model: ${label}). ${AGENT_DESCRIPTION}\n` +
      `[agent] roster: ${peers.map((p) => p.id).join(", ") || "(no peers yet)"}. Waiting for runs...`,
  );

  let closing = false;
  const shutdown = async () => {
    if (closing) return;
    closing = true;
    console.log("\n[agent] shutting down...");
    await bus.deregister();
    await bus.close();
    process.exit(0);
  };
  process.on("SIGINT", shutdown);
  process.on("SIGTERM", shutdown);
}

main().catch((err) => {
  console.error("[agent] fatal:", err);
  process.exit(1);
});
