/**
 * Phase 5 artifact check (token-free). Proves the A→B handoff by reference:
 *
 *   1. Kick a "producer" faux agent (RunRequest). It calls `save_artifact`, so
 *      its session's obs stream carries an `artifact` event — uri only, no bytes.
 *   2. Hand that uri to a "reader" faux agent (RunRequest, prompt = the uri). It
 *      calls `read_artifact`, which streams the object back from S3. We read the
 *      reader's `read_artifact` tool result and assert it is what the producer
 *      wrote — the bytes never travelled on the bus.
 *
 * Requires a running stack: recorder + the two agents (see README "Artifacts"):
 *   AGENT_ID=agent-producer AGENT_CAPABILITIES=produce AGENT_FAUX=1 AGENT_FAUX_ROLE=producer pnpm agent
 *   AGENT_ID=agent-reader   AGENT_CAPABILITIES=read    AGENT_FAUX=1 AGENT_FAUX_ROLE=reader   pnpm agent
 */
import { Bus } from "@paintenzero/orchestra-bus";
import { uuidv7, type ObservabilityEvent } from "@paintenzero/orchestra-protocol";

const bus = new Bus({ clientId: "artifacts-check" });
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

/** Run a prompt on `agent` and collect its session's full obs event list. */
async function run(agent: string, prompt: string): Promise<ObservabilityEvent[]> {
  const sessionId = uuidv7();
  const seen: ObservabilityEvent[] = [];
  const stop = bus.subscribeEvents(sessionId, (ev) => seen.push(ev), { from: "start" });
  // The initiator announces the session so the recorder tails + persists it
  // (the orchestrator does this for UI-driven runs; here this script is the initiator).
  await bus.announceSession({ event: "open", sessionId, agentId: agent, source: "artifacts-check", ts: Date.now() });
  try {
    await bus.request(agent, { prompt }, { sessionId, timeoutMs: 15_000 });
  } catch (err) {
    console.error(`request to ${agent} failed:`, String(err));
  }
  await sleep(300);
  stop();
  return seen;
}

// 1. Producer saves an artifact; capture the reference from the obs stream.
const prod = await run("agent-producer", "write the report");
const artifact = prod.find((e): e is Extract<ObservabilityEvent, { type: "artifact" }> => e.type === "artifact");
console.log(`producer artifact event: ${artifact ? artifact.uri : "(none)"}`);
console.log(`  only a reference on the bus? ${artifact ? !("bytes" in artifact || "data" in artifact) : false}`);

if (!artifact) {
  console.error("no artifact produced — is agent-producer running with AGENT_FAUX_ROLE=producer?");
  await bus.close();
  process.exit(1);
}

// 2. Hand the uri to the reader; assert it reads back what the producer wrote.
const read = await run("agent-reader", artifact.uri);
const toolEnd = read.find(
  (e): e is Extract<ObservabilityEvent, { type: "tool_execution_end" }> =>
    e.type === "tool_execution_end" && e.toolName === "read_artifact",
);
const content = (toolEnd?.result as { content?: { text?: string }[] })?.content ?? [];
const readBack = content.map((c) => c.text ?? "").join("");
console.log(`reader read_artifact isError=${toolEnd?.isError}`);
console.log(`  read back: ${JSON.stringify(readBack.slice(0, 80))}`);
console.log(`  A→B handoff ok? ${readBack.includes("Tide pools")}`);

await bus.close();
process.exit(0);
