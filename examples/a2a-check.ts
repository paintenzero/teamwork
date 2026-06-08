/**
 * Phase 4 guard check. Sends crafted DelegateRequests to agent-researcher (which
 * always tries to delegate onward to "agent-writer" by id) with seeded delegation
 * paths / budgets, then inspects the researcher's child-session trace to confirm
 * the ask_agent guard fired (cycle / depth / deadline) — and that the call
 * returns rather than hangs. Requires the demo stack from the README running:
 * recorder + orchestrator + agent-writer (worker) + agent-researcher
 * (AGENT_DELEGATE_TO=agent-writer).
 */
import { Bus } from "@paintenzero/orchestra-bus";
import { uuidv7, type DelegateResult, type SessionEvent } from "@paintenzero/orchestra-protocol";

const bus = new Bus({ clientId: "a2a-check" });
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

// Record session-open announcements so we can find the researcher's child session.
const opened: SessionEvent[] = [];
bus.watchSessions((ev) => opened.push(ev));

async function scenario(
  label: string,
  path: string[],
  budget: { maxDepth?: number; deadlineTs?: number } | undefined,
): Promise<void> {
  const parentSessionId = uuidv7();
  opened.length = 0;
  let replied = true;
  try {
    await bus.request<DelegateResult>(
      "agent-researcher",
      { prompt: "produce a short report", parentSessionId, delegationPath: path, budget },
      { sessionId: parentSessionId, timeoutMs: 15_000 },
    );
  } catch {
    replied = false; // a hang would surface as a RequestTimeoutError here
  }
  await sleep(500);

  const child = opened.find(
    (o) => o.parentSessionId === parentSessionId && o.agentId === "agent-researcher",
  );
  let toolErr: { isError: boolean; text: string } | undefined;
  if (child) {
    for (const env of await bus.readEvents(child.sessionId)) {
      const ev = env.payload;
      if (ev.type === "tool_execution_end" && ev.toolName === "ask_agent") {
        const content = (ev.result as { content?: { text?: string }[] })?.content ?? [];
        toolErr = { isError: ev.isError, text: content.map((c) => c.text ?? "").join("") };
      }
    }
  }
  console.log(
    `${label.padEnd(9)} replied=${replied}  ask_agent.isError=${toolErr?.isError}  ` +
      `msg="${toolErr?.text ?? "(no ask_agent event)"}"`,
  );
}

await scenario("happy", [], undefined);
await scenario("cycle", ["agent-writer"], undefined);
await scenario("depth", ["root", "mid"], { maxDepth: 1 });
await scenario("deadline", [], { deadlineTs: Date.now() - 1000 });

await bus.close();
process.exit(0);
